//! Video codecs' frames (RGB8 or I420) to H.264 (openh264, constrained baseline) for a WebRTC video track.
//!
//! Quality `q` (0..1) sets resolution scale `0.25 + 0.75 q` and a bits-per-pixel target
//! `0.03 + 0.12 q`; the allocator's (quality, Hz) becomes the encoder's resolution, frame rate and
//! target bitrate (`bytes_per_frame(q) * hz`).

use crate::codec::{PixelFormat, VideoImage};
use anyhow::{Result, anyhow};
use openh264::OpenH264API;
use openh264::encoder::{BitRate, Encoder, EncoderConfig, FrameRate, FrameType, IntraFramePeriod, Profile, RateControlMode, UsageType};
use openh264::formats::YUVBuffer;
#[cfg(test)]
use crate::codec::image::Rgb8;

const MIN_DIMENSION: u32 = 16;
/// Keyframe at least this often (seconds), on top of PLI/FIR requests from the browser.
const KEYFRAME_SECONDS: f64 = 3.0;
/// Re-create the encoder when bitrate or frame rate drift past this ratio.
const RECONFIGURE_RATIO: f64 = 1.3;

pub fn resolution_scale(quality: f64) -> f64 {
    0.25 + 0.75 * quality.clamp(0.0, 1.0)
}

fn bits_per_pixel(quality: f64) -> f64 {
    0.03 + 0.12 * quality.clamp(0.0, 1.0)
}

/// Even dimensions at `quality` (H.264 4:2:0 needs even sizes).
pub fn scaled_size(width: u32, height: u32, quality: f64) -> (u32, u32) {
    let scale = resolution_scale(quality);
    let even = |value: u32| (((value as f64 * scale).round() as u32) & !1).max(MIN_DIMENSION);
    (even(width), even(height))
}

/// Modeled encoded size per frame (the allocator's price for this stream at `quality`).
pub fn bytes_per_frame(width: u32, height: u32, quality: f64) -> f64 {
    let (scaled_width, scaled_height) = scaled_size(width, height, quality);
    scaled_width as f64 * scaled_height as f64 * bits_per_pixel(quality) / 8.0
}

/// The source range `[start, end)` each destination index averages: a box filter when shrinking,
/// nearest when growing (the even-size rounding can add a pixel).
fn spans(destination_len: u32, source_len: u32) -> Vec<(usize, usize)> {
    (0..destination_len as u64)
        .map(|index| {
            let start = (index * source_len as u64 / destination_len as u64) as u32;
            let end = ((index + 1) * source_len as u64).div_ceil(destination_len as u64) as u32;
            (start as usize, end.clamp(start + 1, source_len) as usize)
        })
        .collect()
}

/// Box-filter downscale (or nearest upscale for the even-size rounding) of a packed plane with
/// `channels` (1 or 3) bytes per pixel. Row at a time: each destination row sums its source rows
/// once into per-column totals, then each destination pixel sums its span of those, so every source
/// byte is read once.
fn resize_plane(source: &[u8], (source_width, source_height): (u32, u32), channels: usize, (width, height): (u32, u32)) -> Vec<u8> {
    match channels {
        1 => resize_packed::<1>(source, (source_width, source_height), (width, height)),
        3 => resize_packed::<3>(source, (source_width, source_height), (width, height)),
        other => unreachable!("{other} channels"),
    }
}

fn resize_packed<const CHANNELS: usize>(source: &[u8], (source_width, source_height): (u32, u32), (width, height): (u32, u32)) -> Vec<u8> {
    let row_len = source_width as usize * CHANNELS;
    if (width, height) == (source_width, source_height) {
        return source[..row_len * height as usize].to_vec();
    }
    let (columns, rows) = (spans(width, source_width), spans(height, source_height));
    let mut out = vec![0u8; width as usize * height as usize * CHANNELS];
    let mut column_totals = vec![0u32; row_len];
    for (destination_row, &(row_start, row_end)) in out.chunks_exact_mut(width as usize * CHANNELS).zip(&rows) {
        let (first, rest) = source[row_start * row_len..row_end * row_len].split_at(row_len);
        for (total, &value) in column_totals.iter_mut().zip(first) {
            *total = value as u32;
        }
        for source_row in rest.chunks_exact(row_len) {
            for (total, &value) in column_totals.iter_mut().zip(source_row) {
                *total += value as u32;
            }
        }
        let row_count = (row_end - row_start) as f32;
        for (pixel, &(column_start, column_end)) in destination_row.as_chunks_mut::<CHANNELS>().0.iter_mut().zip(&columns) {
            let mut totals = [0u32; CHANNELS];
            for source_pixel in column_totals[column_start * CHANNELS..column_end * CHANNELS].as_chunks::<CHANNELS>().0 {
                for (total, value) in totals.iter_mut().zip(source_pixel) {
                    *total += value;
                }
            }
            let scale = 1.0 / (row_count * (column_end - column_start) as f32);
            for (value, total) in pixel.iter_mut().zip(totals) {
                *value = (total as f32 * scale + 0.5) as u8;
            }
        }
    }
    out
}

/// Packed RGB8 to I420 (BT.601 limited range, openh264's own coefficients) in integer math; chroma
/// from each 2×2 block's mean. `width` and `height` are even.
pub(crate) fn rgb_to_i420(rgb: &[u8], width: usize, height: usize) -> Vec<u8> {
    let mut out = vec![0u8; width * height * 3 / 2];
    let (luma, chroma) = out.split_at_mut(width * height);
    let (u_plane, v_plane) = chroma.split_at_mut(width * height / 4);
    for (luma_row, rgb_row) in luma.chunks_exact_mut(width).zip(rgb.chunks_exact(width * 3)) {
        for (value, pixel) in luma_row.iter_mut().zip(rgb_row.chunks_exact(3)) {
            let (r, g, b) = (pixel[0] as i32, pixel[1] as i32, pixel[2] as i32);
            *value = (((66 * r + 129 * g + 25 * b + 128) >> 8) + 16) as u8;
        }
    }
    let half_width = width / 2;
    for (block_row, (u_row, v_row)) in u_plane.chunks_exact_mut(half_width).zip(v_plane.chunks_exact_mut(half_width)).enumerate() {
        let top = &rgb[block_row * 2 * width * 3..][..width * 3];
        let bottom = &rgb[(block_row * 2 + 1) * width * 3..][..width * 3];
        for (column, (u, v)) in u_row.iter_mut().zip(v_row.iter_mut()).enumerate() {
            let at = column * 6;
            let sum = |channel: usize| (top[at + channel] as i32 + top[at + 3 + channel] as i32 + bottom[at + channel] as i32 + bottom[at + 3 + channel] as i32 + 2) >> 2;
            let (r, g, b) = (sum(0), sum(1), sum(2));
            *u = (((-38 * r - 74 * g + 112 * b + 128) >> 8) + 128).clamp(0, 255) as u8;
            *v = (((112 * r - 94 * g - 18 * b + 128) >> 8) + 128).clamp(0, 255) as u8;
        }
    }
    out
}

/// The picture scaled to `width × height` (both even) as an I420 buffer for the encoder.
fn to_yuv(image: &VideoImage, width: u32, height: u32) -> YUVBuffer {
    let source = (image.width(), image.height());
    match image.format() {
        PixelFormat::Rgb8 => {
            let pixels = resize_plane(image.data(), source, 3, (width, height));
            YUVBuffer::from_vec(rgb_to_i420(&pixels, width as usize, height as usize), width as usize, height as usize)
        }
        PixelFormat::I420 => {
            let luma_len = source.0 as usize * source.1 as usize;
            let (luma, chroma) = image.data().split_at(luma_len);
            let (u, v) = chroma.split_at(luma_len / 4);
            let half_source = (source.0 / 2, source.1 / 2);
            let half = (width / 2, height / 2);
            let mut yuv = resize_plane(luma, source, 1, (width, height));
            yuv.extend(resize_plane(u, half_source, 1, half));
            yuv.extend(resize_plane(v, half_source, 1, half));
            YUVBuffer::from_vec(yuv, width as usize, height as usize)
        }
    }
}

pub struct EncodedFrame {
    /// Annex B access unit
    pub data: Vec<u8>,
    pub width: u32,
    pub height: u32,
    pub keyframe: bool,
}

#[derive(Clone, Copy, PartialEq)]
struct Settings {
    width: u32,
    height: u32,
    bitrate_bps: u32,
    fps: f32,
}

/// One per (frontend, subscription): rate control and reference frames are per receiver.
#[derive(Default)]
pub struct VideoEncoder {
    encoder: Option<(Encoder, Settings)>,
    keyframe_requested: bool,
}

impl VideoEncoder {
    pub fn request_keyframe(&mut self) {
        self.keyframe_requested = true;
    }

    pub fn encode(&mut self, image: &VideoImage, quality: f64, hz: f64) -> Result<EncodedFrame> {
        let (width, height) = scaled_size(image.width(), image.height(), quality);
        let hz = hz.max(0.1);
        let bitrate_bps = (bytes_per_frame(image.width(), image.height(), quality) * hz * 8.0).max(10_000.0) as u32;
        let wanted = Settings { width, height, bitrate_bps, fps: hz as f32 };
        let drifted = |old: f64, new: f64| old / new > RECONFIGURE_RATIO || new / old > RECONFIGURE_RATIO;
        let reconfigure = match &self.encoder {
            None => true,
            Some((_, current)) => {
                (current.width, current.height) != (width, height)
                    || drifted(current.bitrate_bps as f64, bitrate_bps as f64)
                    || drifted(current.fps as f64, hz)
            }
        };
        if reconfigure {
            let config = EncoderConfig::new()
                .bitrate(BitRate::from_bps(bitrate_bps))
                .max_frame_rate(FrameRate::from_hz(wanted.fps))
                .rate_control_mode(RateControlMode::Bitrate)
                .usage_type(UsageType::CameraVideoRealTime)
                .profile(Profile::Baseline)
                .skip_frames(false)
                .intra_frame_period(IntraFramePeriod::from_num_frames(((hz * KEYFRAME_SECONDS).ceil() as u32).max(1)));
            let encoder = Encoder::with_api_config(OpenH264API::from_source(), config).map_err(|e| anyhow!("openh264: {e}"))?;
            self.encoder = Some((encoder, wanted));
            self.keyframe_requested = false;
        }
        let (encoder, _) = self.encoder.as_mut().expect("encoder configured above");
        if std::mem::take(&mut self.keyframe_requested) {
            encoder.force_intra_frame();
        }
        let yuv = to_yuv(image, width, height);
        let bitstream = encoder.encode(&yuv).map_err(|e| anyhow!("openh264: {e}"))?;
        let keyframe = matches!(bitstream.frame_type(), FrameType::IDR | FrameType::I);
        Ok(EncodedFrame { data: bitstream.to_vec(), width, height, keyframe })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(width: u32, height: u32) -> VideoImage {
        let mut pixels = Vec::with_capacity((width * height * 3) as usize);
        for y in 0..height {
            for x in 0..width {
                pixels.extend_from_slice(match (y < height / 2, x < width / 2) {
                    (true, true) => &[255, 0, 0],
                    (true, false) => &[0, 255, 0],
                    (false, true) => &[0, 0, 255],
                    (false, false) => &[255, 255, 255],
                });
            }
        }
        Rgb8 { width, height, pixels }.into()
    }

    #[test]
    fn sizes_are_even_and_shrink_with_quality() {
        assert_eq!(scaled_size(320, 240, 1.0), (320, 240));
        assert_eq!(scaled_size(321, 241, 1.0), (320, 240));
        assert_eq!(scaled_size(320, 240, 0.0), (80, 60));
        assert!(bytes_per_frame(320, 240, 0.2) < bytes_per_frame(320, 240, 0.8));
    }

    #[test]
    fn resize_averages_quadrants() {
        let small = resize_plane(pattern(320, 240).data(), (320, 240), 3, (2, 2));
        assert_eq!(small, vec![255, 0, 0, 0, 255, 0, 0, 0, 255, 255, 255, 255]);
    }

    #[test]
    fn rgb_to_i420_matches_openh264() {
        let (width, height) = (64usize, 48usize);
        let rgb: Vec<u8> = (0..width * height * 3).map(|index| ((index * 7919) % 251) as u8).collect();
        let ours = rgb_to_i420(&rgb, width, height);
        let theirs = YUVBuffer::from_rgb8_source(openh264::formats::RgbSliceU8::new(&rgb, (width, height)));
        use openh264::formats::YUVSource;
        let (y, u, v) = (theirs.y(), theirs.u(), theirs.v());
        let reference: Vec<u8> = y.iter().chain(u).chain(v).copied().collect();
        let worst = ours.iter().zip(&reference).map(|(a, b)| (*a as i32 - *b as i32).abs()).max().unwrap();
        assert!(worst <= 1, "differs from openh264's conversion by up to {worst}");
    }

    #[test]
    fn encodes_keyframe_then_smaller_frames() {
        let mut encoder = VideoEncoder::default();
        let frame = pattern(320, 240);
        let first = encoder.encode(&frame, 1.0, 10.0).unwrap();
        assert!(first.keyframe && first.data.starts_with(&[0, 0, 0, 1]));
        assert_eq!((first.width, first.height), (320, 240));
        let second = encoder.encode(&frame, 1.0, 10.0).unwrap();
        assert!(!second.keyframe);
        encoder.request_keyframe();
        assert!(encoder.encode(&frame, 1.0, 10.0).unwrap().keyframe);
        let low = encoder.encode(&frame, 0.0, 10.0).unwrap();
        assert_eq!((low.width, low.height), (80, 60));
    }

    #[test]
    fn encodes_i420() {
        let (width, height) = (64u32, 48u32);
        let mut data = vec![200u8; (width * height) as usize];
        data.extend(vec![90u8; (width * height / 2) as usize]);
        let image = VideoImage::i420(width, height, data).unwrap();
        let mut encoder = VideoEncoder::default();
        let frame = encoder.encode(&image, 0.5, 10.0).unwrap();
        assert!(frame.keyframe && frame.data.starts_with(&[0, 0, 0, 1]));
        assert_eq!((frame.width, frame.height), scaled_size(width, height, 0.5));
        assert!(VideoImage::i420(63, 48, vec![0; 63 * 48 * 3 / 2]).is_err(), "odd sizes are refused");
        assert!(VideoImage::rgb8(2, 2, vec![0; 11]).is_err(), "wrong length is refused");
    }

    /// Per-stage timing on a real camera frame: `ZW_BENCH_JPEG=frame.jpg cargo test --release bench_stages -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn bench_stages() {
        let path = std::env::var("ZW_BENCH_JPEG").expect("ZW_BENCH_JPEG");
        let jpeg = std::fs::read(path).unwrap();
        let time = |name: &str, runs: u32, mut work: Box<dyn FnMut() + '_>| {
            work();
            let start = std::time::Instant::now();
            for _ in 0..runs {
                work();
            }
            println!("{name:<40} {:7.2} ms", start.elapsed().as_secs_f64() * 1000.0 / runs as f64);
        };
        time("jpeg -> rgb (zune)", 10, Box::new(|| drop(crate::codec::image::compressed_to_rgb(&jpeg, "jpeg").unwrap())));
        time("jpeg -> i420 (the video path)", 10, Box::new(|| drop(crate::codec::image::compressed_to_video(&jpeg, "jpeg").unwrap())));
        let image = crate::codec::image::compressed_to_video(&jpeg, "jpeg").unwrap();
        println!("source {}x{} {:?}", image.width(), image.height(), image.format());
        // the same picture one pixel over, so every P-frame has real motion to code
        let shifted: Vec<u8> = image.data()[1..].iter().chain(&image.data()[..1]).copied().collect();
        let shifted = VideoImage { data: shifted, ..image.clone() };
        for quality in [0.8, 0.6, 0.3, 0.1] {
            let (width, height) = scaled_size(image.width(), image.height(), quality);
            time(&format!("to_yuv q{quality} {width}x{height}"), 10, Box::new(|| drop(to_yuv(&image, width, height))));
            let mut encoder = VideoEncoder::default();
            let mut flip = false;
            time(&format!("encode (to_yuv + h264) q{quality}"), 20, Box::new(|| {
                flip = !flip;
                drop(encoder.encode(if flip { &image } else { &shifted }, quality, 30.0).unwrap())
            }));
        }
    }
}
