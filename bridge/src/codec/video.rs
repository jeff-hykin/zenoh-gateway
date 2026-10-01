//! Video codecs' frames (RGB8 or I420) to H.264 (openh264, constrained baseline) for a WebRTC video track.
//!
//! Quality `q` (0..1) sets resolution scale `0.25 + 0.75 q` and a bits-per-pixel target
//! `0.03 + 0.12 q`; the allocator's (quality, Hz) becomes the encoder's resolution, frame rate and
//! target bitrate (`bytes_per_frame(q) * hz`).

use crate::codec::{PixelFormat, VideoImage};
use anyhow::{Result, anyhow};
use openh264::OpenH264API;
use openh264::encoder::{BitRate, Encoder, EncoderConfig, FrameRate, FrameType, IntraFramePeriod, Profile, RateControlMode, UsageType};
use openh264::formats::{RgbSliceU8, YUVBuffer};
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

/// Box-filter downscale (or nearest upscale for the even-size rounding) of a packed plane with
/// `channels` bytes per pixel.
fn resize_plane(source: &[u8], (source_width, source_height): (u32, u32), channels: usize, (width, height): (u32, u32)) -> Vec<u8> {
    if (width, height) == (source_width, source_height) {
        return source[..width as usize * height as usize * channels].to_vec();
    }
    let mut out = vec![0u8; width as usize * height as usize * channels];
    let span = |dest: u32, dest_len: u32, source_len: u32| {
        let start = (dest as u64 * source_len as u64 / dest_len as u64) as u32;
        let end = (((dest as u64 + 1) * source_len as u64).div_ceil(dest_len as u64) as u32).clamp(start + 1, source_len);
        (start, end)
    };
    let mut sum = vec![0u32; channels];
    for y in 0..height {
        let (y0, y1) = span(y, height, source_height);
        for x in 0..width {
            let (x0, x1) = span(x, width, source_width);
            sum.fill(0);
            for source_y in y0..y1 {
                let row_start = source_y as usize * source_width as usize;
                let row = &source[(row_start + x0 as usize) * channels..(row_start + x1 as usize) * channels];
                for pixel in row.chunks_exact(channels) {
                    for (total, &value) in sum.iter_mut().zip(pixel) {
                        *total += value as u32;
                    }
                }
            }
            let count = (y1 - y0) * (x1 - x0);
            let destination = &mut out[(y as usize * width as usize + x as usize) * channels..][..channels];
            for (value, total) in destination.iter_mut().zip(&sum) {
                *value = ((total + count / 2) / count) as u8;
            }
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
            YUVBuffer::from_rgb8_source(RgbSliceU8::new(&pixels, (width as usize, height as usize)))
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
}
