//! RGB8 frames to H.264 (openh264, constrained baseline) for a WebRTC video track.
//!
//! Quality `q` (0..1) sets resolution scale `0.25 + 0.75 q` and a bits-per-pixel target
//! `0.03 + 0.12 q`; the allocator's (quality, Hz) becomes the encoder's resolution, frame rate and
//! target bitrate (`bytes_per_frame(q) * hz`).

use crate::codec::image::Rgb8;
use anyhow::{Result, anyhow};
use openh264::OpenH264API;
use openh264::encoder::{BitRate, Encoder, EncoderConfig, FrameRate, FrameType, IntraFramePeriod, Profile, RateControlMode, UsageType};
use openh264::formats::{RgbSliceU8, YUVBuffer};

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

/// Box-filter downscale (or nearest upscale for the even-size rounding) of packed RGB8.
pub fn resize(rgb: &Rgb8, width: u32, height: u32) -> Vec<u8> {
    if (width, height) == (rgb.width, rgb.height) {
        return rgb.pixels.clone();
    }
    let mut out = vec![0u8; width as usize * height as usize * 3];
    let span = |dest: u32, dest_len: u32, source_len: u32| {
        let start = (dest as u64 * source_len as u64 / dest_len as u64) as u32;
        let end = (((dest as u64 + 1) * source_len as u64).div_ceil(dest_len as u64) as u32).clamp(start + 1, source_len);
        (start, end)
    };
    for y in 0..height {
        let (y0, y1) = span(y, height, rgb.height);
        for x in 0..width {
            let (x0, x1) = span(x, width, rgb.width);
            let mut sum = [0u32; 3];
            for source_y in y0..y1 {
                let row = &rgb.pixels[(source_y as usize * rgb.width as usize + x0 as usize) * 3..(source_y as usize * rgb.width as usize + x1 as usize) * 3];
                for pixel in row.chunks_exact(3) {
                    for channel in 0..3 {
                        sum[channel] += pixel[channel] as u32;
                    }
                }
            }
            let count = (y1 - y0) * (x1 - x0);
            let destination = &mut out[(y as usize * width as usize + x as usize) * 3..][..3];
            for channel in 0..3 {
                destination[channel] = ((sum[channel] + count / 2) / count) as u8;
            }
        }
    }
    out
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

    pub fn encode(&mut self, rgb: &Rgb8, quality: f64, hz: f64) -> Result<EncodedFrame> {
        let (width, height) = scaled_size(rgb.width, rgb.height, quality);
        let hz = hz.max(0.1);
        let bitrate_bps = (bytes_per_frame(rgb.width, rgb.height, quality) * hz * 8.0).max(10_000.0) as u32;
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
        let pixels = resize(rgb, width, height);
        let yuv = YUVBuffer::from_rgb8_source(RgbSliceU8::new(&pixels, (width as usize, height as usize)));
        let bitstream = encoder.encode(&yuv).map_err(|e| anyhow!("openh264: {e}"))?;
        let keyframe = matches!(bitstream.frame_type(), FrameType::IDR | FrameType::I);
        Ok(EncodedFrame { data: bitstream.to_vec(), width, height, keyframe })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(width: u32, height: u32) -> Rgb8 {
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
        Rgb8 { width, height, pixels }
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
        let small = resize(&pattern(320, 240), 2, 2);
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
}
