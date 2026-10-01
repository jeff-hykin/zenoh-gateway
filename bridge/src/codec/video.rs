//! Video encoders: the [`VideoEncoder`] trait, the default one (pictures, RGB8 or I420, to H.264 with openh264,
//! constrained baseline), and the [`VideoPolicy`] that turns a stream's grant into a picture size.
//!
//! The allocator grants each video stream bytes/s and a frame rate; the encoder runs at that bitrate. The picture keeps
//! its full size unless the grant would leave fewer than `min_bits_per_pixel` there (an encoder at its coarsest
//! quantizer overshoots below that), then it shrinks just enough, never below `min_resolution_scale`.

use crate::codec::{DecodedFrame, PixelFormat, VideoImage};
use anyhow::{Result, anyhow, ensure};
use openh264::OpenH264API;
use openh264::encoder::{BitRate, Encoder, EncoderConfig, FrameRate, FrameType, IntraFramePeriod, MatrixCoefficients, Profile, RateControlMode, UsageType, VuiConfig};
use openh264::formats::YUVBuffer;

const MIN_DIMENSION: u32 = 16;
/// Keyframe at least this often (seconds), on top of PLI/FIR requests from the browser.
const KEYFRAME_SECONDS: f64 = 3.0;
/// Re-create the encoder when the frame rate drifts past this ratio (bitrate changes apply in place).
const RECONFIGURE_RATIO: f64 = 1.3;
/// What the encoders signal: the BT.601 matrix (what the pictures are), BT.709 primaries and transfer (what browsers
/// assume for video, so only the matrix differs from their guess).
const VUI: VuiConfig = VuiConfig::bt709().matrix_coefficients(MatrixCoefficients::Smpte170M);
/// Lowest bitrate handed to an encoder, bits/s.
const MIN_BITRATE: f64 = 10_000.0;

/// How video streams spend their grant: the server's default ([`ServerBuilder::video_policy`](crate::ServerBuilder::video_policy)),
/// which subscriptions override with `maxBitrate`, `minResolutionScale` and `maxResolution`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct VideoPolicy {
    /// Most bits/s a stream asks the allocator for; `None`: `max_bits_per_pixel` at the source's size and rate.
    pub max_bitrate: Option<f64>,
    /// See `max_bitrate` (default 0.3: ~17 Mbit/s for 720p60).
    pub max_bits_per_pixel: f64,
    /// Below this many bits per pixel per frame the picture shrinks (default 0.05: openh264's coarsest quantizer
    /// still overshoots below ~0.03 on a busy scene).
    pub min_bits_per_pixel: f64,
    /// Smallest share of the source's width and height (default 0.25).
    pub min_resolution_scale: f64,
    /// Box the picture is fitted into, aspect kept (default none: the source's size).
    pub max_resolution: Option<(u32, u32)>,
}

impl Default for VideoPolicy {
    fn default() -> Self {
        VideoPolicy { max_bitrate: None, max_bits_per_pixel: 0.3, min_bits_per_pixel: 0.05, min_resolution_scale: 0.25, max_resolution: None }
    }
}

impl VideoPolicy {
    pub(crate) fn validate(&self) -> Result<()> {
        let positive = |value: f64| value.is_finite() && value > 0.0;
        ensure!(self.max_bitrate.is_none_or(positive), "max bitrate must be a positive number of bits/s");
        ensure!(positive(self.max_bits_per_pixel) && positive(self.min_bits_per_pixel), "bits per pixel must be positive");
        ensure!(self.min_resolution_scale > 0.0 && self.min_resolution_scale <= 1.0, "min resolution scale must be within (0, 1]");
        ensure!(self.max_resolution.is_none_or(|(width, height)| width >= MIN_DIMENSION && height >= MIN_DIMENSION), "max resolution must be at least {MIN_DIMENSION}x{MIN_DIMENSION}");
        Ok(())
    }

    /// Share of the source's width and height that fits `max_resolution`.
    fn fit(&self, (width, height): (u32, u32)) -> f64 {
        self.max_resolution.map_or(1.0, |(max_width, max_height)| (max_width as f64 / width as f64).min(max_height as f64 / height as f64).min(1.0))
    }

    /// The allocator's price of one frame at `quality`: from the smallest picture at `min_bits_per_pixel` (0) to the
    /// most a frame may take at `frame_hz` frames/s (1).
    pub(crate) fn frame_bytes(&self, source: (u32, u32), frame_hz: f64, quality: f64) -> f64 {
        let pixels = source.0 as f64 * source.1 as f64;
        let most = match self.max_bitrate {
            Some(bps) => bps / frame_hz.max(0.1),
            None => self.max_bits_per_pixel * pixels * self.fit(source).powi(2),
        } / 8.0;
        let least = (self.min_bits_per_pixel * pixels * self.min_resolution_scale.min(self.fit(source)).powi(2) / 8.0).min(most);
        least + (most - least) * quality.clamp(0.0, 1.0)
    }

    /// Even output size for `bitrate_bps` at `fps`: the source's (fitted into `max_resolution`), shrunk only to keep
    /// `min_bits_per_pixel` and to `scale_cap` (the CPU governor's), never below `min_resolution_scale`.
    pub(crate) fn size(&self, source: (u32, u32), bitrate_bps: f64, fps: f64, scale_cap: f64) -> (u32, u32) {
        let fit = self.fit(source);
        let affordable = (bitrate_bps / (fps.max(0.1) * source.0 as f64 * source.1 as f64 * self.min_bits_per_pixel)).sqrt();
        let scale = fit.min(affordable).min(scale_cap).max(self.min_resolution_scale.min(fit));
        let even = |value: u32| (((value as f64 * scale).round() as u32) & !1).max(MIN_DIMENSION);
        (even(source.0), even(source.1))
    }
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

/// Box-filter downscale (or nearest upscale for the even-size rounding) of a packed plane with `CHANNELS` bytes per
/// pixel. Each destination row sums its source rows into per-column totals, then each pixel its span of those, so every
/// source byte is read once.
fn resize_plane<const CHANNELS: usize>(source: &[u8], (source_width, source_height): (u32, u32), (width, height): (u32, u32)) -> Vec<u8> {
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

/// Packed RGB8 to I420 (BT.601 limited range, openh264's own coefficients) in integer math; chroma from each 2×2 block's
/// mean. `width` and `height` are even. The encoders tag BT.601 ([`VUI`]): browsers read untagged HD video as BT.709 (a
/// ~2.7 dB loss), and tagged BT.601 measured as good as BT.709 through openh264 and ~0.5 dB better through VideoToolbox.
fn rgb_to_i420(rgb: &[u8], width: usize, height: usize) -> Vec<u8> {
    let mut out = vec![0u8; width * height * 3 / 2];
    let (luma, chroma) = out.split_at_mut(width * height);
    let (u_plane, v_plane) = chroma.split_at_mut(width * height / 4);
    for (luma_row, rgb_row) in luma.chunks_exact_mut(width).zip(rgb.chunks_exact(width * 3)) {
        for (value, pixel) in luma_row.iter_mut().zip(rgb_row.as_chunks::<3>().0) {
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

/// The picture scaled to `width × height` (both even) as planar I420 (BT.601 limited range).
pub(crate) fn to_i420(image: &VideoImage, width: u32, height: u32) -> Vec<u8> {
    let source = (image.width(), image.height());
    match image.format() {
        PixelFormat::Rgb8 => {
            let pixels = resize_plane::<3>(image.data(), source, (width, height));
            rgb_to_i420(&pixels, width as usize, height as usize)
        }
        PixelFormat::I420 => {
            let luma_len = source.0 as usize * source.1 as usize;
            let (luma, chroma) = image.data().split_at(luma_len);
            let (u, v) = chroma.split_at(luma_len / 4);
            let half_source = (source.0 / 2, source.1 / 2);
            let half = (width / 2, height / 2);
            let mut yuv = resize_plane::<1>(luma, source, (width, height));
            yuv.extend(resize_plane::<1>(u, half_source, half));
            yuv.extend(resize_plane::<1>(v, half_source, half));
            yuv
        }
    }
}

/// The WebRTC video codec a [`VideoEncoder`] produces; the bridge negotiates it and packetizes its frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VideoFormat {
    /// H.264 constrained baseline, Annex B access units.
    H264,
    /// VP8 frames.
    Vp8,
    /// VP9 (profile 0) frames.
    Vp9,
    /// AV1 temporal units (OBUs with size fields).
    Av1,
}

impl VideoFormat {
    /// The RTP mime type, e.g. `"video/AV1"`.
    pub fn mime_type(self) -> &'static str {
        match self {
            VideoFormat::H264 => "video/H264",
            VideoFormat::Vp8 => "video/VP8",
            VideoFormat::Vp9 => "video/VP9",
            VideoFormat::Av1 => "video/AV1",
        }
    }
}

/// What the bridge asks of the next frame: the bitrate and rate the allocator granted, the size the [`VideoPolicy`]
/// and CPU governor picked for them, and whether a viewer needs a keyframe.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub struct VideoTarget {
    /// The allocator's quality, 0..1: the share of the stream's most bits per frame it was granted.
    pub quality: f64,
    /// Even output size: the source's, unless the grant is under the policy's bits-per-pixel floor there.
    pub width: u32,
    /// See `width`.
    pub height: u32,
    /// What the allocator granted, bits/s: encode at this.
    pub bitrate_bps: u32,
    /// Frames per second the stream is granted.
    pub fps: f64,
    /// A viewer joined or lost a frame (PLI/FIR): this frame must be a keyframe.
    pub keyframe: bool,
}

impl VideoTarget {
    /// `width × height` (even) at `bitrate_bps` and `fps`, quality 1, no keyframe asked for: e.g. to try an encoder.
    pub fn new(width: u32, height: u32, bitrate_bps: u32, fps: f64) -> Self {
        VideoTarget { quality: 1.0, width, height, bitrate_bps, fps, keyframe: false }
    }
}

/// One encoded frame, in its [`VideoFormat`]'s bitstream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedVideo {
    /// The frame's bitstream.
    pub data: Vec<u8>,
    /// Its size, in pixels.
    pub width: u32,
    /// See `width`.
    pub height: u32,
    /// Decodable on its own.
    pub keyframe: bool,
}

/// Turns a video codec's decoded frames into one WebRTC codec's frames. The bridge makes one per encode session (the
/// viewers of a stream at one target share it) with [`Codec::video_encoder`](crate::Codec::video_encoder), else the
/// server's [`ServerBuilder::video_encoder`](crate::ServerBuilder::video_encoder), else [`H264Encoder`], and calls it on
/// tokio's blocking pool; it negotiates [`format`](Self::format), packetizes and paces the frames and measures them for
/// the allocator. A hardware encoder (VideoToolbox, NVENC, a Jetson's) implements this.
pub trait VideoEncoder: Send {
    /// The codec of the frames (fixed for the encoder's life).
    fn format(&self) -> VideoFormat;

    /// Encodes one decoded frame at `target`; `None` while a pipelined encoder has nothing out yet.
    fn encode(&mut self, frame: &DecodedFrame, target: &VideoTarget) -> Result<Option<EncodedVideo>>;
}

#[derive(Clone, Copy, PartialEq)]
struct Settings {
    width: u32,
    height: u32,
    fps: f32,
}

/// The default encoder: [`DecodedFrame::Video`] to H.264 with openh264 (BT.601 signaled), re-created when the
/// target's size or rate moves; bitrate changes apply in place.
#[derive(Default)]
pub struct H264Encoder {
    encoder: Option<(Encoder, Settings, u32)>,
}

/// Sets openh264's target bitrate and ceiling (the layer's; target ≤ ceiling holds at every step) without a new encoder.
fn set_bitrate(encoder: &mut Encoder, from_bps: u32, to_bps: u32) -> Result<()> {
    use openh264_sys2::{ENCODER_OPTION_BITRATE, ENCODER_OPTION_MAX_BITRATE, SBitrateInfo, SPATIAL_LAYER_0};
    let mut info = SBitrateInfo { iLayer: SPATIAL_LAYER_0, iBitrate: to_bps.min(i32::MAX as u32) as i32 };
    let order = if to_bps > from_bps { [ENCODER_OPTION_MAX_BITRATE, ENCODER_OPTION_BITRATE] } else { [ENCODER_OPTION_BITRATE, ENCODER_OPTION_MAX_BITRATE] };
    for option in order {
        // SAFETY: the encoder is initialized (it encoded a frame) and `info` outlives the call
        let status = unsafe { encoder.raw_api().set_option(option, (&raw mut info).cast()) };
        ensure!(status == 0, "openh264 refused bitrate {to_bps} (status {status})");
    }
    Ok(())
}

impl VideoEncoder for H264Encoder {
    fn format(&self) -> VideoFormat {
        VideoFormat::H264
    }

    fn encode(&mut self, frame: &DecodedFrame, target: &VideoTarget) -> Result<Option<EncodedVideo>> {
        let DecodedFrame::Video(image) = frame else { return Err(anyhow!("the software H.264 encoder takes pictures, got {frame:?}")) };
        let VideoTarget { width, height, bitrate_bps, fps: hz, .. } = *target;
        let wanted = Settings { width, height, fps: hz as f32 };
        let drifted = |old: f64, new: f64| old / new > RECONFIGURE_RATIO || new / old > RECONFIGURE_RATIO;
        let reconfigure = self.encoder.as_ref().is_none_or(|(_, current, _)| (current.width, current.height) != (width, height) || drifted(current.fps as f64, hz));
        if reconfigure {
            let config = EncoderConfig::new()
                .bitrate(BitRate::from_bps(bitrate_bps))
                .max_frame_rate(FrameRate::from_hz(wanted.fps))
                .rate_control_mode(RateControlMode::Bitrate)
                .usage_type(UsageType::CameraVideoRealTime)
                .profile(Profile::Baseline)
                .skip_frames(false)
                .vui(VUI)
                .intra_frame_period(IntraFramePeriod::from_num_frames(((hz * KEYFRAME_SECONDS).ceil() as u32).max(1)));
            let encoder = Encoder::with_api_config(OpenH264API::from_source(), config).map_err(|e| anyhow!("openh264: {e}"))?;
            self.encoder = Some((encoder, wanted, bitrate_bps));
        } else if let Some((encoder, _, current_bps)) = self.encoder.as_mut() {
            if *current_bps != bitrate_bps {
                set_bitrate(encoder, *current_bps, bitrate_bps)?;
                *current_bps = bitrate_bps;
            }
            if target.keyframe {
                // a new encoder starts with one anyway
                encoder.force_intra_frame();
            }
        }
        let (encoder, _, _) = self.encoder.as_mut().expect("encoder configured above");
        let yuv = YUVBuffer::from_vec(to_i420(image, width, height), width as usize, height as usize);
        let bitstream = encoder.encode(&yuv).map_err(|e| anyhow!("openh264: {e}"))?;
        let keyframe = matches!(bitstream.frame_type(), FrameType::IDR | FrameType::I);
        Ok(Some(EncodedVideo { data: bitstream.to_vec(), width, height, keyframe }))
    }
}

/// The target for a grant of `bitrate_bps` at `hz` for a `source`-sized picture, at most `scale_cap` of its size.
pub(crate) fn target(policy: &VideoPolicy, source: (u32, u32), quality: f64, bitrate_bps: f64, hz: f64, scale_cap: f64, keyframe: bool) -> VideoTarget {
    let (fps, bitrate_bps) = (hz.max(0.1), bitrate_bps.max(MIN_BITRATE));
    let (width, height) = policy.size(source, bitrate_bps, fps, scale_cap);
    VideoTarget { quality, width, height, bitrate_bps: bitrate_bps as u32, fps, keyframe }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Four solid quadrants: red, green / blue, white.
    fn quadrants(width: u32, height: u32) -> VideoImage {
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
        VideoImage::rgb8(width, height, pixels).unwrap()
    }

    #[test]
    fn full_size_until_the_bits_per_pixel_floor() {
        let policy = VideoPolicy::default();
        let hd = (1280, 720);
        // 0.05 bits per pixel at 720p60 is 2.76 Mbit/s: above it the picture keeps its size
        assert_eq!(policy.size(hd, 3e6, 60.0, 1.0), (1280, 720));
        assert_eq!(policy.size((321, 241), 1e9, 60.0, 1.0), (320, 240), "even sizes");
        let (width, _) = policy.size(hd, 2.76e6 / 4.0, 60.0, 1.0);
        assert!((638..=642).contains(&width), "a quarter of the bits: half the width, {width}");
        assert_eq!(policy.size(hd, 1e3, 60.0, 1.0), (320, 180), "never below min_resolution_scale");
        assert_eq!(policy.size(hd, 1e9, 60.0, 0.5), (640, 360), "the CPU governor's cap");
        let boxed = VideoPolicy { max_resolution: Some((640, 640)), ..policy };
        assert_eq!(boxed.size(hd, 1e9, 60.0, 1.0), (640, 360), "fitted into max_resolution");
        // the price: the smallest picture at the floor (quality 0) to the most bits per frame (1)
        assert!((policy.frame_bytes(hd, 60.0, 1.0) - 0.3 * 1280.0 * 720.0 / 8.0).abs() < 1.0);
        assert!((policy.frame_bytes(hd, 60.0, 0.0) - 0.05 * 320.0 * 180.0 / 8.0).abs() < 1.0);
        let capped = VideoPolicy { max_bitrate: Some(6e6), ..policy };
        assert!((capped.frame_bytes(hd, 60.0, 1.0) - 12_500.0).abs() < 1e-6, "6 Mbit/s at 60 Hz");
        assert!(capped.frame_bytes(hd, 60.0, 0.3) < capped.frame_bytes(hd, 60.0, 0.6));
        assert!(VideoPolicy { min_resolution_scale: 0.0, ..policy }.validate().is_err());
    }

    #[test]
    fn resize_averages_quadrants() {
        let small = resize_plane::<3>(quadrants(320, 240).data(), (320, 240), (2, 2));
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

    /// The default encoder's frame for `image` at `bitrate` bits/s, 10 Hz.
    fn encode(encoder: &mut H264Encoder, image: &VideoImage, bitrate: f64, keyframe: bool) -> EncodedVideo {
        let frame = DecodedFrame::Video(image.clone());
        encoder.encode(&frame, &target(&VideoPolicy::default(), (image.width(), image.height()), 1.0, bitrate, 10.0, 1.0, keyframe)).unwrap().unwrap()
    }

    #[test]
    fn encodes_keyframe_then_smaller_frames() {
        let mut encoder = H264Encoder::default();
        assert_eq!(encoder.format(), VideoFormat::H264);
        let frame = quadrants(320, 240);
        let first = encode(&mut encoder, &frame, 1e6, false);
        assert!(first.keyframe && first.data.starts_with(&[0, 0, 0, 1]));
        assert_eq!((first.width, first.height), (320, 240));
        assert!(!encode(&mut encoder, &frame, 1e6, false).keyframe);
        assert!(!encode(&mut encoder, &frame, 2e6, false).keyframe, "a new bitrate applies in place");
        assert!(encode(&mut encoder, &frame, 2e6, true).keyframe, "asked for");
        let low = encode(&mut encoder, &frame, 1e3, false);
        assert_eq!((low.width, low.height), (162, 122), "10 kbit/s (the floor) at 10 Hz keeps 0.05 bits per pixel at about half the width");
        assert!(encoder.encode(&DecodedFrame::data(1u8), &target(&VideoPolicy::default(), (8, 8), 1.0, 1e6, 10.0, 1.0, false)).is_err(), "takes pictures only");
    }

    #[test]
    fn encodes_i420() {
        let (width, height) = (64u32, 48u32);
        let mut data = vec![200u8; (width * height) as usize];
        data.extend(vec![90u8; (width * height / 2) as usize]);
        let image = VideoImage::i420(width, height, data).unwrap();
        let frame = encode(&mut H264Encoder::default(), &image, 1e6, false);
        assert!(frame.keyframe && frame.data.starts_with(&[0, 0, 0, 1]));
        assert_eq!((frame.width, frame.height), (width, height));
        let small = quadrants(64, 48).to_i420(32, 24).unwrap();
        assert_eq!((small.width(), small.height(), small.format(), small.data()[0]), (32, 24, PixelFormat::I420, 82), "red's luma");
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
        let decode = || {
            use zune_jpeg::zune_core::{bytestream::ZCursor, colorspace::ColorSpace, options::DecoderOptions};
            let mut decoder = zune_jpeg::JpegDecoder::new_with_options(ZCursor::new(&jpeg), DecoderOptions::default().jpeg_set_out_colorspace(ColorSpace::RGB));
            let pixels = decoder.decode().unwrap();
            let (width, height) = decoder.dimensions().unwrap();
            VideoImage::rgb8(width as u32, height as u32, pixels).unwrap()
        };
        time("jpeg -> rgb (zune)", 10, Box::new(|| drop(decode())));
        let image = decode();
        println!("source {}x{} {:?}", image.width(), image.height(), image.format());
        // the same picture one pixel over, so every P-frame has real motion to code
        let shifted: Vec<u8> = image.data()[1..].iter().chain(&image.data()[..1]).copied().collect();
        let shifted = VideoImage { data: shifted, ..image.clone() };
        for bitrate in [8e6, 2e6, 5e5] {
            let (width, height) = VideoPolicy::default().size((image.width(), image.height()), bitrate, 10.0, 1.0);
            time(&format!("to_i420 {width}x{height}"), 10, Box::new(|| drop(to_i420(&image, width, height))));
            let mut encoder = H264Encoder::default();
            let mut flip = false;
            time(&format!("encode (to_i420 + h264) {bitrate} bit/s"), 20, Box::new(|| {
                flip = !flip;
                drop(encode(&mut encoder, if flip { &image } else { &shifted }, bitrate, false))
            }));
        }
    }
}
