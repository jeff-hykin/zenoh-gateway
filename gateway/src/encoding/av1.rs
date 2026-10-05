//! The built-in AV1 encoder (feature `av1`): rav1e at its fastest preset in low-latency mode, for channel `video-av1`.
//! rav1e still looks 3 frames ahead (its scene-change lookahead, fixed even in low-latency mode), so each frame comes
//! out 3 frames late (100 ms at 30 Hz); a hardware AV1 encoder ([`ServerBuilder::video_encoder`](crate::ServerBuilder::video_encoder))
//! has none of that.

use super::video::{EncodedVideo, VideoEncoder, VideoFormat, VideoTarget};
use super::DecodedFrame;
use anyhow::{Result, bail};

/// Keyframe at least this often (frames), on top of PLI/FIR requests from the browser.
const KEYFRAME_FRAMES: u64 = 90;
/// Re-create the encoder when the bitrate or frame rate drifts past this ratio (rav1e can't change them in place).
const RECONFIGURE_RATIO: f64 = 1.3;

#[derive(Clone, Copy, PartialEq)]
struct Settings {
    width: u32,
    height: u32,
    bitrate_bps: u32,
    fps: f64,
}

/// [`DecodedFrame::Video`] to AV1 with rav1e (speed 10, low latency, tiles across the cores), re-created when the
/// target's size moves or its bitrate or rate drift.
#[derive(Default)]
pub struct Av1Encoder {
    context: Option<(rav1e::Context<u8>, Settings)>,
}

impl VideoEncoder for Av1Encoder {
    fn format(&self) -> VideoFormat {
        VideoFormat::Av1
    }

    fn encode(&mut self, frame: &DecodedFrame, target: &VideoTarget) -> Result<Option<EncodedVideo>> {
        let DecodedFrame::Video(image) = frame else { bail!("the AV1 encoder takes pictures, got {frame:?}") };
        let wanted = Settings { width: target.width, height: target.height, bitrate_bps: target.bitrate_bps, fps: target.fps };
        let drifted = |old: f64, new: f64| old / new > RECONFIGURE_RATIO || new / old > RECONFIGURE_RATIO;
        let reconfigure = self.context.as_ref().is_none_or(|(_, current)| {
            (current.width, current.height) != (wanted.width, wanted.height) || drifted(current.bitrate_bps as f64, wanted.bitrate_bps as f64) || drifted(current.fps, wanted.fps)
        });
        if reconfigure {
            let mut config = rav1e::config::EncoderConfig::with_speed_preset(10);
            (config.width, config.height, config.bitrate) = (wanted.width as usize, wanted.height as usize, wanted.bitrate_bps.min(i32::MAX as u32) as i32);
            config.time_base = rav1e::data::Rational::new(1, wanted.fps.round().max(1.0) as u64);
            (config.low_latency, config.max_key_frame_interval, config.speed_settings.rdo_lookahead_frames) = (true, KEYFRAME_FRAMES, 1);
            config.speed_settings.scene_detection_mode = rav1e::prelude::SceneDetectionSpeed::None;
            let threads = std::thread::available_parallelism().map_or(1, |cores| cores.get()).min(8);
            config.tiles = threads;
            let context = rav1e::Config::new().with_encoder_config(config).with_threads(threads).new_context().map_err(|error| anyhow::anyhow!("rav1e: {error}"))?;
            self.context = Some((context, wanted));
        }
        let (context, _) = self.context.as_mut().expect("created above");
        let (width, height) = (wanted.width, wanted.height);
        let picture = image.to_i420(width, height)?;
        let (luma, chroma) = picture.data().split_at((width * height) as usize);
        let (u, v) = chroma.split_at(chroma.len() / 2);
        let mut input = context.new_frame();
        for (plane, (data, stride)) in input.planes.iter_mut().zip([(luma, width), (u, width / 2), (v, width / 2)]) {
            plane.copy_from_raw_u8(data, stride as usize, 1);
        }
        // a new encoder starts with a keyframe anyway
        let keyframe = (target.keyframe && !reconfigure).then(|| rav1e::prelude::FrameParameters { frame_type_override: rav1e::prelude::FrameTypeOverride::Key, ..Default::default() });
        context.send_frame((input, keyframe)).map_err(|error| anyhow::anyhow!("rav1e: {error}"))?;
        loop {
            match context.receive_packet() {
                Ok(packet) => return Ok(Some(EncodedVideo { data: packet.data, width, height, keyframe: packet.frame_type == rav1e::prelude::FrameType::KEY })),
                Err(rav1e::EncoderStatus::Encoded) => continue,
                Err(rav1e::EncoderStatus::NeedMoreData) => return Ok(None),
                Err(error) => bail!("rav1e: {error}"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::VideoImage;

    #[test]
    fn encodes_a_keyframe_then_smaller_frames() {
        let (width, height) = (320u32, 240u32);
        let pixels: Vec<u8> = (0..width * height * 3).map(|index| (index % 251) as u8).collect();
        let frame = DecodedFrame::Video(VideoImage::rgb8(width, height, pixels).unwrap());
        let mut encoder = Av1Encoder::default();
        let target = VideoTarget::new(width, height, 500_000, 30.0);
        let outputs: Vec<Option<EncodedVideo>> = (0..12).map(|_| encoder.encode(&frame, &target).unwrap()).collect();
        let delay = outputs.iter().position(Option::is_some).expect("some output in 12 frames");
        assert!(delay <= 4, "rav1e's lookahead holds {delay} frames");
        let frames: Vec<EncodedVideo> = outputs.into_iter().flatten().collect();
        assert!(frames[0].keyframe, "the first is a keyframe");
        assert_eq!((frames[0].width, frames[0].height), (width, height));
        assert!(frames.iter().skip(1).all(|frame| !frame.keyframe && frame.data.len() < frames[0].data.len()));
        assert!(encoder.encode(&DecodedFrame::data(1u8), &target).is_err(), "takes pictures only");
    }
}
