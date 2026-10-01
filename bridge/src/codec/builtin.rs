//! The built-in codecs (`ros2-*` and `dimos-*`), implemented through the public [`Codec`] trait
//! like any external codec.
//!
//! The input type decides the output:
//! - `*-image`, `*-compressed-image`: color/mono images, H.264 on a WebRTC video track
//! - `*-depth`, `*-compressed-depth`: lossless depth (u16/f32) on the data channel, zstd
//! - `*-pointcloud2`: voxel + int16 quantized points on the data channel, zstd

use super::wire::{self, Protocol};
use super::{Codec, CodecOutput, CodecSample, DecodedFrame, depth, image, pointcloud};
use anyhow::Result;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Input {
    Image,
    CompressedImage,
    Depth,
    CompressedDepth,
    PointCloud2,
}

/// One `<protocol>-<input>` codec.
struct Builtin {
    name: &'static str,
    protocol: Protocol,
    input: Input,
}

const BUILTINS: [(&str, Protocol, Input); 10] = [
    ("ros2-image", Protocol::Ros2, Input::Image),
    ("ros2-compressed-image", Protocol::Ros2, Input::CompressedImage),
    ("ros2-depth", Protocol::Ros2, Input::Depth),
    ("ros2-compressed-depth", Protocol::Ros2, Input::CompressedDepth),
    ("ros2-pointcloud2", Protocol::Ros2, Input::PointCloud2),
    ("dimos-image", Protocol::Dimos, Input::Image),
    ("dimos-compressed-image", Protocol::Dimos, Input::CompressedImage),
    ("dimos-depth", Protocol::Dimos, Input::Depth),
    ("dimos-compressed-depth", Protocol::Dimos, Input::CompressedDepth),
    ("dimos-pointcloud2", Protocol::Dimos, Input::PointCloud2),
];

/// Every built-in codec.
pub fn all() -> Vec<Arc<dyn Codec>> {
    BUILTINS.iter().map(|&(name, protocol, input)| Arc::new(Builtin { name, protocol, input }) as Arc<dyn Codec>).collect()
}

impl Codec for Builtin {
    fn name(&self) -> &str {
        self.name
    }

    fn output(&self) -> CodecOutput {
        match self.input {
            Input::Image | Input::CompressedImage => CodecOutput::Video,
            Input::Depth | Input::CompressedDepth | Input::PointCloud2 => CodecOutput::Data,
        }
    }

    fn decode(&self, sample: &CodecSample<'_>) -> Result<DecodedFrame> {
        let payload = sample.payload;
        Ok(match self.input {
            Input::Image => DecodedFrame::Video(image::raw_to_rgb(&wire::parse_image(self.protocol, payload)?)?.into()),
            Input::CompressedImage => {
                let message = wire::parse_compressed_image(self.protocol, payload)?;
                DecodedFrame::Video(image::compressed_to_rgb(message.data, &message.format)?.into())
            }
            Input::Depth => DecodedFrame::data(image::raw_to_depth(&wire::parse_image(self.protocol, payload)?)?),
            Input::CompressedDepth => {
                let message = wire::parse_compressed_image(self.protocol, payload)?;
                DecodedFrame::data(image::compressed_to_depth(message.data, &message.format)?)
            }
            Input::PointCloud2 => DecodedFrame::data(pointcloud::read_points(&wire::parse_point_cloud(self.protocol, payload)?)?),
        })
    }

    fn encode(&self, frame: &DecodedFrame, quality: f64) -> Result<Vec<u8>> {
        match self.input {
            Input::Depth | Input::CompressedDepth => depth::encode(frame.downcast::<image::Depth>()?, quality),
            Input::PointCloud2 => pointcloud::encode_points(frame.downcast::<pointcloud::Points>()?, quality),
            Input::Image | Input::CompressedImage => anyhow::bail!("{} is a video codec", self.name),
        }
    }

    fn estimated_bytes(&self, payload_bytes: usize, quality: f64) -> f64 {
        let payload_bytes = payload_bytes as f64;
        match self.input {
            // lossless zstd roughly halves depth; lower quality sends 1/stride² of the pixels
            Input::Depth | Input::CompressedDepth => payload_bytes * 0.5 * depth::size_factor(quality),
            // voxel thinning depends on point density; a rough monotone guess until measured
            Input::PointCloud2 => payload_bytes * 0.25 * (0.15 + 0.85 * quality.clamp(0.0, 1.0)),
            // priced by the bridge's video model, never asked
            Input::Image | Input::CompressedImage => payload_bytes * 0.05,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::wire::tests::fixture;

    fn decode_and_encode(name: &str, file: &str, quality: f64) -> Vec<u8> {
        let codec = all().into_iter().find(|codec| codec.name() == name).unwrap();
        let payload = fixture(file);
        let encoding = zenoh::bytes::Encoding::default();
        let frame = codec.decode(&CodecSample::new("k", &payload, &encoding)).unwrap();
        codec.encode(&frame, quality).unwrap()
    }

    #[test]
    fn outputs_and_round_trips() {
        let names: Vec<_> = all().iter().map(|codec| (codec.name().to_owned(), codec.output())).collect();
        assert_eq!(names.len(), 10);
        assert!(names.contains(&("ros2-image".to_owned(), CodecOutput::Video)));
        assert!(names.contains(&("dimos-depth".to_owned(), CodecOutput::Data)));
        let depth = decode_and_encode("ros2-depth", "ros2/depth_16UC1.cdr", 1.0);
        assert_eq!(&depth[..2], &[1, 1], "depth header: version 1, 16UC1");
        let half = decode_and_encode("dimos-depth", "dimos/depth_16UC1.bin", 0.5);
        assert_eq!(u16::from_le_bytes([half[2], half[3]]), 2, "stride 2 at quality 0.5");
        let cloud = decode_and_encode("dimos-pointcloud2", "dimos/pointcloud_xyzi.bin", 1.0);
        assert_eq!(u32::from_le_bytes(cloud[4..8].try_into().unwrap()), 20000);
        let codec = all().into_iter().find(|codec| codec.name() == "dimos-image").unwrap();
        let payload = fixture("dimos/image_rgb8.bin");
        let encoding = zenoh::bytes::Encoding::default();
        let DecodedFrame::Video(image) = codec.decode(&CodecSample::new("k", &payload, &encoding)).unwrap() else { panic!("video expected") };
        assert_eq!((image.width(), image.height()), (320, 240));
    }
}
