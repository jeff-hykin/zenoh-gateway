//! Hardcoded transcoders, picked explicitly by the subscribe option `codec`.
//!
//! A codec name is `<protocol>-<input>`; the input type decides the output path:
//! - `*-image`, `*-compressed-image`: color/mono images, H.264 on a WebRTC video track
//! - `*-depth`, `*-compressed-depth`: lossless depth (u16/f32) on the data channel, zstd
//! - `*-pointcloud2`: voxel + int16 quantized points on the data channel, zstd
//!
//! Encoding is lazy (only frames the sender will send) and off the async runtime. Data-channel
//! encodes are shared across frontends through a small cache keyed by (codec, quality, payload).

pub mod depth;
pub mod image;
pub mod pointcloud;
pub mod video;
pub mod wire;

use anyhow::Result;
use std::collections::VecDeque;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use wire::Protocol;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Input {
    Image,
    CompressedImage,
    Depth,
    CompressedDepth,
    PointCloud2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Output {
    Video,
    Depth,
    PointCloud,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Codec {
    pub protocol: Protocol,
    pub input: Input,
}

const CODECS: [(&str, Protocol, Input); 10] = [
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

impl Codec {
    pub fn parse(name: &str) -> Result<Codec, String> {
        CODECS
            .iter()
            .find(|(codec_name, _, _)| *codec_name == name)
            .map(|&(_, protocol, input)| Codec { protocol, input })
            .ok_or_else(|| format!("unknown codec {name:?} (known: {})", CODECS.map(|(codec_name, _, _)| codec_name).join(", ")))
    }

    pub fn name(&self) -> &'static str {
        CODECS.iter().find(|(_, protocol, input)| (*protocol, *input) == (self.protocol, self.input)).map(|(name, _, _)| *name).unwrap_or("?")
    }

    pub fn output(&self) -> Output {
        match self.input {
            Input::Image | Input::CompressedImage => Output::Video,
            Input::Depth | Input::CompressedDepth => Output::Depth,
            Input::PointCloud2 => Output::PointCloud,
        }
    }

    /// Encoded size at `quality` relative to quality 1, before anything was measured.
    pub fn size_prior(&self, quality: f64) -> f64 {
        match self.output() {
            Output::Depth => depth::size_factor(quality),
            // voxel thinning depends on point density; a rough monotone guess until measured
            Output::PointCloud => 0.15 + 0.85 * quality.clamp(0.0, 1.0),
            Output::Video => {
                let full = video::bytes_per_frame(640, 480, 1.0);
                video::bytes_per_frame(640, 480, quality) / full
            }
        }
    }

    /// Encoded size at quality 1 relative to the raw payload, before anything was measured.
    pub fn compression_prior(&self) -> f64 {
        match self.output() {
            Output::Depth => 0.5,
            Output::PointCloud => 0.25,
            Output::Video => 0.05,
        }
    }

    /// Payload to data-channel bytes (depth and point cloud codecs).
    pub fn encode(&self, payload: &[u8], quality: f64) -> Result<Vec<u8>> {
        match self.input {
            Input::Depth => depth::encode(&image::raw_to_depth(&wire::parse_image(self.protocol, payload)?)?, quality),
            Input::CompressedDepth => {
                let message = wire::parse_compressed_image(self.protocol, payload)?;
                depth::encode(&image::compressed_to_depth(message.data, &message.format)?, quality)
            }
            Input::PointCloud2 => pointcloud::encode(&wire::parse_point_cloud(self.protocol, payload)?, quality),
            Input::Image | Input::CompressedImage => anyhow::bail!("{} is a video codec", self.name()),
        }
    }

    /// Payload to RGB8 (video codecs).
    pub fn decode_rgb(&self, payload: &[u8]) -> Result<image::Rgb8> {
        match self.input {
            Input::Image => image::raw_to_rgb(&wire::parse_image(self.protocol, payload)?),
            Input::CompressedImage => {
                let message = wire::parse_compressed_image(self.protocol, payload)?;
                image::compressed_to_rgb(message.data, &message.format)
            }
            _ => anyhow::bail!("{} is not a video codec", self.name()),
        }
    }
}

/// Identity of a sample's payload, for sharing work across frontends.
pub fn payload_hash(key: &str, payload: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    payload.hash(&mut hasher);
    hasher.finish()
}

/// Quality as a cache-key bucket (1/1000 steps; the allocator picks from 0.1 steps and the bounds).
pub fn quality_bucket(quality: f64) -> u16 {
    (quality.clamp(0.0, 1.0) * 1000.0).round() as u16
}

type Shared<T> = Arc<OnceLock<Result<Arc<T>, String>>>;

/// A tiny FIFO of in-flight or finished results. Concurrent callers for the same key block on
/// one `OnceLock`, so each (codec, quality, payload) is computed once while it stays cached.
struct WorkCache<K, T> {
    capacity: usize,
    entries: Mutex<VecDeque<(K, Shared<T>)>>,
}

impl<K: PartialEq + Clone, T> WorkCache<K, T> {
    const fn new(capacity: usize) -> Self {
        WorkCache { capacity, entries: Mutex::new(VecDeque::new()) }
    }

    /// Runs `compute` unless someone already did (or is doing) it; `true` = shared result.
    fn get_or_compute(&self, key: K, compute: impl FnOnce() -> Result<T>) -> (Result<Arc<T>, String>, bool) {
        let (cell, shared) = {
            let mut entries = self.entries.lock().unwrap();
            match entries.iter().find(|(existing, _)| *existing == key) {
                Some((_, cell)) => (cell.clone(), true),
                None => {
                    let cell: Shared<T> = Arc::default();
                    entries.push_back((key, cell.clone()));
                    while entries.len() > self.capacity {
                        entries.pop_front();
                    }
                    (cell, false)
                }
            }
        };
        let mut computed_here = false;
        let result = cell.get_or_init(|| {
            computed_here = true;
            compute().map(Arc::new).map_err(|error| format!("{error:#}"))
        });
        (result.clone(), shared && !computed_here)
    }
}

static ENCODED: LazyLock<WorkCache<(Codec, u16, u64), Vec<u8>>> = LazyLock::new(|| WorkCache::new(64));
// decoded frames are large (6 MB at 1080p), keep few
static DECODED: LazyLock<WorkCache<(Codec, u64), image::Rgb8>> = LazyLock::new(|| WorkCache::new(8));

/// Blocking: encodes (or reuses another frontend's encode of) a data-channel codec payload.
pub fn encode_shared(codec: Codec, quality: f64, hash: u64, payload: &[u8]) -> (Result<Arc<Vec<u8>>, String>, bool) {
    ENCODED.get_or_compute((codec, quality_bucket(quality), hash), || codec.encode(payload, quality))
}

/// Blocking: decodes (or reuses another frontend's decode of) a video codec payload.
pub fn decode_shared(codec: Codec, hash: u64, payload: &[u8]) -> (Result<Arc<image::Rgb8>, String>, bool) {
    DECODED.get_or_compute((codec, hash), || codec.decode_rgb(payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip_and_unknown_fails() {
        for (name, _, _) in CODECS {
            assert_eq!(Codec::parse(name).unwrap().name(), name);
        }
        let error = Codec::parse("ros2-jpeg").unwrap_err();
        assert!(error.contains("unknown codec") && error.contains("dimos-pointcloud2"), "{error}");
    }

    #[test]
    fn cache_computes_once() {
        let cache: WorkCache<u32, u32> = WorkCache::new(2);
        let (first, shared) = cache.get_or_compute(1, || Ok(10));
        assert_eq!((*first.unwrap(), shared), (10, false));
        let (again, shared) = cache.get_or_compute(1, || panic!("must not recompute"));
        assert_eq!((*again.unwrap(), shared), (10, true));
        let _ = cache.get_or_compute(2, || Ok(20));
        let _ = cache.get_or_compute(3, || Ok(30));
        let (evicted, shared) = cache.get_or_compute(1, || Ok(11));
        assert_eq!((*evicted.unwrap(), shared), (11, false), "oldest entry evicted at capacity");
    }
}
