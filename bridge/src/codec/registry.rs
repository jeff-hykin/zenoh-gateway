//! The server's codecs by name, and the caches that share decodes and encodes across frontends.

use super::{Codec, CodecOutput, CodecSample, Compress, DecodedFrame, H264Encoder, VideoEncoder, VideoPolicy};
use anyhow::{Result, ensure};
use std::collections::{BTreeMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, OnceLock};

/// Encoded results kept for sharing (they are small).
const ENCODED_CAPACITY: usize = 64;
/// Decoded frames kept for sharing; they are large (6 MB for a 1080p picture), so few.
const DECODED_CAPACITY: usize = 8;

type Shared<T> = Arc<OnceLock<Result<Arc<T>, String>>>;

/// A tiny FIFO of in-flight or finished results; callers for the same key block on one `OnceLock`, so it computes once.
struct WorkCache<K, T> {
    capacity: usize,
    entries: Mutex<VecDeque<(K, Shared<T>)>>,
}

impl<K: PartialEq, T> WorkCache<K, T> {
    fn new(capacity: usize) -> Self {
        WorkCache { capacity, entries: Mutex::new(VecDeque::new()) }
    }

    /// Runs `compute` unless someone already did (or is doing) it; `true` = shared result.
    fn get_or_compute(&self, key: K, compute: impl FnOnce() -> Result<T, String>) -> (Result<Arc<T>, String>, bool) {
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
            compute().map(Arc::new)
        });
        (result.clone(), shared && !computed_here)
    }
}

/// Identity of a sample, for sharing work across frontends.
pub fn sample_hash(sample: &CodecSample<'_>) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sample.key.hash(&mut hasher);
    sample.payload.hash(&mut hasher);
    hasher.finish()
}

/// Quality as a cache-key bucket (1/1000 steps; the allocator picks from 0.1 steps and the bounds).
pub fn quality_bucket(quality: f64) -> u16 {
    (quality.clamp(0.0, 1.0) * 1000.0).round() as u16
}

/// Makes the server's video encoders (e.g. hardware ones), for codecs without their own.
pub type VideoEncoderFactory = Arc<dyn Fn() -> Box<dyn VideoEncoder> + Send + Sync>;

/// Name → codec, plus the decode/encode caches and the video encode sessions.
pub struct CodecRegistry {
    codecs: BTreeMap<String, Arc<dyn Codec>>,
    decoded: WorkCache<(String, u64), DecodedFrame>,
    encoded: WorkCache<(String, u16, u64, Compress), Encoded>,
    video_encoder: Option<VideoEncoderFactory>,
    /// the server's default; subscriptions override parts of it
    pub video_policy: VideoPolicy,
    pub video_sessions: crate::media::VideoSessions,
}

/// A data codec's output as sent: `compressed` when `bytes` are its zstd.
pub struct Encoded {
    pub bytes: Vec<u8>,
    pub compressed: bool,
}

impl CodecRegistry {
    /// `codecs` by name; a name registered twice is an error.
    pub fn new(registered: impl IntoIterator<Item = Arc<dyn Codec>>) -> Result<Self> {
        let mut codecs: BTreeMap<String, Arc<dyn Codec>> = BTreeMap::new();
        for codec in registered {
            let name = codec.name().to_owned();
            ensure!(!name.is_empty(), "a codec's name must not be empty");
            ensure!(codecs.insert(name.clone(), codec).is_none(), "codec {name:?} is registered twice");
        }
        Ok(CodecRegistry {
            codecs,
            decoded: WorkCache::new(DECODED_CAPACITY),
            encoded: WorkCache::new(ENCODED_CAPACITY),
            video_encoder: None,
            video_policy: VideoPolicy::default(),
            video_sessions: Default::default(),
        })
    }

    /// The server's video encoder (`None`: software H.264) and default policy.
    pub fn with_video(self, video_encoder: Option<VideoEncoderFactory>, video_policy: VideoPolicy) -> Self {
        CodecRegistry { video_encoder, video_policy, ..self }
    }

    /// A new encoder for a video codec: its own, else the server's, else software H.264.
    pub fn video_encoder(&self, codec: &dyn Codec) -> Box<dyn VideoEncoder> {
        codec.video_encoder().or_else(|| self.video_encoder.as_ref().map(|factory| factory())).unwrap_or_else(|| Box::new(H264Encoder::default()))
    }

    /// The codec called `name`, or the error a subscription is rejected with.
    pub fn get(&self, name: &str) -> Result<Arc<dyn Codec>, String> {
        self.codecs.get(name).cloned().ok_or_else(|| format!("unknown codec {name:?} (known: {})", self.codecs.keys().cloned().collect::<Vec<_>>().join(", ")))
    }

    /// Every codec's (name, output), sorted by name.
    pub fn list(&self) -> impl Iterator<Item = (&str, CodecOutput)> {
        self.codecs.iter().map(|(name, codec)| (name.as_str(), codec.output()))
    }

    /// Blocking: decodes `sample` (or reuses another frontend's decode of it). `true` = reused.
    pub fn decode_shared(&self, codec: &dyn Codec, sample: &CodecSample<'_>, hash: u64) -> (Result<Arc<DecodedFrame>, String>, bool) {
        self.decoded.get_or_compute((codec.name().to_owned(), hash), || codec.decode(sample).map_err(|error| format!("{error:#}")))
    }

    /// Blocking: a data codec's bytes for `sample` at `quality`, compressed as asked (or another frontend's). `true` = reused.
    pub fn encode_shared(&self, codec: &dyn Codec, sample: &CodecSample<'_>, hash: u64, quality: f64, compress: Compress) -> (Result<Arc<Encoded>, String>, bool) {
        self.encoded.get_or_compute((codec.name().to_owned(), quality_bucket(quality), hash, compress), || {
            let (frame, _) = self.decode_shared(codec, sample, hash);
            let bytes = codec.encode(&*frame?, quality).map_err(|error| format!("{error:#}"))?;
            let compressed = compress.apply(&bytes);
            Ok(Encoded { compressed: compressed.is_some(), bytes: compressed.unwrap_or(bytes) })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Named(&'static str);

    impl Codec for Named {
        fn name(&self) -> &str {
            self.0
        }

        fn output(&self) -> CodecOutput {
            CodecOutput::Data
        }

        fn decode(&self, sample: &CodecSample<'_>) -> Result<DecodedFrame> {
            Ok(DecodedFrame::data(sample.payload.to_vec()))
        }
    }

    #[test]
    fn registered_unknown_and_duplicate_fail() {
        let registry = CodecRegistry::new([Arc::new(Named("custom")) as Arc<dyn Codec>, Arc::new(Named("other"))]).unwrap();
        for name in ["other", "custom"] {
            assert_eq!(registry.get(name).unwrap().name(), name);
        }
        let error = registry.get("missing").err().unwrap();
        assert!(error.contains("unknown codec") && error.contains("other") && error.contains("custom"), "{error}");
        let duplicate = CodecRegistry::new([Arc::new(Named("custom")) as Arc<dyn Codec>, Arc::new(Named("custom"))]).err().unwrap();
        assert!(duplicate.to_string().contains("registered twice"), "{duplicate}");
        assert!(CodecRegistry::new([Arc::new(Named("")) as Arc<dyn Codec>]).is_err());
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

    #[test]
    fn default_encode_errors_and_encodes_share_decodes() {
        let registry = CodecRegistry::new([Arc::new(Named("custom")) as Arc<dyn Codec>]).unwrap();
        let codec = registry.get("custom").unwrap();
        let encoding = zenoh::bytes::Encoding::default();
        let sample = CodecSample::new("a/b", b"xyz", &encoding);
        let hash = sample_hash(&sample);
        let (encoded, _) = registry.encode_shared(&*codec, &sample, hash, 1.0, Compress::Zstd);
        assert!(encoded.err().unwrap().contains("no data-channel encoder"));
        let (_, reused) = registry.decode_shared(&*codec, &sample, hash);
        assert!(reused, "the encode attempt's decode is cached");
    }

    struct Repeat;

    impl Codec for Repeat {
        fn name(&self) -> &str {
            "repeat"
        }

        fn output(&self) -> CodecOutput {
            CodecOutput::Data
        }

        fn decode(&self, sample: &CodecSample<'_>) -> Result<DecodedFrame> {
            Ok(DecodedFrame::data(sample.payload.repeat(1000)))
        }

        fn encode(&self, frame: &DecodedFrame, _: f64) -> Result<Vec<u8>> {
            Ok(frame.downcast::<Vec<u8>>()?.clone())
        }
    }

    #[test]
    fn encodes_compress_and_share_per_compression() {
        let registry = CodecRegistry::new([Arc::new(Repeat) as Arc<dyn Codec>]).unwrap();
        let encoding = zenoh::bytes::Encoding::default();
        let sample = CodecSample::new("a/b", b"xyz", &encoding);
        let hash = sample_hash(&sample);
        let (plain, _) = registry.encode_shared(&Repeat, &sample, hash, 1.0, Compress::None);
        let (zstd, reused) = registry.encode_shared(&Repeat, &sample, hash, 1.0, Compress::Zstd);
        let (plain, zstd) = (plain.unwrap(), zstd.unwrap());
        assert!(!reused && !plain.compressed && zstd.compressed && zstd.bytes.len() < plain.bytes.len() / 10);
        assert_eq!(zstd::bulk::decompress(&zstd.bytes, 3000).unwrap(), plain.bytes);
        assert!(registry.encode_shared(&Repeat, &sample, hash, 1.0, Compress::Zstd).1, "same compression is shared");
        let tiny = CodecSample::new("a/b", b"", &encoding);
        let (tiny, _) = registry.encode_shared(&Repeat, &tiny, sample_hash(&tiny), 1.0, Compress::Zstd);
        assert!(!tiny.unwrap().compressed, "a message zstd would grow is sent as is");
    }
}
