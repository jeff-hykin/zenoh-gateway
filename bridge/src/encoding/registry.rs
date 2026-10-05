//! The server's message encodings by name, its video encoders by format, and the caches that share decodes and
//! encodes across frontends.

use super::{Channel, Compress, DecodedFrame, EncodeOptions, EncodingOutput, EncodingSample, H264Encoder, MessageEncoding, VideoEncoder, VideoFormat, VideoPolicy};
use anyhow::{Result, ensure};
use std::collections::{BTreeMap, HashMap, VecDeque};
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
pub fn sample_hash(sample: &EncodingSample<'_>) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sample.key.hash(&mut hasher);
    sample.payload.hash(&mut hasher);
    hasher.finish()
}

/// Quality as a cache-key bucket (1/1000 steps; the allocator picks from 0.1 steps and the bounds).
pub fn quality_bucket(quality: f64) -> u16 {
    (quality.clamp(0.0, 1.0) * 1000.0).round() as u16
}

/// Makes the server's video encoders of one format (e.g. hardware ones), for encodings without their own.
pub type VideoEncoderFactory = Arc<dyn Fn() -> Box<dyn VideoEncoder> + Send + Sync>;

/// Name → encoding, the video encoders by format, plus the decode/encode caches and the video encode sessions.
pub struct EncodingRegistry {
    codecs: BTreeMap<String, Arc<dyn MessageEncoding>>,
    decoded: WorkCache<(String, EncodingOutput, u64), DecodedFrame>,
    encoded: WorkCache<(String, u16, String, u64, Compress), Encoded>,
    video_encoders: HashMap<VideoFormat, VideoEncoderFactory>,
    /// the server's default; subscriptions override parts of it
    pub video_policy: VideoPolicy,
    pub video_sessions: crate::media::VideoSessions,
}

/// A subscription's encoding (None = raw bytes), its channel, and what it sends there.
#[derive(Clone)]
pub struct Resolved {
    pub encoding: Option<Arc<dyn MessageEncoding>>,
    pub channel: Channel,
    pub output: EncodingOutput,
}

/// A data encoding's output as sent: `compressed` when `bytes` are its zstd.
pub struct Encoded {
    pub bytes: Vec<u8>,
    pub compressed: bool,
}

/// The built-in software encoder of `format`, if this build has one.
fn builtin_video_encoder(format: VideoFormat) -> Option<Box<dyn VideoEncoder>> {
    match format {
        VideoFormat::H264 => Some(Box::new(H264Encoder::default())),
        #[cfg(feature = "av1")]
        VideoFormat::Av1 => Some(Box::new(super::av1::Av1Encoder::default())),
        _ => None,
    }
}

impl EncodingRegistry {
    /// `encodings` by name; a name registered twice is an error.
    pub fn new(registered: impl IntoIterator<Item = Arc<dyn MessageEncoding>>) -> Result<Self> {
        let mut codecs: BTreeMap<String, Arc<dyn MessageEncoding>> = BTreeMap::new();
        for codec in registered {
            let name = codec.name().to_owned();
            ensure!(!name.is_empty(), "an encoding's name must not be empty");
            ensure!(codecs.insert(name.clone(), codec).is_none(), "encoding {name:?} is registered twice");
        }
        Ok(EncodingRegistry {
            codecs,
            decoded: WorkCache::new(DECODED_CAPACITY),
            encoded: WorkCache::new(ENCODED_CAPACITY),
            video_encoders: HashMap::new(),
            video_policy: VideoPolicy::default(),
            video_sessions: Default::default(),
        })
    }

    /// The server's video encoders (by the format each makes) and default policy.
    pub fn with_video(self, video_encoders: impl IntoIterator<Item = (VideoFormat, VideoEncoderFactory)>, video_policy: VideoPolicy) -> Self {
        EncodingRegistry { video_encoders: video_encoders.into_iter().collect(), video_policy, ..self }
    }

    /// A new encoder of `format` for a video encoding: its own, else the server's, else the built-in one, else why none.
    pub fn video_encoder(&self, codec: &dyn MessageEncoding, format: VideoFormat) -> Result<Box<dyn VideoEncoder>, String> {
        codec
            .video_encoder(format)
            .or_else(|| self.video_encoders.get(&format).map(|factory| factory()))
            .or_else(|| builtin_video_encoder(format))
            .ok_or_else(|| format!("this server has no {} encoder", Channel::Video(format).as_str()))
    }

    /// The encoding called `name`, or the error a subscription is rejected with.
    pub fn get(&self, name: &str) -> Result<Arc<dyn MessageEncoding>, String> {
        self.codecs.get(name).cloned().ok_or_else(|| format!("unknown encoding {name:?} (known: {})", self.codecs.keys().cloned().collect::<Vec<_>>().join(", ")))
    }

    /// Every encoding's (name, output on its default channel), sorted by name.
    pub fn list(&self) -> impl Iterator<Item = (&str, EncodingOutput)> {
        self.codecs.iter().map(|(name, codec)| (name.as_str(), codec.output()))
    }

    /// Blocking: decodes `sample` for `channel` (or reuses another frontend's decode of it). `true` = reused.
    pub fn decode_shared(&self, codec: &dyn MessageEncoding, sample: &EncodingSample<'_>, hash: u64, channel: Channel) -> (Result<Arc<DecodedFrame>, String>, bool) {
        self.decoded.get_or_compute((codec.name().to_owned(), channel.kind(), hash), || codec.decode(sample, channel).map_err(|error| format!("{error:#}")))
    }

    /// Blocking: a data encoding's bytes for `sample` with `options`, compressed as asked (or another frontend's).
    /// `true` = reused.
    pub fn encode_shared(&self, codec: &dyn MessageEncoding, sample: &EncodingSample<'_>, hash: u64, options: &EncodeOptions, compress: Compress) -> (Result<Arc<Encoded>, String>, bool) {
        let key = (codec.name().to_owned(), quality_bucket(options.quality), serde_json::Value::Object(options.options.clone()).to_string(), hash, compress);
        self.encoded.get_or_compute(key, || {
            let (frame, _) = self.decode_shared(codec, sample, hash, Channel::Data);
            let bytes = codec.encode(&*frame?, options).map_err(|error| format!("{error:#}"))?;
            let compressed = compress.apply(&bytes);
            Ok(Encoded { compressed: compressed.is_some(), bytes: compressed.unwrap_or(bytes) })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Named(&'static str);

    impl MessageEncoding for Named {
        fn name(&self) -> &str {
            self.0
        }

        fn output(&self) -> EncodingOutput {
            EncodingOutput::Data
        }

        fn decode(&self, sample: &EncodingSample<'_>, _: Channel) -> Result<DecodedFrame> {
            Ok(DecodedFrame::data(sample.payload.to_vec()))
        }
    }

    #[test]
    fn registered_unknown_and_duplicate_fail() {
        let registry = EncodingRegistry::new([Arc::new(Named("custom")) as Arc<dyn MessageEncoding>, Arc::new(Named("other"))]).unwrap();
        for name in ["other", "custom"] {
            assert_eq!(registry.get(name).unwrap().name(), name);
        }
        let error = registry.get("missing").err().unwrap();
        assert!(error.contains("unknown encoding") && error.contains("other") && error.contains("custom"), "{error}");
        let duplicate = EncodingRegistry::new([Arc::new(Named("custom")) as Arc<dyn MessageEncoding>, Arc::new(Named("custom"))]).err().unwrap();
        assert!(duplicate.to_string().contains("registered twice"), "{duplicate}");
        assert!(EncodingRegistry::new([Arc::new(Named("")) as Arc<dyn MessageEncoding>]).is_err());
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
        let registry = EncodingRegistry::new([Arc::new(Named("custom")) as Arc<dyn MessageEncoding>]).unwrap();
        let codec = registry.get("custom").unwrap();
        let encoding = zenoh::bytes::Encoding::default();
        let sample = EncodingSample::new("a/b", b"xyz", &encoding);
        let hash = sample_hash(&sample);
        let (encoded, _) = registry.encode_shared(&*codec, &sample, hash, &EncodeOptions::quality(1.0), Compress::Zstd);
        assert!(encoded.err().unwrap().contains("no data-channel encoder"));
        let (_, reused) = registry.decode_shared(&*codec, &sample, hash, Channel::Data);
        assert!(reused, "the encode attempt's decode is cached");
    }

    struct Repeat;

    impl MessageEncoding for Repeat {
        fn name(&self) -> &str {
            "repeat"
        }

        fn output(&self) -> EncodingOutput {
            EncodingOutput::Data
        }

        fn decode(&self, sample: &EncodingSample<'_>, _: Channel) -> Result<DecodedFrame> {
            Ok(DecodedFrame::data(sample.payload.repeat(1000)))
        }

        fn encode(&self, frame: &DecodedFrame, _: &EncodeOptions) -> Result<Vec<u8>> {
            Ok(frame.downcast::<Vec<u8>>()?.clone())
        }
    }

    #[test]
    fn encodes_compress_and_share_per_compression() {
        let registry = EncodingRegistry::new([Arc::new(Repeat) as Arc<dyn MessageEncoding>]).unwrap();
        let encoding = zenoh::bytes::Encoding::default();
        let sample = EncodingSample::new("a/b", b"xyz", &encoding);
        let hash = sample_hash(&sample);
        let (plain, _) = registry.encode_shared(&Repeat, &sample, hash, &EncodeOptions::quality(1.0), Compress::None);
        let (zstd, reused) = registry.encode_shared(&Repeat, &sample, hash, &EncodeOptions::quality(1.0), Compress::Zstd);
        let (plain, zstd) = (plain.unwrap(), zstd.unwrap());
        assert!(!reused && !plain.compressed && zstd.compressed && zstd.bytes.len() < plain.bytes.len() / 10);
        assert_eq!(zstd::bulk::decompress(&zstd.bytes, 3000).unwrap(), plain.bytes);
        assert!(registry.encode_shared(&Repeat, &sample, hash, &EncodeOptions::quality(1.0), Compress::Zstd).1, "same compression is shared");
        let tiny = EncodingSample::new("a/b", b"", &encoding);
        let (tiny, _) = registry.encode_shared(&Repeat, &tiny, sample_hash(&tiny), &EncodeOptions::quality(1.0), Compress::Zstd);
        assert!(!tiny.unwrap().compressed, "a message zstd would grow is sent as is");
    }
}
