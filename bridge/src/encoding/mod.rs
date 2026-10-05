//! Message encodings: picked per subscription with the subscribe option `encoding`, plus the `channel` the result
//! travels on and the `encodeOptions` the encoding gets.
//!
//! Every encoding implements [`MessageEncoding`] and lives in the server's registry under its
//! [`name`](MessageEncoding::name). An encoding decodes a zenoh sample into a [`DecodedFrame`] for its [`Channel`]:
//! - `video-h264`, `video-vp8`, `video-vp9`, `video-av1` ([`Channel::Video`]): pictures for a [`VideoEncoder`] of that
//!   format (the encoding's own, the server's, or a built-in one), sent on a WebRTC video track;
//! - `audio-opus` ([`Channel::Audio`]): PCM, which the bridge encodes to Opus on an audio track;
//! - `data` ([`Channel::Data`]): bytes from [`MessageEncoding::encode`] for the subscription's data channel, at the
//!   quality the bandwidth allocator picked (at most `encodeOptions.quality`): [`Fields`](crate::Fields), which the
//!   browser client decodes by itself ([`EncodingOutput::Fields`]), or any other bytes ([`EncodingOutput::Data`]).
//!   Data-channel bytes may be zstd-compressed on the way ([`Compress`], [`MessageEncoding::default_compress`]).
//!
//! Work is lazy (only messages the pacing and queues let through) and runs on tokio's blocking pool. Decodes are shared
//! across frontends by (encoding, channel, key, payload), data encodes by (encoding, options, quality, key, payload), so
//! identical requests from several browsers compute once.

#[cfg(feature = "av1")]
pub(crate) mod av1;
pub(crate) mod registry;
pub(crate) mod video;

pub use video::{EncodedVideo, H264Encoder, VideoEncoder, VideoFormat, VideoPolicy, VideoTarget};

use anyhow::{Result, anyhow, ensure};
use std::any::Any;
use std::fmt;

/// What an encoding produces (on its default channel, or as [`MessageEncoding::output_on`] says).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EncodingOutput {
    /// Frames for a [`VideoEncoder`] (by default [`DecodedFrame::Video`] pictures, which
    /// the bridge scales to the allocated quality and encodes to H.264), sent on a WebRTC video
    /// track (the page gets `sub.mediaStream`). The subscription must use `delivery: "latest"`.
    Video,
    /// [`DecodedFrame::Audio`] PCM, which the bridge encodes to Opus in 20 ms packets and sends on a
    /// WebRTC audio track (the page gets `sub.mediaStream`). Never paced or thinned by the allocator.
    Audio,
    /// Bytes from [`MessageEncoding::encode`], sent on the subscription's data channel. In the browser,
    /// `msg.bytes` holds them and `msg.decoded` what the decoder registered for this encoding's name
    /// (client `registerEncoding(name, decoder)`) returned.
    Data,
    /// A [`Fields`](crate::Fields) message from [`MessageEncoding::encode`], sent on the data channel; the
    /// browser client decodes it into `msg.decoded` (an object of numbers, strings and typed arrays)
    /// with no decoding code in the page.
    Fields,
}

impl EncodingOutput {
    /// `"video"`, `"audio"`, `"data"` or `"fields"`, as the browser client sees it.
    pub fn as_str(self) -> &'static str {
        match self {
            EncodingOutput::Video => "video",
            EncodingOutput::Audio => "audio",
            EncodingOutput::Data => "data",
            EncodingOutput::Fields => "fields",
        }
    }
}

/// What a subscription's frames travel on (subscribe option `channel`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    /// A WebRTC video track of this format (`video-h264`, `video-vp8`, `video-vp9`, `video-av1`).
    Video(VideoFormat),
    /// A WebRTC Opus audio track (`audio-opus`).
    Audio,
    /// The subscription's data channel (`data`).
    Data,
}

impl Channel {
    /// Every channel name, as subscriptions spell them.
    pub const NAMES: [&str; 6] = ["video-h264", "video-vp8", "video-vp9", "video-av1", "audio-opus", "data"];

    /// The channel called `name`, e.g. `"video-h264"`.
    pub fn parse(name: &str) -> Result<Self, String> {
        Ok(match name {
            "video-h264" => Channel::Video(VideoFormat::H264),
            "video-vp8" => Channel::Video(VideoFormat::Vp8),
            "video-vp9" => Channel::Video(VideoFormat::Vp9),
            "video-av1" => Channel::Video(VideoFormat::Av1),
            "audio-opus" => Channel::Audio,
            "data" => Channel::Data,
            other => return Err(format!("unknown channel {other:?} (one of {})", Self::NAMES.join(", "))),
        })
    }

    /// The name subscriptions use.
    pub fn as_str(self) -> &'static str {
        match self {
            Channel::Video(VideoFormat::H264) => "video-h264",
            Channel::Video(VideoFormat::Vp8) => "video-vp8",
            Channel::Video(VideoFormat::Vp9) => "video-vp9",
            Channel::Video(VideoFormat::Av1) => "video-av1",
            Channel::Audio => "audio-opus",
            Channel::Data => "data",
        }
    }

    /// Where `output` goes by default: video on `video_format`, audio on Opus, anything else on the data channel.
    pub fn default_for(output: EncodingOutput, video_format: VideoFormat) -> Self {
        match output {
            EncodingOutput::Video => Channel::Video(video_format),
            EncodingOutput::Audio => Channel::Audio,
            EncodingOutput::Data | EncodingOutput::Fields => Channel::Data,
        }
    }

    /// The output kind this channel carries, `Data` standing for both data kinds.
    pub(crate) fn kind(self) -> EncodingOutput {
        match self {
            Channel::Video(_) => EncodingOutput::Video,
            Channel::Audio => EncodingOutput::Audio,
            Channel::Data => EncodingOutput::Data,
        }
    }
}

/// What [`MessageEncoding::encode`] gets: the quality the allocator picked and the subscription's `encodeOptions`
/// (without `quality`, which caps the quality instead).
#[derive(Debug, Clone, PartialEq)]
pub struct EncodeOptions {
    /// 0 = smallest, 1 = best: the allocator's pick, at most `encodeOptions.quality`.
    pub quality: f64,
    /// The subscription's other `encodeOptions`, as the page sent them.
    pub options: serde_json::Map<String, serde_json::Value>,
}

impl EncodeOptions {
    /// Quality `quality` with no other options.
    pub fn quality(quality: f64) -> Self {
        EncodeOptions { quality, options: serde_json::Map::new() }
    }

    /// The string option `name`, if set.
    pub fn str(&self, name: &str) -> Option<&str> {
        self.options.get(name).and_then(|value| value.as_str())
    }
}

/// Compression of a subscription's data-channel messages (subscribe option `compress`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Compress {
    /// As encoded.
    #[default]
    None,
    /// zstd level 3; a message that doesn't shrink is sent as is (each frame flags which it is).
    Zstd,
}

impl Compress {
    /// Blocking: the compressed bytes, or `None` when uncompressed or when that would not shrink them.
    pub(crate) fn apply(self, bytes: &[u8]) -> Option<Vec<u8>> {
        match self {
            Compress::None => None,
            Compress::Zstd => zstd::bulk::compress(bytes, 3).ok().filter(|compressed| compressed.len() < bytes.len()),
        }
    }
}

/// One zenoh sample, as an encoding sees it.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct EncodingSample<'a> {
    /// The sample's key expression (a concrete key, even when the subscription used wildcards).
    pub key: &'a str,
    /// The sample's payload bytes.
    pub payload: &'a [u8],
    /// The sample's zenoh encoding, as the publisher set it.
    pub encoding: &'a zenoh::bytes::Encoding,
}

impl<'a> EncodingSample<'a> {
    /// A sample from its parts (the bridge builds these; tests of an encoding can too).
    pub fn new(key: &'a str, payload: &'a [u8], encoding: &'a zenoh::bytes::Encoding) -> Self {
        EncodingSample { key, payload, encoding }
    }
}

/// Pixel layout of a [`VideoImage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PixelFormat {
    /// Packed 8-bit RGB, row-major, no padding: `width × height × 3` bytes.
    Rgb8,
    /// Planar YUV 4:2:0 (BT.601 limited range, what the bridge's encoders signal): the Y plane
    /// (`width × height`), then U and V (`width/2 × height/2` each). Width and height must be even.
    I420,
}

/// An uncompressed picture that a video encoding hands to the bridge's video encoders.
#[derive(Clone, PartialEq, Eq)]
pub struct VideoImage {
    width: u32,
    height: u32,
    format: PixelFormat,
    data: Vec<u8>,
}

impl fmt::Debug for VideoImage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "VideoImage({}x{} {:?}, {} bytes)", self.width, self.height, self.format, self.data.len())
    }
}

impl VideoImage {
    /// Packed RGB8; fails unless `data` is exactly `width × height × 3` bytes and the picture isn't empty.
    pub fn rgb8(width: u32, height: u32, data: Vec<u8>) -> Result<Self> {
        ensure!(width > 0 && height > 0, "empty image ({width}x{height})");
        ensure!(data.len() == width as usize * height as usize * 3, "rgb8 {width}x{height} needs {} bytes, got {}", width as usize * height as usize * 3, data.len());
        Ok(VideoImage { width, height, format: PixelFormat::Rgb8, data })
    }

    /// Planar I420; fails unless both sizes are even and non-zero and `data` is exactly
    /// `width × height × 3 / 2` bytes.
    pub fn i420(width: u32, height: u32, data: Vec<u8>) -> Result<Self> {
        ensure!(width > 0 && height > 0 && width.is_multiple_of(2) && height.is_multiple_of(2), "i420 needs even, non-zero sizes, got {width}x{height}");
        ensure!(data.len() == width as usize * height as usize * 3 / 2, "i420 {width}x{height} needs {} bytes, got {}", width as usize * height as usize * 3 / 2, data.len());
        Ok(VideoImage { width, height, format: PixelFormat::I420, data })
    }

    /// Width in pixels.
    pub fn width(&self) -> u32 {
        self.width
    }

    /// Height in pixels.
    pub fn height(&self) -> u32 {
        self.height
    }

    /// The pixel layout of [`data`](Self::data).
    pub fn format(&self) -> PixelFormat {
        self.format
    }

    /// The pixels, laid out as [`format`](Self::format) says.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Box-filtered to `width × height` as I420 (BT.601 limited range), e.g. to a [`VideoTarget`]'s size.
    pub fn to_i420(&self, width: u32, height: u32) -> Result<VideoImage> {
        ensure!(width.is_multiple_of(2) && height.is_multiple_of(2), "i420 needs even sizes, got {width}x{height}");
        VideoImage::i420(width, height, video::to_i420(self, width, height))
    }
}

/// Interleaved signed 16-bit PCM that an audio encoding hands to the bridge's Opus path.
#[derive(Clone, PartialEq, Eq)]
pub struct AudioPcm {
    sample_rate: u32,
    channels: u8,
    samples: Vec<i16>,
}

impl fmt::Debug for AudioPcm {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "AudioPcm({} Hz x{}, {} samples)", self.sample_rate, self.channels, self.samples.len())
    }
}

impl AudioPcm {
    /// Fails unless the rate is one Opus takes (8, 12, 16, 24 or 48 kHz; resample others), there
    /// are 1 or 2 channels and the samples are a whole number of frames.
    pub fn new(sample_rate: u32, channels: u8, samples: Vec<i16>) -> Result<Self> {
        ensure!([8000, 12000, 16000, 24000, 48000].contains(&sample_rate), "Opus takes 8, 12, 16, 24 or 48 kHz, not {sample_rate} Hz");
        ensure!((1..=2).contains(&channels) && samples.len().is_multiple_of(channels as usize), "{} samples are not whole frames of {channels} (1 or 2) channels", samples.len());
        Ok(AudioPcm { sample_rate, channels, samples })
    }

    /// Samples per second (per channel).
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// 1 (mono) or 2 (stereo, interleaved left, right).
    pub fn channels(&self) -> u8 {
        self.channels
    }

    /// The samples, channels interleaved.
    pub fn samples(&self) -> &[i16] {
        &self.samples
    }
}

/// What [`MessageEncoding::decode`] produced from one sample. Decoded frames are cached and shared by every
/// frontend that subscribes to the same key with the same encoding.
pub enum DecodedFrame {
    /// A picture for the bridge's software H.264 encoder ([`EncodingOutput::Video`]).
    Video(VideoImage),
    /// PCM for the bridge's Opus encoder ([`EncodingOutput::Audio`]).
    Audio(AudioPcm),
    /// Anything the encoding's own [`encode`](MessageEncoding::encode) understands (data-channel
    /// encodings); build it with [`DecodedFrame::data`], read it back with [`DecodedFrame::downcast`].
    Data(Box<dyn Any + Send + Sync>),
}

impl fmt::Debug for DecodedFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodedFrame::Video(image) => write!(formatter, "DecodedFrame::Video({image:?})"),
            DecodedFrame::Audio(pcm) => write!(formatter, "DecodedFrame::Audio({pcm:?})"),
            DecodedFrame::Data(_) => write!(formatter, "DecodedFrame::Data(..)"),
        }
    }
}

impl DecodedFrame {
    /// Wraps an encoding's own decoded value.
    pub fn data<T: Any + Send + Sync>(value: T) -> Self {
        DecodedFrame::Data(Box::new(value))
    }

    /// The value stored with [`DecodedFrame::data`], if it is a `T`; an error otherwise (a picture,
    /// PCM, or another type).
    pub fn downcast<T: Any>(&self) -> Result<&T> {
        match self {
            DecodedFrame::Data(value) => value.downcast_ref::<T>().ok_or_else(|| anyhow!("decoded frame is not a {}", std::any::type_name::<T>())),
            other => Err(anyhow!("{other:?} is not a {}", std::any::type_name::<T>())),
        }
    }
}

/// A message encoding a subscription picks by name (subscribe option `encoding`).
///
/// Implementations must be cheap to call from several threads at once: the bridge calls
/// [`decode`](MessageEncoding::decode) and [`encode`](MessageEncoding::encode) on tokio's blocking pool, for many
/// frontends and subscriptions concurrently. Keep per-call state in the frames, not in `self`.
///
/// ```
/// use zenoh_web::{Channel, DecodedFrame, EncodeOptions, EncodingOutput, EncodingSample, MessageEncoding};
///
/// /// Upper-cases UTF-8 text; lower quality keeps fewer characters.
/// struct Shout;
///
/// impl MessageEncoding for Shout {
///     fn name(&self) -> &str {
///         "text-uppercase"
///     }
///
///     fn output(&self) -> EncodingOutput {
///         EncodingOutput::Data
///     }
///
///     fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> anyhow::Result<DecodedFrame> {
///         Ok(DecodedFrame::data(std::str::from_utf8(sample.payload)?.to_uppercase()))
///     }
///
///     fn encode(&self, frame: &DecodedFrame, options: &EncodeOptions) -> anyhow::Result<Vec<u8>> {
///         let text = frame.downcast::<String>()?;
///         let keep = (text.len() as f64 * options.quality).ceil() as usize;
///         Ok(text.chars().take(keep).collect::<String>().into_bytes())
///     }
/// }
/// ```
pub trait MessageEncoding: Send + Sync {
    /// The name subscriptions use (`encoding: "<name>"`). Must be unique within a server and stay
    /// the same for the encoding's lifetime; by convention `<protocol>_<message type>`.
    fn name(&self) -> &str;

    /// What it produces on its default channel: pictures (video), PCM (audio), or bytes (data, fields).
    fn output(&self) -> EncodingOutput;

    /// What it produces for a subscription on `channel` with these `encodeOptions` (without `quality`), or why it
    /// refuses them; checked when the subscription opens. Default: only its own output's channel (any video format),
    /// with no options.
    fn output_on(&self, channel: Channel, options: &serde_json::Map<String, serde_json::Value>) -> Result<EncodingOutput, String> {
        let own = self.output();
        let wanted = if own == EncodingOutput::Fields { EncodingOutput::Data } else { own };
        if channel.kind() != wanted {
            return Err(format!("{} sends {}, not on channel {}", self.name(), own.as_str(), channel.as_str()));
        }
        if let Some(option) = options.keys().next() {
            return Err(format!("{} takes no encodeOptions but quality, got {option:?}", self.name()));
        }
        Ok(own)
    }

    /// Video: its own encoder of `format` for one encode session (the viewers of a stream at one target), e.g. one that
    /// passes through frames that arrive already encoded. Default `None`: the server's of that format
    /// ([`ServerBuilder::video_encoder`](crate::ServerBuilder::video_encoder), e.g. a hardware one), else the built-in
    /// one (H.264: openh264; AV1: rav1e, feature `av1`); those take [`DecodedFrame::Video`].
    fn video_encoder(&self, format: VideoFormat) -> Option<Box<dyn VideoEncoder>> {
        let _ = format;
        None
    }

    /// Where this encoding's samples live: with `Some(prefix)` a subscription to `key` reads `<prefix>/<key>` and its
    /// messages carry the keys without the prefix, so an encoding's input can sit apart from the raw topic (e.g. a
    /// relay's decoded frames under `@relay/<encoding>`, which raw subscribers to `key` never see). Default `None`.
    fn key_prefix(&self) -> Option<&str> {
        None
    }

    /// Data channel: compression used when the subscription doesn't set `compress`
    /// (e.g. [`Compress::Zstd`] for output that compresses well). Ignored for video.
    fn default_compress(&self) -> Compress {
        Compress::None
    }

    /// Parses and decodes one sample for `channel`. The result is shared by every frontend that receives this
    /// sample through this encoding on that kind of channel, at any quality and options. Video returns what its
    /// encoder takes ([`DecodedFrame::Video`] for the built-in ones, any size), audio [`DecodedFrame::Audio`], data
    /// whatever [`encode`](Self::encode) takes. An error skips the message and is counted in the subscription's
    /// `encodingErrors` stats.
    fn decode(&self, sample: &EncodingSample<'_>, channel: Channel) -> Result<DecodedFrame>;

    /// Data channel: the bytes to send for `frame` with `options` (`options.quality`: 0 = smallest, 1 = best, picked
    /// by the bandwidth allocator within the subscription's `minQuality..encodeOptions.quality`). Results are shared
    /// across frontends asking for the same options and quality (in 1/1000 steps). Video and audio never get this call.
    fn encode(&self, frame: &DecodedFrame, options: &EncodeOptions) -> Result<Vec<u8>> {
        let _ = (frame, options);
        Err(anyhow!("encoding {:?} has no data-channel encoder", self.name()))
    }

    /// The allocator's prior for the data channel: expected bytes per message as sent (after
    /// any compression) with `options` (at `options.quality`), for a sample of `payload_bytes`. Used until sizes are measured, and after
    /// that as the shape between measured qualities (so it should be monotone in `quality`).
    /// The default assumes the output scales linearly from 10% to 100% of the payload. Video is
    /// priced by the bridge from its [`VideoPolicy`] instead, audio by what it sends.
    fn estimated_bytes(&self, payload_bytes: usize, options: &EncodeOptions) -> f64 {
        payload_bytes as f64 * (0.1 + 0.9 * options.quality.clamp(0.0, 1.0))
    }
}
