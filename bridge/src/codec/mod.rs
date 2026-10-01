//! Codecs: transcoders picked per subscription with the subscribe option `codec`.
//!
//! Every codec implements [`Codec`] and lives in the server's registry under its
//! [`name`](Codec::name). A codec decodes a zenoh sample into a [`DecodedFrame`], then one of:
//! - video ([`CodecOutput::Video`]): a [`VideoEncoder`] (the codec's own, e.g. a hardware one, or
//!   the bridge's software H.264) turns the frames into a WebRTC codec's frames, which the bridge
//!   sends on a video track;
//! - audio ([`CodecOutput::Audio`]): PCM, which the bridge encodes to Opus on an audio track;
//! - data: bytes for the subscription's data channel at the quality the bandwidth allocator
//!   picked: [`Fields`](crate::Fields), which the browser client decodes by itself
//!   ([`CodecOutput::Fields`]), or the codec's own format, decoded by a decoder registered
//!   through the client's `registerCodec` ([`CodecOutput::Data`]). Data-channel bytes may be
//!   zstd-compressed on the way ([`Compress`], [`Codec::default_compress`]).
//!
//! Work is lazy (only messages the pacing and queues let through) and runs on tokio's blocking
//! pool. Decodes are shared across frontends by (codec, key, payload), data encodes by (codec,
//! quality, key, payload), so identical requests from several browsers compute once.

pub(crate) mod registry;
pub(crate) mod video;

pub use video::{EncodedVideo, H264Encoder, VideoEncoder, VideoFormat, VideoPolicy, VideoTarget};

use anyhow::{Result, anyhow, ensure};
use std::any::Any;
use std::fmt;

/// Where a codec's output goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodecOutput {
    /// Frames for the codec's [`VideoEncoder`] (by default [`DecodedFrame::Video`] pictures, which
    /// the bridge scales to the allocated quality and encodes to H.264), sent on a WebRTC video
    /// track (the page gets `sub.mediaStream`). The subscription must use `delivery: "latest"`.
    Video,
    /// [`DecodedFrame::Audio`] PCM, which the bridge encodes to Opus in 20 ms packets and sends on a
    /// WebRTC audio track (the page gets `sub.mediaStream`). Never paced or thinned by the allocator.
    Audio,
    /// Bytes from [`Codec::encode`], sent on the subscription's data channel. In the browser,
    /// `msg.bytes` holds them and `msg.decoded` what the decoder registered for this codec's name
    /// (client `registerCodec(name, decoder)`) returned.
    Data,
    /// A [`Fields`](crate::Fields) message from [`Codec::encode`], sent on the data channel; the
    /// browser client decodes it into `msg.decoded` (an object of numbers, strings and typed arrays)
    /// with no codec code in the page.
    Fields,
}

impl CodecOutput {
    /// `"video"`, `"audio"`, `"data"` or `"fields"`, as the browser client sees it.
    pub fn as_str(self) -> &'static str {
        match self {
            CodecOutput::Video => "video",
            CodecOutput::Audio => "audio",
            CodecOutput::Data => "data",
            CodecOutput::Fields => "fields",
        }
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

/// One zenoh sample, as a codec sees it.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct CodecSample<'a> {
    /// The sample's key expression (a concrete key, even when the subscription used wildcards).
    pub key: &'a str,
    /// The sample's payload bytes.
    pub payload: &'a [u8],
    /// The sample's zenoh encoding, as the publisher set it.
    pub encoding: &'a zenoh::bytes::Encoding,
}

impl<'a> CodecSample<'a> {
    /// A sample from its parts (the bridge builds these; tests of a codec can too).
    pub fn new(key: &'a str, payload: &'a [u8], encoding: &'a zenoh::bytes::Encoding) -> Self {
        CodecSample { key, payload, encoding }
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

/// An uncompressed picture that a video codec hands to the bridge's H.264 path.
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

/// Interleaved signed 16-bit PCM that an audio codec hands to the bridge's Opus path.
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

/// What [`Codec::decode`] produced from one sample. Decoded frames are cached and shared by every
/// frontend that subscribes to the same key with the same codec.
pub enum DecodedFrame {
    /// A picture for the bridge's software H.264 encoder ([`CodecOutput::Video`] codecs).
    Video(VideoImage),
    /// PCM for the bridge's Opus encoder ([`CodecOutput::Audio`] codecs).
    Audio(AudioPcm),
    /// Anything the codec's own [`encode`](Codec::encode) understands (data-channel
    /// codecs); build it with [`DecodedFrame::data`], read it back with [`DecodedFrame::downcast`].
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
    /// Wraps a codec's own decoded value.
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

/// A transcoder a subscription can pick by name (subscribe option `codec`).
///
/// Implementations must be cheap to call from several threads at once: the bridge calls
/// [`decode`](Codec::decode) and [`encode`](Codec::encode) on tokio's blocking pool, for many
/// frontends and subscriptions concurrently. Keep per-call state in the frames, not in `self`.
///
/// ```
/// use zenoh_web::{Codec, CodecOutput, CodecSample, DecodedFrame};
///
/// /// Upper-cases UTF-8 text; lower quality keeps fewer characters.
/// struct Shout;
///
/// impl Codec for Shout {
///     fn name(&self) -> &str {
///         "text-uppercase"
///     }
///
///     fn output(&self) -> CodecOutput {
///         CodecOutput::Data
///     }
///
///     fn decode(&self, sample: &CodecSample<'_>) -> anyhow::Result<DecodedFrame> {
///         Ok(DecodedFrame::data(std::str::from_utf8(sample.payload)?.to_uppercase()))
///     }
///
///     fn encode(&self, frame: &DecodedFrame, quality: f64) -> anyhow::Result<Vec<u8>> {
///         let text = frame.downcast::<String>()?;
///         let keep = (text.len() as f64 * quality).ceil() as usize;
///         Ok(text.chars().take(keep).collect::<String>().into_bytes())
///     }
/// }
/// ```
pub trait Codec: Send + Sync {
    /// The name subscriptions use (`codec: "<name>"`). Must be unique within a server and stay
    /// the same for the codec's lifetime; by convention `<protocol>-<message type>`.
    fn name(&self) -> &str;

    /// Whether the output is a video track, an audio track or bytes on the data channel.
    fn output(&self) -> CodecOutput;

    /// Video codecs: a new encoder for one encode session (the viewers of a stream at one target), which also declares
    /// the WebRTC codec the track negotiates; e.g. one that passes through frames that arrive already encoded. Default
    /// `None`: the server's ([`ServerBuilder::video_encoder`](crate::ServerBuilder::video_encoder), e.g. a hardware
    /// one), else the bridge's software H.264 (openh264); both take [`DecodedFrame::Video`].
    fn video_encoder(&self) -> Option<Box<dyn VideoEncoder>> {
        None
    }

    /// Data-channel codecs: compression used when the subscription doesn't set `compress`
    /// (e.g. [`Compress::Zstd`] for output that compresses well). Ignored for video.
    fn default_compress(&self) -> Compress {
        Compress::None
    }

    /// Parses and decodes one sample. The result is shared by every frontend that receives this
    /// sample through this codec, at any quality. Video codecs return what their encoder takes
    /// ([`DecodedFrame::Video`] for the default, any size), audio codecs [`DecodedFrame::Audio`].
    /// An error skips the message and is counted in the subscription's `codecErrors` stats.
    fn decode(&self, sample: &CodecSample<'_>) -> Result<DecodedFrame>;

    /// Data-channel codecs: the bytes to send for `frame` at `quality` (0 = smallest,
    /// 1 = best, picked by the bandwidth allocator within the subscription's
    /// `minQuality..maxQuality`). Results are shared across frontends asking for the same
    /// quality (in 1/1000 steps). Video and audio codecs never get this call.
    fn encode(&self, frame: &DecodedFrame, quality: f64) -> Result<Vec<u8>> {
        let _ = (frame, quality);
        Err(anyhow!("codec {:?} has no data-channel encoder", self.name()))
    }

    /// The allocator's prior for data-channel codecs: expected bytes per message as sent (after
    /// any compression) at `quality`, for a sample of `payload_bytes`. Used until sizes are measured, and after
    /// that as the shape between measured qualities (so it should be monotone in `quality`).
    /// The default assumes the output scales linearly from 10% to 100% of the payload. Video is
    /// priced by the bridge from its [`VideoPolicy`] instead, audio by what it sends.
    fn estimated_bytes(&self, payload_bytes: usize, quality: f64) -> f64 {
        payload_bytes as f64 * (0.1 + 0.9 * quality.clamp(0.0, 1.0))
    }
}
