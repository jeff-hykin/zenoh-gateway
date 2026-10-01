//! Codecs: transcoders picked per subscription with the subscribe option `codec`.
//!
//! Every codec implements [`Codec`] and lives in the server's registry
//! under its [`name`](Codec::name). A codec decodes a zenoh sample into a [`DecodedFrame`], then
//! either hands the bridge raw video ([`CodecOutput::Video`]: the bridge scales it, encodes H.264
//! and sends it on a WebRTC video track) or encodes bytes for the subscription's data channel at
//! the quality the bandwidth allocator picked: [`Fields`](crate::Fields), which the browser client
//! decodes by itself ([`CodecOutput::Fields`]), or the codec's own format, decoded by a decoder
//! registered through the client's `registerCodec` ([`CodecOutput::Data`]). Data-channel bytes may
//! be zstd-compressed on the way ([`Compress`], [`Codec::default_compress`]).
//!
//! Work is lazy (only messages the pacing and queues let through) and runs on tokio's blocking
//! pool. Decodes are shared across frontends by (codec, key, payload), data encodes by (codec,
//! quality, key, payload), so identical requests from several browsers compute once.

pub(crate) mod registry;
pub(crate) mod video;

use anyhow::{Result, anyhow, ensure};
use std::any::Any;
use std::fmt;

/// Where a codec's output goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodecOutput {
    /// [`DecodedFrame::Video`] frames, which the bridge scales to the allocated quality, encodes
    /// to H.264 and sends on a WebRTC video track (the page gets `sub.mediaStream`). The
    /// subscription must use `delivery: "latest"`. No browser code is needed.
    Video,
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
    /// `"video"` or `"data"`, as the browser client sees it.
    pub fn as_str(self) -> &'static str {
        match self {
            CodecOutput::Video => "video",
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
    /// Planar YUV 4:2:0 (BT.601, the layout H.264 encodes): the Y plane (`width × height`), then
    /// U and V (`width/2 × height/2` each). Width and height must be even.
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
}

/// What [`Codec::decode`] produced from one sample. Decoded frames are cached and shared by every
/// frontend that subscribes to the same key with the same codec.
pub enum DecodedFrame {
    /// A picture for the bridge's H.264 path ([`CodecOutput::Video`] codecs).
    Video(VideoImage),
    /// Anything the codec's own [`encode`](Codec::encode) understands (data-channel
    /// codecs); build it with [`DecodedFrame::data`], read it back with [`DecodedFrame::downcast`].
    Data(Box<dyn Any + Send + Sync>),
}

impl fmt::Debug for DecodedFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodedFrame::Video(image) => write!(formatter, "DecodedFrame::Video({image:?})"),
            DecodedFrame::Data(_) => write!(formatter, "DecodedFrame::Data(..)"),
        }
    }
}

impl DecodedFrame {
    /// Wraps a codec's own decoded value.
    pub fn data<T: Any + Send + Sync>(value: T) -> Self {
        DecodedFrame::Data(Box::new(value))
    }

    /// The value stored with [`DecodedFrame::data`], if it is a `T`; an error otherwise (a video
    /// frame, or another type).
    pub fn downcast<T: Any>(&self) -> Result<&T> {
        match self {
            DecodedFrame::Data(value) => value.downcast_ref::<T>().ok_or_else(|| anyhow!("decoded frame is not a {}", std::any::type_name::<T>())),
            DecodedFrame::Video(_) => Err(anyhow!("decoded frame is video, not {}", std::any::type_name::<T>())),
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

    /// Whether the output is video (the bridge's H.264 track) or bytes on the data channel.
    fn output(&self) -> CodecOutput;

    /// Data-channel codecs: compression used when the subscription doesn't set `compress`
    /// (e.g. [`Compress::Zstd`] for output that compresses well). Ignored for video.
    fn default_compress(&self) -> Compress {
        Compress::None
    }

    /// Parses and decodes one sample. The result is shared by every frontend that receives this
    /// sample through this codec, at any quality. [`CodecOutput::Video`] codecs must return
    /// [`DecodedFrame::Video`] (any size: the bridge scales it per quality and keeps it even).
    /// An error skips the message and is counted in the subscription's `codecErrors` stats.
    fn decode(&self, sample: &CodecSample<'_>) -> Result<DecodedFrame>;

    /// Data-channel codecs: the bytes to send for `frame` at `quality` (0 = smallest,
    /// 1 = best, picked by the bandwidth allocator within the subscription's
    /// `minQuality..maxQuality`). Results are shared across frontends asking for the same
    /// quality (in 1/1000 steps). Video codecs never get this call.
    fn encode(&self, frame: &DecodedFrame, quality: f64) -> Result<Vec<u8>> {
        let _ = (frame, quality);
        Err(anyhow!("codec {:?} has no data-channel encoder", self.name()))
    }

    /// The allocator's prior for data-channel codecs: expected bytes per message as sent (after
    /// any compression) at `quality`, for a sample of `payload_bytes`. Used until sizes are measured, and after
    /// that as the shape between measured qualities (so it should be monotone in `quality`).
    /// The default assumes the output scales linearly from 10% to 100% of the payload. Video
    /// codecs are priced by the bridge from resolution and bits per pixel instead.
    fn estimated_bytes(&self, payload_bytes: usize, quality: f64) -> f64 {
        payload_bytes as f64 * (0.1 + 0.9 * quality.clamp(0.0, 1.0))
    }
}
