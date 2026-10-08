//! Data channel labels: `{"type":"sub"|"pub"|"heartbeat", "key":..., "id":..., "opts":{...}}`.

use crate::encoding::registry::EncodingRegistry;
use crate::encoding::registry::Resolved;
use crate::encoding::{Channel, Compress, EncodingOutput, VideoFormat, VideoPolicy};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct Label {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub key: String,
    /// Client-chosen id, echoed back in stats and used to address a publisher's deadman.
    pub id: Option<u64>,
    #[serde(default)]
    pub opts: Value,
    /// video and audio channels: the browser transceiver (renegotiated earlier) whose track carries the frames
    pub mid: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryKind {
    #[default]
    Latest,
    Reliable,
}

/// Subscribe options. bandwidthPriority, the quality range and the tradeoff feed the per-frontend allocator; `encoding`
/// picks a message encoding (none = raw passthrough), `channel` what its output travels on, `encodeOptions` what the
/// encoding gets (`quality` there: the most the allocator may pick).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubOpts {
    #[serde(default)]
    pub delivery: DeliveryKind,
    pub priority: Option<u8>,
    pub bandwidth_priority: Option<f64>,
    pub max_age: Option<f64>,
    pub max_hz: Option<f64>,
    pub min_quality: Option<f64>,
    pub quality_to_hz_tradeoff: Option<f64>,
    pub encoding: Option<String>,
    /// `video-h264` .. `data`; unset = where the encoding's output goes by default
    pub channel: Option<String>,
    /// passed to the encoding, except `quality` (the most the allocator may pick, default 1)
    pub encode_options: Option<serde_json::Map<String, Value>>,
    /// data-channel compression; unset = the encoding's default (none without one)
    pub compress: Option<Compress>,
    /// video channels: bits/s the stream asks for at most, and how far its picture may shrink (see `VideoPolicy`)
    pub max_bitrate: Option<f64>,
    pub min_resolution_scale: Option<f64>,
    pub max_resolution: Option<(u32, u32)>,
    /// video channels: [min, max] ms the browser may hold a frame to smooth out jitter (default [0, 0]: show at once)
    pub playout_delay: Option<(f64, f64)>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PubOpts {
    #[serde(default)]
    pub delivery: DeliveryKind,
    pub priority: Option<u8>,
    /// Drop puts whose (clock-corrected) send time is older than this many ms.
    pub latency_limit: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HeartbeatOpts {
    pub hz: f64,
    pub misses: u32,
}

/// Normalized delivery policy.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Delivery {
    /// Max pending samples per key: 1 (the newest) for `latest`, unbounded (`None`) for `reliable`.
    pub queue: Option<usize>,
    pub max_age_ms: Option<f64>,
    pub reliable: bool,
}

/// `Err("<name> must be <must_be>, got <value>")` for a value that is set and not `valid`.
fn check(name: &str, value: Option<f64>, valid: impl Fn(f64) -> bool, must_be: &str) -> Result<(), String> {
    match value {
        Some(v) if !valid(v) => Err(format!("{name} must be {must_be}, got {v}")),
        _ => Ok(()),
    }
}

fn check_priority(priority: Option<u8>) -> Result<(), String> {
    check("priority", priority.map(f64::from), |p| (1.0..=7.0).contains(&p), "1..7")
}

fn check_positive(name: &str, value: Option<f64>) -> Result<(), String> {
    check(name, value, |v| v.is_finite() && v > 0.0, "a positive number")
}

impl SubOpts {
    pub fn parse(opts: &Value) -> Result<Self, String> {
        let parsed: SubOpts = serde_json::from_value(opts.clone()).map_err(|e| e.to_string())?;
        check_priority(parsed.priority)?;
        check_positive("maxAge", parsed.max_age)?;
        check_positive("maxHz", parsed.max_hz)?;
        check_positive("maxBitrate", parsed.max_bitrate)?;
        check("minResolutionScale", parsed.min_resolution_scale, |scale| scale > 0.0 && scale <= 1.0, "within (0, 1]")?;
        if parsed.max_resolution.is_some_and(|(width, height)| width < 16 || height < 16) {
            return Err("maxResolution must be at least [16, 16]".into());
        }
        // the RTP extension carries 12 bits of 10 ms each
        if parsed.playout_delay.is_some_and(|(min, max)| !(min >= 0.0 && min <= max && max <= 40_950.0)) {
            return Err("playoutDelay must be [min, max] ms with 0 <= min <= max <= 40950".into());
        }
        check("bandwidthPriority", parsed.bandwidth_priority, |weight| weight.is_finite() && weight >= 0.0, ">= 0")?;
        let quality = match parsed.encode_options.as_ref().and_then(|options| options.get("quality")) {
            None => None,
            Some(value) => Some(value.as_f64().ok_or_else(|| format!("encodeOptions.quality must be a number, got {value}"))?),
        };
        for (name, value) in [("minQuality", parsed.min_quality), ("encodeOptions.quality", quality), ("qualityToHzTradeoff", parsed.quality_to_hz_tradeoff)] {
            check(name, value, |v| (0.0..=1.0).contains(&v), "within 0..1")?;
        }
        if parsed.min_quality.unwrap_or(0.0) > quality.unwrap_or(1.0) {
            return Err("minQuality must be <= encodeOptions.quality".into());
        }
        Ok(parsed)
    }

    /// `encodeOptions` without `quality`: what the encoding gets.
    pub fn encoding_options(&self) -> serde_json::Map<String, Value> {
        let mut options = self.encode_options.clone().unwrap_or_default();
        options.remove("quality");
        options
    }

    /// The subscription's encoding from `registry` (None = raw), the channel it travels on and what it sends there,
    /// filling `compress` in with the encoding's default. Unknown names and channels, options the encoding refuses, a
    /// video format the server can't encode, and video or audio on reliable delivery or with zstd are refused.
    pub fn resolve_encoding(&mut self, registry: &EncodingRegistry) -> Result<Resolved, String> {
        let channel = self.channel.as_deref().map(Channel::parse).transpose()?;
        let Some(name) = self.encoding.as_deref() else {
            if channel.is_some_and(|channel| channel != Channel::Data) {
                return Err(format!("channel {} needs an encoding (raw bytes go on the data channel)", channel.unwrap().as_str()));
            }
            if self.encode_options.is_some() {
                return Err("encodeOptions need an encoding".into());
            }
            self.compress.get_or_insert_default();
            return Ok(Resolved { encoding: None, channel: Channel::Data, output: EncodingOutput::Data });
        };
        let encoding = registry.get(name)?;
        let channel = channel.unwrap_or_else(|| Channel::default_for(encoding.output(), VideoFormat::H264));
        let output = encoding.output_on(channel, &self.encoding_options())?;
        let video = matches!(channel, Channel::Video(_));
        if !video && (self.max_bitrate.is_some() || self.min_resolution_scale.is_some() || self.max_resolution.is_some() || self.playout_delay.is_some()) {
            return Err(format!("maxBitrate, minResolutionScale, maxResolution and playoutDelay are for video channels, not {}", channel.as_str()));
        }
        if let Channel::Video(format) = channel {
            registry.video_encoder(&*encoding, format)?;
        }
        if channel != Channel::Data {
            if self.delivery == DeliveryKind::Reliable {
                return Err(format!("channel {} is a lossy media track, use delivery \"latest\"", channel.as_str()));
            }
            if self.compress == Some(Compress::Zstd) {
                return Err(format!("channel {} is already compressed, compress must be \"none\"", channel.as_str()));
            }
            self.compress = Some(Compress::None);
        }
        self.compress.get_or_insert(encoding.default_compress());
        Ok(Resolved { encoding: Some(encoding), channel, output })
    }

    /// playoutDelay in the RTP extension's 10 ms units.
    pub fn playout_delay_units(&self) -> (u16, u16) {
        let (min, max) = self.playout_delay.unwrap_or((0.0, 0.0));
        ((min / 10.0).round() as u16, (max / 10.0).round() as u16)
    }

    /// The server's video policy with this subscription's overrides.
    pub fn video_policy(&self, server: VideoPolicy) -> VideoPolicy {
        VideoPolicy {
            max_bitrate: self.max_bitrate.or(server.max_bitrate),
            min_resolution_scale: self.min_resolution_scale.unwrap_or(server.min_resolution_scale),
            max_resolution: self.max_resolution.or(server.max_resolution),
            ..server
        }
    }

    /// `minQuality ..= encodeOptions.quality`.
    pub fn quality_range(&self) -> (f64, f64) {
        let most = self.encode_options.as_ref().and_then(|options| options.get("quality")).and_then(Value::as_f64).unwrap_or(1.0);
        (self.min_quality.unwrap_or(0.0), most)
    }

    pub fn delivery(&self) -> Delivery {
        let reliable = self.delivery == DeliveryKind::Reliable;
        Delivery { queue: (!reliable).then_some(1), max_age_ms: self.max_age, reliable }
    }

    /// Minimum spacing between two sends of the same key, from `maxHz`.
    pub fn min_interval(&self) -> Option<Duration> {
        self.max_hz.map(|hz| Duration::from_secs_f64(1.0 / hz))
    }

    /// Every option with its default filled in, for stats.
    pub fn normalized(&self) -> Value {
        json!({
            "delivery": self.delivery,
            "priority": self.priority,
            "bandwidthPriority": self.bandwidth_priority.unwrap_or(1.0),
            "maxAge": self.max_age,
            "maxHz": self.max_hz,
            "minQuality": self.min_quality.unwrap_or(0.0),
            "qualityToHzTradeoff": self.quality_to_hz_tradeoff.unwrap_or(0.5),
            "encoding": self.encoding,
            "channel": self.channel,
            "encodeOptions": self.encode_options,
            "compress": self.compress.unwrap_or_default(),
            "maxBitrate": self.max_bitrate,
            "minResolutionScale": self.min_resolution_scale,
            "maxResolution": self.max_resolution,
            "playoutDelay": self.playout_delay.unwrap_or((0.0, 0.0)),
        })
    }
}

impl PubOpts {
    pub fn parse(opts: &Value) -> Result<Self, String> {
        let parsed: PubOpts = serde_json::from_value(opts.clone()).map_err(|e| e.to_string())?;
        check_priority(parsed.priority)?;
        check_positive("latencyLimit", parsed.latency_limit)?;
        Ok(parsed)
    }

    pub fn zenoh_priority(&self) -> Option<zenoh::qos::Priority> {
        self.priority.and_then(|p| zenoh::qos::Priority::try_from(p).ok())
    }
}

impl HeartbeatOpts {
    pub fn parse(opts: &Value) -> Result<Self, String> {
        let parsed: HeartbeatOpts = serde_json::from_value(opts.clone()).map_err(|e| e.to_string())?;
        check_positive("heartbeatHz", Some(parsed.hz))?;
        if parsed.misses == 0 {
            return Err("heartbeatMisses must be >= 1".into());
        }
        Ok(parsed)
    }

    /// Silence longer than this means the frontend is gone.
    pub fn deadline(&self) -> Duration {
        Duration::from_secs_f64(self.misses as f64 / self.hz)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{DecodedFrame, EncodingSample, MessageEncoding};
    use std::sync::Arc;

    fn sub(opts: &str) -> Result<SubOpts, String> {
        SubOpts::parse(&serde_json::from_str(opts).unwrap())
    }

    struct Named(&'static str, EncodingOutput);

    impl MessageEncoding for Named {
        fn name(&self) -> &str {
            self.0
        }

        fn output(&self) -> EncodingOutput {
            self.1
        }

        fn default_compress(&self) -> Compress {
            Compress::Zstd
        }

        fn decode(&self, _: &EncodingSample<'_>, _: Channel) -> anyhow::Result<DecodedFrame> {
            anyhow::bail!("not decoded in these tests")
        }
    }

    /// Takes `{"format": "a" | "b"}` on the data channel, and video.
    struct Flexible;

    impl MessageEncoding for Flexible {
        fn name(&self) -> &str {
            "flexible"
        }

        fn output(&self) -> EncodingOutput {
            EncodingOutput::Video
        }

        fn output_on(&self, channel: Channel, options: &serde_json::Map<String, Value>) -> Result<EncodingOutput, String> {
            match (channel, options.get("format").and_then(Value::as_str)) {
                (Channel::Video(_), None) => Ok(EncodingOutput::Video),
                (Channel::Data, Some("a" | "b") | None) => Ok(EncodingOutput::Data),
                _ => Err("flexible: video, or data with format a or b".into()),
            }
        }

        fn decode(&self, _: &EncodingSample<'_>, _: Channel) -> anyhow::Result<DecodedFrame> {
            anyhow::bail!("not decoded in these tests")
        }
    }

    #[test]
    fn delivery_forms() {
        let latest = Delivery { queue: Some(1), max_age_ms: None, reliable: false };
        assert_eq!(sub(r#"{}"#).unwrap().delivery(), latest);
        assert_eq!(sub(r#"{"delivery":"latest"}"#).unwrap().delivery(), latest);
        let reliable = sub(r#"{"delivery":"reliable","maxAge":500}"#).unwrap().delivery();
        assert_eq!(reliable, Delivery { queue: None, max_age_ms: Some(500.0), reliable: true });
    }

    #[test]
    fn validation() {
        assert!(sub(r#"{"hz":[1,2]}"#).is_err(), "unknown option names are rejected");
        assert!(sub(r#"{"queueSize":3}"#).is_err());
        assert!(sub(r#"{"delivery":"sometimes"}"#).is_err());
        assert!(sub(r#"{"maxHz":0}"#).is_err());
        assert!(sub(r#"{"priority":9}"#).is_err());
        assert!(sub(r#"{"minQuality":0.8,"encodeOptions":{"quality":0.2}}"#).is_err());
        assert!(sub(r#"{"encodeOptions":{"quality":"high"}}"#).is_err());
        assert!(sub(r#"{"maxQuality":0.5}"#).is_err(), "maxQuality is encodeOptions.quality now");
        assert!(sub(r#"{"codec":"camera"}"#).is_err(), "codec is encoding now");
        assert!(sub(r#"{"qualityToHzTradeoff":2}"#).is_err());
        let encodings = [Arc::new(Named("camera", EncodingOutput::Video)) as Arc<dyn MessageEncoding>, Arc::new(Named("table", EncodingOutput::Data)), Arc::new(Named("mic", EncodingOutput::Audio)), Arc::new(Flexible)];
        let registry = EncodingRegistry::new(encodings).unwrap();
        let resolve = |opts: &str| sub(opts)?.resolve_encoding(&registry).map(|resolved| (resolved.encoding.map(|encoding| encoding.name().to_owned()), resolved.channel.as_str(), resolved.output));
        let compress = |opts: &str| {
            let mut parsed = sub(opts)?;
            parsed.resolve_encoding(&registry)?;
            Ok::<_, String>(parsed.normalized()["compress"].as_str().unwrap().to_owned())
        };
        assert!(resolve(r#"{"encoding":"jpeg"}"#).unwrap_err().contains("unknown encoding"));
        assert!(resolve(r#"{"encoding":"camera","delivery":"reliable"}"#).unwrap_err().contains("lossy"), "video is lossy");
        assert_eq!(resolve(r#"{"encoding":"table","delivery":"reliable"}"#).unwrap(), (Some("table".into()), "data", EncodingOutput::Data));
        assert_eq!(resolve(r#"{}"#).unwrap(), (None, "data", EncodingOutput::Data));
        assert_eq!(resolve(r#"{"encoding":"camera"}"#).unwrap().1, "video-h264", "video goes on H.264 by default");
        assert_eq!(resolve(r#"{"encoding":"mic"}"#).unwrap().1, "audio-opus");
        assert!(resolve(r#"{"encoding":"camera","channel":"video-vp8"}"#).unwrap_err().contains("no video-vp8 encoder"));
        assert!(resolve(r#"{"encoding":"camera","channel":"data"}"#).unwrap_err().contains("not on channel data"), "by default only its own channel");
        assert!(resolve(r#"{"encoding":"camera","channel":"video-h265"}"#).unwrap_err().contains("unknown channel"));
        assert!(resolve(r#"{"encoding":"table","encodeOptions":{"format":"x"}}"#).unwrap_err().contains("takes no encodeOptions"));
        assert_eq!(resolve(r#"{"encoding":"table","encodeOptions":{"quality":0.5}}"#).unwrap().1, "data", "quality is always allowed");
        assert_eq!(resolve(r#"{"encoding":"flexible","channel":"data","encodeOptions":{"format":"a"}}"#).unwrap(), (Some("flexible".into()), "data", EncodingOutput::Data));
        assert!(resolve(r#"{"encoding":"flexible","channel":"data","encodeOptions":{"format":"c"}}"#).is_err());
        assert!(resolve(r#"{"channel":"video-h264"}"#).unwrap_err().contains("needs an encoding"));
        assert!(resolve(r#"{"encodeOptions":{"quality":0.5}}"#).unwrap_err().contains("need an encoding"));
        assert_eq!(sub(r#"{"encoding":"flexible","encodeOptions":{"quality":0.4,"format":"a"}}"#).unwrap().encoding_options(), serde_json::from_str::<serde_json::Map<String, Value>>(r#"{"format":"a"}"#).unwrap(), "the encoding doesn't see quality");
        assert!(sub(r#"{"imageTransport":"jpeg"}"#).is_err(), "JPEG files are gone");
        assert_eq!(compress(r#"{}"#).unwrap(), "none");
        assert_eq!(compress(r#"{"compress":"zstd"}"#).unwrap(), "zstd", "raw topics compress too");
        assert_eq!(compress(r#"{"encoding":"table"}"#).unwrap(), "zstd", "the encoding's default");
        assert_eq!(compress(r#"{"encoding":"table","compress":"none"}"#).unwrap(), "none", "an explicit option overrides it");
        assert_eq!(compress(r#"{"encoding":"camera"}"#).unwrap(), "none");
        assert!(compress(r#"{"encoding":"camera","compress":"zstd"}"#).unwrap_err().contains("already compressed"));
        assert!(sub(r#"{"compress":"gzip"}"#).is_err());
        assert!(sub(r#"{"bandwidthPriority":-1}"#).is_err());
        assert!(sub(r#"{"dangerousMinHz":1}"#).is_err());
        assert!(sub(r#"{"maxBitrate":0}"#).is_err());
        assert_eq!(sub(r#"{"playoutDelay":[0,200]}"#).unwrap().playout_delay_units(), (0, 20), "10 ms units");
        assert!(sub(r#"{"playoutDelay":[100,50]}"#).is_err(), "min above max");
        assert!(sub(r#"{"playoutDelay":[0,50000]}"#).is_err(), "past the extension's 12 bits");
        assert!(resolve(r#"{"encoding":"table","playoutDelay":[0,100]}"#).unwrap_err().contains("are for video channels"));
        assert!(sub(r#"{"minResolutionScale":1.5}"#).is_err());
        assert!(sub(r#"{"maxResolution":[8,8]}"#).is_err());
        assert!(resolve(r#"{"encoding":"table","maxBitrate":1e6}"#).unwrap_err().contains("for video channels"));
        let video = sub(r#"{"encoding":"camera","maxBitrate":4e6,"maxResolution":[640,480]}"#).unwrap();
        assert!(video.clone().resolve_encoding(&registry).is_ok());
        let policy = video.video_policy(VideoPolicy::default());
        assert_eq!((policy.max_bitrate, policy.max_resolution, policy.min_resolution_scale), (Some(4e6), Some((640, 480)), 0.25));
        let full = sub(r#"{"bandwidthPriority":2,"maxHz":20,"minQuality":0.3,"encodeOptions":{"quality":0.9},"qualityToHzTradeoff":0.7}"#).unwrap();
        assert_eq!(full.quality_range(), (0.3, 0.9));
        assert_eq!((full.normalized()["bandwidthPriority"].as_f64(), full.normalized()["qualityToHzTradeoff"].as_f64()), (Some(2.0), Some(0.7)));
        assert_eq!(full.min_interval(), Some(Duration::from_millis(50)));
    }

    #[test]
    fn pub_and_heartbeat() {
        let parsed = PubOpts::parse(&json!({"delivery":"reliable","priority":1,"latencyLimit":200})).unwrap();
        assert_eq!(parsed.latency_limit, Some(200.0));
        assert!(PubOpts::parse(&json!({"latencyLimit":-1})).is_err());
        let heartbeat = HeartbeatOpts::parse(&json!({"hz":5,"misses":3})).unwrap();
        assert_eq!(heartbeat.deadline(), Duration::from_millis(600));
        assert!(HeartbeatOpts::parse(&json!({"hz":5,"misses":0})).is_err());
    }
}
