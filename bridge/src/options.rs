//! Data channel labels: `{"type":"sub"|"pub"|"heartbeat", "key":..., "id":..., "opts":{...}}`.

use crate::codec::registry::CodecRegistry;
use crate::codec::{Codec, CodecOutput};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct Label {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub key: String,
    /// Client-chosen id, echoed back in stats and used to address a publisher's deadman.
    #[serde(default)]
    pub id: Option<u64>,
    #[serde(default)]
    pub opts: Value,
    /// video codecs: the browser transceiver (renegotiated earlier) whose track carries the frames
    #[serde(default)]
    pub mid: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryKind {
    #[default]
    Latest,
    Reliable,
}

/// Present-but-null (what JSON.stringify makes of Infinity) becomes `Some(None)`.
fn present_or_null<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Option<f64>>, D::Error> {
    Option::<f64>::deserialize(deserializer).map(Some)
}

/// Subscribe options. bandwidthPriority, dangerousMinHz, the quality range and the tradeoff feed
/// the per-frontend allocator; `codec` picks a transcoder (none = raw passthrough).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubOpts {
    #[serde(default)]
    pub delivery: DeliveryKind,
    pub priority: Option<u8>,
    pub bandwidth_priority: Option<f64>,
    #[serde(default, deserialize_with = "present_or_null")]
    pub queue_size: Option<Option<f64>>,
    pub max_age: Option<f64>,
    pub max_hz: Option<f64>,
    pub dangerous_min_hz: Option<f64>,
    pub min_quality: Option<f64>,
    pub max_quality: Option<f64>,
    pub quality_to_hz_tradeoff: Option<f64>,
    pub codec: Option<String>,
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
    /// Max pending samples per key; `None` = unbounded.
    pub queue: Option<usize>,
    pub max_age_ms: Option<f64>,
    pub reliable: bool,
}

fn check_priority(priority: Option<u8>) -> Result<(), String> {
    match priority {
        Some(p) if !(1..=7).contains(&p) => Err(format!("priority must be 1..7, got {p}")),
        _ => Ok(()),
    }
}

fn check_positive(name: &str, value: Option<f64>) -> Result<(), String> {
    match value {
        Some(v) if !(v.is_finite() && v > 0.0) => Err(format!("{name} must be a positive number, got {v}")),
        _ => Ok(()),
    }
}

fn check_unit(name: &str, value: Option<f64>) -> Result<(), String> {
    match value {
        Some(v) if !(0.0..=1.0).contains(&v) => Err(format!("{name} must be within 0..1, got {v}")),
        _ => Ok(()),
    }
}

impl SubOpts {
    pub fn parse(opts: &Value) -> Result<Self, String> {
        let parsed: SubOpts = serde_json::from_value(opts.clone()).map_err(|e| e.to_string())?;
        parsed.validate()?;
        Ok(parsed)
    }

    fn validate(&self) -> Result<(), String> {
        check_priority(self.priority)?;
        check_positive("maxAge", self.max_age)?;
        check_positive("maxHz", self.max_hz)?;
        if let Some(Some(n)) = self.queue_size
            && !(n >= 1.0 && n.fract() == 0.0)
        {
            return Err(format!("queueSize must be an integer >= 1 or Infinity, got {n}"));
        }
        match self.bandwidth_priority {
            Some(w) if !(w.is_finite() && w >= 0.0) => return Err(format!("bandwidthPriority must be >= 0, got {w}")),
            _ => {}
        }
        match self.dangerous_min_hz {
            Some(v) if !(v.is_finite() && v >= 0.0) => return Err(format!("dangerousMinHz must be >= 0, got {v}")),
            Some(v) if self.max_hz.is_some_and(|max| v > max) => return Err("dangerousMinHz must be <= maxHz".into()),
            _ => {}
        }
        check_unit("minQuality", self.min_quality)?;
        check_unit("maxQuality", self.max_quality)?;
        check_unit("qualityToHzTradeoff", self.quality_to_hz_tradeoff)?;
        if self.min_quality.unwrap_or(0.0) > self.max_quality.unwrap_or(1.0) {
            return Err("minQuality must be <= maxQuality".into());
        }
        Ok(())
    }

    /// The subscription's codec from `registry` (None = raw); unknown names and video codecs on
    /// reliable delivery are refused.
    pub fn resolve_codec(&self, registry: &CodecRegistry) -> Result<Option<Arc<dyn Codec>>, String> {
        let Some(name) = self.codec.as_deref() else { return Ok(None) };
        let codec = registry.get(name)?;
        if codec.output() == CodecOutput::Video && self.delivery == DeliveryKind::Reliable {
            return Err(format!("{name} is a video codec: frames go over a lossy video track, use delivery \"latest\""));
        }
        Ok(Some(codec))
    }

    pub fn quality_range(&self) -> (f64, f64) {
        (self.min_quality.unwrap_or(0.0), self.max_quality.unwrap_or(1.0))
    }

    pub fn delivery(&self) -> Delivery {
        let reliable = self.delivery == DeliveryKind::Reliable;
        let queue = match self.queue_size {
            Some(Some(n)) => Some(n as usize),
            Some(None) => None,
            None if reliable => None,
            None => Some(1),
        };
        Delivery { queue, max_age_ms: self.max_age, reliable }
    }

    /// Minimum spacing between two sends of the same key, from `maxHz`.
    pub fn min_interval(&self) -> Option<Duration> {
        self.max_hz.map(|hz| Duration::from_secs_f64(1.0 / hz))
    }

    pub fn zenoh_priority(&self) -> Option<zenoh::qos::Priority> {
        self.priority.and_then(|p| zenoh::qos::Priority::try_from(p).ok())
    }

    /// Every option with its default filled in, for stats.
    pub fn normalized(&self) -> Value {
        let delivery = self.delivery();
        json!({
            "delivery": self.delivery,
            "priority": self.priority,
            "bandwidthPriority": self.bandwidth_priority.unwrap_or(1.0),
            "queueSize": delivery.queue,
            "maxAge": self.max_age,
            "maxHz": self.max_hz,
            "dangerousMinHz": self.dangerous_min_hz.unwrap_or(0.0),
            "minQuality": self.min_quality.unwrap_or(0.0),
            "maxQuality": self.max_quality.unwrap_or(1.0),
            "qualityToHzTradeoff": self.quality_to_hz_tradeoff.unwrap_or(0.5),
            "codec": self.codec,
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

    fn sub(opts: &str) -> Result<SubOpts, String> {
        SubOpts::parse(&serde_json::from_str(opts).unwrap())
    }

    #[test]
    fn delivery_forms() {
        let latest = Delivery { queue: Some(1), max_age_ms: None, reliable: false };
        assert_eq!(sub(r#"{}"#).unwrap().delivery(), latest);
        assert_eq!(sub(r#"{"delivery":"latest"}"#).unwrap().delivery(), latest);
        let reliable = sub(r#"{"delivery":"reliable"}"#).unwrap().delivery();
        assert_eq!(reliable, Delivery { queue: None, max_age_ms: None, reliable: true });
        let custom = sub(r#"{"queueSize":3,"maxAge":500}"#).unwrap().delivery();
        assert_eq!(custom, Delivery { queue: Some(3), max_age_ms: Some(500.0), reliable: false });
        assert_eq!(sub(r#"{"queueSize":null}"#).unwrap().delivery().queue, None);
    }

    #[test]
    fn validation() {
        assert!(sub(r#"{"hz":[1,2]}"#).is_err(), "old option names are rejected");
        assert!(sub(r#"{"delivery":"sometimes"}"#).is_err());
        assert!(sub(r#"{"queueSize":0}"#).is_err());
        assert!(sub(r#"{"queueSize":1.5}"#).is_err());
        assert!(sub(r#"{"maxHz":0}"#).is_err());
        assert!(sub(r#"{"priority":9}"#).is_err());
        assert!(sub(r#"{"minQuality":0.8,"maxQuality":0.2}"#).is_err());
        assert!(sub(r#"{"qualityToHzTradeoff":2}"#).is_err());
        assert!(sub(r#"{"maxHz":5,"dangerousMinHz":10}"#).is_err());
        let registry = CodecRegistry::new([]).unwrap();
        let resolve = |opts: &str| sub(opts).unwrap().resolve_codec(&registry).map(|codec| codec.map(|codec| codec.name().to_owned()));
        assert!(resolve(r#"{"codec":"ros2-jpeg"}"#).unwrap_err().contains("unknown codec"));
        assert!(resolve(r#"{"codec":"ros2-image","delivery":"reliable"}"#).unwrap_err().contains("video codec"), "video is lossy");
        assert_eq!(resolve(r#"{"codec":"dimos-depth","delivery":"reliable"}"#).unwrap().as_deref(), Some("dimos-depth"));
        assert_eq!(resolve(r#"{}"#).unwrap(), None);
        let full = sub(r#"{"bandwidthPriority":2,"maxHz":20,"dangerousMinHz":1,"minQuality":0.3,"maxQuality":0.9,"qualityToHzTradeoff":0.7}"#).unwrap();
        assert_eq!(full.normalized()["bandwidthPriority"], 2.0);
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
