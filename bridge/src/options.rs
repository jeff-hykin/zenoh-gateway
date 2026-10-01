//! Data channel labels: `{"type":"sub"|"pub"|..., "key":..., "opts":{...}}`.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Label {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub key: String,
    /// Client-chosen id, echoed back in stats so the client can match channels.
    #[serde(default)]
    pub id: Option<u64>,
    #[serde(default)]
    pub opts: Opts,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Opts {
    #[serde(default)]
    pub delivery: Option<DeliveryOpt>,
    #[serde(default)]
    pub priority: Option<u8>,
    /// `[min, max]` in Hz; phase 1 only enforces max.
    #[serde(default)]
    pub hz: Option<Vec<f64>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum DeliveryOpt {
    Named(String),
    Custom {
        /// `null` (what JSON.stringify makes of Infinity) means unbounded.
        #[serde(default)]
        queue: Option<f64>,
        #[serde(default, rename = "maxAgeMs")]
        max_age_ms: Option<f64>,
    },
}

/// Normalized delivery policy.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Delivery {
    /// Max pending samples per key; `None` = unbounded.
    pub queue: Option<usize>,
    pub max_age_ms: Option<f64>,
    pub reliable: bool,
}

impl Delivery {
    pub const LATEST: Delivery = Delivery { queue: Some(1), max_age_ms: None, reliable: false };
    pub const RELIABLE: Delivery = Delivery { queue: None, max_age_ms: None, reliable: true };
}

impl Opts {
    pub fn delivery(&self) -> Delivery {
        match &self.delivery {
            None => Delivery::LATEST,
            Some(DeliveryOpt::Named(name)) if name == "reliable" => Delivery::RELIABLE,
            Some(DeliveryOpt::Named(_)) => Delivery::LATEST,
            Some(DeliveryOpt::Custom { queue, max_age_ms }) => {
                let max_age_ms = max_age_ms.filter(|ms| ms.is_finite() && *ms > 0.0);
                let queue = match queue {
                    Some(n) if n.is_finite() => Some((*n as usize).max(1)),
                    Some(_) => None,
                    // only an age bound given: let age do the dropping
                    None if max_age_ms.is_some() => None,
                    None => Some(1),
                };
                Delivery { queue, max_age_ms, reliable: false }
            }
        }
    }

    /// Minimum spacing between two sends of the same key, from `hz[1]`.
    pub fn min_interval(&self) -> Option<std::time::Duration> {
        let max_hz = *self.hz.as_ref()?.get(1)?;
        (max_hz.is_finite() && max_hz > 0.0).then(|| std::time::Duration::from_secs_f64(1.0 / max_hz))
    }

    pub fn zenoh_priority(&self) -> Option<zenoh::qos::Priority> {
        self.priority.and_then(|p| zenoh::qos::Priority::try_from(p).ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(opts: &str) -> Opts {
        serde_json::from_str(opts).unwrap()
    }

    #[test]
    fn delivery_forms() {
        assert_eq!(parse(r#"{}"#).delivery(), Delivery::LATEST);
        assert_eq!(parse(r#"{"delivery":"latest"}"#).delivery(), Delivery::LATEST);
        assert_eq!(parse(r#"{"delivery":"reliable"}"#).delivery(), Delivery::RELIABLE);
        let custom = parse(r#"{"delivery":{"queue":3,"maxAgeMs":500}}"#).delivery();
        assert_eq!(custom, Delivery { queue: Some(3), max_age_ms: Some(500.0), reliable: false });
        let unbounded = parse(r#"{"delivery":{"queue":null,"maxAgeMs":200}}"#).delivery();
        assert_eq!(unbounded.queue, None);
    }

    #[test]
    fn hz_cap() {
        let opts = parse(r#"{"hz":[1.0,20.0]}"#);
        assert_eq!(opts.min_interval(), Some(std::time::Duration::from_millis(50)));
        assert_eq!(parse(r#"{}"#).min_interval(), None);
    }
}
