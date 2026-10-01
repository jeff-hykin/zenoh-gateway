//! Per-frontend bandwidth allocation (SPEC "Bandwidth allocation").
//!
//! Every stream (subscription) wants `price(maxQuality) * maxHz` bytes/s, where maxHz is its
//! source rate capped by its `maxHz` option. When the sum exceeds the frontend's budget, streams
//! shrink like CSS flex items with `flex-shrink = bandwidthPriority` (scaled by what they want),
//! none below its floor `price(minQuality) * dangerousMinHz`; a stream that hits its floor freezes
//! there and the rest shrink further. Zero-weight streams shrink only once nothing else can.
//! Floors are honored even when they add up to more than the budget ("dangerous").
//! A transcoded stream then splits its grant between quality and Hz along `qualityToHzTradeoff`.

use serde::Serialize;

/// Bytes per message as a function of quality (constant for untranscoded streams).
pub type Price = Box<dyn Fn(f64) -> f64 + Send>;

pub struct Demand {
    /// bandwidthPriority: flex-shrink weight (0 = shrink last)
    pub weight: f64,
    /// messages/s wanted: source rate capped by maxHz, summed over keys
    pub max_hz: f64,
    /// dangerousMinHz floor, summed over keys (never above max_hz)
    pub min_hz: f64,
    /// `None` for streams without a transcoder (quality doesn't apply)
    pub quality_range: Option<(f64, f64)>,
    pub tradeoff: f64,
    pub price: Price,
    /// reliable streams can't drop messages: their measured rate is reserved, never shrunk
    pub fixed_bytes_per_sec: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Allocation {
    pub demand_bytes_per_sec: f64,
    pub floor_bytes_per_sec: f64,
    pub budget_bytes_per_sec: f64,
    /// granted messages/s summed over keys
    pub hz: f64,
    /// granted / wanted Hz; each key's rate is scaled by this
    pub hz_fraction: f64,
    pub quality: Option<f64>,
    /// the frontend's budget was short and this stream was shrunk
    pub constrained: bool,
}

/// Quality candidates from best to worst: the bounds plus the 0.1 steps between them.
fn quality_candidates(min_quality: f64, max_quality: f64) -> Vec<f64> {
    let mut candidates = vec![max_quality];
    candidates.extend((0..=10).rev().map(|step| step as f64 / 10.0).filter(|&q| q < max_quality - 1e-9 && q > min_quality + 1e-9));
    if min_quality < max_quality {
        candidates.push(min_quality);
    }
    candidates
}

/// Best quality whose price fits `bytes_per_message`, else the lowest allowed.
fn best_quality_within(price: &Price, candidates: &[f64], bytes_per_message: f64) -> f64 {
    candidates.iter().copied().find(|&q| price(q) <= bytes_per_message * (1.0 + 1e-9)).unwrap_or(*candidates.last().unwrap())
}

/// (quality, hz) for a transcoded stream granted `grant` bytes/s.
fn split(demand: &Demand, grant: f64, (min_quality, max_quality): (f64, f64)) -> (f64, f64) {
    let price = &demand.price;
    let full = price(max_quality) * demand.max_hz;
    if full <= 0.0 || grant >= full * (1.0 - 1e-9) {
        return (max_quality, demand.max_hz);
    }
    let candidates = quality_candidates(min_quality, max_quality);
    let ratio = (grant / full).clamp(0.0, 1.0);
    // tradeoff t: message size shrinks by ratio^t, Hz by ratio^(1-t)
    let mut quality = best_quality_within(price, &candidates, price(max_quality) * ratio.powf(demand.tradeoff));
    let mut hz = (grant / price(quality).max(1e-9)).min(demand.max_hz);
    if hz < demand.min_hz {
        hz = demand.min_hz;
        quality = best_quality_within(price, &candidates, grant / hz.max(1e-9));
    }
    (quality, hz)
}

pub fn allocate(budget: f64, demands: &[Demand]) -> Vec<Allocation> {
    let wants: Vec<f64> = demands
        .iter()
        .map(|d| d.fixed_bytes_per_sec.unwrap_or_else(|| (d.price)(d.quality_range.map_or(1.0, |(_, max)| max)) * d.max_hz))
        .collect();
    let floors: Vec<f64> = demands
        .iter()
        .zip(&wants)
        .map(|(d, &want)| match d.fixed_bytes_per_sec {
            Some(fixed) => fixed,
            None => ((d.price)(d.quality_range.map_or(1.0, |(min, _)| min)) * d.min_hz).min(want),
        })
        .collect();
    let mut grants = wants.clone();
    let mut frozen: Vec<bool> = floors.iter().zip(&wants).map(|(floor, want)| floor >= want).collect();
    let constrained = wants.iter().sum::<f64>() > budget;
    if constrained {
        loop {
            let over = grants.iter().sum::<f64>() - budget;
            if over <= 1e-9 {
                break;
            }
            let shrink_weights = |zero_weight_phase: bool| -> Vec<f64> {
                demands
                    .iter()
                    .enumerate()
                    .map(|(i, d)| {
                        if frozen[i] {
                            0.0
                        } else if zero_weight_phase {
                            if d.weight == 0.0 { wants[i] } else { 0.0 }
                        } else {
                            d.weight * wants[i]
                        }
                    })
                    .collect()
            };
            let mut weights = shrink_weights(false);
            if weights.iter().sum::<f64>() <= 0.0 {
                weights = shrink_weights(true);
            }
            let total_weight: f64 = weights.iter().sum();
            if total_weight <= 0.0 {
                // everyone is at a floor: floors exceed the budget
                break;
            }
            let mut froze_any = false;
            for i in 0..demands.len() {
                if weights[i] > 0.0 && grants[i] - over * weights[i] / total_weight <= floors[i] {
                    grants[i] = floors[i];
                    frozen[i] = true;
                    froze_any = true;
                }
            }
            if !froze_any {
                for i in 0..demands.len() {
                    grants[i] -= over * weights[i] / total_weight;
                }
                break;
            }
        }
    }
    demands
        .iter()
        .enumerate()
        .map(|(i, demand)| {
            let shrunk = grants[i] < wants[i] * (1.0 - 1e-9);
            let (quality, hz) = match (demand.quality_range, demand.fixed_bytes_per_sec) {
                (_, Some(_)) => (None, demand.max_hz),
                (Some(range), None) => {
                    let (quality, hz) = split(demand, grants[i], range);
                    (Some(quality), hz)
                }
                (None, None) => (None, (grants[i] / (demand.price)(1.0).max(1e-9)).min(demand.max_hz).max(demand.min_hz)),
            };
            Allocation {
                demand_bytes_per_sec: wants[i],
                floor_bytes_per_sec: floors[i],
                budget_bytes_per_sec: grants[i],
                hz,
                hz_fraction: if demand.max_hz > 0.0 { (hz / demand.max_hz).min(1.0) } else { 1.0 },
                quality,
                constrained: shrunk,
            }
        })
        .collect()
}

/// Data-channel capacity estimate (bytes/s). webrtc-rs doesn't expose the SCTP congestion window,
/// so this combines what the `sub` channels actually pushed into SCTP with two congestion signals:
/// - delay: the connection's RTT (the smallest browser clock-sync sample of the interval) rose
///   `DELAY_THRESHOLD_MS` above its recent minimum, i.e. a queue is building somewhere on the path.
///   The estimate drops at once by 15% and probing pauses 1 s while the queue drains;
/// - loss/backpressure: senders blocked on SCTP more than 20% of the interval: 0.9 x measured rate.
///
/// Otherwise, while streams want more, it probes up: 10% per interval below 90% of the level that
/// last triggered congestion, 2% above it, so overshoot builds queue slowly enough to be caught.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Estimator {
    pub data_bytes_per_sec: f64,
    pub sent_bytes_per_sec: f64,
    pub network_blocked_fraction: f64,
    /// estimate at the last congestion event
    pub congested_at_bytes_per_sec: f64,
    pub delay_events: u64,
    #[serde(skip)]
    hold_probing_until: Option<std::time::Instant>,
}

pub const INITIAL_ESTIMATE: f64 = 1_000_000.0;
const MIN_ESTIMATE: f64 = 16_000.0;
const MAX_ESTIMATE: f64 = 2e9;
const CONGESTED_FRACTION: f64 = 0.2;
const FAST_PROBE_GAIN: f64 = 1.10;
const SLOW_PROBE_GAIN: f64 = 1.02;
/// RTT above its recent minimum by this much means a queue is building.
pub const DELAY_THRESHOLD_MS: f64 = 10.0;
const DELAY_DECREASE: f64 = 0.85;
const HOLD_AFTER_CONGESTION: std::time::Duration = std::time::Duration::from_secs(1);

impl Default for Estimator {
    fn default() -> Self {
        Estimator {
            data_bytes_per_sec: INITIAL_ESTIMATE,
            sent_bytes_per_sec: 0.0,
            network_blocked_fraction: 0.0,
            congested_at_bytes_per_sec: f64::INFINITY,
            delay_events: 0,
            hold_probing_until: None,
        }
    }
}

impl Estimator {
    /// `blocked_secs` is summed over `active_senders` channels during `interval_secs`;
    /// `queue_delay_ms` is the latest RTT minus the recent minimum, if known.
    pub fn update(&mut self, now: std::time::Instant, interval_secs: f64, sent_bytes: f64, blocked_secs: f64, active_senders: usize, data_demand: f64, queue_delay_ms: Option<f64>) {
        if interval_secs <= 0.0 {
            return;
        }
        self.sent_bytes_per_sec = sent_bytes / interval_secs;
        self.network_blocked_fraction = (blocked_secs / (interval_secs * active_senders.max(1) as f64)).min(1.0);
        let holding = self.hold_probing_until.is_some_and(|until| now < until);
        if queue_delay_ms.is_some_and(|delay| delay > DELAY_THRESHOLD_MS) && !holding {
            self.congested_at_bytes_per_sec = self.data_bytes_per_sec;
            self.data_bytes_per_sec *= DELAY_DECREASE;
            self.delay_events += 1;
            self.hold_probing_until = Some(now + HOLD_AFTER_CONGESTION);
        } else if self.network_blocked_fraction > CONGESTED_FRACTION {
            self.congested_at_bytes_per_sec = self.data_bytes_per_sec;
            self.data_bytes_per_sec = (0.9 * self.sent_bytes_per_sec).max(self.data_bytes_per_sec / 2.0);
        } else if !holding && data_demand > self.data_bytes_per_sec * 0.95 {
            let near_last_congestion = self.data_bytes_per_sec > 0.9 * self.congested_at_bytes_per_sec;
            self.data_bytes_per_sec *= if near_last_congestion { SLOW_PROBE_GAIN } else { FAST_PROBE_GAIN };
        }
        self.data_bytes_per_sec = self.data_bytes_per_sec.clamp(MIN_ESTIMATE, MAX_ESTIMATE);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(weight: f64, max_hz: f64, min_hz: f64, bytes: f64) -> Demand {
        Demand { weight, max_hz, min_hz, quality_range: None, tradeoff: 0.5, price: Box::new(move |_| bytes), fixed_bytes_per_sec: None }
    }

    fn transcoded(tradeoff: f64, max_hz: f64, min_hz: f64) -> Demand {
        // price grows with quality: 100 bytes at q=0 .. 1100 at q=1
        Demand { weight: 1.0, max_hz, min_hz, quality_range: Some((0.0, 1.0)), tradeoff, price: Box::new(|q| 100.0 + 1000.0 * q), fixed_bytes_per_sec: None }
    }

    #[test]
    fn enough_budget_grants_demand() {
        let allocations = allocate(1e6, &[raw(1.0, 20.0, 0.0, 1000.0), raw(5.0, 10.0, 1.0, 500.0)]);
        assert!(allocations.iter().all(|a| !a.constrained && a.hz_fraction == 1.0));
        assert_eq!(allocations[0].hz, 20.0);
    }

    #[test]
    fn high_shrink_streams_hit_their_floor_before_the_low_shrink_stream_gives_much() {
        // three streams of 20 KB messages at 20 Hz (400 KB/s each), budget 480 KB/s
        let demands = [raw(0.1, 20.0, 2.0, 20_000.0), raw(10.0, 20.0, 2.0, 20_000.0), raw(10.0, 20.0, 2.0, 20_000.0)];
        let allocations = allocate(480_000.0, &demands);
        let total: f64 = allocations.iter().map(|a| a.budget_bytes_per_sec).sum();
        assert!((total - 480_000.0).abs() < 1.0, "{total}");
        assert!(allocations[0].hz > 19.5, "low-shrink stream keeps its rate: {:?}", allocations[0]);
        assert!(allocations[1].hz < 2.2 && allocations[1].hz >= 2.0, "{:?}", allocations[1]);
        // tighter: high-shrink streams freeze at the floor, the rest comes from the low-shrink one
        let allocations = allocate(300_000.0, &demands);
        assert_eq!(allocations[1].hz, 2.0);
        assert_eq!(allocations[2].hz, 2.0);
        assert!((allocations[0].hz - 11.0).abs() < 1e-6, "{:?}", allocations[0]);
    }

    #[test]
    fn equal_weights_shrink_by_the_same_fraction() {
        let allocations = allocate(600.0, &[raw(1.0, 10.0, 0.0, 100.0), raw(1.0, 10.0, 0.0, 20.0)]);
        assert!((allocations[0].hz_fraction - 0.5).abs() < 1e-9 && (allocations[1].hz_fraction - 0.5).abs() < 1e-9);
    }

    #[test]
    fn zero_weight_shrinks_last_and_floors_can_exceed_the_budget() {
        let allocations = allocate(1000.0, &[raw(0.0, 10.0, 0.0, 100.0), raw(1.0, 10.0, 1.0, 100.0)]);
        assert_eq!(allocations[0].hz, 9.0);
        assert_eq!(allocations[1].hz, 1.0);
        let allocations = allocate(50.0, &[raw(1.0, 10.0, 2.0, 100.0), raw(1.0, 10.0, 2.0, 100.0)]);
        assert!(allocations.iter().all(|a| a.hz == 2.0), "dangerousMinHz is honored even over budget");
    }

    #[test]
    fn reliable_streams_are_reserved() {
        let reliable = Demand { fixed_bytes_per_sec: Some(800.0), ..raw(1.0, 10.0, 0.0, 80.0) };
        let allocations = allocate(1000.0, &[reliable, raw(1.0, 10.0, 0.0, 100.0)]);
        assert_eq!(allocations[0].budget_bytes_per_sec, 800.0);
        assert!((allocations[1].budget_bytes_per_sec - 200.0).abs() < 1e-6);
    }

    #[test]
    fn tradeoff_zero_keeps_quality_one_keeps_hz() {
        // wants 1100 * 10 = 11000 B/s; grant 5500 (half)
        let keep_quality = &allocate(5500.0, &[transcoded(0.0, 10.0, 1.0)])[0];
        assert_eq!(keep_quality.quality, Some(1.0));
        assert!((keep_quality.hz - 5.0).abs() < 1e-9, "{keep_quality:?}");
        let keep_hz = &allocate(5500.0, &[transcoded(1.0, 10.0, 1.0)])[0];
        assert_eq!(keep_hz.hz, 10.0);
        assert!(keep_hz.quality.unwrap() <= 0.45 && keep_hz.quality.unwrap() >= 0.3, "{keep_hz:?}");
        // quality bottoms out, then Hz gives
        let starved = &allocate(500.0, &[transcoded(1.0, 10.0, 1.0)])[0];
        assert_eq!(starved.quality, Some(0.0));
        assert!((starved.hz - 5.0).abs() < 1e-9);
        // Hz bottoms out at its floor, then quality gives (tradeoff 0)
        let floored = &allocate(500.0, &[transcoded(0.0, 10.0, 1.0)])[0];
        assert_eq!(floored.hz, 1.0);
        assert_eq!(floored.quality, Some(0.4));
    }

    #[test]
    fn estimator_backs_off_when_blocked_and_probes_when_wanted() {
        let now = std::time::Instant::now();
        let mut estimator = Estimator::default();
        estimator.update(now, 0.25, 100_000.0, 0.2, 1, 5e6, None);
        assert!((estimator.data_bytes_per_sec - 500_000.0).abs() < 1.0, "halved at most per step: {estimator:?}");
        estimator.update(now, 0.25, 100_000.0, 0.2, 1, 5e6, None);
        assert!((estimator.data_bytes_per_sec - 360_000.0).abs() < 1.0, "0.9 x 400 KB/s measured: {estimator:?}");
        estimator.update(now, 0.25, 50_000.0, 0.0, 1, 5e6, None);
        assert!((estimator.data_bytes_per_sec - 396_000.0).abs() < 1.0, "probes fast below 90% of the last congestion level (500 KB/s): {estimator:?}");
        estimator.update(now, 0.25, 50_000.0, 0.0, 1, 5e6, None);
        assert!((estimator.data_bytes_per_sec - 435_600.0).abs() < 1.0, "{estimator:?}");
        estimator.update(now, 0.25, 50_000.0, 0.0, 1, 5e6, None);
        assert!((estimator.data_bytes_per_sec - 479_160.0).abs() < 1.0, "{estimator:?}");
        estimator.update(now, 0.25, 50_000.0, 0.0, 1, 5e6, None);
        assert!((estimator.data_bytes_per_sec - 479_160.0 * 1.02).abs() < 1.0, "slowly above 90% of it: {estimator:?}");
        estimator.update(now, 0.25, 50_000.0, 0.0, 1, 1000.0, None);
        assert!((estimator.data_bytes_per_sec - 479_160.0 * 1.02).abs() < 1.0, "no probing without demand");
    }

    #[test]
    fn delay_cuts_and_holds() {
        let now = std::time::Instant::now();
        let mut estimator = Estimator::default();
        estimator.update(now, 0.25, 200_000.0, 0.0, 1, 5e6, Some(3.0));
        assert!((estimator.data_bytes_per_sec - 1_100_000.0).abs() < 1.0, "small RTT noise: keeps probing fast");
        estimator.update(now, 0.25, 200_000.0, 0.0, 1, 5e6, Some(25.0));
        assert!((estimator.data_bytes_per_sec - 935_000.0).abs() < 1.0, "cut by 15%: {estimator:?}");
        assert_eq!(estimator.delay_events, 1);
        estimator.update(now + std::time::Duration::from_millis(250), 0.25, 200_000.0, 0.0, 1, 5e6, Some(25.0));
        assert!((estimator.data_bytes_per_sec - 935_000.0).abs() < 1.0, "holds while the queue drains");
        estimator.update(now + std::time::Duration::from_millis(1100), 0.25, 200_000.0, 0.0, 1, 5e6, Some(1.0));
        assert!((estimator.data_bytes_per_sec - 1_028_500.0).abs() < 1.0, "then probes again, fast below 90% of the congestion level: {estimator:?}");
    }
}
