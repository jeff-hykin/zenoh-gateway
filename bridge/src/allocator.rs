//! Per-frontend bandwidth allocation (SPEC "Bandwidth allocation").
//!
//! Every stream (subscription) wants `price(maxQuality) * maxHz` bytes/s, maxHz being its source rate capped by its
//! `maxHz` option. Reliable and strict streams can't drop messages: their measured rate is reserved. When the rest want
//! more than the budget left, they shrink like CSS flex items with `flex-shrink = demand / bandwidthPriority`; priority-0
//! streams give up everything first. A transcoded stream splits its grant between quality and Hz by `qualityToHzTradeoff`.

use serde::Serialize;

/// Bytes per message as a function of quality (constant for untranscoded streams).
pub type Price = Box<dyn Fn(f64) -> f64 + Send>;

pub struct Demand {
    /// bandwidthPriority: higher keeps more (shrink weight = demand / priority); 0 shrinks first
    pub priority: f64,
    /// messages/s wanted: source rate capped by maxHz, summed over keys
    pub max_hz: f64,
    /// `None` for streams without a transcoder (quality doesn't apply)
    pub quality_range: Option<(f64, f64)>,
    pub tradeoff: f64,
    pub price: Price,
    /// reliable and strict streams: their measured rate, reserved, never shrunk
    pub fixed_bytes_per_sec: Option<f64>,
}

impl Demand {
    /// Bytes/s this stream wants.
    pub fn wants(&self) -> f64 {
        self.fixed_bytes_per_sec.unwrap_or_else(|| (self.price)(self.quality_range.map_or(1.0, |(_, max)| max)) * self.max_hz)
    }
}

#[derive(Debug, Clone, Default, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Allocation {
    pub demand_bytes_per_sec: f64,
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

/// (quality, hz) for a transcoded stream granted `grant` bytes/s.
fn split(demand: &Demand, grant: f64, (min_quality, max_quality): (f64, f64)) -> (f64, f64) {
    let price = &demand.price;
    let full = price(max_quality) * demand.max_hz;
    if full <= 0.0 || grant >= full * (1.0 - 1e-9) {
        return (max_quality, demand.max_hz);
    }
    let candidates = quality_candidates(min_quality, max_quality);
    let ratio = (grant / full).clamp(0.0, 1.0);
    // tradeoff t: message size shrinks by ratio^t, Hz by ratio^(1-t); the best quality that fits, else the lowest allowed
    let bytes_per_message = price(max_quality) * ratio.powf(demand.tradeoff);
    let quality = candidates.iter().copied().find(|&q| price(q) <= bytes_per_message * (1.0 + 1e-9)).unwrap_or(min_quality);
    (quality, (grant / price(quality).max(1e-9)).min(demand.max_hz))
}

pub fn allocate(budget: f64, demands: &[Demand]) -> Vec<Allocation> {
    let wants: Vec<f64> = demands.iter().map(Demand::wants).collect();
    let mut grants = wants.clone();
    let mut over = wants.iter().sum::<f64>() - budget;
    // priority-0 streams first, then the rest by demand / priority; a stream stopping at 0 leaves the rest to the others
    for zero_priority_phase in [true, false] {
        let mut active: Vec<usize> = (0..demands.len())
            .filter(|&i| demands[i].fixed_bytes_per_sec.is_none() && grants[i] > 0.0 && (demands[i].priority == 0.0) == zero_priority_phase)
            .collect();
        let weight = |i: usize| if zero_priority_phase { wants[i] } else { wants[i] / demands[i].priority };
        while over > 1e-9 && !active.is_empty() {
            let total: f64 = active.iter().map(|&i| weight(i)).sum();
            let step = active.iter().map(|&i| grants[i] * total / weight(i)).fold(over, f64::min);
            for &i in &active {
                grants[i] = (grants[i] - step * weight(i) / total).max(0.0);
            }
            over -= step;
            active.retain(|&i| grants[i] > 1e-9);
        }
    }
    demands
        .iter()
        .zip(wants)
        .zip(grants)
        .map(|((demand, want), grant)| {
            let (quality, hz) = match (demand.quality_range, demand.fixed_bytes_per_sec) {
                (_, Some(_)) => (None, demand.max_hz),
                (Some(range), None) => {
                    let (quality, hz) = split(demand, grant, range);
                    (Some(quality), hz)
                }
                (None, None) => (None, (grant / (demand.price)(1.0).max(1e-9)).min(demand.max_hz)),
            };
            Allocation {
                demand_bytes_per_sec: want,
                budget_bytes_per_sec: grant,
                hz,
                hz_fraction: if demand.max_hz > 0.0 { (hz / demand.max_hz).min(1.0) } else { 1.0 },
                quality,
                constrained: grant < want * (1.0 - 1e-9),
            }
        })
        .collect()
}

/// Data-channel capacity estimate (bytes/s). webrtc-rs hides SCTP's congestion window, so this combines what the `sub`
/// channels pushed into SCTP with two congestion signals:
/// - delay: the RTT (the interval's smallest clock-sync sample) stayed above its recent minimum by more than the path's
///   usual jitter (`DELAY_THRESHOLD_MS` + twice the median delay over `JITTER_WINDOW`, so an idle jittery Wi-Fi or VPN
///   path doesn't read as congested) for `DELAY_PERSISTENCE` intervals: -15% at once, and probing pauses 1 s;
/// - backpressure: senders blocked on SCTP more than 20% of the interval: 0.9 x measured rate, at most -15% per interval.
///
/// Otherwise, while streams want more, it probes up 50% per interval until the first congestion event, then 10% below
/// 90% of the level that last triggered congestion and 2% above it (overshoot builds queue slowly enough to be caught),
/// and 10% again once that level is 5 s old.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Estimator {
    pub data_bytes_per_sec: f64,
    pub sent_bytes_per_sec: f64,
    pub network_blocked_fraction: f64,
    /// estimate at the last congestion event
    pub congested_at_bytes_per_sec: f64,
    pub delay_events: u64,
    /// queue delay that counts as congestion now (base threshold + 2 x the path's median jitter)
    pub delay_threshold_ms: f64,
    #[serde(skip)]
    hold_probing_until: Option<std::time::Instant>,
    #[serde(skip)]
    delay_samples: std::collections::VecDeque<(std::time::Instant, f64)>,
    #[serde(skip)]
    over_threshold: u32,
    #[serde(skip)]
    last_congestion: Option<std::time::Instant>,
}

pub const INITIAL_ESTIMATE: f64 = 1_000_000.0;
const MIN_ESTIMATE: f64 = 16_000.0;
const MAX_ESTIMATE: f64 = 2e9;
const CONGESTED_FRACTION: f64 = 0.2;
const SLOW_START_GAIN: f64 = 1.5;
const FAST_PROBE_GAIN: f64 = 1.10;
const SLOW_PROBE_GAIN: f64 = 1.02;
/// RTT above its recent minimum by this much means a queue is building.
pub const DELAY_THRESHOLD_MS: f64 = 5.0;
/// Intervals in a row above the threshold before the delay counts as a queue (a lone spike doesn't).
const DELAY_PERSISTENCE: u32 = 2;
/// How far back the path's usual queue-delay jitter is measured.
const JITTER_WINDOW: std::time::Duration = std::time::Duration::from_secs(30);
const DELAY_DECREASE: f64 = 0.85;
/// Blocked on SCTP: down to 0.9 x what went out, but at most this per interval (a long RTT blocks the first intervals).
const BLOCKED_DECREASE: f64 = 0.85;
/// Slow probing (2%) near the last congestion level only while that level is this recent (one early stall held it for good).
const STALE_CONGESTION: std::time::Duration = std::time::Duration::from_secs(5);
const HOLD_AFTER_CONGESTION: std::time::Duration = std::time::Duration::from_secs(1);

impl Default for Estimator {
    fn default() -> Self {
        Estimator {
            data_bytes_per_sec: INITIAL_ESTIMATE,
            sent_bytes_per_sec: 0.0,
            network_blocked_fraction: 0.0,
            congested_at_bytes_per_sec: f64::INFINITY,
            delay_events: 0,
            delay_threshold_ms: DELAY_THRESHOLD_MS,
            hold_probing_until: None,
            delay_samples: std::collections::VecDeque::new(),
            over_threshold: 0,
            last_congestion: None,
        }
    }
}

/// What the frontend's streams did during one allocation interval.
#[derive(Debug, Clone, Copy)]
pub struct Interval {
    pub now: std::time::Instant,
    pub secs: f64,
    pub sent_bytes: f64,
    /// time senders waited on SCTP, summed over `active_senders` channels
    pub blocked_secs: f64,
    pub active_senders: usize,
    pub data_demand: f64,
    /// the interval's smallest RTT minus the recent minimum, if any sample came in
    pub queue_delay_ms: Option<f64>,
}

impl Estimator {
    pub fn update(&mut self, interval: Interval) {
        let Interval { now, secs: interval_secs, sent_bytes, blocked_secs, active_senders, data_demand, queue_delay_ms } = interval;
        if interval_secs <= 0.0 {
            return;
        }
        self.sent_bytes_per_sec = sent_bytes / interval_secs;
        self.network_blocked_fraction = (blocked_secs / (interval_secs * active_senders.max(1) as f64)).min(1.0);
        let holding = self.hold_probing_until.is_some_and(|until| now < until);
        let queued = self.queue_building(now, queue_delay_ms);
        if queued && !holding {
            self.congested_at_bytes_per_sec = self.data_bytes_per_sec;
            self.last_congestion = Some(now);
            self.data_bytes_per_sec *= DELAY_DECREASE;
            self.delay_events += 1;
            self.hold_probing_until = Some(now + HOLD_AFTER_CONGESTION);
        } else if self.network_blocked_fraction > CONGESTED_FRACTION {
            self.congested_at_bytes_per_sec = self.data_bytes_per_sec;
            self.last_congestion = Some(now);
            self.data_bytes_per_sec = (0.9 * self.sent_bytes_per_sec).max(self.data_bytes_per_sec * BLOCKED_DECREASE);
        } else if !holding && data_demand > self.data_bytes_per_sec * 0.95 {
            let stale = self.last_congestion.is_some_and(|at| now.duration_since(at) > STALE_CONGESTION);
            let gain = if self.congested_at_bytes_per_sec.is_infinite() {
                SLOW_START_GAIN
            } else if self.data_bytes_per_sec > 0.9 * self.congested_at_bytes_per_sec && !stale {
                SLOW_PROBE_GAIN
            } else {
                FAST_PROBE_GAIN
            };
            self.data_bytes_per_sec *= gain;
        }
        self.data_bytes_per_sec = self.data_bytes_per_sec.clamp(MIN_ESTIMATE, MAX_ESTIMATE);
    }

    /// Whether this interval's queue delay, and enough before it, stood above the path's jitter.
    fn queue_building(&mut self, now: std::time::Instant, queue_delay_ms: Option<f64>) -> bool {
        let Some(delay) = queue_delay_ms else { return false };
        while self.delay_samples.front().is_some_and(|(at, _)| now.duration_since(*at) > JITTER_WINDOW) {
            self.delay_samples.pop_front();
        }
        let mut recent: Vec<f64> = self.delay_samples.iter().map(|(_, delay)| *delay).collect();
        // the path's own swing: twice its median queue delay (a median, so a queue that stands for
        // less than half the window can't raise the bar it is measured against)
        recent.sort_by(f64::total_cmp);
        self.delay_threshold_ms = DELAY_THRESHOLD_MS + recent.get(recent.len() / 2).map_or(0.0, |median| 2.0 * median);
        self.delay_samples.push_back((now, delay));
        self.over_threshold = if delay > self.delay_threshold_ms { self.over_threshold + 1 } else { 0 };
        self.over_threshold >= DELAY_PERSISTENCE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(priority: f64, max_hz: f64, bytes: f64) -> Demand {
        Demand { priority, max_hz, quality_range: None, tradeoff: 0.5, price: Box::new(move |_| bytes), fixed_bytes_per_sec: None }
    }

    fn transcoded(tradeoff: f64, max_hz: f64) -> Demand {
        // price grows with quality: 100 bytes at q=0 .. 1100 at q=1
        Demand { priority: 1.0, max_hz, quality_range: Some((0.0, 1.0)), tradeoff, price: Box::new(|q| 100.0 + 1000.0 * q), fixed_bytes_per_sec: None }
    }

    #[test]
    fn enough_budget_grants_demand() {
        let allocations = allocate(1e6, &[raw(1.0, 20.0, 1000.0), raw(5.0, 10.0, 500.0)]);
        assert!(allocations.iter().all(|a| !a.constrained && a.hz_fraction == 1.0));
        assert_eq!(allocations[0].hz, 20.0);
    }

    #[test]
    fn equal_priorities_shrink_by_the_same_fraction() {
        let allocations = allocate(600.0, &[raw(1.0, 10.0, 100.0), raw(1.0, 10.0, 20.0)]);
        assert!((allocations[0].hz_fraction - 0.5).abs() < 1e-9 && (allocations[1].hz_fraction - 0.5).abs() < 1e-9);
        assert!(allocations.iter().all(|a| a.constrained));
    }

    #[test]
    fn higher_priority_keeps_more() {
        // three 400 KB/s streams, budget 480 KB/s: the priority-0.1 ones take 100x the cut of the priority-10 one
        let demands = [raw(10.0, 20.0, 20_000.0), raw(0.1, 20.0, 20_000.0), raw(0.1, 20.0, 20_000.0)];
        let allocations = allocate(480_000.0, &demands);
        let total: f64 = allocations.iter().map(|a| a.budget_bytes_per_sec).sum();
        assert!((total - 480_000.0).abs() < 1.0, "{total}");
        assert!(allocations[0].hz > 19.5 && allocations[1].hz < 2.2, "{allocations:?}");
        // tighter: the priority-0.1 streams reach 0, the rest comes from the priority-10 one
        let allocations = allocate(300_000.0, &demands);
        assert_eq!((allocations[1].hz, allocations[2].hz), (0.0, 0.0));
        assert!((allocations[0].hz - 15.0).abs() < 1e-6, "{:?}", allocations[0]);
    }

    #[test]
    fn zero_priority_shrinks_first() {
        let allocations = allocate(1000.0, &[raw(0.0, 10.0, 100.0), raw(1.0, 10.0, 100.0)]);
        assert_eq!((allocations[0].hz, allocations[1].hz), (0.0, 10.0));
        let allocations = allocate(500.0, &[raw(0.0, 10.0, 100.0), raw(1.0, 10.0, 100.0)]);
        assert_eq!((allocations[0].hz, allocations[1].hz), (0.0, 5.0));
    }

    #[test]
    fn reliable_streams_are_reserved_even_over_budget() {
        let reliable = Demand { fixed_bytes_per_sec: Some(800.0), ..raw(1.0, 10.0, 80.0) };
        let allocations = allocate(1000.0, &[reliable, raw(1.0, 10.0, 100.0)]);
        assert_eq!(allocations[0].budget_bytes_per_sec, 800.0);
        assert!((allocations[1].budget_bytes_per_sec - 200.0).abs() < 1e-6);
        let reliable = Demand { fixed_bytes_per_sec: Some(800.0), ..raw(1.0, 10.0, 80.0) };
        let allocations = allocate(500.0, &[reliable, raw(1.0, 10.0, 100.0)]);
        assert_eq!((allocations[0].budget_bytes_per_sec, allocations[1].budget_bytes_per_sec), (800.0, 0.0));
    }

    #[test]
    fn tradeoff_zero_keeps_quality_one_keeps_hz() {
        // wants 1100 * 10 = 11000 B/s; grant 5500 (half)
        let keep_quality = &allocate(5500.0, &[transcoded(0.0, 10.0)])[0];
        assert_eq!(keep_quality.quality, Some(1.0));
        assert!((keep_quality.hz - 5.0).abs() < 1e-9, "{keep_quality:?}");
        let keep_hz = &allocate(5500.0, &[transcoded(1.0, 10.0)])[0];
        assert_eq!(keep_hz.hz, 10.0);
        assert!(keep_hz.quality.unwrap() <= 0.45 && keep_hz.quality.unwrap() >= 0.3, "{keep_hz:?}");
        // quality bottoms out, then Hz gives
        let starved = &allocate(500.0, &[transcoded(1.0, 10.0)])[0];
        assert_eq!(starved.quality, Some(0.0));
        assert!((starved.hz - 5.0).abs() < 1e-9);
    }

    fn sample(now: std::time::Instant, secs: f64, sent_bytes: f64, blocked_secs: f64, active_senders: usize, data_demand: f64, queue_delay_ms: Option<f64>) -> Interval {
        Interval { now, secs, sent_bytes, blocked_secs, active_senders, data_demand, queue_delay_ms }
    }

    #[test]
    fn estimator_backs_off_when_blocked_and_probes_when_wanted() {
        let now = std::time::Instant::now();
        let mut estimator = Estimator::default();
        estimator.update(sample(now, 0.25, 100_000.0, 0.2, 1, 5e6, None));
        assert!((estimator.data_bytes_per_sec - 850_000.0).abs() < 1.0, "down 15% at most per step: {estimator:?}");
        estimator.update(sample(now, 0.25, 100_000.0, 0.2, 1, 5e6, None));
        assert!((estimator.data_bytes_per_sec - 722_500.0).abs() < 1.0, "{estimator:?}");
        estimator.update(sample(now, 0.25, 180_000.0, 0.2, 1, 5e6, None));
        assert!((estimator.data_bytes_per_sec - 648_000.0).abs() < 1.0, "0.9 x 720 KB/s measured: {estimator:?}");
        estimator.update(sample(now, 0.25, 50_000.0, 0.0, 1, 5e6, None));
        assert!((estimator.data_bytes_per_sec - 712_800.0).abs() < 1.0, "probes fast below 90% of the last congestion level (722.5 KB/s): {estimator:?}");
        estimator.update(sample(now, 0.25, 50_000.0, 0.0, 1, 5e6, None));
        assert!((estimator.data_bytes_per_sec - 712_800.0 * 1.02).abs() < 1.0, "slowly around it: {estimator:?}");
        estimator.update(sample(now, 0.25, 50_000.0, 0.0, 1, 1000.0, None));
        assert!((estimator.data_bytes_per_sec - 712_800.0 * 1.02).abs() < 1.0, "no probing without demand");
        // 5 s after the last congestion event its level is stale: fast again
        let later = now + STALE_CONGESTION + std::time::Duration::from_millis(1);
        let before = estimator.data_bytes_per_sec;
        estimator.update(sample(later, 0.25, 50_000.0, 0.0, 1, 5e6, None));
        assert!((estimator.data_bytes_per_sec - before * 1.1).abs() < 1.0, "{estimator:?}");
    }

    #[test]
    fn delay_cuts_and_holds() {
        let now = std::time::Instant::now();
        let at = |interval: u64| now + std::time::Duration::from_millis(250 * interval);
        let mut estimator = Estimator::default();
        // a quiet path (1-2 ms of queue delay), demand capped so the estimate settles
        for interval in 0..40 {
            estimator.update(sample(at(interval), 0.25, 200_000.0, 0.0, 1, 1000.0, Some(1.0 + (interval % 2) as f64)));
        }
        let settled = estimator.data_bytes_per_sec;
        estimator.update(sample(at(41), 0.25, 200_000.0, 0.0, 1, 1000.0, Some(25.0)));
        assert_eq!(estimator.data_bytes_per_sec, settled, "one spike is not a queue: {estimator:?}");
        estimator.update(sample(at(42), 0.25, 200_000.0, 0.0, 1, 1000.0, Some(40.0)));
        assert!((estimator.data_bytes_per_sec - settled * 0.85).abs() < 1.0, "a second in a row is: cut by 15%: {estimator:?}");
        assert_eq!(estimator.delay_events, 1);
        estimator.update(sample(at(43), 0.25, 200_000.0, 0.0, 1, 5e6, Some(1.0)));
        assert!((estimator.data_bytes_per_sec - settled * 0.85).abs() < 1.0, "holds while the queue drains");
        estimator.update(sample(at(47), 0.25, 200_000.0, 0.0, 1, 5e6, Some(1.0)));
        assert!((estimator.data_bytes_per_sec - settled * 0.85 * 1.1).abs() < 1.0, "then probes again, 10% below 90% of the congestion level: {estimator:?}");
    }

    #[test]
    fn a_jittery_idle_path_is_not_congestion() {
        // RTT excess swinging between 5 and 90 ms with nothing queued (seen on Wi-Fi + VPN)
        let now = std::time::Instant::now();
        let mut estimator = Estimator::default();
        let swings = [10.0, 74.0, 18.0, 5.0, 27.0, 32.0, 9.0, 56.0, 10.0, 28.0, 10.0, 45.0, 22.0, 2.0, 73.0, 22.0, 17.0, 11.0, 15.0, 68.0, 7.0, 23.0];
        for (index, delay) in swings.iter().cycle().take(120).enumerate() {
            estimator.update(sample(now + std::time::Duration::from_millis(250 * index as u64), 0.25, 20_000.0, 0.0, 1, 2e6, Some(*delay)));
        }
        assert!(estimator.delay_events <= 3, "{} delay events", estimator.delay_events);
        assert!(estimator.data_bytes_per_sec >= 1_000_000.0, "the estimate stays up: {estimator:?}");
        assert!(estimator.delay_threshold_ms > 30.0, "{estimator:?}");
    }
}
