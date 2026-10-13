//! Per-frontend bandwidth allocation (SPEC "Bandwidth allocation").
//!
//! Every stream (subscription) wants `price(maxQuality) * maxHz` bytes/s, maxHz being its source rate capped by its
//! `maxHz` option. Strict streams' measured rate is reserved. When the rest want more than the budget left, they shrink
//! like CSS flex items with `flex-shrink = demand / bandwidthPriority`; priority-0 streams give up everything first,
//! reliable streams only once every other has given up everything (a reliable tf at 100 Hz, reserved, was twice a
//! 100 kbit/s link and took the strict streams down with it). A transcoded stream splits its grant between quality and Hz by `qualityToHzTradeoff`.

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
    /// strict streams: their measured rate, reserved, never shrunk
    pub fixed_bytes_per_sec: Option<f64>,
    /// reliable streams: shrunk only once every other stream is at 0
    pub shrinks_last: bool,
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
    // priority-0 streams first, then the rest by demand / priority, then reliable ones; a stream stopping at 0 leaves the
    // rest to the others
    #[derive(PartialEq, Clone, Copy)]
    enum Phase {
        ZeroPriority,
        Rest,
        Reliable,
    }
    let phase_of = |demand: &Demand| match (demand.shrinks_last, demand.priority == 0.0) {
        (true, _) => Phase::Reliable,
        (false, true) => Phase::ZeroPriority,
        (false, false) => Phase::Rest,
    };
    for phase in [Phase::ZeroPriority, Phase::Rest, Phase::Reliable] {
        let mut active: Vec<usize> =
            (0..demands.len()).filter(|&i| demands[i].fixed_bytes_per_sec.is_none() && grants[i] > 0.0 && phase_of(&demands[i]) == phase).collect();
        let weight = |i: usize| if demands[i].priority == 0.0 { wants[i] } else { wants[i] / demands[i].priority };
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

/// The frontend's link capacity estimate (bytes/s), one number for its video and data together (two estimates of one
/// link, added, let the allocator hand out 2-36x what a slow link carried, so nothing was ever squeezed). webrtc-rs hides
/// SCTP's congestion window, so this combines what every stream sent with three congestion signals:
/// - delay: the RTT (the interval's smallest clock-sync sample) stayed above its recent minimum by more than the path's
///   usual jitter (`DELAY_THRESHOLD_MS` + the spread of its queue delays over `JITTER_WINDOW`, p90 - p10, at most
///   `MAX_JITTER_ALLOWANCE_MS`, so an idle jittery Wi-Fi or VPN path doesn't read as congested) for `DELAY_PERSISTENCE`
///   intervals: -15% at once, and probing pauses 1 s. A spread, not a level: a standing queue learned as jitter (twice
///   the median) raised the threshold from 70 ms to 1.5 s while the queue grew;
/// - backpressure: senders blocked on SCTP more than 20% of the interval: 0.9 x measured rate, at most -15% per interval;
/// - video overuse: Google Congestion Control (on the video tracks' transport feedback) puts the video below what video
///   sent: the same cut.
///   Waiting on the page (its acks) is reported (`page_blocked_fraction`) and doesn't lower the estimate: a page is slow
///   per message (a big map holds it for a while whatever the rate), so it would drive the estimate down without bound;
///   the delivery-lag limit in `subscription::Pacer` keeps its backlog short instead. It does stop probing, since
///   senders held back by the page aren't testing the network.
///
/// Otherwise, while streams want more and send at least `PROBE_UTILIZATION` of the estimate (an estimate only grows on
/// a link that carried it: probing while idle made up 4 MB/s), it probes up 50% per interval until the first congestion
/// event, then 10% below 90% of the level that last triggered congestion and 2% above it (overshoot builds queue slowly
/// enough to be caught), and 10% again once that level is 5 s old.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Estimator {
    pub data_bytes_per_sec: f64,
    pub sent_bytes_per_sec: f64,
    pub network_blocked_fraction: f64,
    pub page_blocked_fraction: f64,
    /// estimate at the last congestion event
    pub congested_at_bytes_per_sec: f64,
    pub delay_events: u64,
    /// cuts by signal: a growing backlog (sent > 1.3 x acknowledged), senders blocked on SCTP, video overuse
    pub backlog_events: u64,
    pub blocked_events: u64,
    pub video_overuse_events: u64,
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
    /// (when, delivered bytes, sent bytes, secs) of the last `DELIVERY_WINDOW` of intervals
    #[serde(skip)]
    deliveries: std::collections::VecDeque<(std::time::Instant, f64, f64, f64)>,
    /// what the link delivered over the last `DELIVERY_WINDOW`
    pub delivered_bytes_per_sec: f64,
}

/// 512 kbit/s: a slow link isn't flooded at the start (1 MB/s plus video's 1 MB/s filled a 2 Mbit/s link's queue, lost
/// ~40%, and SCTP's backoff silenced the page for 20 s); slow start reaches 8 MB/s in ~3 s.
pub const INITIAL_ESTIMATE: f64 = 64_000.0;
/// 16 kbit/s of payload. Small messages pay ~85 B of headers each, so a 100 kbit/s link carries about half its rate in
/// strict 64 B commands: a 64 kbit/s floor kept it overloaded.
const MIN_ESTIMATE: f64 = 2_000.0;
const MAX_ESTIMATE: f64 = 2e9;
const CONGESTED_FRACTION: f64 = 0.2;
const SLOW_START_GAIN: f64 = 1.5;
const FAST_PROBE_GAIN: f64 = 1.10;
const SLOW_PROBE_GAIN: f64 = 1.02;
/// RTT above its recent minimum by this much means a queue is building.
pub const DELAY_THRESHOLD_MS: f64 = 5.0;
/// Intervals in a row above the threshold before the delay counts as a queue: a second (Wi-Fi retries swing the delay
/// 0-400 ms every few hundred ms; at two intervals they counted as a queue every few seconds, whatever was sent, and cut
/// the R1's 300 KB/s link to 5 KB/s).
const DELAY_PERSISTENCE: u32 = 4;
/// How far back the path's usual queue-delay jitter is measured.
const JITTER_WINDOW: std::time::Duration = std::time::Duration::from_secs(10);
/// A queue delay that every sample of the last `STANDING_QUEUE_WINDOW` (at least `STANDING_QUEUE_SAMPLES`) stayed above.
const STANDING_QUEUE_MS: f64 = 150.0;
const STANDING_QUEUE_WINDOW: std::time::Duration = std::time::Duration::from_secs(3);
const STANDING_QUEUE_SAMPLES: usize = 6;
/// Most queue delay read as the path's jitter rather than a queue (Wi-Fi + Tailscale swung 60-480 ms with nothing queued).
const MAX_JITTER_ALLOWANCE_MS: f64 = 500.0;
/// Probing up only while the streams sent at least this fraction of the estimate (the allocator hands out 75% of it).
const PROBE_UTILIZATION: f64 = 0.5;
/// What the link delivered over this long bounds a cut for a delay that only persisted (`DELAY_PERSISTENCE`): acks
/// arrive in bursts on a jittery path, and a 1 s reading took one such cut from 18 to 6 KB/s.
const SLOW_DELIVERY_WINDOW: std::time::Duration = std::time::Duration::from_secs(3);
const DELAY_DECREASE: f64 = 0.85;
/// Blocked on SCTP: down to 0.9 x what went out, but at most this per interval (a long RTT blocks the first intervals).
const BLOCKED_DECREASE: f64 = 0.85;
/// Slow probing (2%) near the last congestion level only while that level is this recent (one early stall held it for good).
const STALE_CONGESTION: std::time::Duration = std::time::Duration::from_secs(5);
const HOLD_AFTER_CONGESTION: std::time::Duration = std::time::Duration::from_secs(1);
/// Sent over `DELIVERY_WINDOW` more than this times what was acknowledged (plus `BACKLOG_SLACK_BYTES`) is a growing queue.
const BACKLOG_RATIO: f64 = 1.3;
const BACKLOG_SLACK_BYTES: f64 = 4_000.0;
/// The round trip counted as in flight is at most this (a queue itself raises the RTT).
const MAX_IN_FLIGHT_RTT_MS: f64 = 500.0;
/// A delay event on a link the streams were filling cuts to 0.9 x what it delivered over this long, when that is lower than
/// -15%, at most -75% at once (from a 512 kbit/s start on a 100 kbit/s link, -15% a second took 10 s, and its full queue
/// stalled even the strict streams meanwhile).
const DELIVERY_WINDOW: std::time::Duration = std::time::Duration::from_secs(1);

impl Default for Estimator {
    fn default() -> Self {
        Estimator {
            data_bytes_per_sec: INITIAL_ESTIMATE,
            sent_bytes_per_sec: 0.0,
            network_blocked_fraction: 0.0,
            page_blocked_fraction: 0.0,
            congested_at_bytes_per_sec: f64::INFINITY,
            delay_events: 0,
            backlog_events: 0,
            blocked_events: 0,
            video_overuse_events: 0,
            delay_threshold_ms: DELAY_THRESHOLD_MS,
            hold_probing_until: None,
            delay_samples: std::collections::VecDeque::new(),
            over_threshold: 0,
            last_congestion: None,
            deliveries: std::collections::VecDeque::new(),
            delivered_bytes_per_sec: 0.0,
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
    /// time senders waited for the page to ack what it has, summed likewise
    pub page_blocked_secs: f64,
    pub active_senders: usize,
    pub data_demand: f64,
    /// the interval's smallest RTT minus the recent minimum, if any sample came in
    pub queue_delay_ms: Option<f64>,
    /// video congestion control put the video below what it sent
    pub video_overuse: bool,
    /// bytes the browser acknowledged this interval (data channels, plus what video sent)
    pub delivered_bytes: f64,
    /// the path's recent highest round trip, if measured: what is in flight over it isn't a backlog
    pub high_rtt_ms: Option<f64>,
}

impl Estimator {
    pub fn update(&mut self, interval: Interval) {
        let Interval { now, secs: interval_secs, sent_bytes, blocked_secs, page_blocked_secs, active_senders, data_demand, queue_delay_ms, video_overuse, delivered_bytes, high_rtt_ms } = interval;
        if interval_secs <= 0.0 {
            return;
        }
        self.deliveries.push_back((now, delivered_bytes, sent_bytes, interval_secs));
        while self.deliveries.front().is_some_and(|(at, ..)| now.duration_since(*at) >= SLOW_DELIVERY_WINDOW) {
            self.deliveries.pop_front();
        }
        let sums = |window: std::time::Duration| {
            let recent = self.deliveries.iter().filter(|(at, ..)| now.duration_since(*at) < window);
            recent.fold((0.0, 0.0, 0.0), |(d, s, t), (_, delivered, sent, secs)| (d + delivered, s + sent, t + secs))
        };
        let (slow_delivered, _, slow_secs) = sums(SLOW_DELIVERY_WINDOW);
        let slow_delivered_rate = slow_delivered / slow_secs.max(1e-3);
        let (delivered, sent, secs) = sums(DELIVERY_WINDOW);
        self.delivered_bytes_per_sec = delivered / secs.max(1e-3);
        // streams that sent less than half the estimate didn't test the link: what it delivered says nothing about it
        let link_tested = sent / secs.max(1e-3) >= 0.5 * self.data_bytes_per_sec;
        // handed to SCTP faster than the browser acknowledges: a queue is growing somewhere, whatever the RTT says (a
        // queue that grows gradually widens the delay spread it is measured against, and on Wi-Fi jitter hides it)
        // (bytes sent within the last round trip aren't acknowledged yet: at a 400 ms Wi-Fi RTT a burst read as a backlog,
        // and each such cut, capped by what the first second delivered, took the R1's estimate from 1 MB/s to 9 KB/s)
        let in_flight = sent / secs.max(1e-3) * high_rtt_ms.unwrap_or(0.0).min(MAX_IN_FLIGHT_RTT_MS) / 1000.0;
        let backlog_growing = secs >= 0.75 && sent > BACKLOG_RATIO * delivered + BACKLOG_SLACK_BYTES + in_flight;
        let delivered_rate = self.delivered_bytes_per_sec;
        // a link the streams filled: 0.9 x what it delivered; otherwise -15%, but never left above twice what it delivered
        // (after a drop from 20 to 1 Mbit/s, -15% a second from 4.5 MB/s held a 300 ms queue for 15 s)
        let ceiling = if link_tested { 0.9 * delivered_rate } else { 2.0 * delivered_rate };
        let cut = |estimate: f64| (estimate * DELAY_DECREASE).min(ceiling.max(MIN_ESTIMATE)).max(estimate * 0.25);
        self.sent_bytes_per_sec = sent_bytes / interval_secs;
        let sender_secs = interval_secs * active_senders.max(1) as f64;
        self.network_blocked_fraction = (blocked_secs / sender_secs).min(1.0);
        self.page_blocked_fraction = (page_blocked_secs / sender_secs).min(1.0);
        let holding = self.hold_probing_until.is_some_and(|until| now < until);
        let (persisted, standing) = self.queue_building(now, queue_delay_ms);
        let delayed = persisted || standing;
        if (delayed || backlog_growing) && !holding {
            self.backlog_events += u64::from(backlog_growing && !delayed);
            self.congested_at_bytes_per_sec = self.data_bytes_per_sec;
            self.last_congestion = Some(now);
            self.data_bytes_per_sec = if standing || backlog_growing {
                cut(self.data_bytes_per_sec)
            } else {
                // only the delay says so: -15%, no lower than twice what the link delivered over a few seconds
                (self.data_bytes_per_sec * DELAY_DECREASE).min((2.0 * slow_delivered_rate).max(MIN_ESTIMATE)).max(self.data_bytes_per_sec * 0.25)
            };
            self.delay_events += u64::from(delayed);
            self.hold_probing_until = Some(now + HOLD_AFTER_CONGESTION);
        } else if self.network_blocked_fraction > CONGESTED_FRACTION || video_overuse {
            self.blocked_events += u64::from(self.network_blocked_fraction > CONGESTED_FRACTION);
            self.video_overuse_events += u64::from(video_overuse);
            self.congested_at_bytes_per_sec = self.data_bytes_per_sec;
            self.last_congestion = Some(now);
            self.data_bytes_per_sec = (0.9 * self.sent_bytes_per_sec).max(self.data_bytes_per_sec * BLOCKED_DECREASE);
        } else if !holding
            && data_demand > self.data_bytes_per_sec * 0.95
            // over the delivery window: big messages go out in bursts, and one quiet interval held probing back for good
            && sent / secs.max(1e-3) >= self.data_bytes_per_sec * PROBE_UTILIZATION
            && self.page_blocked_fraction <= CONGESTED_FRACTION
        {
            // (held back by the page, the senders never test the network: probing then would only make up a number)
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
    /// (above the threshold `DELAY_PERSISTENCE` intervals in a row, a standing queue)
    fn queue_building(&mut self, now: std::time::Instant, queue_delay_ms: Option<f64>) -> (bool, bool) {
        let Some(delay) = queue_delay_ms else { return (false, false) };
        while self.delay_samples.front().is_some_and(|(at, _)| now.duration_since(*at) > JITTER_WINDOW) {
            self.delay_samples.pop_front();
        }
        let mut recent: Vec<f64> = self.delay_samples.iter().map(|(_, delay)| *delay).collect();
        // the path's own swing: the spread of its queue delays (p90 - p10), capped. A standing queue raises every sample
        // and leaves the spread alone (counted as a level, twice the median, it raised the bar from 70 ms to 1.5 s)
        recent.sort_by(f64::total_cmp);
        let at = |q: f64| recent.get(((recent.len() as f64 - 1.0) * q).round() as usize).copied().unwrap_or(0.0);
        let allowance = (at(0.9) - at(0.1)).min(MAX_JITTER_ALLOWANCE_MS);
        self.delay_threshold_ms = DELAY_THRESHOLD_MS + allowance;
        self.over_threshold = if delay > self.delay_threshold_ms { self.over_threshold + 1 } else { 0 };
        self.delay_samples.push_back((now, delay));
        // a queue that stands under even the least delay of the last few seconds, whatever the spread (a spike at a link
        // change pushed the threshold to its cap and hid a 300 ms queue for 10 s; jitter rarely keeps its least that high)
        let standing = self.delay_samples.iter().filter(|(at, _)| now.duration_since(*at) <= STANDING_QUEUE_WINDOW);
        let (count, least) = standing.fold((0, f64::INFINITY), |(count, least), (_, delay)| (count + 1, least.min(*delay)));
        (self.over_threshold >= DELAY_PERSISTENCE, count >= STANDING_QUEUE_SAMPLES && least > STANDING_QUEUE_MS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(priority: f64, max_hz: f64, bytes: f64) -> Demand {
        Demand { priority, max_hz, quality_range: None, tradeoff: 0.5, price: Box::new(move |_| bytes), fixed_bytes_per_sec: None, shrinks_last: false }
    }

    fn transcoded(tradeoff: f64, max_hz: f64) -> Demand {
        // price grows with quality: 100 bytes at q=0 .. 1100 at q=1
        Demand { priority: 1.0, max_hz, quality_range: Some((0.0, 1.0)), tradeoff, price: Box::new(|q| 100.0 + 1000.0 * q), fixed_bytes_per_sec: None, shrinks_last: false }
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
    fn strict_streams_are_reserved_even_over_budget() {
        let strict = Demand { fixed_bytes_per_sec: Some(800.0), ..raw(1.0, 10.0, 80.0) };
        let allocations = allocate(1000.0, &[strict, raw(1.0, 10.0, 100.0)]);
        assert_eq!(allocations[0].budget_bytes_per_sec, 800.0);
        assert!((allocations[1].budget_bytes_per_sec - 200.0).abs() < 1e-6);
        let strict = Demand { fixed_bytes_per_sec: Some(800.0), ..raw(1.0, 10.0, 80.0) };
        let allocations = allocate(500.0, &[strict, raw(1.0, 10.0, 100.0)]);
        assert_eq!((allocations[0].budget_bytes_per_sec, allocations[1].budget_bytes_per_sec), (800.0, 0.0));
    }

    #[test]
    fn reliable_streams_shrink_last() {
        let reliable = || Demand { shrinks_last: true, ..raw(1.0, 100.0, 200.0) };
        // 20 KB/s reliable + 10 KB/s other on 25 KB/s: the other gives up 5 KB/s, the reliable one nothing
        let allocations = allocate(25_000.0, &[reliable(), raw(10.0, 10.0, 1000.0)]);
        assert_eq!(allocations[0].budget_bytes_per_sec, 20_000.0);
        assert!((allocations[1].budget_bytes_per_sec - 5_000.0).abs() < 1e-6, "{allocations:?}");
        // on 12.5 KB/s the other is squeezed out and the reliable one paced to the link
        let allocations = allocate(12_500.0, &[reliable(), raw(10.0, 10.0, 1000.0)]);
        assert_eq!(allocations[1].hz, 0.0);
        assert!((allocations[0].budget_bytes_per_sec - 12_500.0).abs() < 1e-6, "{allocations:?}");
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
        // delivered as sent unless a test says otherwise
        Interval { now, secs, sent_bytes, blocked_secs, page_blocked_secs: 0.0, active_senders, data_demand, queue_delay_ms, video_overuse: false, delivered_bytes: sent_bytes, high_rtt_ms: None }
    }

    #[test]
    fn estimator_backs_off_when_blocked_and_probes_when_wanted() {
        let now = std::time::Instant::now();
        let mut estimator = Estimator { data_bytes_per_sec: 1_000_000.0, ..Estimator::default() };
        estimator.update(sample(now, 0.25, 100_000.0, 0.2, 1, 5e6, None));
        assert!((estimator.data_bytes_per_sec - 850_000.0).abs() < 1.0, "down 15% at most per step: {estimator:?}");
        estimator.update(sample(now, 0.25, 100_000.0, 0.2, 1, 5e6, None));
        assert!((estimator.data_bytes_per_sec - 722_500.0).abs() < 1.0, "{estimator:?}");
        estimator.update(sample(now, 0.25, 180_000.0, 0.2, 1, 5e6, None));
        assert!((estimator.data_bytes_per_sec - 648_000.0).abs() < 1.0, "0.9 x 720 KB/s measured: {estimator:?}");
        estimator.update(sample(now, 0.25, 100_000.0, 0.0, 1, 5e6, None));
        assert!((estimator.data_bytes_per_sec - 712_800.0).abs() < 1.0, "probes fast below 90% of the last congestion level (722.5 KB/s): {estimator:?}");
        estimator.update(sample(now, 0.25, 100_000.0, 0.0, 1, 5e6, None));
        assert!((estimator.data_bytes_per_sec - 712_800.0 * 1.02).abs() < 1.0, "slowly around it: {estimator:?}");
        estimator.update(sample(now, 0.25, 100_000.0, 0.0, 1, 1000.0, None));
        assert!((estimator.data_bytes_per_sec - 712_800.0 * 1.02).abs() < 1.0, "no probing without demand");
        // (judged over the last second, so a second later)
        estimator.update(sample(now + std::time::Duration::from_millis(1100), 0.25, 20_000.0, 0.0, 1, 5e6, None));
        assert!((estimator.data_bytes_per_sec - 712_800.0 * 1.02).abs() < 1.0, "no probing on a link that isn't carrying the estimate");
        // 5 s after the last congestion event its level is stale: fast again
        let later = now + STALE_CONGESTION + std::time::Duration::from_millis(1);
        let before = estimator.data_bytes_per_sec;
        estimator.update(sample(later, 0.25, 100_000.0, 0.0, 1, 5e6, None));
        assert!((estimator.data_bytes_per_sec - before * 1.1).abs() < 1.0, "{estimator:?}");
    }

    #[test]
    fn waiting_on_the_page_is_reported_not_taken_for_the_network() {
        let now = std::time::Instant::now();
        let mut estimator = Estimator::default();
        let page_bound = Interval { page_blocked_secs: 0.15, ..sample(now, 0.25, 100_000.0, 0.0, 2, 5e6, None) };
        estimator.update(page_bound);
        assert!((estimator.page_blocked_fraction - 0.3).abs() < 1e-9 && estimator.network_blocked_fraction == 0.0);
        assert_eq!(estimator.data_bytes_per_sec, INITIAL_ESTIMATE, "neither lowered nor probed up: {estimator:?}");
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
        for interval in 41..44 {
            estimator.update(sample(at(interval), 0.25, 200_000.0, 0.0, 1, 1000.0, Some(25.0 + interval as f64)));
            assert_eq!(estimator.data_bytes_per_sec, settled, "a spike under a second is not a queue: {estimator:?}");
        }
        estimator.update(sample(at(44), 0.25, 200_000.0, 0.0, 1, 1000.0, Some(40.0)));
        assert!((estimator.data_bytes_per_sec - settled * 0.85).abs() < 1.0, "a second of it is: cut by 15%: {estimator:?}");
        assert_eq!(estimator.delay_events, 1);
        estimator.update(sample(at(45), 0.25, 200_000.0, 0.0, 1, 5e6, Some(1.0)));
        assert!((estimator.data_bytes_per_sec - settled * 0.85).abs() < 1.0, "holds while the queue drains");
        estimator.update(sample(at(49), 0.25, 200_000.0, 0.0, 1, 5e6, Some(1.0)));
        assert!((estimator.data_bytes_per_sec - settled * 0.85 * 1.1).abs() < 1.0, "then probes again, 10% below 90% of the congestion level: {estimator:?}");
    }

    #[test]
    fn a_jittery_idle_path_is_not_congestion() {
        // RTT excess swinging between 5 and 90 ms with nothing queued (seen on Wi-Fi + VPN)
        let now = std::time::Instant::now();
        let mut estimator = Estimator { data_bytes_per_sec: 1_000_000.0, ..Estimator::default() };
        let swings = [10.0, 74.0, 18.0, 5.0, 27.0, 32.0, 9.0, 56.0, 10.0, 28.0, 10.0, 45.0, 22.0, 2.0, 73.0, 22.0, 17.0, 11.0, 15.0, 68.0, 7.0, 23.0];
        for (index, delay) in swings.iter().cycle().take(120).enumerate() {
            estimator.update(sample(now + std::time::Duration::from_millis(250 * index as u64), 0.25, 150_000.0, 0.0, 1, 2e6, Some(*delay)));
        }
        assert!(estimator.delay_events <= 3, "{} delay events", estimator.delay_events);
        assert!(estimator.data_bytes_per_sec >= 1_000_000.0, "the estimate stays up: {estimator:?}");
        assert!(estimator.delay_threshold_ms > 30.0, "{estimator:?}");
    }

    #[test]
    fn a_standing_queue_is_not_learned_as_jitter() {
        // a quiet path, then a queue that stands at 300-400 ms: it keeps cutting, the threshold doesn't climb to it
        let now = std::time::Instant::now();
        let at = |interval: u64| now + std::time::Duration::from_millis(250 * interval);
        let mut estimator = Estimator { data_bytes_per_sec: 1_000_000.0, ..Estimator::default() };
        for interval in 0..20 {
            estimator.update(sample(at(interval), 0.25, 150_000.0, 0.0, 1, 2e6, Some(3.0)));
        }
        for interval in 20..140 {
            estimator.update(sample(at(interval), 0.25, 150_000.0, 0.0, 1, 2e6, Some(300.0 + (interval % 3) as f64 * 50.0)));
        }
        assert!(estimator.delay_threshold_ms <= DELAY_THRESHOLD_MS + MAX_JITTER_ALLOWANCE_MS, "{estimator:?}");
        assert!(estimator.delay_events >= 20, "cut about once a hold period: {estimator:?}");
        assert!(estimator.data_bytes_per_sec < 50_000.0, "{estimator:?}");
    }

    #[test]
    fn a_growing_backlog_cuts_to_what_the_link_delivered() {
        // 48 KB/s sent on a 12.5 KB/s link, no RTT samples at all: once a second of it is seen, cut near the link
        let now = std::time::Instant::now();
        let at = |interval: u64| now + std::time::Duration::from_millis(250 * interval);
        let mut estimator = Estimator { data_bytes_per_sec: 64_000.0, ..Estimator::default() };
        for interval in 0..2 {
            estimator.update(Interval { delivered_bytes: 3_125.0, ..sample(at(interval), 0.25, 12_000.0, 0.0, 1, 1000.0, None) });
        }
        assert_eq!(estimator.data_bytes_per_sec, 64_000.0, "half a second isn't enough to tell");
        for interval in 2..4 {
            estimator.update(Interval { delivered_bytes: 3_125.0, ..sample(at(interval), 0.25, 12_000.0, 0.0, 1, 1000.0, None) });
        }
        assert!(estimator.data_bytes_per_sec <= 16_000.0 && estimator.data_bytes_per_sec > 10_000.0, "at most -75% in one cut: {estimator:?}");
        // delivered as fast as sent: no backlog
        let mut estimator = Estimator { data_bytes_per_sec: 64_000.0, ..Estimator::default() };
        for interval in 0..8 {
            estimator.update(Interval { delivered_bytes: 11_000.0, ..sample(at(interval), 0.25, 12_000.0, 0.0, 1, 1000.0, None) });
        }
        assert_eq!(estimator.data_bytes_per_sec, 64_000.0);
    }

    #[test]
    fn a_delay_event_on_an_idle_link_cuts_to_twice_what_it_delivered() {
        let now = std::time::Instant::now();
        let at = |interval: u64| now + std::time::Duration::from_millis(250 * interval);
        let mut estimator = Estimator { data_bytes_per_sec: 64_000.0, ..Estimator::default() };
        for interval in 0..40 {
            estimator.update(Interval { delivered_bytes: 500.0, ..sample(at(interval), 0.25, 500.0, 0.0, 1, 1000.0, Some(3.0)) });
        }
        for interval in 40..44 {
            estimator.update(Interval { delivered_bytes: 500.0, ..sample(at(interval), 0.25, 500.0, 0.0, 1, 1000.0, Some(400.0)) });
        }
        assert_eq!(estimator.data_bytes_per_sec, 16_000.0, "2 x 2 KB/s delivered, but at most -75% at once: {estimator:?}");
    }

    #[test]
    fn a_standing_queue_is_caught_above_a_capped_threshold() {
        // a queue of at least 300 ms that swings to 700 ms: the spread puts the threshold above every sample, the least
        // delay still shows the queue within ~3 s, and the cut lands within twice what the link delivered
        let now = std::time::Instant::now();
        let at = |interval: u64| now + std::time::Duration::from_millis(250 * interval);
        let mut estimator = Estimator { data_bytes_per_sec: 1_500_000.0, ..Estimator::default() };
        for interval in 0..16 {
            let delay = if interval % 2 == 0 { 300.0 } else { 700.0 };
            estimator.update(Interval { delivered_bytes: 25_000.0, ..sample(at(interval), 0.25, 25_000.0, 0.0, 1, 1000.0, Some(delay)) });
        }
        assert!(estimator.delay_threshold_ms > 300.0, "the spread alone wouldn't: {estimator:?}");
        assert!(estimator.data_bytes_per_sec <= 200_000.0, "{estimator:?}");
    }

    #[test]
    fn video_overuse_cuts_the_one_estimate() {
        let now = std::time::Instant::now();
        let mut estimator = Estimator { data_bytes_per_sec: 1_000_000.0, ..Estimator::default() };
        estimator.update(Interval { video_overuse: true, ..sample(now, 0.25, 50_000.0, 0.0, 1, 5e6, None) });
        assert!((estimator.data_bytes_per_sec - 850_000.0).abs() < 1.0, "{estimator:?}");
    }
}
