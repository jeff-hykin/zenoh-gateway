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

/// The frontend's link as BBR models one (Cardwell et al., "BBR: Congestion-Based Congestion Control", ACM Queue 2016),
/// its video and data together:
/// - bandwidth: the highest delivery rate sampled over the last `BANDWIDTH_ROUNDS` rounds (`SendGate::take_round`: one
///   sample per page ack, plus what video got through). Samples from frames the link didn't hold back only count when
///   higher: streams held back by their own allocation (or idle) say nothing about what the link could carry;
/// - round trip: the least send-to-ack time over `MIN_RTT_WINDOW`. When that is `MIN_RTT_WINDOW` old, the window drops to
///   its minimum until what is in flight is down to it and `PROBE_RTT_TIME` more, so a queue that formed drains (on a
///   slow link that takes longer than 200 ms), and the least time seen meanwhile replaces it:
///   otherwise a standing queue becomes the measure, the window grows with it, and so does the queue;
/// - ack aggregation: the most acks ran ahead of bandwidth x time over the last `BANDWIDTH_ROUNDS` rounds. Acks come in
///   bursts (the page acks in batches; Wi-Fi aggregates frames and its round trip swings 0-400 ms), and a window of
///   bandwidth x the least round trip alone held a jittery link to a fraction of what it carries.
///
/// Streams are budgeted bandwidth x gain and paced at it, and what is in flight is held to `WINDOW_GAIN` x bandwidth x
/// round trip + the ack aggregation. The gain is `STARTUP_GAIN` while the bandwidth keeps growing (by `STARTUP_GROWTH` within `STARTUP_ROUNDS`
/// link-limited rounds), then cycles through `PROBE_GAINS`: a round asking 25% more, one draining what that queued,
/// six at the estimate. Nothing reacts to a delay or a loss, so Wi-Fi jitter can't move it; a link that slows delivers
/// less and its old maximum ages out within `BANDWIDTH_ROUNDS`, while the window keeps the queue about a round trip long.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LinkModel {
    pub bandwidth_bytes_per_sec: f64,
    pub delivered_bytes_per_sec: f64,
    pub gain: f64,
    pub starting_up: bool,
    pub min_rtt_ms: Option<f64>,
    pub extra_acked: f64,
    #[serde(skip)]
    extra_acked_samples: std::collections::VecDeque<(std::time::Instant, f64)>,
    /// the least round trip seen during the ProbeRTT under way
    #[serde(skip)]
    probe_min_rtt: Option<f64>,
    /// (when, bytes/s) of the rounds that count toward the bandwidth
    #[serde(skip)]
    samples: std::collections::VecDeque<(std::time::Instant, f64)>,
    #[serde(skip)]
    min_rtt_at: Option<std::time::Instant>,
    #[serde(skip)]
    probing_rtt: bool,
    #[serde(skip)]
    probe_rtt_until: Option<std::time::Instant>,
    /// startup: the bandwidth last seen growing, and the rounds since; then the place in `PROBE_GAINS`
    #[serde(skip)]
    startup_level: f64,
    #[serde(skip)]
    stalled_rounds: u32,
    #[serde(skip)]
    cycle: usize,
}

/// The first guess, 512 kbit/s: low enough not to flood a slow link (1 MB/s plus video filled a 2 Mbit/s link's queue
/// and SCTP's backoff silenced the page for 20 s), and startup doubles it every round.
pub const INITIAL_BANDWIDTH: f64 = 64_000.0;
/// BBR's constants.
const STARTUP_GAIN: f64 = 2.0 / std::f64::consts::LN_2;
const STARTUP_GROWTH: f64 = 1.25;
const STARTUP_ROUNDS: u32 = 3;
const PROBE_GAINS: [f64; 8] = [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
const BANDWIDTH_ROUNDS: f64 = 10.0;
/// One bandwidth-delay product is what the link holds; the ack aggregation is what acks arriving in bursts need on top
/// (BBR's 2 x covers both at once, and the second product stood as a queue strict streams waited behind).
const WINDOW_GAIN: f64 = 1.0;
const MIN_RTT_WINDOW: std::time::Duration = std::time::Duration::from_secs(10);
const PROBE_RTT_TIME: std::time::Duration = std::time::Duration::from_millis(200);
/// Bytes in flight before any round trip is measured, and the least ever: RFC 6928's 10 packets, BBR's 4.
const INITIAL_WINDOW: usize = 10 * 1200;
const MIN_WINDOW: usize = 4 * 1200;

impl Default for LinkModel {
    fn default() -> Self {
        LinkModel {
            bandwidth_bytes_per_sec: INITIAL_BANDWIDTH,
            delivered_bytes_per_sec: 0.0,
            gain: STARTUP_GAIN,
            starting_up: true,
            min_rtt_ms: None,
            extra_acked: 0.0,
            extra_acked_samples: Default::default(),
            probe_min_rtt: None,
            samples: Default::default(),
            min_rtt_at: None,
            probing_rtt: false,
            probe_rtt_until: None,
            startup_level: 0.0,
            stalled_rounds: 0,
            cycle: 0,
        }
    }
}

impl LinkModel {
    /// One round of `secs`: the gate's samples, what video got through (bytes/s, added to each data sample) and what was
    /// delivered in all (bytes/s, reported).
    pub fn update(&mut self, now: std::time::Instant, secs: f64, round: crate::pacing::RoundSamples, video_bytes_per_sec: f64, delivered_bytes_per_sec: f64) {
        if secs <= 0.0 {
            return;
        }
        self.update_round_trip(now, round.min_rtt_ms, round.drained);
        self.delivered_bytes_per_sec = delivered_bytes_per_sec;
        let link_limited = round.link_limited_rate.is_some();
        if let Some(rate) = round.link_limited_rate {
            self.samples.push_back((now, rate + video_bytes_per_sec));
        }
        if let Some(rate) = round.app_limited_rate.map(|rate| rate + video_bytes_per_sec).filter(|rate| *rate > self.bandwidth_bytes_per_sec) {
            self.samples.push_back((now, rate));
        }
        let round_secs = secs.max(self.min_rtt_ms.unwrap_or(0.0) / 1000.0);
        let window = crate::secs(BANDWIDTH_ROUNDS * round_secs);
        self.extra_acked_samples.push_back((now, round.extra_acked));
        for samples in [&mut self.samples, &mut self.extra_acked_samples] {
            while samples.front().is_some_and(|(at, _)| now.duration_since(*at) > window) {
                samples.pop_front();
            }
        }
        self.extra_acked = self.extra_acked_samples.iter().map(|(_, extra)| *extra).fold(0.0, f64::max);
        let measured = self.samples.iter().map(|(_, rate)| *rate).reduce(f64::max).unwrap_or(0.0);
        if self.starting_up {
            // the first guess stands until a round measures more (the first rounds, while the channels open, deliver next
            // to nothing); growth is judged on what was measured, once anything got through
            self.bandwidth_bytes_per_sec = measured.max(INITIAL_BANDWIDTH);
            if measured >= STARTUP_GROWTH * self.startup_level {
                (self.startup_level, self.stalled_rounds) = (measured, 0);
            } else if link_limited && measured > 0.0 {
                self.stalled_rounds += 1;
            }
            if self.stalled_rounds >= STARTUP_ROUNDS {
                // done: the drain round comes first
                (self.starting_up, self.cycle, self.bandwidth_bytes_per_sec) = (false, 1, measured);
            }
        } else {
            if measured > 0.0 {
                self.bandwidth_bytes_per_sec = measured;
            }
            self.cycle = (self.cycle + 1) % PROBE_GAINS.len();
        }
        self.gain = if self.starting_up { STARTUP_GAIN } else { PROBE_GAINS[self.cycle] };
    }

    fn update_round_trip(&mut self, now: std::time::Instant, round_rtt_ms: Option<f64>, drained: bool) {
        if self.probing_rtt {
            // the clock starts once what is in flight is down to the minimum window
            if self.probe_rtt_until.is_none() && drained {
                self.probe_rtt_until = Some(now + PROBE_RTT_TIME);
            }
            if let Some(rtt) = round_rtt_ms.filter(|_| self.probe_rtt_until.is_some()) {
                self.probe_min_rtt = Some(self.probe_min_rtt.map_or(rtt, |least| least.min(rtt)));
            }
            if self.probe_rtt_until.is_some_and(|until| now >= until) {
                self.probing_rtt = false;
                self.probe_rtt_until = None;
                if let Some(least) = self.probe_min_rtt.take() {
                    (self.min_rtt_ms, self.min_rtt_at) = (Some(least), Some(now));
                }
            }
            return;
        }
        if let Some(rtt) = round_rtt_ms
            && self.min_rtt_ms.is_none_or(|least| rtt <= least)
        {
            (self.min_rtt_ms, self.min_rtt_at) = (Some(rtt), Some(now));
        }
        if !self.starting_up && self.min_rtt_at.is_some_and(|at| now.duration_since(at) > MIN_RTT_WINDOW) {
            self.probing_rtt = true;
        }
    }

    pub fn probing_rtt(&self) -> bool {
        self.probing_rtt
    }

    /// The pacing rate (bytes/s) and the most that may be in flight (bytes).
    pub fn pacing(&self) -> (f64, usize) {
        let window = match (self.probing_rtt(), self.min_rtt_ms) {
            (true, _) => MIN_WINDOW,
            (false, Some(rtt)) => ((WINDOW_GAIN * self.bandwidth_bytes_per_sec * rtt / 1000.0 + self.extra_acked) as usize).max(MIN_WINDOW),
            (false, None) => INITIAL_WINDOW,
        };
        (self.bandwidth_bytes_per_sec * self.gain, window)
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

    use std::time::{Duration, Instant};

    fn sample(rate: f64, link_limited: bool, rtt_ms: f64) -> crate::pacing::RoundSamples {
        let rate = Some(rate);
        let (link_limited_rate, app_limited_rate) = if link_limited { (rate, None) } else { (None, rate) };
        crate::pacing::RoundSamples { link_limited_rate, app_limited_rate, min_rtt_ms: Some(rtt_ms), extra_acked: 0.0, drained: true }
    }

    /// Rounds of 250 ms on a link that carries `capacity` bytes/s: delivered is what the budget allowed, up to it.
    fn run(model: &mut LinkModel, start: Instant, rounds: std::ops::Range<u32>, capacity: f64, link_limited: bool) -> Instant {
        let mut now = start;
        for round in rounds {
            now = start + Duration::from_millis(250 * round as u64);
            let (rate, _) = model.pacing();
            model.update(now, 0.25, sample(rate.min(capacity), link_limited, 20.0), 0.0, rate.min(capacity));
        }
        now
    }

    #[test]
    fn startup_doubles_then_settles_at_what_the_link_delivers() {
        let mut model = LinkModel::default();
        let start = Instant::now();
        run(&mut model, start, 0..40, 2_500_000.0, true);
        assert!(!model.starting_up);
        assert!((2_000_000.0..=2_500_000.0).contains(&model.bandwidth_bytes_per_sec), "{model:?}");
        assert_eq!(model.pacing().1, (model.bandwidth_bytes_per_sec * 0.020) as usize, "bandwidth x round trip");
    }

    #[test]
    fn a_slower_link_ages_out_of_the_bandwidth() {
        let mut model = LinkModel::default();
        let start = Instant::now();
        run(&mut model, start, 0..40, 2_500_000.0, true);
        run(&mut model, start, 40..60, 125_000.0, true);
        assert!(model.bandwidth_bytes_per_sec <= 125_000.0 * 1.25, "{model:?}");
    }

    #[test]
    fn idle_rounds_dont_lower_it() {
        let mut model = LinkModel::default();
        let start = Instant::now();
        let now = run(&mut model, start, 0..40, 1_000_000.0, true);
        let before = model.bandwidth_bytes_per_sec;
        for round in 1..40 {
            model.update(now + Duration::from_millis(250 * round), 0.25, sample(400.0, false, 20.0), 0.0, 400.0);
        }
        assert_eq!(model.bandwidth_bytes_per_sec, before, "the streams wanted no more: the link wasn't measured");
    }

    #[test]
    fn delay_and_loss_dont_move_it() {
        // a jittery path delivers the same bytes, just later: only what arrives counts
        let mut model = LinkModel::default();
        let start = Instant::now();
        run(&mut model, start, 0..40, 300_000.0, true);
        let settled = model.bandwidth_bytes_per_sec;
        for round in 40..80u32 {
            let now = start + Duration::from_millis(250 * round as u64);
            let jitter = if round % 3 == 0 { 0.5 } else { 1.25 };
            model.update(now, 0.25, sample(300_000.0 * jitter, true, 5.0 + (round % 7) as f64 * 60.0), 0.0, 300_000.0 * jitter);
        }
        assert!(model.bandwidth_bytes_per_sec >= settled, "{model:?}");
    }

    #[test]
    fn the_probe_cycle_asks_more_then_drains() {
        let mut model = LinkModel::default();
        run(&mut model, Instant::now(), 0..40, 1_000_000.0, true);
        let gains: Vec<f64> = (0..8)
            .map(|_| {
                model.update(Instant::now(), 0.25, sample(1_000_000.0, true, 20.0), 0.0, 1_000_000.0);
                model.gain
            })
            .collect();
        assert_eq!(gains.iter().filter(|gain| **gain > 1.0).count(), 1);
        assert_eq!(gains.iter().filter(|gain| **gain < 1.0).count(), 1);
    }
}
