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
/// Streams are budgeted bandwidth x gain and paced at it, and what is in flight is held to bandwidth x the typical (median)
/// round trip plus a
/// headroom of (gain - 1) x that + the ack aggregation, at most `latency_target` of the bandwidth while the path is faster than that. The gain is `STARTUP_GAIN` while the bandwidth keeps growing (by `STARTUP_GROWTH` within `STARTUP_ROUNDS`
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
    /// the median round trip lately (`RoundSamples::typical_rtt_ms`)
    pub typical_rtt_ms: Option<f64>,
    /// the path's own round trip (`RoundSamples::path_rtt_ms`: frames sent with almost nothing of ours in flight), so a
    /// queue of ours can't pass for it (measured during ProbeRTT's drain instead, a 640 KB queue the drain didn't empty
    /// read as a 645 ms path, left latency first, and the queue grew)
    pub path_rtt_ms: Option<f64>,
    pub extra_acked: f64,
    #[serde(skip)]
    extra_acked_samples: std::collections::VecDeque<(std::time::Instant, f64)>,
    /// the least round trip seen during the ProbeRTT under way
    #[serde(skip)]
    probe_min_rtt: Option<f64>,
    /// when the last ProbeRTT ended (or the model started)
    #[serde(skip)]
    probed_rtt_at: Option<std::time::Instant>,
    /// (when, bytes/s) of the rounds that count toward the bandwidth
    #[serde(skip)]
    samples: std::collections::VecDeque<(std::time::Instant, f64)>,
    /// (when, bytes/s) of rounds the latency target held back: the bandwidth when no round was link-limited lately (the
    /// target holding every round back, a link that dropped never showed it, and the estimate stayed 16x the link)
    #[serde(skip)]
    target_limited_samples: std::collections::VecDeque<(std::time::Instant, f64)>,
    #[serde(skip)]
    min_rtt_at: Option<std::time::Instant>,
    #[serde(skip)]
    probing_rtt: bool,
    /// when the ProbeRTT under way began
    #[serde(skip)]
    probe_rtt_started: Option<std::time::Instant>,
    #[serde(skip)]
    probe_rtt_until: Option<std::time::Instant>,
    /// startup: the bandwidth last seen growing, and the rounds since; then the place in `PROBE_GAINS`
    #[serde(skip)]
    startup_level: f64,
    #[serde(skip)]
    stalled_rounds: u32,
    /// when startup's estimate last grew by `STARTUP_GROWTH`
    #[serde(skip)]
    grew_at: Option<std::time::Instant>,
    #[serde(skip)]
    cycle: usize,
    /// the bandwidth when the current round began (what a probe must beat)
    #[serde(skip)]
    probe_level: f64,
    /// the last probe found more: probe again at startup's gain
    #[serde(skip)]
    probing_up: bool,
    #[serde(skip)]
    round_started: Option<std::time::Instant>,
    /// something in this round waited on the window or pacing
    #[serde(skip)]
    round_link_limited: bool,
}

/// The first guess, 512 kbit/s: low enough not to flood a slow link (1 MB/s plus video filled a 2 Mbit/s link's queue
/// and SCTP's backoff silenced the page for 20 s), and startup doubles it every round.
pub const INITIAL_BANDWIDTH: f64 = 64_000.0;
/// A 500 kbit/s link: on a path slower than the latency target, faster links may keep more in flight by the square root
/// of their speed over this.
pub const SLOW_LINK: f64 = 62_500.0;
/// What `ServerBuilder::stale_after` defaults to.
pub const DEFAULT_STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(60);
/// What `ServerBuilder::latency_target` defaults to.
pub const DEFAULT_LATENCY_TARGET: std::time::Duration = std::time::Duration::from_millis(300);

/// BBR's constants.
const STARTUP_GAIN: f64 = 2.0 / std::f64::consts::LN_2;
const STARTUP_GROWTH: f64 = 1.25;
const STARTUP_ROUNDS: u32 = 3;
const PROBE_GAINS: [f64; 8] = [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
const BANDWIDTH_ROUNDS: f64 = 10.0;
/// ... and over at least this long: a Wi-Fi stall of a few seconds filled ten 0.55 s rounds with next to nothing, the
/// bandwidth fell from 2.5 MB/s to 1 KB/s, and sending next to nothing it took four minutes to climb back (links change
/// over minutes, so a longer memory costs little).
const BANDWIDTH_WINDOW: std::time::Duration = std::time::Duration::from_secs(10);
const MIN_RTT_WINDOW: std::time::Duration = std::time::Duration::from_secs(10);
const PROBE_RTT_TIME: std::time::Duration = std::time::Duration::from_millis(200);
/// ProbeRTT runs at least this often (not only when the least round trip went stale): its drain sends frames with little
/// in flight, whose round trips are the path's own (`path_rtt_ms`, which picks latency or throughput first in `window`).
const PROBE_RTT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
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
            typical_rtt_ms: None,
            path_rtt_ms: None,
            extra_acked: 0.0,
            extra_acked_samples: Default::default(),
            probe_min_rtt: None,
            probed_rtt_at: None,
            samples: Default::default(),
            target_limited_samples: Default::default(),
            min_rtt_at: None,
            probing_rtt: false,
            probe_rtt_started: None,
            probe_rtt_until: None,
            startup_level: 0.0,
            stalled_rounds: 0,
            grew_at: None,
            cycle: 0,
            probe_level: 0.0,
            probing_up: false,
            round_started: None,
            round_link_limited: false,
        }
    }
}

/// Video's delivery was held back by the link: every packet of its sample waited in the bottleneck's queue
/// (`queue_delay_ms`, the least one-way queueing delay of `media::Delivery`) over a quarter of the latency target, where
/// video's queue brake stops calling it empty. A link with a backlog throughout delivers at its own rate. Not GCC braking: after its
/// cut GCC holds video far under the link for seconds, and counted as the link's rate those rounds took a 125 KB/s
/// link's estimate to 2 KB/s (video_slow).
pub fn video_held_by_link(queue_delay_ms: f64, latency_target: std::time::Duration) -> bool {
    queue_delay_ms > latency_target.as_secs_f64() * 1000.0 / 4.0
}

impl LinkModel {
    /// One round of `secs`: the gate's samples, what video got through (bytes/s over a sample's span, `media::Delivery`;
    /// added to each data sample, on its own when no data was acked: a page with only video otherwise never measured the link, and its video stayed at the
    /// first guess's 1.5 Mbit/s), whether video was held back by the link (queued there, `video_held_by_link`), and what was delivered in all (bytes/s, reported).
    pub fn update(&mut self, now: std::time::Instant, secs: f64, round: crate::pacing::RoundSamples, video_bytes_per_sec: f64, video_link_limited: bool, delivered_bytes_per_sec: f64) {
        if secs <= 0.0 {
            return;
        }
        self.update_round_trip(now, &round);
        if round.typical_rtt_ms.is_some() {
            self.typical_rtt_ms = round.typical_rtt_ms;
        }
        self.delivered_bytes_per_sec = delivered_bytes_per_sec;
        // (rounds spent re-measuring the round trip hold the window down on purpose: they say nothing about the link)
        let link_limited = round.link_limited_rate.is_some() && !self.probing_rtt;
        if let Some(rate) = round.link_limited_rate.filter(|_| link_limited) {
            self.samples.push_back((now, rate + video_bytes_per_sec));
        }
        if let Some(rate) = round.latency_limited_rate.filter(|_| !self.probing_rtt) {
            self.target_limited_samples.push_back((now, rate + video_bytes_per_sec));
        }
        // (data under its share while video queues at the link: the link was busy all the same, and what got through is its rate)
        if let Some(rate) = round.app_limited_rate.map(|rate| rate + video_bytes_per_sec).filter(|rate| video_link_limited || *rate > self.bandwidth_bytes_per_sec) {
            self.samples.push_back((now, rate));
        }
        let no_data_sample = round.link_limited_rate.is_none() && round.app_limited_rate.is_none() && round.latency_limited_rate.is_none();
        let video_rate = video_bytes_per_sec;
        let video_only = no_data_sample && video_rate > 0.0;
        if video_only && (video_link_limited || video_rate > self.bandwidth_bytes_per_sec) {
            self.samples.push_back((now, video_rate));
        }
        // (and at least one rate sample long: probing for 250 ms with 550 ms samples, the extra 25% was averaged with the
        // drain round after it, never measured, and after a drop the estimate stayed at a tenth of the recovered link)
        let round_secs = secs.max(self.min_rtt_ms.unwrap_or(0.0) / 1000.0).max(crate::pacing::SAMPLE_SPAN.as_secs_f64());
        let window = crate::secs(BANDWIDTH_ROUNDS * round_secs).max(BANDWIDTH_WINDOW);
        // a round is at least a round trip: what is delivered now was sent a round trip ago, so judging growth or moving
        // the gain cycle every 250 ms on a 400 ms path ended startup before its doubling showed
        let new_round = self.round_started.is_none_or(|started| now.duration_since(started).as_secs_f64() >= round_secs);
        if new_round {
            self.round_started = Some(now);
            self.round_link_limited = false;
        }
        // (startup ends when growth stalls while the window holds the sender back, the latency target's window too: a
        // round it holds back isn't a link-limited sample, and a 250 kbit/s link never left startup's first guess)
        // (and, starting up, rounds of video alone: offered startup's 2.89x, a camera that stops growing is the link's
        // (or its own) limit either way; held to GCC braking alone, video on a slow link never left startup)
        self.round_link_limited |= link_limited || video_link_limited || (self.starting_up && (round.window_waited || video_only));
        self.extra_acked_samples.push_back((now, round.extra_acked));
        for samples in [&mut self.samples, &mut self.extra_acked_samples, &mut self.target_limited_samples] {
            while samples.front().is_some_and(|(at, _)| now.duration_since(*at) > window) {
                samples.pop_front();
            }
        }
        self.extra_acked = self.extra_acked_samples.iter().map(|(_, extra)| *extra).fold(0.0, f64::max);
        let measured = self.samples.iter().map(|(_, rate)| *rate).reduce(f64::max).or_else(|| self.target_limited_samples.iter().map(|(_, rate)| *rate).reduce(f64::max)).unwrap_or(0.0);
        if self.starting_up {
            // the first guess stands until a round measures more (the first rounds, while the channels open, deliver next
            // to nothing); growth is judged on what was measured, once anything got through
            // (with no sample in the window the estimate stands: aged out by time alone, a page whose demand stayed under
            // the link fell back to the first guess every 10 s, as BBR's max filter doesn't)
            if measured > 0.0 {
                self.bandwidth_bytes_per_sec = measured.max(INITIAL_BANDWIDTH);
            }
            if measured >= STARTUP_GROWTH * self.startup_level {
                (self.startup_level, self.stalled_rounds, self.grew_at) = (measured, 0, Some(now));
            } else if new_round && self.round_link_limited && measured > 0.0 {
                self.stalled_rounds += 1;
            }
            // (or once it hasn't grown for as long as the bandwidth is remembered, held back or not: BBR counts only
            // link-limited rounds, and a page under the link (a camera plus tf, a 400 KB map every 20 s) never left
            // startup, offered 2.89x the estimate with ProbeRTT held off; a probe that finds more probes at startup's gain)
            let grown_lately = self.grew_at.is_none_or(|at| now.duration_since(at) < window);
            if self.stalled_rounds >= STARTUP_ROUNDS || (measured > 0.0 && !grown_lately) {
                // done: the drain round comes first
                (self.starting_up, self.cycle) = (false, 1);
                if measured > 0.0 {
                    self.bandwidth_bytes_per_sec = measured;
                }
            }
        } else {
            if measured > 0.0 {
                self.bandwidth_bytes_per_sec = measured;
            }
            if new_round {
                // a probe that found more probes again, at startup's gain (BBR v2's probe-up, exponential): after a
                // drop and recovery the estimate climbs in seconds, not at 25% per eight rounds. More is half what the
                // probe asked beyond the estimate (at most startup's 25%): a 1.25x probe measures a few % under 1.25x (a
                // sample is padded by the ack delay), and asked to find all 25% it never probed up (est 929 of 2500 KB/s
                // 20 s after the step back); a full link measures it at 1x
                let found = (1.0 + (self.gain - 1.0) / 2.0).min(STARTUP_GROWTH);
                let found_more = self.cycle == 0 && measured >= found * self.probe_level;
                self.probing_up = found_more;
                if !found_more {
                    self.cycle = (self.cycle + 1) % PROBE_GAINS.len();
                }
                self.probe_level = self.bandwidth_bytes_per_sec;
            }
        }
        self.gain = if self.starting_up || self.probing_up { STARTUP_GAIN } else { PROBE_GAINS[self.cycle] };
    }

    fn update_round_trip(&mut self, now: std::time::Instant, round: &crate::pacing::RoundSamples) {
        let (round_rtt_ms, drained) = (round.min_rtt_ms, round.drained);
        if round.path_rtt_ms.is_some() {
            self.path_rtt_ms = round.path_rtt_ms;
        }
        self.probed_rtt_at.get_or_insert(now);
        if self.probing_rtt {
            // the clock starts once what is in flight is down to the minimum window, or after one round regardless: strict
            // streams bypass the window and can keep more than it in flight, and a probe waiting on them never ended (the
            // R1 sat in one with a 5 KB window until its estimate fell to 46 KB/s)
            let started = *self.probe_rtt_started.get_or_insert(now);
            let a_round = crate::secs(self.min_rtt_ms.unwrap_or(0.0) / 1000.0).max(crate::pacing::SAMPLE_SPAN);
            if self.probe_rtt_until.is_none() && (drained || now.duration_since(started) >= a_round) {
                self.probe_rtt_until = Some(now + PROBE_RTT_TIME);
            }
            if let Some(rtt) = round_rtt_ms.filter(|_| self.probe_rtt_until.is_some()) {
                self.probe_min_rtt = Some(self.probe_min_rtt.map_or(rtt, |least| least.min(rtt)));
            }

            if self.probe_rtt_until.is_some_and(|until| now >= until) {
                self.probing_rtt = false;
                (self.probe_rtt_until, self.probe_rtt_started) = (None, None);
                // (the least round trip is fresh again whether or not the probe saw one: left stale, an idle page went
                // straight back into ProbeRTT every other round)
                let least = self.probe_min_rtt.take().or(self.min_rtt_ms);
                (self.min_rtt_ms, self.min_rtt_at) = (least, Some(now));
                self.probed_rtt_at = Some(now);
            }
            return;
        }
        if let Some(rtt) = round_rtt_ms
            && self.min_rtt_ms.is_none_or(|least| rtt <= least)
        {
            (self.min_rtt_ms, self.min_rtt_at) = (Some(rtt), Some(now));
        }
        let stale = self.min_rtt_at.is_some_and(|at| now.duration_since(at) > MIN_RTT_WINDOW);
        let due = self.probed_rtt_at.is_some_and(|at| now.duration_since(at) > PROBE_RTT_INTERVAL);
        if !self.starting_up && (stale || due) {
            self.probing_rtt = true;
        }
    }

    /// Asking for more than the estimate (starting up, or a probe round): the streams then send beyond their shares,
    /// so the link is really tested (spaced by their grants, they never showed it could carry more).
    pub fn probing_bandwidth(&self) -> bool {
        self.gain > 1.0
    }

    pub fn probing_rtt(&self) -> bool {
        self.probing_rtt
    }

    /// The pacing rate (bytes/s) and the most that may be in flight (bytes), see `window`.
    pub fn pacing(&self, latency_target: Option<std::time::Duration>) -> (f64, usize) {
        (self.bandwidth_bytes_per_sec * self.gain, self.window(latency_target).0)
    }

    /// The most that may be in flight (bytes), and whether `latency_target` set it. Two modes:
    /// - the path's own round trip under the target: latency first, nothing in flight longer than the target (beyond the
    ///   path itself it would stand queued);
    /// - the path alone at or over it (Wi-Fi whose delay swings to 400 ms): throughput first, the target scaled by the
    ///   square root of the speed over `SLOW_LINK` (a 2.4 Mbit/s link 600 ms), never under the path's own round trip (a
    ///   cap under what the path takes only starved it).
    ///
    /// A round the target limits delivers less than the link could, so it must not lower the bandwidth (the gate counts
    /// it as app-limited), else the bandwidth, the target's bytes and what got through fell together.
    pub fn window(&self, latency_target: Option<std::time::Duration>) -> (usize, bool) {
        match (self.probing_rtt(), self.typical_rtt_ms.or(self.min_rtt_ms)) {
            (true, _) => (MIN_WINDOW, false),
            (false, Some(rtt)) => {
                // what the link holds over a typical round trip (not the least: on a path whose delay swings, bandwidth x
                // the least let a tenth of the bandwidth through), plus the gain's headroom (startup's, so it can double)
                // and the ack aggregation
                let held = self.bandwidth_bytes_per_sec * rtt / 1000.0;
                let wanted = held + (self.gain.max(1.0) - 1.0) * held + self.extra_acked;
                let path_rtt = self.path_rtt_ms.or(self.min_rtt_ms).unwrap_or(rtt) / 1000.0;
                let most = latency_target.map_or(f64::INFINITY, |target| {
                    let target = target.as_secs_f64();
                    let limit = if path_rtt < target { target } else { (target * (self.bandwidth_bytes_per_sec / SLOW_LINK).sqrt()).max(path_rtt) };
                    // (times the gain, so a probe can still show the link carries more)
                    self.bandwidth_bytes_per_sec * self.gain.max(1.0) * limit
                });
                ((wanted.min(most) as usize).max(MIN_WINDOW), most < wanted)
            }
            (false, None) => (INITIAL_WINDOW, false),
        }
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
        crate::pacing::RoundSamples { link_limited_rate, app_limited_rate, latency_limited_rate: None, min_rtt_ms: Some(rtt_ms), typical_rtt_ms: Some(rtt_ms), path_rtt_ms: Some(rtt_ms), extra_acked: 0.0, drained: true, window_waited: link_limited }
    }

    const ROUND_MS: u64 = 600;

    /// Rounds of `ROUND_MS` (one rate sample long, as a real round is at least) on a link that carries `capacity` bytes/s: delivered is what the budget allowed, up to it.
    fn run(model: &mut LinkModel, start: Instant, rounds: std::ops::Range<u32>, capacity: f64, link_limited: bool) -> Instant {
        let mut now = start;
        for round in rounds {
            now = start + Duration::from_millis(ROUND_MS * round as u64);
            let (rate, _) = model.pacing(None);
            model.update(now, ROUND_MS as f64 / 1000.0, sample(rate.min(capacity), link_limited, 20.0), 0.0, false, rate.min(capacity));
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
        let held = model.bandwidth_bytes_per_sec * 0.020;
        let window = model.pacing(None).1 as f64;
        assert!(window >= held - 1.0 && window <= 1.25 * held + 1.0, "bandwidth x round trip, plus a probe's headroom: {window} vs {held}");
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
    fn a_link_that_comes_back_is_found_in_seconds() {
        // step: 2.5 MB/s, 125 KB/s for 30 s, 2.5 MB/s again. A rate sample reads a few % under what got through (padded
        // by the ack delay), so a 1.25x probe measured 1.23x, never the 25% that probes up: 25% per eight rounds, est
        // 929 of 2500 KB/s 20 s after
        let mut model = LinkModel::default();
        let start = Instant::now();
        let mut estimates = vec![];
        for round in 0..140u64 {
            let now = start + Duration::from_millis(ROUND_MS * round);
            let capacity = if (40..90).contains(&round) { 125_000.0 } else { 2_500_000.0 };
            let (rate, _) = model.pacing(None);
            let delivered = rate.min(capacity) * 0.97;
            model.update(now, ROUND_MS as f64 / 1000.0, sample(delivered, true, 20.0), 0.0, false, delivered);
            estimates.push(model.bandwidth_bytes_per_sec);
        }
        let back = 90 + (10_000 / ROUND_MS) as usize;
        assert!(estimates[back] >= 2_000_000.0, "10 s after the link came back: {:?}", &estimates[90..back + 1]);
    }

    #[test]
    fn latency_first_under_the_target_throughput_first_over_it() {
        let mut model = LinkModel::default();
        run(&mut model, Instant::now(), 0..40, 62_500.0, true);
        model.typical_rtt_ms = Some(2000.0);
        let target = Some(Duration::from_millis(300));
        let (window, capped) = model.window(target);
        assert!(capped && (window as f64) <= (model.bandwidth_bytes_per_sec * 0.3).max(MIN_WINDOW as f64) + 1.0, "a queue doesn't hold itself up: {window}");
        model.path_rtt_ms = Some(400.0);
        let (window, _) = model.window(target);
        let at_least = model.bandwidth_bytes_per_sec * 0.4;
        assert!(window as f64 >= at_least - 1.0, "a path slower than the target alone: never capped under it ({window} < {at_least})");
    }

    #[test]
    fn video_alone_measures_the_link() {
        // a page with only a camera: no data acks, video's delivery is all there is
        let mut model = LinkModel::default();
        let start = Instant::now();
        for round in 0..60u64 {
            let now = start + Duration::from_millis(ROUND_MS * round);
            let (budget, _) = model.pacing(None);
            let video = budget.min(2_500_000.0);
            model.update(now, ROUND_MS as f64 / 1000.0, crate::pacing::RoundSamples::default(), video, budget > 2_500_000.0, video);
        }
        assert!(model.bandwidth_bytes_per_sec >= 2_000_000.0, "video alone climbs to the link: {model:?}");
        assert!(!model.starting_up, "and leaves startup once video is held back by the link");
    }

    // ---- blind spots: page shapes and lifecycles degrade.ts's full page never produces ----

    /// Rounds at the allocator's real interval (250 ms) for a page wanting `data` bytes/s of bulk data and `video`
    /// bytes/s of camera on a link of `capacity`: video gets min(demand, budget) and the link carries what fits;
    /// `video_link_limited` is false, as `peer::allocate` computes it while GCC isn't braking (nor, before 84d38fe, while
    /// GCC's target was steady: the grant was min(GCC, budget), never above GCC).
    /// Data is link-limited only when the budget left after video, or the link, holds it below its demand. Returns the
    /// bandwidth after each round.
    fn page(model: &mut LinkModel, start: Instant, rounds: std::ops::Range<u64>, capacity: f64, data: f64, video: f64) -> Vec<f64> {
        let mut estimates = vec![];
        for round in rounds {
            let now = start + Duration::from_millis(250 * round);
            let (budget, _) = model.pacing(None);
            let video_sent = video.min(budget).min(capacity);
            let data_room = (budget - video_sent).min(capacity - video_sent).max(0.0);
            let data_sent = data.min(data_room);
            let rate = (data > 0.0).then_some(data_sent);
            // (held back by a budget under the estimate, a drain round's, is ours, not the link's: see `SendGate::wait_bulk_turn`)
            let link_limited = data > data_room && (model.gain >= 1.0 || capacity < budget);
            let round_samples = crate::pacing::RoundSamples {
                link_limited_rate: rate.filter(|_| link_limited),
                app_limited_rate: rate.filter(|_| !link_limited),
                min_rtt_ms: rate.map(|_| 20.0),
                typical_rtt_ms: rate.map(|_| 20.0),
                path_rtt_ms: rate.map(|_| 20.0),
                drained: true,
                window_waited: link_limited,
                ..Default::default()
            };
            model.update(now, 0.25, round_samples, video_sent, false, video_sent + data_sent);
            estimates.push(model.bandwidth_bytes_per_sec);
        }
        estimates
    }

    #[test]
    fn video_alone_below_the_link_keeps_its_estimate() {
        // one 1 MB/s camera on a 2.5 MB/s link: never link-limited, so never leaves startup; video's samples count only
        // when higher than the estimate, and 10 s after the last record the window is empty and startup's
        // `measured.max(INITIAL_BANDWIDTH)` puts the estimate (and video's grant) back at 64 KB/s x gain
        let mut model = LinkModel::default();
        let estimates = page(&mut model, Instant::now(), 0..240, 2_500_000.0, 0.0, 1_000_000.0);
        let least = estimates[20..].iter().copied().fold(f64::INFINITY, f64::min);
        assert!(least >= 1_000_000.0, "settled at the camera's rate, then fell to {least} (starting_up {})", model.starting_up);
    }

    #[test]
    fn video_with_a_small_topic_keeps_its_estimate() {
        // a camera plus tf (2 KB/s): the data acks disable the video-only path, data is never held, so nothing is
        // link-limited and the estimate empties every 10 s
        let mut model = LinkModel::default();
        let estimates = page(&mut model, Instant::now(), 0..240, 2_500_000.0, 2_000.0, 1_000_000.0);
        let least = estimates[20..].iter().copied().fold(f64::INFINITY, f64::min);
        assert!(least >= 1_000_000.0, "fell to {least} (starting_up {})", model.starting_up);
    }

    #[test]
    fn a_page_under_the_link_leaves_startup() {
        // video_tf: a camera plus tf on a 2.5 MB/s link, neither ever held back, so no round is link-limited and BBR's
        // startup (which only counts link-limited rounds) never ended: no ProbeRTT, and 2.89x the estimate offered forever
        let mut model = LinkModel::default();
        let estimates = page(&mut model, Instant::now(), 0..240, 2_500_000.0, 20_000.0, 430_000.0);
        assert!(!model.starting_up, "estimates {:?}", &estimates[200..]);
        let least = estimates[40..].iter().copied().fold(f64::INFINITY, f64::min);
        assert!(least >= 450_000.0, "and the estimate stands: {least}");
    }

    #[test]
    fn data_below_the_link_keeps_its_estimate() {
        // 200 KB/s of data on a 2.5 MB/s link (clouds, no camera)
        let mut model = LinkModel::default();
        let estimates = page(&mut model, Instant::now(), 0..240, 2_500_000.0, 200_000.0, 0.0);
        let least = estimates[20..].iter().copied().fold(f64::INFINITY, f64::min);
        assert!(least >= 200_000.0, "fell to {least} (starting_up {})", model.starting_up);
    }

    #[test]
    fn video_held_by_gcc_leaves_startup() {
        // a 1 MB/s camera on a 250 KB/s link that delivers what fits without GCC braking: video_link_limited is false
        // every round, so startup (gain 2.89, video paced at 2.89x the link) never ends and ProbeRTT never runs
        let mut model = LinkModel::default();
        let estimates = page(&mut model, Instant::now(), 0..240, 250_000.0, 0.0, 1_000_000.0);
        assert!(!model.starting_up, "estimates {:?}", &estimates[200..]);
    }

    /// A page with only a camera wanting `camera` bytes/s on a `capacity` link with a drop-tail-free queue: video sends
    /// min(camera, budget, GCC's limit), the link drains the queue at `capacity`, and `video_held_by_link` judges each
    /// round from the queue left. Returns the bandwidth after each round.
    fn video_page(model: &mut LinkModel, start: Instant, rounds: std::ops::Range<u64>, capacity: f64, camera: f64, gcc: impl Fn(u64) -> Option<f64>) -> Vec<f64> {
        let mut queued = 0.0;
        let mut estimates = vec![];
        for round in rounds {
            let now = start + Duration::from_millis(250 * round);
            let (budget, _) = model.pacing(None);
            queued += camera.min(budget).min(gcc(round).unwrap_or(f64::INFINITY)) * 0.25;
            let delivered = queued.min(capacity * 0.25);
            queued -= delivered;
            let held = video_held_by_link(queued / capacity * 1000.0, DEFAULT_LATENCY_TARGET);
            model.update(now, 0.25, crate::pacing::RoundSamples::default(), delivered / 0.25, delivered > 0.0 && held, delivered / 0.25);
            estimates.push(model.bandwidth_bytes_per_sec);
        }
        estimates
    }

    #[test]
    fn video_held_by_gcc_under_a_slow_link_keeps_its_estimate() {
        // video_slow: a camera on a 125 KB/s link; startup's overshoot queued, GCC cut to 25 KB/s and held it there for
        // 20 s while the link sat idle. Counted as the link's rate, its rounds took the estimate to 2 KB/s.
        let mut model = LinkModel::default();
        let estimates = video_page(&mut model, Instant::now(), 0..240, 125_000.0, 1_000_000.0, |round| (16..96).contains(&round).then_some(25_000.0));
        let peak = estimates.iter().copied().fold(0.0, f64::max);
        let least = estimates[40..].iter().copied().fold(f64::INFINITY, f64::min);
        assert!(peak <= 1.25 * 125_000.0 && least >= 0.5 * peak, "peak {peak}, then {least}: {:?}", &estimates[40..]);
    }

    #[test]
    fn a_big_infrequent_message_is_sent_at_the_learned_rate() {
        // a 400 KB map every 20 s on a 2.5 MB/s link: each send is held by pacing (link-limited) and grows the
        // estimate, the burst too short for three stalled rounds; once its samples aged out the next map started again
        // from 64 KB/s
        let mut model = LinkModel::default();
        let start = Instant::now();
        let capacity = 2_500_000.0;
        let mut durations = vec![];
        let mut learned = vec![];
        for map in 0..4u64 {
            learned.push(model.bandwidth_bytes_per_sec);
            let mut remaining = 400_000.0;
            let mut round = map * 80;
            while remaining > 0.0 {
                let now = start + Duration::from_millis(250 * round);
                let (rate, _) = model.pacing(None);
                let sent = (rate.min(capacity) * 0.25).min(remaining);
                // (the last round ran out of map before pacing held it back)
                let held = sent < remaining;
                remaining -= sent;
                let samples = crate::pacing::RoundSamples {
                    link_limited_rate: Some(sent / 0.25).filter(|_| held),
                    app_limited_rate: Some(sent / 0.25).filter(|_| !held),
                    min_rtt_ms: Some(20.0),
                    typical_rtt_ms: Some(20.0),
                    path_rtt_ms: Some(20.0),
                    drained: true,
                    window_waited: held,
                    ..Default::default()
                };
                model.update(now, 0.25, samples, 0.0, false, sent / 0.25);
                round += 1;
            }
            durations.push((round - map * 80) as f64 * 0.25);
            // idle until the next map
            for idle in round..(map + 1) * 80 {
                model.update(start + Duration::from_millis(250 * idle), 0.25, crate::pacing::RoundSamples { drained: true, ..Default::default() }, 0.0, false, 0.0);
            }
        }
        // (out of startup it is paced at the learned rate, not startup's 2.89x: a burst shorter than a round can't show more)
        let at_learned = (400_000.0 / learned[3] / 0.25).ceil() * 0.25;
        assert!(learned[3] >= learned[1] && durations[3] <= at_learned, "400 KB on 2.5 MB/s, seconds per map: {durations:?}, learned {learned:?} (starting_up {})", model.starting_up);
    }

    #[test]
    fn idle_doesnt_loop_probe_rtt() {
        let mut model = LinkModel::default();
        let start = Instant::now();
        let now = run(&mut model, start, 0..40, 1_000_000.0, true);
        let mut probing = 0;
        for round in 1..=120u64 {
            model.update(now + Duration::from_millis(250 * round), 0.25, crate::pacing::RoundSamples { drained: true, ..Default::default() }, 0.0, false, 0.0);
            probing += model.probing_rtt() as u32;
        }
        assert!(probing <= 12, "30 s idle: ProbeRTT (window {MIN_WINDOW} B) in {probing} of 120 rounds");
    }

    #[test]
    fn probe_rtt_ends_while_strict_streams_keep_the_window_full() {
        // strict streams bypass the window and keep more than the minimum in flight: never `drained`; the comment in
        // update_round_trip says the probe ends after one round regardless, but probe_rtt_rounds only counts when
        // round_started == now, which the previous update never sets
        let mut model = LinkModel::default();
        let start = Instant::now();
        let now = run(&mut model, start, 0..40, 1_000_000.0, true);
        let mut probing_rounds = 0;
        for round in 1..=120u64 {
            let samples = crate::pacing::RoundSamples { drained: false, ..sample(1_000_000.0, true, 20.0) };
            model.update(now + Duration::from_millis(250 * round), 0.25, samples, 0.0, false, 1_000_000.0);
            probing_rounds += model.probing_rtt() as u32;
        }
        assert!(probing_rounds <= 12, "ProbeRTT (bulk window {MIN_WINDOW} B) for {probing_rounds} of 120 rounds");
    }

    #[test]
    fn a_few_seconds_stall_doesnt_empty_it() {
        let mut model = LinkModel::default();
        let start = Instant::now();
        run(&mut model, start, 0..40, 2_500_000.0, true);
        let before = model.bandwidth_bytes_per_sec;
        // 5 s of a stalled link: the sender held back, next to nothing acked
        run(&mut model, start, 40..48, 1_000.0, true);
        assert!(model.bandwidth_bytes_per_sec >= 0.5 * before, "a stall shorter than the window: {model:?}");
    }

    #[test]
    fn idle_rounds_dont_lower_it() {
        let mut model = LinkModel::default();
        let start = Instant::now();
        let now = run(&mut model, start, 0..40, 1_000_000.0, true);
        let before = model.bandwidth_bytes_per_sec;
        for round in 1..40 {
            model.update(now + Duration::from_millis(ROUND_MS * round), ROUND_MS as f64 / 1000.0, sample(400.0, false, 20.0), 0.0, false, 400.0);
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
            let now = start + Duration::from_millis(ROUND_MS * round as u64);
            let jitter = if round % 3 == 0 { 0.5 } else { 1.25 };
            model.update(now, ROUND_MS as f64 / 1000.0, sample(300_000.0 * jitter, true, 5.0 + (round % 7) as f64 * 60.0), 0.0, false, 300_000.0 * jitter);
        }
        assert!(model.bandwidth_bytes_per_sec >= settled, "{model:?}");
    }

    #[test]
    fn the_probe_cycle_asks_more_then_drains() {
        let mut model = LinkModel::default();
        let start = Instant::now();
        let now = run(&mut model, start, 0..40, 1_000_000.0, true);
        let gains: Vec<f64> = (1..=8)
            .map(|round| {
                model.update(now + Duration::from_millis(ROUND_MS * round), ROUND_MS as f64 / 1000.0, sample(1_000_000.0, true, 20.0), 0.0, false, 1_000_000.0);
                model.gain
            })
            .collect();
        assert_eq!(gains.iter().filter(|gain| **gain > 1.0).count(), 1);
        assert_eq!(gains.iter().filter(|gain| **gain < 1.0).count(), 1);
    }
}
