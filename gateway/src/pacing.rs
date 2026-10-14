//! One frontend's shared send path (SPEC "Pacing"). Every data frame is in flight from when it is handed to SCTP until
//! the page acks it: the page is part of the path, so a page that falls behind holds the sender back the same way a slow
//! link does. Bulk frames wait while:
//! - a strict stream is sending (strict streams never wait);
//! - what is in flight has reached the window (`allocator::LinkModel::window_bytes`);
//! - the frontend's pacing rate hasn't earned them yet (one token bucket for all its frames).
//!
//! A lossy frame is passed when the page acks a later frame of its own channel (each channel acks on its own timer, so
//! another channel's ack says nothing: counted lost, a slow channel's frames held the estimate at a twentieth of the link), or, when the page has acked nothing for the retransmission timeout, once it is that old (a lost tail).
//! Never by age alone while acks still come: frames waiting in a queue would count as lost and let more in.

use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Bulk chunk size bounds; the chunk is `CHUNK_MS` of the pacing rate, so a strict frame waits behind about that long.
pub const MIN_CHUNK_BYTES: usize = 4 * 1024;
pub const MAX_CHUNK_BYTES: usize = crate::frame::CHUNK_BYTES;
const CHUNK_MS: f64 = 4.0;
/// A lost frame is given up on after max(2 x RTT, this), as TCP's retransmission timer bottoms out at 200 ms (Linux).
const MIN_LOSS_TIMEOUT: Duration = Duration::from_millis(200);
/// The page acks a channel's frames at most this long after they arrive (client `ackDelayMs`), so a rate is sampled over
/// at least `MIN_SAMPLE_TIME`, and the ack delay is added to each sample's time: never above the link, at most ~9% under.
const ACK_DELAY: Duration = Duration::from_millis(50);
const MIN_SAMPLE_TIME: Duration = Duration::from_millis(10 * ACK_DELAY.as_millis() as u64);
/// A frame sent with at most this much in flight queued behind (almost) nothing of ours: its round trip is the path's.
const QUIET_BYTES: usize = 8 * 1024;
/// Acks are remembered this long to sample a rate over (longer than any round trip worth sampling over).
const ACK_HISTORY: Duration = Duration::from_secs(2);
/// The typical round trip is the median over this long.
const TYPICAL_RTT_WINDOW: Duration = Duration::from_secs(2);
/// Ack round trips are kept this long for their minimum.
const RTT_WINDOW: Duration = Duration::from_secs(10);

struct Frame {
    stream: usize,
    bytes: usize,
    lossy: bool,
    sent_at: Instant,
    /// the sender was held back by the link (not its own spacing) when it sent this
    link_limited: bool,
    /// at most `QUIET_BYTES` were in flight when it was sent
    quiet: bool,
}

/// One round of delivery-rate samples (draft-cheng-iccrg-delivery-rate-estimation): the highest from frames sent while
/// the link held the sender back, and from the rest (only telling when higher than the estimate).
#[derive(Debug, Default, Clone, Copy)]
pub struct RoundSamples {
    pub link_limited_rate: Option<f64>,
    pub app_limited_rate: Option<f64>,
    pub min_rtt_ms: Option<f64>,
    /// the median send-to-ack time over the last `TYPICAL_RTT_WINDOW`: what the window must cover on a path whose delay
    /// swings (Wi-Fi: 20-400 ms), where a window of bandwidth x the least delivered a tenth of the bandwidth
    pub typical_rtt_ms: Option<f64>,
    /// the path's own round trip: the median send-to-ack time, over `RTT_WINDOW`, of frames sent while at most
    /// `QUIET_BYTES` were in flight (they queued behind nothing of ours)
    pub path_rtt_ms: Option<f64>,
    /// the most acks ran ahead of bandwidth x time this round (bytes): they arrive in bursts (the page acks in batches,
    /// Wi-Fi aggregates frames), so the window needs this on top of bandwidth x round trip
    pub extra_acked: f64,
    /// what is in flight was within the window when the round ended
    pub drained: bool,
    /// a frame waited on the window this round (the link's or the latency target's)
    pub window_waited: bool,
}

struct Path {
    /// frames the page hasn't gotten past, by place in the send order
    frames: BTreeMap<u64, Frame>,
    next: u64,
    in_flight: usize,
    /// bytes the page acked, ever
    acked_total: u64,
    /// (when, send-to-ack ms) of recent acks
    rtts: std::collections::VecDeque<(Instant, f64)>,
    /// (when, send-to-ack ms) of acked frames sent while at most `QUIET_BYTES` were in flight
    quiet_rtts: std::collections::VecDeque<(Instant, f64)>,
    /// (when, acked_total) after each ack, for `ACK_HISTORY`
    ack_history: std::collections::VecDeque<(Instant, u64)>,
    /// the next bulk frame waited on the window or the pacing rate
    held: bool,
    round: RoundSamples,
    /// the bandwidth the link model last gave, and the current ack epoch: since when, and what was acked in it
    bandwidth: f64,
    epoch_started: Instant,
    epoch_acked: f64,
    window: usize,
    /// the window is the latency limit's, not the link's (see `configure`)
    latency_capped: bool,
    /// bytes/s; tokens may go negative by one frame
    rate: f64,
    tokens: f64,
    refilled: Instant,
}

pub struct SendGate {
    /// a frame the page hasn't acked this long is forgotten (see `Path::expire_lost`)
    stale_after: Duration,
    /// strict streams currently sending a message
    strict_busy: AtomicUsize,
    path: Mutex<Path>,
    chunk_bytes: AtomicUsize,
    /// one multi-chunk bulk message at a time (FIFO), see [`SendGate::message_turn`]
    message_turn: tokio::sync::Mutex<()>,
    changed: Notify,
}

impl Default for SendGate {
    fn default() -> Self {
        SendGate::new(crate::allocator::DEFAULT_STALE_AFTER)
    }
}

impl SendGate {
    pub fn new(stale_after: Duration) -> Self {
        let (rate, window) = crate::allocator::LinkModel::default().pacing(None);
        SendGate {
            stale_after,
            strict_busy: AtomicUsize::new(0),
            path: Mutex::new(Path {
                frames: BTreeMap::new(),
                next: 0,
                in_flight: 0,
                acked_total: 0,
                rtts: Default::default(),
                ack_history: Default::default(),
                quiet_rtts: Default::default(),
                held: false,
                round: RoundSamples::default(),
                bandwidth: crate::allocator::INITIAL_BANDWIDTH,
                epoch_started: Instant::now(),
                epoch_acked: 0.0,
                window,
                latency_capped: false,
                rate,
                tokens: 0.0,
                refilled: Instant::now(),
            }),
            chunk_bytes: AtomicUsize::new(chunk_for(rate)),
            message_turn: tokio::sync::Mutex::new(()),
            changed: Notify::new(),
        }
    }
}

fn chunk_for(rate: f64) -> usize {
    ((rate * CHUNK_MS / 1000.0) as usize).clamp(MIN_CHUNK_BYTES, MAX_CHUNK_BYTES)
}

/// Holds the gate shut for bulk streams while a strict stream sends.
pub struct StrictTurn<'a>(&'a SendGate);

impl Drop for StrictTurn<'_> {
    fn drop(&mut self) {
        self.0.strict_busy.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}

impl Path {
    fn refill(&mut self, now: Instant) {
        // at most one chunk's worth saved up while idle, so an idle frontend can't burst
        let capacity = chunk_for(self.rate) as f64;
        self.tokens = (self.tokens + self.rate * now.duration_since(self.refilled).as_secs_f64()).min(capacity);
        self.refilled = now;
    }

    /// A lost tail: a lossy frame no later frame of its channel passed, older than twice the slowest recent round trip
    /// (queueing included, so a frame waiting in a queue isn't counted lost) plus the page's ack delay, never will be.
    /// Not only while the page is silent: other channels' acks keep coming, and a channel that went quiet after losing
    /// its last frame held the window full and stalled every stream.
    /// Also forgets any frame, reliable ones too, the page hasn't acked in `stale_after`: a page that stopped acking
    /// without disconnecting must not hold the window (and every bulk sender) shut forever.
    fn expire_lost(&mut self, now: Instant, stale_after: Duration) {
        let high_rtt = self.rtts.iter().map(|(_, rtt)| *rtt).reduce(f64::max).unwrap_or(0.0);
        let timeout = crate::secs(2.0 * high_rtt / 1000.0).max(MIN_LOSS_TIMEOUT) + ACK_DELAY;
        let lost: Vec<u64> = self
            .frames
            .iter()
            .filter(|(_, frame)| (frame.lossy && now.duration_since(frame.sent_at) > timeout) || now.duration_since(frame.sent_at) > stale_after)
            .map(|(order, _)| *order)
            .collect();
        for order in lost {
            self.remove(order);
        }
    }

    fn remove(&mut self, order: u64) -> Option<Frame> {
        let frame = self.frames.remove(&order)?;
        self.in_flight -= frame.bytes;
        Some(frame)
    }
}

impl SendGate {
    /// From the allocator: the link's bandwidth and the pacing rate (bytes/s), and the most that may be in flight.
    /// `latency_capped`: the latency limit set the window, so waiting on it isn't the link holding the sender back.
    pub fn configure(&self, bandwidth: f64, rate: f64, window: usize, latency_capped: bool) {
        let mut path = self.path.lock();
        let now = Instant::now();
        path.refill(now);
        path.bandwidth = bandwidth;
        path.rate = rate;
        path.window = window;
        path.latency_capped = latency_capped;
        self.chunk_bytes.store(chunk_for(rate), Ordering::Relaxed);
        drop(path);
        self.changed.notify_waiters();
    }

    pub fn chunk_bytes(&self) -> usize {
        self.chunk_bytes.load(Ordering::Relaxed)
    }

    /// How long an unanswered frame (or anything else of this frontend's) is waited on: `ServerBuilder::stale_after`.
    pub fn stale_after(&self) -> Duration {
        self.stale_after
    }

    /// Sizes of what the gate holds, for the stats' memory section.
    pub fn counts(&self) -> serde_json::Value {
        let path = self.path.lock();
        serde_json::json!({
            "frames": path.frames.len(),
            "inFlightBytes": path.in_flight,
            "rtts": path.rtts.len(),
            "quietRtts": path.quiet_rtts.len(),
            "ackHistory": path.ack_history.len(),
        })
    }

    /// Bytes the page has acked, ever.
    pub fn acked_total(&self) -> u64 {
        self.path.lock().acked_total
    }

    pub fn in_flight(&self) -> usize {
        self.path.lock().in_flight
    }

    /// The round's samples since the previous call: delivery rates, the least send-to-ack time (the path's round trip,
    /// if a frame didn't queue), and how far acks ran ahead of the bandwidth.
    pub fn take_round(&self) -> RoundSamples {
        let mut path = self.path.lock();
        let now = Instant::now();
        while path.rtts.front().is_some_and(|(at, _)| now.duration_since(*at) > RTT_WINDOW) {
            path.rtts.pop_front();
        }
        let drained = path.in_flight <= path.window.max(2 * self.chunk_bytes());
        let mut recent: Vec<f64> = path.rtts.iter().filter(|(at, _)| now.duration_since(*at) <= TYPICAL_RTT_WINDOW).map(|(_, rtt)| *rtt).collect();
        recent.sort_by(f64::total_cmp);
        let typical_rtt_ms = recent.get(recent.len() / 2).copied();
        while path.quiet_rtts.front().is_some_and(|(at, _)| now.duration_since(*at) > RTT_WINDOW) {
            path.quiet_rtts.pop_front();
        }
        let mut quiet: Vec<f64> = path.quiet_rtts.iter().map(|(_, rtt)| *rtt).collect();
        quiet.sort_by(f64::total_cmp);
        let path_rtt_ms = quiet.get(quiet.len() / 2).copied();
        RoundSamples { drained, typical_rtt_ms, path_rtt_ms, ..std::mem::take(&mut path.round) }
    }

    /// The right to send one message bigger than a bulk chunk, held until it is sent whole; waiters get it in arrival
    /// order. Big messages of several streams sent at once interleave their chunks, so each takes as long as all of them
    /// together; one at a time, each takes its own size / rate, and a stream that waited sends its newest sample.
    pub async fn message_turn(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.message_turn.lock().await
    }

    pub fn strict_turn(&self) -> StrictTurn<'_> {
        self.strict_busy.fetch_add(1, Ordering::AcqRel);
        StrictTurn(self)
    }

    /// A frame went to SCTP; returns its place in the frontend's send order. Every frame spends tokens, so bulk yields to
    /// what strict streams sent.
    pub fn frame_sent(&self, stream: usize, bytes: usize, lossy: bool) -> u64 {
        let mut path = self.path.lock();
        let now = Instant::now();
        path.refill(now);
        path.tokens -= bytes as f64;
        let order = path.next;
        path.next += 1;
        let quiet = path.in_flight <= QUIET_BYTES;
        path.in_flight += bytes;
        let frame = Frame { stream, bytes, lossy, sent_at: now, link_limited: std::mem::take(&mut path.held), quiet };
        path.frames.insert(order, frame);
        order
    }

    /// The page acked these frames of one channel (in send order): it is past every lossy frame sent before them too.
    pub fn frames_acked(&self, orders: impl IntoIterator<Item = u64>) {
        let mut path = self.path.lock();
        let now = Instant::now();
        let mut newest = None;
        let acked_before = path.acked_total;
        let mut newest_acked: Option<Frame> = None;
        for order in orders {
            if let Some(frame) = path.remove(order) {
                path.acked_total += frame.bytes as u64;
                let rtt = now.duration_since(frame.sent_at).as_secs_f64() * 1000.0;
                path.rtts.push_back((now, rtt));
                if frame.quiet {
                    path.quiet_rtts.push_back((now, rtt));
                }
                path.round.min_rtt_ms = Some(path.round.min_rtt_ms.map_or(rtt, |least| least.min(rtt)));
                newest_acked = Some(frame);
            }
            newest = Some(order);
        }
        // one rate sample per ack: what was acked since the ack at least a round trip and `MIN_SAMPLE_TIME` back, over that
        // time (each channel acks on its own timer, up to `ACK_DELAY` after arrival, so over a round trip alone a burst of
        // late acks read up to 2x the link); link-limited when the newest frame it covers was held back by the link
        let acked_total = path.acked_total;
        path.ack_history.push_back((now, acked_total));
        while path.ack_history.front().is_some_and(|(at, _)| now.duration_since(*at) > ACK_HISTORY) {
            path.ack_history.pop_front();
        }
        if let Some(frame) = &newest_acked {
            let min_rtt = path.rtts.iter().map(|(_, rtt)| *rtt).reduce(f64::min).unwrap_or(0.0) / 1000.0;
            let span = min_rtt.max(MIN_SAMPLE_TIME.as_secs_f64());
            let start = path.ack_history.iter().rev().find(|(at, _)| now.duration_since(*at).as_secs_f64() >= span).copied();
            if let Some((at, acked_then)) = start {
                // (over the time plus the ack delay: what this ack covers may have arrived that much earlier, and the highest
                // sample wins, so the unpadded time read the link 1.3x fast on a 500 kbit/s link)
                let rate = (acked_total - acked_then) as f64 / (now.duration_since(at) + ACK_DELAY).as_secs_f64();
                let slot = if frame.link_limited { &mut path.round.link_limited_rate } else { &mut path.round.app_limited_rate };
                *slot = Some(slot.map_or(rate, |most| most.max(rate)));
            }
        }
        // ack aggregation, as Linux's BBR measures it: acked since the epoch began beyond bandwidth x its length; an epoch
        // whose acks fell behind the bandwidth starts over
        let acked = (path.acked_total - acked_before) as f64;
        if acked > 0.0 {
            let expected = path.bandwidth * now.duration_since(path.epoch_started).as_secs_f64();
            if path.epoch_acked <= expected {
                (path.epoch_started, path.epoch_acked) = (now, 0.0);
            }
            path.epoch_acked += acked;
            let expected = path.bandwidth * now.duration_since(path.epoch_started).as_secs_f64();
            let extra = (path.epoch_acked - expected).clamp(0.0, path.window as f64);
            path.round.extra_acked = path.round.extra_acked.max(extra);
        }
        if let Some((newest, stream)) = newest.zip(newest_acked.as_ref().map(|frame| frame.stream)) {
            let lost: Vec<u64> = path.frames.range(..newest).filter(|(_, frame)| frame.lossy && frame.stream == stream).map(|(order, _)| *order).collect();
            for order in lost {
                path.remove(order);
            }
        }
        drop(path);
        self.changed.notify_waiters();
    }

    /// Frames of a closed channel the page will never ack.
    pub fn frames_dropped(&self, orders: impl IntoIterator<Item = u64>) {
        let mut path = self.path.lock();
        for order in orders {
            path.remove(order);
        }
        drop(path);
        self.changed.notify_waiters();
    }

    /// Waits until a bulk frame of `bytes` may go; returns how long it waited on the window (the rest is pacing).
    pub async fn wait_bulk_turn(&self, bytes: usize) -> Duration {
        let mut on_window = Duration::ZERO;
        loop {
            let notified = self.changed.notified();
            let wait = {
                let mut path = self.path.lock();
                let now = Instant::now();
                path.expire_lost(now, self.stale_after);
                path.refill(now);
                // (at least two chunks: one on the wire, one queued behind it)
                let window = path.window.max(2 * self.chunk_bytes());
                let window_open = path.in_flight == 0 || path.in_flight + bytes <= window;
                if self.strict_busy.load(Ordering::Acquire) > 0 || !window_open {
                    path.held |= !window_open && !path.latency_capped;
                    path.round.window_waited |= !window_open;
                    // woken by an ack or a strict turn ending; looked at again after the loss timeout for a lost tail
                    (true, MIN_LOSS_TIMEOUT)
                } else if path.tokens < 0.0 {
                    path.held = true;
                    (false, crate::secs(-path.tokens / path.rate.max(1.0)))
                } else {
                    return on_window;
                }
            };
            let started = Instant::now();
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(wait.1) => {}
            }
            if wait.0 {
                on_window += started.elapsed();
            }
        }
    }

    /// Lets go of every frame (the peer is gone).
    pub fn forget_all(&self) {
        let mut path = self.path.lock();
        path.frames.clear();
        path.in_flight = 0;
        drop(path);
        self.changed.notify_waiters();
    }

    /// A closed channel's frames.
    pub fn forget(&self, stream: usize) {
        let mut path = self.path.lock();
        let orders: Vec<u64> = path.frames.iter().filter(|(_, frame)| frame.stream == stream).map(|(order, _)| *order).collect();
        for order in orders {
            path.remove(order);
        }
        drop(path);
        self.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(rate: f64, window: usize) -> SendGate {
        let gate = SendGate::default();
        gate.configure(rate, rate, window, false);
        gate
    }

    #[test]
    fn in_flight_counts_until_the_page_acks_and_an_ack_passes_earlier_lossy_frames() {
        let gate = gate(1e6, 10_000);
        let reliable = gate.frame_sent(1, 100, false);
        let lost = gate.frame_sent(2, 1000, true);
        let acked = gate.frame_sent(2, 1000, true);
        let later = gate.frame_sent(2, 1000, true);
        assert_eq!(gate.in_flight(), 3100);
        gate.frames_acked([acked]);
        assert_eq!(gate.in_flight(), 1100, "the lossy frame before it was lost; the reliable one still waits for its own ack");
        assert_eq!(gate.acked_total(), 1000, "a lost frame isn't delivered");
        gate.frames_acked([lost]);
        assert_eq!(gate.in_flight(), 1100, "a late ack of a passed frame changes nothing");
        gate.frames_acked([reliable]);
        gate.frames_dropped([later]);
        assert_eq!(gate.in_flight(), 0);
        assert!(gate.take_round().min_rtt_ms.is_some());
    }

    #[test]
    fn another_channels_ack_passes_no_frame() {
        let gate = gate(1e6, 10_000);
        gate.frame_sent(1, 1000, true);
        let other = gate.frame_sent(2, 1000, true);
        gate.frames_acked([other]);
        assert_eq!(gate.in_flight(), 1000, "channel 1 acks on its own timer: its frame isn't lost yet");
    }

    #[test]
    fn a_lost_tail_times_out() {
        let gate = gate(1e6, 10_000);
        gate.frame_sent(1, 100, true);
        gate.frame_sent(2, 100, false);
        let mut path = gate.path.lock();
        path.expire_lost(Instant::now() + MIN_LOSS_TIMEOUT / 2, gate.stale_after);
        assert_eq!(path.in_flight, 200, "younger than the loss timeout: maybe still queued");
        path.expire_lost(Instant::now() + MIN_LOSS_TIMEOUT + ACK_DELAY + Duration::from_millis(1), gate.stale_after);
        assert_eq!(path.in_flight, 100, "the lossy tail is lost; the reliable frame waits (SCTP resends it)");
        path.expire_lost(Instant::now() + gate.stale_after + Duration::from_millis(1), gate.stale_after);
        assert_eq!(path.in_flight, 0, "unacked past stale_after: forgotten, the page stopped answering");
    }

    #[tokio::test]
    async fn bulk_waits_for_the_window_and_strict_turns() {
        // a window under two chunks counts as two chunks (8 KB at this rate)
        let gate = std::sync::Arc::new(gate(1e6, 2000));
        assert_eq!(gate.wait_bulk_turn(5000).await, Duration::ZERO, "an empty window always admits one frame");
        let first = gate.frame_sent(1, 5000, false);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let waiting = tokio::spawn({
            let gate = gate.clone();
            async move { gate.wait_bulk_turn(5000).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished(), "10 KB > the 8 KB window");
        gate.frames_acked([first]);
        assert!(waiting.await.unwrap() >= Duration::from_millis(15));
        let turn = gate.strict_turn();
        assert!(tokio::time::timeout(Duration::from_millis(20), gate.wait_bulk_turn(10)).await.is_err(), "bulk waits while strict sends");
        drop(turn);
        assert_eq!(gate.wait_bulk_turn(10).await, Duration::ZERO);
    }

    #[tokio::test]
    async fn bulk_is_paced_at_the_rate() {
        let gate = gate(100_000.0, usize::MAX);
        gate.frame_sent(1, 10_000, true);
        let started = Instant::now();
        gate.wait_bulk_turn(10_000).await;
        let waited = started.elapsed().as_secs_f64();
        assert!((0.09..0.2).contains(&waited), "10 KB at 100 KB/s: {waited}");
    }

    #[test]
    fn chunks_are_a_few_ms_of_the_rate() {
        assert_eq!(chunk_for(1_000_000.0), MIN_CHUNK_BYTES);
        assert_eq!(chunk_for(1e9), MAX_CHUNK_BYTES);
    }
}
