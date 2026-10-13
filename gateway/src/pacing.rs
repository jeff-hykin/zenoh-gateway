//! One frontend's shared send path (SPEC "Pacing"). Every data frame is in flight from when it is handed to SCTP until
//! the page acks it: the page is part of the path, so a page that falls behind holds the sender back the same way a slow
//! link does. Bulk frames wait while:
//! - a strict stream is sending (strict streams never wait);
//! - what is in flight has reached the window (`allocator::LinkModel::window_bytes`);
//! - the frontend's pacing rate hasn't earned them yet (one token bucket for all its frames).
//!
//! A lossy frame is passed when the page acks a frame sent after it (SCTP sends unordered chunks first and in order, so
//! it was lost), or, when the page has acked nothing for the retransmission timeout, once it is that old (a lost tail).
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
/// Ack round trips are kept this long for their minimum.
const RTT_WINDOW: Duration = Duration::from_secs(10);

struct Frame {
    stream: usize,
    bytes: usize,
    lossy: bool,
    sent_at: Instant,
    /// for its delivery-rate sample: what had been acked when it was sent, when the latest of that was acked, the send
    /// time of the first frame of that flight, and whether the sender was held back by the link (not its own spacing)
    acked_before: u64,
    acked_before_at: Instant,
    flight_started: Instant,
    link_limited: bool,
}

/// One round of delivery-rate samples (draft-cheng-iccrg-delivery-rate-estimation): the highest from frames sent while
/// the link held the sender back, and from the rest (only telling when higher than the estimate).
#[derive(Debug, Default, Clone, Copy)]
pub struct RoundSamples {
    pub link_limited_rate: Option<f64>,
    pub app_limited_rate: Option<f64>,
    pub min_rtt_ms: Option<f64>,
    /// the most acks ran ahead of bandwidth x time this round (bytes): they arrive in bursts (the page acks in batches,
    /// Wi-Fi aggregates frames), so the window needs this on top of bandwidth x round trip
    pub extra_acked: f64,
    /// what is in flight was within the window when the round ended
    pub drained: bool,
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
    last_ack_at: Option<Instant>,
    /// send time of the frame the latest ack covered (the start of the flight now being acked)
    flight_started: Instant,
    /// the next bulk frame waited on the window or the pacing rate
    held: bool,
    round: RoundSamples,
    /// the bandwidth the link model last gave, and the current ack epoch: since when, and what was acked in it
    bandwidth: f64,
    epoch_started: Instant,
    epoch_acked: f64,
    window: usize,
    /// bytes/s; tokens may go negative by one frame
    rate: f64,
    tokens: f64,
    refilled: Instant,
}

pub struct SendGate {
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
        let (rate, window) = crate::allocator::LinkModel::default().pacing();
        SendGate {
            strict_busy: AtomicUsize::new(0),
            path: Mutex::new(Path {
                frames: BTreeMap::new(),
                next: 0,
                in_flight: 0,
                acked_total: 0,
                rtts: Default::default(),
                last_ack_at: None,
                flight_started: Instant::now(),
                held: false,
                round: RoundSamples::default(),
                bandwidth: crate::allocator::INITIAL_BANDWIDTH,
                epoch_started: Instant::now(),
                epoch_acked: 0.0,
                window,
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

    /// A lost tail: with the page silent for the loss timeout, lossy frames that old will never be acked.
    fn expire_lost(&mut self, now: Instant) {
        let high_rtt = self.rtts.iter().map(|(_, rtt)| *rtt).reduce(f64::max).unwrap_or(0.0);
        let timeout = crate::secs(2.0 * high_rtt / 1000.0).max(MIN_LOSS_TIMEOUT);
        if self.last_ack_at.is_some_and(|at| now.duration_since(at) <= timeout) {
            return;
        }
        let lost: Vec<u64> = self.frames.iter().filter(|(_, frame)| frame.lossy && now.duration_since(frame.sent_at) > timeout).map(|(order, _)| *order).collect();
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
    pub fn configure(&self, bandwidth: f64, rate: f64, window: usize) {
        let mut path = self.path.lock();
        let now = Instant::now();
        path.refill(now);
        path.bandwidth = bandwidth;
        path.rate = rate;
        path.window = window;
        self.chunk_bytes.store(chunk_for(rate), Ordering::Relaxed);
        drop(path);
        self.changed.notify_waiters();
    }

    pub fn chunk_bytes(&self) -> usize {
        self.chunk_bytes.load(Ordering::Relaxed)
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
        RoundSamples { drained, ..std::mem::take(&mut path.round) }
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
        path.in_flight += bytes;
        if path.frames.is_empty() {
            // nothing in flight: a new flight starts now (an idle gap isn't delivery time)
            path.flight_started = now;
        }
        let frame = Frame {
            stream,
            bytes,
            lossy,
            sent_at: now,
            acked_before: path.acked_total,
            acked_before_at: path.last_ack_at.filter(|_| !path.frames.is_empty()).unwrap_or(now),
            flight_started: path.flight_started,
            link_limited: std::mem::take(&mut path.held),
        };
        path.frames.insert(order, frame);
        order
    }

    /// The page acked these frames of one channel (in send order): it is past every lossy frame sent before them too.
    pub fn frames_acked(&self, orders: impl IntoIterator<Item = u64>) {
        let mut path = self.path.lock();
        let now = Instant::now();
        path.last_ack_at = Some(now);
        let mut newest = None;
        let acked_before = path.acked_total;
        let mut newest_acked: Option<Frame> = None;
        for order in orders {
            if let Some(frame) = path.remove(order) {
                path.acked_total += frame.bytes as u64;
                let rtt = now.duration_since(frame.sent_at).as_secs_f64() * 1000.0;
                path.rtts.push_back((now, rtt));
                path.round.min_rtt_ms = Some(path.round.min_rtt_ms.map_or(rtt, |least| least.min(rtt)));
                newest_acked = Some(frame);
            }
            newest = Some(order);
        }
        // one rate sample per ack, from the newest frame it covers: what was acked since that frame was sent, over the
        // longer of its flight's send time and ack time, and never over less than a round trip (an ack burst isn't a rate)
        if let Some(frame) = newest_acked {
            let delivered = (path.acked_total - frame.acked_before) as f64;
            let interval = frame.sent_at.duration_since(frame.flight_started).max(now.duration_since(frame.acked_before_at)).as_secs_f64();
            let min_rtt = path.rtts.iter().map(|(_, rtt)| *rtt).reduce(f64::min).unwrap_or(0.0) / 1000.0;
            if interval > 0.0 && interval >= min_rtt {
                let rate = delivered / interval;
                let slot = if frame.link_limited { &mut path.round.link_limited_rate } else { &mut path.round.app_limited_rate };
                *slot = Some(slot.map_or(rate, |most| most.max(rate)));
            }
            path.flight_started = frame.sent_at;
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
        if let Some(newest) = newest {
            let lost: Vec<u64> = path.frames.range(..newest).filter(|(_, frame)| frame.lossy).map(|(order, _)| *order).collect();
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
                path.expire_lost(now);
                path.refill(now);
                // (at least two chunks: one on the wire, one queued behind it)
                let window = path.window.max(2 * self.chunk_bytes());
                let window_open = path.in_flight == 0 || path.in_flight + bytes <= window;
                if self.strict_busy.load(Ordering::Acquire) > 0 || !window_open {
                    path.held |= !window_open;
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
        gate.configure(rate, rate, window);
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
    fn a_lost_tail_times_out() {
        let gate = gate(1e6, 10_000);
        gate.frame_sent(1, 100, true);
        gate.frame_sent(2, 100, false);
        let mut path = gate.path.lock();
        let later = Instant::now() + MIN_LOSS_TIMEOUT + Duration::from_millis(1);
        path.last_ack_at = Some(later);
        path.expire_lost(later);
        assert_eq!(path.in_flight, 200, "acks still coming: a frame that waits in a queue isn't lost");
        path.last_ack_at = None;
        path.expire_lost(later);
        assert_eq!(path.in_flight, 100, "the page silent: the lossy tail is lost; the reliable frame never is (SCTP resends it)");
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
