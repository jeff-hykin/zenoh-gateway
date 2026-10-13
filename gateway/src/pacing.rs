//! Keeping one frontend's shared path short, so a strict-priority stream never waits behind bulk data (SPEC "Pacing"):
//! - bulk streams send chunk by chunk through a per-frontend [`SendGate`]: no chunk starts while a strict stream is
//!   sending, and bulk bytes in SCTP stay under a small in-flight limit (a strict frame waits behind ~one chunk);
//! - each bulk stream is paced by a [`TokenBucket`] at its granted rate, in chunks of a few ms of the budget.

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::Mutex;
use webrtc::data_channel::DataChannel;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Bulk chunk size bounds; the chunk is sized to `CHUNK_MS` of the frontend's budget.
pub const MIN_CHUNK_BYTES: usize = 4 * 1024;
pub const MAX_CHUNK_BYTES: usize = crate::frame::CHUNK_BYTES;
const CHUNK_MS: f64 = 4.0;
/// Re-check while waiting, in case a wakeup is missed (outstanding bytes drain without an event).
const GATE_BACKSTOP: Duration = Duration::from_millis(5);
/// Token-bucket rate = granted rate x this, so a message finishes a little before the next is due.
pub const PACING_SLACK: f64 = 1.25;
/// A channel's send window ([`SendGate::window_bytes`]) is the budget times this many round trips, within bounds.
const WINDOW_RTTS: f64 = 2.0;
pub const MAX_WINDOW_BYTES: usize = 4 * 1024 * 1024;
/// The RTT the window covers is at most the path's minimum plus this, so the window's own queue can't grow it.
const WINDOW_MAX_EXTRA_RTT_MS: f64 = 500.0;
/// Channels wait while the page is further behind than this ([`SendGate::page_lag`]): the backlog a frontend's page may
/// hold. Bytes alone can't bound it: a window sized for the network is seconds of backlog for a phone that decodes
/// slower than the link delivers, and its heartbeat replies wait behind all of it in its one event loop.
pub const MAX_PAGE_LAG_MS: f64 = 250.0;
/// Channels also wait while a frame has been in SCTP undelivered for longer than the path's recent highest RTT (at most
/// its minimum + `NETWORK_LAG_MAX_EXTRA_RTT_MS`) plus this ([`SendGate::network_lag`]). SCTP sends unordered chunks
/// before ordered ones, so a `reliable` stream's frames wait behind every lossy chunk queued after them: without this
/// bound a tf feed sat 13 s in SCTP on a jittery path while bulk kept its unordered queue full.
const NETWORK_LAG_MARGIN_MS: f64 = 250.0;
const NETWORK_LAG_MAX_EXTRA_RTT_MS: f64 = 500.0;
/// A frame lost on the way stays in SCTP until its retransmission timer (at least 1 s, rtc-sctp's RTO_MIN) resends or
/// abandons it, so the gate allows this much more: at 2% loss it was shut nearly all the time and clouds got 7 of 369
/// messages out.
const LOSS_NETWORK_LAG_EXTRA: Duration = Duration::from_millis(1000);

/// Every frame of a frontend's `sub` channels the page hasn't gotten past, in send order. The page is one event loop, so
/// its backlog is all its channels' together. An ack of any frame means the page is past every *lossy* frame sent before
/// it (SCTP sends unordered chunks first and in order, so one sent earlier and still unacked was lost); a reliable frame
/// is never lost but can wait in SCTP behind lossy ones sent after it, so only its own channel's ack passes it.
#[derive(Default)]
struct PageQueue {
    next: u64,
    unpassed: std::collections::BTreeMap<u64, PageFrame>,
    /// the page has gotten past every frame sent before this one
    passed: u64,
    /// frame bytes handed to SCTP so far, by stream
    sent_bytes: HashMap<usize, usize>,
    /// of those, how many its peer has acknowledged, by stream
    delivered_bytes: HashMap<usize, usize>,
    /// acknowledged bytes over every stream, ever (the estimator's delivery rate)
    delivered_total: u64,
}

struct PageFrame {
    sent_at: Instant,
    lossy: bool,
    stream: usize,
    /// the stream's bytes handed to SCTP up to and including this frame
    end_offset: usize,
    /// when SCTP was first seen to have delivered it to the browser (its peer acknowledged it)
    delivered_at: Option<Instant>,
}

pub struct SendGate {
    /// strict streams currently sending a message
    strict_busy: AtomicUsize,
    /// bulk bytes outstanding in SCTP, by subscription
    outstanding: Mutex<HashMap<usize, usize>>,
    /// each bulk subscription's channel, so a waiter can refresh everyone's outstanding bytes
    channels: Mutex<HashMap<usize, Arc<dyn DataChannel>>>,
    inflight_limit: AtomicUsize,
    chunk_bytes: AtomicUsize,
    window_bytes: AtomicUsize,
    page: Mutex<PageQueue>,
    /// [`SendGate::max_network_lag`] in microseconds
    max_network_lag_us: AtomicU64,
    /// the path's recent round trip (see `set_window`) in microseconds: the page's acks cross it too
    network_rtt_us: AtomicU64,
    /// one multi-chunk bulk message at a time (FIFO), see [`SendGate::message_turn`]
    message_turn: tokio::sync::Mutex<()>,
    changed: Notify,
}

impl Default for SendGate {
    fn default() -> Self {
        SendGate {
            strict_busy: AtomicUsize::new(0),
            outstanding: Mutex::new(HashMap::new()),
            channels: Mutex::new(HashMap::new()),
            inflight_limit: AtomicUsize::new(usize::MAX),
            chunk_bytes: AtomicUsize::new(MAX_CHUNK_BYTES),
            window_bytes: AtomicUsize::new(crate::subscription::BACKED_UP_BYTES),
            page: Mutex::default(),
            max_network_lag_us: AtomicU64::new(((NETWORK_LAG_MAX_EXTRA_RTT_MS + NETWORK_LAG_MARGIN_MS) * 1000.0) as u64),
            network_rtt_us: AtomicU64::new((NETWORK_LAG_MAX_EXTRA_RTT_MS * 1000.0) as u64),
            message_turn: tokio::sync::Mutex::new(()),
            changed: Notify::new(),
        }
    }
}

/// Holds the gate shut for bulk streams while a strict stream sends.
pub struct StrictTurn<'a>(&'a SendGate);

impl Drop for StrictTurn<'_> {
    fn drop(&mut self) {
        self.0.strict_busy.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}

impl SendGate {
    /// Bytes of every `sub` channel the browser has acknowledged, ever.
    pub fn delivered_total(&self) -> u64 {
        self.page.lock().delivered_total
    }

    /// From the allocator: the budget sets bulk chunk size and the in-flight limit, budget x (min RTT + `slack_ms`), about
    /// one bandwidth-delay product. The limit only protects strict streams: without one (`!limit_inflight`) there is none.
    pub fn configure(&self, budget_bytes_per_sec: f64, min_rtt_ms: Option<f64>, slack_ms: f64, limit_inflight: bool) {
        let chunk = ((budget_bytes_per_sec * CHUNK_MS / 1000.0) as usize).clamp(MIN_CHUNK_BYTES, MAX_CHUNK_BYTES);
        let window = budget_bytes_per_sec * (min_rtt_ms.unwrap_or(50.0) + slack_ms) / 1000.0;
        self.chunk_bytes.store(chunk, Ordering::Relaxed);
        let limit = if limit_inflight { (window as usize).max(2 * chunk) } else { usize::MAX };
        self.inflight_limit.store(limit, Ordering::Relaxed);
        self.changed.notify_waiters();
    }

    /// From the allocator: each channel's send window, about two bandwidth-delay products at the path's recent highest
    /// RTT (a fixed 64 KB held a Wi-Fi path with 400 ms RTT swings to ~150 KB/s, and SCTP's cwnd never grew past it).
    pub fn set_window(&self, budget_bytes_per_sec: f64, min_rtt_ms: Option<f64>, high_rtt_ms: Option<f64>) {
        let rtt_ms = match (min_rtt_ms, high_rtt_ms) {
            (Some(min), Some(high)) => high.min(min + WINDOW_MAX_EXTRA_RTT_MS),
            (min, high) => high.or(min).unwrap_or(0.0),
        };
        let window = (budget_bytes_per_sec * WINDOW_RTTS * rtt_ms / 1000.0) as usize;
        self.window_bytes.store(window.clamp(crate::subscription::BACKED_UP_BYTES, MAX_WINDOW_BYTES), Ordering::Relaxed);
        let network_rtt_ms = match (min_rtt_ms, high_rtt_ms) {
            (Some(min), Some(high)) => high.clamp(min, min + NETWORK_LAG_MAX_EXTRA_RTT_MS),
            _ => NETWORK_LAG_MAX_EXTRA_RTT_MS,
        };
        self.max_network_lag_us.store(((network_rtt_ms + NETWORK_LAG_MARGIN_MS) * 1000.0) as u64, Ordering::Relaxed);
        self.network_rtt_us.store((network_rtt_ms * 1000.0) as u64, Ordering::Relaxed);
    }

    /// How long a frame may wait in SCTP undelivered before channels stop sending.
    pub fn max_network_lag(&self) -> Duration {
        Duration::from_micros(self.max_network_lag_us.load(Ordering::Relaxed))
    }

    /// How long the oldest frame SCTP hasn't delivered to the browser has been in it.
    pub fn network_lag(&self, now: Instant) -> Duration {
        let page = self.page.lock();
        page.unpassed.values().filter(|frame| frame.delivered_at.is_none()).map(|frame| frame.sent_at).min().map_or(Duration::ZERO, |at| now.duration_since(at))
    }

    /// The path holds a frame longer than it should: past `max_network_lag` plus `LOSS_NETWORK_LAG_EXTRA`.
    pub fn network_lagging(&self, now: Instant) -> bool {
        self.network_lag(now) > self.max_network_lag() + LOSS_NETWORK_LAG_EXTRA
    }

    /// How far behind the page may be before channels stop sending: `MAX_PAGE_LAG_MS` plus the path's round trip, since
    /// page lag runs until the page's ack arrives, which crosses the network back (on the R1's Wi-Fi, 60-650 ms of network
    /// read as a 564 ms page lag, and the senders waited on a page that was keeping up).
    pub fn max_page_lag(&self) -> Duration {
        Duration::from_secs_f64(MAX_PAGE_LAG_MS / 1000.0) + Duration::from_micros(self.network_rtt_us.load(Ordering::Relaxed))
    }

    /// The right to send one message bigger than a bulk chunk, held until it is sent whole; waiters get it in arrival
    /// order. Big messages of several streams sent at once interleave their chunks, so each takes as long as all of them
    /// together (three 200 KB clouds and a map on a 500 KB/s link: every one arrived ~1.7 s old); one at a time, each
    /// takes its own size / rate, and a stream that waited sends its newest sample.
    pub async fn message_turn(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.message_turn.lock().await
    }

    /// A frame went to the page; returns its place in the frontend's send order.
    pub fn frame_sent(&self, stream: usize, bytes: usize, lossy: bool) -> u64 {
        let mut page = self.page.lock();
        let order = page.next;
        page.next += 1;
        let sent = page.sent_bytes.entry(stream).or_default();
        *sent += bytes;
        let end_offset = *sent;
        page.unpassed.insert(order, PageFrame { sent_at: Instant::now(), lossy, stream, end_offset, delivered_at: None });
        order
    }

    /// SCTP holds `outstanding` bytes of `stream` its peer hasn't acknowledged: the frames before them reached the browser.
    pub fn observe_delivered(&self, stream: usize, outstanding: usize, now: Instant) {
        let mut page = self.page.lock();
        let delivered = page.sent_bytes.get(&stream).copied().unwrap_or(0).saturating_sub(outstanding);
        let previous = page.delivered_bytes.insert(stream, delivered).unwrap_or(0);
        page.delivered_total += delivered.saturating_sub(previous) as u64;
        for frame in page.unpassed.values_mut() {
            if frame.stream == stream && frame.end_offset <= delivered && frame.delivered_at.is_none() {
                frame.delivered_at = Some(now);
            }
        }
    }

    /// Reads every channel's unacknowledged bytes (`observe_delivered`).
    pub async fn refresh_delivered(&self) {
        let channels: Vec<(usize, Arc<dyn DataChannel>)> = self.channels.lock().iter().map(|(id, channel)| (*id, channel.clone())).collect();
        for (id, channel) in channels {
            let outstanding = channel.outstanding_bytes().await.unwrap_or(0);
            self.observe_delivered(id, outstanding, Instant::now());
        }
    }

    /// The page acked these frames of one channel (in send order): it is past every lossy frame sent before them too.
    pub fn frames_acked(&self, orders: impl IntoIterator<Item = u64>) {
        let mut page = self.page.lock();
        let mut newest = None;
        for order in orders {
            page.unpassed.remove(&order);
            newest = Some(order);
        }
        if let Some(newest) = newest {
            page.unpassed.retain(|order, frame| *order > newest || !frame.lossy);
            page.passed = page.passed.max(newest + 1);
        }
    }

    /// The page has gotten past every lossy frame sent before this place in the send order: any of them still unacked
    /// was lost. It only grows, so a change means the page made progress.
    pub fn passed(&self) -> u64 {
        self.page.lock().passed
    }

    /// Frames of a closed channel the page will never ack.
    pub fn frames_dropped(&self, orders: impl IntoIterator<Item = u64>) {
        let mut page = self.page.lock();
        for order in orders {
            page.unpassed.remove(&order);
        }
    }

    /// How long the page has had the oldest frame it hasn't gotten past: from when SCTP delivered it to the browser, so the
    /// network's own delay (a Wi-Fi path's RTT swinging 60-480 ms) doesn't count, only the page's backlog.
    pub fn page_lag(&self, now: Instant) -> Duration {
        let page = self.page.lock();
        page.unpassed.values().filter_map(|frame| frame.delivered_at).min().map_or(Duration::ZERO, |at| now.duration_since(at))
    }

    /// Bytes a channel may have in SCTP (sent, not yet acknowledged) before it waits.
    pub fn window_bytes(&self) -> usize {
        self.window_bytes.load(Ordering::Relaxed)
    }

    /// The bulk in-flight limit, if any.
    pub fn inflight_limit(&self) -> Option<usize> {
        Some(self.inflight_limit.load(Ordering::Relaxed)).filter(|limit| *limit != usize::MAX)
    }

    pub fn chunk_bytes(&self) -> usize {
        self.chunk_bytes.load(Ordering::Relaxed)
    }

    pub fn strict_turn(&self) -> StrictTurn<'_> {
        self.strict_busy.fetch_add(1, Ordering::AcqRel);
        StrictTurn(self)
    }

    pub fn report_outstanding(&self, stream: usize, bytes: usize) {
        set_outstanding(&mut self.outstanding.lock(), stream, bytes);
        self.changed.notify_waiters();
    }

    /// Admits `bytes` of bulk if the gate is open, counting them at once so senders woken together can't all burst.
    fn try_admit(&self, stream: usize, bytes: usize) -> bool {
        if self.strict_busy.load(Ordering::Acquire) > 0 {
            return false;
        }
        let mut outstanding = self.outstanding.lock();
        let total: usize = outstanding.values().sum();
        if total >= self.inflight_limit.load(Ordering::Relaxed) {
            return false;
        }
        *outstanding.entry(stream).or_default() += bytes;
        true
    }

    pub fn register(&self, stream: usize, channel: Arc<dyn DataChannel>) {
        self.channels.lock().insert(stream, channel);
    }

    /// Re-reads every bulk channel's outstanding bytes (idle streams don't report while SCTP drains them).
    async fn refresh_all(&self) {
        let channels: Vec<(usize, Arc<dyn DataChannel>)> = self.channels.lock().iter().map(|(id, channel)| (*id, channel.clone())).collect();
        let mut readings = Vec::with_capacity(channels.len());
        for (id, channel) in channels {
            readings.push((id, channel.outstanding_bytes().await.unwrap_or(0)));
        }
        let mut outstanding = self.outstanding.lock();
        for (id, bytes) in readings {
            set_outstanding(&mut outstanding, id, bytes);
        }
    }

    /// Waits until a bulk chunk of `bytes` from `stream` may go.
    pub async fn wait_bulk_turn(&self, stream: usize, bytes: usize) -> Duration {
        let started = Instant::now();
        loop {
            let notified = self.changed.notified();
            if self.try_admit(stream, bytes) {
                return started.elapsed();
            }
            tokio::select! {
                _ = notified => {}
                _ = tokio::time::sleep(GATE_BACKSTOP) => {}
            }
            self.refresh_all().await;
        }
    }

    /// Lets go of every channel: they hold the peer connection, whose handler holds the peer state that holds this gate.
    pub fn forget_all(&self) {
        self.channels.lock().clear();
        self.outstanding.lock().clear();
        *self.page.lock() = PageQueue::default();
        self.changed.notify_waiters();
    }

    pub fn forget(&self, stream: usize) {
        self.channels.lock().remove(&stream);
        let mut page = self.page.lock();
        page.sent_bytes.remove(&stream);
        page.delivered_bytes.remove(&stream);
        drop(page);
        self.report_outstanding(stream, 0);
    }
}

fn set_outstanding(outstanding: &mut HashMap<usize, usize>, stream: usize, bytes: usize) {
    if bytes == 0 {
        outstanding.remove(&stream);
    } else {
        outstanding.insert(stream, bytes);
    }
}

/// Smooth pacing at a rate; `None` rate = unpaced.
#[derive(Debug)]
pub struct TokenBucket {
    rate: Option<f64>,
    tokens: f64,
    capacity: f64,
    refilled: Instant,
}

impl Default for TokenBucket {
    fn default() -> Self {
        TokenBucket { rate: None, tokens: 0.0, capacity: MAX_CHUNK_BYTES as f64, refilled: Instant::now() }
    }
}

impl TokenBucket {
    pub fn set_rate(&mut self, rate: Option<f64>, capacity: usize) {
        self.refill(Instant::now());
        self.rate = rate.map(|rate| rate.max(1000.0));
        self.capacity = capacity as f64;
        self.tokens = self.tokens.min(self.capacity);
    }

    fn refill(&mut self, now: Instant) {
        if let Some(rate) = self.rate {
            self.tokens = (self.tokens + rate * now.duration_since(self.refilled).as_secs_f64()).min(self.capacity);
        }
        self.refilled = now;
    }

    /// Takes `bytes` (may go into debt by one chunk); returns how long to wait before sending.
    pub fn take(&mut self, bytes: usize, now: Instant) -> Duration {
        let Some(rate) = self.rate else { return Duration::ZERO };
        self.refill(now);
        let wait = if self.tokens >= 0.0 { Duration::ZERO } else { crate::secs(-self.tokens / rate) };
        self.tokens -= bytes as f64;
        wait
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_paces_at_its_rate() {
        let mut bucket = TokenBucket::default();
        let start = Instant::now();
        assert_eq!(bucket.take(10_000, start), Duration::ZERO, "unpaced");
        bucket.set_rate(Some(100_000.0), 10_000);
        bucket.tokens = 0.0;
        bucket.refilled = start;
        assert_eq!(bucket.take(10_000, start), Duration::ZERO, "first chunk goes, into debt");
        let wait = bucket.take(10_000, start);
        assert!((wait.as_secs_f64() - 0.1).abs() < 1e-6, "next waits 10 KB / 100 KB/s: {wait:?}");
        assert!(bucket.take(10_000, start + Duration::from_millis(100)) > Duration::ZERO);
    }

    #[test]
    fn send_window_covers_two_round_trips_at_the_highest_rtt() {
        let gate = SendGate::default();
        assert_eq!(gate.window_bytes(), crate::subscription::BACKED_UP_BYTES, "64 KB before any RTT is known");
        gate.set_window(1_000_000.0, Some(10.0), Some(400.0));
        assert_eq!(gate.window_bytes(), 800_000, "1 MB/s x 2 x 400 ms");
        gate.set_window(1_000_000.0, Some(10.0), Some(2000.0));
        assert_eq!(gate.window_bytes(), 1_020_000, "at most the minimum RTT + 500 ms");
        gate.set_window(100_000.0, Some(10.0), Some(20.0));
        assert_eq!(gate.window_bytes(), crate::subscription::BACKED_UP_BYTES, "never below 64 KB");
        gate.set_window(1e9, Some(10.0), Some(400.0));
        assert_eq!(gate.window_bytes(), MAX_WINDOW_BYTES);
    }

    #[test]
    fn page_lag_counts_from_delivery_to_the_browser_and_acks_pass_lossy_frames() {
        let gate = SendGate::default();
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        let (tf, cloud) = (1, 2);
        assert_eq!(gate.page_lag(at(0)), Duration::ZERO);
        let reliable = gate.frame_sent(tf, 100, false);
        let lost = gate.frame_sent(cloud, 1000, true);
        let first = gate.frame_sent(cloud, 1000, true);
        let second = gate.frame_sent(cloud, 1000, true);
        assert_eq!(gate.page_lag(at(400)), Duration::ZERO, "nothing has reached the browser yet: the network's delay");
        gate.observe_delivered(tf, 0, at(100));
        gate.observe_delivered(cloud, 1000, at(150));
        assert_eq!(gate.page_lag(at(400)), Duration::from_millis(300), "delivered at 100 ms, not yet acked");
        // an ack of a later frame (any channel) means the page is past the earlier lossy ones: lost or acked soon
        gate.frames_acked([first]);
        assert_eq!(gate.page_lag(at(400)), Duration::from_millis(300), "a reliable frame still counts");
        gate.frames_acked([reliable]);
        assert_eq!(gate.page_lag(at(400)), Duration::ZERO, "{second} hasn't reached the browser");
        gate.observe_delivered(cloud, 0, at(200));
        assert_eq!(gate.page_lag(at(400)), Duration::from_millis(200));
        gate.frames_acked([lost]);
        assert_eq!(gate.page_lag(at(400)), Duration::from_millis(200), "a late ack of an older frame changes nothing");
        gate.frames_dropped([second]);
        assert_eq!(gate.page_lag(at(400)), Duration::ZERO, "a closed channel's frames don't hold the rest");
    }

    #[test]
    fn network_lag_is_the_oldest_frame_still_in_sctp() {
        let gate = SendGate::default();
        gate.set_window(1e6, Some(40.0), Some(300.0));
        assert_eq!(gate.max_network_lag(), Duration::from_millis(550), "recent highest RTT + 250 ms");
        gate.set_window(1e6, Some(40.0), Some(5000.0));
        assert_eq!(gate.max_network_lag(), Duration::from_millis(790), "at most min + 500 ms + 250 ms");
        let t0 = Instant::now();
        gate.frame_sent(1, 100, false);
        gate.frame_sent(2, 100, true);
        assert!(gate.network_lag(t0 + Duration::from_secs(1)) >= Duration::from_millis(990));
        gate.observe_delivered(1, 0, t0);
        gate.observe_delivered(2, 0, t0);
        assert_eq!(gate.network_lag(t0 + Duration::from_secs(1)), Duration::ZERO, "both reached the browser");
    }

    #[test]
    fn a_lost_frame_may_wait_for_the_retransmission_timer() {
        let gate = SendGate::default();
        gate.set_window(1e6, Some(40.0), Some(300.0));
        let t0 = Instant::now();
        gate.frame_sent(1, 100, true);
        assert!(!gate.network_lagging(t0 + Duration::from_millis(1200)), "lost, waiting on the retransmission timer");
        assert!(gate.network_lagging(t0 + Duration::from_millis(1600)));
    }

    #[test]
    fn gate_sizes_chunks_and_blocks_bulk_during_strict_turns() {
        let gate = SendGate::default();
        gate.configure(1_000_000.0, Some(10.0), 5.0, true);
        assert_eq!(gate.chunk_bytes(), MIN_CHUNK_BYTES);
        assert_eq!(gate.inflight_limit.load(Ordering::Relaxed), 15_000);
        assert!(gate.try_admit(1, 10_000));
        assert!(gate.try_admit(2, 10_000), "below the limit when admitted");
        assert!(!gate.try_admit(3, 1000), "admitted bytes count at once, before any send");
        gate.forget(1);
        gate.forget(2);
        let turn = gate.strict_turn();
        assert!(!gate.try_admit(1, 1000));
        drop(turn);
        assert!(gate.try_admit(1, 1000));
        gate.configure(1e9, Some(1.0), 5.0, true);
        assert_eq!(gate.chunk_bytes(), MAX_CHUNK_BYTES);
        // a jittery path widens the window by its jitter allowance
        gate.configure(1_000_000.0, Some(10.0), 50.0, true);
        assert_eq!(gate.inflight_limit(), Some(60_000));
        // no strict stream to protect: no limit
        gate.configure(1_000_000.0, Some(10.0), 50.0, false);
        assert_eq!(gate.inflight_limit(), None);
        assert!(gate.try_admit(1, 10_000_000));
    }
}
