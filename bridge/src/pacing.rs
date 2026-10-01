//! Keeping one frontend's shared path (wifi queue, UDP, SCTP congestion window) short, so a
//! strict-priority stream never waits behind bulk data (SPEC "Bandwidth allocation", "Pacing"):
//! - bulk (non-strict) streams send chunk by chunk through a per-frontend [`SendGate`]: no chunk
//!   starts while a strict stream is sending, and bulk bytes outstanding in SCTP stay under a
//!   small in-flight limit, so a strict frame waits behind at most about one bulk chunk;
//! - each bulk stream is paced by a [`TokenBucket`] at its granted rate, a smooth trickle instead of
//!   whole-message bursts; chunks are sized to a few milliseconds of the frontend's budget.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use webrtc::data_channel::DataChannel;
use std::sync::atomic::{AtomicUsize, Ordering};
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

pub struct SendGate {
    /// strict streams currently sending a message
    strict_busy: AtomicUsize,
    /// bulk bytes outstanding in SCTP, by subscription
    outstanding: Mutex<HashMap<usize, usize>>,
    /// each bulk subscription's channel, so a waiter can refresh everyone's outstanding bytes
    channels: Mutex<HashMap<usize, Arc<dyn DataChannel>>>,
    inflight_limit: AtomicUsize,
    chunk_bytes: AtomicUsize,
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
    /// From the allocator: the budget sets bulk chunk size and in-flight limit, budget x (min RTT +
    /// `slack_ms`): about one bandwidth-delay product, so bulk can use its budget without standing
    /// in a queue.
    /// The limit exists so a strict stream waits behind little bulk; `limit_inflight` is false when the
    /// frontend has no strict stream, and then bulk is limited only by its pacing and SCTP.
    pub fn configure(&self, budget_bytes_per_sec: f64, min_rtt_ms: Option<f64>, slack_ms: f64, limit_inflight: bool) {
        let chunk = ((budget_bytes_per_sec * CHUNK_MS / 1000.0) as usize).clamp(MIN_CHUNK_BYTES, MAX_CHUNK_BYTES);
        let window = budget_bytes_per_sec * (min_rtt_ms.unwrap_or(50.0) + slack_ms) / 1000.0;
        self.chunk_bytes.store(chunk, Ordering::Relaxed);
        let limit = if limit_inflight { (window as usize).max(2 * chunk) } else { usize::MAX };
        self.inflight_limit.store(limit, Ordering::Relaxed);
        self.changed.notify_waiters();
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
        let mut outstanding = self.outstanding.lock().unwrap();
        if bytes == 0 {
            outstanding.remove(&stream);
        } else {
            outstanding.insert(stream, bytes);
        }
        drop(outstanding);
        self.changed.notify_waiters();
    }

    /// Admits `bytes` of bulk if the gate is open, counting them as outstanding right away, so
    /// bulk senders woken together can't all pass and burst into the link.
    fn try_admit(&self, stream: usize, bytes: usize) -> bool {
        if self.strict_busy.load(Ordering::Acquire) > 0 {
            return false;
        }
        let mut outstanding = self.outstanding.lock().unwrap();
        let total: usize = outstanding.values().sum();
        if total >= self.inflight_limit.load(Ordering::Relaxed) {
            return false;
        }
        *outstanding.entry(stream).or_default() += bytes;
        true
    }

    pub fn register(&self, stream: usize, channel: Arc<dyn DataChannel>) {
        self.channels.lock().unwrap().insert(stream, channel);
    }

    /// Re-reads every bulk channel's outstanding bytes: idle streams don't report on their own,
    /// and SCTP drains their bytes meanwhile.
    async fn refresh_all(&self) {
        let channels: Vec<(usize, Arc<dyn DataChannel>)> = self.channels.lock().unwrap().iter().map(|(id, channel)| (*id, channel.clone())).collect();
        let mut readings = Vec::with_capacity(channels.len());
        for (id, channel) in channels {
            readings.push((id, channel.outstanding_bytes().await.unwrap_or(0)));
        }
        let mut outstanding = self.outstanding.lock().unwrap();
        for (id, bytes) in readings {
            if bytes == 0 {
                outstanding.remove(&id);
            } else {
                outstanding.insert(id, bytes);
            }
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

    pub fn forget(&self, stream: usize) {
        self.channels.lock().unwrap().remove(&stream);
        self.report_outstanding(stream, 0);
    }
}

/// Smooth pacing at a rate; `None` rate = unpaced.
#[derive(Debug)]
pub struct TokenBucket {
    rate: Option<f64>,
    tokens: f64,
    depth: f64,
    refilled: Instant,
}

impl Default for TokenBucket {
    fn default() -> Self {
        TokenBucket { rate: None, tokens: 0.0, depth: MAX_CHUNK_BYTES as f64, refilled: Instant::now() }
    }
}

impl TokenBucket {
    pub fn set_rate(&mut self, rate: Option<f64>, depth: usize) {
        self.refill(Instant::now());
        self.rate = rate.map(|rate| rate.max(1000.0));
        self.depth = depth as f64;
        self.tokens = self.tokens.min(self.depth);
    }

    fn refill(&mut self, now: Instant) {
        if let Some(rate) = self.rate {
            self.tokens = (self.tokens + rate * now.duration_since(self.refilled).as_secs_f64()).min(self.depth);
        }
        self.refilled = now;
    }

    /// Takes `bytes` (may go into debt by one chunk); returns how long to wait before sending.
    pub fn take(&mut self, bytes: usize, now: Instant) -> Duration {
        let Some(rate) = self.rate else { return Duration::ZERO };
        self.refill(now);
        let wait = if self.tokens >= 0.0 { Duration::ZERO } else { Duration::from_secs_f64(-self.tokens / rate) };
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
