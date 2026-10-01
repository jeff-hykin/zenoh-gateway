//! One `sub` data channel: a zenoh AdvancedSubscriber feeding per-key delivery queues, drained
//! into the data channel only while it is not backed up.

use crate::frame;
use crate::options::{Delivery, Label, SubOpts};
use log::{debug, warn};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use zenoh::bytes::ZBytes;
use zenoh::sample::Sample;
use zenoh_ext::{AdvancedSubscriberBuilderExt, HistoryConfig};

/// Above this many unacknowledged bytes the channel counts as backed up.
pub const BACKED_UP_BYTES: usize = 64 * 1024;
/// Resume sending once the channel drains to this.
pub const RESUME_BYTES: usize = 32 * 1024;
/// Backstop re-check while backed up, in case a low-water event is missed.
const BACKSTOP: Duration = Duration::from_millis(20);
/// Max bytes sent but not yet acknowledged as consumed by the page's JS (see `ack`).
/// SCTP's buffered amount only covers the network: a busy browser main thread acks SCTP
/// on its network thread and then queues messages internally without limit.
pub const UNCONSUMED_WINDOW: usize = 256 * 1024;
/// A sent frame never acked for this long is assumed lost (lossy channels), not unconsumed.
const ASSUME_LOST_AFTER: Duration = Duration::from_secs(1);
/// Lossy channels blocked only by the ack window send one probe frame after this (doubling
/// while no ack comes back): if the window's tail was lost, the probe's ack releases it.
const FIRST_PROBE_AFTER: Duration = Duration::from_millis(50);
const MAX_PROBE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubStats {
    pub received: u64,
    pub sent: u64,
    pub dropped_queue: u64,
    pub dropped_age: u64,
    pub dropped_send_error: u64,
    pub queued: usize,
    pub queued_bytes: usize,
    pub outstanding_bytes: usize,
    pub backed_up: bool,
    pub unconsumed_bytes: usize,
    /// frames (chunks) handed to SCTP; `sent` counts whole messages
    pub frames_sent: u64,
    /// lossy channel stopped sending a chunked message that outlived maxAge
    pub abandoned_partial: u64,
    /// lossy channel blocked by the ack window sent a frame anyway (tail-loss recovery)
    pub probes: u64,
    /// time the sender waited for SCTP to release bytes (network/peer receive path)
    pub blocked_on_network_ms: f64,
    /// time the sender waited only for the page to consume what it already has
    pub blocked_on_page_ms: f64,
    /// Bridge receive time minus sample timestamp (upstream lag), largest seen.
    pub max_receive_lag_ms: f64,
}

struct Pending {
    payload: ZBytes,
    timestamp_ms: f64,
    seq: u32,
    arrived: Instant,
    priority: u8,
}

#[derive(Default)]
struct KeyQueue {
    items: VecDeque<Pending>,
    last_sent: Option<Instant>,
}

#[derive(Default)]
struct SubState {
    keys: HashMap<String, KeyQueue>,
    next_seq: u32,
    stats: SubStats,
    /// (frameId, frame bytes, sent at) in send order, until the page acks a frameId at or past it
    unconsumed: VecDeque<(u32, usize, Instant)>,
    /// acks that released something; lets a blocked sender notice progress
    acks_with_progress: u64,
}

impl SubState {
    fn forget_lost(&mut self, now: Instant) {
        while let Some(&(_, len, sent_at)) = self.unconsumed.front() {
            if now.duration_since(sent_at) < ASSUME_LOST_AFTER {
                break;
            }
            self.unconsumed.pop_front();
            self.stats.unconsumed_bytes -= len;
        }
    }
}

/// `a <= b` for wrapping u32 sequence numbers.
fn seq_at_or_before(a: u32, b: u32) -> bool {
    b.wrapping_sub(a) < u32::MAX / 2
}

pub struct SubShared {
    delivery: Delivery,
    min_interval: Option<Duration>,
    priority_override: Option<u8>,
    state: Mutex<SubState>,
    data_ready: Notify,
    drained: Notify,
    closed: AtomicBool,
}

pub fn now_unix_ms() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0)
}

fn sample_timestamp_ms(sample: &Sample) -> f64 {
    sample
        .timestamp()
        .and_then(|ts| ts.get_time().to_system_time().duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or_else(now_unix_ms)
}

impl SubShared {
    pub fn new(opts: &SubOpts) -> Self {
        SubShared {
            delivery: opts.delivery(),
            min_interval: opts.min_interval(),
            priority_override: opts.zenoh_priority().map(|p| p as u8),
            state: Mutex::new(SubState::default()),
            data_ready: Notify::new(),
            drained: Notify::new(),
            closed: AtomicBool::new(false),
        }
    }

    pub fn stats(&self) -> SubStats {
        self.state.lock().unwrap().stats.clone()
    }

    fn push(&self, sample: Sample) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        let payload_len = sample.payload().len();
        {
            let mut state = self.state.lock().unwrap();
            let seq = state.next_seq;
            state.next_seq = state.next_seq.wrapping_add(1);
            state.stats.received += 1;
            state.stats.queued += 1;
            state.stats.queued_bytes += payload_len;
            let timestamp_ms = sample_timestamp_ms(&sample);
            state.stats.max_receive_lag_ms = state.stats.max_receive_lag_ms.max(now_unix_ms() - timestamp_ms);
            let pending = Pending {
                timestamp_ms,
                seq,
                arrived: Instant::now(),
                priority: self.priority_override.unwrap_or(sample.priority() as u8),
                payload: sample.payload().clone(),
            };
            let key = sample.key_expr().as_str();
            let state = &mut *state;
            let queue = match state.keys.get_mut(key) {
                Some(queue) => queue,
                None => state.keys.entry(key.to_owned()).or_default(),
            };
            queue.items.push_back(pending);
            if let Some(cap) = self.delivery.queue {
                while queue.items.len() > cap {
                    let dropped = queue.items.pop_front().unwrap();
                    state.stats.dropped_queue += 1;
                    state.stats.queued -= 1;
                    state.stats.queued_bytes -= dropped.payload.len();
                }
            }
        }
        self.data_ready.notify_one();
    }

    /// Next frame to send (highest priority, then oldest), or when to look again.
    fn pick(&self, now: Instant) -> (Option<(String, Pending)>, Option<Instant>) {
        let mut state = self.state.lock().unwrap();
        let state = &mut *state;
        if let Some(max_age_ms) = self.delivery.max_age_ms {
            let max_age = Duration::from_secs_f64(max_age_ms / 1000.0);
            for queue in state.keys.values_mut() {
                while queue.items.front().is_some_and(|item| now.duration_since(item.arrived) > max_age) {
                    let dropped = queue.items.pop_front().unwrap();
                    state.stats.dropped_age += 1;
                    state.stats.queued -= 1;
                    state.stats.queued_bytes -= dropped.payload.len();
                }
            }
        }
        let mut best: Option<(&String, (u8, Instant))> = None;
        let mut wake_at: Option<Instant> = None;
        for (key, queue) in state.keys.iter() {
            let Some(head) = queue.items.front() else { continue };
            if let (Some(interval), Some(last_sent)) = (self.min_interval, queue.last_sent) {
                let ready_at = last_sent + interval;
                if ready_at > now {
                    wake_at = Some(wake_at.map_or(ready_at, |w| w.min(ready_at)));
                    continue;
                }
            }
            let rank = (head.priority, head.arrived);
            if best.as_ref().is_none_or(|(_, best_rank)| rank < *best_rank) {
                best = Some((key, rank));
            }
        }
        let Some((key, _)) = best else { return (None, wake_at) };
        let key = key.clone();
        let queue = state.keys.get_mut(&key).unwrap();
        let item = queue.items.pop_front().unwrap();
        queue.last_sent = Some(now);
        state.stats.queued -= 1;
        state.stats.queued_bytes -= item.payload.len();
        (Some((key, item)), wake_at)
    }

    /// Page consumed every frame up to `frame_id` (4-byte little endian message on the sub channel).
    fn ack(&self, frame_id: u32) {
        let mut state = self.state.lock().unwrap();
        let mut released = false;
        while let Some(&(sent_frame_id, len, _)) = state.unconsumed.front() {
            if !seq_at_or_before(sent_frame_id, frame_id) {
                break;
            }
            state.unconsumed.pop_front();
            state.stats.unconsumed_bytes -= len;
            released = true;
        }
        if released {
            state.acks_with_progress += 1;
        }
        drop(state);
        self.drained.notify_one();
    }

    /// Older than this subscription's maxAge (never, without one).
    fn expired(&self, item: &Pending, now: Instant) -> bool {
        self.delivery.max_age_ms.is_some_and(|max_age_ms| now.duration_since(item.arrived).as_secs_f64() * 1000.0 > max_age_ms)
    }

    fn unconsumed_bytes(&self) -> usize {
        let mut state = self.state.lock().unwrap();
        state.forget_lost(Instant::now());
        state.stats.unconsumed_bytes
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.data_ready.notify_one();
        self.drained.notify_one();
    }
}

/// Runs a `sub` channel until it closes.
pub async fn run(dc: Arc<dyn DataChannel>, label: Label, session: zenoh::Session, shared: Arc<SubShared>) {
    let _ = dc.set_buffered_amount_low_threshold(RESUME_BYTES as u32).await;
    let feed = shared.clone();
    let subscriber = session
        .declare_subscriber(label.key.clone())
        .callback(move |sample| feed.push(sample))
        .advanced()
        .history(HistoryConfig::default().detect_late_publishers().max_samples(1))
        .await;
    let subscriber = match subscriber {
        Ok(subscriber) => subscriber,
        Err(error) => {
            warn!("subscribe {:?} failed: {error}", label.key);
            let _ = dc.close().await;
            return;
        }
    };
    debug!("subscribed {:?} with {:?}", label.key, shared.delivery);

    let sender = tokio::spawn(send_loop(dc.clone(), shared.clone()));
    while let Some(event) = dc.poll().await {
        match event {
            DataChannelEvent::OnBufferedAmountLow => shared.drained.notify_one(),
            DataChannelEvent::OnMessage(message) if message.data.len() == 4 => {
                shared.ack(u32::from_le_bytes([message.data[0], message.data[1], message.data[2], message.data[3]]));
            }
            DataChannelEvent::OnClose => break,
            _ => {}
        }
    }
    shared.close();
    let _ = sender.await;
    drop(subscriber);
    debug!("unsubscribed {:?}", label.key);
}

/// Paces one channel: tracks SCTP backlog, the page's ack window and tail-loss probes.
struct Pacer {
    probe_interval: Duration,
    acks_seen: u64,
}

impl Pacer {
    /// After a frame went out: wait while the network or the page is behind.
    async fn wait_writable(&mut self, dc: &Arc<dyn DataChannel>, shared: &SubShared) {
        let mut outstanding = dc.outstanding_bytes().await.unwrap_or(0);
        let unconsumed = {
            let mut state = shared.state.lock().unwrap();
            state.stats.outstanding_bytes = outstanding;
            state.stats.unconsumed_bytes
        };
        if outstanding < BACKED_UP_BYTES && unconsumed < UNCONSUMED_WINDOW {
            return;
        }
        shared.state.lock().unwrap().stats.backed_up = true;
        let mut probe_at = Instant::now() + self.probe_interval;
        while (outstanding > RESUME_BYTES || shared.unconsumed_bytes() >= UNCONSUMED_WINDOW)
            && !shared.closed.load(Ordering::Relaxed)
        {
            let waiting_on_network = outstanding > RESUME_BYTES;
            let wait_started = Instant::now();
            tokio::select! {
                _ = shared.drained.notified() => {}
                _ = tokio::time::sleep(BACKSTOP) => {}
            }
            let waited_ms = wait_started.elapsed().as_secs_f64() * 1000.0;
            let acks_now = {
                let mut state = shared.state.lock().unwrap();
                if waiting_on_network {
                    state.stats.blocked_on_network_ms += waited_ms;
                } else {
                    state.stats.blocked_on_page_ms += waited_ms;
                }
                state.acks_with_progress
            };
            outstanding = dc.outstanding_bytes().await.unwrap_or(0);
            if acks_now != self.acks_seen {
                self.acks_seen = acks_now;
                self.probe_interval = FIRST_PROBE_AFTER;
                probe_at = Instant::now() + self.probe_interval;
            }
            let window_only = outstanding <= RESUME_BYTES;
            if window_only && !shared.delivery.reliable && Instant::now() >= probe_at {
                shared.state.lock().unwrap().stats.probes += 1;
                // if this probe isn't acked either, the page is busy rather than the tail lost
                self.probe_interval = (self.probe_interval * 2).min(MAX_PROBE_INTERVAL);
                break;
            }
        }
        let mut state = shared.state.lock().unwrap();
        state.stats.outstanding_bytes = outstanding;
        state.stats.backed_up = false;
    }
}

async fn send_loop(dc: Arc<dyn DataChannel>, shared: Arc<SubShared>) {
    let mut warned_send_error = false;
    let mut pacer = Pacer { probe_interval: FIRST_PROBE_AFTER, acks_seen: 0 };
    let mut next_frame_id: u32 = 0;
    while !shared.closed.load(Ordering::Relaxed) {
        let (next, wake_at) = shared.pick(Instant::now());
        let Some((key, item)) = next else {
            match wake_at {
                Some(at) => {
                    tokio::select! {
                        _ = shared.data_ready.notified() => {}
                        _ = tokio::time::sleep_until(at.into()) => {}
                    }
                }
                None => shared.data_ready.notified().await,
            }
            continue;
        };
        let payload = item.payload.to_bytes();
        let chunk_count = frame::chunk_count(payload.len());
        let mut completed = true;
        for chunk_index in 0..chunk_count {
            // a lossy channel stops a chunked message that outlived maxAge; otherwise it finishes
            // what it started, so big messages make progress even when newer ones keep arriving
            if chunk_index > 0 && !shared.delivery.reliable && shared.expired(&item, Instant::now()) {
                shared.state.lock().unwrap().stats.abandoned_partial += 1;
                completed = false;
                break;
            }
            let frame_id = next_frame_id;
            next_frame_id = next_frame_id.wrapping_add(1);
            let header = frame::FrameHeader { key: &key, timestamp_ms: item.timestamp_ms, seq: item.seq, frame_id, chunk_index, chunk_count };
            let frame = frame::encode(&header, frame::chunk(&payload, chunk_index));
            let frame_len = frame.len();
            if let Err(error) = dc.send(frame).await {
                if shared.closed.load(Ordering::Relaxed) {
                    return;
                }
                shared.state.lock().unwrap().stats.dropped_send_error += 1;
                if !warned_send_error {
                    warn!("send on {key:?} failed ({frame_len} bytes): {error}");
                    warned_send_error = true;
                }
                if matches!(error, webrtc::error::Error::ErrDataChannelClosed) {
                    return;
                }
                completed = false;
                break;
            }
            {
                let mut state = shared.state.lock().unwrap();
                state.unconsumed.push_back((frame_id, frame_len, Instant::now()));
                state.stats.unconsumed_bytes += frame_len;
                state.stats.frames_sent += 1;
            }
            pacer.wait_writable(&dc, &shared).await;
            if shared.closed.load(Ordering::Relaxed) {
                return;
            }
        }
        if completed {
            shared.state.lock().unwrap().stats.sent += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared(opts: &str) -> SubShared {
        SubShared::new(&SubOpts::parse(&serde_json::from_str(opts).unwrap()).unwrap())
    }

    fn pending(seq: u32, priority: u8, arrived: Instant) -> Pending {
        Pending { payload: ZBytes::from(vec![0u8; 10]), timestamp_ms: 0.0, seq, arrived, priority }
    }

    fn insert(shared: &SubShared, key: &str, item: Pending, cap: Option<usize>) {
        let mut state = shared.state.lock().unwrap();
        state.stats.queued += 1;
        state.stats.queued_bytes += item.payload.len();
        let queue = state.keys.entry(key.to_owned()).or_default();
        queue.items.push_back(item);
        if let Some(cap) = cap {
            while queue.items.len() > cap {
                queue.items.pop_front();
            }
        }
    }

    #[test]
    fn priority_then_age() {
        let shared = shared(r#"{"delivery":"reliable"}"#);
        let t0 = Instant::now();
        insert(&shared, "a/low", pending(0, 6, t0), None);
        insert(&shared, "a/high", pending(1, 2, t0 + Duration::from_millis(1)), None);
        let (first, _) = shared.pick(t0 + Duration::from_millis(2));
        assert_eq!(first.unwrap().0, "a/high");
        let (second, _) = shared.pick(t0 + Duration::from_millis(2));
        assert_eq!(second.unwrap().0, "a/low");
    }

    #[test]
    fn ack_releases_window() {
        let shared = shared(r#"{}"#);
        let now = Instant::now();
        {
            let mut state = shared.state.lock().unwrap();
            for seq in [5u32, 6, 9] {
                state.unconsumed.push_back((seq, 100, now));
                state.stats.unconsumed_bytes += 100;
            }
        }
        shared.ack(6);
        assert_eq!(shared.unconsumed_bytes(), 100);
        shared.ack(u32::MAX);
        assert_eq!(shared.unconsumed_bytes(), 100, "a seq far behind is not an ack");
        shared.ack(9);
        assert_eq!(shared.unconsumed_bytes(), 0);
    }

    #[test]
    fn max_age_drops() {
        let shared = shared(r#"{"maxAge":100,"queueSize":null}"#);
        let t0 = Instant::now();
        insert(&shared, "a/x", pending(0, 5, t0), None);
        let (next, _) = shared.pick(t0 + Duration::from_millis(500));
        assert!(next.is_none());
        assert_eq!(shared.stats().dropped_age, 1);
    }

    #[test]
    fn hz_cap_defers() {
        let shared = shared(r#"{"maxHz":10,"queueSize":5}"#);
        let t0 = Instant::now();
        insert(&shared, "a/x", pending(0, 5, t0), None);
        insert(&shared, "a/x", pending(1, 5, t0), None);
        assert!(shared.pick(t0).0.is_some());
        let (next, wake_at) = shared.pick(t0 + Duration::from_millis(10));
        assert!(next.is_none());
        assert_eq!(wake_at, Some(t0 + Duration::from_millis(100)));
        assert!(shared.pick(t0 + Duration::from_millis(100)).0.is_some());
    }
}
