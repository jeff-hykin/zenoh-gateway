//! One `sub` data channel: a zenoh AdvancedSubscriber feeding per-key delivery queues, drained
//! into the data channel only while it is not backed up.

use crate::frame;
use crate::options::{Delivery, Label};
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

fn now_unix_ms() -> f64 {
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
    pub fn new(label: &Label) -> Self {
        SubShared {
            delivery: label.opts.delivery(),
            min_interval: label.opts.min_interval(),
            priority_override: label.opts.zenoh_priority().map(|p| p as u8),
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
            let pending = Pending {
                timestamp_ms: sample_timestamp_ms(&sample),
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
            DataChannelEvent::OnClose => break,
            _ => {}
        }
    }
    shared.close();
    let _ = sender.await;
    drop(subscriber);
    debug!("unsubscribed {:?}", label.key);
}

async fn send_loop(dc: Arc<dyn DataChannel>, shared: Arc<SubShared>) {
    let mut warned_send_error = false;
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
        let frame = frame::encode(&key, item.timestamp_ms, item.seq, &item.payload.to_bytes());
        let frame_len = frame.len();
        if let Err(error) = dc.send(frame).await {
            if shared.closed.load(Ordering::Relaxed) {
                break;
            }
            shared.state.lock().unwrap().stats.dropped_send_error += 1;
            if !warned_send_error {
                warn!("send on {key:?} failed ({frame_len} bytes): {error}");
                warned_send_error = true;
            }
            if matches!(error, webrtc::error::Error::ErrDataChannelClosed) {
                break;
            }
            continue;
        }
        let mut outstanding = dc.outstanding_bytes().await.unwrap_or(0);
        let backed_up = outstanding >= BACKED_UP_BYTES;
        {
            let mut state = shared.state.lock().unwrap();
            state.stats.sent += 1;
            state.stats.outstanding_bytes = outstanding;
            state.stats.backed_up = backed_up;
        }
        if !backed_up {
            continue;
        }
        while outstanding > RESUME_BYTES && !shared.closed.load(Ordering::Relaxed) {
            tokio::select! {
                _ = shared.drained.notified() => {}
                _ = tokio::time::sleep(BACKSTOP) => {}
            }
            outstanding = dc.outstanding_bytes().await.unwrap_or(0);
        }
        let mut state = shared.state.lock().unwrap();
        state.stats.outstanding_bytes = outstanding;
        state.stats.backed_up = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared(opts: &str) -> SubShared {
        let label: Label = serde_json::from_str(&format!(r#"{{"type":"sub","key":"a/**","opts":{opts}}}"#)).unwrap();
        SubShared::new(&label)
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
    fn max_age_drops() {
        let shared = shared(r#"{"delivery":{"maxAgeMs":100}}"#);
        let t0 = Instant::now();
        insert(&shared, "a/x", pending(0, 5, t0), None);
        let (next, _) = shared.pick(t0 + Duration::from_millis(500));
        assert!(next.is_none());
        assert_eq!(shared.stats().dropped_age, 1);
    }

    #[test]
    fn hz_cap_defers() {
        let shared = shared(r#"{"hz":[1,10],"delivery":{"queue":5}}"#);
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
