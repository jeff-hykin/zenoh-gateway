//! One `pub` data channel: every message from the browser (`f64 sentAtMs | payload`) becomes a zenoh put.

use crate::options::{DeliveryKind, Label, PubOpts};
use crate::subscription::now_unix_ms;
use log::{debug, warn};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::Mutex;
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use zenoh::qos::{CongestionControl, Priority};

/// Browser -> gateway put header: the browser's send time, in the browser's clock.
pub const PUT_HEADER_LEN: usize = 8;

#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PubStats {
    pub received: u64,
    pub put: u64,
    pub put_errors: u64,
    pub malformed: u64,
    /// older than latencyLimit after clock correction
    pub dropped_stale: u64,
    /// accepted without an age check because no clock offset was known yet
    pub unsynced: u64,
    pub rejected_tripped: u64,
    /// dropped because another client leases the key
    pub rejected_leased: u64,
    pub last_age_ms: Option<f64>,
    pub deadman_armed: bool,
    pub tripped: bool,
}

#[derive(Default)]
pub struct PubShared {
    pub stats: Mutex<PubStats>,
    /// Set once this stream's deadman fired; every later put is rejected.
    pub tripped: AtomicBool,
}

impl PubShared {
    pub fn snapshot(&self) -> PubStats {
        let mut stats = self.stats.lock().clone();
        stats.tripped = self.tripped.load(Ordering::Acquire);
        stats
    }
}

/// INTERACTIVE_HIGH and above go out as express (no batching).
fn is_express(priority: Priority) -> bool {
    (priority as u8) <= (Priority::InteractiveHigh as u8)
}

/// Gateway-clock age of a put stamped `sent_at_ms` in the browser clock; `offset_ms` = gateway - browser.
fn age_ms(sent_at_ms: f64, offset_ms: f64, gateway_now_ms: f64) -> f64 {
    gateway_now_ms - (sent_at_ms + offset_ms)
}

/// A message waiting for the put ahead of it, and when it is no longer worth putting.
struct Waiting {
    payload: Vec<u8>,
    expires: Instant,
}

/// One put at a time, given up at its message's deadline.
type Putting = std::pin::Pin<Box<dyn Future<Output = Option<zenoh::Result<()>>> + Send>>;

/// `blocker` says why a put may not go out now (another client's lease); the browser hears
/// `{"blocked": reason | null}` on this channel whenever that changes.
/// A put waits at most the stream's latencyLimit, else `put_timeout`, and the channel keeps being read meanwhile (an
/// unread channel stops the whole connection's reads): `latest` keeps only the newest message waiting, `reliable` every
/// one, until its own deadline passes (counted in `dropped_stale`).
#[allow(clippy::too_many_arguments)]
pub async fn run(dc: Arc<dyn DataChannel>, label: Label, opts: PubOpts, session: zenoh::Session, shared: Arc<PubShared>, clock_offset_ms: Arc<Mutex<Option<f64>>>, blocker: impl Fn() -> Option<String>, put_timeout: Duration) {
    let priority = opts.zenoh_priority().unwrap_or_default();
    let reliable = opts.delivery == DeliveryKind::Reliable;
    let congestion_control = if reliable { CongestionControl::Block } else { CongestionControl::Drop };
    let publisher = session
        .declare_publisher(label.key.clone())
        .priority(priority)
        .congestion_control(congestion_control)
        .express(is_express(priority))
        .await;
    let publisher = match publisher {
        Ok(publisher) => Arc::new(publisher),
        Err(error) => {
            warn!("declare publisher {:?} failed: {error}", label.key);
            let _ = dc.close().await;
            return;
        }
    };
    debug!("publisher {:?} priority={priority:?} congestion={congestion_control:?} latencyLimit={:?}", label.key, opts.latency_limit);
    let mut blocked: Option<String> = None;
    let mut waiting: VecDeque<Waiting> = VecDeque::new();
    let mut putting: Option<Putting> = None;
    loop {
        // the next waiting message still worth putting goes as soon as nothing is in flight
        while putting.is_none() && let Some(next) = waiting.pop_front() {
            let now = Instant::now();
            if next.expires <= now {
                shared.stats.lock().dropped_stale += 1;
                continue;
            }
            let publisher = publisher.clone();
            let left = next.expires - now;
            putting = Some(Box::pin(async move { tokio::time::timeout(left, publisher.put(next.payload)).await.ok() }));
        }
        let event = tokio::select! {
            event = dc.poll() => event,
            result = async { putting.as_mut().expect("guarded").await }, if putting.is_some() => {
                putting = None;
                let mut stats = shared.stats.lock();
                match result {
                    Some(Ok(())) => stats.put += 1,
                    Some(Err(error)) => {
                        stats.put_errors += 1;
                        warn!("put {:?} failed: {error}", label.key);
                    }
                    None => {
                        stats.put_errors += 1;
                        warn!("put {:?} gave up: zenoh held it past its deadline", label.key);
                    }
                }
                continue;
            }
        };
        match event {
            Some(DataChannelEvent::OnMessage(message)) => {
                let now_blocked = blocker();
                if now_blocked != blocked {
                    blocked = now_blocked;
                    let _ = dc.send_text(&serde_json::json!({"blocked": blocked}).to_string()).await;
                }
                let mut stats = shared.stats.lock();
                stats.received += 1;
                if shared.tripped.load(Ordering::Acquire) {
                    stats.rejected_tripped += 1;
                    continue;
                }
                if blocked.is_some() {
                    stats.rejected_leased += 1;
                    continue;
                }
                if message.data.len() < PUT_HEADER_LEN {
                    stats.malformed += 1;
                    continue;
                }
                let sent_at_ms = f64::from_le_bytes(message.data[..PUT_HEADER_LEN].try_into().unwrap());
                let limit = opts.latency_limit.map(|limit| crate::secs(limit / 1000.0));
                let worth = match *clock_offset_ms.lock() {
                    Some(offset_ms) => {
                        let age = age_ms(sent_at_ms, offset_ms, now_unix_ms());
                        stats.last_age_ms = Some(age);
                        if opts.latency_limit.is_some_and(|limit| age > limit) {
                            stats.dropped_stale += 1;
                            continue;
                        }
                        limit.map_or(put_timeout, |limit| limit.saturating_sub(crate::secs(age / 1000.0)))
                    }
                    None => {
                        stats.unsynced += 1;
                        limit.unwrap_or(put_timeout)
                    }
                };
                // latest: a newer message replaces one still waiting
                if !reliable && waiting.pop_back().is_some() {
                    stats.dropped_stale += 1;
                }
                waiting.push_back(Waiting { payload: message.data[PUT_HEADER_LEN..].to_vec(), expires: Instant::now() + worth });
            }
            Some(DataChannelEvent::OnClose) | None => break,
            Some(_) => {}
        }
    }
    // what the page sent before it closed still goes, each by its own deadline
    if let Some(put) = putting {
        put.await;
    }
    for next in waiting {
        let _ = tokio::time::timeout(next.expires.saturating_duration_since(Instant::now()), publisher.put(next.payload)).await;
    }
    debug!("publisher {:?} closed", label.key);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn express_threshold() {
        assert!(is_express(Priority::RealTime));
        assert!(is_express(Priority::InteractiveHigh));
        assert!(!is_express(Priority::InteractiveLow));
        assert!(!is_express(Priority::Data));
    }

    #[test]
    fn age_uses_offset() {
        // browser clock runs 30 s ahead of the gateway: offset = gateway - browser = -30000
        assert_eq!(age_ms(31_000.0, -30_000.0, 1_100.0), 100.0);
    }
}
