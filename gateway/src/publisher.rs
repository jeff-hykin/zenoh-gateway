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
use zenoh::pubsub::Publisher;
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
    /// accepted without an age check: no clock offset yet (microphones: nor a sender report)
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

/// Declares the zenoh publisher a `pub` channel puts through; closes the channel if zenoh refuses.
pub(crate) async fn declare(dc: &Arc<dyn DataChannel>, label: &Label, opts: &PubOpts, session: &zenoh::Session) -> Option<Publisher<'static>> {
    let priority = opts.zenoh_priority().unwrap_or_default();
    let congestion_control = if opts.delivery == DeliveryKind::Reliable { CongestionControl::Block } else { CongestionControl::Drop };
    let publisher = session
        .declare_publisher(label.key.clone())
        .priority(priority)
        .congestion_control(congestion_control)
        .express(is_express(priority))
        .await;
    match publisher {
        Ok(publisher) => {
            debug!("publisher {:?} priority={priority:?} congestion={congestion_control:?} latencyLimit={:?}", label.key, opts.latency_limit);
            Some(publisher)
        }
        Err(error) => {
            warn!("declare publisher {:?} failed: {error}", label.key);
            let _ = dc.close().await;
            None
        }
    }
}

/// Whether a put may go out now: not after the deadman fired, nor while another client leases the key (`blocker` says
/// why not). The browser hears `{"blocked": reason | null}` on the channel whenever that changes.
#[derive(Default)]
pub(crate) struct Admission {
    blocked: Option<String>,
}

impl Admission {
    /// Counts the message as received, and as rejected when it may not go out.
    pub(crate) async fn admit(&mut self, dc: &Arc<dyn DataChannel>, shared: &PubShared, blocker: &impl Fn() -> Option<String>) -> bool {
        let now_blocked = blocker();
        if now_blocked != self.blocked {
            self.blocked = now_blocked;
            let _ = dc.send_text(&serde_json::json!({"blocked": self.blocked}).to_string()).await;
        }
        let mut stats = shared.stats.lock();
        stats.received += 1;
        if shared.tripped.load(Ordering::Acquire) {
            stats.rejected_tripped += 1;
            return false;
        }
        if self.blocked.is_some() {
            stats.rejected_leased += 1;
            return false;
        }
        true
    }
}

/// The age of a put stamped `sent_at_ms` (the page's clock) on the gateway's clock, through the connection's clock
/// sync, recorded in `last_age_ms`; `None` (counted `unsynced`) without a stamp or a clock offset yet.
fn age(shared: &PubShared, sent_at_ms: Option<f64>, clock_offset_ms: &Mutex<Option<f64>>) -> Option<f64> {
    let mut stats = shared.stats.lock();
    let (Some(sent_at_ms), Some(offset_ms)) = (sent_at_ms, *clock_offset_ms.lock()) else {
        stats.unsynced += 1;
        return None;
    };
    let age = age_ms(sent_at_ms, offset_ms, now_unix_ms());
    stats.last_age_ms = Some(age);
    Some(age)
}

/// Whether a put stamped `sent_at_ms` is within `latency_limit` (see [`age`]); an unsynced one goes out.
pub(crate) fn fresh(shared: &PubShared, sent_at_ms: Option<f64>, clock_offset_ms: &Mutex<Option<f64>>, latency_limit: Option<f64>) -> bool {
    match age(shared, sent_at_ms, clock_offset_ms) {
        Some(age) if latency_limit.is_some_and(|limit| age > limit) => {
            shared.stats.lock().dropped_stale += 1;
            false
        }
        _ => true,
    }
}

/// Counts a put's outcome, `None` being one zenoh held past its deadline.
fn count(shared: &PubShared, key: &impl std::fmt::Debug, result: Option<zenoh::Result<()>>) {
    let mut stats = shared.stats.lock();
    match result {
        Some(Ok(())) => stats.put += 1,
        Some(Err(error)) => {
            stats.put_errors += 1;
            warn!("put {key:?} failed: {error}");
        }
        None => {
            stats.put_errors += 1;
            warn!("put {key:?} gave up: zenoh held it past its deadline");
        }
    }
}

/// Puts `payload` and counts the outcome.
pub(crate) async fn put(publisher: &Publisher<'static>, payload: Vec<u8>, shared: &PubShared) {
    count(shared, publisher.key_expr(), Some(publisher.put(payload).await));
}

/// `blocker` says why a put may not go out now (another client's lease); see [`Admission`].
/// A put waits at most the stream's latencyLimit, else `put_timeout`, and the channel keeps being read meanwhile (an
/// unread channel stops the whole connection's reads): `latest` keeps only the newest message waiting, `reliable` every
/// one, until its own deadline passes (counted in `dropped_stale`).
#[allow(clippy::too_many_arguments)]
pub async fn run(dc: Arc<dyn DataChannel>, label: Label, opts: PubOpts, session: zenoh::Session, shared: Arc<PubShared>, clock_offset_ms: Arc<Mutex<Option<f64>>>, blocker: impl Fn() -> Option<String>, put_timeout: Duration) {
    let reliable = opts.delivery == DeliveryKind::Reliable;
    let Some(publisher) = declare(&dc, &label, &opts, &session).await else { return };
    let publisher = Arc::new(publisher);
    let mut admission = Admission::default();
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
                count(&shared, &label.key, result);
                continue;
            }
        };
        match event {
            Some(DataChannelEvent::OnMessage(message)) => {
                if !admission.admit(&dc, &shared, &blocker).await {
                    continue;
                }
                if message.data.len() < PUT_HEADER_LEN {
                    shared.stats.lock().malformed += 1;
                    continue;
                }
                let sent_at_ms = f64::from_le_bytes(message.data[..PUT_HEADER_LEN].try_into().unwrap());
                let limit = opts.latency_limit.map(|limit| crate::secs(limit / 1000.0));
                let worth = match age(&shared, Some(sent_at_ms), &clock_offset_ms) {
                    Some(age) => {
                        if opts.latency_limit.is_some_and(|limit| age > limit) {
                            shared.stats.lock().dropped_stale += 1;
                            continue;
                        }
                        limit.map_or(put_timeout, |limit| limit.saturating_sub(crate::secs(age / 1000.0)))
                    }
                    None => limit.unwrap_or(put_timeout),
                };
                let mut stats = shared.stats.lock();
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
        count(&shared, &label.key, put.await);
    }
    for next in waiting {
        count(&shared, &label.key, tokio::time::timeout(next.expires.saturating_duration_since(Instant::now()), publisher.put(next.payload)).await.ok());
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
