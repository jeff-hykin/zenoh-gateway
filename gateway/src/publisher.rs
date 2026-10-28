//! One `pub` data channel: every message from the browser (`f64 sentAtMs | payload`) becomes a zenoh put.

use crate::options::{DeliveryKind, Label, PubOpts};
use crate::subscription::now_unix_ms;
use log::{debug, warn};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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

/// Whether a put stamped `sent_at_ms` (the page's clock) is within `latency_limit` on the gateway's clock, through the
/// connection's clock sync; records its age. Without a stamp or a clock offset yet it goes out, counted as `unsynced`.
pub(crate) fn fresh(shared: &PubShared, sent_at_ms: Option<f64>, clock_offset_ms: &Mutex<Option<f64>>, latency_limit: Option<f64>) -> bool {
    let mut stats = shared.stats.lock();
    let (Some(sent_at_ms), Some(offset_ms)) = (sent_at_ms, *clock_offset_ms.lock()) else {
        stats.unsynced += 1;
        return true;
    };
    let age = age_ms(sent_at_ms, offset_ms, now_unix_ms());
    stats.last_age_ms = Some(age);
    if latency_limit.is_some_and(|limit| age > limit) {
        stats.dropped_stale += 1;
        return false;
    }
    true
}

/// Puts `payload` and counts the outcome.
pub(crate) async fn put(publisher: &Publisher<'static>, payload: Vec<u8>, shared: &PubShared) {
    let result = publisher.put(payload).await;
    let mut stats = shared.stats.lock();
    match result {
        Ok(()) => stats.put += 1,
        Err(error) => {
            stats.put_errors += 1;
            warn!("put {:?} failed: {error}", publisher.key_expr());
        }
    }
}

/// `blocker` says why a put may not go out now (another client's lease).
pub async fn run(dc: Arc<dyn DataChannel>, label: Label, opts: PubOpts, session: zenoh::Session, shared: Arc<PubShared>, clock_offset_ms: Arc<Mutex<Option<f64>>>, blocker: impl Fn() -> Option<String>) {
    let Some(publisher) = declare(&dc, &label, &opts, &session).await else { return };
    let mut admission = Admission::default();
    while let Some(event) = dc.poll().await {
        match event {
            DataChannelEvent::OnMessage(message) => {
                if !admission.admit(&dc, &shared, &blocker).await {
                    continue;
                }
                if message.data.len() < PUT_HEADER_LEN {
                    shared.stats.lock().malformed += 1;
                    continue;
                }
                let sent_at_ms = f64::from_le_bytes(message.data[..PUT_HEADER_LEN].try_into().unwrap());
                if !fresh(&shared, Some(sent_at_ms), &clock_offset_ms, opts.latency_limit) {
                    continue;
                }
                put(&publisher, message.data[PUT_HEADER_LEN..].to_vec(), &shared).await;
            }
            DataChannelEvent::OnClose => break,
            _ => {}
        }
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
