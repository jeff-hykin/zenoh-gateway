//! One `pub` data channel: every message from the browser becomes a zenoh put.

use crate::options::Label;
use log::{debug, warn};
use serde::Serialize;
use std::sync::{Arc, Mutex};
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use zenoh::qos::{CongestionControl, Priority};

#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PubStats {
    pub received: u64,
    pub put: u64,
    pub put_errors: u64,
}

#[derive(Default)]
pub struct PubShared {
    pub stats: Mutex<PubStats>,
}

/// INTERACTIVE_HIGH and above go out as express (no batching).
fn is_express(priority: Priority) -> bool {
    (priority as u8) <= (Priority::InteractiveHigh as u8)
}

pub async fn run(dc: Arc<dyn DataChannel>, label: Label, session: zenoh::Session, shared: Arc<PubShared>) {
    let delivery = label.opts.delivery();
    let priority = label.opts.zenoh_priority().unwrap_or_default();
    let congestion_control = if delivery.reliable { CongestionControl::Block } else { CongestionControl::Drop };
    let publisher = session
        .declare_publisher(label.key.clone())
        .priority(priority)
        .congestion_control(congestion_control)
        .express(is_express(priority))
        .await;
    let publisher = match publisher {
        Ok(publisher) => publisher,
        Err(error) => {
            warn!("declare publisher {:?} failed: {error}", label.key);
            let _ = dc.close().await;
            return;
        }
    };
    debug!("publisher {:?} priority={priority:?} congestion={congestion_control:?}", label.key);
    while let Some(event) = dc.poll().await {
        match event {
            DataChannelEvent::OnMessage(message) => {
                shared.stats.lock().unwrap().received += 1;
                let result = publisher.put(message.data.to_vec()).await;
                let mut stats = shared.stats.lock().unwrap();
                match result {
                    Ok(()) => stats.put += 1,
                    Err(error) => {
                        stats.put_errors += 1;
                        warn!("put {:?} failed: {error}", label.key);
                    }
                }
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
}
