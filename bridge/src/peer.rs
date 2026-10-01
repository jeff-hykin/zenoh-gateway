//! One browser = one PeerConnection; each data channel it opens is dispatched by its label.

use crate::options::Label;
use crate::publisher::{self, PubShared};
use crate::subscription::{self, SubShared};
use base64::Engine;
use log::{debug, info, warn};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfigurationBuilder,
    RTCIceGatheringState, RTCPeerConnectionState, RTCSessionDescription, SettingEngineBuilder,
};
use rtc::peer_connection::configuration::setting_engine::SctpMaxMessageSize;

/// Largest message we accept from (and advertise to) the browser; Chrome sends 256 KiB too.
const MAX_MESSAGE_SIZE: u32 = 256 * 1024;
const GATHER_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_GET_TIMEOUT_MS: u64 = 5000;

enum ChannelStats {
    Sub(Arc<SubShared>),
    Pub(Arc<PubShared>),
}

struct ChannelEntry {
    label: Label,
    stats: ChannelStats,
}

/// Per-connection state shared by the handler and its channel tasks.
#[derive(Default)]
struct PeerState {
    channels: Mutex<HashMap<u64, ChannelEntry>>,
    next_channel: AtomicU64,
}

pub struct Bridge {
    pub session: zenoh::Session,
    peers: Mutex<HashMap<u64, Arc<dyn PeerConnection>>>,
    next_peer: AtomicU64,
}

impl Bridge {
    pub fn new(session: zenoh::Session) -> Arc<Self> {
        Arc::new(Bridge { session, peers: Mutex::new(HashMap::new()), next_peer: AtomicU64::new(1) })
    }

    /// Non-trickle signaling: take an offer, return an answer with all our candidates in it.
    pub async fn answer(self: &Arc<Self>, offer: RTCSessionDescription) -> anyhow::Result<RTCSessionDescription> {
        let peer_id = self.next_peer.fetch_add(1, Ordering::Relaxed);
        let (gathered_tx, mut gathered_rx) = mpsc::channel::<()>(1);
        let handler = Arc::new(Handler {
            bridge: Arc::downgrade(self),
            peer_id,
            gathered_tx,
            state: Arc::new(PeerState::default()),
        });
        let setting_engine = SettingEngineBuilder::new()
            .with_sctp_max_message_size(SctpMaxMessageSize::Bounded(MAX_MESSAGE_SIZE))
            .build();
        let peer_connection = PeerConnectionBuilder::new()
            .with_configuration(RTCConfigurationBuilder::new().build())
            .with_setting_engine(setting_engine)
            .with_handler(handler)
            .with_udp_addrs(vec!["0.0.0.0:0".to_string(), "127.0.0.1:0".to_string()])
            .build()
            .await?;
        peer_connection.set_remote_description(offer).await?;
        let answer = peer_connection.create_answer(None).await?;
        peer_connection.set_local_description(answer).await?;
        if tokio::time::timeout(GATHER_TIMEOUT, gathered_rx.recv()).await.is_err() {
            warn!("peer {peer_id}: ICE gathering timed out, answering with what we have");
        }
        let local = peer_connection
            .local_description()
            .await
            .ok_or_else(|| anyhow::anyhow!("no local description"))?;
        self.peers.lock().unwrap().insert(peer_id, Arc::new(peer_connection));
        info!("peer {peer_id}: answered");
        Ok(local)
    }

    fn drop_peer(&self, peer_id: u64) {
        if let Some(peer_connection) = self.peers.lock().unwrap().remove(&peer_id) {
            info!("peer {peer_id}: gone");
            tokio::spawn(async move {
                let _ = peer_connection.close().await;
            });
        }
    }
}

struct Handler {
    bridge: std::sync::Weak<Bridge>,
    peer_id: u64,
    gathered_tx: mpsc::Sender<()>,
    state: Arc<PeerState>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for Handler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            let _ = self.gathered_tx.try_send(());
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        debug!("peer {}: {state}", self.peer_id);
        if matches!(state, RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed)
            && let Some(bridge) = self.bridge.upgrade()
        {
            bridge.drop_peer(self.peer_id);
        }
    }

    async fn on_data_channel(&self, dc: Arc<dyn DataChannel>) {
        // Must not block here: the driver waits for this to return.
        let Some(bridge) = self.bridge.upgrade() else { return };
        let state = self.state.clone();
        let peer_id = self.peer_id;
        tokio::spawn(async move {
            let raw_label = dc.label().await.unwrap_or_default();
            if raw_label == "control" {
                run_control(dc, bridge, state).await;
                return;
            }
            let label: Label = match serde_json::from_str(&raw_label) {
                Ok(label) => label,
                Err(error) => {
                    warn!("peer {peer_id}: bad channel label {raw_label:?}: {error}");
                    let _ = dc.close().await;
                    return;
                }
            };
            let entry_id = state.next_channel.fetch_add(1, Ordering::Relaxed);
            let session = bridge.session.clone();
            drop(bridge);
            match label.kind.as_str() {
                "sub" => {
                    let shared = Arc::new(SubShared::new(&label));
                    let stats = ChannelStats::Sub(shared.clone());
                    state.channels.lock().unwrap().insert(entry_id, ChannelEntry { label: label.clone(), stats });
                    subscription::run(dc, label, session, shared).await;
                }
                "pub" => {
                    let shared = Arc::new(PubShared::default());
                    let stats = ChannelStats::Pub(shared.clone());
                    state.channels.lock().unwrap().insert(entry_id, ChannelEntry { label: label.clone(), stats });
                    publisher::run(dc, label, session, shared).await;
                }
                other => {
                    warn!("peer {peer_id}: unknown channel type {other:?}");
                    let _ = dc.close().await;
                }
            }
            state.channels.lock().unwrap().remove(&entry_id);
        });
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ControlRequest {
    #[serde(default)]
    id: Value,
    op: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

/// `control` channel: JSON requests in, JSON responses out (`{id, ok, ...}`).
async fn run_control(dc: Arc<dyn DataChannel>, bridge: Arc<Bridge>, state: Arc<PeerState>) {
    let session = bridge.session.clone();
    drop(bridge);
    while let Some(event) = dc.poll().await {
        match event {
            DataChannelEvent::OnMessage(message) => {
                let request: ControlRequest = match serde_json::from_slice(&message.data) {
                    Ok(request) => request,
                    Err(error) => {
                        let _ = dc.send_text(&json!({"ok": false, "error": error.to_string()}).to_string()).await;
                        continue;
                    }
                };
                match request.op.as_str() {
                    // a get can take seconds; don't hold up stats and pings behind it
                    "get" => {
                        let dc = dc.clone();
                        let session = session.clone();
                        tokio::spawn(async move {
                            let response = handle_get(&session, &request).await;
                            let _ = dc.send_text(&response.to_string()).await;
                        });
                    }
                    "stats" => {
                        let response = json!({"id": request.id, "ok": true, "channels": collect_stats(&state)});
                        let _ = dc.send_text(&response.to_string()).await;
                    }
                    "ping" => {
                        let _ = dc.send_text(&json!({"id": request.id, "ok": true}).to_string()).await;
                    }
                    other => {
                        let response = json!({"id": request.id, "ok": false, "error": format!("unknown op {other}")});
                        let _ = dc.send_text(&response.to_string()).await;
                    }
                }
            }
            DataChannelEvent::OnClose => break,
            _ => {}
        }
    }
}

async fn handle_get(session: &zenoh::Session, request: &ControlRequest) -> Value {
    let base64 = base64::engine::general_purpose::STANDARD;
    let timeout = Duration::from_millis(request.timeout_ms.unwrap_or(DEFAULT_GET_TIMEOUT_MS));
    let replies = match session.get(request.key.as_str()).timeout(timeout).await {
        Ok(replies) => replies,
        Err(error) => return json!({"id": request.id, "ok": false, "error": error.to_string()}),
    };
    let mut results = Vec::new();
    while let Ok(reply) = replies.recv_async().await {
        match reply.result() {
            Ok(sample) => results.push(json!({
                "key": sample.key_expr().as_str(),
                "bytes": base64.encode(sample.payload().to_bytes()),
            })),
            Err(error) => results.push(json!({"error": base64.encode(error.payload().to_bytes())})),
        }
    }
    json!({"id": request.id, "ok": true, "replies": results})
}

fn collect_stats(state: &PeerState) -> Vec<Value> {
    let channels = state.channels.lock().unwrap();
    channels
        .values()
        .map(|entry| {
            let stats = match &entry.stats {
                ChannelStats::Sub(shared) => serde_json::to_value(shared.stats()).unwrap_or_default(),
                ChannelStats::Pub(shared) => serde_json::to_value(shared.stats.lock().unwrap().clone()).unwrap_or_default(),
            };
            json!({"id": entry.label.id, "type": entry.label.kind, "key": entry.label.key, "stats": stats})
        })
        .collect()
}
