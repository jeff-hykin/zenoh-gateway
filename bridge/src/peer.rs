//! One browser = one PeerConnection; each data channel it opens is dispatched by its label.

use crate::options::{HeartbeatOpts, Label, PubOpts, SubOpts};
use crate::publisher::{self, PubShared};
use crate::subscription::{self, SubShared, now_unix_ms};
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
use zenoh::qos::{CongestionControl, Priority, Reliability};

/// Largest message we accept from (and advertise to) the browser; Chrome sends 256 KiB too.
const MAX_MESSAGE_SIZE: u32 = 256 * 1024;
const GATHER_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_GET_TIMEOUT_MS: u64 = 5000;
/// A connection that stays `disconnected` this long is treated as gone.
const DISCONNECTED_GRACE: Duration = Duration::from_secs(15);

enum ChannelStats {
    Sub(Arc<SubShared>),
    Pub(Arc<PubShared>),
}

struct ChannelEntry {
    label: Label,
    /// normalized options, reported in stats
    opts: Value,
    stats: ChannelStats,
}

/// One armed deadman: published once, reliably, when its frontend is judged gone.
struct Deadman {
    key: String,
    bytes: Vec<u8>,
    shared: Arc<PubShared>,
}

#[derive(Debug, Default, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct HeartbeatStats {
    configured: bool,
    hz: f64,
    misses: u32,
    beats: u64,
    deadlines_missed: u64,
}

/// Per-connection state shared by the handler and its channel tasks.
struct PeerState {
    peer_id: u64,
    session: zenoh::Session,
    channels: Mutex<HashMap<u64, ChannelEntry>>,
    next_channel: AtomicU64,
    connection_state: Mutex<Option<RTCPeerConnectionState>>,
    /// bridge clock minus browser clock, as estimated (and reported) by the browser
    clock_offset_ms: Arc<Mutex<Option<f64>>>,
    rtt_ms: Mutex<Option<f64>>,
    /// keyed by the publisher's label id
    deadmen: Mutex<HashMap<u64, Deadman>>,
    control: Mutex<Option<Arc<dyn DataChannel>>>,
    heartbeat: Mutex<HeartbeatStats>,
}

impl PeerState {
    fn new(peer_id: u64, session: zenoh::Session) -> Self {
        PeerState {
            peer_id,
            session,
            channels: Mutex::new(HashMap::new()),
            next_channel: AtomicU64::new(0),
            connection_state: Mutex::new(None),
            clock_offset_ms: Arc::new(Mutex::new(None)),
            rtt_ms: Mutex::new(None),
            deadmen: Mutex::new(HashMap::new()),
            control: Mutex::new(None),
            heartbeat: Mutex::new(HeartbeatStats::default()),
        }
    }

    /// Clock sample reported by the browser (it has all four NTP timestamps).
    fn record_clock(&self, offset_ms: Option<f64>, rtt_ms: Option<f64>) {
        if let Some(offset) = offset_ms.filter(|v| v.is_finite()) {
            *self.clock_offset_ms.lock().unwrap() = Some(offset);
        }
        if let Some(rtt) = rtt_ms.filter(|v| v.is_finite()) {
            *self.rtt_ms.lock().unwrap() = Some(rtt);
        }
    }

    fn find_publisher(&self, pub_id: u64) -> Option<(String, Arc<PubShared>)> {
        let channels = self.channels.lock().unwrap();
        channels.values().find_map(|entry| match &entry.stats {
            ChannelStats::Pub(shared) if entry.label.id == Some(pub_id) => Some((entry.label.key.clone(), shared.clone())),
            _ => None,
        })
    }

    /// Publishes every armed deadman once (REAL_TIME, blocking, reliable) and trips its stream.
    async fn fire_deadmen(&self, reason: &str) {
        let armed: Vec<(u64, Deadman)> = self.deadmen.lock().unwrap().drain().collect();
        for (pub_id, deadman) in armed {
            // trip first, so a put racing in behind the deadman can't undo it
            deadman.shared.tripped.store(true, Ordering::Release);
            deadman.shared.stats.lock().unwrap().deadman_armed = false;
            let result = self
                .session
                .put(deadman.key.as_str(), deadman.bytes)
                .priority(Priority::RealTime)
                .congestion_control(CongestionControl::Block)
                .reliability(Reliability::Reliable)
                .express(true)
                .await;
            match result {
                Ok(()) => info!("peer {}: deadman fired on {:?} ({reason})", self.peer_id, deadman.key),
                Err(error) => warn!("peer {}: deadman on {:?} failed: {error}", self.peer_id, deadman.key),
            }
            let control = self.control.lock().unwrap().clone();
            if let Some(control) = control {
                let event = json!({"event": "tripped", "id": pub_id, "reason": reason});
                let _ = control.send_text(&event.to_string()).await;
            }
        }
    }
}

struct PeerEntry {
    connection: Arc<dyn PeerConnection>,
    state: Arc<PeerState>,
}

pub struct Bridge {
    pub session: zenoh::Session,
    peers: Mutex<HashMap<u64, PeerEntry>>,
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
        let state = Arc::new(PeerState::new(peer_id, self.session.clone()));
        let handler = Arc::new(Handler { bridge: Arc::downgrade(self), peer_id, gathered_tx, state: state.clone() });
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
        self.peers.lock().unwrap().insert(peer_id, PeerEntry { connection: Arc::new(peer_connection), state });
        info!("peer {peer_id}: answered");
        Ok(local)
    }

    fn drop_peer(&self, peer_id: u64) {
        if let Some(entry) = self.peers.lock().unwrap().remove(&peer_id) {
            info!("peer {peer_id}: gone");
            tokio::spawn(async move {
                entry.state.fire_deadmen("disconnected").await;
                let _ = entry.connection.close().await;
            });
        }
    }

    /// SIGINT/SIGTERM: fire every frontend's deadmen before the process exits.
    pub async fn shutdown(&self) {
        let peers: Vec<PeerEntry> = self.peers.lock().unwrap().drain().map(|(_, entry)| entry).collect();
        for entry in &peers {
            entry.state.fire_deadmen("shutdown").await;
        }
        for entry in peers {
            let _ = entry.connection.close().await;
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
        *self.state.connection_state.lock().unwrap() = Some(state);
        match state {
            RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed => {
                if let Some(bridge) = self.bridge.upgrade() {
                    bridge.drop_peer(self.peer_id);
                }
            }
            // ICE may sit in `disconnected` without ever reaching `failed`
            RTCPeerConnectionState::Disconnected => {
                let (bridge, state, peer_id) = (self.bridge.clone(), self.state.clone(), self.peer_id);
                tokio::spawn(async move {
                    tokio::time::sleep(DISCONNECTED_GRACE).await;
                    let still_down = *state.connection_state.lock().unwrap() != Some(RTCPeerConnectionState::Connected);
                    if still_down && let Some(bridge) = bridge.upgrade() {
                        bridge.drop_peer(peer_id);
                    }
                });
            }
            _ => {}
        }
    }

    async fn on_data_channel(&self, dc: Arc<dyn DataChannel>) {
        // Must not block here: the driver waits for this to return.
        let bridge = self.bridge.clone();
        let state = self.state.clone();
        tokio::spawn(run_channel(dc, bridge, state));
    }
}

async fn run_channel(dc: Arc<dyn DataChannel>, bridge: std::sync::Weak<Bridge>, state: Arc<PeerState>) {
    let peer_id = state.peer_id;
    let raw_label = dc.label().await.unwrap_or_default();
    if raw_label == "control" {
        *state.control.lock().unwrap() = Some(dc.clone());
        run_control(dc, state.clone()).await;
        *state.control.lock().unwrap() = None;
        // the client keeps `control` open for its whole life, so its close means the browser left
        if let Some(bridge) = bridge.upgrade() {
            bridge.drop_peer(peer_id);
        }
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
    let session = state.session.clone();
    let entry_id = state.next_channel.fetch_add(1, Ordering::Relaxed);
    let register = |opts: Value, stats: ChannelStats| {
        state.channels.lock().unwrap().insert(entry_id, ChannelEntry { label: label.clone(), opts, stats });
    };
    let rejected = match label.kind.as_str() {
        "sub" => match SubOpts::parse(&label.opts) {
            Ok(opts) => {
                let shared = Arc::new(SubShared::new(&opts));
                register(opts.normalized(), ChannelStats::Sub(shared.clone()));
                subscription::run(dc.clone(), label.clone(), session, shared).await;
                None
            }
            Err(error) => Some(error),
        },
        "pub" => match PubOpts::parse(&label.opts) {
            Ok(opts) => {
                let shared = Arc::new(PubShared::default());
                register(json!({"delivery": opts.delivery, "priority": opts.priority, "latencyLimit": opts.latency_limit}), ChannelStats::Pub(shared.clone()));
                publisher::run(dc.clone(), label.clone(), opts, session, shared, state.clock_offset_ms.clone()).await;
                None
            }
            Err(error) => Some(error),
        },
        "heartbeat" => match HeartbeatOpts::parse(&label.opts) {
            Ok(opts) => {
                run_heartbeat(dc.clone(), opts, state.clone()).await;
                None
            }
            Err(error) => Some(error),
        },
        other => Some(format!("unknown channel type {other:?}")),
    };
    if let Some(error) = rejected {
        warn!("peer {peer_id}: rejected channel {raw_label}: {error}");
        let _ = dc.close().await;
    }
    state.channels.lock().unwrap().remove(&entry_id);
    // a publisher stream that goes away takes its deadman with it (the client cleared or closed it)
    if label.kind == "pub"
        && let Some(id) = label.id
    {
        state.deadmen.lock().unwrap().remove(&id);
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClockSample {
    t0: f64,
    #[serde(default)]
    offset_ms: Option<f64>,
    #[serde(default)]
    rtt_ms: Option<f64>,
}

/// Heartbeat channel (unordered, no retransmits). Each beat is also a clock-sync ping:
/// `{t0, offsetMs?, rttMs?}` in, `{t0, t1, t2}` (bridge receive/send times) out.
async fn run_heartbeat(dc: Arc<dyn DataChannel>, opts: HeartbeatOpts, state: Arc<PeerState>) {
    let already_configured = {
        let mut heartbeat = state.heartbeat.lock().unwrap();
        let already_configured = heartbeat.configured;
        if !already_configured {
            *heartbeat = HeartbeatStats { configured: true, hz: opts.hz, misses: opts.misses, ..Default::default() };
        }
        already_configured
    };
    if already_configured {
        warn!("peer {}: second heartbeat channel rejected", state.peer_id);
        let _ = dc.close().await;
        return;
    }
    let deadline = opts.deadline();
    let mut last_beat = tokio::time::Instant::now();
    let mut fired_since_last_beat = false;
    loop {
        tokio::select! {
            event = dc.poll() => match event {
                Some(DataChannelEvent::OnMessage(message)) => {
                    let t1 = now_unix_ms();
                    last_beat = tokio::time::Instant::now();
                    fired_since_last_beat = false;
                    state.heartbeat.lock().unwrap().beats += 1;
                    if let Ok(sample) = serde_json::from_slice::<ClockSample>(&message.data) {
                        state.record_clock(sample.offset_ms, sample.rtt_ms);
                        let reply = json!({"t0": sample.t0, "t1": t1, "t2": now_unix_ms()});
                        let _ = dc.send_text(&reply.to_string()).await;
                    }
                }
                Some(DataChannelEvent::OnClose) | None => break,
                _ => {}
            },
            _ = tokio::time::sleep_until(last_beat + deadline), if !fired_since_last_beat => {
                fired_since_last_beat = true;
                state.heartbeat.lock().unwrap().deadlines_missed += 1;
                warn!("peer {}: heartbeat missed for {deadline:?}", state.peer_id);
                state.fire_deadmen("heartbeat").await;
            }
        }
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
    #[serde(default)]
    t0: Option<f64>,
    #[serde(default)]
    offset_ms: Option<f64>,
    #[serde(default)]
    rtt_ms: Option<f64>,
    #[serde(default)]
    pub_id: Option<u64>,
    #[serde(default)]
    bytes: Option<String>,
}

fn ok(id: &Value, extra: Value) -> Value {
    let mut response = json!({"id": id, "ok": true});
    if let (Some(response), Value::Object(extra)) = (response.as_object_mut(), extra) {
        response.extend(extra);
    }
    response
}

fn fail(id: &Value, error: impl std::fmt::Display) -> Value {
    json!({"id": id, "ok": false, "error": error.to_string()})
}

/// `control` channel: JSON requests in, JSON responses out (`{id, ok, ...}`), plus
/// unsolicited `{event: "tripped", id, reason}` notifications.
async fn run_control(dc: Arc<dyn DataChannel>, state: Arc<PeerState>) {
    while let Some(event) = dc.poll().await {
        let DataChannelEvent::OnMessage(message) = event else {
            if matches!(event, DataChannelEvent::OnClose) {
                break;
            }
            continue;
        };
        let t1 = now_unix_ms();
        let request: ControlRequest = match serde_json::from_slice(&message.data) {
            Ok(request) => request,
            Err(error) => {
                let _ = dc.send_text(&fail(&Value::Null, error).to_string()).await;
                continue;
            }
        };
        let response = match request.op.as_str() {
            // a get can take seconds; don't hold up stats and pings behind it
            "get" => {
                let dc = dc.clone();
                let session = state.session.clone();
                tokio::spawn(async move {
                    let response = handle_get(&session, &request).await;
                    let _ = dc.send_text(&response.to_string()).await;
                });
                continue;
            }
            "ping" => {
                state.record_clock(request.offset_ms, request.rtt_ms);
                ok(&request.id, json!({"t0": request.t0, "t1": t1, "t2": now_unix_ms()}))
            }
            "stats" => {
                let clock = json!({"offsetMs": *state.clock_offset_ms.lock().unwrap(), "rttMs": *state.rtt_ms.lock().unwrap()});
                let heartbeat = serde_json::to_value(state.heartbeat.lock().unwrap().clone()).unwrap_or_default();
                ok(&request.id, json!({"channels": collect_stats(&state), "clock": clock, "heartbeat": heartbeat}))
            }
            "setDeadman" => set_deadman(&state, &request).await,
            "clearDeadman" => {
                let pub_id = request.pub_id.unwrap_or_default();
                if let Some(deadman) = state.deadmen.lock().unwrap().remove(&pub_id) {
                    deadman.shared.stats.lock().unwrap().deadman_armed = false;
                }
                ok(&request.id, json!({}))
            }
            other => fail(&request.id, format!("unknown op {other}")),
        };
        let _ = dc.send_text(&response.to_string()).await;
    }
}

async fn set_deadman(state: &PeerState, request: &ControlRequest) -> Value {
    let Some(pub_id) = request.pub_id else { return fail(&request.id, "setDeadman needs pubId") };
    if !state.heartbeat.lock().unwrap().configured {
        return fail(&request.id, "setDeadman needs a heartbeat: connect(url, { heartbeatHz, heartbeatMisses })");
    }
    let bytes = match base64::engine::general_purpose::STANDARD.decode(request.bytes.as_deref().unwrap_or_default()) {
        Ok(bytes) => bytes,
        Err(error) => return fail(&request.id, error),
    };
    // the publisher's channel can be open on the browser side a moment before we register it
    let mut found = state.find_publisher(pub_id);
    for _ in 0..100 {
        if found.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        found = state.find_publisher(pub_id);
    }
    let Some((key, shared)) = found else { return fail(&request.id, format!("no publisher {pub_id}")) };
    if shared.tripped.load(Ordering::Acquire) {
        return fail(&request.id, "publisher is tripped; create a new one");
    }
    shared.stats.lock().unwrap().deadman_armed = true;
    state.deadmen.lock().unwrap().insert(pub_id, Deadman { key, bytes, shared });
    ok(&request.id, json!({}))
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
                ChannelStats::Pub(shared) => serde_json::to_value(shared.snapshot()).unwrap_or_default(),
            };
            json!({"id": entry.label.id, "type": entry.label.kind, "key": entry.label.key, "opts": entry.opts, "stats": stats})
        })
        .collect()
}
