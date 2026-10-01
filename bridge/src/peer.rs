//! One browser = one PeerConnection; each data channel it opens is dispatched by its label.

use crate::allocator::{self, Estimator};
use crate::codec::registry::CodecRegistry;
use crate::codec::{Codec, CodecOutput};
use crate::options::{HeartbeatOpts, Label, PubOpts, SubOpts};
use crate::pacing::SendGate;
use crate::publisher::{self, PubShared};
use crate::subscription::{self, SubShared, now_unix_ms};
use crate::video::{self, VideoTrack};
use base64::Engine;
use log::{debug, info, warn};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
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
const DEFAULT_LIST_PROBE_MS: u64 = 600;
const LIVELINESS_QUERY_TIMEOUT: Duration = Duration::from_secs(1);
/// A connection that stays `disconnected` this long is treated as gone.
const DISCONNECTED_GRACE: Duration = Duration::from_secs(15);
/// Longest a shutdown waits for one browser connection to close.
const SHUTDOWN_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
/// How often each frontend's bandwidth is re-estimated and re-allocated.
const ALLOCATION_INTERVAL: Duration = Duration::from_millis(250);
/// The RTT baseline is the minimum over this window.
const RTT_BASELINE_WINDOW: Duration = Duration::from_secs(30);
/// The send window covers the highest RTT over this window.
const WINDOW_RTT_WINDOW: Duration = Duration::from_secs(10);

/// Bridge-wide allocation settings (builder options / command-line flags).
#[derive(Debug, Clone, Copy)]
pub struct AllocationConfig {
    /// cap on each frontend's budget, bytes/s
    pub max_bandwidth: Option<f64>,
    /// fraction of the estimate the allocator hands out (`--bandwidth-target-fraction`)
    pub target_fraction: f64,
}

/// The frontend-level side of allocation, reported in stats as `bandwidth`.
#[derive(Debug, Default, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct BandwidthStats {
    /// data channels: delivery-rate estimate (see allocator::Estimator)
    data_estimate_bytes_per_sec: f64,
    /// video tracks: GCC target from TWCC feedback (counted only while a video track is in use)
    video_estimate_bytes_per_sec: f64,
    /// `--max-bandwidth-bytes-per-sec`
    cap_bytes_per_sec: Option<f64>,
    /// min(cap, target fraction x (data + video estimate)): what the allocator divides
    budget_bytes_per_sec: f64,
    target_fraction: f64,
    /// latest RTT minus its 30 s minimum (the delay trigger's input)
    queue_delay_ms: Option<f64>,
    min_rtt_ms: Option<f64>,
    delay_events: u64,
    /// queue delay that counts as congestion (5 ms + 2 x the path's median jitter)
    delay_threshold_ms: f64,
    /// bulk bytes allowed in flight (`null`: no strict stream to protect, so no limit)
    bulk_inflight_limit: Option<usize>,
    /// bytes/s reserved for strict-priority and reliable streams
    reserved_bytes_per_sec: f64,
    bulk_chunk_bytes: usize,
    demand_bytes_per_sec: f64,
    sent_bytes_per_sec: f64,
    network_blocked_fraction: f64,
    constrained: bool,
}

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
    codecs: Arc<CodecRegistry>,
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
    /// set once built; cleared when the peer goes (it holds the handler, which holds us)
    connection: Mutex<Option<Arc<dyn PeerConnection>>>,
    /// one renegotiation at a time
    negotiation: tokio::sync::Mutex<()>,
    video_tracks: Mutex<HashMap<String, Arc<VideoTrack>>>,
    video_target_bps: Arc<AtomicU64>,
    config: AllocationConfig,
    gate: Arc<SendGate>,
    /// (when, rtt ms) reported by the browser's clock sync
    rtt_samples: Mutex<VecDeque<(Instant, f64)>>,
    last_allocation: Mutex<Instant>,
    estimator: Mutex<Estimator>,
    bandwidth: Mutex<BandwidthStats>,
    gone: AtomicBool,
}

impl PeerState {
    fn new(peer_id: u64, bridge: &Bridge, video_target_bps: Arc<AtomicU64>) -> Self {
        let config = bridge.allocation;
        PeerState {
            peer_id,
            session: bridge.session.clone(),
            codecs: bridge.codecs.clone(),
            channels: Mutex::new(HashMap::new()),
            next_channel: AtomicU64::new(0),
            connection_state: Mutex::new(None),
            clock_offset_ms: Arc::new(Mutex::new(None)),
            rtt_ms: Mutex::new(None),
            deadmen: Mutex::new(HashMap::new()),
            control: Mutex::new(None),
            heartbeat: Mutex::new(HeartbeatStats::default()),
            connection: Mutex::new(None),
            negotiation: tokio::sync::Mutex::new(()),
            video_tracks: Mutex::new(HashMap::new()),
            video_target_bps,
            config,
            gate: Arc::default(),
            rtt_samples: Mutex::new(VecDeque::new()),
            last_allocation: Mutex::new(Instant::now()),
            estimator: Mutex::new(Estimator::default()),
            bandwidth: Mutex::new(BandwidthStats { cap_bytes_per_sec: config.max_bandwidth, target_fraction: config.target_fraction, ..Default::default() }),
            gone: AtomicBool::new(false),
        }
    }

    fn subscriptions(&self) -> Vec<Arc<SubShared>> {
        let channels = self.channels.lock().unwrap();
        channels
            .values()
            .filter_map(|entry| match &entry.stats {
                ChannelStats::Sub(shared) => Some(shared.clone()),
                ChannelStats::Pub(_) => None,
            })
            .collect()
    }

    /// (queue delay, minimum RTT): the smallest RTT since `since` minus the window's minimum. A
    /// standing queue raises every sample; scheduling noise in the browser only some of them.
    fn queue_delay(&self, since: Instant) -> (Option<f64>, Option<f64>) {
        let samples = self.rtt_samples.lock().unwrap();
        let minimum = samples.iter().map(|(_, rtt)| *rtt).reduce(f64::min);
        let newest = samples.iter().filter(|(at, _)| *at >= since).map(|(_, rtt)| *rtt).reduce(f64::min);
        (newest.zip(minimum).map(|(newest, minimum)| newest - minimum), minimum)
    }

    /// The highest RTT over the last `WINDOW_RTT_WINDOW` (what a send window must cover).
    fn high_rtt(&self, now: Instant) -> Option<f64> {
        let samples = self.rtt_samples.lock().unwrap();
        samples.iter().filter(|(at, _)| now.duration_since(*at) <= WINDOW_RTT_WINDOW).map(|(_, rtt)| *rtt).reduce(f64::max)
    }

    /// One allocation round: measure, update the estimate, divide the budget among streams.
    fn allocate(&self, interval_secs: f64) {
        let now = Instant::now();
        let since = std::mem::replace(&mut *self.last_allocation.lock().unwrap(), now);
        let (queue_delay_ms, min_rtt_ms) = self.queue_delay(since);
        let high_rtt_ms = self.high_rtt(now);
        let subscriptions = self.subscriptions();
        let usages: Vec<subscription::Usage> = subscriptions.iter().map(|shared| shared.usage(interval_secs)).collect();
        let data_usages = || usages.iter().filter(|usage| !usage.is_video);
        let sent: u64 = data_usages().map(|usage| usage.bytes_sent).sum();
        let blocked_ms: f64 = data_usages().map(|usage| usage.network_blocked_ms).sum();
        let active = data_usages().filter(|usage| usage.bytes_sent > 0 || usage.network_blocked_ms > 0.0).count();
        let data_demand: f64 = data_usages().map(|usage| usage.demand.wants()).sum();
        let total_demand: f64 = usages.iter().map(|usage| usage.demand.wants()).sum();
        let estimator = {
            let mut estimator = self.estimator.lock().unwrap();
            estimator.update(allocator::Interval { now, secs: interval_secs, sent_bytes: sent as f64, blocked_secs: blocked_ms / 1000.0, active_senders: active, data_demand, queue_delay_ms });
            estimator.clone()
        };
        let has_video = usages.iter().any(|usage| usage.is_video);
        let video_estimate = if has_video { video::gcc_target_bytes_per_sec(&self.video_target_bps) } else { 0.0 };
        let estimate = estimator.data_bytes_per_sec + video_estimate;
        let target_fraction = self.config.target_fraction;
        let usable = estimate * target_fraction;
        let budget = self.config.max_bandwidth.map_or(usable, |cap| cap.min(usable));
        let is_video: Vec<bool> = usages.iter().map(|usage| usage.is_video).collect();
        let demands: Vec<allocator::Demand> = usages.into_iter().map(|usage| usage.demand).collect();
        let reserved: f64 = demands.iter().filter_map(|demand| demand.fixed_bytes_per_sec).sum();
        // the in-flight limit only protects strict streams' latency; without one it only costs throughput
        let strict_present = subscriptions.iter().any(|shared| shared.is_strict());
        self.gate.configure((budget - reserved).max(0.0), min_rtt_ms, allocator::DELAY_THRESHOLD_MS, strict_present);
        self.gate.set_window(budget, min_rtt_ms, high_rtt_ms);
        let video_cap = video_estimate.min(budget);
        let allocations = allocate_within_video_cap(budget, video_cap, demands, &is_video);
        for (shared, allocation) in subscriptions.iter().zip(allocations) {
            shared.apply(allocation);
        }
        *self.bandwidth.lock().unwrap() = BandwidthStats {
            data_estimate_bytes_per_sec: estimator.data_bytes_per_sec,
            video_estimate_bytes_per_sec: video_estimate,
            cap_bytes_per_sec: self.config.max_bandwidth,
            budget_bytes_per_sec: budget,
            target_fraction,
            queue_delay_ms,
            min_rtt_ms,
            delay_events: estimator.delay_events,
            delay_threshold_ms: estimator.delay_threshold_ms,
            bulk_inflight_limit: self.gate.inflight_limit(),
            reserved_bytes_per_sec: reserved,
            bulk_chunk_bytes: self.gate.chunk_bytes(),
            demand_bytes_per_sec: total_demand,
            sent_bytes_per_sec: estimator.sent_bytes_per_sec,
            network_blocked_fraction: estimator.network_blocked_fraction,
            constrained: total_demand > budget,
        };
    }

    /// Clock sample reported by the browser (it has all four NTP timestamps).
    fn record_clock(&self, offset_ms: Option<f64>, rtt_ms: Option<f64>) {
        if let Some(offset) = offset_ms.filter(|v| v.is_finite()) {
            *self.clock_offset_ms.lock().unwrap() = Some(offset);
        }
        if let Some(rtt) = rtt_ms.filter(|v| v.is_finite() && *v >= 0.0) {
            *self.rtt_ms.lock().unwrap() = Some(rtt);
            let now = Instant::now();
            let mut samples = self.rtt_samples.lock().unwrap();
            samples.push_back((now, rtt));
            while samples.front().is_some_and(|(at, _)| now.duration_since(*at) > RTT_BASELINE_WINDOW) {
                samples.pop_front();
            }
        }
    }

    /// Sends `{event, ...}` on the control channel, waiting briefly if it is still opening
    /// (a page creates its channels together with `control`).
    async fn send_event(&self, event: Value) {
        for _ in 0..200 {
            let control = self.control.lock().unwrap().clone();
            if let Some(control) = control {
                let _ = control.send_text(&event.to_string()).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        warn!("peer {}: control channel never opened, dropped {event}", self.peer_id);
    }

    async fn send_rejection(&self, id: Option<u64>, reason: &str) {
        self.send_event(json!({"event": "rejected", "id": id, "reason": reason})).await;
    }

    async fn send_accepted(&self, id: Option<u64>) {
        self.send_event(json!({"event": "accepted", "id": id})).await;
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

/// Every connected browser, and what they share: the zenoh session and the codecs.
pub struct Bridge {
    session: zenoh::Session,
    codecs: Arc<CodecRegistry>,
    allocation: AllocationConfig,
    peers: Mutex<HashMap<u64, PeerEntry>>,
    next_peer: AtomicU64,
}

impl Bridge {
    pub fn new(session: zenoh::Session, codecs: CodecRegistry, allocation: AllocationConfig) -> Arc<Self> {
        Arc::new(Bridge {
            session,
            codecs: Arc::new(codecs),
            allocation,
            peers: Mutex::new(HashMap::new()),
            next_peer: AtomicU64::new(1),
        })
    }

    /// Non-trickle signaling: take an offer, return an answer with all our candidates in it.
    pub async fn answer(self: &Arc<Self>, offer: RTCSessionDescription) -> anyhow::Result<RTCSessionDescription> {
        let peer_id = self.next_peer.fetch_add(1, Ordering::Relaxed);
        let (gathered_tx, mut gathered_rx) = mpsc::channel::<()>(1);
        let (media_engine, interceptors, video_target_bps) = video::media_setup()?;
        let state = Arc::new(PeerState::new(peer_id, self, video_target_bps));
        let handler = Arc::new(Handler { bridge: Arc::downgrade(self), peer_id, gathered_tx, state: state.clone() });
        let setting_engine = SettingEngineBuilder::new()
            .with_sctp_max_message_size(SctpMaxMessageSize::Bounded(MAX_MESSAGE_SIZE))
            .build();
        let peer_connection = PeerConnectionBuilder::new()
            .with_configuration(RTCConfigurationBuilder::new().build())
            .with_setting_engine(setting_engine)
            .with_media_engine(media_engine)
            .with_interceptor_registry(interceptors)
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
        let connection: Arc<dyn PeerConnection> = Arc::new(peer_connection);
        *state.connection.lock().unwrap() = Some(connection.clone());
        tokio::spawn(run_allocator(Arc::downgrade(&state)));
        self.peers.lock().unwrap().insert(peer_id, PeerEntry { connection, state });
        info!("peer {peer_id}: answered");
        Ok(local)
    }

    fn drop_peer(&self, peer_id: u64) {
        if let Some(entry) = self.peers.lock().unwrap().remove(&peer_id) {
            info!("peer {peer_id}: gone");
            entry.state.gone.store(true, Ordering::Relaxed);
            entry.state.connection.lock().unwrap().take();
            entry.state.video_tracks.lock().unwrap().clear();
            // the channels of a browser that vanished never report their own close
            for channel in entry.state.channels.lock().unwrap().values() {
                if let ChannelStats::Sub(shared) = &channel.stats {
                    shared.close();
                }
            }
            tokio::spawn(async move {
                entry.state.fire_deadmen("disconnected").await;
                let _ = entry.connection.close().await;
            });
        }
    }

    /// Fires every frontend's deadmen ("shutdown") and closes its connection.
    pub async fn shutdown(&self) {
        let peers: Vec<PeerEntry> = self.peers.lock().unwrap().drain().map(|(_, entry)| entry).collect();
        for entry in &peers {
            entry.state.fire_deadmen("shutdown").await;
        }
        // a peer whose transport is already broken can take forever to close; never hold up exit
        for entry in peers {
            let _ = tokio::time::timeout(SHUTDOWN_CLOSE_TIMEOUT, entry.connection.close()).await;
        }
    }
}

/// Re-allocates the frontend's bandwidth every ALLOCATION_INTERVAL until it goes.
async fn run_allocator(state: Weak<PeerState>) {
    let mut ticker = tokio::time::interval(ALLOCATION_INTERVAL);
    let mut last = Instant::now();
    loop {
        ticker.tick().await;
        let Some(state) = state.upgrade() else { break };
        if state.gone.load(Ordering::Relaxed) {
            break;
        }
        let now = Instant::now();
        state.allocate(now.duration_since(last).as_secs_f64());
        last = now;
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
        "sub" => match SubOpts::parse(&label.opts).and_then(|opts| {
            let codec = opts.resolve_codec(&state.codecs)?;
            // JPEG files ride the subscription's own data channel; only H.264 needs a track
            let video = if opts.jpeg() { None } else { bind_video(&state, codec.as_deref(), &label)? };
            Ok((opts, codec, video))
        }) {
            Ok((opts, codec, video)) => {
                let shared = Arc::new(SubShared::new(&opts, codec, state.codecs.clone(), state.gate.clone()));
                register(opts.normalized(), ChannelStats::Sub(shared.clone()));
                state.send_accepted(label.id).await;
                subscription::run(dc.clone(), label.clone(), session, shared, video).await;
                None
            }
            Err(error) => Some(error),
        },
        "pub" => match PubOpts::parse(&label.opts) {
            Ok(opts) => {
                let shared = Arc::new(PubShared::default());
                register(json!({"delivery": opts.delivery, "priority": opts.priority, "latencyLimit": opts.latency_limit}), ChannelStats::Pub(shared.clone()));
                state.send_accepted(label.id).await;
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
        state.send_rejection(label.id, &error).await;
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

/// A video codec subscription claims the track the browser renegotiated for it (by mid).
fn bind_video(state: &PeerState, codec: Option<&dyn Codec>, label: &Label) -> Result<Option<Arc<VideoTrack>>, String> {
    let Some(codec) = codec.filter(|codec| codec.output() == CodecOutput::Video) else { return Ok(None) };
    let mid = label.mid.as_deref().ok_or_else(|| format!("{} is a video codec: the label needs the mid of a renegotiated video transceiver", codec.name()))?;
    let track = state.video_tracks.lock().unwrap().get(mid).cloned().ok_or_else(|| format!("no video track for mid {mid:?} (renegotiate with addVideo first)"))?;
    if !track.claim() {
        return Err(format!("video track {mid:?} is in use by another subscription"));
    }
    Ok(Some(track))
}

/// `allocator::allocate` over every stream, except that video streams together get no more than
/// `video_cap` (GCC's target): their bytes leave through the GCC-paced RTP track, not the data channels, so a
/// share of the data-channel estimate is bandwidth they cannot use. Granting it anyway made the
/// encoders outrun the pacer, and the excess queued there as latency (seconds, after a subscription
/// started, while GCC was still climbing from its initial rate). Data streams get what is left.
fn allocate_within_video_cap(budget: f64, video_cap: f64, demands: Vec<allocator::Demand>, is_video: &[bool]) -> Vec<allocator::Allocation> {
    let joint = allocator::allocate(budget, &demands);
    let video_total: f64 = joint.iter().zip(is_video).filter(|(_, video)| **video).map(|(allocation, _)| allocation.budget_bytes_per_sec).sum();
    if video_total <= video_cap * (1.0 + 1e-9) {
        return joint;
    }
    let (mut video_indices, mut video_demands, mut data_indices, mut data_demands) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (index, demand) in demands.into_iter().enumerate() {
        if is_video[index] {
            video_indices.push(index);
            video_demands.push(demand);
        } else {
            data_indices.push(index);
            data_demands.push(demand);
        }
    }
    let video_allocations = allocator::allocate(video_cap, &video_demands);
    let video_used: f64 = video_allocations.iter().map(|allocation| allocation.budget_bytes_per_sec).sum();
    let data_allocations = allocator::allocate((budget - video_used).max(0.0), &data_demands);
    let mut out: Vec<Option<allocator::Allocation>> = vec![None; is_video.len()];
    for (index, allocation) in video_indices.into_iter().zip(video_allocations).chain(data_indices.into_iter().zip(data_allocations)) {
        out[index] = Some(allocation);
    }
    out.into_iter().map(|allocation| allocation.expect("every stream allocated")).collect()
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
    probe_ms: Option<u64>,
    #[serde(default)]
    bytes: Option<String>,
    #[serde(default)]
    sdp: Option<RTCSessionDescription>,
    #[serde(default)]
    add_video: bool,
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
            "listTopics" => {
                let dc = dc.clone();
                let session = state.session.clone();
                tokio::spawn(async move {
                    let filter = if request.key.is_empty() { "**".to_owned() } else { request.key.clone() };
                    let probe = Duration::from_millis(request.probe_ms.unwrap_or(DEFAULT_LIST_PROBE_MS));
                    let response = match list_topics(&session, &filter, probe).await {
                        Ok(topics) => ok(&request.id, json!({"topics": topics})),
                        Err(error) => fail(&request.id, error),
                    };
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
                let bandwidth = serde_json::to_value(state.bandwidth.lock().unwrap().clone()).unwrap_or_default();
                ok(&request.id, json!({"channels": collect_stats(&state), "clock": clock, "heartbeat": heartbeat, "bandwidth": bandwidth}))
            }
            "setDeadman" => set_deadman(&state, &request).await,
            "codecs" => {
                let codecs: Vec<Value> = state.codecs.list().map(|(name, output)| json!({"name": name, "output": output.as_str()})).collect();
                ok(&request.id, json!({"codecs": codecs}))
            }
            // can wait on the peer connection's driver; keep stats and pings flowing meanwhile
            "renegotiate" => {
                let dc = dc.clone();
                let state = state.clone();
                tokio::spawn(async move {
                    let response = handle_renegotiate(&state, request).await;
                    let _ = dc.send_text(&response.to_string()).await;
                });
                continue;
            }
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

/// Browser-initiated renegotiation (it added a recvonly video transceiver): answer, and with
/// `addVideo` report the mid of the new track.
async fn handle_renegotiate(state: &PeerState, request: ControlRequest) -> Value {
    let Some(offer) = request.sdp else { return fail(&request.id, "renegotiate needs sdp") };
    let Some(connection) = state.connection.lock().unwrap().clone() else { return fail(&request.id, "peer is gone") };
    let _negotiating = state.negotiation.lock().await;
    match video::renegotiate(&connection, offer, request.add_video).await {
        Ok((answer, track)) => {
            let mid = track.map(|track| {
                let mid = track.mid.clone();
                state.video_tracks.lock().unwrap().insert(mid.clone(), track);
                mid
            });
            ok(&request.id, json!({"sdp": answer, "mid": mid}))
        }
        Err(error) => {
            warn!("peer {}: renegotiation failed: {error:#}", state.peer_id);
            fail(&request.id, format!("{error:#}"))
        }
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

/// Every key the bridge can find live under `filter`, with where it was seen:
/// - `advancedPublisher`: liveliness tokens of zenoh-ext AdvancedPublishers with publisher_detection
/// - `token`: any other liveliness token
/// - `sample`: data seen on a `filter` subscription during `probe` (catches undeclared publishers);
///   a zero `probe` skips that subscription
///
/// Plain publishers, subscribers and queryables without a token are invisible.
async fn list_topics(session: &zenoh::Session, filter: &str, probe: Duration) -> anyhow::Result<Vec<Value>> {
    let found: Arc<Mutex<BTreeMap<String, BTreeSet<&'static str>>>> = Arc::default();
    let note = |found: &Mutex<BTreeMap<String, BTreeSet<&'static str>>>, key: &str, source: &'static str| {
        found.lock().unwrap().entry(key.to_owned()).or_default().insert(source);
    };
    let sink = found.clone();
    // probe 0 = tokens only: no subscription, so publishers that only send while matched (matching
    // listeners) aren't woken by a listing
    let probe_subscriber = match probe.is_zero() {
        true => None,
        false => Some(
            session
                .declare_subscriber(filter)
                .callback(move |sample| note(&sink, sample.key_expr().as_str(), "sample"))
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?,
        ),
    };
    for selector in [filter.to_owned(), format!("{filter}/@adv/pub/**")] {
        let Ok(replies) = session.liveliness().get(selector.as_str()).timeout(LIVELINESS_QUERY_TIMEOUT).await else { continue };
        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let token = sample.key_expr().as_str();
                match token.split_once("/@adv/pub/") {
                    Some((key, _)) => note(&found, key, "advancedPublisher"),
                    None => note(&found, token, "token"),
                }
            }
        }
    }
    tokio::time::sleep(probe).await;
    drop(probe_subscriber);
    let found = found.lock().unwrap();
    Ok(found.iter().map(|(key, sources)| json!({"key": key, "sources": sources})).collect())
}

async fn handle_get(session: &zenoh::Session, request: &ControlRequest) -> Value {
    let base64 = base64::engine::general_purpose::STANDARD;
    let timeout = Duration::from_millis(request.timeout_ms.unwrap_or(DEFAULT_GET_TIMEOUT_MS));
    let replies = match session.get(request.key.as_str()).timeout(timeout).await {
        Ok(replies) => replies,
        Err(error) => return fail(&request.id, error),
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
    ok(&request.id, json!({"replies": results}))
}

fn collect_stats(state: &PeerState) -> Vec<Value> {
    let channels = state.channels.lock().unwrap();
    channels
        .values()
        .map(|entry| {
            let (stats, allocation) = match &entry.stats {
                ChannelStats::Sub(shared) => (serde_json::to_value(shared.stats()).unwrap_or_default(), serde_json::to_value(shared.allocation()).unwrap_or_default()),
                ChannelStats::Pub(shared) => (serde_json::to_value(shared.snapshot()).unwrap_or_default(), Value::Null),
            };
            json!({"id": entry.label.id, "type": entry.label.kind, "key": entry.label.key, "opts": entry.opts, "stats": stats, "allocation": allocation})
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream(bytes_per_message: f64, hz: f64) -> allocator::Demand {
        allocator::Demand { max_hz: hz, quality_range: None, tradeoff: 0.5, price: Box::new(move |_| bytes_per_message), fixed_bytes_per_sec: None }
    }

    #[test]
    fn video_gets_no_more_than_its_cap_and_data_gets_the_rest() {
        // budget 1 MB/s; the video stream wants 500 KB/s but GCC allows 100 KB/s; data wants 500 KB/s
        let allocations = allocate_within_video_cap(1_000_000.0, 100_000.0, vec![stream(50_000.0, 10.0), stream(50_000.0, 10.0)], &[true, false]);
        assert!(allocations[0].budget_bytes_per_sec <= 100_000.0 + 1e-6, "{:?}", allocations[0]);
        assert_eq!(allocations[1].budget_bytes_per_sec, 500_000.0, "{:?}", allocations[1]);
        // under the cap nothing changes
        let allocations = allocate_within_video_cap(1_000_000.0, 600_000.0, vec![stream(50_000.0, 10.0), stream(50_000.0, 10.0)], &[true, false]);
        assert_eq!((allocations[0].budget_bytes_per_sec, allocations[1].budget_bytes_per_sec), (500_000.0, 500_000.0));
    }

    /// zenoh 1.7.0-1.10.1 answer the admin space under the routing tables' read lock and take it again, so a racing declaration deadlocked every thread (it hung web_ctrl).
    #[test]
    fn list_topics_survives_concurrent_declarations() {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
            runtime.block_on(async {
                let mut config = zenoh::Config::default();
                config.insert_json5("scouting/multicast/enabled", "false").unwrap();
                config.insert_json5("listen/endpoints", "[]").unwrap();
                config.insert_json5("adminspace/enabled", "true").unwrap();
                config.insert_json5("adminspace/permissions", r#"{"read": true, "write": false}"#).unwrap();
                let session = zenoh::open(config).await.unwrap();
                let deadline = Instant::now() + Duration::from_secs(3);
                let mut tasks = Vec::new();
                for worker in 0..3 {
                    let session = session.clone();
                    tasks.push(tokio::spawn(async move {
                        let mut round = 0;
                        while Instant::now() < deadline {
                            let subscriber = session.declare_subscriber(format!("deadlock/{worker}/{round}")).await.unwrap();
                            subscriber.undeclare().await.unwrap();
                            round += 1;
                        }
                    }));
                }
                for _ in 0..3 {
                    let session = session.clone();
                    tasks.push(tokio::spawn(async move {
                        while Instant::now() < deadline {
                            list_topics(&session, "**", Duration::ZERO).await.unwrap();
                            // what listTopics used to ask: the bridge's own declarations, from the admin space
                            let replies = session.get("@/*/*/subscriber/**").timeout(Duration::from_secs(1)).await.unwrap();
                            while replies.recv_async().await.is_ok() {}
                        }
                    }));
                }
                for task in tasks {
                    task.await.unwrap();
                }
            });
            let _ = done_tx.send(());
        });
        assert!(done_rx.recv_timeout(Duration::from_secs(30)).is_ok(), "listTopics and declarations deadlocked");
    }
}
