//! One browser = one PeerConnection; each data channel it opens is dispatched by its label.

use crate::allocator::{self, Estimator};
use crate::api;
use crate::auth::{Grant, Leases};
use crate::ice::{self, IceServer};
use crate::encoding::registry::{EncodingRegistry, Resolved};
use crate::encoding::Channel;
use crate::options::{HeartbeatOpts, Label, PubOpts, SubOpts};
use crate::pacing::SendGate;
use crate::publisher::{self, PubShared};
use crate::subscription::{self, SubShared, now_unix_ms};
use crate::media::{self, MediaTrack, TrackClaim};
use base64::Engine;
use log::{debug, info, warn};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use parking_lot::Mutex;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfigurationBuilder,
    RTCIceGatheringState, RTCPeerConnectionState, RTCSessionDescription, SettingEngineBuilder,
};
use rtc::peer_connection::configuration::RTCIceServer;
use rtc::peer_connection::configuration::setting_engine::SctpMaxMessageSize;
use zenoh::qos::{CongestionControl, Priority, Reliability};

/// Largest message we accept from (and advertise to) the browser; Chrome sends 256 KiB too.
const MAX_MESSAGE_SIZE: u32 = 256 * 1024;
const GATHER_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_LIST_PROBE_MS: u64 = 600;
/// A page's probeMs is capped: its `**` subscriber recorded every key and kept the peer alive until it ended.
const MAX_LIST_PROBE_MS: u64 = 10_000;
const LIVELINESS_QUERY_TIMEOUT: Duration = Duration::from_secs(1);
/// A connection that stays `disconnected` this long is treated as gone.
const DISCONNECTED_GRACE: Duration = Duration::from_secs(15);
/// An answered connection that hasn't connected this long after is treated as gone (ICE may never report `failed`).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Longest a shutdown waits for one browser connection to close.
const SHUTDOWN_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
/// How often each frontend's bandwidth is re-estimated and re-allocated.
const ALLOCATION_INTERVAL: Duration = Duration::from_millis(250);
/// The RTT baseline is the minimum over this window (a standing queue lasting longer would become the baseline).
const RTT_BASELINE_WINDOW: Duration = Duration::from_secs(60);
/// The video pacer's floor (32 kbit/s), whatever the allocator lets video take.
const VIDEO_HEADROOM_BYTES_PER_SEC: f64 = 4_000.0;
/// GCC below this fraction of what video sent is congestion for the whole link.
const VIDEO_OVERUSE_FRACTION: f64 = 0.85;
/// The send window covers the highest RTT over this window.
const WINDOW_RTT_WINDOW: Duration = Duration::from_secs(10);

/// Gateway-wide allocation settings (builder options / command-line flags).
#[derive(Debug, Clone, Copy)]
pub struct AllocationConfig {
    /// cap on each frontend's budget, bytes/s
    pub max_bandwidth: Option<f64>,
    /// fraction of the estimate the allocator hands out (`--bandwidth-target-fraction`)
    pub target_fraction: f64,
}

/// How the gateway's side of each connection reaches browsers (builder options / command-line flags).
#[derive(Debug, Clone, Default)]
pub struct ConnectConfig {
    pub ice_servers: Vec<IceServer>,
    /// told about every request a connection's grant refuses
    pub(crate) on_refused: Option<crate::auth::RefusalHook>,
    /// coturn's static-auth-secret and the minted credentials' lifetime
    pub turn_secret: Option<(String, Duration)>,
    /// servers minted per connection, after the static ones
    pub ice_servers_fn: Option<ice::IceHook>,
    pub udp_ports: Option<RangeInclusive<u16>>,
}

/// The frontend-level side of allocation, reported in stats as `bandwidth`.
#[derive(Debug, Default, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct BandwidthStats {
    /// the link's capacity estimate, video and data together (see allocator::Estimator)
    data_estimate_bytes_per_sec: f64,
    /// most the video tracks may take: GCC's target from TWCC feedback (only while a video track is in use; GCC grows while
    /// video sends less, so it caps video's share of the link estimate, never adds to it)
    video_estimate_bytes_per_sec: f64,
    /// what the video tracks sent
    video_sent_bytes_per_sec: f64,
    /// `--max-bandwidth-bytes-per-sec`
    cap_bytes_per_sec: Option<f64>,
    /// min(cap, target fraction x the link estimate): what the allocator divides
    budget_bytes_per_sec: f64,
    target_fraction: f64,
    /// latest RTT minus its 60 s minimum (the delay trigger's input)
    queue_delay_ms: Option<f64>,
    min_rtt_ms: Option<f64>,
    delay_events: u64,
    backlog_events: u64,
    blocked_events: u64,
    video_overuse_events: u64,
    /// queue delay that counts as congestion (5 ms + the spread of the path's queue delays over 10 s, at most 505 ms)
    delay_threshold_ms: f64,
    /// bulk bytes allowed in flight (`null`: no strict stream to protect, so no limit)
    bulk_inflight_limit: Option<usize>,
    /// bytes/s reserved for strict-priority and reliable streams
    reserved_bytes_per_sec: f64,
    bulk_chunk_bytes: usize,
    demand_bytes_per_sec: f64,
    sent_bytes_per_sec: f64,
    /// what the browser acknowledged over the last second (data channels, plus what video sent)
    delivered_bytes_per_sec: f64,
    network_blocked_fraction: f64,
    /// share of the interval senders waited for the page (its JS behind on what arrived) rather than the network
    page_blocked_fraction: f64,
    /// how long the page has had the oldest frame it hasn't gotten past, and how long before channels wait
    page_lag_ms: f64,
    max_page_lag_ms: f64,
    /// how long the oldest frame SCTP hasn't delivered has been in it, and how long before channels wait
    network_lag_ms: f64,
    max_network_lag_ms: f64,
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
    codecs: Arc<EncodingRegistry>,
    channels: Mutex<HashMap<u64, ChannelEntry>>,
    next_channel: AtomicU64,
    connection_state: Mutex<Option<RTCPeerConnectionState>>,
    /// gateway clock minus browser clock, as estimated (and reported) by the browser
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
    tracks: Mutex<HashMap<String, Arc<MediaTrack>>>,
    video_rate: Arc<media::VideoRate>,
    config: AllocationConfig,
    gate: Arc<SendGate>,
    /// (when, rtt ms) reported by the browser's clock sync
    rtt_samples: Mutex<VecDeque<(Instant, f64)>>,
    last_allocation: Mutex<Instant>,
    estimator: Mutex<Estimator>,
    /// `SendGate::delivered_total` at the last allocation
    delivered_total: AtomicU64,
    /// wire bytes per frame byte sent lately (`subscription::PACKET_OVERHEAD_BYTES`), to read acknowledged frame bytes
    wire_scale: Mutex<f64>,
    bandwidth: Mutex<BandwidthStats>,
    gone: AtomicBool,
    /// flips to true when the peer is dropped: its channel tasks end on it, since a closed connection's data channels
    /// never report their own close (each one left waiting held this state and the connection, ~2 MB a browser)
    gone_signal: tokio::sync::watch::Sender<bool>,
    /// reached `connected` at least once
    ever_connected: AtomicBool,
    gateway: Weak<Gateway>,
    /// the bearer token it connected with, and what it may do
    token: Option<String>,
    grant: Grant,
    on_refused: Option<crate::auth::RefusalHook>,
    leases: Arc<Leases>,
    /// queryables, liveliness, matching listeners it declared through the control channel
    api: api::ApiState,
    /// events from those (queries, liveliness, matching), forwarded to the control channel
    api_events: tokio::sync::mpsc::UnboundedSender<Value>,
}

/// PeerStates alive: a browser connection's state must go when the connection does.
static LIVE_PEER_STATES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// How many browser connections' states are alive (dropped ones included until their tasks end): for leak tests.
#[doc(hidden)]
pub fn live_peer_states() -> usize {
    LIVE_PEER_STATES.load(Ordering::Relaxed)
}

impl Drop for PeerState {
    fn drop(&mut self) {
        LIVE_PEER_STATES.fetch_sub(1, Ordering::Relaxed);
    }
}

impl PeerState {
    fn new(
        peer_id: u64,
        gateway: &Arc<Gateway>,
        video_rate: Arc<media::VideoRate>,
        token: Option<String>,
        grant: Grant,
    ) -> (Self, tokio::sync::mpsc::UnboundedReceiver<Value>) {
        let config = gateway.allocation;
        let (api_events, api_events_receiver) = tokio::sync::mpsc::unbounded_channel();
        LIVE_PEER_STATES.fetch_add(1, Ordering::Relaxed);
        let state = PeerState {
            api: api::ApiState::default(),
            api_events,
            gateway: Arc::downgrade(gateway),
            token,
            grant,
            on_refused: gateway.connect_config.on_refused.clone(),
            leases: gateway.leases.clone(),
            peer_id,
            session: gateway.session.clone(),
            codecs: gateway.codecs.clone(),
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
            tracks: Mutex::new(HashMap::new()),
            video_rate,
            config,
            gate: Arc::default(),
            rtt_samples: Mutex::new(VecDeque::new()),
            last_allocation: Mutex::new(Instant::now()),
            estimator: Mutex::new(Estimator::default()),
            delivered_total: AtomicU64::new(0),
            wire_scale: Mutex::new(1.0),
            bandwidth: Mutex::new(BandwidthStats { cap_bytes_per_sec: config.max_bandwidth, target_fraction: config.target_fraction, ..Default::default() }),
            gone: AtomicBool::new(false),
            gone_signal: tokio::sync::watch::Sender::new(false),
            ever_connected: AtomicBool::new(false),
        };
        (state, api_events_receiver)
    }

    /// Resolves once the peer is dropped.
    async fn wait_gone(&self) {
        let _ = self.gone_signal.subscribe().wait_for(|gone| *gone).await;
    }

    fn subscriptions(&self) -> Vec<Arc<SubShared>> {
        let channels = self.channels.lock();
        channels.values().filter_map(|entry| if let ChannelStats::Sub(shared) = &entry.stats { Some(shared.clone()) } else { None }).collect()
    }

    /// (queue delay, minimum RTT): the smallest RTT since `since` minus the window's minimum. A
    /// standing queue raises every sample; scheduling noise in the browser only some of them.
    fn queue_delay(&self, since: Instant) -> (Option<f64>, Option<f64>) {
        let samples = self.rtt_samples.lock();
        let minimum = samples.iter().map(|(_, rtt)| *rtt).reduce(f64::min);
        let newest = samples.iter().filter(|(at, _)| *at >= since).map(|(_, rtt)| *rtt).reduce(f64::min);
        (newest.zip(minimum).map(|(newest, minimum)| newest - minimum), minimum)
    }

    /// The highest RTT over the last `WINDOW_RTT_WINDOW` (what a send window must cover).
    fn high_rtt(&self, now: Instant) -> Option<f64> {
        let samples = self.rtt_samples.lock();
        samples.iter().filter(|(at, _)| now.duration_since(*at) <= WINDOW_RTT_WINDOW).map(|(_, rtt)| *rtt).reduce(f64::max)
    }

    /// One allocation round: measure, update the estimate, divide the budget among streams.
    fn allocate(&self, interval_secs: f64) {
        let now = Instant::now();
        let since = std::mem::replace(&mut *self.last_allocation.lock(), now);
        let (queue_delay_ms, min_rtt_ms) = self.queue_delay(since);
        let high_rtt_ms = self.high_rtt(now);
        let subscriptions = self.subscriptions();
        let usages: Vec<subscription::Usage> = subscriptions.iter().map(|shared| shared.usage(interval_secs)).collect();
        let data_usages = || usages.iter().filter(|usage| !usage.on_track);
        let sent: u64 = usages.iter().map(|usage| usage.bytes_sent).sum();
        let video_sent = usages.iter().filter(|usage| usage.on_track).map(|usage| usage.bytes_sent).sum::<u64>() as f64 / interval_secs.max(1e-3);
        let has_video = usages.iter().any(|usage| usage.on_track);
        let gcc = self.video_rate.gcc_bytes_per_sec();
        let video_overuse = has_video && video_sent > VIDEO_HEADROOM_BYTES_PER_SEC && gcc < VIDEO_OVERUSE_FRACTION * video_sent;
        let blocked_ms: f64 = data_usages().map(|usage| usage.network_blocked_ms).sum();
        let page_blocked_ms: f64 = data_usages().map(|usage| usage.page_blocked_ms).sum();
        let active = data_usages().filter(|usage| usage.bytes_sent > 0 || usage.network_blocked_ms > 0.0 || usage.page_blocked_ms > 0.0).count();
        let total_demand: f64 = usages.iter().map(|usage| usage.demand.wants()).sum();
        // acknowledged frame bytes, scaled to the wire as what was sent was (frames + per-packet overhead)
        let data_sent = data_usages().map(|usage| usage.bytes_sent).sum::<u64>() as f64;
        let data_frame_bytes = data_usages().map(|usage| usage.frame_bytes).sum::<u64>() as f64;
        let wire_scale = {
            // the latest interval that sent anything (acks of a burst keep arriving after it)
            let mut scale = self.wire_scale.lock();
            if data_frame_bytes > 0.0 {
                *scale = (data_sent / data_frame_bytes).max(1.0);
            }
            *scale
        };
        let delivered_total = self.gate.delivered_total();
        let delivered = delivered_total.saturating_sub(self.delivered_total.swap(delivered_total, Ordering::Relaxed)) as f64 * wire_scale + video_sent * interval_secs;
        let estimator = {
            let mut estimator = self.estimator.lock();
            estimator.update(allocator::Interval { now, secs: interval_secs, sent_bytes: sent as f64, blocked_secs: blocked_ms / 1000.0, page_blocked_secs: page_blocked_ms / 1000.0, active_senders: active, data_demand: total_demand, queue_delay_ms, video_overuse, delivered_bytes: delivered, high_rtt_ms });
            estimator.clone()
        };
        let video_estimate = if has_video { gcc } else { 0.0 };
        let usable = estimator.data_bytes_per_sec * self.config.target_fraction;
        let budget = self.config.max_bandwidth.map_or(usable, |cap| cap.min(usable));
        self.video_rate.set_cap_bytes_per_sec(video_estimate.min(budget).max(VIDEO_HEADROOM_BYTES_PER_SEC));
        let is_video: Vec<bool> = usages.iter().map(|usage| usage.on_track).collect();
        let demands: Vec<allocator::Demand> = usages.into_iter().map(|usage| usage.demand).collect();
        let reserved: f64 = demands.iter().filter_map(|demand| demand.fixed_bytes_per_sec).sum();
        // the in-flight limit only protects strict streams' latency; without one it only costs throughput
        let strict_present = subscriptions.iter().any(|shared| shared.is_strict());
        self.gate.configure((budget - reserved).max(0.0), min_rtt_ms, allocator::DELAY_THRESHOLD_MS, strict_present);
        self.gate.set_window(budget, min_rtt_ms, high_rtt_ms);
        let allocations = allocate_within_video_cap(budget, video_estimate.min(budget), demands, &is_video);
        let video_granted: f64 = allocations.iter().zip(&is_video).filter(|(_, video)| **video).map(|(allocation, _)| allocation.budget_bytes_per_sec).sum();
        let bulk_budget = (budget - video_granted - reserved).max(0.0);
        for (shared, allocation) in subscriptions.iter().zip(allocations) {
            shared.apply(allocation, bulk_budget);
        }
        *self.bandwidth.lock() = BandwidthStats {
            data_estimate_bytes_per_sec: estimator.data_bytes_per_sec,
            video_estimate_bytes_per_sec: video_estimate,
            video_sent_bytes_per_sec: video_sent,
            cap_bytes_per_sec: self.config.max_bandwidth,
            budget_bytes_per_sec: budget,
            target_fraction: self.config.target_fraction,
            queue_delay_ms,
            min_rtt_ms,
            delay_events: estimator.delay_events,
            backlog_events: estimator.backlog_events,
            blocked_events: estimator.blocked_events,
            video_overuse_events: estimator.video_overuse_events,
            delay_threshold_ms: estimator.delay_threshold_ms,
            bulk_inflight_limit: self.gate.inflight_limit(),
            reserved_bytes_per_sec: reserved,
            bulk_chunk_bytes: self.gate.chunk_bytes(),
            demand_bytes_per_sec: total_demand,
            sent_bytes_per_sec: estimator.sent_bytes_per_sec,
            delivered_bytes_per_sec: estimator.delivered_bytes_per_sec,
            network_blocked_fraction: estimator.network_blocked_fraction,
            page_blocked_fraction: estimator.page_blocked_fraction,
            page_lag_ms: self.gate.page_lag(now).as_secs_f64() * 1000.0,
            max_page_lag_ms: self.gate.max_page_lag().as_secs_f64() * 1000.0,
            network_lag_ms: self.gate.network_lag(now).as_secs_f64() * 1000.0,
            max_network_lag_ms: self.gate.max_network_lag().as_secs_f64() * 1000.0,
            constrained: total_demand > budget,
        };
    }

    /// Clock sample reported by the browser (it has all four NTP timestamps).
    fn record_clock(&self, offset_ms: Option<f64>, rtt_ms: Option<f64>) {
        if let Some(offset) = offset_ms.filter(|v| v.is_finite()) {
            *self.clock_offset_ms.lock() = Some(offset);
        }
        if let Some(rtt) = rtt_ms.filter(|v| v.is_finite() && *v >= 0.0) {
            *self.rtt_ms.lock() = Some(rtt);
            let now = Instant::now();
            let mut samples = self.rtt_samples.lock();
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
            let control = self.control.lock().clone();
            if let Some(control) = control {
                let _ = control.send_text(&event.to_string()).await;
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        warn!("peer {}: control channel never opened, dropped {event}", self.peer_id);
    }

    /// Its grant and the gateway's refusal hook.
    fn access(&self) -> crate::auth::Access<'_> {
        crate::auth::Access { grant: &self.grant, on_refused: self.on_refused.as_ref() }
    }

    async fn send_accepted(&self, id: Option<u64>) {
        self.send_event(json!({"event": "accepted", "id": id})).await;
    }

    fn find_publisher(&self, pub_id: u64) -> Option<(String, Arc<PubShared>)> {
        let channels = self.channels.lock();
        channels.values().find_map(|entry| match &entry.stats {
            ChannelStats::Pub(shared) if entry.label.id == Some(pub_id) => Some((entry.label.key.clone(), shared.clone())),
            _ => None,
        })
    }

    /// Publishes every armed deadman once (REAL_TIME, blocking, reliable) and trips its stream.
    async fn fire_deadmen(&self, reason: &str) {
        let armed: Vec<(u64, Deadman)> = self.deadmen.lock().drain().collect();
        for (pub_id, deadman) in armed {
            if let Some(blocked) = self.leases.blocker(self.peer_id, &deadman.key) {
                warn!("peer {}: deadman on {:?} not fired: {blocked}", self.peer_id, deadman.key);
                continue;
            }
            // trip first, so a put racing in behind the deadman can't undo it
            deadman.shared.tripped.store(true, Ordering::Release);
            deadman.shared.stats.lock().deadman_armed = false;
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
            let control = self.control.lock().clone();
            if let Some(control) = control {
                let event = json!({"event": "tripped", "id": pub_id, "reason": reason});
                let _ = control.send_text(&event.to_string()).await;
            }
        }
    }

    async fn lease_lost(&self, group: &str, reason: &str) {
        info!("peer {}: lease {group:?} lost ({reason})", self.peer_id);
        self.send_event(json!({"event": "leaseLost", "group": group, "reason": reason})).await;
    }
}

struct PeerEntry {
    connection: Arc<dyn PeerConnection>,
    state: Arc<PeerState>,
}

/// Every connected browser, and what they share: the zenoh session and the encodings.
pub struct Gateway {
    session: zenoh::Session,
    codecs: Arc<EncodingRegistry>,
    allocation: AllocationConfig,
    peers: Mutex<HashMap<u64, PeerEntry>>,
    next_peer: AtomicU64,
    pub leases: Arc<Leases>,
    pub connect_config: ConnectConfig,
    /// with `udp_ports`: peer id -> its port
    udp_ports_in_use: Mutex<HashMap<u64, u16>>,
}

impl Gateway {
    pub fn new(session: zenoh::Session, codecs: EncodingRegistry, allocation: AllocationConfig, leases: Leases, connect_config: ConnectConfig) -> Arc<Self> {
        Arc::new(Gateway {
            session,
            codecs: Arc::new(codecs),
            allocation,
            peers: Mutex::new(HashMap::new()),
            next_peer: AtomicU64::new(1),
            leases: Arc::new(leases),
            connect_config,
            udp_ports_in_use: Mutex::new(HashMap::new()),
        })
    }

    /// A port of `udp_ports` no other connection uses and nothing else has bound, reserved for `peer_id`.
    fn reserve_udp_port(&self, peer_id: u64) -> anyhow::Result<Option<u16>> {
        let Some(range) = self.connect_config.udp_ports.clone() else {
            return Ok(None);
        };
        let mut in_use = self.udp_ports_in_use.lock();
        let used: HashSet<u16> = in_use.values().copied().collect();
        let port = range.clone().find(|port| !used.contains(port) && std::net::UdpSocket::bind(("0.0.0.0", *port)).is_ok());
        let port = port.ok_or_else(|| anyhow::anyhow!("no free UDP port in {range:?} for another connection"))?;
        in_use.insert(peer_id, port);
        Ok(Some(port))
    }

    async fn lease_lost(&self, holder: u64, group: &str, reason: &str) {
        let state = self.peers.lock().get(&holder).map(|entry| entry.state.clone());
        if let Some(state) = state {
            state.lease_lost(group, reason).await;
        }
    }

    /// Every open subscription: its key expression and encoding.
    pub fn subscriptions(&self) -> Vec<(String, Option<String>)> {
        let peers = self.peers.lock();
        let channels = peers.values().flat_map(|entry| entry.state.channels.lock().values().filter_map(|channel| match &channel.stats {
            ChannelStats::Sub(shared) => Some((channel.label.key.clone(), shared.codec.as_ref().map(|codec| codec.name().to_owned()))),
            _ => None,
        }).collect::<Vec<_>>());
        channels.collect()
    }

    /// Ends `group`'s lease, telling its holder `reason`; false when nobody holds it.
    pub async fn expire_lease(&self, group: &str, reason: &str) -> bool {
        match self.leases.end(group, |_, _| true) {
            Some(holder) => {
                self.lease_lost(holder, group, reason).await;
                true
            }
            None => false,
        }
    }

    /// Closes every connection made with `token`.
    pub fn revoke(&self, token: &str) -> usize {
        let revoked: Vec<u64> = self.peers.lock().iter().filter(|(_, entry)| entry.state.token.as_deref() == Some(token)).map(|(id, _)| *id).collect();
        for peer_id in &revoked {
            self.drop_peer(*peer_id, "revoked");
        }
        revoked.len()
    }

    /// Non-trickle signaling: take an offer, return an answer with all our candidates in it.
    pub async fn answer(self: &Arc<Self>, offer: RTCSessionDescription, token: Option<String>, grant: Grant) -> anyhow::Result<RTCSessionDescription> {
        let peer_id = self.next_peer.fetch_add(1, Ordering::Relaxed);
        self.answer_as(peer_id, offer, token, grant).await
    }

    async fn answer_as(self: &Arc<Self>, peer_id: u64, offer: RTCSessionDescription, token: Option<String>, grant: Grant) -> anyhow::Result<RTCSessionDescription> {
        let request = ice::IceRequest { side: ice::IceSide::Gateway, token: token.clone() };
        let (gathered_tx, mut gathered_rx) = mpsc::channel::<()>(1);
        let (media_engine, interceptors, video_rate) = media::media_setup()?;
        let (state, mut api_events) = PeerState::new(peer_id, self, video_rate, token, grant);
        let state = Arc::new(state);
        let mut pending = PendingPeer { gateway: Arc::downgrade(self), peer_id, state: state.clone(), connection: None, armed: true };
        let forwarder = Arc::downgrade(&state);
        tokio::spawn(async move {
            while let Some(event) = api_events.recv().await {
                let Some(state) = forwarder.upgrade() else {
                    break;
                };
                state.send_event(event).await;
            }
        });
        let port = self.reserve_udp_port(peer_id)?.unwrap_or(0);
        let config = &self.connect_config;
        let ice_servers = ice::servers(&config.ice_servers, config.turn_secret.as_ref(), config.ice_servers_fn.as_ref(), request)
            .await
            .into_iter()
            .map(|server| RTCIceServer { urls: server.urls, username: server.username, credential: server.credential })
            .collect();
        let handler = Arc::new(Handler { gateway: Arc::downgrade(self), peer_id, gathered_tx, state: Arc::downgrade(&state) });
        let setting_engine = SettingEngineBuilder::new()
            .with_sctp_max_message_size(SctpMaxMessageSize::Bounded(MAX_MESSAGE_SIZE))
            .build();
        let peer_connection: Arc<dyn PeerConnection> = Arc::new(PeerConnectionBuilder::new()
            .with_configuration(RTCConfigurationBuilder::new().with_ice_servers(ice_servers).build())
            .with_setting_engine(setting_engine)
            .with_media_engine(media_engine)
            .with_interceptor_registry(interceptors)
            .with_handler(handler)
            .with_udp_addrs(vec![format!("0.0.0.0:{port}"), format!("127.0.0.1:{port}")])
            .build()
            .await?);
        pending.connection = Some(peer_connection.clone());
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
        let connection = peer_connection;
        *state.connection.lock() = Some(connection.clone());
        let gateway = Arc::downgrade(self);
        crate::spawn_isolated(format!("peer {peer_id}: allocator"), run_allocator(Arc::downgrade(&state)), async move {
            if let Some(gateway) = gateway.upgrade() {
                gateway.drop_peer(peer_id, "internal error");
            }
        });
        let watched = state.clone();
        pending.armed = false;
        self.peers.lock().insert(peer_id, PeerEntry { connection, state });
        info!("peer {peer_id}: answered");
        // failed or closed while gathering: drop_peer ran before the peer was in `peers` and did nothing
        if matches!(*watched.connection_state.lock(), Some(RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed)) {
            self.drop_peer(peer_id, "disconnected");
        }
        // a browser that gave up before connecting (a phone retrying over a bad link) may never move ICE to `failed`
        let gateway = Arc::downgrade(self);
        tokio::spawn(async move {
            tokio::select! {
                () = tokio::time::sleep(CONNECT_TIMEOUT) => {}
                () = watched.wait_gone() => return,
            }
            if !watched.ever_connected.load(Ordering::Relaxed) && let Some(gateway) = gateway.upgrade() {
                gateway.drop_peer(peer_id, "never connected");
            }
        });
        Ok(local)
    }

    fn drop_peer(&self, peer_id: u64, reason: &'static str) {
        self.udp_ports_in_use.lock().remove(&peer_id);
        self.leases.release_all(peer_id);
        if let Some(entry) = self.peers.lock().remove(&peer_id) {
            info!("peer {peer_id}: gone ({reason})");
            entry.state.gone.store(true, Ordering::Relaxed);
            // the channels of a browser that vanished never report their own close; closed before the gone signal,
            // which cancels the channel tasks (a subscription cancelled first would never be closed)
            for channel in entry.state.channels.lock().values() {
                if let ChannelStats::Sub(shared) = &channel.stats {
                    shared.close();
                }
            }
            entry.state.gone_signal.send_replace(true);
            entry.state.api.clear();
            entry.state.connection.lock().take();
            entry.state.tracks.lock().clear();
            entry.state.gate.forget_all();
            tokio::spawn(async move {
                entry.state.fire_deadmen(reason).await;
                // a browser can take ~30 s to notice a closed connection by itself
                let control = entry.state.control.lock().take();
                if let Some(control) = control {
                    let _ = control.send_text(&json!({"event": "closed", "reason": reason}).to_string()).await;
                    // closing right away would drop it unsent
                    for _ in 0..50 {
                        if control.outstanding_bytes().await.unwrap_or(0) == 0 {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
                let _ = entry.connection.close().await;
            });
        }
    }

    /// Fires every frontend's deadmen ("shutdown") and closes its connection.
    pub async fn shutdown(&self) {
        let peers: Vec<PeerEntry> = self.peers.lock().drain().map(|(_, entry)| entry).collect();
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
        // what the browser acknowledged, read now: senders only read it while sending, and a burst's last reading
        // before its acks came in would look like a growing backlog
        state.gate.refresh_delivered().await;
        let now = Instant::now();
        state.allocate(now.duration_since(last).as_secs_f64());
        last = now;
    }
}

/// A connection being answered that never made it into `peers` (an error, or the HTTP request dropped mid-answer):
/// dropping it closes the connection, whose driver otherwise ran on holding all of it, and frees its UDP port.
struct PendingPeer {
    gateway: Weak<Gateway>,
    peer_id: u64,
    state: Arc<PeerState>,
    connection: Option<Arc<dyn PeerConnection>>,
    armed: bool,
}

impl Drop for PendingPeer {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(gateway) = self.gateway.upgrade() {
            gateway.udp_ports_in_use.lock().remove(&self.peer_id);
        }
        self.state.gone.store(true, Ordering::Relaxed);
        self.state.gone_signal.send_replace(true);
        if let (Some(connection), Ok(runtime)) = (self.connection.take(), tokio::runtime::Handle::try_current()) {
            runtime.spawn(async move {
                let _ = connection.close().await;
            });
        }
    }
}

struct Handler {
    gateway: std::sync::Weak<Gateway>,
    peer_id: u64,
    gathered_tx: mpsc::Sender<()>,
    /// weak: the connection holds this handler, and a strong state here made every channel the state keeps a cycle
    state: Weak<PeerState>,
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
        if let Some(peer) = self.state.upgrade() {
            *peer.connection_state.lock() = Some(state);
            if state == RTCPeerConnectionState::Connected {
                peer.ever_connected.store(true, Ordering::Relaxed);
            }
        }
        match state {
            RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed => {
                if let Some(gateway) = self.gateway.upgrade() {
                    gateway.drop_peer(self.peer_id, "disconnected");
                }
            }
            // ICE may sit in `disconnected` without ever reaching `failed`
            RTCPeerConnectionState::Disconnected => {
                let (gateway, state, peer_id) = (self.gateway.clone(), self.state.clone(), self.peer_id);
                tokio::spawn(async move {
                    tokio::time::sleep(DISCONNECTED_GRACE).await;
                    let still_down = state.upgrade().is_some_and(|state| *state.connection_state.lock() != Some(RTCPeerConnectionState::Connected));
                    if still_down && let Some(gateway) = gateway.upgrade() {
                        gateway.drop_peer(peer_id, "disconnected");
                    }
                });
            }
            _ => {}
        }
    }

    async fn on_data_channel(&self, dc: Arc<dyn DataChannel>) {
        // Must not block here: the driver waits for this to return.
        let closing = dc.clone();
        let Some(state) = self.state.upgrade().filter(|state| !state.gone.load(Ordering::Relaxed)) else {
            tokio::spawn(async move {
                let _ = closing.close().await;
            });
            return;
        };
        crate::spawn_isolated(format!("peer {}: channel", self.peer_id), run_channel(dc, self.gateway.clone(), state), async move {
            let _ = closing.close().await;
        });
    }
}

async fn run_channel(dc: Arc<dyn DataChannel>, gateway: std::sync::Weak<Gateway>, state: Arc<PeerState>) {
    let peer_id = state.peer_id;
    let raw_label = dc.label().await.unwrap_or_default();
    if raw_label == "control" {
        *state.control.lock() = Some(dc.clone());
        tokio::select! {
            () = run_control(dc, state.clone()) => {}
            () = state.wait_gone() => {}
        }
        // a dropped peer's control is drop_peer's to close with a "closed" event (gone is set before the signal)
        if !state.gone.load(Ordering::Relaxed) {
            *state.control.lock() = None;
        }
        // the client keeps `control` open for its whole life, so its close means the browser left
        if let Some(gateway) = gateway.upgrade() {
            gateway.drop_peer(peer_id, "disconnected");
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
        state.channels.lock().insert(entry_id, ChannelEntry { label: label.clone(), opts, stats });
    };
    let work = async { match label.kind.as_str() {
        "sub" => match state.access().check(crate::auth::Action::Subscribe, &label.key).and_then(|()| SubOpts::parse(&label.opts)).and_then(|mut opts| {
            let resolved = opts.resolve_encoding(&state.codecs)?;
            let track = bind_track(&state, &resolved, &label)?;
            Ok((opts, resolved, track))
        }) {
            Ok((opts, resolved, track)) => {
                let shared = Arc::new(SubShared::new(&opts, resolved, state.codecs.clone(), state.gate.clone()));
                register(opts.normalized(), ChannelStats::Sub(shared.clone()));
                state.leases.changed();
                state.send_accepted(label.id).await;
                subscription::run(dc.clone(), label.clone(), session, shared, track, state.grant.acl.is_some().then(|| Arc::new(state.grant.clone()))).await;
                None
            }
            Err(error) => Some(error),
        },
        "pub" => match state.access().check(crate::auth::Action::Publish, &label.key).and_then(|()| PubOpts::parse(&label.opts)) {
            Ok(opts) => {
                let shared = Arc::new(PubShared::default());
                register(json!({"delivery": opts.delivery, "priority": opts.priority, "latencyLimit": opts.latency_limit}), ChannelStats::Pub(shared.clone()));
                state.send_accepted(label.id).await;
                let (leases, key) = (state.leases.clone(), label.key.clone());
                let blocker = move || leases.blocker(peer_id, &key);
                publisher::run(dc.clone(), label.clone(), opts, session, shared, state.clock_offset_ms.clone(), blocker).await;
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
    } };
    // a dropped peer's channels never report their close: without this each waited forever, holding the peer's state
    let rejected = tokio::select! {
        rejected = work => rejected,
        () = state.wait_gone() => None,
    };
    if let Some(error) = rejected {
        warn!("peer {peer_id}: rejected channel {raw_label}: {error}");
        state.send_event(json!({"event": "rejected", "id": label.id, "reason": error})).await;
        // the client closes a rejected channel itself; closing it from both ends at once lets the second reset land on
        // the next channel that reuses its stream id (it then closes before opening)
        let client_closed = async {
            while let Some(event) = dc.poll().await {
                if matches!(event, DataChannelEvent::OnClose) {
                    break;
                }
            }
        };
        if tokio::time::timeout(Duration::from_secs(5), client_closed).await.is_err() {
            let _ = dc.close().await;
        }
    }
    if state.channels.lock().remove(&entry_id).is_some_and(|entry| matches!(entry.stats, ChannelStats::Sub(_))) {
        state.leases.changed();
    }
    // a publisher stream that goes away takes its deadman with it (the client cleared or closed it)
    if label.kind == "pub"
        && let Some(id) = label.id
    {
        state.deadmen.lock().remove(&id);
    }
}

/// The options a running subscription may change (the rest decide what it is, so they need a new subscription).
const UPDATABLE: [&str; 9] =
    ["maxHz", "minQuality", "qualityToHzTradeoff", "bandwidthPriority", "maxBitrate", "minResolutionScale", "maxResolution", "playoutDelay", "encodeOptions"];

/// Changes a running subscription's options in place: same channel, same track, the next frame uses them.
fn update_subscription(state: &PeerState, request: &ControlRequest) -> Value {
    let result = (|| -> Result<Value, String> {
        let sub_id = request.sub_id.ok_or("updateSubscription needs subId")?;
        let Some(Value::Object(changes)) = &request.opts else { return Err("updateSubscription needs opts (an object)".into()) };
        let mut channels = state.channels.lock();
        let entry = channels
            .values_mut()
            .find(|entry| entry.label.kind == "sub" && entry.label.id == Some(sub_id))
            .ok_or_else(|| format!("no subscription with id {sub_id}"))?;
        let ChannelStats::Sub(shared) = &entry.stats else { return Err(format!("no subscription with id {sub_id}")) };
        let mut merged = match &entry.label.opts {
            Value::Object(opts) => opts.clone(),
            _ => serde_json::Map::new(),
        };
        for (name, value) in changes {
            if !UPDATABLE.contains(&name.as_str()) {
                return Err(format!("{name} can't change on a running subscription (it may change: {})", UPDATABLE.join(", ")));
            }
            // encodeOptions: only its quality, the rest is how the encoding works
            if name == "encodeOptions" {
                let Value::Object(options) = value else { return Err("encodeOptions must be an object".into()) };
                if let Some(other) = options.keys().find(|option| *option != "quality") {
                    return Err(format!("encodeOptions.{other} can't change on a running subscription (encodeOptions.quality may)"));
                }
                let current = merged.entry("encodeOptions").or_insert_with(|| json!({}));
                if let (Value::Object(current), Some(quality)) = (current, options.get("quality")) {
                    current.insert("quality".into(), quality.clone());
                }
                continue;
            }
            // null clears an option back to its default
            if value.is_null() {
                merged.remove(name);
            } else {
                merged.insert(name.clone(), value.clone());
            }
        }
        let merged = Value::Object(merged);
        let mut opts = SubOpts::parse(&merged)?;
        opts.resolve_encoding(&state.codecs)?;
        shared.retune(&opts);
        entry.opts = opts.normalized();
        entry.label.opts = merged;
        Ok(json!({"opts": entry.opts}))
    })();
    // the allocator's next tick (every ALLOCATION_INTERVAL) picks up the new demand
    match result {
        Ok(extra) => ok(&request.id, extra),
        Err(error) => fail(&request.id, error),
    }
}

/// A video or audio channel's subscription claims the track the browser renegotiated for it (by mid).
fn bind_track(state: &PeerState, resolved: &Resolved, label: &Label) -> Result<Option<TrackClaim>, String> {
    let Some(mime) = media::track_mime(resolved.channel) else {
        return Ok(None);
    };
    let channel = resolved.channel.as_str();
    let mid = label.mid.as_deref().ok_or_else(|| format!("channel {channel}: the label needs the mid of a renegotiated transceiver"))?;
    let track = state.tracks.lock().get(mid).cloned().ok_or_else(|| format!("no track for mid {mid:?} (renegotiate with the channel first)"))?;
    if track.mime != mime {
        return Err(format!("track {mid:?} carries {}, channel {channel} needs {mime}", track.mime));
    }
    Ok(Some(track.claim()))
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
    let (video, data): (Vec<_>, Vec<_>) = demands.into_iter().enumerate().partition(|(index, _)| is_video[*index]);
    let (video_indices, video_demands): (Vec<usize>, Vec<_>) = video.into_iter().unzip();
    let (data_indices, data_demands): (Vec<usize>, Vec<_>) = data.into_iter().unzip();
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
    offset_ms: Option<f64>,
    rtt_ms: Option<f64>,
}

/// Heartbeat channel (unordered, no retransmits). Each beat is also a clock-sync ping:
/// `{t0, offsetMs?, rttMs?}` in, `{t0, t1, t2}` (gateway receive/send times) out.
async fn run_heartbeat(dc: Arc<dyn DataChannel>, opts: HeartbeatOpts, state: Arc<PeerState>) {
    let already_configured = {
        let mut heartbeat = state.heartbeat.lock();
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
                    state.heartbeat.lock().beats += 1;
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
                state.heartbeat.lock().deadlines_missed += 1;
                warn!("peer {}: heartbeat missed for {deadline:?}", state.peer_id);
                state.fire_deadmen("heartbeat").await;
                for group in state.leases.release_all(state.peer_id) {
                    state.lease_lost(&group, "heartbeat").await;
                }
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
    t0: Option<f64>,
    offset_ms: Option<f64>,
    rtt_ms: Option<f64>,
    pub_id: Option<u64>,
    probe_ms: Option<u64>,
    bytes: Option<String>,
    sdp: Option<RTCSessionDescription>,
    /// renegotiate: add a track for this channel (`video-h264`, `audio-opus`, ...)
    channel: Option<String>,
    /// lease ops: the group, its keys (a group the server doesn't define) and maxSeconds
    group: Option<String>,
    keys: Option<Vec<String>>,
    max_seconds: Option<f64>,
    /// updateSubscription: the subscription's channel id, and the options to change
    sub_id: Option<u64>,
    opts: Option<Value>,
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
        if let Ok(request) = serde_json::from_slice::<api::ApiRequest>(&message.data)
            && api::OPS.contains(&request.op.as_str())
        {
            let (dc, state) = (dc.clone(), state.clone());
            let slow = api::SLOW_OPS.contains(&request.op.as_str());
            let task = async move {
                let context = api::Context { session: &state.session, access: state.access(), api: &state.api, events: state.api_events.clone() };
                let response = api::handle(context, request).await;
                let _ = dc.send_text(&response.to_string()).await;
            };
            // ops that wait on the network don't hold up pings and stats
            if slow {
                tokio::spawn(task);
            } else {
                task.await;
            }
            continue;
        }
        let request: ControlRequest = match serde_json::from_slice(&message.data) {
            Ok(request) => request,
            Err(error) => {
                let _ = dc.send_text(&fail(&Value::Null, error).to_string()).await;
                continue;
            }
        };
        let response = match request.op.as_str() {
            // these can take seconds or wait on the peer connection's driver; keep stats and pings flowing meanwhile
            "listTopics" | "renegotiate" | "expireLease" => {
                let (dc, state) = (dc.clone(), state.clone());
                tokio::spawn(async move {
                    let response = match request.op.as_str() {
                        "listTopics" => handle_list_topics(&state.session, &request, &state.grant).await,
                        "expireLease" => expire_lease(&state, &request).await,
                        _ => handle_renegotiate(&state, &request).await,
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
                let clock = json!({"offsetMs": *state.clock_offset_ms.lock(), "rttMs": *state.rtt_ms.lock()});
                let heartbeat = serde_json::to_value(state.heartbeat.lock().clone()).unwrap_or_default();
                let bandwidth = serde_json::to_value(state.bandwidth.lock().clone()).unwrap_or_default();
                ok(&request.id, json!({"channels": collect_stats(&state), "clock": clock, "heartbeat": heartbeat, "bandwidth": bandwidth}))
            }
            "setDeadman" => set_deadman(&state, &request).await,
            "encodings" => {
                let encodings: Vec<Value> = state.codecs.list().map(|(name, output)| json!({"name": name, "output": output.as_str()})).collect();
                ok(&request.id, json!({"encodings": encodings}))
            }
            "lease" => take_lease(&state, &request),
            "updateSubscription" => update_subscription(&state, &request),
            "releaseLease" => {
                state.leases.end(request.group.as_deref().unwrap_or_default(), |holder, _| holder == state.peer_id);
                ok(&request.id, json!({}))
            }
            "clearDeadman" => {
                let pub_id = request.pub_id.unwrap_or_default();
                if let Some(deadman) = state.deadmen.lock().remove(&pub_id) {
                    deadman.shared.stats.lock().deadman_armed = false;
                }
                ok(&request.id, json!({}))
            }
            other => fail(&request.id, format!("unknown op {other}")),
        };
        let _ = dc.send_text(&response.to_string()).await;
    }
}

/// Browser-initiated renegotiation (it added a recvonly transceiver): answer, and with `channel` add a
/// track of that channel's format and report its mid.
async fn handle_renegotiate(state: &PeerState, request: &ControlRequest) -> Value {
    let Some(offer) = request.sdp.clone() else {
        return fail(&request.id, "renegotiate needs sdp");
    };
    let mime = match request.channel.as_deref().map(Channel::parse) {
        None => None,
        Some(Ok(channel)) => match media::track_mime(channel) {
            Some(mime) => Some(mime),
            None => return fail(&request.id, format!("{} has no track", channel.as_str())),
        },
        Some(Err(error)) => return fail(&request.id, error),
    };
    let Some(connection) = state.connection.lock().clone() else {
        return fail(&request.id, "peer is gone");
    };
    let _negotiating = state.negotiation.lock().await;
    match media::renegotiate(&connection, offer, mime).await {
        Ok((answer, track)) => {
            let mid = track.map(|track| {
                let mid = track.mid.clone();
                state.tracks.lock().insert(mid.clone(), track);
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

/// Takes (or renews) a lease; it ends at maxSeconds, when the heartbeat stops, or on disconnect.
fn take_lease(state: &Arc<PeerState>, request: &ControlRequest) -> Value {
    if !state.heartbeat.lock().configured {
        return fail(&request.id, "a lease needs a heartbeat: connect(url, { heartbeatHz, heartbeatMisses })");
    }
    let group = request.group.clone().unwrap_or_default();
    match state.leases.take(state.peer_id, &state.grant, &group, request.keys.clone(), request.max_seconds) {
        Ok((keys, expires_in, serial)) => {
            if let Some(after) = expires_in {
                let (gateway, group) = (state.gateway.clone(), group.clone());
                tokio::spawn(async move {
                    tokio::time::sleep(after).await;
                    if let Some(gateway) = gateway.upgrade()
                        && let Some(holder) = gateway.leases.end(&group, |_, lease_serial| lease_serial == serial)
                    {
                        gateway.lease_lost(holder, &group, "maxSeconds").await;
                    }
                });
            }
            info!("peer {}: leased {group:?} {keys:?}", state.peer_id);
            ok(&request.id, json!({"group": group, "keys": keys, "expiresInMs": expires_in.map(|after| after.as_millis() as u64)}))
        }
        Err(error) => fail(&request.id, error),
    }
}

/// Ends another client's lease, with the grant's forceExpire right.
async fn expire_lease(state: &PeerState, request: &ControlRequest) -> Value {
    let group = request.group.as_deref().unwrap_or_default();
    if !state.grant.force_expire {
        return fail(&request.id, format!("not authorized to force-expire lease {group:?}"));
    }
    let Some(gateway) = state.gateway.upgrade() else {
        return fail(&request.id, "shutting down");
    };
    match gateway.leases.end(group, |_, _| true) {
        Some(holder) => {
            gateway.lease_lost(holder, group, &format!("force-expired by peer {}", state.peer_id)).await;
            ok(&request.id, json!({}))
        }
        None => fail(&request.id, format!("no lease {group:?} is held")),
    }
}

async fn set_deadman(state: &PeerState, request: &ControlRequest) -> Value {
    let Some(pub_id) = request.pub_id else {
        return fail(&request.id, "setDeadman needs pubId");
    };
    if !state.heartbeat.lock().configured {
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
    let Some((key, shared)) = found else {
        return fail(&request.id, format!("no publisher {pub_id}"));
    };
    if shared.tripped.load(Ordering::Acquire) {
        return fail(&request.id, "publisher is tripped; create a new one");
    }
    shared.stats.lock().deadman_armed = true;
    state.deadmen.lock().insert(pub_id, Deadman { key, bytes, shared });
    ok(&request.id, json!({}))
}

/// Every key the gateway can find live under `filter`, with where it was seen:
/// - `advancedPublisher`: liveliness tokens of zenoh-ext AdvancedPublishers with publisher_detection
/// - `token`: any other liveliness token
/// - `sample`: data seen on a `filter` subscription during `probe` (catches undeclared publishers);
///   a zero `probe` skips that subscription
///
/// Plain publishers, subscribers and queryables without a token are invisible.
async fn list_topics(session: &zenoh::Session, filter: &str, probe: Duration) -> anyhow::Result<Vec<Value>> {
    let found: Arc<Mutex<BTreeMap<String, BTreeSet<&'static str>>>> = Arc::default();
    let note = |found: &Mutex<BTreeMap<String, BTreeSet<&'static str>>>, key: &str, source: &'static str| {
        found.lock().entry(key.to_owned()).or_default().insert(source);
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
        let Ok(replies) = session.liveliness().get(selector.as_str()).timeout(LIVELINESS_QUERY_TIMEOUT).await else {
            continue;
        };
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
    let found = found.lock();
    Ok(found.iter().map(|(key, sources)| json!({"key": key, "sources": sources})).collect())
}

async fn handle_list_topics(session: &zenoh::Session, request: &ControlRequest, grant: &Grant) -> Value {
    let filter = if request.key.is_empty() { "**" } else { &request.key };
    match list_topics(session, filter, Duration::from_millis(request.probe_ms.unwrap_or(DEFAULT_LIST_PROBE_MS).min(MAX_LIST_PROBE_MS))).await {
        Ok(mut topics) => {
            topics.retain(|topic| grant.allows(crate::auth::Action::List, topic["key"].as_str().unwrap_or_default()).is_ok());
            ok(&request.id, json!({"topics": topics}))
        }
        Err(error) => fail(&request.id, error),
    }
}

fn collect_stats(state: &PeerState) -> Vec<Value> {
    let channels = state.channels.lock();
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
        allocator::Demand { priority: 1.0, max_hz: hz, quality_range: None, tradeoff: 0.5, price: Box::new(move |_| bytes_per_message), fixed_bytes_per_sec: None, shrinks_last: false }
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
                            // what listTopics used to ask: the gateway's own declarations, from the admin space
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
