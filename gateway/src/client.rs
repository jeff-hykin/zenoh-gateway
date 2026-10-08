//! A Rust client for a zenoh-gateway server (cargo feature `client`). It connects the way the browser
//! client (`client/zenoh_gateway.ts`) does, so a program with no browser can subscribe, publish and
//! query through a gateway: e.g. a relay that takes one robot's best stream per camera and re-serves
//! it through its own [`Server`](crate::Server). Video and audio arrive encoded (H.264/VP8/VP9/AV1
//! access units, Opus packets); the client decodes nothing.
//!
//! Reconnecting is left to the caller: once [`Client::closed`] resolves, connect again and
//! subscribe again (armed deadmen have fired by then, see SPEC "Heartbeat and deadman").
//!
//! ```no_run
//! # async fn run() -> anyhow::Result<()> {
//! use zenoh_gateway::client::{Client, ClientOptions, Message, SubscribeOptions};
//! let client = Client::connect("http://robot.local:7448", ClientOptions::default()).await?;
//! let mut camera = client.subscribe("camera/front", SubscribeOptions { encoding: Some("ros2_image".into()), ..Default::default() }).await?;
//! while let Some(message) = camera.recv().await {
//!     if let Message::Video(frame) = message {
//!         println!("{:?} access unit, {} bytes, keyframe {}", frame.format, frame.data.len(), frame.keyframe);
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use crate::encoding::{Compress, VideoFormat};
use crate::fields::{self, Field};
use crate::{frame, media};

mod api;
use anyhow::{Context, Result, anyhow, bail, ensure};
pub use api::*;
use base64::Engine;
use bytes::BytesMut;
use log::warn;
use rtc::peer_connection::configuration::setting_engine::SctpMaxMessageSize;
use rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use rtc::rtp::Packet;
use rtc::rtp::codec::{av1::Av1Depacketizer, h264::H264Packet, vp8::Vp8Packet, vp9::Vp9Packet};
use rtc::rtp::packetizer::Depacketizer;
use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;
use rtc::rtp_transceiver::{RTCRtpTransceiverDirection, RTCRtpTransceiverInit};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use webrtc::data_channel::{DataChannel, DataChannelEvent, RTCDataChannelInit};
use webrtc::media_stream::track_remote::{TrackRemote, TrackRemoteEvent};
use webrtc::peer_connection::{
    PeerConnection, PeerConnectionBuilder, PeerConnectionEventHandler, RTCConfigurationBuilder, RTCIceGatheringState, RTCIceServer,
    RTCIceTransportPolicy, RTCPeerConnectionState, RTCSessionDescription, SettingEngineBuilder,
};
use webrtc::rtp_transceiver::RtpTransceiver;

pub use crate::IceServer;

const GATHER_TIMEOUT: Duration = Duration::from_secs(3);
const OPEN_TIMEOUT: Duration = Duration::from_secs(10);
const PING_TIMEOUT: Duration = Duration::from_secs(3);
const STATS_INTERVAL: Duration = Duration::from_secs(1);
const INITIAL_CLOCK_PINGS: usize = 5;
const CLOCK_WINDOW: usize = 16;
/// the gateway's limit too (peer.rs)
const MAX_MESSAGE_SIZE: u32 = 256 * 1024;
/// consumption acks: at least this often in bytes, else after `ACK_DELAY`
const ACK_EVERY_BYTES: usize = 16 * 1024;
const ACK_DELAY: Duration = Duration::from_millis(5);
/// incomplete chunked messages (data) or access units (video) kept before the oldest is dropped
const MAX_PARTIALS: usize = 8;
/// a `latest` publisher drops puts while this much is unsent, as the browser client does
const BACKED_UP_BYTES: usize = 64 * 1024;
/// a stream that lost a frame asks for a keyframe at most this often
const PLI_INTERVAL: Duration = Duration::from_millis(500);
const QUEUE: usize = 64;

/// Connection settings.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// sent as `Authorization: Bearer <token>` on `POST /offer`, for a server with an authorize hook
    pub token: Option<String>,
    /// STUN/TURN servers; None (default) asks the gateway (`GET /zenoh-gateway/ice`, TURN credentials minted for this client)
    pub ice_servers: Option<Vec<IceServer>>,
    /// send everything through TURN (ICE transport policy "relay"); also on when the gateway's ICE reply says
    /// `"iceTransportPolicy": "relay"`
    pub relay_only: bool,
    /// heartbeats per second, 0 (default) for none; deadmen need them
    pub heartbeat_hz: f64,
    /// silence of `heartbeat_misses / heartbeat_hz` seconds fires this client's deadmen (default 3)
    pub heartbeat_misses: u32,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions { token: None, ice_servers: None, relay_only: false, heartbeat_hz: 0.0, heartbeat_misses: 3 }
    }
}

/// `delivery` of a subscription or publisher (SPEC "Delivery → transport mapping").
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Delivery {
    /// unordered, no retransmits; only the newest sample per key waits
    #[default]
    Latest,
    /// ordered and reliable; every sample is queued
    Reliable,
}

/// The browser client's subscribe options; unset ones take the gateway's defaults, and the gateway
/// checks them (a bad one makes [`Client::subscribe`] fail with its reason).
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeOptions {
    /// default latest
    pub delivery: Option<Delivery>,
    /// zenoh priority 1..7 (default: each sample's own)
    pub priority: Option<u8>,
    /// ms; drop anything older
    pub max_age: Option<f64>,
    /// the gateway never sends a key faster than this
    pub max_hz: Option<f64>,
    /// when bandwidth is short, higher keeps more (default 1; 0 gives up everything first)
    pub bandwidth_priority: Option<f64>,
    /// 0..1, encoded streams only
    pub min_quality: Option<f64>,
    /// 0 keeps quality and drops Hz, 1 keeps Hz and drops quality (default 0.5)
    pub quality_to_hz_tradeoff: Option<f64>,
    /// a message encoding the gateway registered ([`Client::encodings`])
    pub encoding: Option<String>,
    /// `video-h264`, `video-vp8`, `video-vp9`, `video-av1`, `audio-opus` or `data` (default: where the encoding's
    /// output goes)
    pub channel: Option<String>,
    /// passed to the encoding; `quality` there (0..1) is the most the allocator may pick
    pub encode_options: Option<serde_json::Map<String, Value>>,
    /// data-channel compression (default: the encoding's); not for video or audio
    pub compress: Option<Compress>,
    /// video channels: most bits/s the stream asks for (default: the server's)
    pub max_bitrate: Option<f64>,
    /// video channels: smallest share of the source's width and height the picture may shrink to (default 0.25)
    pub min_resolution_scale: Option<f64>,
    /// video channels: (width, height) box the picture is fitted into
    pub max_resolution: Option<(u32, u32)>,
    /// video channels: (min, max) ms the browser may hold a frame to smooth out jitter (default (0, 0): show at once)
    pub playout_delay: Option<(f64, f64)>,
}

/// Publisher options.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PublisherOptions {
    /// default latest
    pub delivery: Option<Delivery>,
    /// zenoh priority 1..7
    pub priority: Option<u8>,
    /// ms; the gateway drops puts older than this (clock-corrected)
    pub latency_limit: Option<f64>,
    /// re-send the last value this often (client side)
    #[serde(skip)]
    pub repeat_ms: Option<u64>,
}

/// One thing a subscription received.
#[derive(Debug, Clone)]
pub enum Message {
    /// A raw sample, or a data-channel encoding's output.
    Data(DataMessage),
    /// A video codec's access unit, from the subscription's RTP track.
    Video(VideoFrame),
    /// A video codec's per-frame metadata, from the subscription's data channel (it travels apart
    /// from the frame, so it can arrive just before or after its [`Message::Video`]).
    VideoInfo(VideoFrameInfo),
    /// An audio codec's Opus packet (20 ms), from the subscription's RTP track.
    Audio(AudioPacket),
}

/// A data-channel message, decompressed.
#[derive(Debug, Clone)]
pub struct DataMessage {
    /// the sample's key
    pub key: String,
    /// the payload: raw sample bytes, or the encoding's output
    pub bytes: Vec<u8>,
    /// when the gateway received the sample (unix ms, gateway clock)
    pub timestamp_ms: f64,
    /// numbers messages per subscription (gaps are drops)
    pub seq: u32,
    /// fields output: the parsed fields (see [`crate::fields`])
    pub fields: Option<BTreeMap<String, Field>>,
    /// a delete sample (no payload), not a put
    pub delete: bool,
    /// the sample's encoding, when not the default (raw messages only)
    pub encoding: Option<String>,
    /// the sample's attachment (raw messages only)
    pub attachment: Option<Vec<u8>>,
}

/// An encoded video frame as the gateway's encoder produced it.
#[derive(Debug, Clone)]
pub struct VideoFrame {
    /// the negotiated codec
    pub format: VideoFormat,
    /// H.264: an Annex B access unit; VP8/VP9: a frame; AV1: a temporal unit's OBUs
    pub data: Vec<u8>,
    /// decodable on its own (H.264 IDR, VP8/VP9 key frame, AV1 sequence header)
    pub keyframe: bool,
    /// 90 kHz RTP timestamp
    pub rtp_timestamp: u32,
    /// when its last packet arrived
    pub received_at: Instant,
}

/// The 28-byte metadata frame the gateway sends per video frame (SPEC "Wire format").
#[derive(Debug, Clone)]
pub struct VideoFrameInfo {
    /// the sample's key
    pub key: String,
    /// when the gateway received the sample (unix ms, gateway clock)
    pub timestamp_ms: f64,
    /// numbers messages per subscription
    pub seq: u32,
    /// a keyframe
    pub keyframe: bool,
    /// encoded size
    pub width: u32,
    /// encoded size
    pub height: u32,
    /// the source picture's size
    pub source_width: u32,
    /// the source picture's size
    pub source_height: u32,
    /// the quality it was encoded at, 0..1
    pub quality: f32,
    /// the encoded frame's bytes
    pub encoded_bytes: u32,
}

/// One Opus packet.
#[derive(Debug, Clone)]
pub struct AudioPacket {
    /// the Opus packet
    pub data: Vec<u8>,
    /// 48 kHz RTP timestamp
    pub rtp_timestamp: u32,
}

/// A message encoding the gateway registered.
#[derive(Debug, Clone, Deserialize)]
pub struct EncodingInfo {
    /// what `encoding` names
    pub name: String,
    /// on its default channel: `"video"`, `"audio"`, `"fields"` or `"data"`
    pub output: String,
}

/// A key [`Client::list_topics`] found, with where it was seen (`token`, `advancedPublisher`, `sample`).
#[derive(Debug, Clone, Deserialize)]
pub struct Topic {
    /// the key
    pub key: String,
    /// how the gateway saw it (SPEC "Topic enumeration")
    pub sources: Vec<String>,
}

/// One reply to [`Client::get`] / [`Client::get_with`].
#[derive(Debug, Clone)]
pub struct GetReply {
    /// the replying key (None for an error reply)
    pub key: Option<String>,
    /// the payload, or the error's
    pub bytes: Vec<u8>,
    /// an error reply
    pub error: bool,
    /// the sample's (or the error's) encoding
    pub encoding: Option<String>,
    /// the sample's attachment
    pub attachment: Option<Vec<u8>>,
    /// a delete reply (`reply_del`)
    pub delete: bool,
    /// the sample's timestamp, unix ms
    pub timestamp_ms: Option<f64>,
}

/// The peer connection's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// signalling
    Connecting,
    /// up
    Connected,
    /// ICE disconnected or a ping failed; may recover
    Degraded,
    /// closed or failed for good; reconnecting is the caller's job
    Lost,
}

fn now_ms() -> f64 {
    crate::subscription::now_unix_ms()
}

/// NTP-style samples; the offset of the lowest-RTT recent one (SPEC "Clock sync").
#[derive(Default)]
struct Clock {
    samples: VecDeque<(f64, f64)>,
    offset_ms: Option<f64>,
    rtt_ms: Option<f64>,
}

impl Clock {
    fn add(&mut self, t0: f64, t1: f64, t2: f64, t3: f64) {
        let (rtt, offset) = ((t3 - t0) - (t2 - t1), ((t1 - t0) + (t2 - t3)) / 2.0);
        if !rtt.is_finite() || !offset.is_finite() {
            return;
        }
        self.samples.push_back((offset, rtt));
        if self.samples.len() > CLOCK_WINDOW {
            self.samples.pop_front();
        }
        self.offset_ms = self.samples.iter().min_by(|a, b| a.1.total_cmp(&b.1)).map(|sample| sample.0);
        self.rtt_ms = Some(rtt);
    }
}

/// Why something ended (a publisher's deadman, a lease), once; the first reason sticks.
struct Ending(watch::Sender<Option<String>>);

impl Ending {
    fn new() -> Self {
        Ending(watch::channel(None).0)
    }

    fn end(&self, reason: String) {
        self.0.send_if_modified(|current| current.is_none() && current.replace(reason).is_none());
    }

    fn reason(&self) -> Option<String> {
        self.0.borrow().clone()
    }

    async fn wait(&self) -> String {
        let mut ended = self.0.subscribe();
        ended.wait_for(Option::is_some).await.map(|reason| reason.clone().unwrap_or_default()).unwrap_or_default()
    }
}

/// A publisher's state, shared with the control reader and its channel task.
struct PubState {
    tripped: Ending,
    deadman_armed: AtomicBool,
    last: Mutex<Option<Vec<u8>>>,
    /// why the gateway drops this publisher's puts (another client's lease)
    blocked: Mutex<Option<String>>,
}

impl PubState {
    fn trip(&self, reason: String) {
        self.deadman_armed.store(false, Ordering::Release);
        self.tripped.end(reason);
    }
}

#[derive(Default)]
struct Endpoint {
    accepted: Option<oneshot::Sender<Result<(), String>>>,
    publisher: Option<Arc<PubState>>,
}

/// A recvonly transceiver and the gateway track bound to it; reused by later subscriptions of its channel.
struct MediaSlot {
    mid: String,
    codec: String,
    transceiver: Arc<dyn RtpTransceiver>,
    track: OnceLock<Arc<dyn TrackRemote>>,
    ssrc: AtomicU32,
    sink: Mutex<Option<mpsc::Sender<Packet>>>,
}

impl MediaSlot {
    async fn request_keyframe(&self) -> Result<()> {
        let track = self.track.get().context("no video has arrived yet")?;
        let pli = PictureLossIndication { sender_ssrc: 0, media_ssrc: self.ssrc.load(Ordering::Relaxed) };
        track.write_rtcp(vec![Box::new(pli)]).await.map_err(|error| anyhow!("{error}"))
    }
}

/// What the peer connection's handler and the channel tasks share.
struct Shared {
    requests: Mutex<HashMap<u64, oneshot::Sender<Value>>>,
    endpoints: Mutex<HashMap<u64, Endpoint>>,
    clock: Mutex<Clock>,
    state: watch::Sender<ConnectionState>,
    gathered: Notify,
    media: Mutex<Vec<Arc<MediaSlot>>>,
    leases: Mutex<HashMap<String, Arc<Ending>>>,
    /// lease requests awaiting their reply (request id -> group, lease), held as the reply is read so a `leaseLost` right behind it finds them
    pending_leases: Mutex<HashMap<u64, (String, Arc<Ending>)>>,
    /// queryables, liveliness subscribers and matching listeners: (event, its id) -> where its events go
    api_routes: Mutex<HashMap<(&'static str, u64), mpsc::UnboundedSender<Value>>>,
    /// events that arrived before the reply naming their handle (a liveliness history can), held for it
    api_early: Mutex<HashMap<(&'static str, u64), Vec<Value>>>,
}

impl Shared {
    fn set_state(&self, state: ConnectionState) {
        self.state.send_if_modified(|current| {
            let changed = *current != state && *current != ConnectionState::Lost;
            if changed {
                *current = state;
            }
            changed
        });
    }

    /// Gone for good: pending requests fail, and publishers with an armed deadman trip ("disconnected"),
    /// as the gateway fires their deadmen when it loses us.
    fn lost(&self) {
        if *self.state.borrow() == ConnectionState::Lost {
            return;
        }
        self.set_state(ConnectionState::Lost);
        self.requests.lock().unwrap().clear();
        for endpoint in self.endpoints.lock().unwrap().values() {
            if let Some(publisher) = endpoint.publisher.as_ref().filter(|publisher| publisher.deadman_armed.load(Ordering::Acquire)) {
                publisher.trip("disconnected".into());
            }
        }
        for (_, lease) in self.leases.lock().unwrap().drain() {
            lease.end("disconnected".into());
        }
    }

    fn on_control_message(&self, text: &str) {
        let Ok(message) = serde_json::from_str::<Value>(text) else {
            return;
        };
        let id = message["id"].as_u64().unwrap_or_default();
        let Some(event) = message["event"].as_str() else {
            if let Some((group, ending)) = self.pending_leases.lock().unwrap().remove(&id).filter(|_| message["ok"] == true)
                && let Some(previous) = self.leases.lock().unwrap().insert(group, ending)
            {
                previous.end("renewed".into());
            }
            if let Some(reply) = self.requests.lock().unwrap().remove(&id) {
                let _ = reply.send(message);
            }
            return;
        };
        if let Some((event, id_field)) = api::EVENT_IDS.iter().find(|(name, _)| *name == event) {
            let id = message[*id_field].as_u64().unwrap_or_default();
            let routes = self.api_routes.lock().unwrap();
            match routes.get(&(*event, id)) {
                Some(route) => {
                    let _ = route.send(message);
                }
                None => {
                    let mut early = self.api_early.lock().unwrap();
                    // ids are never reused, so a stray event for a handle long gone only costs memory: cap it
                    if early.len() < 1000 {
                        early.entry((*event, id)).or_default().push(message);
                    }
                }
            }
            return;
        }
        let reason = message["reason"].as_str().unwrap_or_default().to_owned();
        match event {
            // revoked
            "closed" => return self.lost(),
            "leaseLost" => {
                if let Some(lease) = self.leases.lock().unwrap().remove(message["group"].as_str().unwrap_or_default()) {
                    lease.end(reason);
                }
                return;
            }
            _ => {}
        }
        let mut endpoints = self.endpoints.lock().unwrap();
        let Some(endpoint) = endpoints.get_mut(&id) else {
            return;
        };
        match event {
            "accepted" | "rejected" => {
                if let Some(accepted) = endpoint.accepted.take() {
                    let _ = accepted.send(if event == "accepted" { Ok(()) } else { Err(reason) });
                }
            }
            "tripped" => {
                if let Some(publisher) = &endpoint.publisher {
                    publisher.trip(reason);
                }
            }
            _ => {}
        }
    }
}

struct Handler {
    shared: Arc<Shared>,
}

#[async_trait::async_trait]
impl PeerConnectionEventHandler for Handler {
    async fn on_ice_gathering_state_change(&self, state: RTCIceGatheringState) {
        if state == RTCIceGatheringState::Complete {
            self.shared.gathered.notify_one();
        }
    }

    async fn on_connection_state_change(&self, state: RTCPeerConnectionState) {
        match state {
            RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed => self.shared.lost(),
            RTCPeerConnectionState::Disconnected => self.shared.set_state(ConnectionState::Degraded),
            RTCPeerConnectionState::Connected => self.shared.set_state(ConnectionState::Connected),
            _ => {}
        }
    }

    async fn on_track(&self, track: Arc<dyn TrackRemote>) {
        // the driver waits for this to return, and finding the transceiver asks the driver
        tokio::spawn(route_track(self.shared.clone(), track));
    }
}

/// Finds the slot whose transceiver received `track` and forwards its RTP to that slot's subscription.
async fn route_track(shared: Arc<Shared>, track: Arc<dyn TrackRemote>) {
    let same = |a: &Arc<dyn TrackRemote>| std::ptr::addr_eq(Arc::as_ptr(a), Arc::as_ptr(&track));
    let mut found = None;
    for _ in 0..100 {
        let slots = shared.media.lock().unwrap().clone();
        for slot in slots {
            if let Ok(Some(receiver)) = slot.transceiver.receiver().await
                && same(receiver.track())
            {
                found = Some(slot);
            }
        }
        if found.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let Some(slot) = found else {
        return warn!("zenoh-gateway client: a track arrived on no known transceiver");
    };
    let _ = slot.track.set(track.clone());
    while let Some(event) = track.poll().await {
        if let TrackRemoteEvent::OnRtpPacket(packet) = event {
            slot.ssrc.store(packet.header.ssrc, Ordering::Relaxed);
            let sink = slot.sink.lock().unwrap().clone();
            // a consumer that falls behind loses packets; its stream then waits for a keyframe
            if let Some(sink) = sink {
                let _ = sink.try_send(packet);
            }
        }
    }
}

struct Inner {
    connection: Arc<dyn PeerConnection>,
    control: Arc<dyn DataChannel>,
    shared: Arc<Shared>,
    options: ClientOptions,
    encodings: Vec<EncodingInfo>,
    next_id: AtomicU64,
    negotiation: tokio::sync::Mutex<()>,
    free_slots: Mutex<Vec<Arc<MediaSlot>>>,
    heartbeat_paused: Arc<AtomicBool>,
}

impl Inner {
    async fn request(&self, body: Value, timeout: Duration) -> Result<Value> {
        self.request_as(self.next_id.fetch_add(1, Ordering::Relaxed), body, timeout).await
    }

    /// [`Inner::request`] with the caller's id (taken from `next_id`).
    async fn request_as(&self, id: u64, mut body: Value, timeout: Duration) -> Result<Value> {
        body["id"] = json!(id);
        let op = body["op"].as_str().unwrap_or_default().to_owned();
        let (reply_tx, reply_rx) = oneshot::channel();
        self.shared.requests.lock().unwrap().insert(id, reply_tx);
        self.control.send_text(&body.to_string()).await.map_err(|error| anyhow!("{op}: {error}"))?;
        let reply = tokio::time::timeout(timeout, reply_rx).await;
        self.shared.requests.lock().unwrap().remove(&id);
        let reply = reply.map_err(|_| anyhow!("{op} timed out"))?.map_err(|_| anyhow!("{op}: connection lost"))?;
        ensure!(reply["ok"] == true, "zenoh-gateway: {}", reply["error"].as_str().unwrap_or("error"));
        Ok(reply)
    }

    /// Clock-sync ping over `control`, which also reports our estimate to the gateway.
    async fn ping(&self) -> Result<()> {
        let (offset, rtt) = {
            let clock = self.shared.clock.lock().unwrap();
            (clock.offset_ms, clock.rtt_ms)
        };
        let t0 = now_ms();
        let reply = self.request(json!({"op": "ping", "t0": t0, "offsetMs": offset, "rttMs": rtt}), PING_TIMEOUT).await?;
        self.shared.clock.lock().unwrap().add(t0, reply["t1"].as_f64().unwrap_or(f64::NAN), reply["t2"].as_f64().unwrap_or(f64::NAN), now_ms());
        Ok(())
    }

    /// A recvonly transceiver bound to a gateway track of `codec` (a channel name): a free one, or a new one
    /// renegotiated over `control` (SPEC "Video").
    async fn media_slot(&self, codec: &str, kind: RtpCodecKind, reuse: bool) -> Result<Arc<MediaSlot>> {
        let free = reuse.then(|| {
            let mut free = self.free_slots.lock().unwrap();
            free.iter().position(|slot| slot.codec == codec).map(|index| free.remove(index))
        });
        if let Some(slot) = free.flatten() {
            return Ok(slot);
        }
        let _negotiating = self.negotiation.lock().await;
        let init = RTCRtpTransceiverInit { direction: RTCRtpTransceiverDirection::Recvonly, streams: vec![], send_encodings: vec![] };
        let transceiver = self.connection.add_transceiver_from_kind(kind, Some(init)).await?;
        let offer = self.connection.create_offer(None).await?;
        self.connection.set_local_description(offer).await?;
        let offer = self.connection.local_description().await.context("no local description")?;
        let reply = self.request(json!({"op": "renegotiate", "channel": codec, "sdp": offer}), OPEN_TIMEOUT).await?;
        self.connection.set_remote_description(serde_json::from_value(reply["sdp"].clone())?).await?;
        let mid = transceiver.mid().await?.context("the transceiver has no mid")?;
        ensure!(reply["mid"] == mid.as_str(), "gateway bound mid {}, expected {mid}", reply["mid"]);
        let slot = Arc::new(MediaSlot { mid, codec: codec.to_owned(), transceiver, track: OnceLock::new(), ssrc: AtomicU32::new(0), sink: Mutex::new(None) });
        self.shared.media.lock().unwrap().push(slot.clone());
        Ok(slot)
    }

    /// Opens a `sub`/`pub` channel, runs `task` on its events, and waits until the gateway accepted
    /// it and it is open here (the gateway's `accepted` can beat the channel's own open).
    async fn open_endpoint<F>(&self, label: Value, init: RTCDataChannelInit, publisher: Option<Arc<PubState>>, task: impl FnOnce(Arc<dyn DataChannel>, oneshot::Sender<()>) -> F) -> Result<Arc<dyn DataChannel>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (id, key) = (label["id"].as_u64().unwrap_or_default(), label["key"].as_str().unwrap_or_default().to_owned());
        let (accepted_tx, accepted_rx) = oneshot::channel();
        self.shared.endpoints.lock().unwrap().insert(id, Endpoint { accepted: Some(accepted_tx), publisher });
        let opened = async {
            let channel = self.connection.create_data_channel(&label.to_string(), Some(init)).await.map_err(|error| anyhow!("creating the channel: {error}"))?;
            let (opened_tx, opened_rx) = oneshot::channel();
            tokio::spawn(task(channel.clone(), opened_tx));
            let ready = async {
                match accepted_rx.await {
                    Ok(Ok(())) => {}
                    Ok(Err(reason)) => bail!("gateway rejected {key}: {reason}"),
                    Err(_) => bail!("connection lost"),
                }
                opened_rx.await.map_err(|_| anyhow!("{key}: channel closed before opening"))
            };
            match tokio::time::timeout(OPEN_TIMEOUT, ready).await {
                Ok(Ok(())) => Ok(channel),
                failed => {
                    let _ = channel.close().await;
                    Err(failed.unwrap_or_else(|_| Err(anyhow!("{key}: opening timed out"))).unwrap_err())
                }
            }
        };
        let result = opened.await;
        if result.is_err() {
            self.shared.endpoints.lock().unwrap().remove(&id);
        }
        result
    }

    fn release_slot(&self, slot: Arc<MediaSlot>) {
        slot.sink.lock().unwrap().take();
        if *self.shared.state.borrow() != ConnectionState::Lost {
            self.free_slots.lock().unwrap().push(slot);
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // a connection dropped unclosed keeps its driver task running
        let connection = self.connection.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move { connection.close().await });
        }
    }
}

/// A connection to a zenoh-gateway server. Clones share it; [`close`](Self::close) ends it for all.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

impl Client {
    /// Signals like the browser (non-trickle `POST <url>/offer`), opens `control` (and the heartbeat
    /// channel), fetches the encodings and takes a few clock samples before returning.
    pub async fn connect(url: &str, options: ClientOptions) -> Result<Client> {
        let signalling = Signalling::Http { url: url.trim_end_matches('/').to_owned(), http: reqwest::Client::new() };
        Self::connect_with(signalling, options).await
    }

    /// [`connect`](Self::connect), signalling over zenoh instead of HTTP: queries `zenoh-gateway/<name>/offer` (and `/ice`)
    /// through `session`, which must reach a server built with `ServerBuilder::zenoh_signalling(name)` (e.g. a robot
    /// whose zenoh dialled out to this side). The media then flows over WebRTC as usual (SPEC "Signalling over zenoh").
    pub async fn connect_zenoh(session: &zenoh::Session, name: &str, options: ClientOptions) -> Result<Client> {
        Self::connect_with(Signalling::Zenoh { session: session.clone(), name: name.to_owned() }, options).await
    }

    async fn connect_with(signalling: Signalling, options: ClientOptions) -> Result<Client> {
        let (state, _) = watch::channel(ConnectionState::Connecting);
        let shared = Arc::new(Shared { requests: Mutex::default(), endpoints: Mutex::default(), clock: Mutex::default(), state, gathered: Notify::new(), media: Mutex::default(), leases: Mutex::default(), pending_leases: Mutex::default(), api_routes: Mutex::default(), api_early: Mutex::default() });
        let (ice_servers, server_relay_only) = match &options.ice_servers {
            Some(servers) => (servers.clone(), false),
            None => signalling.ice_servers(options.token.as_deref()).await?,
        };
        let ice_servers = ice_servers.into_iter().map(|server| RTCIceServer { urls: server.urls, username: server.username, credential: server.credential }).collect();
        let policy = if options.relay_only || server_relay_only { RTCIceTransportPolicy::Relay } else { RTCIceTransportPolicy::All };
        let (media_engine, interceptors, _) = media::media_setup()?;
        let connection: Arc<dyn PeerConnection> = Arc::new(
            PeerConnectionBuilder::new()
                .with_configuration(RTCConfigurationBuilder::new().with_ice_servers(ice_servers).with_ice_transport_policy(policy).build())
                .with_setting_engine(SettingEngineBuilder::new().with_sctp_max_message_size(SctpMaxMessageSize::Bounded(MAX_MESSAGE_SIZE)).build())
                .with_media_engine(media_engine)
                .with_interceptor_registry(interceptors)
                .with_handler(Arc::new(Handler { shared: shared.clone() }))
                .with_udp_addrs(vec!["0.0.0.0:0".to_owned(), "127.0.0.1:0".to_owned()])
                .build()
                .await?,
        );
        let connected = async {
            let control = connection.create_data_channel("control", Some(channel_init(Delivery::Reliable, None))).await?;
            let (control_open_tx, control_open_rx) = oneshot::channel();
            tokio::spawn(run_control(control.clone(), shared.clone(), control_open_tx));
            let heartbeat_paused = Arc::new(AtomicBool::new(false));
            if options.heartbeat_hz > 0.0 {
                let label = json!({"type": "heartbeat", "opts": {"hz": options.heartbeat_hz, "misses": options.heartbeat_misses}});
                let channel = connection.create_data_channel(&label.to_string(), Some(channel_init(Delivery::Latest, None))).await?;
                tokio::spawn(run_heartbeat(channel, shared.clone(), heartbeat_paused.clone(), options.heartbeat_hz));
            }
            let offer = connection.create_offer(None).await?;
            connection.set_local_description(offer).await?;
            let _ = tokio::time::timeout(GATHER_TIMEOUT, shared.gathered.notified()).await;
            let offer = connection.local_description().await.context("no local description")?;
            connection.set_remote_description(signalling.offer(options.token.as_deref(), &offer).await?).await?;
            tokio::time::timeout(OPEN_TIMEOUT, control_open_rx).await.map_err(|_| anyhow!("control channel open timed out"))?.map_err(|_| anyhow!("control channel closed before opening"))?;
            let mut inner = Inner {
                connection: connection.clone(),
                control,
                shared: shared.clone(),
                options: options.clone(),
                encodings: Vec::new(),
                next_id: AtomicU64::new(1),
                negotiation: tokio::sync::Mutex::new(()),
                free_slots: Mutex::default(),
                heartbeat_paused,
            };
            inner.encodings = serde_json::from_value(inner.request(json!({"op": "encodings"}), PING_TIMEOUT).await?["encodings"].take())?;
            // the gateway needs a clock offset before the first put
            for _ in 0..INITIAL_CLOCK_PINGS {
                inner.ping().await?;
            }
            Ok(Arc::new(inner))
        };
        let inner = match connected.await {
            Ok(inner) => inner,
            Err(error) => {
                let _ = connection.close().await;
                return Err(error);
            }
        };
        shared.set_state(ConnectionState::Connected);
        tokio::spawn(run_pinger(Arc::downgrade(&inner)));
        Ok(Client { inner })
    }

    /// The message encodings the gateway runs.
    pub fn encodings(&self) -> &[EncodingInfo] {
        &self.inner.encodings
    }

    /// The connection's state now.
    pub fn state(&self) -> ConnectionState {
        *self.inner.shared.state.borrow()
    }

    /// Resolves once the connection is [`ConnectionState::Lost`] (reconnecting is the caller's job).
    pub async fn closed(&self) {
        let _ = self.inner.shared.state.subscribe().wait_for(|state| *state == ConnectionState::Lost).await;
    }

    /// Gateway clock minus this client's (unix ms), from the lowest-RTT recent sample.
    pub fn clock_offset_ms(&self) -> Option<f64> {
        self.inner.shared.clock.lock().unwrap().offset_ms
    }

    /// The latest round trip to the gateway.
    pub fn rtt_ms(&self) -> Option<f64> {
        self.inner.shared.clock.lock().unwrap().rtt_ms
    }

    /// Keys live under `filter` (SPEC "Topic enumeration"); `probe_ms` (default 600) also catches
    /// publishers that put meanwhile, 0 lists liveliness tokens only.
    pub async fn list_topics(&self, filter: &str, probe_ms: Option<u64>) -> Result<Vec<Topic>> {
        let probe_ms = probe_ms.unwrap_or(600);
        let mut reply = self.inner.request(json!({"op": "listTopics", "key": filter, "probeMs": probe_ms}), Duration::from_millis(probe_ms + 5000)).await?;
        Ok(serde_json::from_value(reply["topics"].take())?)
    }

    /// A zenoh query through the gateway (see [`Client::get_with`] for options).
    pub async fn get(&self, key: &str, timeout: Duration) -> Result<Vec<GetReply>> {
        self.get_with(key, GetOptions { timeout: Some(timeout), ..Default::default() }).await
    }

    /// The gateway's stats for this connection (`channels`, `clock`, `heartbeat`, `bandwidth`; SPEC "Bandwidth allocation").
    pub async fn stats(&self) -> Result<Value> {
        self.inner.request(json!({"op": "stats"}), PING_TIMEOUT).await
    }

    /// Stops (or resumes) sending heartbeats, so the gateway fires this client's deadmen; for testing deadman wiring.
    pub fn pause_heartbeat(&self, paused: bool) {
        self.inner.heartbeat_paused.store(paused, Ordering::Relaxed);
    }

    /// Subscribes to `key`, returning once the gateway accepted the channel (or with its reason for
    /// refusing). Video and audio channels first renegotiate a track (or reuse a closed subscription's).
    pub async fn subscribe(&self, key: &str, options: SubscribeOptions) -> Result<Subscription> {
        let inner = &self.inner;
        let output = options.encoding.as_ref().map(|name| inner.encodings.iter().find(|info| &info.name == name).map_or("data", |info| info.output.as_str()));
        let channel = options.channel.clone().unwrap_or_else(|| match output {
            Some("video") => "video-h264".to_owned(),
            Some("audio") => "audio-opus".to_owned(),
            _ => "data".to_owned(),
        });
        let media_kind = if channel.starts_with("video-") {
            Some(RtpCodecKind::Video)
        } else if channel.starts_with("audio-") {
            Some(RtpCodecKind::Audio)
        } else {
            None
        };
        let kind = media_kind.map(|kind| if kind == RtpCodecKind::Video { "video" } else { "audio" }.to_owned());
        // a reused track can still be held by the gateway for a moment after its last subscription closed
        for reuse in [true, false] {
            let slot = match media_kind {
                Some(media_kind) => Some(inner.media_slot(&channel, media_kind, reuse).await?),
                None => None,
            };
            match self.subscribe_on(key, &options, kind.clone(), slot).await {
                Err(error) if reuse && media_kind.is_some() && error.to_string().contains("in use") => {
                    continue;
                }
                result => return result,
            }
        }
        unreachable!("the second attempt returns")
    }

    async fn subscribe_on(&self, key: &str, options: &SubscribeOptions, kind: Option<String>, slot: Option<Arc<MediaSlot>>) -> Result<Subscription> {
        let inner = &self.inner;
        let id = inner.next_id.fetch_add(1, Ordering::Relaxed);
        let mut label = json!({"type": "sub", "key": key, "id": id, "opts": options_json(options)});
        let (messages_tx, messages) = mpsc::channel(QUEUE);
        let mut packets = None;
        if let Some(slot) = &slot {
            label["mid"] = json!(slot.mid);
            let (packets_tx, packets_rx) = mpsc::channel(1024);
            *slot.sink.lock().unwrap() = Some(packets_tx);
            packets = Some(packets_rx);
        }
        let task = SubscriptionTask { kind, slot: slot.clone(), messages: messages_tx, packets, client: Arc::downgrade(inner) };
        match inner.open_endpoint(label, channel_init(options.delivery.unwrap_or_default(), options.max_age), None, |channel, opened| task.run(channel, opened)).await {
            Ok(channel) => Ok(Subscription { key: key.to_owned(), id, messages, channel, slot, client: Arc::downgrade(inner) }),
            // the channel's task frees the slot when it ends
            Err(error) => Err(error),
        }
    }

    /// A publisher on `key`, returned once the gateway accepted it.
    pub async fn publish(&self, key: &str, options: PublisherOptions) -> Result<Publisher> {
        let inner = &self.inner;
        let id = inner.next_id.fetch_add(1, Ordering::Relaxed);
        let delivery = options.delivery.unwrap_or_default();
        let state = Arc::new(PubState { tripped: Ending::new(), deadman_armed: AtomicBool::new(false), last: Mutex::new(None), blocked: Mutex::new(None) });
        let task_state = state.clone();
        let label = json!({"type": "pub", "key": key, "id": id, "opts": options_json(&options)});
        let channel = inner.open_endpoint(label, channel_init(delivery, None), Some(state.clone()), |channel, opened| run_publisher_channel(channel, opened, task_state)).await?;
        let repeat = options.repeat_ms.map(|ms| tokio::spawn(repeat_puts(channel.clone(), state.clone(), delivery, Duration::from_millis(ms.max(1)))));
        Ok(Publisher { key: key.to_owned(), id, channel, state, delivery, repeat, client: Arc::downgrade(inner) })
    }

    /// Takes (or renews) the exclusive right to publish on `group`'s keys among the gateway's clients: the
    /// server's group, or `keys` for one it doesn't define (SPEC "Leases"). Needs a heartbeat; it ends
    /// when the heartbeat stops, at `max_seconds`, on disconnect, on [`Lease::release`] or by force-expiry.
    pub async fn lease(&self, group: &str, keys: Option<Vec<String>>, max_seconds: Option<f64>) -> Result<Lease> {
        ensure!(self.inner.options.heartbeat_hz > 0.0, "a lease needs a heartbeat (ClientOptions::heartbeat_hz)");
        let (id, ending) = (self.inner.next_id.fetch_add(1, Ordering::Relaxed), Arc::new(Ending::new()));
        // registered by the control reader as it reads the reply (renewing a lease held before)
        self.inner.shared.pending_leases.lock().unwrap().insert(id, (group.to_owned(), ending.clone()));
        let reply = self.inner.request_as(id, json!({"op": "lease", "group": group, "keys": keys, "maxSeconds": max_seconds}), PING_TIMEOUT).await;
        self.inner.shared.pending_leases.lock().unwrap().remove(&id);
        let reply = reply?;
        let keys = serde_json::from_value(reply["keys"].clone())?;
        Ok(Lease { group: group.to_owned(), keys, expires_in_ms: reply["expiresInMs"].as_u64(), ending, client: Arc::downgrade(&self.inner) })
    }

    /// Ends another client's lease on `group` (needs the grant's force-expire right).
    pub async fn expire_lease(&self, group: &str) -> Result<()> {
        self.inner.request(json!({"op": "expireLease", "group": group}), PING_TIMEOUT).await.map(drop)
    }

    /// Closes the connection: the gateway fires this client's armed deadmen ("disconnected").
    pub async fn close(&self) {
        self.inner.shared.lost();
        let _ = self.inner.connection.close().await;
    }
}

/// The options as the gateway reads them: camelCase, unset ones left out.
fn options_json(options: &impl Serialize) -> Value {
    let mut value = serde_json::to_value(options).unwrap_or_default();
    if let Value::Object(map) = &mut value {
        map.retain(|_, value| !value.is_null());
    }
    value
}

/// Delivery → data channel reliability (SPEC "Delivery → transport mapping").
fn channel_init(delivery: Delivery, max_age_ms: Option<f64>) -> RTCDataChannelInit {
    match (delivery, max_age_ms) {
        (Delivery::Reliable, _) => RTCDataChannelInit { ordered: true, ..Default::default() },
        (Delivery::Latest, Some(age)) => RTCDataChannelInit { ordered: false, max_packet_life_time: Some(age.round().clamp(1.0, 65535.0) as u16), ..Default::default() },
        (Delivery::Latest, None) => RTCDataChannelInit { ordered: false, max_retransmits: Some(0), ..Default::default() },
    }
}

async fn run_control(channel: Arc<dyn DataChannel>, shared: Arc<Shared>, opened: oneshot::Sender<()>) {
    let mut opened = Some(opened);
    while let Some(event) = channel.poll().await {
        match event {
            DataChannelEvent::OnOpen => {
                let _ = opened.take().map(|opened| opened.send(()));
            }
            DataChannelEvent::OnMessage(message) => shared.on_control_message(&String::from_utf8_lossy(&message.data)),
            DataChannelEvent::OnClose => break,
            _ => {}
        }
    }
    // `control` lives as long as the connection
    shared.lost();
}

/// Beats `{t0, offsetMs, rttMs}`; each `{t0, t1, t2}` reply is a clock sample.
async fn run_heartbeat(channel: Arc<dyn DataChannel>, shared: Arc<Shared>, paused: Arc<AtomicBool>, hz: f64) {
    let mut ticker = tokio::time::interval(Duration::from_secs_f64(1.0 / hz));
    loop {
        tokio::select! {
            event = channel.poll() => match event {
                Some(DataChannelEvent::OnMessage(message)) => {
                    if let Ok(reply) = serde_json::from_slice::<Value>(&message.data) {
                        let time = |name: &str| reply[name].as_f64().unwrap_or(f64::NAN);
                        shared.clock.lock().unwrap().add(time("t0"), time("t1"), time("t2"), now_ms());
                    }
                }
                Some(DataChannelEvent::OnClose) | None => break,
                _ => {}
            },
            _ = ticker.tick() => {
                if !paused.load(Ordering::Relaxed) {
                    let beat = {
                        let clock = shared.clock.lock().unwrap();
                        json!({"t0": now_ms(), "offsetMs": clock.offset_ms, "rttMs": clock.rtt_ms})
                    };
                    let _ = channel.send_text(&beat.to_string()).await;
                }
            }
        }
    }
}

/// Clock sync (and the gateway's RTT samples) every second while connected; a failed ping is `Degraded`.
async fn run_pinger(inner: Weak<Inner>) {
    let mut ticker = tokio::time::interval(STATS_INTERVAL);
    ticker.tick().await;
    loop {
        ticker.tick().await;
        let Some(inner) = inner.upgrade() else { break };
        let state = *inner.shared.state.borrow();
        match (state, inner.ping().await) {
            (ConnectionState::Lost, _) => break,
            (ConnectionState::Degraded, Ok(())) => inner.shared.set_state(ConnectionState::Connected),
            (ConnectionState::Connected, Err(_)) => inner.shared.set_state(ConnectionState::Degraded),
            _ => {}
        }
    }
}

/// A publisher's channel: its open, `{"blocked": reason | null}` from the gateway, its close.
async fn run_publisher_channel(channel: Arc<dyn DataChannel>, opened: oneshot::Sender<()>, state: Arc<PubState>) {
    let mut opened = Some(opened);
    while let Some(event) = channel.poll().await {
        match event {
            DataChannelEvent::OnOpen => {
                let _ = opened.take().map(|opened| opened.send(()));
            }
            DataChannelEvent::OnMessage(message) => {
                if let Ok(update) = serde_json::from_slice::<Value>(&message.data) {
                    *state.blocked.lock().unwrap() = update["blocked"].as_str().map(str::to_owned);
                }
            }
            DataChannelEvent::OnClose => break,
            _ => {}
        }
    }
}

/// A subscription: its messages arrive with [`recv`](Self::recv) (or as a `Stream`). The gateway
/// stops sending while they are not consumed (64 queued here, then the consumption window). Dropping
/// it closes the channel; a video or audio subscription's track is kept for the next one of its codec.
pub struct Subscription {
    key: String,
    id: u64,
    messages: mpsc::Receiver<Message>,
    channel: Arc<dyn DataChannel>,
    slot: Option<Arc<MediaSlot>>,
    client: Weak<Inner>,
}

impl Subscription {
    /// The key expression subscribed to.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The next message; None once the channel or connection closed.
    pub async fn recv(&mut self) -> Option<Message> {
        self.messages.recv().await
    }

    /// Video codecs: asks the gateway for a keyframe (an RTCP PLI on the track, as a browser sends
    /// after loss; the client also sends one itself when it drops a frame). Fails before any video arrived.
    pub async fn request_keyframe(&self) -> Result<()> {
        self.slot.as_ref().context("not a video subscription")?.request_keyframe().await
    }

    /// Changes the running subscription's options in place (same channel and track): any of maxHz, minQuality,
    /// qualityToHzTradeoff, bandwidthPriority, maxBitrate, minResolutionScale, maxResolution, playoutDelay and
    /// encodeOptions.quality, as JSON (`null` puts one back to its default). Returns the options now in force.
    pub async fn update(&self, changes: Value) -> Result<Value> {
        let inner = self.client.upgrade().context("the client closed")?;
        let mut reply = inner.request(json!({"op": "updateSubscription", "subId": self.id, "opts": changes}), PING_TIMEOUT).await?;
        Ok(reply["opts"].take())
    }
}

impl futures::Stream for Subscription {
    type Item = Message;

    fn poll_next(self: std::pin::Pin<&mut Self>, context: &mut std::task::Context<'_>) -> std::task::Poll<Option<Message>> {
        self.get_mut().messages.poll_recv(context)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if let Some(inner) = self.client.upgrade() {
            inner.shared.endpoints.lock().unwrap().remove(&self.id);
        }
        close_in_background(&self.channel);
    }
}

fn close_in_background(channel: &Arc<dyn DataChannel>) {
    let channel = channel.clone();
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn(async move { channel.close().await });
    }
}

/// A chunked message being reassembled (SPEC "Large messages").
struct Partial {
    key: String,
    timestamp_ms: f64,
    flags: u8,
    chunks: Vec<Option<Vec<u8>>>,
    received: usize,
}

/// Turns one subscription's frames (and track packets) into messages, and acks consumption.
struct SubscriptionTask {
    /// `video` or `audio` on a media channel, None on the data channel
    kind: Option<String>,
    slot: Option<Arc<MediaSlot>>,
    messages: mpsc::Sender<Message>,
    packets: Option<mpsc::Receiver<Packet>>,
    client: Weak<Inner>,
}

enum Event {
    Frame(Vec<u8>),
    Packet(Packet),
    Ack,
    Nothing,
    Closed,
}

impl SubscriptionTask {
    async fn run(mut self, channel: Arc<dyn DataChannel>, opened: oneshot::Sender<()>) {
        let mut opened = Some(opened);
        let mut partials = BTreeMap::new();
        let mut assembler = Assembler { units: VecDeque::new(), waiting_for_keyframe: true, last_pli: Instant::now() };
        let (mut consumed, mut unacked_bytes, mut ack_at) = (0u32, 0usize, None::<tokio::time::Instant>);
        loop {
            let event = tokio::select! {
                event = channel.poll() => match event {
                    Some(DataChannelEvent::OnOpen) => {
                        let _ = opened.take().map(|opened| opened.send(()));
                        Event::Nothing
                    }
                    Some(DataChannelEvent::OnMessage(message)) => Event::Frame(message.data.to_vec()),
                    Some(DataChannelEvent::OnClose) | None => Event::Closed,
                    _ => Event::Nothing,
                },
                packet = next_packet(&mut self.packets) => packet.map_or(Event::Nothing, Event::Packet),
                _ = tokio::time::sleep_until(ack_at.unwrap_or_else(tokio::time::Instant::now)), if ack_at.is_some() => Event::Ack,
            };
            let message = match event {
                Event::Closed => break,
                Event::Nothing => None,
                Event::Packet(packet) => self.on_packet(packet, &mut assembler).await,
                Event::Ack => {
                    ack_at = None;
                    unacked_bytes = 0;
                    let _ = channel.send(BytesMut::from(&consumed.to_le_bytes()[..])).await;
                    None
                }
                Event::Frame(data) => {
                    let Some((header, chunk)) = frame::decode(&data) else {
                        continue;
                    };
                    let message = reassemble(&mut partials, &header, chunk).and_then(|(key, timestamp_ms, flags, bytes)| {
                        to_message(self.kind.as_deref(), key, timestamp_ms, header.seq, flags, bytes).unwrap_or_else(|error| {
                            warn!("zenoh-gateway client: a message on {} did not decode: {error:#}", header.key);
                            None
                        })
                    });
                    // a dropped subscription's messages go nowhere until its channel closes
                    if let Some(message) = message {
                        let _ = self.messages.send(message).await;
                    }
                    // consumed once queued: the queue's limit is the backpressure
                    consumed = header.frame_id;
                    unacked_bytes += data.len();
                    if unacked_bytes >= ACK_EVERY_BYTES {
                        ack_at = Some(tokio::time::Instant::now());
                    } else {
                        ack_at.get_or_insert_with(|| tokio::time::Instant::now() + ACK_DELAY);
                    }
                    None
                }
            };
            if let Some(message) = message {
                let _ = self.messages.send(message).await;
            }
        }
        // the gateway has let go of the track once the channel closed
        if let (Some(slot), Some(inner)) = (self.slot.take(), self.client.upgrade()) {
            inner.release_slot(slot);
        }
    }

    async fn on_packet(&self, packet: Packet, assembler: &mut Assembler) -> Option<Message> {
        let mime = media::format_of_payload_type(packet.header.payload_type)?;
        let Some(format) = video_format(mime) else {
            return Some(Message::Audio(AudioPacket { rtp_timestamp: packet.header.timestamp, data: packet.payload.to_vec() }));
        };
        let frame = assembler.push(packet, format);
        if assembler.waiting_for_keyframe
            && assembler.last_pli.elapsed() >= PLI_INTERVAL
            && let Some(slot) = &self.slot
        {
            assembler.last_pli = Instant::now();
            let _ = slot.request_keyframe().await;
        }
        frame.map(Message::Video)
    }
}

async fn next_packet(packets: &mut Option<mpsc::Receiver<Packet>>) -> Option<Packet> {
    let Some(receiver) = packets else {
        return std::future::pending().await;
    };
    let packet = receiver.recv().await;
    if packet.is_none() {
        *packets = None;
    }
    packet
}

/// A whole message (key, timestamp, flags, bytes) once all its chunks are here; incomplete older
/// ones are dropped when a newer one completes or more than `MAX_PARTIALS` wait.
fn reassemble(partials: &mut BTreeMap<u32, Partial>, header: &frame::FrameHeader<'_>, chunk: &[u8]) -> Option<(String, f64, u8, Vec<u8>)> {
    if header.chunk_count <= 1 {
        return Some((header.key.to_owned(), header.timestamp_ms, header.flags, chunk.to_vec()));
    }
    let count = header.chunk_count as usize;
    let partial = partials.entry(header.seq).or_insert_with(|| Partial { key: header.key.to_owned(), timestamp_ms: header.timestamp_ms, flags: header.flags, chunks: vec![None; count], received: 0 });
    if let Some(slot @ None) = partial.chunks.get_mut(header.chunk_index as usize) {
        *slot = Some(chunk.to_vec());
        partial.received += 1;
    }
    if partial.received < partial.chunks.len() {
        while partials.len() > MAX_PARTIALS {
            partials.pop_first();
        }
        return None;
    }
    let partial = partials.remove(&header.seq)?;
    partials.retain(|seq, _| *seq > header.seq);
    Some((partial.key, partial.timestamp_ms, partial.flags, partial.chunks.into_iter().flatten().flatten().collect()))
}

/// A whole data-channel message as what the subscription's channel makes of it (fields when the frame says so).
fn to_message(kind: Option<&str>, key: String, timestamp_ms: f64, seq: u32, flags: u8, bytes: Vec<u8>) -> Result<Option<Message>> {
    let bytes = if flags & frame::ZSTD != 0 { zstd::decode_all(&bytes[..])? } else { bytes };
    Ok(match kind {
        Some("video") => {
            ensure!(bytes.len() >= media::METADATA_LEN && bytes[0] == 1, "not a video metadata frame");
            let word = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
            Some(Message::VideoInfo(VideoFrameInfo {
                key,
                timestamp_ms,
                seq,
                keyframe: bytes[1] & 1 == 1,
                width: word(4),
                height: word(8),
                source_width: word(12),
                source_height: word(16),
                quality: f32::from_bits(word(20)),
                encoded_bytes: word(24),
            }))
        }
        // one empty frame per sample, for a browser callback
        Some("audio") => None,
        _ => {
            let delete = flags & frame::DELETE != 0;
            let (encoding, attachment, bytes) = if flags & frame::META != 0 {
                let (encoding, attachment, payload) = frame::decode_meta(&bytes).context("bad META header")?;
                (Some(encoding.to_owned()), (!attachment.is_empty()).then(|| attachment.to_vec()), payload.to_vec())
            } else {
                (None, None, bytes)
            };
            let fields = if flags & frame::FIELDS != 0 && !delete { Some(fields::parse(&bytes)?) } else { None };
            Some(Message::Data(DataMessage { key, bytes, timestamp_ms, seq, fields, delete, encoding, attachment }))
        }
    })
}

fn video_format(mime: &str) -> Option<VideoFormat> {
    [VideoFormat::H264, VideoFormat::Vp8, VideoFormat::Vp9, VideoFormat::Av1].into_iter().find(|format| format.mime_type() == mime)
}

/// Rebuilds access units from RTP (reordered and retransmitted packets included). After a lost
/// frame it drops frames until a keyframe, so what it yields always decodes.
struct Assembler {
    /// packets per RTP timestamp, oldest first
    units: VecDeque<(u32, Vec<Packet>)>,
    waiting_for_keyframe: bool,
    last_pli: Instant,
}

impl Assembler {
    fn push(&mut self, packet: Packet, format: VideoFormat) -> Option<VideoFrame> {
        let timestamp = packet.header.timestamp;
        let index = match self.units.iter().position(|(unit, _)| *unit == timestamp) {
            Some(index) => index,
            None => {
                self.units.push_back((timestamp, Vec::new()));
                self.units.len() - 1
            }
        };
        self.units[index].1.push(packet);
        let Some(data) = complete(&mut self.units[index].1, format) else {
            while self.units.len() > MAX_PARTIALS {
                self.units.pop_front();
                self.waiting_for_keyframe = true;
            }
            return None;
        };
        // an older unit still incomplete is lost
        self.waiting_for_keyframe |= index > 0;
        self.units.drain(..=index);
        let keyframe = is_keyframe(format, &data);
        self.waiting_for_keyframe &= !keyframe;
        (!self.waiting_for_keyframe).then(|| VideoFrame { format, data, keyframe, rtp_timestamp: timestamp, received_at: Instant::now() })
    }
}

/// The unit's frame once every packet from its first to its marker is here.
fn complete(packets: &mut Vec<Packet>, format: VideoFormat) -> Option<Vec<u8>> {
    let last = packets.iter().find(|packet| packet.header.marker)?.header.sequence_number;
    let behind = |packet: &Packet| last.wrapping_sub(packet.header.sequence_number) as usize;
    packets.sort_by_key(|packet| std::cmp::Reverse(behind(packet)));
    packets.dedup_by_key(|packet| packet.header.sequence_number);
    let count = packets.len();
    if packets.iter().enumerate().any(|(index, packet)| behind(packet) != count - 1 - index) {
        return None;
    }
    let mut depacketizer: Box<dyn Depacketizer + Send> = match format {
        VideoFormat::H264 => Box::new(H264Packet::default()),
        VideoFormat::Vp8 => Box::new(Vp8Packet::default()),
        VideoFormat::Vp9 => Box::new(Vp9Packet::default()),
        VideoFormat::Av1 => Box::new(Av1Depacketizer::default()),
    };
    if !depacketizer.is_partition_head(&packets[0].payload) {
        return None;
    }
    let mut data = Vec::new();
    for packet in packets.iter() {
        data.extend_from_slice(&depacketizer.depacketize(&packet.payload).ok()?);
    }
    Some(data)
}

/// Decodable on its own: an H.264 IDR slice, a VP8/VP9 key frame, an AV1 sequence header.
fn is_keyframe(format: VideoFormat, data: &[u8]) -> bool {
    match format {
        VideoFormat::H264 => data.windows(4).any(|window| window[..3] == [0, 0, 1] && window[3] & 0x1f == 5),
        VideoFormat::Vp8 => data.first().is_some_and(|byte| byte & 1 == 0),
        // frame marker 0b10, profile 0-2, not show_existing_frame, frame_type 0
        VideoFormat::Vp9 => data.first().is_some_and(|byte| byte >> 6 == 2 && byte & 0x30 != 0x30 && byte & 0x0c == 0),
        VideoFormat::Av1 => {
            let mut rest = data;
            while let Some(&header) = rest.first() {
                if (header >> 3) & 0x0f == 1 {
                    return true;
                }
                if header & 2 == 0 {
                    return false;
                }
                let mut at = 1 + ((header >> 2) & 1) as usize;
                let mut size = 0usize;
                for shift in (0..56).step_by(7) {
                    let Some(&byte) = rest.get(at) else {
                        return false;
                    };
                    at += 1;
                    size |= ((byte & 0x7f) as usize) << shift;
                    if byte & 0x80 == 0 {
                        break;
                    }
                }
                rest = rest.get(at + size..).unwrap_or_default();
            }
            false
        }
    }
}

/// Puts on one key (SPEC "Heartbeat and deadman" for the deadman). Dropping it closes the channel,
/// which also clears its deadman on the gateway.
pub struct Publisher {
    key: String,
    id: u64,
    channel: Arc<dyn DataChannel>,
    state: Arc<PubState>,
    delivery: Delivery,
    repeat: Option<tokio::task::JoinHandle<()>>,
    client: Weak<Inner>,
}

async fn send_put(channel: &Arc<dyn DataChannel>, delivery: Delivery, payload: &[u8], sent_at_ms: f64) -> Result<()> {
    if delivery == Delivery::Latest && channel.outstanding_bytes().await.unwrap_or_default() > BACKED_UP_BYTES {
        return Ok(());
    }
    let mut frame = BytesMut::with_capacity(8 + payload.len());
    frame.extend_from_slice(&sent_at_ms.to_le_bytes());
    frame.extend_from_slice(payload);
    channel.send(frame).await.map_err(|error| anyhow!("put: {error}"))
}

async fn repeat_puts(channel: Arc<dyn DataChannel>, state: Arc<PubState>, delivery: Delivery, every: Duration) {
    let mut ticker = tokio::time::interval(every);
    loop {
        ticker.tick().await;
        let last = state.last.lock().unwrap().clone();
        if state.tripped.reason().is_some() {
            break;
        }
        if let Some(last) = last
            && send_put(&channel, delivery, &last, now_ms()).await.is_err()
        {
            break;
        }
    }
}

impl Publisher {
    /// The key published on.
    pub fn key(&self) -> &str {
        &self.key
    }

    fn check(&self) -> Result<()> {
        match &self.state.tripped.reason() {
            Some(reason) => bail!("publisher {} is tripped (deadman fired: {reason}); create a new publisher", self.key),
            None => Ok(()),
        }
    }

    /// Puts `bytes`, stamped now. A `latest` put made while 64 KiB are still unsent is dropped.
    pub async fn put(&self, bytes: impl AsRef<[u8]>) -> Result<()> {
        self.put_at(bytes, now_ms()).await
    }

    /// Puts `bytes` produced at `timestamp_ms` (unix ms, this client's clock), for `latencyLimit`.
    pub async fn put_at(&self, bytes: impl AsRef<[u8]>, timestamp_ms: f64) -> Result<()> {
        self.check()?;
        if self.repeat.is_some() {
            *self.state.last.lock().unwrap() = Some(bytes.as_ref().to_vec());
        }
        send_put(&self.channel, self.delivery, bytes.as_ref(), timestamp_ms).await
    }

    /// Stores `bytes` on the gateway, published once (REAL_TIME, reliable) if this client's heartbeat
    /// stops, it disconnects, or the gateway shuts down; then this publisher is tripped.
    pub async fn set_deadman(&self, bytes: impl AsRef<[u8]>) -> Result<()> {
        let inner = self.client.upgrade().context("the client is gone")?;
        ensure!(inner.options.heartbeat_hz > 0.0, "set_deadman needs a heartbeat (ClientOptions::heartbeat_hz)");
        self.check()?;
        let bytes = base64::engine::general_purpose::STANDARD.encode(bytes);
        inner.request(json!({"op": "setDeadman", "pubId": self.id, "bytes": bytes}), PING_TIMEOUT).await?;
        self.state.deadman_armed.store(true, Ordering::Release);
        Ok(())
    }

    /// Disarms the deadman.
    pub async fn clear_deadman(&self) -> Result<()> {
        let inner = self.client.upgrade().context("the client is gone")?;
        inner.request(json!({"op": "clearDeadman", "pubId": self.id}), PING_TIMEOUT).await?;
        self.state.deadman_armed.store(false, Ordering::Release);
        Ok(())
    }

    /// Why the deadman fired (`"heartbeat"`, `"disconnected"`, `"shutdown"`), None while it hasn't.
    pub fn tripped(&self) -> Option<String> {
        self.state.tripped.reason()
    }

    /// Why the gateway is dropping this publisher's puts right now (another client's lease), None while it isn't.
    pub fn blocked(&self) -> Option<String> {
        self.state.blocked.lock().unwrap().clone()
    }

    /// Waits until the deadman fires and returns the reason.
    pub async fn wait_tripped(&self) -> String {
        self.state.tripped.wait().await
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        if let Some(repeat) = &self.repeat {
            repeat.abort();
        }
        if let Some(inner) = self.client.upgrade() {
            inner.shared.endpoints.lock().unwrap().remove(&self.id);
        }
        close_in_background(&self.channel);
    }
}

/// An exclusive right to publish on a group of keys among the gateway's clients ([`Client::lease`]).
pub struct Lease {
    /// the group
    pub group: String,
    /// the keys it covers
    pub keys: Vec<String>,
    /// when it expires (`max_seconds`), None without a limit
    pub expires_in_ms: Option<u64>,
    ending: Arc<Ending>,
    client: Weak<Inner>,
}

impl Lease {
    /// Why it ended (`"heartbeat"`, `"maxSeconds"`, `"disconnected"`, `"released"`, `"renewed"`, force-expiry), None while held.
    pub fn lost(&self) -> Option<String> {
        self.ending.reason()
    }

    /// Waits until it ends and returns why.
    pub async fn wait_lost(&self) -> String {
        self.ending.wait().await
    }

    /// Gives it up.
    pub async fn release(&self) -> Result<()> {
        if self.ending.reason().is_some() {
            return Ok(());
        }
        self.ending.end("released".into());
        let inner = self.client.upgrade().context("the client is gone")?;
        {
            let mut leases = inner.shared.leases.lock().unwrap();
            if leases.get(&self.group).is_some_and(|held| Arc::ptr_eq(held, &self.ending)) {
                leases.remove(&self.group);
            }
        }
        inner.request(json!({"op": "releaseLease", "group": self.group}), PING_TIMEOUT).await.map(drop)
    }
}

/// How [`Client`] reaches the server's signalling: HTTP, or zenoh queryables (SPEC "Signalling over zenoh").
enum Signalling {
    Http { url: String, http: reqwest::Client },
    Zenoh { session: zenoh::Session, name: String },
}

impl Signalling {
    /// The server's ICE servers (`GET /zenoh-gateway/ice`, or `zenoh-gateway/<name>/ice`), and whether it asks for relay only.
    async fn ice_servers(&self, token: Option<&str>) -> Result<(Vec<IceServer>, bool)> {
        let mut reply = match self {
            Signalling::Http { url, http } => {
                let request = http.get(format!("{url}{}", crate::ICE_PATH));
                let response = refuse_unauthorized(bearer(request, token).send().await.context("GET /zenoh-gateway/ice")?).await?;
                // a server without the route (older) has none to offer
                if !response.status().is_success() {
                    return Ok((Vec::new(), false));
                }
                response.json::<Value>().await?
            }
            Signalling::Zenoh { .. } => self.query("ice", json!({"token": token})).await?,
        };
        let relay_only = reply["iceTransportPolicy"] == "relay";
        Ok((serde_json::from_value(reply["iceServers"].take())?, relay_only))
    }

    /// Sends the offer, returns the answer.
    async fn offer(&self, token: Option<&str>, offer: &RTCSessionDescription) -> Result<RTCSessionDescription> {
        match self {
            Signalling::Http { url, http } => {
                let response = refuse_unauthorized(bearer(http.post(format!("{url}/offer")).json(offer), token).send().await.context("POST /offer")?).await?;
                let status = response.status();
                ensure!(status.is_success(), "gateway refused the offer: {status} {}", response.text().await.unwrap_or_default());
                Ok(response.json::<RTCSessionDescription>().await?)
            }
            Signalling::Zenoh { .. } => Ok(serde_json::from_value(self.query("offer", json!({"token": token, "offer": offer})).await?)?),
        }
    }

    /// One query on `zenoh-gateway/<name>/<op>`: the first reply's JSON; an error reply with status 401 is a refused token.
    async fn query(&self, op: &str, body: Value) -> Result<Value> {
        let Signalling::Zenoh { session, name } = self else { unreachable!("zenoh signalling only") };
        let key = format!("{}/{name}/{op}", crate::SIGNALLING_PREFIX);
        let replies = session.get(&key).payload(body.to_string()).timeout(OPEN_TIMEOUT).await.map_err(|error| anyhow!("querying {key}: {error}"))?;
        let reply = replies.recv_async().await.map_err(|_| anyhow!("no zenoh-gateway server answered on {key} (is it built with zenoh_signalling({name:?}) and reachable over zenoh?)"))?;
        match reply.result() {
            Ok(sample) => Ok(serde_json::from_slice(&sample.payload().to_bytes())?),
            Err(error) => {
                let error: Value = serde_json::from_slice(&error.payload().to_bytes()).unwrap_or_default();
                let reason = error["error"].as_str().unwrap_or("error");
                if error["status"] == 401 {
                    bail!("gateway refused the token: {reason}");
                }
                bail!("gateway refused the {op}: {reason}")
            }
        }
    }
}

fn bearer(request: reqwest::RequestBuilder, token: Option<&str>) -> reqwest::RequestBuilder {
    match token {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}

/// A 401 is final: the token is refused (or was revoked).
async fn refuse_unauthorized(response: reqwest::Response) -> Result<reqwest::Response> {
    if response.status() == reqwest::StatusCode::UNAUTHORIZED {
        bail!("gateway refused the token: {}", response.text().await.unwrap_or_default());
    }
    Ok(response)
}
