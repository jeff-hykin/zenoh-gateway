//! Media subscriptions: frames decoded by a message encoding, encoded (a `VideoEncoder` of the channel's format, or Opus
//! in `audio`) and written to a WebRTC track. The browser adds a recvonly transceiver and renegotiates over `control`
//! naming the channel; the gateway answers with a track of that channel's format bound to the transceiver's mid, which the
//! `sub` channel's label names (it then carries a small frame per media frame). Later subscriptions reuse the track.

use crate::encoding::registry::{self, EncodingRegistry};
use crate::encoding::video::{EncodedVideo, VideoEncoder, VideoPolicy, target};
use crate::encoding::{Channel, DecodedFrame, EncodingSample, MessageEncoding, VideoFormat};
use crate::subscription::{self, SubShared};
use anyhow::{Context, Result};
use bytes::Bytes;
use rtc::interceptor::{Attribute, BandwidthEstimator, EstimatorStats, Gcc, Interceptor, Packet, PacketReport, Registry, Slot, StreamInfo, TaggedPacket};
use rtc::media::Sample;
use rtc::media_stream::MediaStreamTrack;
use rtc::peer_connection::configuration::interceptor_registry::{CongestionFeedback, configure_congestion_control, register_default_interceptors};
use rtc::peer_connection::configuration::media_engine::MediaEngine;
use rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest;
use rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use rtc::rtp::extension::HeaderExtension;
use rtc::rtp::extension::playout_delay_extension::PlayoutDelayExtension;
use rtc::rtp_transceiver::rtp_sender::RTCRtpHeaderExtensionCapability;
use rtc::rtp_transceiver::rtp_sender::{RTCPFeedback, RTCRtpCodec, RTCRtpCodecParameters, RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};
use webrtc::data_channel::DataChannel;
use webrtc::media_stream::track_local::static_sample::TrackLocalStaticSample;
use webrtc::media_stream::track_local::{TrackLocal, TrackLocalEvent};
use webrtc::peer_connection::{PeerConnection, RTCSessionDescription};
use webrtc::rtp_transceiver::RtpSender;

/// Every format a track can have, with the payload type the gateway offers it at.
const FORMATS: [(&str, u8); 5] = [("video/H264", 102), ("video/VP8", 96), ("video/VP9", 98), ("video/AV1", 45), (OPUS, 111)];
const OPUS: &str = "audio/opus";
/// Google Congestion Control bounds, bits/s. It starts where the data-channel estimator starts
/// (`allocator::INITIAL_ESTIMATE`, 1 MB/s), since video is allocated no more than this estimate. The ceiling is only a
/// sanity bound (at 50 Mbit/s it capped 8 HD cameras on an idle link): streams' `maxBitrate` bounds what video asks for.
const GCC_INITIAL_BPS: f64 = crate::allocator::INITIAL_ESTIMATE * 8.0;
const GCC_MIN_BPS: f64 = 50_000.0;
const GCC_MAX_BPS: f64 = 2_000_000_000.0;
/// The pacer's rate as a multiple of the GCC estimate (see `ReportingEstimator::target_bitrate`).
const PACING_FACTOR: f64 = 2.5;
/// Asks the browser to show each frame as soon as it is decoded (min = max = 0): no jitter-buffer
/// smoothing, which on a jittery path held frames ~50-100 ms for even pacing.
const PLAYOUT_DELAY_URI: &str = "http://www.webrtc.org/experiments/rtp-hdrext/playout-delay";
/// Video metadata frame on the `sub` channel (SPEC "Wire formats").
pub const METADATA_LEN: usize = 28;

/// The track format of a payload type the gateway (and the Rust client) offers.
#[cfg(feature = "client")]
pub fn format_of_payload_type(payload_type: u8) -> Option<&'static str> {
    FORMATS.iter().find(|(_, offered)| *offered == payload_type).map(|(mime, _)| *mime)
}

fn rtp_codec(mime: &str) -> RTCRtpCodec {
    if mime == OPUS {
        return RTCRtpCodec { mime_type: mime.to_owned(), clock_rate: 48_000, channels: 2, sdp_fmtp_line: "minptime=10;useinbandfec=1".to_owned(), rtcp_feedback: vec![] };
    }
    let feedback = |typ: &str, parameter: &str| RTCPFeedback { typ: typ.to_owned(), parameter: parameter.to_owned() };
    let sdp_fmtp_line = match mime {
        // constrained baseline, what openh264 produces and every browser decodes
        "video/H264" => "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
        "video/VP9" => "profile-id=0",
        _ => "",
    };
    RTCRtpCodec { mime_type: mime.to_owned(), clock_rate: 90_000, channels: 0, sdp_fmtp_line: sdp_fmtp_line.to_owned(), rtcp_feedback: vec![feedback("ccm", "fir"), feedback("nack", ""), feedback("nack", "pli")] }
}

/// The track format a media channel carries (None: the data channel).
pub fn track_mime(channel: Channel) -> Option<&'static str> {
    match channel {
        Channel::Video(format) => Some(format.mime_type()),
        Channel::Audio => Some(OPUS),
        Channel::Data => None,
    }
}

/// Forwards to GCC and publishes its target bitrate where the allocator can read it.
struct ReportingEstimator {
    inner: Gcc,
    target_bps: Arc<AtomicU64>,
}

impl ReportingEstimator {
    fn publish(&self) {
        self.target_bps.store(self.inner.target_bitrate().to_bits(), Ordering::Relaxed);
    }
}

impl BandwidthEstimator for ReportingEstimator {
    fn on_reports(&mut self, now: Instant, reports: &[PacketReport]) {
        self.inner.on_reports(now, reports);
        self.publish();
    }

    /// What the pacer drains at: a multiple of the estimate, as libwebrtc paces; the allocator keeps the encoders near the
    /// estimate itself. Paced at exactly the estimate, keyframes queued and held video seconds behind its data channel.
    fn target_bitrate(&self) -> f64 {
        self.inner.target_bitrate() * PACING_FACTOR
    }

    fn handle_timeout(&mut self, now: Instant) {
        self.inner.handle_timeout(now);
        self.publish();
    }

    fn poll_timeout(&self) -> Option<Instant> {
        self.inner.poll_timeout()
    }

    fn stats(&self) -> EstimatorStats {
        self.inner.stats()
    }
}

/// Hands inbound PLI/FIR to the track's RTCP reader: the interceptor chain ends every RTCP packet no
/// interceptor marked for the application, so without it keyframe requests never reached the encoder.
#[derive(Default)]
struct KeyframeRequests {
    read: VecDeque<TaggedPacket>,
    write: VecDeque<TaggedPacket>,
}

impl rtc::sansio::Protocol<TaggedPacket, TaggedPacket, ()> for KeyframeRequests {
    type Rout = TaggedPacket;
    type Wout = TaggedPacket;
    type Eout = ();
    type Error = rtc::shared::error::Error;
    type Time = Instant;

    fn handle_read(&mut self, mut message: TaggedPacket) -> Result<(), Self::Error> {
        if let Packet::Rtcp(packets) = &message.message.packet
            && packets.iter().any(|packet| packet.as_any().is::<PictureLossIndication>() || packet.as_any().is::<FullIntraRequest>())
        {
            message.message.add(Attribute::DeliverToApplication);
        }
        self.read.push_back(message);
        Ok(())
    }

    fn poll_read(&mut self) -> Option<TaggedPacket> {
        self.read.pop_front()
    }

    fn handle_write(&mut self, message: TaggedPacket) -> Result<(), Self::Error> {
        self.write.push_back(message);
        Ok(())
    }

    fn poll_write(&mut self) -> Option<TaggedPacket> {
        self.write.pop_front()
    }
}

impl Interceptor for KeyframeRequests {
    fn bind_local_stream(&mut self, _info: &StreamInfo) {}
    fn unbind_local_stream(&mut self, _info: &StreamInfo) {}
    fn bind_remote_stream(&mut self, _info: &StreamInfo) {}
    fn unbind_remote_stream(&mut self, _info: &StreamInfo) {}
}

/// Track formats, RTCP reports, NACK and TWCC-fed GCC; returns the GCC target (f64 bits/s as bits in an AtomicU64).
pub fn media_setup() -> Result<(MediaEngine, Registry, Arc<AtomicU64>)> {
    let mut media_engine = MediaEngine::default();
    for (mime, payload_type) in FORMATS {
        let kind = if mime == OPUS { RtpCodecKind::Audio } else { RtpCodecKind::Video };
        media_engine.register_codec(RTCRtpCodecParameters { rtp_codec: rtp_codec(mime), payload_type }, kind)?;
    }
    media_engine.register_header_extension(RTCRtpHeaderExtensionCapability { uri: PLAYOUT_DELAY_URI.to_owned() }, RtpCodecKind::Video, None)?;
    let target_bps = Arc::new(AtomicU64::new(GCC_INITIAL_BPS.to_bits()));
    let estimator = ReportingEstimator { inner: Gcc::new(GCC_INITIAL_BPS, GCC_MIN_BPS, GCC_MAX_BPS), target_bps: target_bps.clone() };
    let registry = configure_congestion_control(Registry::new(), estimator, CongestionFeedback::Twcc, &mut media_engine)?;
    let registry = register_default_interceptors(registry, &mut media_engine)?.with(Slot::Custom(14_000), KeyframeRequests::default());
    Ok((media_engine, registry, target_bps))
}

pub fn gcc_target_bytes_per_sec(target_bps: &AtomicU64) -> f64 {
    f64::from_bits(target_bps.load(Ordering::Relaxed)) / 8.0
}

pub struct MediaTrack {
    pub mid: String,
    /// its format, e.g. "video/H264"
    pub mime: &'static str,
    track: Arc<TrackLocalStaticSample>,
    ssrc: u32,
    payload_type: u8,
    keyframe_requested: Arc<AtomicBool>,
    /// PLI/FIR requests from the browser
    pub keyframe_requests: Arc<AtomicU64>,
    /// the claim writing to the track (0: none)
    owner: AtomicU64,
}

static NEXT_CLAIM: AtomicU64 = AtomicU64::new(1);

impl MediaTrack {
    /// Claims the track for a subscription. The newest claim wins: a page that closes a subscription and opens another on
    /// the same transceiver must not wait for the old one's send loop to notice it closed.
    pub fn claim(self: &Arc<Self>) -> TrackClaim {
        let token = NEXT_CLAIM.fetch_add(1, Ordering::Relaxed);
        if self.owner.swap(token, Ordering::AcqRel) != 0 {
            log::debug!("track {}: a new subscription took it over", self.mid);
        }
        TrackClaim { track: self.clone(), token }
    }

    /// `playout_delay`: (min, max) in 10 ms units, video only (audio has its own jitter buffer and no playout-delay extension).
    pub async fn write(&self, data: Vec<u8>, duration: Duration, playout_delay: (u16, u16)) -> Result<()> {
        let sample = Sample { data: Bytes::from(data), duration, ..Sample::new(Instant::now()) };
        let writer = self.track.sample_writer(self.ssrc, self.payload_type);
        let (min_delay, max_delay) = playout_delay;
        let writer = if self.mime == OPUS { writer } else { writer.with_extension(HeaderExtension::PlayoutDelay(PlayoutDelayExtension { min_delay, max_delay })) };
        writer.write_sample(&sample).await?;
        Ok(())
    }
}

/// One subscription's hold on a track; dropping it frees the track unless a newer claim took it over.
pub struct TrackClaim {
    track: Arc<MediaTrack>,
    token: u64,
}

impl TrackClaim {
    /// False once a newer subscription claimed the track: this one must stop writing to it.
    pub fn is_current(&self) -> bool {
        self.track.owner.load(Ordering::Acquire) == self.token
    }
}

impl std::ops::Deref for TrackClaim {
    type Target = MediaTrack;
    fn deref(&self) -> &MediaTrack {
        &self.track
    }
}

impl Drop for TrackClaim {
    fn drop(&mut self) {
        let _ = self.track.owner.compare_exchange(self.token, 0, Ordering::AcqRel, Ordering::Acquire);
    }
}

/// Watches the track's RTCP for keyframe requests until the track is dropped.
fn spawn_rtcp_reader(track: Arc<TrackLocalStaticSample>, owner: Weak<MediaTrack>, keyframe_requested: Arc<AtomicBool>, keyframe_requests: Arc<AtomicU64>) {
    tokio::spawn(async move {
        while owner.strong_count() > 0 {
            match track.poll().await {
                Some(TrackLocalEvent::OnRtcpPacket(packets)) => {
                    for packet in packets {
                        let any = packet.as_any();
                        if any.is::<PictureLossIndication>() || any.is::<FullIntraRequest>() {
                            keyframe_requested.store(true, Ordering::Release);
                            keyframe_requests.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                // not bound yet (or unbound): look again shortly
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    });
}

/// Applies a browser offer (renegotiation over `control`). With `add` (a track format), first adds
/// a track, which pairs with the offer's new recvonly m-line of its kind, and returns it with its mid.
pub async fn renegotiate(connection: &Arc<dyn PeerConnection>, offer: RTCSessionDescription, add: Option<&'static str>) -> Result<(RTCSessionDescription, Option<Arc<MediaTrack>>)> {
    let added = if let Some(mime) = add {
        let ssrc = rand_ssrc();
        let kind = if mime == OPUS { RtpCodecKind::Audio } else { RtpCodecKind::Video };
        let track = Arc::new(TrackLocalStaticSample::new(
            Instant::now(),
            MediaStreamTrack::new(
                format!("zenoh-gateway-{ssrc}"),
                format!("zenoh-gateway-track-{ssrc}"),
                "zenoh-gateway".to_owned(),
                kind,
                vec![RTCRtpEncodingParameters { rtp_coding_parameters: RTCRtpCodingParameters { ssrc: Some(ssrc), ..Default::default() }, codec: rtp_codec(mime), ..Default::default() }],
            ),
        )?);
        let sender = connection.add_track(track.clone() as Arc<dyn TrackLocal>).await?;
        Some((track, sender, ssrc))
    } else {
        None
    };
    connection.set_remote_description(offer).await?;
    let answer = connection.create_answer(None).await?;
    connection.set_local_description(answer).await?;
    let local = connection.local_description().await.context("no local description after renegotiation")?;
    let (Some((track, sender, ssrc)), Some(mime)) = (added, add) else { return Ok((local, None)) };
    let mut mid = None;
    for transceiver in connection.get_transceivers().await {
        if let Ok(Some(transceiver_sender)) = transceiver.sender().await
            && transceiver_sender.id() == sender.id()
        {
            mid = transceiver.mid().await?;
        }
    }
    let mid = mid.context("the offer had no new m-line for the track (add a recvonly transceiver of its kind before renegotiating)")?;
    let payload_type = negotiated_payload_type(&sender, mime).await.with_context(|| format!("the browser did not accept {mime}"))?;
    let keyframe_requested = Arc::new(AtomicBool::new(true));
    let video = Arc::new(MediaTrack { mid, mime, track: track.clone(), ssrc, payload_type, keyframe_requested, keyframe_requests: Arc::default(), owner: AtomicU64::new(0) });
    spawn_rtcp_reader(track, Arc::downgrade(&video), video.keyframe_requested.clone(), video.keyframe_requests.clone());
    Ok((local, Some(video)))
}

async fn negotiated_payload_type(sender: &Arc<dyn RtpSender>, mime: &str) -> Result<u8> {
    let codecs = sender.get_parameters().await?.rtp_parameters.codecs;
    codecs.iter().find(|codec| codec.rtp_codec.mime_type.eq_ignore_ascii_case(mime)).map(|codec| codec.payload_type).context("not negotiated")
}

fn rand_ssrc() -> u32 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(Instant::now().elapsed().as_nanos() ^ std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos());
    (hasher.finish() as u32).max(1)
}

/// `u8 version=1 | u8 flags (bit0 keyframe) | u16 0 | u32 width | u32 height | u32 sourceWidth |
///  u32 sourceHeight | f32 quality | u32 encodedBytes`, little endian.
fn metadata(frame: &EncodedVideo, source: (u32, u32), quality: f64) -> [u8; METADATA_LEN] {
    let mut out = [0u8; METADATA_LEN];
    out[0] = 1;
    out[1] = frame.keyframe as u8;
    out[4..8].copy_from_slice(&frame.width.to_le_bytes());
    out[8..12].copy_from_slice(&frame.height.to_le_bytes());
    out[12..16].copy_from_slice(&source.0.to_le_bytes());
    out[16..20].copy_from_slice(&source.1.to_le_bytes());
    out[20..24].copy_from_slice(&(quality as f32).to_le_bytes());
    out[24..28].copy_from_slice(&(frame.data.len() as u32).to_le_bytes());
    out
}

/// A decoded frame and whether another frontend's decode was reused, or the decode error.
type DecodeOutcome = std::result::Result<(Arc<DecodedFrame>, bool), String>;

/// A picked frame and its decode, running on the blocking pool.
pub struct Decoding {
    pub key: String,
    pub item: subscription::Pending,
    /// (what came of the sample, milliseconds it took, its identity across frontends: key + payload hash)
    pub task: tokio::task::JoinHandle<(DecodeOutcome, f64, u64)>,
}

/// Starts decoding a picked sample.
pub fn start_decode(shared: &SubShared, codec: &Arc<dyn MessageEncoding>, key: String, item: subscription::Pending) -> Decoding {
    let payload = item.payload.to_bytes().into_owned();
    let (hash_key, encoding, codecs, codec, channel) = (key.clone(), item.encoding.clone(), shared.codecs.clone(), codec.clone(), shared.channel);
    let task = tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let sample = EncodingSample::new(&hash_key, &payload, &encoding);
        let hash = registry::sample_hash(&sample);
        let (decoded, reused) = codecs.decode_shared(&*codec, &sample, hash, channel);
        (decoded.map(|decoded| (decoded, reused)), started.elapsed().as_secs_f64() * 1000.0, hash)
    });
    Decoding { key, item, task }
}

/// Resolution-scale steps the CPU governor moves by.
const CPU_STEP: f64 = 0.1;
/// Share of the frame interval an encode may take before the picture steps down.
const CPU_HEADROOM: f64 = 0.85;
/// Time after a step before the next, so the costs are measured at the new size.
const CPU_SETTLE: Duration = Duration::from_secs(1);
/// Stepping up needs the next step's predicted encode within this share of the frame interval, and this long since the
/// last step: a resolution change costs a keyframe, and a flapping ceiling grew the browser's jitter buffer.
const CPU_UP_HEADROOM: f64 = 0.6;
const CPU_UP_SETTLE: Duration = Duration::from_secs(5);
/// Weight of the newest sample in the cost averages.
const CPU_EWMA_GAIN: f64 = 0.2;

fn ewma(average: Option<f64>, sample: f64) -> f64 {
    average.map_or(sample, |average| average + CPU_EWMA_GAIN * (sample - average))
}

/// Keeps an encode session within what the cores can encode at its frame rate (on a small CPU the rate collapsed): past
/// `CPU_HEADROOM` of the frame interval the picture's scale ceiling steps down from the scale in use (never below
/// `minResolutionScale`), and back up when the encode cost predicted at the next step (it scales with pixels) fits. It
/// only ever lowers the size the bitrate policy picked, never the bitrate or the rate, so the two can't fight; a
/// hardware encoder rarely moves it. Decode cost never moves it: it overlaps encoding and does not depend on size.
struct CpuGovernor {
    cap: f64,
    encode_ms: Option<f64>,
    changed_at: Option<Instant>,
}

impl CpuGovernor {
    fn new() -> Self {
        CpuGovernor { cap: 1.0, encode_ms: None, changed_at: None }
    }

    fn observe_encode(&mut self, ms: f64) {
        self.encode_ms = Some(ewma(self.encode_ms, ms));
    }

    /// The ceiling on the picture's scale, given the scale the last frame was encoded at.
    fn scale_cap(&mut self, current: f64, floor: f64, hz: f64, now: Instant) -> f64 {
        let since_change = self.changed_at.map(|at| now.duration_since(at));
        let settled = since_change.is_none_or(|elapsed| elapsed >= CPU_SETTLE);
        if let (true, Some(encode_ms)) = (settled, self.encode_ms) {
            let interval_ms = 1000.0 / hz.max(0.1);
            let current = current.min(self.cap);
            if encode_ms > interval_ms * CPU_HEADROOM && current > floor + 1e-9 {
                self.cap = (current - CPU_STEP).max(floor);
                self.encode_ms = None;
                self.changed_at = Some(now);
            } else if self.cap < 1.0 && since_change.is_none_or(|elapsed| elapsed >= CPU_UP_SETTLE) {
                let next = (self.cap + CPU_STEP).min(1.0);
                if encode_ms * (next / self.cap).powi(2) < interval_ms * CPU_UP_HEADROOM {
                    self.cap = next;
                    self.encode_ms = None;
                    self.changed_at = Some(now);
                }
            }
        }
        self.cap.max(floor)
    }
}

/// Encoded frames a session keeps for members that are a frame or two behind.
const SESSION_LOG: usize = 8;
/// One sample seen by two frontends: same payload, timestamps this close.
const SAME_SAMPLE_MS: f64 = 2.0;
/// Viewers share an encode while their grants are within this ratio of each other (it runs at the lowest).
const SHARE_RATIO: f64 = 1.25;

/// The encodes of every video stream, shared by viewers of one stream at one target (SPEC "Video"). Each member of a
/// session sends every frame of it, in order, so one encoder's reference chain serves them all.
#[derive(Default)]
pub struct VideoSessions {
    sessions: Mutex<Vec<Arc<EncodeSession>>>,
    next_member: AtomicU64,
    next_session: AtomicU64,
}

struct EncodeSession {
    /// creation order: a lone member moves to an older session it fits, so two never stay apart at one grant
    id: u64,
    codec: String,
    format: VideoFormat,
    key: String,
    policy: VideoPolicy,
    /// member → (bits/s, frames/s) it is granted
    grants: Mutex<HashMap<u64, (f64, f64)>>,
    state: tokio::sync::Mutex<SessionState>,
}

struct SessionState {
    encoder: Option<Box<dyn VideoEncoder>>,
    governor: CpuGovernor,
    log: VecDeque<Arc<SessionFrame>>,
    next_seq: u64,
    /// the newest sample encoded: (timestamp ms, decode hash)
    last_input: Option<(f64, u64)>,
    keyframe_wanted: bool,
    source: (u32, u32),
    scale: f64,
}

struct SessionFrame {
    seq: u64,
    frame: EncodedVideo,
    source: (u32, u32),
    timestamp_ms: f64,
    encoded_by: u64,
}

impl EncodeSession {
    /// Whether a member granted `bitrate` can share this session: the others' grants and its own are close.
    fn fits(&self, member: u64, bitrate: f64) -> bool {
        let grants = self.grants.lock().unwrap();
        let others = grants.iter().filter(|(id, _)| **id != member).map(|(_, (bps, _))| *bps);
        let (low, high) = others.fold((bitrate, bitrate), |(low, high), bps| (low.min(bps), high.max(bps)));
        high <= low.max(1.0) * SHARE_RATIO
    }
}

impl VideoSessions {
    /// The session `member` encodes in at `grant`: `current` while it still fits (unless it is alone there and an older
    /// session of the same stream and policy fits too), else the oldest such session that fits, else a new one.
    fn place(&self, current: Option<Arc<EncodeSession>>, member: u64, (codec, format, key): (&str, VideoFormat, &str), policy: VideoPolicy, grant: (f64, f64)) -> Arc<EncodeSession> {
        let mut sessions = self.sessions.lock().unwrap();
        let same_stream = |session: &&Arc<EncodeSession>| session.codec == codec && session.format == format && session.key == key && session.policy == policy;
        let oldest_fit = sessions.iter().filter(same_stream).filter(|session| session.fits(member, grant.0)).min_by_key(|session| session.id).cloned();
        let session = match (current, oldest_fit) {
            // the policy changes when the subscription is updated (updateSubscription)
            (Some(current), oldest) if current.policy == policy && current.fits(member, grant.0) && (current.grants.lock().unwrap().len() > 1 || oldest.as_ref().is_none_or(|oldest| oldest.id >= current.id)) => current,
            (current, oldest) => {
                if let Some(current) = current {
                    Self::leave_locked(&mut sessions, &current, member);
                }
                oldest.filter(|oldest| sessions.iter().any(|session| Arc::ptr_eq(session, oldest))).unwrap_or_else(|| {
                    let state = SessionState { encoder: None, governor: CpuGovernor::new(), log: VecDeque::new(), next_seq: 0, last_input: None, keyframe_wanted: true, source: (640, 480), scale: 1.0 };
                    let id = self.next_session.fetch_add(1, Ordering::Relaxed);
                    let session = Arc::new(EncodeSession { id, codec: codec.to_owned(), format, key: key.to_owned(), policy, grants: Mutex::default(), state: tokio::sync::Mutex::new(state) });
                    sessions.push(session.clone());
                    session
                })
            }
        };
        session.grants.lock().unwrap().insert(member, grant);
        session
    }

    fn leave(&self, session: &Arc<EncodeSession>, member: u64) {
        Self::leave_locked(&mut self.sessions.lock().unwrap(), session, member);
    }

    fn leave_locked(sessions: &mut Vec<Arc<EncodeSession>>, session: &Arc<EncodeSession>, member: u64) {
        let mut grants = session.grants.lock().unwrap();
        grants.remove(&member);
        if grants.is_empty() {
            sessions.retain(|other| !Arc::ptr_eq(other, session));
        }
    }

    #[cfg(test)]
    fn count(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }
}

/// One viewer's place in its session: the next frame it hasn't sent, and whether it has had a keyframe there.
struct Member {
    id: u64,
    session: Option<Arc<EncodeSession>>,
    next_seq: Option<u64>,
    synced: bool,
}

/// A picked, decoded sample to encode.
struct Input {
    decoded: Arc<DecodedFrame>,
    timestamp_ms: f64,
    hash: u64,
    quality: f64,
    keyframe_requested: bool,
}

/// What one step gave a member: frames to send, and how the encode went (if it ran one).
#[derive(Default)]
struct Step {
    frames: Vec<Arc<SessionFrame>>,
    encode_ms: Option<f64>,
    error: Option<String>,
    scale_cap: f64,
}

/// One member's turn with one sample: encode it if it is newer than anything the session encoded (otherwise another
/// member already did), then take every frame of the session this member hasn't sent, from a keyframe on.
async fn step(session: Arc<EncodeSession>, member: &mut Member, input: Input, codecs: Arc<EncodingRegistry>, codec: Arc<dyn MessageEncoding>) -> Step {
    let mut state = session.state.lock().await;
    let mut out = Step::default();
    // a new member starts at the newest frame if that is a keyframe, else at the next one
    let next_seq = *member.next_seq.get_or_insert_with(|| state.log.back().filter(|logged| logged.frame.keyframe).map_or(state.next_seq, |logged| logged.seq));
    let can_sync = member.synced || state.log.iter().any(|logged| logged.seq >= next_seq && logged.frame.keyframe);
    if !can_sync || input.keyframe_requested {
        state.keyframe_wanted = true;
    }
    // without zenoh timestamps each frontend stamps a sample on arrival, so the same sample differs by a little
    let newer = state.last_input.is_none_or(|(timestamp_ms, hash)| if hash == input.hash { input.timestamp_ms > timestamp_ms + SAME_SAMPLE_MS } else { input.timestamp_ms >= timestamp_ms });
    if newer {
        state.last_input = Some((input.timestamp_ms, input.hash));
        let is_picture = if let DecodedFrame::Video(image) = &*input.decoded {
            state.source = (image.width(), image.height());
            true
        } else {
            false
        };
        let grants: Vec<(f64, f64)> = session.grants.lock().unwrap().values().copied().collect();
        let bitrate = grants.iter().map(|grant| grant.0).fold(f64::INFINITY, f64::min);
        let hz = grants.iter().map(|grant| grant.1).fold(0.0, f64::max);
        let floor = session.policy.min_resolution_scale;
        let (scale, source) = (state.scale, state.source);
        out.scale_cap = state.governor.scale_cap(scale, floor, hz, Instant::now());
        let target = target(&session.policy, source, input.quality, bitrate, hz, out.scale_cap, state.keyframe_wanted);
        let mut encoder = match state.encoder.take().map_or_else(|| codecs.video_encoder(&*codec, session.format), Ok) {
            Ok(encoder) => encoder,
            Err(error) => {
                out.error = Some(error);
                return out;
            }
        };
        let decoded = input.decoded.clone();
        let encoding = tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let result = encoder.encode(&decoded, &target).map_err(|error| format!("{error:#}"));
            (encoder, result, started.elapsed().as_secs_f64() * 1000.0)
        });
        match encoding.await {
            Ok((encoder, result, encode_ms)) => {
                state.encoder = Some(encoder);
                state.governor.observe_encode(encode_ms);
                out.encode_ms = state.governor.encode_ms;
                match result {
                    Ok(Some(frame)) => {
                        if !is_picture {
                            state.source = (frame.width, frame.height);
                        }
                        state.scale = frame.width as f64 / state.source.0.max(1) as f64;
                        state.keyframe_wanted &= !frame.keyframe;
                        let seq = state.next_seq;
                        state.next_seq += 1;
                        let logged = SessionFrame { seq, source: state.source, frame, timestamp_ms: input.timestamp_ms, encoded_by: member.id };
                        state.log.push_back(Arc::new(logged));
                        if state.log.len() > SESSION_LOG {
                            state.log.pop_front();
                        }
                    }
                    Ok(None) => {}
                    Err(error) => out.error = Some(error),
                }
            }
            Err(error) => out.error = Some(format!("encoder task failed: {error}")),
        }
    } else {
        out.scale_cap = state.governor.cap;
    }
    if member.synced && state.log.front().is_some_and(|oldest| next_seq < oldest.seq) {
        // fell further behind than the log reaches: start over from a keyframe
        member.synced = false;
        state.keyframe_wanted = true;
    }
    for frame in state.log.iter().filter(|frame| frame.seq >= next_seq) {
        member.synced |= frame.frame.keyframe;
        if member.synced {
            out.frames.push(frame.clone());
        }
    }
    member.next_seq = Some(state.next_seq);
    out
}

/// Sends a video subscription's frames: pick (paced), decode (shared across frontends), encode in the encode session
/// of its grant (shared by viewers at that grant, off the runtime), write to the track. The next decode overlaps this
/// encode, so the slower of the two sets the rate (in series a 1920x1536 jpeg on a Jetson Orin core, ~16 ms + ~20 ms,
/// could not keep up with 30 Hz).
pub async fn send_loop(dc: Arc<dyn DataChannel>, shared: Arc<SubShared>, track: TrackClaim) {
    let Some(codec) = shared.codec.clone() else { return };
    let sessions = &shared.codecs.video_sessions;
    let mut member = Member { id: sessions.next_member.fetch_add(1, Ordering::Relaxed), session: None, next_seq: None, synced: false };
    let mut next_frame_id: u32 = 0;
    let mut last_write: Option<Instant> = None;
    let mut warned_write = false;
    let mut next: Option<Decoding> = None;
    let mut decode_ms: Option<f64> = None;
    while !shared.is_closed() && track.is_current() {
        let decoding = match next.take() {
            Some(decoding) => decoding,
            None => {
                let (picked, wake_at) = shared.pick(Instant::now());
                let Some((key, item)) = picked else {
                    shared.wait_for_data(wake_at).await;
                    continue;
                };
                start_decode(&shared, &codec, key, item)
            }
        };
        let Decoding { key, item, task } = decoding;
        let (decoded, hash) = match task.await {
            Ok((Ok((decoded, reused)), ms, hash)) => {
                if !reused {
                    decode_ms = Some(ewma(decode_ms, ms));
                }
                (decoded, hash)
            }
            Ok((Err(error), _, _)) => {
                shared.record_encoding_error(&error);
                continue;
            }
            Err(error) => {
                shared.record_encoding_error(&format!("decoder task failed: {error}"));
                continue;
            }
        };
        let (quality, bitrate, hz) = shared.video_grant(&key);
        let previous = member.session.take();
        let Channel::Video(format) = shared.channel else { return };
        let session = sessions.place(previous.clone(), member.id, (codec.name(), format, &key), shared.tuning().video_policy, (bitrate, hz));
        if previous.is_none_or(|previous| !Arc::ptr_eq(&previous, &session)) {
            (member.next_seq, member.synced) = (None, false);
        }
        member.session = Some(session.clone());
        let input = Input { decoded, timestamp_ms: item.timestamp_ms, hash, quality, keyframe_requested: track.keyframe_requested.swap(false, Ordering::AcqRel) };
        let outcome = {
            let stepping = step(session, &mut member, input, shared.codecs.clone(), codec.clone());
            tokio::pin!(stepping);
            // while it encodes, pick the next frame and start decoding it as soon as one may go
            loop {
                if next.is_some() {
                    break (&mut stepping).await;
                }
                let (picked, wake_at) = shared.pick(Instant::now());
                if let Some((key, item)) = picked {
                    next = Some(start_decode(&shared, &codec, key, item));
                    continue;
                }
                tokio::select! {
                    outcome = &mut stepping => break outcome,
                    _ = shared.wait_for_data(wake_at) => {}
                }
            }
        };
        if let Some(error) = &outcome.error {
            shared.record_encoding_error(error);
        }
        shared.record_video_timing(decode_ms, outcome.encode_ms, outcome.scale_cap);
        for logged in outcome.frames {
            let frame = &logged.frame;
            shared.note_video_source(logged.source.0, logged.source.1);
            let now = Instant::now();
            let duration = last_write.map_or(Duration::from_secs_f64(1.0 / hz.max(0.1)), |previous| now.duration_since(previous).max(Duration::from_millis(1)));
            last_write = Some(now);
            let meta = metadata(frame, logged.source, quality);
            if !track.is_current() {
                break;
            }
            if let Err(error) = track.write(frame.data.clone(), duration, shared.tuning().playout_delay).await {
                if !warned_write {
                    log::warn!("video track {}: write failed: {error:#}", track.mid);
                    warned_write = true;
                }
                shared.record_encoding_error(&format!("track write: {error:#}"));
                continue;
            }
            let shared_encode = logged.encoded_by != member.id;
            shared.record_video_frame(frame.data.len(), (frame.width, frame.height), quality, frame.keyframe, shared_encode, track.keyframe_requests.load(Ordering::Relaxed));
            if subscription::send_small_frame(&dc, &key, logged.timestamp_ms, item.seq, next_frame_id, &meta).await.is_err() && shared.is_closed() {
                break;
            }
            next_frame_id = next_frame_id.wrapping_add(1);
        }
    }
    if let Some(session) = member.session.take() {
        sessions.leave(&session, member.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::{EncodingOutput, EncodingSample, VideoImage, VideoTarget};
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn cpu_governor_trades_resolution_for_frame_rate() {
        let start = Instant::now();
        let mut governor = CpuGovernor::new();
        // 30 Hz leaves 33 ms a frame; encoding takes 50
        governor.observe_encode(50.0);
        assert!((governor.scale_cap(1.0, 0.25, 30.0, start) - 0.9).abs() < 1e-9, "one step down");
        governor.observe_encode(50.0);
        assert!((governor.scale_cap(0.9, 0.25, 30.0, start + Duration::from_millis(100)) - 0.9).abs() < 1e-9, "settles before the next step");
        governor.observe_encode(45.0);
        assert!((governor.scale_cap(0.9, 0.25, 30.0, start + CPU_SETTLE) - 0.8).abs() < 1e-9);
        // steps down from the scale in use: the bitrate policy had already shrunk the picture to 0.5
        let mut shrunk = CpuGovernor::new();
        shrunk.observe_encode(50.0);
        assert!((shrunk.scale_cap(0.5, 0.25, 30.0, start) - 0.4).abs() < 1e-9);
        // never below minResolutionScale
        let mut floor = CpuGovernor::new();
        floor.observe_encode(500.0);
        assert_eq!(floor.scale_cap(0.6, 0.5, 30.0, start), 0.5);
        floor.observe_encode(500.0);
        assert_eq!(floor.scale_cap(0.5, 0.5, 30.0, start + CPU_SETTLE), 0.5);
        // cheap again: steps back up when the next step's predicted cost fits
        governor.observe_encode(5.0);
        let later = start + CPU_SETTLE + CPU_UP_SETTLE;
        assert!((governor.scale_cap(0.8, 0.25, 30.0, start + CPU_SETTLE * 2) - 0.8).abs() < 1e-9, "waits longer to step up");
        assert!((governor.scale_cap(0.8, 0.25, 30.0, later) - 0.9).abs() < 1e-9, "steps up");
        // a step up that would land near the budget is not taken (no flapping)
        let mut near = CpuGovernor::new();
        near.observe_encode(40.0);
        assert!((near.scale_cap(1.0, 0.25, 30.0, start) - 0.9).abs() < 1e-9);
        near.observe_encode(17.0);
        assert!((near.scale_cap(0.9, 0.25, 30.0, start + CPU_UP_SETTLE) - 0.9).abs() < 1e-9, "17 ms now, ~21 predicted: stays");
    }

    /// Counts the frames it encodes; each is a keyframe when asked for.
    struct Counting(Arc<AtomicUsize>);

    impl VideoEncoder for Counting {
        fn format(&self) -> VideoFormat {
            VideoFormat::H264
        }

        fn encode(&mut self, _: &DecodedFrame, target: &VideoTarget) -> anyhow::Result<Option<EncodedVideo>> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(Some(EncodedVideo { data: vec![0; 10], width: target.width, height: target.height, keyframe: target.keyframe }))
        }
    }

    struct Camera(Arc<AtomicUsize>);

    impl MessageEncoding for Camera {
        fn name(&self) -> &str {
            "camera"
        }

        fn output(&self) -> EncodingOutput {
            EncodingOutput::Video
        }

        fn video_encoder(&self, _: VideoFormat) -> Option<Box<dyn VideoEncoder>> {
            Some(Box::new(Counting(self.0.clone())))
        }

        fn decode(&self, _: &EncodingSample<'_>, _: Channel) -> anyhow::Result<DecodedFrame> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn viewers_at_one_grant_share_one_encode() {
        let encodes = Arc::new(AtomicUsize::new(0));
        let codec: Arc<dyn MessageEncoding> = Arc::new(Camera(encodes.clone()));
        let codecs = Arc::new(EncodingRegistry::new([codec.clone()]).unwrap());
        let sessions = &codecs.video_sessions;
        let picture = Arc::new(DecodedFrame::Video(VideoImage::rgb8(64, 48, vec![0; 64 * 48 * 3]).unwrap()));
        let mut viewers: Vec<Member> = (0..3).map(|id| Member { id, session: None, next_seq: None, synced: false }).collect();
        let grants = [(4e6, 30.0), (4.4e6, 30.0), (1e6, 30.0)];
        let mut sent = vec![Vec::new(); 3];
        for sample in 0..10u64 {
            for (index, viewer) in viewers.iter_mut().enumerate() {
                let session = sessions.place(viewer.session.take(), viewer.id, ("camera", VideoFormat::H264, "cam0"), VideoPolicy::default(), grants[index]);
                viewer.session = Some(session.clone());
                let input = Input { decoded: picture.clone(), timestamp_ms: sample as f64 * 33.0, hash: sample, quality: 1.0, keyframe_requested: false };
                let step = step(session, viewer, input, codecs.clone(), codec.clone()).await;
                sent[index].extend(step.frames.iter().map(|frame| (frame.seq, frame.frame.keyframe, frame.encoded_by)));
            }
        }
        assert_eq!(sessions.count(), 2, "4 and 4.4 Mbit/s share one session, 1 Mbit/s has its own");
        // a viewer that started alone (its first grant was a guess) joins the older session once its grant fits there
        let late = sessions.place(None, 9, ("camera", VideoFormat::H264, "cam0"), VideoPolicy::default(), (16e6, 30.0));
        assert_eq!(sessions.count(), 3);
        let merged = sessions.place(Some(late), 9, ("camera", VideoFormat::H264, "cam0"), VideoPolicy::default(), (4.2e6, 30.0));
        assert!(Arc::ptr_eq(&merged, viewers[0].session.as_ref().unwrap()) && sessions.count() == 2);
        sessions.leave(&merged, 9);
        assert_eq!(encodes.load(Ordering::Relaxed), 20, "10 samples, encoded once per session");
        assert_eq!(sent[0], sent[1], "both viewers send the same frames, in order");
        assert!(sent[0][0].1 && sent[2][0].1, "each viewer starts at a keyframe");
        assert_eq!(sent[1].iter().filter(|frame| frame.2 == 0).count(), 10, "viewer 1 sent the frames viewer 0 encoded");
        // a viewer whose grant moves away leaves for a session of its own, from a keyframe
        let session = sessions.place(viewers[1].session.take(), 1, ("camera", VideoFormat::H264, "cam0"), VideoPolicy::default(), (12e6, 30.0));
        assert!(!Arc::ptr_eq(&session, viewers[0].session.as_ref().unwrap()));
        assert_eq!(sessions.count(), 3);
        for viewer in viewers.iter().filter(|viewer| viewer.session.is_some()) {
            sessions.leave(viewer.session.as_ref().unwrap(), viewer.id);
        }
        sessions.leave(&session, 1);
        assert_eq!(sessions.count(), 0, "a session ends with its last viewer");
    }
}
