//! Media subscriptions: frames decoded by a message encoding, encoded (a `VideoEncoder` of the channel's format, or Opus
//! in `audio`) and written to a WebRTC track. The browser adds a recvonly transceiver and renegotiates over `control`
//! naming the channel; the gateway answers with a track of that channel's format bound to the transceiver's mid, which the
//! `sub` channel's label names (it then carries a small frame per media frame). Later subscriptions reuse the track.

use crate::encoding::registry::{self, EncodingRegistry};
use crate::encoding::video::{EncodedVideo, VideoEncoder, VideoPolicy, target};
use crate::encoding::{Channel, DecodedFrame, EncodingSample, MessageEncoding, MessageHeader, VideoFormat};
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
use rtc::rtcp::sender_report::SenderReport;
use rtc::rtp::extension::HeaderExtension;
use rtc::rtp::extension::playout_delay_extension::PlayoutDelayExtension;
use rtc::rtp_transceiver::rtp_sender::RTCRtpHeaderExtensionCapability;
use rtc::rtp_transceiver::rtp_sender::{RTCPFeedback, RTCRtpCodec, RTCRtpCodecParameters, RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::future::Future;
use std::sync::Arc;
use parking_lot::Mutex;
use std::time::{Duration, Instant};
use webrtc::data_channel::DataChannel;
use webrtc::media_stream::track_local::static_sample::TrackLocalStaticSample;
use webrtc::media_stream::track_local::{TrackLocal, TrackLocalEvent};
use webrtc::peer_connection::{PeerConnection, RTCSessionDescription};
use webrtc::rtp_transceiver::RtpSender;

/// Every format a track can have, with the payload type the gateway offers it at.
const FORMATS: [(&str, u8); 5] = [("video/H264", 102), ("video/VP8", 96), ("video/VP9", 98), ("video/AV1", 45), (OPUS, 111)];
const OPUS: &str = "audio/opus";
/// Google Congestion Control bounds, bits/s. It starts where the link estimator starts (`allocator::INITIAL_ESTIMATE`,
/// 512 kbit/s), since video is allocated no more than that estimate. The ceiling is only a sanity bound (at 50 Mbit/s it
/// capped 8 HD cameras on an idle link): the link estimate and 1.5 x what video sent bound what video gets.
const GCC_MIN_BPS: f64 = 50_000.0;
const GCC_MAX_BPS: f64 = 2_000_000_000.0;
/// Video RTP's clock (H.264, VP8, VP9 and AV1 all run at 90 kHz).
const VIDEO_CLOCK_RATE: f64 = 90_000.0;
/// The pacer's rate as a multiple of the GCC estimate (see `ReportingEstimator::target_bitrate`).
const PACING_FACTOR: f64 = 2.5;
/// Video's queueing delay is measured against the least one-way delay over this long.
const QUEUE_BASE_WINDOW: Duration = Duration::from_secs(10);
/// GCC limits video for this long after it last cut its target (see `VideoRate::gcc_limit_bytes_per_sec`).
const GCC_BRAKE: Duration = Duration::from_secs(5);
/// Asks the browser to show each frame as soon as it is decoded (min = max = 0): no jitter-buffer
/// smoothing, which on a jittery path held frames ~50-100 ms for even pacing.
const PLAYOUT_DELAY_URI: &str = "http://www.webrtc.org/experiments/rtp-hdrext/playout-delay";
/// Carries each video frame's source time inside its RTP packets: a browser's requestVideoFrameCallback `captureTime`
/// (Chrome) and `RTCRtpReceiver.getSynchronizationSources()` `captureTimestamp` read it per frame.
const ABS_CAPTURE_TIME_URI: &str = "http://www.webrtc.org/experiments/rtp-hdrext/abs-capture-time";
/// How often a video track carries abs-capture-time (sampled, not every frame: each stamp costs 17 bytes per packet).
const CAPTURE_TIME_INTERVAL: Duration = Duration::from_secs(1);
/// Seconds from the NTP epoch (1900) to the unix epoch (1970).
const NTP_UNIX_OFFSET_SECS: u64 = 2_208_988_800;
/// Frames whose pacer departure is awaited, per video stream (enqueued but not yet left).
const PACER_PENDING_FRAMES: usize = 256;
/// Pacer delays kept per video stream for its percentiles.
const PACER_DELAY_SAMPLES: usize = 128;
/// Video metadata frame on the `sub` channel (SPEC "Wire formats").
pub const METADATA_LEN: usize = 64;
/// What metadata frames carried before `media_time_ms` and `written_ms` (0.5.19 and earlier).
#[cfg_attr(not(feature = "client"), allow(dead_code))]
pub const METADATA_LEN_V1: usize = 28;
/// What metadata frames carried before the RTP timestamp, gateway receive and encode times.
#[cfg_attr(not(feature = "client"), allow(dead_code))]
pub const METADATA_LEN_V2: usize = 44;
/// metadata flag: bytes 44..48 hold the frame's RTP timestamp
pub const METADATA_FLAG_RTP: u8 = 2;
/// metadata flag: the frame's message header follows byte 64 ([`frame::encode_header`](crate::frame::encode_header): 0.5.31 sent
/// `f64 stamp | u16 frameIdLength | frameId`, 0.5.32+ adds `i32 sec | u32 nanosec | u8 hasSeq | u32 seq`)
pub const METADATA_FLAG_HEADER: u8 = 4;

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
/// A connection's video rates, bits/s as f64 bits: GCC's target (published by its estimator) and the most the allocator
/// lets video take (the pacer's ceiling).
#[derive(Default)]
pub struct VideoRate {
    gcc_bps: AtomicU64,
    cap_bps: AtomicU64,
    /// the link model's bandwidth (video and data together), bits/s: what the pacer may burst a frame out at
    link_bps: AtomicU64,
    /// the link's measured bandwidth (times its gain while it probes) over what video is allowed now (f64 bits): the picture is sized for the grant times
    /// this, so a brake or GCC cut lowers the bitrate, not the size (each new size is a new encoder and a keyframe, and
    /// a size that followed the brake's 6-63 KB/s swings cost a keyframe a second on a 500 kbit/s link)
    size_ratio: AtomicU64,
    /// the link model is in its startup and video isn't queued at the link (see `steady_size`)
    starting_up: AtomicBool,
    /// bytes of video the browser's transport feedback reported arrived, ever
    received_bytes: AtomicU64,
    /// when GCC last cut its target (it found the path congested)
    braked_at: parking_lot::Mutex<Option<std::time::Instant>>,
    /// how long video's packets of the latest feedback waited in a queue (ms, f64 bits): their median one-way delay
    /// above the least of the last `QUEUE_BASE_WINDOW`
    queue_delay_ms: AtomicU64,
    /// what video delivered by the receiver's clock, see `VideoRate::delivery`
    delivery: parking_lot::Mutex<Option<Delivery>>,
    /// the RTP pacer's backlog and the tracks it asks for keyframes (`rtp_pacer`)
    pub pacer: Arc<crate::rtp_pacer::PacerShared>,
    /// each video stream's first RTP timestamp and how long its frames waited in the pacer (see [`RtpTap`])
    pub rtp: Mutex<HashMap<u32, RtpStream>>,
}

/// One outgoing video stream as the interceptor chain saw it.
#[derive(Default)]
pub struct RtpStream {
    /// the RTP timestamp of its first packet: a frame's is this plus its ticks on the track
    first_timestamp: Option<u32>,
    /// (RTP timestamp, when its first packet entered the pacer) of frames whose last packet hasn't left yet
    enqueued: VecDeque<(u32, Instant)>,
    /// ms the latest frames waited in the pacer (first packet in to last packet out)
    pacer_ms: VecDeque<f64>,
}

impl RtpStream {
    /// (p50, p95) of the latest frames' pacer delays, ms.
    pub fn pacer_percentiles(&self) -> Option<(f64, f64)> {
        let mut sorted: Vec<f64> = self.pacer_ms.iter().copied().collect();
        sorted.sort_by(f64::total_cmp);
        let at = |q: f64| sorted[((sorted.len() - 1) as f64 * q).round() as usize];
        (!sorted.is_empty()).then(|| (at(0.5), at(0.95)))
    }
}

/// Watches departing video RTP from two places in the chain: above the pacer (`Enqueue`, where a frame's packets arrive
/// together, so the first of a stream gives its first RTP timestamp) and below it (`Depart`, where they leave on the
/// pacer's clock). Audio streams aren't tracked (no marker per frame).
struct RtpTap {
    side: TapSide,
    rate: Arc<VideoRate>,
    read: VecDeque<TaggedPacket>,
    write: VecDeque<TaggedPacket>,
}

#[derive(Clone, Copy, PartialEq)]
enum TapSide {
    Enqueue,
    Depart,
}

impl rtc::sansio::Protocol<TaggedPacket, TaggedPacket, ()> for RtpTap {
    type Rout = TaggedPacket;
    type Wout = TaggedPacket;
    type Eout = ();
    type Error = rtc::shared::error::Error;
    type Time = Instant;

    fn handle_read(&mut self, message: TaggedPacket) -> Result<(), Self::Error> {
        self.read.push_back(message);
        Ok(())
    }

    fn poll_read(&mut self) -> Option<TaggedPacket> {
        self.read.pop_front()
    }

    fn handle_write(&mut self, message: TaggedPacket) -> Result<(), Self::Error> {
        if let Packet::Rtp(packet) = &message.message.packet {
            let header = &packet.header;
            let mut streams = self.rate.rtp.lock();
            if let Some(stream) = streams.get_mut(&header.ssrc) {
                stream.observe(self.side, header.timestamp, header.marker, Instant::now());
            }
        }
        self.write.push_back(message);
        Ok(())
    }

    fn poll_write(&mut self) -> Option<TaggedPacket> {
        self.write.pop_front()
    }
}

impl RtpStream {
    fn observe(&mut self, side: TapSide, timestamp: u32, marker: bool, now: Instant) {
        match side {
            TapSide::Enqueue => {
                self.first_timestamp.get_or_insert(timestamp);
                if self.enqueued.back().is_none_or(|(last, _)| *last != timestamp) {
                    if self.enqueued.len() == PACER_PENDING_FRAMES {
                        self.enqueued.pop_front();
                    }
                    self.enqueued.push_back((timestamp, now));
                }
            }
            // (a retransmission's frame left long ago: not in `enqueued`)
            TapSide::Depart if marker && self.enqueued.iter().any(|(queued, _)| *queued == timestamp) => {
                // frames leave in order: anything older than this one left without its marker (lost to the pacer)
                while let Some(&(queued, at)) = self.enqueued.front() {
                    self.enqueued.pop_front();
                    if queued == timestamp {
                        if self.pacer_ms.len() == PACER_DELAY_SAMPLES {
                            self.pacer_ms.pop_front();
                        }
                        self.pacer_ms.push_back(now.saturating_duration_since(at).as_secs_f64() * 1000.0);
                        break;
                    }
                }
            }
            TapSide::Depart => {}
        }
    }
}

impl Interceptor for RtpTap {
    fn bind_local_stream(&mut self, info: &StreamInfo) {
        if self.side == TapSide::Enqueue && info.mime_type.starts_with("video/") {
            self.rate.rtp.lock().entry(info.ssrc).or_default();
        }
    }
    fn unbind_local_stream(&mut self, info: &StreamInfo) {
        self.rate.rtp.lock().remove(&info.ssrc);
    }
    fn bind_remote_stream(&mut self, _info: &StreamInfo) {}
    fn unbind_remote_stream(&mut self, _info: &StreamInfo) {}
}

/// The abs-capture-time RTP header extension: the frame's capture time as a 64-bit NTP timestamp (UQ32.32), and the
/// capture clock's offset from this sender's NTP clock (Q32.32; 0: the gateway's clock, which zenoh timestamps and its
/// own receive stamps both are, to within NTP on one host).
struct AbsCaptureTime {
    capture_unix_ms: f64,
}

impl AbsCaptureTime {
    fn ntp(&self) -> u64 {
        let unix = (self.capture_unix_ms.max(0.0) / 1000.0) + NTP_UNIX_OFFSET_SECS as f64;
        let seconds = unix.trunc();
        ((seconds as u64) << 32) | ((unix - seconds) * 4_294_967_296.0) as u64
    }
}

impl rtc::shared::marshal::MarshalSize for AbsCaptureTime {
    fn marshal_size(&self) -> usize {
        16
    }
}

impl rtc::shared::marshal::Marshal for AbsCaptureTime {
    fn marshal_to(&self, buf: &mut [u8]) -> rtc::shared::error::Result<usize> {
        if buf.len() < 16 {
            return Err(rtc::shared::error::Error::ErrBufferTooSmall);
        }
        buf[0..8].copy_from_slice(&self.ntp().to_be_bytes());
        buf[8..16].fill(0);
        Ok(16)
    }
}

/// Video's delivery rate over the last `pacing::SAMPLE_SPAN` of arrivals (the receiver's clock, so feedback that comes
/// late or in bursts doesn't move it: per allocation round, what feedback reported arrived read 0 for seconds on a
/// 125 KB/s link while video got through), and the least queueing delay of its packets, 0 unless the bottleneck was
/// never idle the whole span (a link with a backlog throughout delivered at its own rate).
#[derive(Debug, Clone, Copy)]
pub struct Delivery {
    pub bytes_per_sec: f64,
    pub least_queue_ms: f64,
    /// when the feedback it came from got here
    pub at: std::time::Instant,
}

impl VideoRate {
    pub fn gcc_bytes_per_sec(&self) -> f64 {
        f64::from_bits(self.gcc_bps.load(Ordering::Relaxed)) / 8.0
    }

    /// GCC's target while it brakes (it cut its target within `GCC_BRAKE`), else `None`. GCC is a brake, not a ceiling:
    /// it climbs ~8% a second from its first guess, so held under it video took 30 s to reach a link with room from the
    /// first second; the link model (which measures video's delivery too) sets the pace otherwise.
    pub fn gcc_limit_bytes_per_sec(&self) -> Option<f64> {
        self.braked_at.lock().filter(|at| at.elapsed() < GCC_BRAKE).map(|_| self.gcc_bytes_per_sec())
    }

    /// What the allocator granted video; the pacer never goes below GCC's own floor (RTCP and a keyframe still get out).
    pub fn set_cap_bytes_per_sec(&self, bytes_per_sec: f64) {
        self.cap_bps.store((bytes_per_sec * 8.0).max(GCC_MIN_BPS).to_bits(), Ordering::Relaxed);
    }

    /// See `size_ratio`.
    pub fn set_size_ratio(&self, ratio: f64) {
        self.size_ratio.store(ratio.to_bits(), Ordering::Relaxed);
    }

    pub fn size_ratio(&self) -> f64 {
        f64::from_bits(self.size_ratio.load(Ordering::Relaxed))
    }

    pub fn set_starting_up(&self, starting_up: bool) {
        self.starting_up.store(starting_up, Ordering::Relaxed);
    }

    pub fn starting_up(&self) -> bool {
        self.starting_up.load(Ordering::Relaxed)
    }

    /// The link model's bandwidth estimate (see `link_bps`).
    pub fn set_link_bytes_per_sec(&self, bytes_per_sec: f64) {
        self.link_bps.store((bytes_per_sec * 8.0).to_bits(), Ordering::Relaxed);
    }

    /// What video delivered lately (`Delivery`), or `None` once no feedback came for a sample's span (no video, or
    /// none got through).
    pub fn delivery(&self) -> Option<Delivery> {
        (*self.delivery.lock()).filter(|delivery| delivery.at.elapsed() <= crate::pacing::SAMPLE_SPAN)
    }

    /// How long video waited in a queue, per the latest transport feedback (ms).
    pub fn queue_delay_ms(&self) -> f64 {
        f64::from_bits(self.queue_delay_ms.load(Ordering::Relaxed))
    }

    /// Bytes of video the browser reported arrived, ever: what video really got through (GCC's target is a guess that
    /// climbs without limit while no video flows).
    pub fn received_bytes(&self) -> u64 {
        self.received_bytes.load(Ordering::Relaxed)
    }

    fn cap_bps(&self) -> f64 {
        f64::from_bits(self.cap_bps.load(Ordering::Relaxed))
    }

    /// What the RTP pacer drains at, bits/s: a multiple of what the link carries (video's grant, or the whole link's
    /// estimate when that is more), under GCC's target while it brakes. Paced at a multiple of video's grant alone, a
    /// frame took 40% of a frame interval to leave and a keyframe several (75 ms p50, 270 ms p95 from track to browser
    /// on localhost, the link mostly idle); never above a multiple of the link's estimate, so a keyframe can't burst
    /// into a 100 kbit/s link at 1.3 Mbit/s (paced off GCC alone, it held the strict streams behind it for seconds).
    pub fn pacing_bps(&self) -> f64 {
        let gcc = self.gcc_limit_bytes_per_sec().map_or(f64::INFINITY, |limit| limit * 8.0);
        let link = f64::from_bits(self.link_bps.load(Ordering::Relaxed));
        gcc.min(self.cap_bps().max(link)) * PACING_FACTOR
    }

    /// How long a frame written now would wait in the RTP pacer (`rtp_pacer::PacerShared::delay`).
    pub fn pacer_delay(&self) -> Duration {
        self.pacer.delay()
    }
}

struct ReportingEstimator {
    inner: Gcc,
    rate: Arc<VideoRate>,
    /// the first departure seen (one-way delays are measured from it) and (when, least one-way delay) per feedback
    epoch: Option<Instant>,
    one_way_floor: VecDeque<(Instant, f64)>,
    /// (arrival ms on the receiver's clock, bytes, one-way delay ms) of packets that arrived, over a sample's span
    arrivals: VecDeque<(f64, usize, f64)>,
    /// the newest arrival (receiver's clock, ms) when feedback last reported a packet lost
    lost_at: f64,
}

impl ReportingEstimator {
    fn publish(&self) {
        let target = self.inner.target_bitrate();
        let previous = f64::from_bits(self.rate.gcc_bps.swap(target.to_bits(), Ordering::Relaxed));
        if target < previous * 0.95 {
            *self.rate.braked_at.lock() = Some(std::time::Instant::now());
        }
    }
}

impl BandwidthEstimator for ReportingEstimator {
    fn on_reports(&mut self, now: Instant, reports: &[PacketReport]) {
        let arrived: usize = reports.iter().filter(|report| report.arrived).map(|report| report.size).sum();
        self.rate.received_bytes.fetch_add(arrived as u64, Ordering::Relaxed);
        // video's queueing delay: one-way delay (receiver clock minus send time, offset unknown) above its recent least
        let epoch = *self.epoch.get_or_insert_with(|| reports.first().map_or(now, |report| report.departure));
        let one_way: Vec<f64> = reports
            .iter()
            .filter_map(|report| report.arrival.map(|arrival| arrival.as_secs_f64() * 1000.0 - report.departure.saturating_duration_since(epoch).as_secs_f64() * 1000.0))
            .collect();
        if let Some(least) = one_way.iter().copied().reduce(f64::min) {
            self.one_way_floor.push_back((now, least));
            while self.one_way_floor.front().is_some_and(|(at, _)| now.saturating_duration_since(*at) > QUEUE_BASE_WINDOW) {
                self.one_way_floor.pop_front();
            }
            let floor = self.one_way_floor.iter().map(|(_, delay)| *delay).fold(f64::INFINITY, f64::min);
            // the batch's median (a Wi-Fi aggregation burst delays a few packets without a standing queue)
            let mut sorted = one_way.clone();
            sorted.sort_by(f64::total_cmp);
            let median = sorted[sorted.len() / 2];
            self.rate.queue_delay_ms.store((median - floor).max(0.0).to_bits(), Ordering::Relaxed);
            for report in reports {
                if let Some(arrival) = report.arrival.filter(|_| report.arrived) {
                    let arrival_ms = arrival.as_secs_f64() * 1000.0;
                    self.arrivals.push_back((arrival_ms, report.size, arrival_ms - report.departure.saturating_duration_since(epoch).as_secs_f64() * 1000.0));
                }
            }
            let span_ms = crate::pacing::SAMPLE_SPAN.as_secs_f64() * 1000.0;
            let newest = self.arrivals.iter().map(|(at, _, _)| *at).fold(f64::NEG_INFINITY, f64::max);
            if reports.iter().any(|report| !report.arrived) {
                self.lost_at = newest;
            }
            // (one packet from before the span is kept: it says the span is covered)
            while self.arrivals.get(1).is_some_and(|(at, _, _)| *at <= newest - span_ms) {
                self.arrivals.pop_front();
            }
            if self.arrivals.front().is_some_and(|(at, _, _)| *at <= newest - span_ms) {
                let within = || self.arrivals.iter().filter(|(at, _, _)| *at > newest - span_ms);
                let bytes: usize = within().map(|(_, bytes, _)| *bytes).sum();
                // the link had no idle moment: each packet was already queued when the one before it arrived (a
                // keyframe that queued after a quiet stretch had a high least delay, but a span half idle, and read the
                // 125 KB/s link at 57)
                // and lost nothing: a full drop-tail queue also carried resends and RTCP the arrivals don't count (a
                // 1 s queue on a 125 KB/s link read 62-86 KB/s of video, and the estimate halved)
                let busy = self.lost_at <= newest - span_ms && self.arrivals.iter().zip(self.arrivals.iter().skip(1)).all(|((previous, _, _), (at, _, one_way))| at - (one_way - floor) <= *previous);
                let least_queue_ms = if busy { within().map(|(_, _, one_way)| one_way - floor).fold(f64::INFINITY, f64::min).max(0.0) } else { 0.0 };
                *self.rate.delivery.lock() = Some(Delivery { bytes_per_sec: bytes as f64 / (span_ms / 1000.0), least_queue_ms, at: std::time::Instant::now() });
            }
        }
        self.inner.on_reports(now, reports);
        self.publish();
    }

    /// What the pacer drains at: a multiple of the estimate, as libwebrtc paces; the allocator keeps the encoders near the
    /// estimate itself. Paced at exactly the estimate, keyframes queued and held video seconds behind its data channel.
    /// Never above a multiple of the allocator's video cap: paced off GCC alone, a keyframe burst into a 100 kbit/s link
    /// at 1.3 Mbit/s and held the strict streams behind it for seconds.
    fn target_bitrate(&self) -> f64 {
        self.rate.pacing_bps()
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

/// The newest RTCP sender report of each of the browser's streams, by SSRC: (NTP wallclock, RTP timestamp) at one
/// instant, which maps a packet's RTP timestamp to its capture time on the page's wall clock.
#[derive(Default)]
pub struct SenderReports(parking_lot::Mutex<HashMap<u32, (u64, u32)>>);

impl SenderReports {
    /// The newest (NTP time, RTP time) the stream `ssrc` reported.
    pub fn get(&self, ssrc: u32) -> Option<(u64, u32)> {
        self.0.lock().get(&ssrc).copied()
    }
}

/// Files inbound sender reports in [`SenderReports`] (the chain ends them otherwise).
struct SenderReportReader {
    reports: Arc<SenderReports>,
    read: VecDeque<TaggedPacket>,
    write: VecDeque<TaggedPacket>,
}

impl rtc::sansio::Protocol<TaggedPacket, TaggedPacket, ()> for SenderReportReader {
    type Rout = TaggedPacket;
    type Wout = TaggedPacket;
    type Eout = ();
    type Error = rtc::shared::error::Error;
    type Time = Instant;

    fn handle_read(&mut self, message: TaggedPacket) -> Result<(), Self::Error> {
        if let Packet::Rtcp(packets) = &message.message.packet {
            for report in packets.iter().filter_map(|packet| packet.as_any().downcast_ref::<SenderReport>()) {
                self.reports.0.lock().insert(report.ssrc, (report.ntp_time, report.rtp_time));
            }
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

impl Interceptor for SenderReportReader {
    fn bind_local_stream(&mut self, _info: &StreamInfo) {}
    fn unbind_local_stream(&mut self, _info: &StreamInfo) {}
    fn bind_remote_stream(&mut self, _info: &StreamInfo) {}
    fn unbind_remote_stream(&mut self, _info: &StreamInfo) {}
}

/// Track formats, RTCP reports, NACK and TWCC-fed GCC (starting from `initial_bandwidth` bytes/s); returns the
/// connection's video rates and the browser's sender reports.
pub fn media_setup(initial_bandwidth: f64) -> Result<(MediaEngine, Registry, Arc<VideoRate>, Arc<SenderReports>)> {
    let mut media_engine = MediaEngine::default();
    for (mime, payload_type) in FORMATS {
        let kind = if mime == OPUS { RtpCodecKind::Audio } else { RtpCodecKind::Video };
        media_engine.register_codec(RTCRtpCodecParameters { rtp_codec: rtp_codec(mime), payload_type }, kind)?;
    }
    media_engine.register_header_extension(RTCRtpHeaderExtensionCapability { uri: PLAYOUT_DELAY_URI.to_owned() }, RtpCodecKind::Video, None)?;
    media_engine.register_header_extension(RTCRtpHeaderExtensionCapability { uri: ABS_CAPTURE_TIME_URI.to_owned() }, RtpCodecKind::Video, None)?;
    let gcc_initial_bps = initial_bandwidth * 8.0;
    let rate = Arc::new(VideoRate { gcc_bps: AtomicU64::new(gcc_initial_bps.to_bits()), cap_bps: AtomicU64::new(gcc_initial_bps.to_bits()), link_bps: AtomicU64::new(0), size_ratio: AtomicU64::new(1f64.to_bits()), starting_up: AtomicBool::new(true), received_bytes: AtomicU64::new(0), braked_at: Default::default(), queue_delay_ms: AtomicU64::new(0), delivery: Default::default(), pacer: Default::default(), rtp: Default::default() });
    let estimator = ReportingEstimator { inner: Gcc::new(gcc_initial_bps, GCC_MIN_BPS, GCC_MAX_BPS), rate: rate.clone(), epoch: None, one_way_floor: VecDeque::new(), arrivals: VecDeque::new(), lost_at: f64::NEG_INFINITY };
    let registry = configure_congestion_control(Registry::new(), estimator, CongestionFeedback::Twcc, &mut media_engine)?;
    let reports = Arc::new(SenderReports::default());
    let reader = SenderReportReader { reports: reports.clone(), read: VecDeque::new(), write: VecDeque::new() };
    let registry = register_default_interceptors(registry, &mut media_engine)?.with(Slot::Custom(14_000), KeyframeRequests::default())
        .with(Slot::Custom(14_001), reader)
        .with(Slot::Custom(15_000), RtpTap { side: TapSide::Enqueue, rate: rate.clone(), read: VecDeque::new(), write: VecDeque::new() })
        .with(Slot::Custom(2_500), RtpTap { side: TapSide::Depart, rate: rate.clone(), read: VecDeque::new(), write: VecDeque::new() })
        // (replaces configure_congestion_control's pacer, which logs that it did)
        .with(Slot::Pacer, crate::rtp_pacer::AgedPacer::new(rate.pacer.clone()));
    Ok((media_engine, registry, rate, reports))
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
    /// it has carried a playout delay outside low-latency rendering (see [`sent_playout_delay`])
    left_low_latency: AtomicBool,
    /// told when the track is dropped: its RTCP reader ends then, even while `poll` never returns
    dropped: Arc<tokio::sync::Notify>,
    /// the connection's video rates (and its RTP pacer's backlog)
    pub rate: Arc<VideoRate>,
    /// the next sample's media time on the track: its RTP timestamp less the first's, in ticks of `VIDEO_CLOCK_RATE`
    media_ticks: parking_lot::Mutex<u32>,
    /// when it last carried abs-capture-time
    capture_stamped_at: parking_lot::Mutex<Option<Instant>>,
}

/// A sample written to a track.
pub struct Written {
    /// its media time on the track, ms (see `MediaTrack::media_ticks`)
    pub media_time_ms: f64,
    /// its RTP timestamp, once the track's first packet was seen
    pub rtp_timestamp: Option<u32>,
}

impl Drop for MediaTrack {
    fn drop(&mut self) {
        self.rate.pacer.forget_track(self.ssrc);
        // (notify_one keeps the wake-up if the reader isn't waiting right now)
        self.dropped.notify_one();
    }
}

/// libwebrtc renders "as soon as decoded" (render time 0) while the playout delay is low-latency: min 0 and max at most
/// 500 ms (kLowLatencyStreamMaxPlayoutDelayThreshold), in 10 ms units.
const LOW_LATENCY_MAX_UNITS: u16 = 50;

/// The playout delay to put on a packet, given the one asked for and whether the track has left low-latency rendering.
/// A stream can't go back to it: Firefox stops showing a receiver's frames for good once their render time drops from
/// a real time to 0 (they decode, and the picture stays frozen), so after a non-zero minimum (High quality's
/// [100, 400] ms) a low-latency [0, x] goes out as [10 ms, max(x, 10 ms)], the nearest delay that keeps real render times.
fn sent_playout_delay((min, max): (u16, u16), left_low_latency: bool) -> (u16, u16) {
    let low_latency = min == 0 && max <= LOW_LATENCY_MAX_UNITS;
    if low_latency && left_low_latency {
        (1, max.max(1))
    } else {
        (min, max)
    }
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
    /// `capture_unix_ms`: the source sample's time, sent in the abs-capture-time extension (video only).
    /// `keyframe`: the RTP pacer lets its packets wait longer (`rtp_pacer::MAX_KEYFRAME_AGE`).
    pub async fn write(&self, data: Vec<u8>, duration: Duration, playout_delay: (u16, u16), capture_unix_ms: Option<f64>, keyframe: bool) -> Result<Written> {
        if keyframe {
            let first = self.rate.rtp.lock().get(&self.ssrc).and_then(|stream| stream.first_timestamp);
            self.rate.pacer.mark_keyframe(self.ssrc, first.map(|first| first.wrapping_add(*self.media_ticks.lock())));
        }
        let sample = Sample { data: Bytes::from(data), duration, ..Sample::new(Instant::now()) };
        let writer = self.track.sample_writer(self.ssrc, self.payload_type);
        let low_latency = playout_delay.0 == 0 && playout_delay.1 <= LOW_LATENCY_MAX_UNITS;
        if !low_latency && self.mime != OPUS {
            self.left_low_latency.store(true, Ordering::Relaxed);
        }
        let (min_delay, max_delay) = sent_playout_delay(playout_delay, self.left_low_latency.load(Ordering::Relaxed));
        let writer = if self.mime == OPUS { writer } else { writer.with_extension(HeaderExtension::PlayoutDelay(PlayoutDelayExtension { min_delay, max_delay })) };
        let stamp_due = {
            let mut stamped_at = self.capture_stamped_at.lock();
            let due = stamped_at.is_none_or(|at| at.elapsed() >= CAPTURE_TIME_INTERVAL);
            if due {
                *stamped_at = Some(Instant::now());
            }
            due
        };
        let writer = match capture_unix_ms {
            Some(capture_unix_ms) if self.mime != OPUS && stamp_due => writer.with_extension(HeaderExtension::Custom { uri: ABS_CAPTURE_TIME_URI.into(), extension: Box::new(AbsCaptureTime { capture_unix_ms }) }),
            _ => writer,
        };
        writer.write_sample(&sample).await?;
        // in RTP ticks as webrtc-rs counts them (each sample's duration truncated to whole ticks), so the sum doesn't drift
        let at = {
            let mut ticks = self.media_ticks.lock();
            let at = *ticks;
            *ticks = ticks.wrapping_add((duration.as_secs_f64() * VIDEO_CLOCK_RATE) as u32);
            at
        };
        let rtp_timestamp = self.rate.rtp.lock().get(&self.ssrc).and_then(|stream| stream.first_timestamp).map(|first| first.wrapping_add(at));
        Ok(Written { media_time_ms: at as f64 * 1000.0 / VIDEO_CLOCK_RATE, rtp_timestamp })
    }

    /// (p50, p95) ms the track's latest frames waited in the RTP pacer.
    pub fn pacer_delay_percentiles(&self) -> Option<(f64, f64)> {
        self.rate.rtp.lock().get(&self.ssrc).and_then(RtpStream::pacer_percentiles)
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
fn spawn_rtcp_reader(track: Arc<TrackLocalStaticSample>, dropped: Arc<tokio::sync::Notify>, keyframe_requested: Arc<AtomicBool>, keyframe_requests: Arc<AtomicU64>) {
    tokio::spawn(read_rtcp(move || {
        let track = track.clone();
        async move { track.poll().await }
    }, dropped, keyframe_requested, keyframe_requests));
}

/// [`spawn_rtcp_reader`]'s loop over `poll`; it ends when `dropped` is notified, whatever `poll` is doing, so the track
/// (held by `poll`) goes with its `MediaTrack`.
async fn read_rtcp<F: Future<Output = Option<TrackLocalEvent>>>(mut poll: impl FnMut() -> F, dropped: Arc<tokio::sync::Notify>, keyframe_requested: Arc<AtomicBool>, keyframe_requests: Arc<AtomicU64>) {
    loop {
        let event = tokio::select! {
            event = poll() => event,
            () = dropped.notified() => return,
        };
        match event {
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
            _ => tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(100)) => {}
                () = dropped.notified() => return,
            },
        }
    }
}

/// The mids of `sdp`'s m-sections still in use (port not 0): a subscription the page closed stopped its transceiver, and
/// the next offer rejects that m-line (port 0) or recycles it under a new mid.
pub fn live_mids(sdp: &str) -> std::collections::HashSet<String> {
    sdp.split("\nm=")
        .skip(1)
        .filter(|section| section.split_whitespace().nth(1).is_some_and(|port| port != "0"))
        .filter_map(|section| section.lines().find_map(|line| line.trim().strip_prefix("a=mid:")).map(str::to_owned))
        .collect()
}

/// Stops the transceivers of tracks the browser dropped: their sender's interceptor streams unbind (a 1024-packet NACK
/// buffer, ~1.2 MB a video track, and the congestion-control history) and the RTCP reader ends with the track. Never
/// stopped, each video resubscribe kept one until the page closed.
pub async fn stop_tracks(connection: &Arc<dyn PeerConnection>, dropped: &[Arc<MediaTrack>]) {
    if dropped.is_empty() {
        return;
    }
    for transceiver in connection.get_transceivers().await {
        let Ok(Some(mid)) = transceiver.mid().await else { continue };
        if dropped.iter().any(|track| track.mid == mid)
            && let Err(error) = transceiver.stop().await
        {
            log::warn!("stopping the transceiver of dropped track {mid}: {error}");
        }
    }
}

/// Applies a browser offer (renegotiation over `control`). With `add` (a track format), first adds
/// a track, which pairs with the offer's new recvonly m-line of its kind, and returns it with its mid.
pub async fn renegotiate(connection: &Arc<dyn PeerConnection>, offer: RTCSessionDescription, add: Option<&'static str>, rate: &Arc<VideoRate>) -> Result<(RTCSessionDescription, Option<Arc<MediaTrack>>)> {
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
    let dropped = Arc::new(tokio::sync::Notify::new());
    let video = Arc::new(MediaTrack { mid, mime, track: track.clone(), ssrc, payload_type, keyframe_requested, keyframe_requests: Arc::default(), owner: AtomicU64::new(0), left_low_latency: AtomicBool::new(false), dropped: dropped.clone(), rate: rate.clone(), media_ticks: Default::default(), capture_stamped_at: Default::default() });
    if mime != OPUS {
        rate.pacer.register_track(ssrc, video.keyframe_requested.clone());
    }
    spawn_rtcp_reader(track, dropped, video.keyframe_requested.clone(), video.keyframe_requests.clone());
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
/// `media_time_ms`: the frame's media time on its track (its RTP timestamp less the track's first, in ms), so a page can
/// tell which frame it is showing; `written_ms`: when it went to the track (unix ms, the gateway's clock).
/// Then `u32 rtpTimestamp` (valid when flags bit1 is set: a requestVideoFrameCallback `rtpTimestamp` names the frame
/// exactly) | `f64 receivedMs | f64 encodedMs`: when the gateway got the sample and finished encoding it (unix ms).
/// The frame header's timestamp is the sample's own (its zenoh timestamp, else `receivedMs`). With flags bit2, the
/// picture's message header follows ([`frame::encode_header`](crate::frame::encode_header)).
pub(crate) fn metadata(frame: &EncodedVideo, source: (u32, u32), quality: f64, written: &Written, times: FrameTimes, header: Option<(&MessageHeader, &crate::frame::SampleMeta)>) -> Vec<u8> {
    let mut out = vec![0u8; METADATA_LEN];
    out[28..36].copy_from_slice(&written.media_time_ms.to_le_bytes());
    out[36..44].copy_from_slice(&times.written_ms.to_le_bytes());
    out[44..48].copy_from_slice(&written.rtp_timestamp.unwrap_or(0).to_le_bytes());
    out[48..56].copy_from_slice(&times.received_ms.to_le_bytes());
    out[56..64].copy_from_slice(&times.encoded_ms.to_le_bytes());
    out[0] = 1;
    out[1] = frame.keyframe as u8 | if written.rtp_timestamp.is_some() { METADATA_FLAG_RTP } else { 0 };
    out[4..8].copy_from_slice(&frame.width.to_le_bytes());
    out[8..12].copy_from_slice(&frame.height.to_le_bytes());
    out[12..16].copy_from_slice(&source.0.to_le_bytes());
    out[16..20].copy_from_slice(&source.1.to_le_bytes());
    out[20..24].copy_from_slice(&(quality as f32).to_le_bytes());
    out[24..28].copy_from_slice(&(frame.data.len() as u32).to_le_bytes());
    if let Some(header) = header.and_then(|(header, sample)| crate::frame::encode_header(header, sample)) {
        out[1] |= METADATA_FLAG_HEADER;
        out.extend_from_slice(&header);
    }
    out
}

/// A video frame's gateway times (unix ms, the gateway's clock).
pub(crate) struct FrameTimes {
    pub(crate) received_ms: f64,
    pub(crate) encoded_ms: f64,
    pub(crate) written_ms: f64,
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

/// A decode's frame, whether it was another frontend's, the ms it took and the sample's identity; or why there is none.
/// Given up after `timeout` (`ServerBuilder::encode_timeout`): a hung decoder keeps only its blocking thread.
pub async fn finish_decode(task: tokio::task::JoinHandle<(DecodeOutcome, f64, u64)>, timeout: Duration) -> std::result::Result<(Arc<DecodedFrame>, bool, f64, u64), String> {
    match tokio::time::timeout(timeout, task).await {
        Ok(Ok((Ok((decoded, reused)), ms, hash))) => Ok((decoded, reused, ms, hash)),
        Ok(Ok((Err(error), _, _))) => Err(error),
        Ok(Err(error)) => Err(format!("decoder task failed: {error}")),
        Err(_) => Err(format!("decode took over {timeout:?}: skipped")),
    }
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

/// A session's picture keeps its size until the bitrate's size differs from it by this ratio (width): every new size
/// rebuilds a hardware encoder and costs a keyframe, and a size that followed each 250 ms grant rebuilt the R1's
/// nvv4l2h264enc 16,000 times in 12 h.
const SIZE_BAND: f64 = 1.1;
/// The picture shrinks at most this often, and grows only this long after its last change, except back to full size,
/// which it does at once (a subscription that started on a low grant stayed small for seconds) unless the quantizer
/// shrank it (taken at once, it flapped with the quantizer between 640 and 560 wide each second, a keyframe each).
const SIZE_DOWN_SETTLE: Duration = Duration::from_secs(1);
const SIZE_UP_SETTLE: Duration = Duration::from_secs(5);

/// (source size, size in use, when it was picked)
type SizeInUse = ((u32, u32), (u32, u32), Instant);

/// The size to encode at: `wanted` (the bitrate policy's) once it is far enough from the size in use and that size has
/// settled, else the size in use. A new source size, and full size (unless `qp_shrunk`), are taken at once.
/// `starting_up`: the link model is still looking for the link's bandwidth, so the picture grows at once (held back
/// `SIZE_UP_SETTLE`, video sent a third of its budget, the link model saw no growth and ended its startup at 93 KB/s on
/// a 1 MB/s link).
fn steady_size(current: Option<SizeInUse>, source: (u32, u32), wanted: (u32, u32), now: Instant, starting_up: bool, qp_shrunk: bool) -> ((u32, u32), bool) {
    let Some((_, size, changed_at)) = current.filter(|(current_source, _, _)| *current_source == source) else {
        return (wanted, true);
    };
    let ratio = wanted.0 as f64 / size.0.max(1) as f64;
    let since = now.duration_since(changed_at);
    let change = (ratio < 1.0 / SIZE_BAND && since >= SIZE_DOWN_SETTLE) || (ratio > SIZE_BAND && (starting_up || since >= SIZE_UP_SETTLE))
        || (!qp_shrunk && wanted.0 >= source.0 & !1 && size.0 < source.0 & !1);
    if change { (wanted, true) } else { (size, false) }
}

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
    /// each member's (bits/s, frames/s, bits/s its picture is sized for, whether its link model is starting up)
    grants: Mutex<HashMap<u64, (f64, f64, f64, bool)>>,
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
    /// (source, size in use, when it was picked): see `steady_size`
    size: Option<SizeInUse>,
    qp: QpScaler,
    /// the size in use is the quantizer's, under the bitrate's (see `steady_size`)
    qp_shrunk: bool,
}

/// Sizes the picture by the encoder's quantizer, as libwebrtc's quality scaler does, but by an equation instead of its
/// thresholds and steps: H.264's bits per frame halve each +6 QP, so at the bitrate now the picture that reaches
/// `VideoPolicy::target_qp` has `pixels x (bitrate now / bitrate then) x 2^((target - QP) / 6)`, from the average QP of
/// the last `SIZE_DOWN_SETTLE` of delta frames at the size in use. A picture too big for its bitrate (openh264 then
/// skips frames, as at 0.1 bit/pixel on a 500 kbit/s link: 2 of 3) shrinks; one that compresses well grows back up to
/// the size its bitrate affords (`video::target`). A coarse quantizer shrinks the picture only while the encoder spends
/// its bitrate: one with bits to spare (a still picture, nothing to code) isn't short of them.
#[derive(Default)]
struct QpScaler {
    /// (when, QP, bits, bits/s the picture was sized for, bits/s the encoder was given) of each delta frame at `size`
    frames: VecDeque<(Instant, f64, f64, f64, f64)>,
    size: (u32, u32),
    /// when its first frame at `size` came
    since: Option<Instant>,
}

impl QpScaler {
    fn observe(&mut self, now: Instant, size: (u32, u32), qp: f64, bits: f64, size_bitrate: f64, bitrate: f64) {
        if size != self.size {
            self.frames.clear();
            self.size = size;
            self.since = Some(now);
        }
        self.frames.push_back((now, qp, bits, size_bitrate, bitrate));
        while self.frames.front().is_some_and(|(at, ..)| now.duration_since(*at) > SIZE_DOWN_SETTLE) {
            self.frames.pop_front();
        }
    }

    /// The share of `source`'s width and height the picture should take at `bitrate`, once a frame was measured.
    fn scale(&self, source: (u32, u32), bitrate: f64, target_qp: f64) -> Option<f64> {
        if self.frames.is_empty() || source.0 == 0 {
            return None;
        }
        let count = self.frames.len() as f64;
        let qp = self.frames.iter().map(|(_, qp, ..)| qp).sum::<f64>() / count;
        let then = self.frames.iter().map(|(_, _, _, size_bitrate, _)| size_bitrate).sum::<f64>() / count;
        let given = self.frames.iter().map(|(.., bitrate)| bitrate).sum::<f64>() / count;
        let window = self.since.map_or(SIZE_DOWN_SETTLE, |since| self.frames.back().map_or(SIZE_DOWN_SETTLE, |(last, ..)| last.duration_since(since)).min(SIZE_DOWN_SETTLE));
        let spent = self.frames.iter().map(|(_, _, bits, ..)| bits).sum::<f64>() / window.as_secs_f64().max(f64::EPSILON);
        let qp_factor = 2f64.powf((target_qp - qp) / 6.0);
        let qp_factor = if spent < given { qp_factor.max(1.0) } else { qp_factor };
        let pixels = self.size.0 as f64 * self.size.1 as f64 * (bitrate / then.max(1.0)) * qp_factor;
        Some((pixels / (source.0 as f64 * source.1 as f64)).sqrt())
    }
}

struct SessionFrame {
    seq: u64,
    frame: EncodedVideo,
    source: (u32, u32),
    timestamp_ms: f64,
    /// when the gateway got its sample, and when its encode finished (unix ms, the gateway's clock)
    received_ms: f64,
    encoded_ms: f64,
    encoded_by: u64,
    /// its picture's message header, if the encoding gave one
    header: Option<MessageHeader>,
    /// its sample's zenoh encoding and attachment, when they say something
    sample: crate::frame::SampleMeta,
}

impl EncodeSession {
    /// Whether a member granted `bitrate` can share this session: the others' grants and its own are close.
    fn fits(&self, member: u64, bitrate: f64) -> bool {
        let grants = self.grants.lock();
        let others = grants.iter().filter(|(id, _)| **id != member).map(|(_, (bps, _, _, _))| *bps);
        let (low, high) = others.fold((bitrate, bitrate), |(low, high), bps| (low.min(bps), high.max(bps)));
        high <= low.max(1.0) * SHARE_RATIO
    }
}

impl VideoSessions {
    /// The session `member` encodes in at `grant`: `current` while it still fits (unless it is alone there and an older
    /// session of the same stream and policy fits too), else the oldest such session that fits, else a new one.
    fn place(&self, current: Option<Arc<EncodeSession>>, member: u64, (codec, format, key): (&str, VideoFormat, &str), policy: VideoPolicy, grant: (f64, f64, f64, bool)) -> Arc<EncodeSession> {
        let mut sessions = self.sessions.lock();
        let same_stream = |session: &&Arc<EncodeSession>| session.codec == codec && session.format == format && session.key == key && session.policy == policy;
        let oldest_fit = sessions.iter().filter(same_stream).filter(|session| session.fits(member, grant.0)).min_by_key(|session| session.id).cloned();
        let session = match (current, oldest_fit) {
            // the policy changes when the subscription is updated (updateSubscription)
            (Some(current), oldest) if current.policy == policy && current.fits(member, grant.0) && (current.grants.lock().len() > 1 || oldest.as_ref().is_none_or(|oldest| oldest.id >= current.id)) => current,
            (current, oldest) => {
                if let Some(current) = current {
                    Self::leave_locked(&mut sessions, &current, member);
                }
                oldest.filter(|oldest| sessions.iter().any(|session| Arc::ptr_eq(session, oldest))).unwrap_or_else(|| {
                    let state = SessionState { encoder: None, governor: CpuGovernor::new(), log: VecDeque::new(), next_seq: 0, last_input: None, keyframe_wanted: true, source: (640, 480), scale: 1.0, size: None, qp: QpScaler::default(), qp_shrunk: false };
                    let id = self.next_session.fetch_add(1, Ordering::Relaxed);
                    let session = Arc::new(EncodeSession { id, codec: codec.to_owned(), format, key: key.to_owned(), policy, grants: Mutex::default(), state: tokio::sync::Mutex::new(state) });
                    sessions.push(session.clone());
                    session
                })
            }
        };
        session.grants.lock().insert(member, grant);
        session
    }

    fn leave(&self, session: &Arc<EncodeSession>, member: u64) {
        Self::leave_locked(&mut self.sessions.lock(), session, member);
    }

    fn leave_locked(sessions: &mut Vec<Arc<EncodeSession>>, session: &Arc<EncodeSession>, member: u64) {
        let mut grants = session.grants.lock();
        grants.remove(&member);
        if grants.is_empty() {
            sessions.retain(|other| !Arc::ptr_eq(other, session));
        }
    }

    #[cfg(test)]
    fn count(&self) -> usize {
        self.sessions.lock().len()
    }
}

/// One viewer's place in its session: the next frame it hasn't sent, and whether it has had a keyframe there.
struct Member {
    id: u64,
    session: Option<Arc<EncodeSession>>,
    next_seq: Option<u64>,
    synced: bool,
    /// leaves `session` when dropped: a send loop that panicked or was cancelled kept its encoder (and its grant) forever
    registry: Option<Arc<crate::encoding::registry::EncodingRegistry>>,
}

impl Drop for Member {
    fn drop(&mut self) {
        if let (Some(registry), Some(session)) = (&self.registry, self.session.take()) {
            registry.video_sessions.leave(&session, self.id);
        }
    }
}

/// A picked, decoded sample to encode.
struct Input {
    decoded: Arc<DecodedFrame>,
    timestamp_ms: f64,
    received_ms: f64,
    hash: u64,
    quality: f64,
    keyframe_requested: bool,
    /// the sample's zenoh encoding and attachment, when they say something
    sample: crate::frame::SampleMeta,
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
        let (is_picture, header) = if let DecodedFrame::Video(image) = &*input.decoded {
            state.source = (image.width(), image.height());
            (true, image.message_header().cloned())
        } else {
            (false, None)
        };
        let grants: Vec<(f64, f64, f64, bool)> = session.grants.lock().values().copied().collect();
        let starting_up = grants.iter().any(|grant| grant.3);
        let bitrate = grants.iter().map(|grant| grant.0).fold(f64::INFINITY, f64::min);
        let size_bitrate = grants.iter().map(|grant| grant.2).fold(f64::INFINITY, f64::min);
        let hz = grants.iter().map(|grant| grant.1).fold(0.0, f64::max);
        // (only while bandwidth is short: granted its whole demand, the policy's bits per pixel already set the quality at
        // full size, and a coarse quantizer shrank a 20 Mbit/s link's picture to 1184x666)
        let most = session.policy.frame_bytes(state.source, hz.max(0.1), 1.0) * 8.0 * hz.max(0.1);
        let qp_scale = (bitrate < most).then(|| state.qp.scale(state.source, size_bitrate, session.policy.target_qp)).flatten();
        let (scale, source) = (state.scale, state.source);
        let floor = session.policy.floor_scale(source);
        out.scale_cap = state.governor.scale_cap(scale, floor, hz, Instant::now());
        let mut target = target(&session.policy, source, input.quality, bitrate, size_bitrate, qp_scale, hz, out.scale_cap, state.keyframe_wanted);
        let now = Instant::now();
        let (size, changed) = steady_size(state.size, source, (target.width, target.height), now, starting_up, state.qp_shrunk);
        if changed {
            // the quantizer set it when its size is under the bitrate's
            let by_bitrate = session.policy.size(source, size_bitrate.max(1.0), hz.max(0.1), out.scale_cap);
            state.qp_shrunk = size.0 < by_bitrate.0;
            state.size = Some((source, size, now));
        }
        (target.width, target.height) = size;
        // (made on the blocking pool too: a hardware encoder's setup can block)
        let (existing, format, timeout) = (state.encoder.take(), session.format, codecs.encode_timeout);
        let decoded = input.decoded.clone();
        let encoding = tokio::task::spawn_blocking(move || {
            let mut encoder = existing.map_or_else(|| codecs.video_encoder(&*codec, format), Ok)?;
            let started = Instant::now();
            let result = encoder.encode(&decoded, &target).map_err(|error| format!("{error:#}"));
            let qp = encoder.last_qp();
            Ok::<_, String>((encoder, result, started.elapsed().as_secs_f64() * 1000.0, qp))
        });
        // an encode that hangs gives up its encoder (rebuilt next frame, from a keyframe) and the session's lock, which
        // every viewer of the session waits on
        let Ok(encoded) = tokio::time::timeout(timeout, encoding).await else {
            state.keyframe_wanted = true;
            out.error = Some(format!("video encode took over {timeout:?}: the encoder is rebuilt"));
            return out;
        };
        match encoded {
            Ok(Err(error)) => out.error = Some(error),
            Ok(Ok((encoder, result, encode_ms, qp))) => {
                state.encoder = Some(encoder);
                state.governor.observe_encode(encode_ms);
                out.encode_ms = state.governor.encode_ms;
                match result {
                    Ok(Some(frame)) => {
                        // (a keyframe's quantizer says nothing about what a frame of this picture needs)
                        if !frame.keyframe
                            && let Some(qp) = qp
                        {
                            state.qp.observe(Instant::now(), (frame.width, frame.height), qp, frame.data.len() as f64 * 8.0, size_bitrate, bitrate);
                        }
                        if !is_picture {
                            state.source = (frame.width, frame.height);
                        }
                        state.scale = frame.width as f64 / state.source.0.max(1) as f64;
                        state.keyframe_wanted &= !frame.keyframe;
                        let seq = state.next_seq;
                        state.next_seq += 1;
                        let logged = SessionFrame { seq, source: state.source, frame, timestamp_ms: input.timestamp_ms, received_ms: input.received_ms, encoded_ms: crate::subscription::now_unix_ms(), encoded_by: member.id, header, sample: input.sample.clone() };
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

/// Most samples a video stream decodes ahead of the one it encodes, each on its own blocking thread.
const MAX_DECODE_AHEAD: usize = 4;

/// Decodes to keep in flight ahead of the frame being encoded: one (overlapping the encode) while the encode is the
/// slower stage, more while a decode takes longer than an encode, so that the stream's rate is not one core's decode
/// rate (a 1080p q95 jpeg took ~38 ms on a Jetson Orin core against ~1 ms of hardware encode: at most ~26 fps).
fn decode_ahead(decode_ms: Option<f64>, encode_ms: Option<f64>) -> usize {
    match (decode_ms, encode_ms) {
        (Some(decode_ms), Some(encode_ms)) => ((decode_ms / encode_ms.max(1.0)).ceil() as usize).clamp(1, MAX_DECODE_AHEAD),
        _ => 1,
    }
}

/// Awaits `work` while starting decodes of samples as they may go (paced by `pick`), up to `limit` waiting in `ahead`.
async fn picking_while<T>(work: impl Future<Output = T>, ahead: &mut VecDeque<Decoding>, limit: usize, shared: &SubShared, codec: &Arc<dyn MessageEncoding>) -> T {
    tokio::pin!(work);
    loop {
        if ahead.len() >= limit {
            return work.await;
        }
        let (picked, wake_at) = shared.pick(Instant::now());
        if let Some((key, item)) = picked {
            ahead.push_back(start_decode(shared, codec, key, item));
            continue;
        }
        tokio::select! {
            output = &mut work => return output,
            _ = shared.wait_for_data(wake_at) => {}
        }
    }
}

/// Sends a video subscription's frames: pick (paced), decode (shared across frontends), encode in the encode session
/// of its grant (shared by viewers at that grant, off the runtime), write to the track. Later samples decode while this
/// one decodes and encodes ([`decode_ahead`]), in order, so the slower stage's throughput sets the rate, not the two
/// in series (a 1920x1536 jpeg on a Jetson Orin core, ~16 ms + ~20 ms, could not keep up with 30 Hz) nor one decode.
pub async fn send_loop(dc: Arc<dyn DataChannel>, shared: Arc<SubShared>, track: TrackClaim) {
    let Some(codec) = shared.codec.clone() else { return };
    let sessions = &shared.codecs.video_sessions;
    let mut member = Member { id: sessions.next_member.fetch_add(1, Ordering::Relaxed), session: None, next_seq: None, synced: false, registry: Some(shared.codecs.clone()) };
    let mut next_frame_id: u32 = 0;
    let mut last_write: Option<Instant> = None;
    let mut warned_write = false;
    let mut ahead: VecDeque<Decoding> = VecDeque::new();
    let (mut decode_ms, mut encode_ms): (Option<f64>, Option<f64>) = (None, None);
    // one frame at the granted rate: what may still wait in the RTP pacer when the next frame is encoded
    let mut frame_interval = Duration::ZERO;
    while !shared.is_closed() && track.is_current() {
        // nothing new is decoded or encoded while what was written would still be in the pacer when the next frame is
        // due: the newest sample goes once it has drained (dropping encoded frames instead would break the picture until a keyframe)
        let behind = track.rate.pacer_delay();
        if ahead.is_empty() && behind > frame_interval {
            shared.record_pacer_wait();
            // (looked at again each frame: the pacer's rate can change meanwhile)
            tokio::time::sleep((behind - frame_interval).min(frame_interval.max(Duration::from_millis(1)))).await;
            continue;
        }
        let decoding = match ahead.pop_front() {
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
        let depth = decode_ahead(decode_ms, encode_ms);
        let finished = picking_while(finish_decode(task, shared.codecs.encode_timeout), &mut ahead, depth - 1, &shared, &codec).await;
        let (decoded, hash) = match finished {
            Ok((decoded, reused, ms, hash)) => {
                if !reused {
                    decode_ms = Some(ewma(decode_ms, ms));
                }
                (decoded, hash)
            }
            Err(error) => {
                shared.record_encoding_error(&error);
                continue;
            }
        };
        let source = match &*decoded {
            DecodedFrame::Video(image) => (image.width(), image.height()),
            // (a frame the encoder renders itself: its size once it has encoded one, no floor before)
            _ => shared.video_source().unwrap_or_default(),
        };
        let (quality, bitrate, hz) = shared.video_grant(&key, source);
        frame_interval = crate::secs(1.0 / hz.max(0.1));
        let previous = member.session.take();
        let Channel::Video(format) = shared.channel else { return };
        let session = sessions.place(previous.clone(), member.id, (codec.name(), format, &key), shared.tuning().video_policy, (bitrate, hz, bitrate * track.rate.size_ratio(), track.rate.starting_up()));
        if previous.is_none_or(|previous| !Arc::ptr_eq(&previous, &session)) {
            (member.next_seq, member.synced) = (None, false);
        }
        member.session = Some(session.clone());
        let input = Input { decoded, timestamp_ms: item.timestamp_ms, received_ms: item.received_ms, hash, quality, keyframe_requested: track.keyframe_requested.swap(false, Ordering::AcqRel), sample: subscription::sample_meta(&item) };
        let stepping = step(session, &mut member, input, shared.codecs.clone(), codec.clone());
        let outcome = picking_while(stepping, &mut ahead, depth, &shared, &codec).await;
        encode_ms = outcome.encode_ms.or(encode_ms);
        if let Some(error) = &outcome.error {
            shared.record_encoding_error(error);
        }
        shared.record_video_timing(decode_ms, outcome.encode_ms, outcome.scale_cap);
        for logged in outcome.frames {
            let frame = &logged.frame;
            shared.note_video_source(logged.source.0, logged.source.1);
            let now = Instant::now();
            let duration = last_write.map_or(crate::secs(1.0 / hz.max(0.1)), |previous| now.duration_since(previous).max(Duration::from_millis(1)));
            last_write = Some(now);
            if !track.is_current() {
                break;
            }
            let written = match track.write(frame.data.clone(), duration, shared.tuning().playout_delay, Some(logged.timestamp_ms), frame.keyframe).await {
                Ok(written) => written,
                Err(error) => {
                    if !warned_write {
                        log::warn!("video track {}: write failed: {error:#}", track.mid);
                        warned_write = true;
                    }
                    shared.record_encoding_error(&format!("track write: {error:#}"));
                    continue;
                }
            };
            let times = FrameTimes { received_ms: logged.received_ms, encoded_ms: logged.encoded_ms, written_ms: crate::subscription::now_unix_ms() };
            shared.record_video_latency(times.received_ms - logged.timestamp_ms, times.encoded_ms - times.received_ms, times.written_ms - times.encoded_ms, track.pacer_delay_percentiles());
            let meta = metadata(frame, logged.source, quality, &written, times, logged.header.as_ref().filter(|_| shared.sends_header()).map(|header| (header, &logged.sample)));
            let shared_encode = logged.encoded_by != member.id;
            shared.record_video_frame(frame.data.len(), (frame.width, frame.height), quality, frame.keyframe, shared_encode, track.keyframe_requests.load(Ordering::Relaxed));
            if subscription::send_small_frame(&dc, &key, logged.timestamp_ms, item.seq, next_frame_id, 0, &meta).await.is_err() && shared.is_closed() {
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
    #[test]
    fn abs_capture_time_is_ntp_with_a_zero_offset() {
        use rtc::shared::marshal::Marshal;
        let mut out = [0xffu8; 16];
        // 2024-01-01T00:00:00.5Z: 1_704_067_200.5 unix seconds
        let written = super::AbsCaptureTime { capture_unix_ms: 1_704_067_200_500.0 }.marshal_to(&mut out).unwrap();
        assert_eq!(written, 16);
        let ntp = u64::from_be_bytes(out[0..8].try_into().unwrap());
        assert_eq!(ntp >> 32, 1_704_067_200 + super::NTP_UNIX_OFFSET_SECS, "whole seconds since 1900");
        assert_eq!(ntp & 0xffff_ffff, 1 << 31, "half a second");
        assert_eq!(&out[8..16], &[0; 8], "capture clock offset 0");
    }

    #[test]
    fn a_frame_s_pacer_delay_is_first_packet_in_to_marker_out() {
        use super::{RtpStream, TapSide::*};
        use std::time::{Duration, Instant};
        let (mut stream, t0) = (RtpStream::default(), Instant::now());
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        // frame 1000: two packets in at 0 ms, out at 5 and 40 ms; frame 4000 in at 10, out at 60
        stream.observe(Enqueue, 1000, false, at(0));
        stream.observe(Enqueue, 1000, true, at(0));
        stream.observe(Enqueue, 4000, true, at(10));
        stream.observe(Depart, 1000, false, at(5));
        stream.observe(Depart, 1000, true, at(40));
        // a retransmission of frame 1000's marker packet leaves later: not a new delay, nothing popped
        stream.observe(Depart, 1000, true, at(55));
        assert_eq!(stream.enqueued.len(), 1, "frame 4000 still awaited");
        stream.observe(Depart, 4000, true, at(60));
        assert_eq!(stream.first_timestamp, Some(1000));
        assert_eq!(stream.pacer_ms.iter().map(|ms| ms.round() as u64).collect::<Vec<_>>(), vec![40, 50]);
        assert_eq!(stream.pacer_percentiles().map(|(p50, p95)| (p50.round(), p95.round())), Some((50.0, 50.0)));
    }

    #[test]
    fn a_rejected_or_recycled_m_line_is_not_live() {
        let sdp = "v=0\r\no=- 1 2 IN IP4 0.0.0.0\r\ns=-\r\nm=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\na=mid:0\r\nm=video 0 UDP/TLS/RTP/SAVPF 96\r\na=mid:1\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:3\r\n";
        let live = super::live_mids(sdp);
        assert!(live.contains("0") && live.contains("3"), "{live:?}");
        assert!(!live.contains("1"), "port 0: rejected");
        assert!(!live.contains("2"), "absent: recycled under a new mid");
    }

    #[tokio::test]
    async fn the_rtcp_reader_ends_when_its_track_drops_though_poll_never_returns() {
        use std::sync::Arc;
        let dropped = Arc::new(tokio::sync::Notify::new());
        // stands in for the track `poll` holds
        let held = Arc::new(());
        let reader = tokio::spawn(super::read_rtcp(
            {
                let held = held.clone();
                move || {
                    let held = held.clone();
                    async move {
                        let _held = held;
                        std::future::pending::<Option<webrtc::media_stream::track_local::TrackLocalEvent>>().await
                    }
                }
            },
            dropped.clone(),
            Arc::default(),
            Arc::default(),
        ));
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!reader.is_finished());
        // what MediaTrack's Drop does
        dropped.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), reader).await.expect("the reader ends").unwrap();
        assert_eq!(Arc::strong_count(&held), 1, "and lets go of the track");
    }

    #[test]
    fn the_picture_keeps_its_size_through_small_and_quick_changes() {
        use super::{SIZE_DOWN_SETTLE, SIZE_UP_SETTLE, steady_size};
        let (source, t0) = ((1920, 1536), std::time::Instant::now());
        let at = |size: (u32, u32), after: std::time::Duration| Some((source, size, t0 - after));
        assert_eq!(steady_size(None, source, (1360, 1088), t0, false, false), ((1360, 1088), true), "the first frame takes its size");
        let settled = SIZE_UP_SETTLE * 2;
        assert_eq!(steady_size(at((1360, 1088), settled), source, (1312, 1050), t0, false, false).0, (1360, 1088), "a few percent smaller: kept");
        assert_eq!(steady_size(at((1360, 1088), settled), source, (1440, 1152), t0, false, false).0, (1360, 1088), "a few percent bigger: kept");
        assert_eq!(steady_size(at((1360, 1088), settled), source, (960, 768), t0, false, false), ((960, 768), true), "much smaller: shrinks");
        assert_eq!(steady_size(at((1360, 1088), SIZE_DOWN_SETTLE / 2), source, (960, 768), t0, false, false).0, (1360, 1088), "not twice a second");
        assert_eq!(steady_size(at((960, 768), SIZE_DOWN_SETTLE * 2), source, (1360, 1088), t0, false, false).0, (960, 768), "grows only after it settled");
        assert_eq!(steady_size(at((960, 768), SIZE_DOWN_SETTLE * 2), source, (1360, 1088), t0, true, false).0, (1360, 1088), "or at once while the link model starts up");
        assert_eq!(steady_size(at((960, 768), settled), source, (1360, 1088), t0, false, false).0, (1360, 1088));
        assert_eq!(steady_size(at((1792, 1434), settled), source, (1920, 1536), t0, false, false).0, (1920, 1536), "back to full size");
        assert_eq!(steady_size(at((960, 768), SIZE_DOWN_SETTLE / 2), source, (1920, 1536), t0, false, false).0, (1920, 1536), "full size at once");
        assert_eq!(steady_size(at((960, 768), SIZE_DOWN_SETTLE / 2), source, (1920, 1536), t0, false, true).0, (960, 768), "not after the quantizer shrank it");
        assert_eq!(steady_size(Some(((640, 480), (320, 240), t0)), source, (1920, 1536), t0, false, false), ((1920, 1536), true), "a new source: at once");
    }

    #[test]
    fn playout_delay_never_returns_to_low_latency() {
        use super::sent_playout_delay;
        assert_eq!(sent_playout_delay((0, 0), false), (0, 0), "low latency from the start: as asked");
        assert_eq!(sent_playout_delay((0, 40), false), (0, 40));
        assert_eq!(sent_playout_delay((10, 40), true), (10, 40), "outside low latency: as asked");
        assert_eq!(sent_playout_delay((0, 60), true), (0, 60), "min 0 but max past 500 ms isn't low latency");
        assert_eq!(sent_playout_delay((0, 0), true), (1, 1), "back to [0, 0] after a non-zero min: [10, 10] ms");
        assert_eq!(sent_playout_delay((0, 30), true), (1, 30));
    }

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
        let mut viewers: Vec<Member> = (0..3).map(|id| Member { id, session: None, next_seq: None, synced: false, registry: None }).collect();
        let grants = [(4e6, 30.0, 4e6, false), (4.4e6, 30.0, 4.4e6, false), (1e6, 30.0, 1e6, false)];
        let mut sent = vec![Vec::new(); 3];
        for sample in 0..10u64 {
            for (index, viewer) in viewers.iter_mut().enumerate() {
                let session = sessions.place(viewer.session.take(), viewer.id, ("camera", VideoFormat::H264, "cam0"), VideoPolicy::default(), grants[index]);
                viewer.session = Some(session.clone());
                let input = Input { decoded: picture.clone(), timestamp_ms: sample as f64 * 33.0, received_ms: sample as f64 * 33.0, hash: sample, quality: 1.0, keyframe_requested: false, sample: None };
                let step = step(session, viewer, input, codecs.clone(), codec.clone()).await;
                sent[index].extend(step.frames.iter().map(|frame| (frame.seq, frame.frame.keyframe, frame.encoded_by)));
            }
        }
        assert_eq!(sessions.count(), 2, "4 and 4.4 Mbit/s share one session, 1 Mbit/s has its own");
        // a viewer that started alone (its first grant was a guess) joins the older session once its grant fits there
        let late = sessions.place(None, 9, ("camera", VideoFormat::H264, "cam0"), VideoPolicy::default(), (16e6, 30.0, 16e6, false));
        assert_eq!(sessions.count(), 3);
        let merged = sessions.place(Some(late), 9, ("camera", VideoFormat::H264, "cam0"), VideoPolicy::default(), (4.2e6, 30.0, 4.2e6, false));
        assert!(Arc::ptr_eq(&merged, viewers[0].session.as_ref().unwrap()) && sessions.count() == 2);
        sessions.leave(&merged, 9);
        assert_eq!(encodes.load(Ordering::Relaxed), 20, "10 samples, encoded once per session");
        assert_eq!(sent[0], sent[1], "both viewers send the same frames, in order");
        assert!(sent[0][0].1 && sent[2][0].1, "each viewer starts at a keyframe");
        assert_eq!(sent[1].iter().filter(|frame| frame.2 == 0).count(), 10, "viewer 1 sent the frames viewer 0 encoded");
        // a viewer whose grant moves away leaves for a session of its own, from a keyframe
        let session = sessions.place(viewers[1].session.take(), 1, ("camera", VideoFormat::H264, "cam0"), VideoPolicy::default(), (12e6, 30.0, 12e6, false));
        assert!(!Arc::ptr_eq(&session, viewers[0].session.as_ref().unwrap()));
        assert_eq!(sessions.count(), 3);
        for viewer in viewers.iter().filter(|viewer| viewer.session.is_some()) {
            sessions.leave(viewer.session.as_ref().unwrap(), viewer.id);
        }
        sessions.leave(&session, 1);
        assert_eq!(sessions.count(), 0, "a session ends with its last viewer");
    }

    #[tokio::test]
    async fn a_picture_s_header_rides_its_frame_s_metadata() {
        let codec: Arc<dyn MessageEncoding> = Arc::new(Camera(Arc::default()));
        let codecs = Arc::new(EncodingRegistry::new([codec.clone()]).unwrap());
        let mut viewer = Member { id: 0, session: None, next_seq: None, synced: false, registry: None };
        let mut sent = Vec::new();
        for (sample, frame_id) in [(0u64, "cam_optical"), (1, "cam_optical"), (2, "cam2_optical")] {
            let picture = VideoImage::rgb8(64, 48, vec![0; 64 * 48 * 3]).unwrap().with_header(100.0 + sample as f64 * 0.5, frame_id);
            let session = codecs.video_sessions.place(viewer.session.take(), 0, ("camera", VideoFormat::H264, "cam0"), VideoPolicy::default(), (4e6, 30.0, 4e6, false));
            viewer.session = Some(session.clone());
            let input = Input { decoded: Arc::new(DecodedFrame::Video(picture)), timestamp_ms: sample as f64 * 33.0, received_ms: sample as f64 * 33.0, hash: sample, quality: 1.0, keyframe_requested: false, sample: Some(("image/raw".into(), Some(vec![9]))) };
            sent.extend(step(session, &mut viewer, input, codecs.clone(), codec.clone()).await.frames);
        }
        codecs.video_sessions.leave(viewer.session.as_ref().unwrap(), 0);
        let headers: Vec<_> = sent.iter().map(|frame| frame.header.as_ref().map(|header| (header.stamp(), header.frame_id.clone())).unwrap()).collect();
        assert_eq!(headers, vec![(100.0, "cam_optical".into()), (100.5, "cam_optical".into()), (101.0, "cam2_optical".into())], "each frame its own picture's");
        let times = || super::FrameTimes { received_ms: 1.0, encoded_ms: 2.0, written_ms: 3.0 };
        let written = super::Written { media_time_ms: 0.0, rtp_timestamp: Some(90) };
        let mut bare = super::metadata(&sent[2].frame, (64, 48), 1.0, &written, times(), None);
        let header = sent[2].header.as_ref().map(|header| (header, &sent[2].sample));
        assert_eq!((bare.len(), bare[1] & super::METADATA_FLAG_HEADER), (super::METADATA_LEN, 0), "no header: the 64 bytes as before");
        let with = super::metadata(&sent[2].frame, (64, 48), 1.0, &written, times(), header);
        bare[1] |= super::METADATA_FLAG_HEADER;
        assert_eq!(&with[..super::METADATA_LEN], &bare[..], "the first 64 bytes unchanged but the flag");
        assert_eq!(f64::from_le_bytes(with[64..72].try_into().unwrap()), 101.0);
        assert_eq!((u16::from_le_bytes([with[72], with[73]]), &with[74..86]), (12, &b"cam2_optical"[..]), "0.5.31's layout first");
        assert_eq!(crate::frame::decode_header(&with[64..]), Some((sent[2].header.clone().unwrap(), Some(("image/raw".into(), Some(vec![9]))))), "then the exact stamp, seq and the sample's encoding and attachment");
    }
}
