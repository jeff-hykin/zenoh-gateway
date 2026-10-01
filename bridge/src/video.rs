//! Video subscriptions: frames decoded by a codec, encoded to H.264 and written to a WebRTC
//! video track (one track per frontend and subscription). The browser adds a recvonly
//! transceiver and renegotiates over `control`; the bridge answers with a new track bound to that
//! transceiver's mid. A subscription's `sub` data channel names the mid in its label and carries a
//! small metadata frame per video frame. Tracks are reused by later subscriptions on the same mid.

use crate::codec::registry;
use crate::codec::video::{EncodedFrame, VideoEncoder};
use crate::codec::{Codec, CodecSample, DecodedFrame};
use crate::subscription::{self, SubShared};
use anyhow::{Context, Result};
use bytes::Bytes;
use rtc::interceptor::{BandwidthEstimator, EstimatorStats, Gcc, PacketReport, Registry};
use rtc::media::Sample;
use rtc::media_stream::MediaStreamTrack;
use rtc::peer_connection::configuration::interceptor_registry::{CongestionFeedback, configure_congestion_control, register_default_interceptors};
use rtc::peer_connection::configuration::media_engine::{MIME_TYPE_H264, MediaEngine};
use rtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest;
use rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
use rtc::rtp_transceiver::rtp_sender::{RTCPFeedback, RTCRtpCodec, RTCRtpCodecParameters, RTCRtpCodingParameters, RTCRtpEncodingParameters, RtpCodecKind};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use webrtc::data_channel::DataChannel;
use webrtc::media_stream::track_local::static_sample::TrackLocalStaticSample;
use webrtc::media_stream::track_local::{TrackLocal, TrackLocalEvent};
use webrtc::peer_connection::{PeerConnection, RTCSessionDescription};
use webrtc::rtp_transceiver::RtpSender;

const H264_PAYLOAD_TYPE: u8 = 102;
/// Google Congestion Control bounds, bits/s. It starts where the data-channel estimator starts
/// (`allocator::INITIAL_ESTIMATE`, 1 MB/s), since video is allocated no more than this estimate.
const GCC_INITIAL_BPS: f64 = crate::allocator::INITIAL_ESTIMATE * 8.0;
const GCC_MIN_BPS: f64 = 50_000.0;
const GCC_MAX_BPS: f64 = 50_000_000.0;
/// The pacer's rate as a multiple of the GCC estimate (see `ReportingEstimator::target_bitrate`).
const PACING_FACTOR: f64 = 2.5;
/// Video metadata frame on the `sub` channel (SPEC "Wire formats").
pub const METADATA_LEN: usize = 28;

fn h264_codec() -> RTCRtpCodec {
    let feedback = |typ: &str, parameter: &str| RTCPFeedback { typ: typ.to_owned(), parameter: parameter.to_owned() };
    RTCRtpCodec {
        mime_type: MIME_TYPE_H264.to_owned(),
        clock_rate: 90_000,
        channels: 0,
        // constrained baseline, what openh264 produces and every browser decodes
        sdp_fmtp_line: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f".to_owned(),
        rtcp_feedback: vec![feedback("ccm", "fir"), feedback("nack", ""), feedback("nack", "pli")],
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

    /// What the pacer drains at: a multiple of the estimate, as libwebrtc paces (its default pace
    /// multiplier is 2.5). The pacer smooths bursts; it is not the rate limit — the allocator keeps
    /// the encoders near the estimate itself. Paced at exactly the estimate, every keyframe or
    /// overshoot queued behind it and that queue only drained as fast as the estimate grew, which
    /// held video seconds behind its data channel after a subscription started.
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

/// H.264 + RTCP reports + NACK + send-side congestion control (TWCC feedback into GCC).
/// Returns the GCC target (f64 bits/s as bits in an AtomicU64).
pub fn media_setup() -> Result<(MediaEngine, Registry, Arc<AtomicU64>)> {
    let mut media_engine = MediaEngine::default();
    media_engine.register_codec(RTCRtpCodecParameters { rtp_codec: h264_codec(), payload_type: H264_PAYLOAD_TYPE }, RtpCodecKind::Video)?;
    let target_bps = Arc::new(AtomicU64::new(GCC_INITIAL_BPS.to_bits()));
    let estimator = ReportingEstimator { inner: Gcc::new(GCC_INITIAL_BPS, GCC_MIN_BPS, GCC_MAX_BPS), target_bps: target_bps.clone() };
    let registry = configure_congestion_control(Registry::new(), estimator, CongestionFeedback::Twcc, &mut media_engine)?;
    let registry = register_default_interceptors(registry, &mut media_engine)?;
    Ok((media_engine, registry, target_bps))
}

pub fn gcc_target_bytes_per_sec(target_bps: &AtomicU64) -> f64 {
    f64::from_bits(target_bps.load(Ordering::Relaxed)) / 8.0
}

pub struct VideoTrack {
    pub mid: String,
    track: Arc<TrackLocalStaticSample>,
    ssrc: u32,
    payload_type: u8,
    keyframe_requested: Arc<AtomicBool>,
    /// PLI/FIR requests from the browser
    pub keyframe_requests: Arc<AtomicU64>,
    in_use: AtomicBool,
}

impl VideoTrack {
    /// Claims the track for one subscription; false if another one holds it.
    pub fn claim(&self) -> bool {
        self.in_use.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok()
    }

    fn release(&self) {
        self.in_use.store(false, Ordering::Release);
    }

    async fn write(&self, data: Vec<u8>, duration: Duration) -> Result<()> {
        let sample = Sample { data: Bytes::from(data), duration, ..Sample::new(Instant::now()) };
        self.track.sample_writer(self.ssrc, self.payload_type).write_sample(&sample).await?;
        Ok(())
    }
}

/// Watches the track's RTCP for keyframe requests until the track is dropped.
fn spawn_rtcp_reader(track: Arc<TrackLocalStaticSample>, owner: Weak<VideoTrack>, keyframe_requested: Arc<AtomicBool>, keyframe_requests: Arc<AtomicU64>) {
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

/// Applies a browser offer (renegotiation over `control`). With `add_video`, first adds a video
/// track, which pairs with the offer's new recvonly video m-line, and returns it with its mid.
pub async fn renegotiate(connection: &Arc<dyn PeerConnection>, offer: RTCSessionDescription, add_video: bool) -> Result<(RTCSessionDescription, Option<Arc<VideoTrack>>)> {
    let added = if add_video {
        let ssrc = rand_ssrc();
        let track = Arc::new(TrackLocalStaticSample::new(
            Instant::now(),
            MediaStreamTrack::new(
                format!("zenoh-web-{ssrc}"),
                format!("zenoh-web-video-{ssrc}"),
                "zenoh-web video".to_owned(),
                RtpCodecKind::Video,
                vec![RTCRtpEncodingParameters { rtp_coding_parameters: RTCRtpCodingParameters { ssrc: Some(ssrc), ..Default::default() }, codec: h264_codec(), ..Default::default() }],
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
    let Some((track, sender, ssrc)) = added else { return Ok((local, None)) };
    let mut mid = None;
    for transceiver in connection.get_transceivers().await {
        if let Ok(Some(transceiver_sender)) = transceiver.sender().await
            && transceiver_sender.id() == sender.id()
        {
            mid = transceiver.mid().await?;
        }
    }
    let mid = mid.context("the offer had no new video m-line for the track (add a recvonly video transceiver before renegotiating)")?;
    let payload_type = negotiated_payload_type(&sender).await.unwrap_or(H264_PAYLOAD_TYPE);
    let keyframe_requested = Arc::new(AtomicBool::new(true));
    let keyframe_requests = Arc::new(AtomicU64::new(0));
    let video = Arc::new(VideoTrack { mid, track: track.clone(), ssrc, payload_type, keyframe_requested: keyframe_requested.clone(), keyframe_requests: keyframe_requests.clone(), in_use: AtomicBool::new(false) });
    spawn_rtcp_reader(track, Arc::downgrade(&video), keyframe_requested, keyframe_requests);
    Ok((local, Some(video)))
}

async fn negotiated_payload_type(sender: &Arc<dyn RtpSender>) -> Result<u8> {
    sender.get_parameters().await?.rtp_parameters.codecs.first().map(|codec| codec.payload_type).context("sender has no negotiated codec")
}

fn rand_ssrc() -> u32 {
    use std::hash::{BuildHasher, Hasher};
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(Instant::now().elapsed().as_nanos() ^ std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos());
    (hasher.finish() as u32).max(1)
}

/// `u8 version=1 | u8 flags (bit0 keyframe) | u16 0 | u32 width | u32 height | u32 sourceWidth |
///  u32 sourceHeight | f32 quality | u32 encodedBytes`, little endian.
fn metadata(frame: &EncodedFrame, source: (u32, u32), quality: f64) -> [u8; METADATA_LEN] {
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

/// A picked frame and its decode, running on the blocking pool.
struct Decoding {
    key: String,
    item: subscription::Pending,
    /// (decoded frame, reused another frontend's decode, milliseconds it took)
    task: tokio::task::JoinHandle<(std::result::Result<Arc<DecodedFrame>, String>, bool, f64)>,
}

fn start_decode(shared: &SubShared, codec: &Arc<dyn Codec>, key: String, item: subscription::Pending) -> Decoding {
    let payload = item.payload.to_bytes().into_owned();
    let (hash_key, encoding, codecs, codec) = (key.clone(), item.encoding.clone(), shared.codecs.clone(), codec.clone());
    let task = tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let sample = CodecSample::new(&hash_key, &payload, &encoding);
        let (decoded, reused) = codecs.decode_shared(&*codec, &sample, registry::sample_hash(&sample));
        (decoded, reused, started.elapsed().as_secs_f64() * 1000.0)
    });
    Decoding { key, item, task }
}

/// Quality steps the CPU governor moves by.
const CPU_STEP: f64 = 0.1;
/// Share of the frame interval the slower pipeline stage may take before quality steps down.
const CPU_HEADROOM: f64 = 0.85;
/// Time after a step before the next, so the costs are measured at the new size.
const CPU_SETTLE: Duration = Duration::from_secs(1);
/// Weight of the newest sample in the cost averages.
const CPU_EWMA_GAIN: f64 = 0.2;

/// Keeps a video stream within what the machine's cores can encode at the granted rate. The
/// allocator picks quality for bandwidth; on a small CPU (a Jetson with a 1920x1536 camera) the
/// encoder then fell behind and the frame rate collapsed instead. When scaling + encoding a frame
/// takes more than `CPU_HEADROOM` of the frame interval, the ceiling steps down (never below
/// minQuality), trading resolution for frames the way a bandwidth shortfall would; it steps back up
/// when the encode cost predicted at the next step (it scales with pixels) fits again. Decoding
/// overlaps encoding and does not depend on quality, so it never moves the ceiling: a slow decode
/// caps the frame rate whatever the resolution.
struct CpuGovernor {
    cap: f64,
    decode_ms: Option<f64>,
    encode_ms: Option<f64>,
    changed_at: Option<Instant>,
}

impl CpuGovernor {
    fn new() -> Self {
        CpuGovernor { cap: 1.0, decode_ms: None, encode_ms: None, changed_at: None }
    }

    fn observe_decode(&mut self, ms: f64, reused: bool) {
        if !reused {
            self.decode_ms = Some(self.decode_ms.map_or(ms, |average| average + CPU_EWMA_GAIN * (ms - average)));
        }
    }

    fn observe_encode(&mut self, ms: f64) {
        self.encode_ms = Some(self.encode_ms.map_or(ms, |average| average + CPU_EWMA_GAIN * (ms - average)));
    }

    /// The quality to encode at: `allocated`, lowered to the governor's ceiling.
    fn quality(&mut self, allocated: f64, min_quality: f64, hz: f64, now: Instant) -> f64 {
        let floor = min_quality.min(allocated);
        let settled = self.changed_at.is_none_or(|at| now.duration_since(at) >= CPU_SETTLE);
        if let (true, Some(encode_ms)) = (settled, self.encode_ms) {
            let budget_ms = 1000.0 / hz.max(0.1) * CPU_HEADROOM;
            let current = allocated.min(self.cap);
            if encode_ms > budget_ms && current > floor + 1e-9 {
                self.cap = (current - CPU_STEP).max(floor);
                self.encode_ms = None;
                self.changed_at = Some(now);
            } else if self.cap < 1.0 && self.cap < allocated {
                let next = (self.cap + CPU_STEP).min(1.0);
                let pixels = |quality: f64| crate::codec::video::resolution_scale(quality).powi(2);
                let predicted_ms = encode_ms * pixels(next) / pixels(self.cap);
                if predicted_ms < budget_ms * 0.9 {
                    self.cap = next;
                    self.encode_ms = None;
                    self.changed_at = Some(now);
                }
            }
        }
        allocated.min(self.cap).max(floor)
    }
}

/// Sends a video subscription's frames: pick (paced by maxHz and the allocation), decode (shared
/// across frontends), encode at the allocated quality and Hz (off the runtime), write to the track.
/// The next frame's decode overlaps this frame's encode: in series, a big camera frame (a 1920x1536
/// jpeg is ~16 ms to decode and ~20 ms to scale and encode on a Jetson Orin core) could not keep up
/// with 30 Hz; overlapped, the slower of the two sets the rate.
pub async fn send_loop(dc: Arc<dyn DataChannel>, shared: Arc<SubShared>, track: Arc<VideoTrack>) {
    let Some(codec) = shared.codec.clone() else { return };
    let mut encoder = Some(VideoEncoder::default());
    let mut next_frame_id: u32 = 0;
    let mut last_write: Option<Instant> = None;
    let mut warned_write = false;
    let mut next: Option<Decoding> = None;
    let mut governor = CpuGovernor::new();
    // a new subscriber starts from a keyframe (the new encoder's first frame is one anyway)
    track.keyframe_requested.store(true, Ordering::Release);
    while !shared.is_closed() {
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
        let (decoded, reused) = match task.await {
            Ok((Ok(decoded), reused, decode_ms)) => {
                governor.observe_decode(decode_ms, reused);
                (decoded, reused)
            }
            Ok((Err(error), _, _)) => {
                shared.record_codec_error(&error);
                continue;
            }
            Err(error) => {
                shared.record_codec_error(&format!("decoder task failed: {error}"));
                continue;
            }
        };
        let hz = shared.key_hz(&key);
        let quality = governor.quality(shared.current_quality(), shared.min_quality(), hz, Instant::now());
        let keyframe = track.keyframe_requested.swap(false, Ordering::AcqRel);
        let mut working = encoder.take().unwrap_or_default();
        let codec_name = codec.name().to_owned();
        let mut encoding = tokio::task::spawn_blocking(move || {
            let DecodedFrame::Video(image) = &*decoded else {
                return (working, Err(format!("video codec {codec_name:?} decoded to a data frame")));
            };
            if keyframe {
                working.request_keyframe();
            }
            let started = Instant::now();
            let result = working
                .encode(image, quality, hz)
                .map(|frame| (frame, (image.width(), image.height()), started.elapsed().as_secs_f64() * 1000.0))
                .map_err(|error| format!("{error:#}"));
            (working, result)
        });
        // while it encodes, pick the next frame and start decoding it as soon as one may go
        let outcome = loop {
            if next.is_some() {
                break (&mut encoding).await;
            }
            let (picked, wake_at) = shared.pick(Instant::now());
            if let Some((key, item)) = picked {
                next = Some(start_decode(&shared, &codec, key, item));
                continue;
            }
            tokio::select! {
                outcome = &mut encoding => break outcome,
                _ = shared.wait_for_data(wake_at) => {}
            }
        };
        let (frame, source) = match outcome {
            Ok((returned, Ok((frame, source, encode_ms)))) => {
                encoder = Some(returned);
                governor.observe_encode(encode_ms);
                shared.record_video_timing(governor.decode_ms, governor.encode_ms, governor.cap);
                (frame, source)
            }
            Ok((returned, Err(error))) => {
                encoder = Some(returned);
                shared.record_codec_error(&error);
                continue;
            }
            Err(error) => {
                shared.record_codec_error(&format!("encoder task failed: {error}"));
                continue;
            }
        };
        shared.note_video_source(source.0, source.1);
        let now = Instant::now();
        let duration = last_write.map_or(Duration::from_secs_f64(1.0 / hz.max(0.1)), |previous| now.duration_since(previous).max(Duration::from_millis(1)));
        last_write = Some(now);
        let meta = metadata(&frame, source, quality);
        let (width, height, keyframe, encoded_len) = (frame.width, frame.height, frame.keyframe, frame.data.len());
        if let Err(error) = track.write(frame.data, duration).await {
            if !warned_write {
                log::warn!("video track {}: write failed: {error:#}", track.mid);
                warned_write = true;
            }
            shared.record_codec_error(&format!("track write: {error:#}"));
            continue;
        }
        shared.record_video_frame(encoded_len, (width, height), quality, keyframe, reused, track.keyframe_requests.load(Ordering::Relaxed));
        if subscription::send_small_frame(&dc, &key, &item, next_frame_id, &meta).await.is_err() && shared.is_closed() {
            break;
        }
        next_frame_id = next_frame_id.wrapping_add(1);
    }
    track.release();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_governor_trades_resolution_for_frame_rate() {
        let start = Instant::now();
        let mut governor = CpuGovernor::new();
        // 30 Hz leaves 33 ms a frame; encoding takes 50
        governor.observe_decode(15.0, false);
        governor.observe_encode(50.0);
        assert!((governor.quality(0.8, 0.1, 30.0, start) - 0.7).abs() < 1e-9, "one step down");
        assert!((governor.quality(0.8, 0.1, 30.0, start + Duration::from_millis(100)) - 0.7).abs() < 1e-9, "settles before the next step");
        governor.observe_encode(45.0);
        assert!((governor.quality(0.8, 0.1, 30.0, start + CPU_SETTLE) - 0.6).abs() < 1e-9);
        // a slow decode alone (it does not depend on quality) leaves the ceiling alone
        let mut decode_bound = CpuGovernor::new();
        decode_bound.observe_decode(40.0, false);
        decode_bound.observe_encode(10.0);
        assert_eq!(decode_bound.quality(0.8, 0.1, 30.0, start), 0.8);
        // never below minQuality
        let mut floor = CpuGovernor::new();
        floor.observe_encode(500.0);
        assert_eq!(floor.quality(0.5, 0.5, 30.0, start), 0.5);
        // cheap again: steps back up when the next step's predicted cost fits
        governor.observe_encode(5.0);
        let later = start + CPU_SETTLE * 3;
        assert!((governor.quality(0.8, 0.1, 30.0, later) - 0.7).abs() < 1e-9, "steps up");
        // and never above what the allocator granted
        governor.observe_encode(1.0);
        assert!(governor.quality(0.3, 0.1, 30.0, later + CPU_SETTLE) <= 0.3 + 1e-9);
    }
}
