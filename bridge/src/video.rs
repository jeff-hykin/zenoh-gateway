//! Video subscriptions: frames decoded by a codec, encoded to H.264 and written to a WebRTC
//! video track (one track per frontend and subscription). The browser adds a recvonly
//! transceiver and renegotiates over `control`; the bridge answers with a new track bound to that
//! transceiver's mid. A subscription's `sub` data channel names the mid in its label and carries a
//! small metadata frame per video frame. Tracks are reused by later subscriptions on the same mid.

use crate::codec::{self, video::VideoEncoder};
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
/// Google Congestion Control bounds, bits/s.
const GCC_INITIAL_BPS: f64 = 1_000_000.0;
const GCC_MIN_BPS: f64 = 50_000.0;
const GCC_MAX_BPS: f64 = 50_000_000.0;
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

    fn target_bitrate(&self) -> f64 {
        self.inner.target_bitrate()
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
fn metadata(frame: &codec::video::EncodedFrame, source: (u32, u32), quality: f64) -> [u8; METADATA_LEN] {
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

/// Sends a video subscription's frames: pick (paced by maxHz and the allocation), decode (shared
/// across frontends), encode at the allocated quality and Hz (off the runtime), write to the track.
pub async fn send_loop(dc: Arc<dyn DataChannel>, shared: Arc<SubShared>, track: Arc<VideoTrack>) {
    let Some(codec) = shared.codec else { return };
    let mut encoder = Some(VideoEncoder::default());
    let mut next_frame_id: u32 = 0;
    let mut last_write: Option<Instant> = None;
    let mut warned_write = false;
    // a new subscriber starts from a keyframe (the new encoder's first frame is one anyway)
    track.keyframe_requested.store(true, Ordering::Release);
    while !shared.is_closed() {
        let (next, wake_at) = shared.pick(Instant::now());
        let Some((key, item)) = next else {
            shared.wait_for_data(wake_at).await;
            continue;
        };
        let quality = shared.current_quality();
        let hz = shared.key_hz(&key);
        let keyframe = track.keyframe_requested.swap(false, Ordering::AcqRel);
        let payload = item.payload.to_bytes().into_owned();
        let mut working = encoder.take().unwrap_or_default();
        let hash_key = key.clone();
        let outcome = tokio::task::spawn_blocking(move || {
            let hash = codec::payload_hash(&hash_key, &payload);
            let (rgb, reused) = codec::decode_shared(codec, hash, &payload);
            let result = rgb.and_then(|rgb| {
                if keyframe {
                    working.request_keyframe();
                }
                working.encode(&rgb, quality, hz).map(|frame| (frame, (rgb.width, rgb.height), reused)).map_err(|error| format!("{error:#}"))
            });
            (working, result)
        })
        .await;
        let (frame, source, reused) = match outcome {
            Ok((returned, Ok(encoded))) => {
                encoder = Some(returned);
                encoded
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
