//! One `sub` data channel: a zenoh AdvancedSubscriber feeding per-key delivery queues, drained
//! into the data channel only while it is not backed up.

use crate::allocator::{Allocation, Demand};
use crate::encoding::registry::{self, EncodingRegistry, Resolved};
use crate::encoding::{Channel, Compress, EncodeOptions, EncodingOutput, EncodingSample, MessageEncoding, VideoPolicy};
use crate::frame;
use crate::options::{Delivery, Label, SubOpts};
use crate::pacing::SendGate;
use log::{debug, warn};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use parking_lot::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use zenoh::bytes::{Encoding, ZBytes};
use zenoh::sample::{Sample, SampleKind};
use zenoh_ext::{AdvancedSubscriberBuilderExt, HistoryConfig};

/// Weight of the newest sample in rate and size averages.
const EWMA_GAIN: f64 = 0.5;
/// Per-frame overhead on the wire besides the payload (frame header + key, SCTP/DTLS/UDP).
const FRAME_OVERHEAD_BYTES: f64 = 90.0;
/// What each frame costs the link beyond its own bytes (SCTP chunk + common header, DTLS record, UDP, IP): counted in
/// what a stream sent, so the estimate and the reservations are in the link's bytes (100+ small frames a second are a
/// third of a 250 kbit/s link, and counted as payload they overloaded it).
pub const PACKET_OVERHEAD_BYTES: u64 = 80;
/// A video stream's price is at most this times its measured frame size (scaled by quality).
const VIDEO_PRICE_HEADROOM: f64 = 1.5;
/// A `reliable` key's samples that have waited this long are dropped while a newer one waits behind them (a page that
/// can't keep up would otherwise grow the queue without bound); order is kept and the drops show as `seq` gaps.
const RELIABLE_MAX_BACKLOG: Duration = Duration::from_secs(2);
/// Streams at this zenoh priority or more urgent (2 INTERACTIVE_HIGH, 1 REAL_TIME) bypass allocation and pacing.
const STRICT_PRIORITY: u8 = 2;

#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubStats {
    pub received: u64,
    pub sent: u64,
    pub dropped_queue: u64,
    pub dropped_age: u64,
    pub dropped_send_error: u64,
    /// video: times the send loop held off encoding because written frames still waited in the RTP pacer
    pub pacer_waits: u64,
    pub queued: usize,
    pub queued_bytes: usize,
    /// bytes sent the page hasn't acked
    pub unconsumed_bytes: usize,
    /// frames (chunks) handed to SCTP; `sent` counts whole messages
    pub frames_sent: u64,
    /// lossy channel stopped sending a chunked message that outlived maxAge
    pub abandoned_partial: u64,
    /// time a bulk sender waited for the frontend's in-flight window (the link or the page behind)
    pub blocked_ms: f64,
    /// time a bulk sender waited on the frontend's pacing rate or a strict stream's turn
    pub paced_ms: f64,
    /// Gateway receive time minus sample timestamp (upstream lag), largest seen.
    pub max_receive_lag_ms: f64,
    /// Time a message's last frame was handed to SCTP minus its sample timestamp, largest seen.
    pub max_send_lag_ms: f64,
    /// frame bytes handed to SCTP (data channel) or video bytes handed to the track
    pub bytes_sent: u64,
    /// transcodes this subscription ran / reused from another frontend's identical request (video: frames another
    /// viewer's subscription encoded in the shared encode session)
    pub encodes: u64,
    pub shared_encodes: u64,
    pub encoding_errors: u64,
    pub last_encoding_error: Option<String>,
    /// quality of the last transcoded message
    pub quality: Option<f64>,
    pub keyframes: u64,
    /// PLI/FIR keyframe requests the browser sent on this stream's track
    pub keyframe_requests: u64,
    pub video_width: u32,
    pub video_height: u32,
    /// video: smoothed decode / scale+encode time per frame, and the CPU governor's ceiling on the picture's scale (1 = none)
    pub decode_ms: Option<f64>,
    pub encode_ms: Option<f64>,
    pub cpu_scale_cap: Option<f64>,
    /// video: where its latest frames' time went inside the gateway
    pub video_latency: Option<VideoLatency>,
}

/// A video stream's gateway stages, ms: smoothed over its frames, and the RTP pacer's over its latest 128.
#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VideoLatency {
    /// the sample's own timestamp to the gateway getting it (zenoh, and the publisher's clock against this one's;
    /// 0 for samples without a zenoh timestamp)
    pub source_to_received_ms: f64,
    /// queued, decoded, scaled and encoded
    pub received_to_encoded_ms: f64,
    /// handed to the track (waits on the pacer rule and a shared encode session)
    pub encoded_to_written_ms: f64,
    /// first packet into the RTP pacer to last packet out
    pub pacer_p50_ms: Option<f64>,
    pub pacer_p95_ms: Option<f64>,
}

pub struct Pending {
    pub payload: ZBytes,
    pub encoding: Encoding,
    pub kind: SampleKind,
    pub attachment: Option<ZBytes>,
    pub timestamp_ms: f64,
    /// when the gateway got it (unix ms, its clock)
    pub received_ms: f64,
    pub seq: u32,
    arrived: Instant,
    priority: u8,
}

#[derive(Default)]
struct KeyQueue {
    items: VecDeque<Pending>,
    last_sent: Option<Instant>,
    /// arrivals since the allocator last looked, and the resulting source rate
    arrivals: u32,
    rate_hz: f64,
    /// the latest sample's arrival: a key quiet for `stale_after` with nothing queued is forgotten (kept, every key a
    /// wildcard subscription ever matched stayed for its life)
    last_arrival: Option<Instant>,
}

/// How often a key may send.
#[derive(Debug, PartialEq)]
enum Spacing {
    Any,
    Every(Duration),
    /// granted nothing
    Never,
}

#[derive(Default)]
struct SubState {
    keys: HashMap<String, KeyQueue>,
    next_seq: u32,
    stats: SubStats,
    /// (frameId, frame bytes, place in the frontend's send order, when sent) in send order, until the page acks a frameId at
    /// or past it (or `stale_after` passes)
    unconsumed: VecDeque<(u32, usize, u64, Instant)>,
    allocation: Allocation,
    /// the link model is probing for bandwidth this round (see `apply`)
    probing: bool,
    payload_bytes: f64,
    /// bytes on the wire per message (untranscoded)
    message_bytes: f64,
    /// transcoded bytes per message, by quality bucket
    encoded_bytes: BTreeMap<u16, f64>,
    /// source resolution of a video stream
    video_source: Option<(u32, u32)>,
    /// a video stream's encoded bytes per frame (average) and the quality it encoded at last
    video_frame_bytes: Option<(f64, f64)>,
    /// counters as of the allocator's previous look
    accounted_bytes_sent: u64,
    accounted_frames_sent: u64,
    accounted_blocked_ms: f64,
}

impl SubStats {
    fn unqueue(&mut self, item: &Pending) {
        self.queued -= 1;
        self.queued_bytes -= item.payload.len();
    }
}

fn ewma(previous: f64, sample: f64) -> f64 {
    if previous <= 0.0 { sample } else { previous + EWMA_GAIN * (sample - previous) }
}

/// What the allocator learns from one subscription since its previous look.
pub struct Usage {
    pub demand: Demand,
    /// on the wire: its frames plus `PACKET_OVERHEAD_BYTES` each
    pub bytes_sent: u64,
    /// time it waited on the in-flight window
    pub blocked_ms: f64,
    /// sent on an RTP track (video, audio), not the data channel
    pub on_track: bool,
}

/// `a <= b` for wrapping u32 sequence numbers.
fn seq_at_or_before(a: u32, b: u32) -> bool {
    b.wrapping_sub(a) < u32::MAX / 2
}

/// A subscription's options that `updateSubscription` may change while it runs.
#[derive(Clone, Copy)]
pub struct Tuning {
    min_interval: Option<Duration>,
    max_hz: Option<f64>,
    /// bandwidthPriority: higher keeps more when bandwidth is short
    bandwidth_priority: f64,
    quality_range: (f64, f64),
    tradeoff: f64,
    /// video channels: the server's policy with this subscription's overrides
    pub video_policy: VideoPolicy,
    /// video channels: (min, max) playout delay for the browser, in 10 ms units
    pub playout_delay: (u16, u16),
}

impl Tuning {
    fn new(opts: &SubOpts, server_policy: VideoPolicy, video: bool) -> Self {
        Tuning {
            min_interval: opts.min_interval(),
            max_hz: opts.max_hz,
            bandwidth_priority: opts.bandwidth_priority.unwrap_or(1.0),
            quality_range: opts.quality_range(),
            tradeoff: opts.tradeoff(video),
            video_policy: opts.video_policy(server_policy),
            playout_delay: opts.playout_delay_units(),
        }
    }
}

pub struct SubShared {
    delivery: Delivery,
    priority_override: Option<u8>,
    pub codec: Option<Arc<dyn MessageEncoding>>,
    /// what the frames travel on, and what the encoding sends there
    pub channel: Channel,
    pub output: EncodingOutput,
    /// `encodeOptions` without `quality`
    encode_options: serde_json::Map<String, serde_json::Value>,
    /// the encoding's key prefix with its trailing `/`
    key_prefix: Option<String>,
    compress: Compress,
    /// the server's encodings, with the caches that share work across frontends
    pub codecs: Arc<EncodingRegistry>,
    /// the options `updateSubscription` may change while it runs
    tuning: Mutex<Tuning>,
    /// this frontend's shared send gate, and this stream's id in it
    gate: Arc<SendGate>,
    stream_id: usize,
    /// published priority of the latest sample (used when the subscription sets none)
    sample_priority: AtomicU8,
    state: Mutex<SubState>,
    data_ready: Notify,
    closed: AtomicBool,
    /// flips to true on close, so `run` also stops when the channel never reports its own close
    closed_signal: tokio::sync::watch::Sender<bool>,
    /// ended by another client's lease on its key
    pub leased_away: AtomicBool,
}

pub fn now_unix_ms() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0)
}

/// A video source's size before its first frame is decoded: a 16:9 picture of 3 bytes per pixel (a raw rgb8/bgr8 sample;
/// a compressed one guesses small and is priced cheap until its first frame says otherwise). Priced as the raw sample
/// instead, a 720p camera (2.7 MB) wanted 40 MB/s, was granted 0.016 Hz, and its first frame went out a minute later:
/// nothing was sent meanwhile, so nothing measured the link either.
fn guessed_video_source(payload_bytes: f64) -> Option<(u32, u32)> {
    let pixels = payload_bytes / 3.0;
    (pixels >= 64.0 * 64.0).then(|| {
        let height = (pixels * 9.0 / 16.0).sqrt();
        ((height * 16.0 / 9.0).round() as u32, height.round() as u32)
    })
}

fn sample_timestamp_ms(sample: &Sample) -> f64 {
    let since_epoch = sample.timestamp().and_then(|ts| ts.get_time().to_system_time().duration_since(UNIX_EPOCH).ok());
    since_epoch.map_or_else(now_unix_ms, |d| d.as_secs_f64() * 1000.0)
}

impl SubShared {
    pub fn new(opts: &SubOpts, resolved: Resolved, codecs: Arc<EncodingRegistry>, gate: Arc<SendGate>) -> Self {
        let Resolved { encoding: codec, channel, output } = resolved;
        static NEXT_STREAM: AtomicUsize = AtomicUsize::new(0);
        SubShared {
            gate,
            stream_id: NEXT_STREAM.fetch_add(1, Ordering::Relaxed),
            sample_priority: AtomicU8::new(0),
            delivery: opts.delivery(),
            tuning: Mutex::new(Tuning::new(opts, codecs.video_policy, matches!(channel, Channel::Video(_)))),
            priority_override: opts.priority,
            key_prefix: codec.as_ref().and_then(|codec| codec.key_prefix()).map(|prefix| format!("{}/", prefix.trim_end_matches('/'))),
            codec,
            channel,
            output,
            encode_options: opts.encoding_options(),
            compress: opts.compress.unwrap_or_default(),
            codecs,
            state: Mutex::new(SubState::default()),
            data_ready: Notify::new(),
            closed: AtomicBool::new(false),
            closed_signal: tokio::sync::watch::Sender::new(false),
            leased_away: AtomicBool::new(false),
        }
    }

    pub fn tuning(&self) -> Tuning {
        *self.tuning.lock()
    }

    /// Applies new options to the running subscription (`updateSubscription`); the next frame uses them.
    pub fn retune(&self, opts: &SubOpts) {
        *self.tuning.lock() = Tuning::new(opts, self.codecs.video_policy, matches!(self.channel, Channel::Video(_)));
        self.data_ready.notify_one();
    }

    /// The zenoh key expression a subscription to `key` reads (see [`MessageEncoding::key_prefix`]).
    fn zenoh_key(&self, key: &str) -> String {
        format!("{}{key}", self.key_prefix.as_deref().unwrap_or_default())
    }

    pub fn stats(&self) -> SubStats {
        self.state.lock().stats.clone()
    }

    pub fn allocation(&self) -> Allocation {
        self.state.lock().allocation.clone()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// Resolves once the subscription is closed: whatever its sender waits on (the window, the message turn) is
    /// given up then, since that wait can depend on its own frames the page will never ack.
    pub async fn wait_closed(&self) {
        let _ = self.closed_signal.subscribe().wait_for(|closed| *closed).await;
    }

    /// [`SendGate::wait_bulk_turn`] until the subscription closes (None).
    async fn bulk_turn(&self, bytes: usize) -> Option<Duration> {
        tokio::select! {
            on_window = self.gate.wait_bulk_turn(bytes) => Some(on_window),
            () = self.wait_closed() => None,
        }
    }

    /// Lets go of every frame of this channel the gate still counts in flight: the page will never ack them.
    fn release_frames(&self) {
        self.gate.frames_dropped(self.state.lock().unconsumed.drain(..).map(|(_, _, order, _)| order));
        self.gate.forget(self.stream_id);
    }

    /// How long this stream's data can tolerate waiting in the link's queue (seconds): its maxAge (older is useless to the
    /// page), never less than one of its messages takes to cross at `bandwidth` (a 400 KB map can't arrive sooner
    /// anyway). `None` without a maxAge: no opinion (its rate says how fast a sensor runs, not how urgent it is).
    pub fn latency_tolerance(&self, bandwidth: f64) -> Option<f64> {
        let max_age = self.delivery.max_age_ms? / 1000.0;
        let state = self.state.lock();
        let message_bytes = state.video_frame_bytes.map_or(state.message_bytes, |(_, bytes)| bytes);
        Some(max_age.max(message_bytes / bandwidth.max(1.0)))
    }

    /// Strict-priority tier: its priority (subscribe option, else as published) is
    /// `STRICT_PRIORITY` or more urgent. Strict streams bypass allocation and pacing and preempt bulk streams.
    pub fn is_strict(&self) -> bool {
        let priority = self.priority_override.unwrap_or_else(|| self.sample_priority.load(Ordering::Relaxed));
        priority != 0 && priority <= STRICT_PRIORITY
    }

    /// Quality to transcode at now: the allocation's, or the best allowed before the first one.
    pub fn current_quality(&self) -> f64 {
        self.state.lock().allocation.quality.unwrap_or(self.tuning().quality_range.1)
    }

    /// Granted Hz for one key of this stream (video encoders size their bitrate by it).
    pub fn key_hz(&self, key: &str) -> f64 {
        let state = self.state.lock();
        let rate = state.keys.get(key).map_or(0.0, |queue| queue.rate_hz);
        // never above the source's own rate: the encoder spreads its bitrate over this many frames a second
        let wanted = match (self.tuning().max_hz, rate > 0.0) {
            (Some(max), true) => max.min(rate),
            (Some(max), false) => max,
            (None, true) => rate,
            (None, false) => 30.0,
        };
        self.allocated_key_hz(rate, &state.allocation).unwrap_or(wanted)
    }

    /// What a video key is granted: (quality, bits/s, frames/s). Before the first allocation, its floor picture at
    /// `source`'s size: a camera with no grant once fell back to the most it may ask for and took a 100 kbit/s link down.
    pub fn video_grant(&self, key: &str, source: (u32, u32)) -> (f64, f64, f64) {
        let hz = self.key_hz(key);
        let state = self.state.lock();
        let allocation = &state.allocation;
        if allocation.hz > 0.0 && allocation.budget_bytes_per_sec > 0.0 {
            let quality = allocation.quality.unwrap_or(self.tuning().quality_range.1);
            return (quality, allocation.budget_bytes_per_sec * 8.0 * (hz / allocation.hz).min(1.0), hz);
        }
        let quality = self.tuning().quality_range.0;
        let floor = self.tuning().video_policy.frame_bytes(source, hz, quality);
        (quality, floor * 8.0 * hz, hz)
    }

    /// Per-key Hz cap from the allocation (None = no allocation cap).
    fn allocated_key_hz(&self, rate_hz: f64, allocation: &Allocation) -> Option<f64> {
        if self.reserved() || !allocation.constrained || rate_hz <= 0.0 {
            return None;
        }
        let wanted = self.tuning().max_hz.map_or(rate_hz, |max| max.min(rate_hz));
        Some(wanted * allocation.hz_fraction)
    }

    /// Spacing between two sends of a key: maxHz, tightened by the allocation. A key granted nothing sends nothing until
    /// it is granted some (at a 0.05 Hz floor a 400 KB map alone took 20 KB/s of a 12 KB/s link); a new allocation wakes it.
    fn key_interval(&self, queue: &KeyQueue, allocation: &Allocation, probing: bool) -> Spacing {
        // a probe lifts a data stream's spacing (the frontend's pacing and window still hold it), never video's (its frames
        // bypass both: a camera granted nothing woke at full size and put seconds of queue on a slow link) nor a stream
        // granted nothing
        let lift = probing && !self.on_track();
        match self.allocated_key_hz(queue.rate_hz, allocation).filter(|hz| !(lift && *hz > 0.0)) {
            Some(hz) if hz <= 0.0 => Spacing::Never,
            Some(hz) => Spacing::Every(crate::secs(1.0 / hz).max(self.tuning().min_interval.unwrap_or_default())),
            None => self.tuning().min_interval.map_or(Spacing::Any, Spacing::Every),
        }
    }

    /// A video or audio channel's subscription (frames on an RTP track).
    pub fn on_track(&self) -> bool {
        self.channel != Channel::Data
    }

    /// What the encoding's `encode` gets at `quality`.
    pub fn encode_options(&self, quality: f64) -> EncodeOptions {
        EncodeOptions { quality, options: self.encode_options.clone() }
    }

    /// Reserved at its measured rate, never paced or shrunk: strict-priority and audio streams. (Reliable ones shrink
    /// last, see `allocator`, their queue bounded by `RELIABLE_MAX_BACKLOG`.)
    pub fn reserved(&self) -> bool {
        self.is_strict() || self.channel == Channel::Audio
    }

    pub fn video_source(&self) -> Option<(u32, u32)> {
        self.state.lock().video_source
    }

    pub fn note_video_source(&self, width: u32, height: u32) {
        self.state.lock().video_source = Some((width, height));
    }

    /// Measures rates since the previous call and describes what this stream wants.
    pub fn usage(&self, interval_secs: f64) -> Usage {
        let mut state = self.state.lock();
        let (now, stale_after) = (Instant::now(), self.gate.stale_after());
        state.keys.retain(|_, queue| !queue.items.is_empty() || queue.last_arrival.is_some_and(|at| now.duration_since(at) < stale_after));
        // frames the page never acked within stale_after (the gate forgot them too)
        while state.unconsumed.front().is_some_and(|(_, _, _, sent)| now.duration_since(*sent) > stale_after) {
            let (_, len, _, _) = state.unconsumed.pop_front().unwrap();
            state.stats.unconsumed_bytes -= len;
        }
        let mut max_hz = 0.0;
        for queue in state.keys.values_mut() {
            queue.rate_hz = ewma(queue.rate_hz, queue.arrivals as f64 / interval_secs.max(1e-3));
            queue.arrivals = 0;
            max_hz += self.tuning().max_hz.map_or(queue.rate_hz, |max| max.min(queue.rate_hz));
        }
        let frames_sent = state.stats.frames_sent - state.accounted_frames_sent;
        state.accounted_frames_sent = state.stats.frames_sent;
        let frame_bytes = state.stats.bytes_sent - state.accounted_bytes_sent;
        let bytes_sent = frame_bytes + frames_sent * PACKET_OVERHEAD_BYTES;
        let blocked_ms = state.stats.blocked_ms - state.accounted_blocked_ms;
        state.accounted_bytes_sent = state.stats.bytes_sent;
        state.accounted_blocked_ms = state.stats.blocked_ms;
        let payload_bytes = state.payload_bytes;
        let price: crate::allocator::Price = match &self.codec {
            None => {
                let message_bytes = if state.message_bytes > 0.0 { state.message_bytes + PACKET_OVERHEAD_BYTES as f64 } else { payload_bytes + FRAME_OVERHEAD_BYTES };
                Box::new(move |_| message_bytes)
            }
            Some(_) if matches!(self.channel, Channel::Video(_)) => match state.video_source.or_else(|| guessed_video_source(payload_bytes)) {
              // before its first frame and with no samples yet, the source's size is unknown: priced as the source sample
              None => {
                let sample_bytes = payload_bytes + FRAME_OVERHEAD_BYTES;
                Box::new(move |_| sample_bytes)
              }
              Some(source) => {
                let keys = state.keys.values().filter(|queue| queue.rate_hz > 0.0).count().max(1);
                let (policy, frame_hz) = (self.tuning().video_policy, max_hz / keys as f64);
                let model = move |quality: f64| policy.frame_bytes(source, frame_hz, quality);
                // what its frames measured, with room to grow 1.5x per allocation: a simple or still picture encodes far
                // below the model, and priced by the model alone it never showed the link more (a solid 320x240 camera
                // at 10 Hz was held to 5 fps on localhost)
                // (never below the full picture's floor: grants follow the price and the picture's size follows the grant, so a
                // solid picture's tiny frames shrank it to half size)
                let (measured, full_size) = (state.video_frame_bytes, policy.full_size_floor_bytes(source));
                Box::new(move |quality| match measured {
                    Some((at, bytes)) => model(quality).min((VIDEO_PRICE_HEADROOM * bytes * model(quality) / model(at).max(1e-9)).max(full_size)),
                    None => model(quality),
                })
              }
            },
            Some(codec) => {
                // measured sizes per quality, scaled between qualities by the encoding's own estimate
                let (codec, payload_len, options) = (codec.clone(), payload_bytes.round() as usize, self.encode_options.clone());
                let model = move |quality: f64| codec.estimated_bytes(payload_len, &EncodeOptions { quality, options: options.clone() }).max(0.0);
                let measured: Vec<(f64, f64)> = state.encoded_bytes.iter().map(|(&bucket, &bytes)| (bucket as f64 / 1000.0, bytes)).collect();
                Box::new(move |quality| {
                    let nearest = measured.iter().min_by(|a, b| (a.0 - quality).abs().total_cmp(&(b.0 - quality).abs()));
                    match nearest {
                        Some(&(measured_quality, bytes)) => bytes * model(quality) / model(measured_quality).max(1e-9),
                        None => model(quality) + FRAME_OVERHEAD_BYTES,
                    }
                })
            }
        };
        let demand = Demand {
            priority: self.tuning().bandwidth_priority,
            max_hz,
            quality_range: self.codec.as_ref().map(|_| self.tuning().quality_range),
            tradeoff: self.tuning().tradeoff,
            price,
            fixed_bytes_per_sec: self.reserved().then(|| bytes_sent as f64 / interval_secs.max(1e-3)),
            shrinks_last: self.delivery.reliable,
        };
        Usage { demand, bytes_sent, blocked_ms, on_track: self.on_track() }
    }

    /// The grant sets how often a message starts (`key_interval`) and its quality; a message itself goes out at the
    /// frontend's pacing rate (paced at its grant, a 400 KB map granted 50 KB/s took 8 s to send and arrived 8 s old).
    /// `probing`: the link model is asking for more than its estimate; the grant's spacing is lifted for the round (the
    /// frontend's pacing and window still cap the total), so what gets through measures the link, not the shares.
    pub fn apply(&self, allocation: Allocation, probing: bool) {
        let mut state = self.state.lock();
        state.allocation = allocation;
        state.probing = probing;
        drop(state);
        self.data_ready.notify_one();
    }

    pub(crate) fn record_encode(&self, bucket: u16, quality: f64, encoded_len: usize, shared: bool) {
        let mut state = self.state.lock();
        let previous = state.encoded_bytes.get(&bucket).copied().unwrap_or(0.0);
        state.encoded_bytes.insert(bucket, ewma(previous, encoded_len as f64 + FRAME_OVERHEAD_BYTES));
        state.stats.quality = Some(quality);
        *if shared { &mut state.stats.shared_encodes } else { &mut state.stats.encodes } += 1;
    }

    pub fn record_encoding_error(&self, error: &str) {
        let mut state = self.state.lock();
        state.stats.encoding_errors += 1;
        if state.stats.last_encoding_error.as_deref() != Some(error) {
            warn!("encoding {}: {error}", self.codec.as_ref().map_or("?", |codec| codec.name()));
            state.stats.last_encoding_error = Some(error.to_owned());
        }
    }

    /// The video pipeline's per-frame costs and the CPU governor's ceiling on the picture's scale.
    pub fn record_pacer_wait(&self) {
        self.state.lock().stats.pacer_waits += 1;
    }

    pub fn record_video_timing(&self, decode_ms: Option<f64>, encode_ms: Option<f64>, cpu_scale_cap: f64) {
        let mut state = self.state.lock();
        state.stats.decode_ms = decode_ms;
        state.stats.encode_ms = encode_ms.or(state.stats.encode_ms);
        state.stats.cpu_scale_cap = Some(cpu_scale_cap);
    }

    /// A video frame's gateway stages (ms) and its track's pacer delay percentiles.
    pub fn record_video_latency(&self, source_to_received: f64, received_to_encoded: f64, encoded_to_written: f64, pacer: Option<(f64, f64)>) {
        let mut state = self.state.lock();
        let latency = state.stats.video_latency.get_or_insert_with(|| VideoLatency { source_to_received_ms: source_to_received, received_to_encoded_ms: received_to_encoded, encoded_to_written_ms: encoded_to_written, ..Default::default() });
        latency.source_to_received_ms = ewma(latency.source_to_received_ms, source_to_received);
        latency.received_to_encoded_ms = ewma(latency.received_to_encoded_ms, received_to_encoded);
        latency.encoded_to_written_ms = ewma(latency.encoded_to_written_ms, encoded_to_written);
        (latency.pacer_p50_ms, latency.pacer_p95_ms) = (pacer.map(|(p50, _)| p50), pacer.map(|(_, p95)| p95));
    }

    /// Audio packets went to the track.
    pub fn record_media_bytes(&self, bytes: usize) {
        let mut state = self.state.lock();
        state.stats.bytes_sent += bytes as u64;
        state.stats.sent += 1;
    }

    /// A video frame went to the track.
    pub fn record_video_frame(&self, bytes: usize, (width, height): (u32, u32), quality: f64, keyframe: bool, shared_encode: bool, keyframe_requests: u64) {
        let mut state = self.state.lock();
        state.stats.keyframe_requests = keyframe_requests;
        state.stats.bytes_sent += bytes as u64;
        state.stats.sent += 1;
        state.stats.video_width = width;
        state.stats.video_height = height;
        state.stats.quality = Some(quality);
        state.stats.keyframes += keyframe as u64;
        let average = ewma(state.video_frame_bytes.map_or(0.0, |(_, average)| average), bytes as f64);
        state.video_frame_bytes = Some((quality, average));
        *if shared_encode { &mut state.stats.shared_encodes } else { &mut state.stats.encodes } += 1;
    }

    fn push(&self, sample: Sample) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        let payload_len = sample.payload().len();
        self.sample_priority.store(sample.priority() as u8, Ordering::Relaxed);
        {
            let mut state = self.state.lock();
            let seq = state.next_seq;
            state.next_seq = state.next_seq.wrapping_add(1);
            state.stats.received += 1;
            state.stats.queued += 1;
            state.stats.queued_bytes += payload_len;
            let timestamp_ms = sample_timestamp_ms(&sample);
            let received_ms = now_unix_ms();
            state.stats.max_receive_lag_ms = state.stats.max_receive_lag_ms.max(received_ms - timestamp_ms);
            let pending = Pending {
                encoding: sample.encoding().clone(),
                kind: sample.kind(),
                attachment: sample.attachment().cloned(),
                timestamp_ms,
                received_ms,
                seq,
                arrived: Instant::now(),
                priority: self.priority_override.unwrap_or(sample.priority() as u8),
                payload: sample.payload().clone(),
            };
            let key = self.key_prefix.as_ref().and_then(|prefix| sample.key_expr().as_str().strip_prefix(prefix.as_str())).unwrap_or(sample.key_expr().as_str());
            let state = &mut *state;
            let queue = match state.keys.get_mut(key) {
                Some(queue) => queue,
                None => state.keys.entry(key.to_owned()).or_default(),
            };
            queue.items.push_back(pending);
            queue.arrivals += 1;
            queue.last_arrival = Some(Instant::now());
            // trimmed here too: a sender blocked on a stalled page never reaches `next`, and the queue grew without bound
            let max_age = self.delivery.max_age_ms.map(|max_age_ms| crate::secs(max_age_ms / 1000.0));
            if let Some(limit) = max_age.or(self.delivery.reliable.then_some(RELIABLE_MAX_BACKLOG)) {
                let now = Instant::now();
                while queue.items.len() > 1 && queue.items.front().is_some_and(|item| now.duration_since(item.arrived) > limit) {
                    state.stats.unqueue(&queue.items.pop_front().unwrap());
                    *if max_age.is_some() { &mut state.stats.dropped_age } else { &mut state.stats.dropped_queue } += 1;
                }
            }
            state.payload_bytes = ewma(state.payload_bytes, payload_len as f64);
            if let Some(cap) = self.delivery.queue {
                while queue.items.len() > cap {
                    state.stats.unqueue(&queue.items.pop_front().unwrap());
                    state.stats.dropped_queue += 1;
                }
            }
        }
        self.data_ready.notify_one();
    }

    /// Next frame to send (highest priority, then oldest), or when to look again.
    pub fn pick(&self, now: Instant) -> (Option<(String, Pending)>, Option<Instant>) {
        let (key, wake_at) = self.next(now, true);
        (key.and_then(|(key, item)| Some((key, item?))), wake_at)
    }

    /// Whether `pick` would return a message now, or when to look again.
    fn ready(&self, now: Instant) -> (bool, Option<Instant>) {
        let (next, wake_at) = self.next(now, false);
        (next.is_some(), wake_at)
    }

    /// The key to send next and, when `take`, its sample (taken off the queue).
    fn next(&self, now: Instant, take: bool) -> (Option<(String, Option<Pending>)>, Option<Instant>) {
        let mut state = self.state.lock();
        let state = &mut *state;
        if let Some(max_age_ms) = self.delivery.max_age_ms {
            let max_age = crate::secs(max_age_ms / 1000.0);
            for queue in state.keys.values_mut() {
                while queue.items.front().is_some_and(|item| now.duration_since(item.arrived) > max_age) {
                    state.stats.unqueue(&queue.items.pop_front().unwrap());
                    state.stats.dropped_age += 1;
                }
            }
        } else if self.delivery.reliable {
            for queue in state.keys.values_mut() {
                while queue.items.len() > 1 && queue.items.front().is_some_and(|item| now.duration_since(item.arrived) > RELIABLE_MAX_BACKLOG) {
                    state.stats.unqueue(&queue.items.pop_front().unwrap());
                    state.stats.dropped_queue += 1;
                }
            }
        }
        let mut best: Option<(&String, (u8, Instant))> = None;
        let mut wake_at: Option<Instant> = None;
        for (key, queue) in state.keys.iter() {
            let Some(head) = queue.items.front() else {
                continue;
            };
            match (self.key_interval(queue, &state.allocation, state.probing), queue.last_sent) {
                (Spacing::Never, _) => continue,
                (Spacing::Every(interval), Some(last_sent)) if last_sent + interval > now => {
                    let ready_at = last_sent + interval;
                    wake_at = Some(wake_at.map_or(ready_at, |w| w.min(ready_at)));
                    continue;
                }
                // a video key held to fewer frames than its source sends: once its spacing is up, the next frame to
                // arrive goes rather than the one that waited out the spacing (up to a source interval old: at 2.7 of 5
                // Hz that was 100 ms p50 of the picture's age), unless no frame comes within a source interval
                (Spacing::Every(interval), Some(last_sent)) if matches!(self.channel, Channel::Video(_)) && queue.rate_hz > 0.0 && head.arrived < last_sent + interval => {
                    let fresh_by = last_sent + interval + crate::secs(1.0 / queue.rate_hz);
                    if now < fresh_by {
                        wake_at = Some(wake_at.map_or(fresh_by, |w| w.min(fresh_by)));
                        continue;
                    }
                }
                _ => {}
            }
            let rank = (head.priority, head.arrived);
            if best.as_ref().is_none_or(|(_, best_rank)| rank < *best_rank) {
                best = Some((key, rank));
            }
        }
        let Some((key, _)) = best else {
            return (None, wake_at);
        };
        let key = key.clone();
        if !take {
            return (Some((key, None)), wake_at);
        }
        let queue = state.keys.get_mut(&key).unwrap();
        let item = queue.items.pop_front().unwrap();
        queue.last_sent = Some(now);
        state.stats.unqueue(&item);
        (Some((key, Some(item))), wake_at)
    }

    /// Page consumed every frame up to `frame_id` (4-byte little endian message on the sub channel).
    fn ack(&self, frame_id: u32) {
        let mut state = self.state.lock();
        let mut acked = Vec::new();
        while let Some(&(sent_frame_id, len, order, _)) = state.unconsumed.front() {
            if !seq_at_or_before(sent_frame_id, frame_id) {
                break;
            }
            state.unconsumed.pop_front();
            state.stats.unconsumed_bytes -= len;
            acked.push(order);
        }
        drop(state);
        self.gate.frames_acked(acked);
    }

    /// Older than this subscription's maxAge (never, without one).
    fn expired(&self, item: &Pending, now: Instant) -> bool {
        self.delivery.max_age_ms.is_some_and(|max_age_ms| now.duration_since(item.arrived).as_secs_f64() * 1000.0 > max_age_ms)
    }

    /// Bytes on the wire per message, as last measured (0 before the first).
    fn typical_message_bytes(&self) -> f64 {
        self.state.lock().message_bytes
    }

    /// Waits for new data or until `wake_at` (a key's pacing), whichever comes first.
    pub async fn wait_for_data(&self, wake_at: Option<Instant>) {
        tokio::select! {
            _ = self.data_ready.notified() => {}
            _ = tokio::time::sleep_until(wake_at.unwrap_or_else(Instant::now).into()), if wake_at.is_some() => {}
        }
    }

    /// How long SCTP tries to deliver one of this channel's frames: the client opens lossy channels with
    /// maxPacketLifeTime = maxAge, else 1 s (`defaultPacketLifetimeMs`), at most 65535 ms; `None` when reliable.
    pub fn packet_lifetime(&self) -> Option<Duration> {
        (!self.delivery.reliable).then(|| Duration::from_millis(self.delivery.max_age_ms.unwrap_or(1000.0).clamp(0.0, 65535.0) as u64))
    }

    /// Sizes of what this subscription holds, for the stats' memory section.
    pub fn counts(&self) -> serde_json::Value {
        let state = self.state.lock();
        serde_json::json!({
            "keys": state.keys.len(),
            "queued": state.keys.values().map(|queue| queue.items.len()).sum::<usize>(),
            "queuedBytes": state.keys.values().flat_map(|queue| queue.items.iter()).map(|item| item.payload.len()).sum::<usize>(),
            "unacked": state.unconsumed.len(),
            "encodedSizes": state.encoded_bytes.len(),
        })
    }

    /// Ends the subscription: its sender stops and `run` returns, dropping the zenoh subscriber.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.data_ready.notify_one();
        self.closed_signal.send_replace(true);
    }
}

/// Ends a subscription when dropped (`run` cancelled mid-way skipped its cleanup, and the detached sender kept the
/// channel, its connection and its encoder alive).
struct Unsubscribe(Arc<SubShared>);

impl Drop for Unsubscribe {
    fn drop(&mut self) {
        self.0.close();
        self.0.release_frames();
    }
}

/// Runs a `sub` channel until it closes. Video and audio channels send frames to `track` instead of `dc`.
/// `rules`: a grant with an ACL, whose put/delete egress rules decide which samples reach the browser.
pub async fn run(dc: Arc<dyn DataChannel>, label: Label, session: zenoh::Session, shared: Arc<SubShared>, track: Option<crate::media::TrackClaim>, rules: Option<Arc<crate::Grant>>) {
    let feed = shared.clone();
    let subscriber = session
        .declare_subscriber(shared.zenoh_key(&label.key))
        .callback(move |sample| {
            if let Some(rules) = &rules {
                let message = if sample.kind() == SampleKind::Delete { crate::auth::AclMessage::Delete } else { crate::auth::AclMessage::Put };
                // dropped like zenoh's egress ACL drops it
                if !rules.passes(message, sample.key_expr().as_str()) {
                    return;
                }
            }
            feed.push(sample)
        })
        .advanced()
        .history(HistoryConfig::default().detect_late_publishers().max_samples(1))
        .await;
    let subscriber = match subscriber {
        Ok(subscriber) => subscriber,
        Err(error) => {
            warn!("subscribe {:?} failed: {error}", label.key);
            let _ = dc.close().await;
            return;
        }
    };
    debug!("subscribed {:?} with {:?}", label.key, shared.delivery);

    // however `run` ends, cancelled with its peer included, the sender stops and the gate lets go of its frames
    let _unsubscribe = Unsubscribe(shared.clone());
    let sending: std::pin::Pin<Box<dyn Future<Output = ()> + Send>> = match track {
        Some(track) if track.mime.starts_with("audio/") => Box::pin(crate::audio::send_loop(dc.clone(), shared.clone(), track)),
        Some(track) => Box::pin(crate::media::send_loop(dc.clone(), shared.clone(), track)),
        None => Box::pin(send_loop(dc.clone(), shared.clone())),
    };
    // a sender that panics ends this subscription (the loop below sees `closed`), not the gateway
    let closing = shared.clone();
    let sender = crate::spawn_isolated(format!("sender {:?}", label.key), sending, async move { closing.close() });
    // A browser that vanishes (killed, crashed, lid closed) never closes its channels; the peer is
    // dropped instead and closes this subscription, which must end it even though `poll` never
    // returns. Without this, every such browser left its streams decoding and encoding forever.
    let mut closed = shared.closed_signal.subscribe();
    loop {
        let event = tokio::select! {
            event = dc.poll() => event,
            _ = closed.wait_for(|closed| *closed) => None,
        };
        match event {
            None | Some(DataChannelEvent::OnClose) => break,
            Some(DataChannelEvent::OnMessage(message)) if message.data.len() == 4 => {
                shared.ack(u32::from_le_bytes([message.data[0], message.data[1], message.data[2], message.data[3]]));
            }
            Some(_) => {}
        }
    }
    shared.close();
    // its own unacked frames first: they could hold the window shut that its sender is waiting on
    shared.release_frames();
    let _ = sender.await;
    // and whatever it sent while finishing
    shared.release_frames();
    drop(subscriber);
    debug!("unsubscribed {:?}", label.key);
}

/// A message's bytes as sent: the sample payload (maybe compressed), or a transcoder's output.
enum Body<'a> {
    Raw(std::borrow::Cow<'a, [u8]>),
    Compressed(Vec<u8>),
    /// raw bytes behind a [`frame::META`] header
    Owned(Vec<u8>),
    Encoded(Arc<registry::Encoded>),
}

impl Body<'_> {
    fn bytes(&self) -> &[u8] {
        match self {
            Body::Raw(bytes) => bytes,
            Body::Compressed(bytes) => bytes,
            Body::Owned(bytes) => bytes,
            Body::Encoded(encoded) => &encoded.bytes,
        }
    }

    /// The frame header's flags.
    fn flags(&self) -> u8 {
        match self {
            Body::Compressed(_) => frame::ZSTD,
            Body::Encoded(encoded) if encoded.compressed => frame::ZSTD,
            _ => 0,
        }
    }
}

/// Whether a raw message carries the sample's encoding and attachment ([`frame::META`]): only when they say something.
fn needs_meta(item: &Pending) -> bool {
    item.attachment.is_some() || item.encoding != Encoding::default()
}

/// A raw payload (behind its [`frame::META`] header when [`needs_meta`]), zstd-compressed off the async runtime when
/// the subscription asks for it.
async fn raw_body(compress: Compress, item: &Pending) -> Body<'_> {
    let with_meta = needs_meta(item).then(|| {
        let attachment = item.attachment.as_ref().map(|attachment| attachment.to_bytes());
        frame::encode_meta(&item.encoding.to_string(), attachment.as_deref(), &item.payload.to_bytes())
    });
    if compress != Compress::None {
        let message = with_meta.clone().unwrap_or_else(|| item.payload.to_bytes().into_owned());
        if let Ok(Some(compressed)) = tokio::task::spawn_blocking(move || compress.apply(&message)).await {
            return Body::Compressed(compressed);
        }
    }
    match with_meta {
        Some(message) => Body::Owned(message),
        None => Body::Raw(item.payload.to_bytes()),
    }
}

/// Transcodes off the async runtime (shared with other frontends asking for the same encode).
async fn encode(shared: &SubShared, codec: Arc<dyn MessageEncoding>, key: &str, item: &Pending) -> Option<Arc<registry::Encoded>> {
    let quality = shared.current_quality();
    let options = shared.encode_options(quality);
    let payload = item.payload.to_bytes().into_owned();
    let (key, encoding, codecs, compress) = (key.to_owned(), item.encoding.clone(), shared.codecs.clone(), shared.compress);
    let timeout = codecs.encode_timeout;
    let encoding_task = tokio::task::spawn_blocking(move || {
        let sample = EncodingSample::new(&key, &payload, &encoding);
        codecs.encode_shared(&*codec, &sample, registry::sample_hash(&sample), &options, compress)
    });
    // (a hung encoder keeps its blocking thread; the sender, and the message turn it may hold, go on without it)
    let Ok(outcome) = tokio::time::timeout(timeout, encoding_task).await else {
        shared.record_encoding_error(&format!("encode took over {timeout:?}: skipped"));
        return None;
    };
    let error = match outcome {
        Ok((Ok(encoded), reused)) => {
            // the price model learns the bytes actually sent, after compression
            shared.record_encode(registry::quality_bucket(quality), quality, encoded.bytes.len(), reused);
            return Some(encoded);
        }
        Ok((Err(error), _)) => error,
        Err(error) => format!("encoder task failed: {error}"),
    };
    shared.record_encoding_error(&error);
    None
}

async fn send_loop(dc: Arc<dyn DataChannel>, shared: Arc<SubShared>) {
    let mut sender = MessageSender::new();
    while !shared.closed.load(Ordering::Relaxed) {
        let (ready, wake_at) = shared.ready(Instant::now());
        if !ready {
            shared.wait_for_data(wake_at).await;
            continue;
        }
        // a bulk stream whose messages span several chunks sends one whole message at a time across the frontend, and
        // picks its sample once it has the turn, so the one it sends is the newest
        let _turn = match shared.is_strict() || shared.typical_message_bytes() <= shared.gate.chunk_bytes() as f64 {
            true => None,
            false => tokio::select! {
                turn = shared.gate.message_turn() => Some(turn),
                () = shared.wait_closed() => return,
            },
        };
        let (next, _) = shared.pick(Instant::now());
        let Some((key, item)) = next else {
            continue;
        };
        // encode on send: only messages the pacing and queues let through get transcoded
        let body = match &shared.codec {
            // a delete has no payload to transcode: it goes as an empty raw message with its DELETE flag
            Some(codec) if item.kind != SampleKind::Delete => match encode(&shared, codec.clone(), &key, &item).await {
                Some(encoded) => Body::Encoded(encoded),
                None => continue,
            },
            _ => raw_body(shared.compress, &item).await,
        };
        let mut flags = body.flags() | if shared.output == EncodingOutput::Fields { frame::FIELDS } else { 0 };
        if item.kind == SampleKind::Delete {
            flags |= frame::DELETE;
        }
        if (shared.codec.is_none() || item.kind == SampleKind::Delete) && needs_meta(&item) {
            flags |= frame::META;
        }
        if sender.send(&dc, &shared, &key, &item, body.bytes(), flags).await == Sent::Closed {
            return;
        }
    }
}

/// What became of one message.
#[derive(Debug, PartialEq)]
pub enum Sent {
    Whole,
    /// abandoned part way (outlived maxAge) or a send failed
    Partial,
    /// the channel or subscription is gone
    Closed,
}

/// Sends whole messages on a subscription's channel: chunking, the frontend's send gate (bulk) or the strict tier, and
/// the stats.
pub struct MessageSender {
    next_frame_id: u32,
    warned_send_error: bool,
}

impl MessageSender {
    pub fn new() -> Self {
        MessageSender { next_frame_id: 0, warned_send_error: false }
    }

    pub async fn send(&mut self, dc: &Arc<dyn DataChannel>, shared: &SubShared, key: &str, item: &Pending, payload: &[u8], flags: u8) -> Sent {
        // strict: whole message at once, bulk held off meanwhile; bulk: small paced chunks
        let strict_turn = shared.is_strict().then(|| shared.gate.strict_turn());
        let chunk_bytes = if strict_turn.is_some() { frame::CHUNK_BYTES } else { shared.gate.chunk_bytes() };
        let chunk_count = frame::chunk_count(payload.len(), chunk_bytes);
        let mut message_bytes = 0usize;
        for chunk_index in 0..chunk_count {
            // a lossy channel stops a chunked message that outlived maxAge; otherwise it finishes
            // what it started, so big messages make progress even when newer ones keep arriving
            if chunk_index > 0 && !shared.delivery.reliable && shared.expired(item, Instant::now()) {
                shared.state.lock().stats.abandoned_partial += 1;
                return Sent::Partial;
            }
            let frame_id = self.next_frame_id;
            self.next_frame_id = self.next_frame_id.wrapping_add(1);
            let header = frame::FrameHeader { key, timestamp_ms: item.timestamp_ms, seq: item.seq, frame_id, chunk_index, chunk_count, flags };
            let frame = frame::encode(&header, frame::chunk(payload, chunk_index, chunk_bytes));
            let frame_len = frame.len();
            if strict_turn.is_none() {
                let started = Instant::now();
                let Some(on_window) = shared.bulk_turn(frame_len).await else {
                    return Sent::Closed;
                };
                let mut state = shared.state.lock();
                state.stats.blocked_ms += on_window.as_secs_f64() * 1000.0;
                state.stats.paced_ms += started.elapsed().saturating_sub(on_window).as_secs_f64() * 1000.0;
            }
            if let Err(error) = dc.send(frame).await {
                if shared.closed.load(Ordering::Relaxed) {
                    return Sent::Closed;
                }
                shared.state.lock().stats.dropped_send_error += 1;
                if !self.warned_send_error {
                    warn!("send on {key:?} failed ({frame_len} bytes): {error}");
                    self.warned_send_error = true;
                }
                return if matches!(error, webrtc::error::Error::ErrDataChannelClosed) { Sent::Closed } else { Sent::Partial };
            }
            message_bytes += frame_len;
            {
                let mut state = shared.state.lock();
                let order = shared.gate.frame_sent(shared.stream_id, frame_len + PACKET_OVERHEAD_BYTES as usize, shared.packet_lifetime());
                state.unconsumed.push_back((frame_id, frame_len, order, Instant::now()));
                state.stats.unconsumed_bytes += frame_len;
                state.stats.frames_sent += 1;
                state.stats.bytes_sent += frame_len as u64;
            }
            if shared.closed.load(Ordering::Relaxed) {
                return Sent::Closed;
            }
        }
        drop(strict_turn);
        let mut state = shared.state.lock();
        state.stats.sent += 1;
        state.stats.max_send_lag_ms = state.stats.max_send_lag_ms.max(now_unix_ms() - item.timestamp_ms);
        state.message_bytes = ewma(state.message_bytes, message_bytes as f64);
        Sent::Whole
    }
}

/// Sends one small frame (e.g. a video frame's metadata) without pacing; returns its size.
pub async fn send_small_frame(dc: &Arc<dyn DataChannel>, key: &str, timestamp_ms: f64, seq: u32, frame_id: u32, payload: &[u8]) -> Result<usize, webrtc::error::Error> {
    let frame = frame::encode(&frame::FrameHeader { key, timestamp_ms, seq, frame_id, chunk_index: 0, chunk_count: 1, flags: 0 }, payload);
    let length = frame.len();
    dc.send(frame).await.map(|_| length)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared(opts: &str) -> SubShared {
        let registry = Arc::new(EncodingRegistry::new([]).unwrap());
        SubShared::new(&SubOpts::parse(&serde_json::from_str(opts).unwrap()).unwrap(), Resolved { encoding: None, channel: Channel::Data, output: EncodingOutput::Data }, registry, Arc::default())
    }

    #[test]
    fn video_hz_is_max_hz_capped_at_the_source_rate() {
        let capped = shared(r#"{"maxHz":30}"#);
        assert_eq!(capped.key_hz("cam"), 30.0, "no measured rate yet");
        capped.state.lock().keys.entry("cam".into()).or_default().rate_hz = 4.5;
        assert_eq!(capped.key_hz("cam"), 4.5, "a 4.5 Hz source is encoded as 4.5 Hz, not 30");
        let uncapped = shared(r#"{}"#);
        uncapped.state.lock().keys.entry("cam".into()).or_default().rate_hz = 12.0;
        assert_eq!(uncapped.key_hz("cam"), 12.0);
        // retune changes it on the running subscription
        uncapped.retune(&SubOpts::parse(&serde_json::json!({"maxHz": 5})).unwrap());
        assert_eq!(uncapped.key_hz("cam"), 5.0);
    }

    #[test]
    fn decayed_rate_does_not_panic_under_constrained_allocation() {
        let sub = shared(r#"{}"#);
        {
            let mut state = sub.state.lock();
            let queue = state.keys.entry("map".into()).or_default();
            queue.rate_hz = 1e-30;
            queue.last_sent = Some(Instant::now());
            queue.items.push_back(pending(1, 3, Instant::now()));
            state.allocation = Allocation { constrained: true, hz_fraction: 0.5, ..Default::default() };
        }
        let _ = sub.pick(Instant::now());
    }

    #[test]
    fn strict_tier_by_option_or_published_priority() {
        assert!(shared(r#"{"priority":2}"#).is_strict());
        assert!(!shared(r#"{"priority":3}"#).is_strict());
        let unset = shared(r#"{}"#);
        assert!(!unset.is_strict(), "no sample yet");
        unset.sample_priority.store(1, Ordering::Relaxed);
        assert!(unset.is_strict());
    }

    fn pending(seq: u32, priority: u8, arrived: Instant) -> Pending {
        Pending {
            payload: ZBytes::from(vec![0u8; 10]),
            encoding: Encoding::default(),
            kind: SampleKind::Put,
            attachment: None,
            timestamp_ms: 0.0,
            received_ms: 0.0,
            seq,
            arrived,
            priority,
        }
    }

    fn insert(shared: &SubShared, key: &str, item: Pending, cap: Option<usize>) {
        let mut state = shared.state.lock();
        state.stats.queued += 1;
        state.stats.queued_bytes += item.payload.len();
        let queue = state.keys.entry(key.to_owned()).or_default();
        queue.items.push_back(item);
        if let Some(cap) = cap {
            while queue.items.len() > cap {
                queue.items.pop_front();
            }
        }
    }

    #[test]
    fn priority_then_age() {
        let shared = shared(r#"{"delivery":"reliable"}"#);
        let t0 = Instant::now();
        insert(&shared, "a/low", pending(0, 6, t0), None);
        insert(&shared, "a/high", pending(1, 2, t0 + Duration::from_millis(1)), None);
        let (first, _) = shared.pick(t0 + Duration::from_millis(2));
        assert_eq!(first.unwrap().0, "a/high");
        let (second, _) = shared.pick(t0 + Duration::from_millis(2));
        assert_eq!(second.unwrap().0, "a/low");
    }

    #[test]
    fn acks_take_frames_out_of_flight() {
        let shared = shared(r#"{"delivery": "reliable"}"#);
        {
            let mut state = shared.state.lock();
            for seq in [5u32, 6, 9] {
                state.unconsumed.push_back((seq, 100, shared.gate.frame_sent(0, 100, None), Instant::now()));
                state.stats.unconsumed_bytes += 100;
            }
        }
        shared.ack(6);
        assert_eq!((shared.stats().unconsumed_bytes, shared.gate.in_flight()), (100, 100));
        shared.ack(u32::MAX);
        assert_eq!(shared.gate.in_flight(), 100, "a seq far behind is not an ack");
        shared.ack(9);
        assert_eq!((shared.stats().unconsumed_bytes, shared.gate.in_flight()), (0, 0));
    }

    #[tokio::test]
    async fn a_closed_subscriptions_sender_stops_waiting_on_its_own_unacked_frames() {
        let shared = Arc::new(shared(r#"{"delivery": "reliable"}"#));
        shared.gate.configure(1e6, 1e6, 10_000, false);
        {
            // its own reliable frames fill the window, and the page will never ack them
            let mut state = shared.state.lock();
            let order = shared.gate.frame_sent(shared.stream_id, 20_000, None);
            state.unconsumed.push_back((0, 20_000, order, Instant::now()));
        }
        let waiting = tokio::spawn({
            let shared = shared.clone();
            async move { shared.bulk_turn(10_000).await }
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!waiting.is_finished(), "the window is full");
        shared.close();
        let turn = tokio::time::timeout(Duration::from_secs(1), waiting).await.expect("returns at once, not after stale_after").unwrap();
        assert_eq!(turn, None, "closed: no turn");
        shared.release_frames();
        assert_eq!((shared.gate.in_flight(), shared.counts()["unacked"].as_u64()), (0, Some(0)), "the gate lets go of its frames");
    }

    #[test]
    fn max_age_drops() {
        let shared = shared(r#"{"maxAge":100,"delivery":"reliable"}"#);
        let t0 = Instant::now();
        insert(&shared, "a/x", pending(0, 5, t0), None);
        let (next, _) = shared.pick(t0 + Duration::from_millis(500));
        assert!(next.is_none());
        assert_eq!(shared.stats().dropped_age, 1);
    }

    #[test]
    fn reliable_keeps_order_and_drops_only_a_backlog_older_than_its_limit() {
        let shared = shared(r#"{"delivery":"reliable"}"#);
        let t0 = Instant::now();
        for seq in 0..5 {
            insert(&shared, "tf", pending(seq, 5, t0 + Duration::from_millis(100 * seq as u64)), None);
        }
        // 2.25 s later the first three have waited past the limit; the newest of them would be kept if it were alone
        let now = t0 + RELIABLE_MAX_BACKLOG + Duration::from_millis(250);
        let seqs: Vec<u32> = std::iter::from_fn(|| shared.pick(now).0.map(|(_, item)| item.seq)).collect();
        assert_eq!(seqs, vec![3, 4], "in order, the stale ones dropped");
        assert_eq!(shared.stats().dropped_queue, 3);
        insert(&shared, "tf", pending(9, 5, t0), None);
        assert_eq!(shared.pick(now).0.map(|(_, item)| item.seq), Some(9), "a lone sample is never dropped for age");
    }

    #[tokio::test]
    async fn raw_payloads_compress_when_asked_and_when_it_shrinks() {
        let item = |payload: Vec<u8>| Pending { payload: ZBytes::from(payload), ..pending(0, 5, Instant::now()) };
        let text = item(b"zenoh ".repeat(1000));
        let body = raw_body(Compress::Zstd, &text).await;
        assert_eq!(body.flags(), frame::ZSTD);
        assert_eq!(zstd::bulk::decompress(body.bytes(), 6000).unwrap(), b"zenoh ".repeat(1000));
        assert_eq!(raw_body(Compress::None, &text).await.flags(), 0);
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let noise: Vec<u8> = (0..4096).map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        }).collect();
        let noisy = item(noise.clone());
        let body = raw_body(Compress::Zstd, &noisy).await;
        assert_eq!((body.flags(), body.bytes()), (0, &noise[..]), "incompressible bytes go as they are");
    }

    #[test]
    fn hz_cap_defers() {
        let shared = shared(r#"{"maxHz":10,"delivery":"reliable"}"#);
        let t0 = Instant::now();
        insert(&shared, "a/x", pending(0, 5, t0), None);
        insert(&shared, "a/x", pending(1, 5, t0), None);
        assert!(shared.pick(t0).0.is_some());
        let (next, wake_at) = shared.pick(t0 + Duration::from_millis(10));
        assert!(next.is_none());
        assert_eq!(wake_at, Some(t0 + Duration::from_millis(100)));
        assert!(shared.pick(t0 + Duration::from_millis(100)).0.is_some());
    }
}
