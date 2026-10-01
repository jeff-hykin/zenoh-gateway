//! One `sub` data channel: a zenoh AdvancedSubscriber feeding per-key delivery queues, drained
//! into the data channel only while it is not backed up.

use crate::allocator::{Allocation, Demand};
use crate::codec::registry::{self, CodecRegistry};
use crate::codec::{Codec, CodecOutput, CodecSample};
use crate::frame;
use crate::options::{Delivery, Label, SubOpts};
use crate::pacing::{PACING_SLACK, SendGate, TokenBucket};
use log::{debug, warn};
use serde::Serialize;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use zenoh::bytes::{Encoding, ZBytes};
use zenoh::sample::Sample;
use zenoh_ext::{AdvancedSubscriberBuilderExt, HistoryConfig};

/// Above this many unacknowledged bytes the channel counts as backed up.
pub const BACKED_UP_BYTES: usize = 64 * 1024;
/// Resume sending once the channel drains to this.
pub const RESUME_BYTES: usize = 32 * 1024;
/// Backstop re-check while backed up, in case a low-water event is missed.
const BACKSTOP: Duration = Duration::from_millis(20);
/// Max bytes sent but not yet acknowledged as consumed by the page's JS (see `ack`).
/// SCTP's buffered amount only covers the network: a busy browser main thread acks SCTP
/// on its network thread and then queues messages internally without limit.
pub const UNCONSUMED_WINDOW: usize = 256 * 1024;
/// A sent frame never acked for this long is assumed lost (lossy channels), not unconsumed.
const ASSUME_LOST_AFTER: Duration = Duration::from_secs(1);
/// Lossy channels blocked only by the ack window send one probe frame after this (doubling
/// while no ack comes back): if the window's tail was lost, the probe's ack releases it.
const FIRST_PROBE_AFTER: Duration = Duration::from_millis(50);
const MAX_PROBE_INTERVAL: Duration = Duration::from_secs(1);
/// An allocation never paces a key slower than this (a starved key still trickles).
const MIN_ALLOCATED_HZ: f64 = 0.05;
/// Weight of the newest sample in rate and size averages.
const EWMA_GAIN: f64 = 0.5;
/// Per-frame overhead on the wire besides the payload (frame header + key, SCTP/DTLS/UDP).
const FRAME_OVERHEAD_BYTES: f64 = 90.0;

#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubStats {
    pub received: u64,
    pub sent: u64,
    pub dropped_queue: u64,
    pub dropped_age: u64,
    pub dropped_send_error: u64,
    pub queued: usize,
    pub queued_bytes: usize,
    pub outstanding_bytes: usize,
    pub backed_up: bool,
    pub unconsumed_bytes: usize,
    /// frames (chunks) handed to SCTP; `sent` counts whole messages
    pub frames_sent: u64,
    /// lossy channel stopped sending a chunked message that outlived maxAge
    pub abandoned_partial: u64,
    /// lossy channel blocked by the ack window sent a frame anyway (tail-loss recovery)
    pub probes: u64,
    /// time the sender waited for SCTP to release bytes (network/peer receive path)
    pub blocked_on_network_ms: f64,
    /// time the sender waited only for the page to consume what it already has
    pub blocked_on_page_ms: f64,
    /// time a bulk sender waited on its token bucket and the frontend's send gate
    pub paced_ms: f64,
    /// Bridge receive time minus sample timestamp (upstream lag), largest seen.
    pub max_receive_lag_ms: f64,
    /// Time a message's last frame was handed to SCTP minus its sample timestamp, largest seen
    /// (upstream lag + queueing, pacing and encoding in the bridge).
    pub max_send_lag_ms: f64,
    /// frame bytes handed to SCTP (data channel) or video bytes handed to the track
    pub bytes_sent: u64,
    /// transcodes this subscription ran / reused from another frontend's identical request
    pub encodes: u64,
    pub shared_encodes: u64,
    pub codec_errors: u64,
    pub last_codec_error: Option<String>,
    /// quality of the last transcoded message
    pub quality: Option<f64>,
    pub keyframes: u64,
    /// PLI/FIR keyframe requests the browser sent on this stream's track
    pub keyframe_requests: u64,
    pub video_width: u32,
    pub video_height: u32,
    /// video: smoothed time to decode / to scale and encode one frame, and the quality ceiling
    /// the CPU governor holds (1 = none) so both fit the frame interval
    pub decode_ms: Option<f64>,
    pub encode_ms: Option<f64>,
    pub cpu_quality_cap: Option<f64>,
}

pub struct Pending {
    pub payload: ZBytes,
    pub encoding: Encoding,
    pub timestamp_ms: f64,
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
}

#[derive(Default)]
struct SubState {
    keys: HashMap<String, KeyQueue>,
    next_seq: u32,
    stats: SubStats,
    /// (frameId, frame bytes, sent at) in send order, until the page acks a frameId at or past it
    unconsumed: VecDeque<(u32, usize, Instant)>,
    /// acks that released something; lets a blocked sender notice progress
    acks_with_progress: u64,
    allocation: Allocation,
    payload_bytes: f64,
    /// bytes on the wire per message (untranscoded)
    message_bytes: f64,
    /// transcoded bytes per message, by quality bucket
    encoded_bytes: BTreeMap<u16, f64>,
    /// source resolution of a video stream
    video_source: Option<(u32, u32)>,
    /// counters as of the allocator's previous look
    accounted_bytes_sent: u64,
    accounted_blocked_ms: f64,
}

fn ewma(previous: f64, sample: f64) -> f64 {
    if previous <= 0.0 { sample } else { previous + EWMA_GAIN * (sample - previous) }
}

/// What the allocator learns from one subscription since its previous look.
pub struct Usage {
    pub demand: Demand,
    pub bytes_sent: u64,
    pub network_blocked_ms: f64,
    pub is_video: bool,
}

impl SubState {
    fn forget_lost(&mut self, now: Instant) {
        while let Some(&(_, len, sent_at)) = self.unconsumed.front() {
            if now.duration_since(sent_at) < ASSUME_LOST_AFTER {
                break;
            }
            self.unconsumed.pop_front();
            self.stats.unconsumed_bytes -= len;
        }
    }
}

/// `a <= b` for wrapping u32 sequence numbers.
fn seq_at_or_before(a: u32, b: u32) -> bool {
    b.wrapping_sub(a) < u32::MAX / 2
}

pub struct SubShared {
    delivery: Delivery,
    min_interval: Option<Duration>,
    priority_override: Option<u8>,
    pub codec: Option<Arc<dyn Codec>>,
    /// the server's codecs, with the caches that share work across frontends
    pub codecs: Arc<CodecRegistry>,
    max_hz: Option<f64>,
    min_hz: f64,
    weight: f64,
    quality_range: (f64, f64),
    tradeoff: f64,
    /// this frontend's shared send gate, and this stream's id in it
    gate: Arc<SendGate>,
    stream_id: usize,
    /// streams at this zenoh priority or more urgent bypass allocation and pacing (0 = none)
    strict_threshold: u8,
    /// published priority of the latest sample (used when the subscription sets none)
    sample_priority: AtomicU8,
    bucket: Mutex<TokenBucket>,
    state: Mutex<SubState>,
    data_ready: Notify,
    drained: Notify,
    closed: AtomicBool,
    /// flips to true on close, so `run` also stops when the channel never reports its own close
    closed_signal: tokio::sync::watch::Sender<bool>,
}

pub fn now_unix_ms() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64() * 1000.0).unwrap_or(0.0)
}

fn sample_timestamp_ms(sample: &Sample) -> f64 {
    sample
        .timestamp()
        .and_then(|ts| ts.get_time().to_system_time().duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs_f64() * 1000.0)
        .unwrap_or_else(now_unix_ms)
}

impl SubShared {
    pub fn new(opts: &SubOpts, codec: Option<Arc<dyn Codec>>, codecs: Arc<CodecRegistry>, gate: Arc<SendGate>, strict_threshold: u8) -> Self {
        static NEXT_STREAM: AtomicUsize = AtomicUsize::new(0);
        SubShared {
            gate,
            stream_id: NEXT_STREAM.fetch_add(1, Ordering::Relaxed),
            strict_threshold,
            sample_priority: AtomicU8::new(0),
            bucket: Mutex::new(TokenBucket::default()),
            delivery: opts.delivery(),
            min_interval: opts.min_interval(),
            priority_override: opts.zenoh_priority().map(|p| p as u8),
            codec,
            codecs,
            max_hz: opts.max_hz,
            min_hz: opts.dangerous_min_hz.unwrap_or(0.0),
            weight: opts.bandwidth_priority.unwrap_or(1.0),
            quality_range: opts.quality_range(),
            tradeoff: opts.quality_to_hz_tradeoff.unwrap_or(0.5),
            state: Mutex::new(SubState::default()),
            data_ready: Notify::new(),
            drained: Notify::new(),
            closed: AtomicBool::new(false),
            closed_signal: tokio::sync::watch::Sender::new(false),
        }
    }

    pub fn stats(&self) -> SubStats {
        self.state.lock().unwrap().stats.clone()
    }

    pub fn allocation(&self) -> Allocation {
        self.state.lock().unwrap().allocation.clone()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// Strict-priority tier: its priority (subscribe option, else as published) is at or above the
    /// bridge's threshold. Strict streams bypass allocation and pacing and preempt bulk streams.
    pub fn is_strict(&self) -> bool {
        let priority = self.priority_override.unwrap_or_else(|| self.sample_priority.load(Ordering::Relaxed));
        priority != 0 && priority <= self.strict_threshold
    }

    /// The subscription's minQuality.
    pub fn min_quality(&self) -> f64 {
        self.quality_range.0
    }

    /// Quality to transcode at now: the allocation's, or the best allowed before the first one.
    pub fn current_quality(&self) -> f64 {
        self.state.lock().unwrap().allocation.quality.unwrap_or(self.quality_range.1)
    }

    /// Granted Hz for one key of this stream (video encoders size their bitrate by it).
    pub fn key_hz(&self, key: &str) -> f64 {
        let state = self.state.lock().unwrap();
        let rate = state.keys.get(key).map_or(0.0, |queue| queue.rate_hz);
        self.allocated_key_hz(rate, &state.allocation).or(self.max_hz).unwrap_or(if rate > 0.0 { rate } else { 30.0 })
    }

    /// Per-key Hz cap from the allocation (None = no allocation cap).
    fn allocated_key_hz(&self, rate_hz: f64, allocation: &Allocation) -> Option<f64> {
        if self.delivery.reliable || self.is_strict() || !allocation.constrained || rate_hz <= 0.0 {
            return None;
        }
        let wanted = self.max_hz.map_or(rate_hz, |max| max.min(rate_hz));
        let floor = self.min_hz.min(wanted);
        Some((wanted * allocation.hz_fraction).max(floor).max(MIN_ALLOCATED_HZ))
    }

    /// Spacing between two sends of a key: maxHz, tightened by the allocation.
    fn key_interval(&self, queue: &KeyQueue, allocation: &Allocation) -> Option<Duration> {
        match self.allocated_key_hz(queue.rate_hz, allocation) {
            Some(hz) => Some(Duration::from_secs_f64(1.0 / hz).max(self.min_interval.unwrap_or_default())),
            None => self.min_interval,
        }
    }

    /// A video codec's subscription (frames go to a video track, not the data channel).
    pub fn is_video(&self) -> bool {
        self.codec.as_ref().is_some_and(|codec| codec.output() == CodecOutput::Video)
    }

    pub fn note_video_source(&self, width: u32, height: u32) {
        self.state.lock().unwrap().video_source = Some((width, height));
    }

    /// Measures rates since the previous call and describes what this stream wants.
    pub fn usage(&self, interval_secs: f64) -> Usage {
        let mut state = self.state.lock().unwrap();
        let (mut max_hz, mut min_hz) = (0.0, 0.0);
        for queue in state.keys.values_mut() {
            queue.rate_hz = ewma(queue.rate_hz, queue.arrivals as f64 / interval_secs.max(1e-3));
            queue.arrivals = 0;
            let wanted = self.max_hz.map_or(queue.rate_hz, |max| max.min(queue.rate_hz));
            max_hz += wanted;
            min_hz += self.min_hz.min(wanted);
        }
        let bytes_sent = state.stats.bytes_sent - state.accounted_bytes_sent;
        let network_blocked_ms = state.stats.blocked_on_network_ms - state.accounted_blocked_ms;
        state.accounted_bytes_sent = state.stats.bytes_sent;
        state.accounted_blocked_ms = state.stats.blocked_on_network_ms;
        let payload_bytes = state.payload_bytes;
        let price: crate::allocator::Price = match &self.codec {
            None => {
                let message_bytes = if state.message_bytes > 0.0 { state.message_bytes } else { payload_bytes + FRAME_OVERHEAD_BYTES };
                Box::new(move |_| message_bytes)
            }
            Some(codec) if codec.output() == CodecOutput::Video => {
                let (width, height) = state.video_source.unwrap_or((640, 480));
                Box::new(move |quality| crate::codec::video::bytes_per_frame(width, height, quality))
            }
            Some(codec) => {
                // measured sizes, scaled between qualities by the codec's own estimate
                let codec = codec.clone();
                let payload_len = payload_bytes.round() as usize;
                let measured: Vec<(f64, f64)> = state.encoded_bytes.iter().map(|(&bucket, &bytes)| (bucket as f64 / 1000.0, bytes)).collect();
                Box::new(move |quality| {
                    let estimate = |quality: f64| codec.estimated_bytes(payload_len, quality).max(0.0);
                    let nearest = measured.iter().min_by(|a, b| (a.0 - quality).abs().total_cmp(&(b.0 - quality).abs()));
                    match nearest {
                        Some(&(measured_quality, bytes)) => bytes * estimate(quality) / estimate(measured_quality).max(1e-9),
                        None => estimate(quality) + FRAME_OVERHEAD_BYTES,
                    }
                })
            }
        };
        let demand = Demand {
            weight: self.weight,
            max_hz,
            min_hz,
            quality_range: self.codec.as_ref().map(|_| self.quality_range),
            tradeoff: self.tradeoff,
            price,
            fixed_bytes_per_sec: (self.delivery.reliable || self.is_strict()).then(|| bytes_sent as f64 / interval_secs.max(1e-3)),
        };
        Usage { demand, bytes_sent, network_blocked_ms, is_video: self.is_video() }
    }

    pub fn apply(&self, allocation: Allocation) {
        // bulk streams trickle at their grant; strict and reliable ones aren't paced
        let paced = !self.delivery.reliable && !self.is_strict() && allocation.demand_bytes_per_sec > 0.0;
        let rate = paced.then_some(allocation.budget_bytes_per_sec * PACING_SLACK);
        self.bucket.lock().unwrap().set_rate(rate, 2 * self.gate.chunk_bytes());
        self.state.lock().unwrap().allocation = allocation;
        self.data_ready.notify_one();
    }

    fn record_encode(&self, bucket: u16, quality: f64, encoded_len: usize, shared: bool) {
        let mut state = self.state.lock().unwrap();
        let previous = state.encoded_bytes.get(&bucket).copied().unwrap_or(0.0);
        state.encoded_bytes.insert(bucket, ewma(previous, encoded_len as f64 + FRAME_OVERHEAD_BYTES));
        state.stats.quality = Some(quality);
        if shared {
            state.stats.shared_encodes += 1;
        } else {
            state.stats.encodes += 1;
        }
    }

    pub fn record_codec_error(&self, error: &str) {
        let mut state = self.state.lock().unwrap();
        state.stats.codec_errors += 1;
        if state.stats.last_codec_error.as_deref() != Some(error) {
            warn!("codec {}: {error}", self.codec.as_ref().map_or("?", |codec| codec.name()));
            state.stats.last_codec_error = Some(error.to_owned());
        }
    }

    /// The video pipeline's per-frame costs and the CPU governor's quality ceiling.
    pub fn record_video_timing(&self, decode_ms: Option<f64>, encode_ms: Option<f64>, cpu_quality_cap: f64) {
        let mut state = self.state.lock().unwrap();
        state.stats.decode_ms = decode_ms;
        state.stats.encode_ms = encode_ms;
        state.stats.cpu_quality_cap = Some(cpu_quality_cap);
    }

    /// A video frame went to the track.
    pub fn record_video_frame(&self, bytes: usize, (width, height): (u32, u32), quality: f64, keyframe: bool, shared_decode: bool, keyframe_requests: u64) {
        let mut state = self.state.lock().unwrap();
        state.stats.keyframe_requests = keyframe_requests;
        state.stats.bytes_sent += bytes as u64;
        state.stats.sent += 1;
        state.stats.video_width = width;
        state.stats.video_height = height;
        state.stats.quality = Some(quality);
        state.stats.keyframes += keyframe as u64;
        if shared_decode {
            state.stats.shared_encodes += 1;
        } else {
            state.stats.encodes += 1;
        }
    }

    fn push(&self, sample: Sample) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }
        let payload_len = sample.payload().len();
        self.sample_priority.store(sample.priority() as u8, Ordering::Relaxed);
        {
            let mut state = self.state.lock().unwrap();
            let seq = state.next_seq;
            state.next_seq = state.next_seq.wrapping_add(1);
            state.stats.received += 1;
            state.stats.queued += 1;
            state.stats.queued_bytes += payload_len;
            let timestamp_ms = sample_timestamp_ms(&sample);
            state.stats.max_receive_lag_ms = state.stats.max_receive_lag_ms.max(now_unix_ms() - timestamp_ms);
            let pending = Pending {
                encoding: sample.encoding().clone(),
                timestamp_ms,
                seq,
                arrived: Instant::now(),
                priority: self.priority_override.unwrap_or(sample.priority() as u8),
                payload: sample.payload().clone(),
            };
            let key = sample.key_expr().as_str();
            let state = &mut *state;
            let queue = match state.keys.get_mut(key) {
                Some(queue) => queue,
                None => state.keys.entry(key.to_owned()).or_default(),
            };
            queue.items.push_back(pending);
            queue.arrivals += 1;
            state.payload_bytes = ewma(state.payload_bytes, payload_len as f64);
            if let Some(cap) = self.delivery.queue {
                while queue.items.len() > cap {
                    let dropped = queue.items.pop_front().unwrap();
                    state.stats.dropped_queue += 1;
                    state.stats.queued -= 1;
                    state.stats.queued_bytes -= dropped.payload.len();
                }
            }
        }
        self.data_ready.notify_one();
    }

    /// Next frame to send (highest priority, then oldest), or when to look again.
    pub fn pick(&self, now: Instant) -> (Option<(String, Pending)>, Option<Instant>) {
        let mut state = self.state.lock().unwrap();
        let state = &mut *state;
        if let Some(max_age_ms) = self.delivery.max_age_ms {
            let max_age = Duration::from_secs_f64(max_age_ms / 1000.0);
            for queue in state.keys.values_mut() {
                while queue.items.front().is_some_and(|item| now.duration_since(item.arrived) > max_age) {
                    let dropped = queue.items.pop_front().unwrap();
                    state.stats.dropped_age += 1;
                    state.stats.queued -= 1;
                    state.stats.queued_bytes -= dropped.payload.len();
                }
            }
        }
        let mut best: Option<(&String, (u8, Instant))> = None;
        let mut wake_at: Option<Instant> = None;
        for (key, queue) in state.keys.iter() {
            let Some(head) = queue.items.front() else { continue };
            if let (Some(interval), Some(last_sent)) = (self.key_interval(queue, &state.allocation), queue.last_sent) {
                let ready_at = last_sent + interval;
                if ready_at > now {
                    wake_at = Some(wake_at.map_or(ready_at, |w| w.min(ready_at)));
                    continue;
                }
            }
            let rank = (head.priority, head.arrived);
            if best.as_ref().is_none_or(|(_, best_rank)| rank < *best_rank) {
                best = Some((key, rank));
            }
        }
        let Some((key, _)) = best else { return (None, wake_at) };
        let key = key.clone();
        let queue = state.keys.get_mut(&key).unwrap();
        let item = queue.items.pop_front().unwrap();
        queue.last_sent = Some(now);
        state.stats.queued -= 1;
        state.stats.queued_bytes -= item.payload.len();
        (Some((key, item)), wake_at)
    }

    /// Page consumed every frame up to `frame_id` (4-byte little endian message on the sub channel).
    fn ack(&self, frame_id: u32) {
        let mut state = self.state.lock().unwrap();
        let mut released = false;
        while let Some(&(sent_frame_id, len, _)) = state.unconsumed.front() {
            if !seq_at_or_before(sent_frame_id, frame_id) {
                break;
            }
            state.unconsumed.pop_front();
            state.stats.unconsumed_bytes -= len;
            released = true;
        }
        if released {
            state.acks_with_progress += 1;
        }
        drop(state);
        self.drained.notify_one();
    }

    /// Older than this subscription's maxAge (never, without one).
    fn expired(&self, item: &Pending, now: Instant) -> bool {
        self.delivery.max_age_ms.is_some_and(|max_age_ms| now.duration_since(item.arrived).as_secs_f64() * 1000.0 > max_age_ms)
    }

    fn unconsumed_bytes(&self) -> usize {
        let mut state = self.state.lock().unwrap();
        state.forget_lost(Instant::now());
        state.stats.unconsumed_bytes
    }

    /// Waits for new data or until `wake_at` (a key's pacing), whichever comes first.
    pub async fn wait_for_data(&self, wake_at: Option<Instant>) {
        match wake_at {
            Some(at) => {
                tokio::select! {
                    _ = self.data_ready.notified() => {}
                    _ = tokio::time::sleep_until(at.into()) => {}
                }
            }
            None => self.data_ready.notified().await,
        }
    }

    /// Ends the subscription: its sender stops and `run` returns, dropping the zenoh subscriber.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.data_ready.notify_one();
        self.drained.notify_one();
        self.closed_signal.send_replace(true);
    }
}

/// Runs a `sub` channel until it closes. Video codecs send frames to `video` instead of `dc`.
pub async fn run(dc: Arc<dyn DataChannel>, label: Label, session: zenoh::Session, shared: Arc<SubShared>, video: Option<Arc<crate::video::VideoTrack>>) {
    let _ = dc.set_buffered_amount_low_threshold(RESUME_BYTES as u32).await;
    let feed = shared.clone();
    let subscriber = session
        .declare_subscriber(label.key.clone())
        .callback(move |sample| feed.push(sample))
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

    shared.gate.register(shared.stream_id, dc.clone());
    let sender = match video {
        Some(track) => tokio::spawn(crate::video::send_loop(dc.clone(), shared.clone(), track)),
        None => tokio::spawn(send_loop(dc.clone(), shared.clone())),
    };
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
            Some(DataChannelEvent::OnBufferedAmountLow) => shared.drained.notify_one(),
            Some(DataChannelEvent::OnMessage(message)) if message.data.len() == 4 => {
                shared.ack(u32::from_le_bytes([message.data[0], message.data[1], message.data[2], message.data[3]]));
            }
            Some(_) => {}
        }
    }
    shared.close();
    let _ = sender.await;
    shared.gate.forget(shared.stream_id);
    drop(subscriber);
    debug!("unsubscribed {:?}", label.key);
}

/// Paces one channel: tracks SCTP backlog, the page's ack window and tail-loss probes.
struct Pacer {
    probe_interval: Duration,
    acks_seen: u64,
}

impl Pacer {
    /// After a frame went out: wait while the network or the page is behind.
    async fn wait_writable(&mut self, dc: &Arc<dyn DataChannel>, shared: &SubShared) {
        let mut outstanding = dc.outstanding_bytes().await.unwrap_or(0);
        let unconsumed = {
            let mut state = shared.state.lock().unwrap();
            state.stats.outstanding_bytes = outstanding;
            state.stats.unconsumed_bytes
        };
        if outstanding < BACKED_UP_BYTES && unconsumed < UNCONSUMED_WINDOW {
            return;
        }
        shared.state.lock().unwrap().stats.backed_up = true;
        let mut probe_at = Instant::now() + self.probe_interval;
        while (outstanding > RESUME_BYTES || shared.unconsumed_bytes() >= UNCONSUMED_WINDOW)
            && !shared.closed.load(Ordering::Relaxed)
        {
            let waiting_on_network = outstanding > RESUME_BYTES;
            let wait_started = Instant::now();
            tokio::select! {
                _ = shared.drained.notified() => {}
                _ = tokio::time::sleep(BACKSTOP) => {}
            }
            let waited_ms = wait_started.elapsed().as_secs_f64() * 1000.0;
            let acks_now = {
                let mut state = shared.state.lock().unwrap();
                if waiting_on_network {
                    state.stats.blocked_on_network_ms += waited_ms;
                } else {
                    state.stats.blocked_on_page_ms += waited_ms;
                }
                state.acks_with_progress
            };
            outstanding = dc.outstanding_bytes().await.unwrap_or(0);
            if acks_now != self.acks_seen {
                self.acks_seen = acks_now;
                self.probe_interval = FIRST_PROBE_AFTER;
                probe_at = Instant::now() + self.probe_interval;
            }
            let window_only = outstanding <= RESUME_BYTES;
            if window_only && !shared.delivery.reliable && Instant::now() >= probe_at {
                shared.state.lock().unwrap().stats.probes += 1;
                // if this probe isn't acked either, the page is busy rather than the tail lost
                self.probe_interval = (self.probe_interval * 2).min(MAX_PROBE_INTERVAL);
                break;
            }
        }
        let mut state = shared.state.lock().unwrap();
        state.stats.outstanding_bytes = outstanding;
        state.stats.backed_up = false;
    }
}

/// A message's bytes as sent: the sample payload, or a transcoder's output.
enum Body<'a> {
    Raw(std::borrow::Cow<'a, [u8]>),
    Encoded(Arc<Vec<u8>>),
}

impl Body<'_> {
    fn bytes(&self) -> &[u8] {
        match self {
            Body::Raw(bytes) => bytes,
            Body::Encoded(bytes) => bytes,
        }
    }
}

/// Transcodes off the async runtime (shared with other frontends asking for the same encode).
async fn encode(shared: &SubShared, codec: Arc<dyn Codec>, key: &str, item: &Pending) -> Option<Arc<Vec<u8>>> {
    let quality = shared.current_quality();
    let payload = item.payload.to_bytes().into_owned();
    let (key, encoding, codecs) = (key.to_owned(), item.encoding.clone(), shared.codecs.clone());
    let outcome = tokio::task::spawn_blocking(move || {
        let sample = CodecSample::new(&key, &payload, &encoding);
        codecs.encode_shared(&*codec, &sample, registry::sample_hash(&sample), quality)
    })
    .await;
    match outcome {
        Ok((Ok(encoded), reused)) => {
            shared.record_encode(registry::quality_bucket(quality), quality, encoded.len(), reused);
            Some(encoded)
        }
        Ok((Err(error), _)) => {
            shared.record_codec_error(&error);
            None
        }
        Err(error) => {
            shared.record_codec_error(&format!("encoder task failed: {error}"));
            None
        }
    }
}

async fn send_loop(dc: Arc<dyn DataChannel>, shared: Arc<SubShared>) {
    let mut warned_send_error = false;
    let mut pacer = Pacer { probe_interval: FIRST_PROBE_AFTER, acks_seen: 0 };
    let mut next_frame_id: u32 = 0;
    while !shared.closed.load(Ordering::Relaxed) {
        let (next, wake_at) = shared.pick(Instant::now());
        let Some((key, item)) = next else {
            shared.wait_for_data(wake_at).await;
            continue;
        };
        // encode on send: only messages the pacing and queues let through get transcoded
        let body = match &shared.codec {
            Some(codec) => match encode(&shared, codec.clone(), &key, &item).await {
                Some(encoded) => Body::Encoded(encoded),
                None => continue,
            },
            None => Body::Raw(item.payload.to_bytes()),
        };
        let payload = body.bytes();
        // strict: whole message at once, bulk held off meanwhile; bulk: small paced chunks
        let strict_turn = shared.is_strict().then(|| shared.gate.strict_turn());
        let chunk_bytes = if strict_turn.is_some() { frame::CHUNK_BYTES } else { shared.gate.chunk_bytes() };
        let chunk_count = frame::chunk_count(payload.len(), chunk_bytes);
        let mut completed = true;
        let mut message_bytes = 0usize;
        for chunk_index in 0..chunk_count {
            // a lossy channel stops a chunked message that outlived maxAge; otherwise it finishes
            // what it started, so big messages make progress even when newer ones keep arriving
            if chunk_index > 0 && !shared.delivery.reliable && shared.expired(&item, Instant::now()) {
                shared.state.lock().unwrap().stats.abandoned_partial += 1;
                completed = false;
                break;
            }
            let frame_id = next_frame_id;
            next_frame_id = next_frame_id.wrapping_add(1);
            let header = frame::FrameHeader { key: &key, timestamp_ms: item.timestamp_ms, seq: item.seq, frame_id, chunk_index, chunk_count };
            let frame = frame::encode(&header, frame::chunk(payload, chunk_index, chunk_bytes));
            let frame_len = frame.len();
            if strict_turn.is_none() {
                let wait = shared.bucket.lock().unwrap().take(frame_len, Instant::now());
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                let gated = shared.gate.wait_bulk_turn(shared.stream_id, frame_len).await;
                shared.state.lock().unwrap().stats.paced_ms += (wait + gated).as_secs_f64() * 1000.0;
            }
            if let Err(error) = dc.send(frame).await {
                if shared.closed.load(Ordering::Relaxed) {
                    return;
                }
                shared.state.lock().unwrap().stats.dropped_send_error += 1;
                if !warned_send_error {
                    warn!("send on {key:?} failed ({frame_len} bytes): {error}");
                    warned_send_error = true;
                }
                if matches!(error, webrtc::error::Error::ErrDataChannelClosed) {
                    return;
                }
                completed = false;
                break;
            }
            message_bytes += frame_len;
            {
                let mut state = shared.state.lock().unwrap();
                state.unconsumed.push_back((frame_id, frame_len, Instant::now()));
                state.stats.unconsumed_bytes += frame_len;
                state.stats.frames_sent += 1;
                state.stats.bytes_sent += frame_len as u64;
            }
            pacer.wait_writable(&dc, &shared).await;
            if strict_turn.is_none() {
                shared.gate.report_outstanding(shared.stream_id, shared.state.lock().unwrap().stats.outstanding_bytes);
            }
            if shared.closed.load(Ordering::Relaxed) {
                return;
            }
        }
        drop(strict_turn);
        if completed {
            let mut state = shared.state.lock().unwrap();
            state.stats.sent += 1;
            state.stats.max_send_lag_ms = state.stats.max_send_lag_ms.max(now_unix_ms() - item.timestamp_ms);
            state.message_bytes = ewma(state.message_bytes, message_bytes as f64);
        }
    }
}

/// Sends one small frame (e.g. a video frame's metadata) without pacing; returns its size.
pub async fn send_small_frame(dc: &Arc<dyn DataChannel>, key: &str, item: &Pending, frame_id: u32, payload: &[u8]) -> Result<usize, webrtc::error::Error> {
    let header = frame::FrameHeader { key, timestamp_ms: item.timestamp_ms, seq: item.seq, frame_id, chunk_index: 0, chunk_count: 1 };
    let frame = frame::encode(&header, payload);
    let length = frame.len();
    dc.send(frame).await.map(|_| length)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared(opts: &str) -> SubShared {
        let registry = Arc::new(CodecRegistry::new([]).unwrap());
        SubShared::new(&SubOpts::parse(&serde_json::from_str(opts).unwrap()).unwrap(), None, registry, Arc::default(), 2)
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
        Pending { payload: ZBytes::from(vec![0u8; 10]), encoding: Encoding::default(), timestamp_ms: 0.0, seq, arrived, priority }
    }

    fn insert(shared: &SubShared, key: &str, item: Pending, cap: Option<usize>) {
        let mut state = shared.state.lock().unwrap();
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
    fn ack_releases_window() {
        let shared = shared(r#"{}"#);
        let now = Instant::now();
        {
            let mut state = shared.state.lock().unwrap();
            for seq in [5u32, 6, 9] {
                state.unconsumed.push_back((seq, 100, now));
                state.stats.unconsumed_bytes += 100;
            }
        }
        shared.ack(6);
        assert_eq!(shared.unconsumed_bytes(), 100);
        shared.ack(u32::MAX);
        assert_eq!(shared.unconsumed_bytes(), 100, "a seq far behind is not an ack");
        shared.ack(9);
        assert_eq!(shared.unconsumed_bytes(), 0);
    }

    #[test]
    fn max_age_drops() {
        let shared = shared(r#"{"maxAge":100,"queueSize":null}"#);
        let t0 = Instant::now();
        insert(&shared, "a/x", pending(0, 5, t0), None);
        let (next, _) = shared.pick(t0 + Duration::from_millis(500));
        assert!(next.is_none());
        assert_eq!(shared.stats().dropped_age, 1);
    }

    #[test]
    fn hz_cap_defers() {
        let shared = shared(r#"{"maxHz":10,"queueSize":5}"#);
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
