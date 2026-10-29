//! The RTP pacer (in place of rtc-interceptor's `PacerInterceptor`, at `Slot::Pacer`): the same token bucket, rated by
//! the congestion controller's target, but no frame waits in it longer than the connection's latency target
//! (`PacerShared::set_queue_limit`, from `ServerBuilder::latency_target` and the subscriptions' maxAge) past what it takes
//! to leave at the bucket's rate (its bits / the rate: a frame bigger than that much of the link can't be faster, and
//! with a flat 200 ms each such keyframe was dropped, which asked for another, which was dropped too: at 500 kbit/s 12
//! keyframes and 495 packets dropped in 10 s). rtc-interceptor's held
//! up to 4096 packets (5 MB: 40 s at 1 Mbit/s) with no age limit, so after the link fell (8 Mbit/s to 500 kbit/s) the
//! frames encoded for the old rate, their retransmissions and the frames after them queued there for 11 s, and video
//! stayed 11 s behind. Here a packet that waited out its frame's limit is dropped, the track it belonged to is asked for
//! a keyframe (the frames after a hole don't decode), a retransmission of a dropped packet is dropped too, and the
//! queue's delay is published for the video send loops, which stop encoding while it is high (`VideoRate::pacer_delay`).
//! As in libwebrtc's `PacingController`, a keyframe entering the queue flushes its stream's older packets there: they
//! are what it replaces, and it gets out sooner.

use parking_lot::Mutex;
use rtc::interceptor::{Attribute, Interceptor, Packet, Pacer, StreamInfo, TaggedPacket};
use rtc::shared::marshal::MarshalSize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// The rate before the congestion controller's first target (rtc-interceptor's default).
const INITIAL_BPS: f64 = 1_000_000.0;
/// Dropped packets remembered, so their retransmissions are dropped too.
const DROPPED_MEMORY: usize = 4096;

/// What the pacer shares with the gateway: its backlog, and each video track's keyframe request by SSRC.
pub struct PacerShared {
    /// the longest a frame waits for the bucket past its own time to leave (its bits / the rate) before it is dropped
    queue_limit: Mutex<Duration>,
    /// the rate the gateway set (bits/s), applied at the pacer's next tick: rated only by feedback, a pacer whose video
    /// had stopped heard none and drained at a stale 15 KB/s for 15 s
    rate_bps: Mutex<Option<f64>>,
    /// (bits queued, the bucket's rate in bits/s, the head packet's enqueue time, as of when)
    backlog: Mutex<(f64, f64, Option<Instant>, Option<Instant>)>,
    keyframe_flags: Mutex<HashMap<u32, Arc<AtomicBool>>>,
    /// each video track's latest keyframe by RTP timestamp (`None`: its first frame, whose timestamp isn't known yet)
    keyframes: Mutex<HashMap<u32, Option<u32>>>,
    dropped_packets: std::sync::atomic::AtomicU64,
}

impl Default for PacerShared {
    fn default() -> Self {
        PacerShared { queue_limit: Mutex::new(crate::allocator::DEFAULT_LATENCY_TARGET), rate_bps: Mutex::new(None), backlog: Default::default(), keyframe_flags: Default::default(), keyframes: Default::default(), dropped_packets: Default::default() }
    }
}

impl PacerShared {
    /// The connection's latency target: how long a frame may wait past its own time to leave.
    pub fn set_queue_limit(&self, limit: Duration) {
        *self.queue_limit.lock() = limit;
    }

    /// The bucket's rate (bits/s), as the congestion controller's feedback also sets it.
    pub fn set_rate(&self, bits_per_second: f64) {
        *self.rate_bps.lock() = Some(bits_per_second);
    }

    /// A video track's SSRC and its keyframe request, raised when the pacer drops one of its packets.
    pub fn register_track(&self, ssrc: u32, keyframe_requested: Arc<AtomicBool>) {
        self.keyframe_flags.lock().insert(ssrc, keyframe_requested);
    }

    pub fn forget_track(&self, ssrc: u32) {
        self.keyframe_flags.lock().remove(&ssrc);
        self.keyframes.lock().remove(&ssrc);
    }

    /// The frame about to be written on `ssrc` is a keyframe (`rtp_timestamp`: its RTP timestamp, `None` for a stream's
    /// first frame): it flushes the stream's older packets from the queue.
    pub fn mark_keyframe(&self, ssrc: u32, rtp_timestamp: Option<u32>) {
        self.keyframes.lock().insert(ssrc, rtp_timestamp);
    }

    /// How long a packet queued now would wait: what is queued drained at the bucket's rate since it was measured, or
    /// the head packet's wait so far when that is longer.
    pub fn delay(&self) -> Duration {
        let (bits, bps, head_since, at) = *self.backlog.lock();
        let Some(at) = at else { return Duration::ZERO };
        let now = Instant::now();
        let draining = (bits / bps.max(1.0) - now.saturating_duration_since(at).as_secs_f64()).max(0.0);
        let head = head_since.map_or(0.0, |since| now.saturating_duration_since(since).as_secs_f64());
        crate::secs(if bits > 0.0 { draining.max(head) } else { 0.0 })
    }

    /// Packets dropped for waiting too long, ever.
    pub fn dropped_packets(&self) -> u64 {
        self.dropped_packets.load(Ordering::Relaxed)
    }
}

struct Queued {
    packet: TaggedPacket,
    bits: f64,
    since: Instant,
    /// when it is dropped (`AgedPacer::deadline`)
    deadline: Instant,
    /// (SSRC, RTP timestamp): the frame it belongs to
    frame: (u32, u32),
}

pub struct AgedPacer {
    bucket: Pacer,
    queue: VecDeque<Queued>,
    queued_bits: f64,
    released: VecDeque<TaggedPacket>,
    read_queue: VecDeque<TaggedPacket>,
    write_queue: VecDeque<TaggedPacket>,
    dropped: HashSet<(u32, u16)>,
    dropped_order: VecDeque<(u32, u16)>,
    /// each queued frame's bits so far (its packets queued yet, sent or not) and packets still queued
    frames: HashMap<(u32, u32), (f64, usize)>,
    shared: Arc<PacerShared>,
}

impl AgedPacer {
    pub fn new(shared: Arc<PacerShared>) -> Self {
        AgedPacer {
            bucket: Pacer::new(INITIAL_BPS),
            queue: VecDeque::new(),
            queued_bits: 0.0,
            released: VecDeque::new(),
            read_queue: VecDeque::new(),
            write_queue: VecDeque::new(),
            dropped: HashSet::new(),
            dropped_order: VecDeque::new(),
            frames: HashMap::new(),
            shared,
        }
    }

    fn publish(&self, now: Instant) {
        *self.shared.backlog.lock() = (self.queued_bits, self.bucket.target_bitrate(), self.queue.front().map(|queued| queued.since), Some(now));
    }

    fn remember_dropped(&mut self, key: (u32, u16)) {
        if self.dropped.insert(key) {
            self.dropped_order.push_back(key);
            if self.dropped_order.len() > DROPPED_MEMORY
                && let Some(old) = self.dropped_order.pop_front()
            {
                self.dropped.remove(&old);
            }
        }
    }

    /// When a packet queued `now` is dropped: the queue limit, plus the time its frame up to it takes to leave at the
    /// bucket's rate now (set at entry: a rate that later fell, while no feedback came, stretched a frame's wait to 17 s).
    fn deadline(&self, now: Instant, frame_bits_so_far: f64) -> Instant {
        now + *self.shared.queue_limit.lock() + crate::secs(frame_bits_so_far / self.bucket.target_bitrate().max(1.0))
    }

    fn apply_rate(&mut self) {
        if let Some(bits_per_second) = self.shared.rate_bps.lock().take() {
            self.bucket.set_target_bitrate(bits_per_second);
        }
    }

    /// Accounts for `queued` leaving the queue (sent or dropped).
    fn dequeued(&mut self, queued: &Queued) {
        self.queued_bits -= queued.bits;
        if let Some((_, packets)) = self.frames.get_mut(&queued.frame) {
            *packets -= 1;
            if *packets == 0 {
                self.frames.remove(&queued.frame);
            }
        }
        if self.queue.is_empty() {
            self.queued_bits = 0.0;
        }
    }

    fn drop_packet(&mut self, queued: Queued) {
        self.dequeued(&queued);
        self.shared.dropped_packets.fetch_add(1, Ordering::Relaxed);
        if let Packet::Rtp(rtp) = &queued.packet.message.packet {
            self.remember_dropped((rtp.header.ssrc, rtp.header.sequence_number));
        }
    }

    fn drop_stale(&mut self, now: Instant) {
        let mut asked = HashSet::new();
        while let Some(head) = self.queue.front() {
            if now < head.deadline {
                break;
            }
            let queued = self.queue.pop_front().expect("front just checked");
            let ssrc = queued.frame.0;
            self.drop_packet(queued);
            if asked.insert(ssrc)
                && let Some(flag) = self.shared.keyframe_flags.lock().get(&ssrc)
            {
                flag.store(true, Ordering::Release);
            }
        }
    }

    /// A keyframe entering the queue: the stream's packets queued before it are what it replaces (libwebrtc's pacer does
    /// the same), dropped without asking for another keyframe.
    fn flush_before_keyframe(&mut self, ssrc: u32) {
        let (flushed, kept): (VecDeque<Queued>, VecDeque<Queued>) = std::mem::take(&mut self.queue).into_iter().partition(|queued| queued.frame.0 == ssrc);
        self.queue = kept;
        for queued in flushed {
            self.drop_packet(queued);
        }
    }
}

impl rtc::sansio::Protocol<TaggedPacket, TaggedPacket, ()> for AgedPacer {
    type Rout = TaggedPacket;
    type Wout = TaggedPacket;
    type Eout = ();
    type Error = rtc::shared::error::Error;
    type Time = Instant;

    fn handle_read(&mut self, message: TaggedPacket) -> Result<(), Self::Error> {
        // the congestion controller's target rides the feedback packet it came from (rtc-interceptor's pacer does the same)
        if let Some(Attribute::TargetBitrateChanged { bits_per_second }) = message.message.get(&Attribute::TargetBitrateChanged { bits_per_second: 0.0 }) {
            self.bucket.set_target_bitrate(*bits_per_second);
        }
        self.read_queue.push_back(message);
        Ok(())
    }

    fn poll_read(&mut self) -> Option<TaggedPacket> {
        self.read_queue.pop_front()
    }

    fn handle_write(&mut self, message: TaggedPacket) -> Result<(), Self::Error> {
        let Packet::Rtp(rtp) = &message.message.packet else {
            // RTCP is feedback, only useful fresh: never paced
            self.write_queue.push_back(message);
            return Ok(());
        };
        // a retransmission of a packet dropped for its age: what it belongs to is gone (a keyframe is on its way)
        if self.dropped.contains(&(rtp.header.ssrc, rtp.header.sequence_number)) {
            self.shared.dropped_packets.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.apply_rate();
        self.bucket.refill(message.now);
        let bits = (rtp.marshal_size() * 8) as f64;
        let frame = (rtp.header.ssrc, rtp.header.timestamp);
        let keyframe = {
            let mut keyframes = self.shared.keyframes.lock();
            match keyframes.get_mut(&rtp.header.ssrc) {
                Some(Some(timestamp)) => *timestamp == rtp.header.timestamp,
                Some(first) => {
                    *first = Some(rtp.header.timestamp);
                    true
                }
                None => false,
            }
        };
        if keyframe && !self.frames.contains_key(&frame) {
            self.flush_before_keyframe(frame.0);
        }
        let entry = self.frames.entry(frame).or_insert((0.0, 0));
        (entry.0, entry.1) = (entry.0 + bits, entry.1 + 1);
        let frame_bits_so_far = entry.0;
        self.queued_bits += bits;
        let since = message.now;
        let deadline = self.deadline(since, frame_bits_so_far);
        self.queue.push_back(Queued { packet: message, bits, since, deadline, frame });
        self.publish(since);
        Ok(())
    }

    fn poll_write(&mut self) -> Option<TaggedPacket> {
        self.write_queue.pop_front().or_else(|| self.released.pop_front())
    }

    fn handle_timeout(&mut self, now: Instant) -> Result<(), Self::Error> {
        self.apply_rate();
        self.bucket.refill(now);
        self.drop_stale(now);
        while let Some(head) = self.queue.front() {
            if !self.bucket.can_release(head.bits) {
                break;
            }
            let queued = self.queue.pop_front().expect("front just checked");
            self.bucket.consume(queued.bits);
            self.dequeued(&queued);
            let mut packet = queued.packet;
            // it departs now: the send history after this must see the release instant, not the enqueue one
            packet.now = now;
            self.released.push_back(packet);
        }
        if self.queue.is_empty() {
            self.queued_bits = 0.0;
        }
        self.publish(now);
        Ok(())
    }

    fn poll_timeout(&mut self) -> Option<Instant> {
        let head = self.queue.front()?;
        let release = self.bucket.releasable_at(head.bits);
        let stale = head.deadline;
        Some(release.map_or(stale, |release| release.min(stale)))
    }
}

impl Interceptor for AgedPacer {
    fn bind_local_stream(&mut self, _info: &StreamInfo) {}
    fn unbind_local_stream(&mut self, _info: &StreamInfo) {}
    fn bind_remote_stream(&mut self, _info: &StreamInfo) {}
    fn unbind_remote_stream(&mut self, _info: &StreamInfo) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtc::interceptor::AttributedPacket;
    use rtc::sansio::Protocol;
    use rtc::shared::TransportContext;

    const SSRC: u32 = 7;

    fn rtp(sequence_number: u16, timestamp: u32, now: Instant) -> TaggedPacket {
        let mut packet = rtc::rtp::Packet { payload: bytes::Bytes::from(vec![0; 1000]), ..Default::default() };
        (packet.header.ssrc, packet.header.sequence_number, packet.header.timestamp) = (SSRC, sequence_number, timestamp);
        TaggedPacket { now, transport: TransportContext::default(), message: AttributedPacket::new(Packet::Rtp(packet)) }
    }

    /// A frame bigger than the queue limit's worth of the link (default 300 ms) (5 packets, ~40 kbit, at 80 kbit/s: 0.5 s) leaves whole: its
    /// last packet necessarily waits its frame's time to leave.
    #[test]
    fn a_frame_bigger_than_the_age_limit_of_the_link_still_leaves() {
        let shared = Arc::new(PacerShared::default());
        let requested = Arc::new(AtomicBool::new(false));
        shared.register_track(SSRC, requested.clone());
        let mut pacer = AgedPacer::new(shared.clone());
        pacer.bucket.set_target_bitrate(80_000.0);
        let start = Instant::now();
        shared.mark_keyframe(SSRC, Some(100));
        for sequence_number in 0..5 {
            pacer.handle_write(rtp(sequence_number, 100, start)).unwrap();
        }
        let mut sent = 0;
        for ms in (0..=1000).step_by(10) {
            pacer.handle_timeout(start + Duration::from_millis(ms)).unwrap();
            while pacer.poll_write().is_some() {
                sent += 1;
            }
        }
        assert_eq!((sent, shared.dropped_packets()), (5, 0));
        assert!(!requested.load(Ordering::Acquire));
    }

    /// Nothing leaves (a 1 bit/s bucket, so a frame's own time to leave is ~8000 s): a keyframe flushes its stream's
    /// older packets without asking for another keyframe, and leaves other streams' alone.
    #[test]
    fn a_keyframe_flushes_its_stream_s_older_packets() {
        let shared = Arc::new(PacerShared::default());
        let requested = Arc::new(AtomicBool::new(false));
        shared.register_track(SSRC, requested.clone());
        let mut pacer = AgedPacer::new(shared.clone());
        pacer.bucket.set_target_bitrate(1.0);
        let start = Instant::now();
        let mut other = rtp(9, 50, start);
        if let Packet::Rtp(packet) = &mut other.message.packet {
            packet.header.ssrc = SSRC + 1;
        }
        pacer.handle_write(other).unwrap();
        pacer.handle_write(rtp(1, 100, start)).unwrap();
        pacer.handle_write(rtp(2, 100, start)).unwrap();
        shared.mark_keyframe(SSRC, Some(200));
        pacer.handle_write(rtp(3, 200, start)).unwrap();
        pacer.handle_write(rtp(4, 200, start)).unwrap();
        assert_eq!(shared.dropped_packets(), 2);
        assert_eq!(pacer.queue.iter().map(|queued| queued.frame).collect::<Vec<_>>(), [(SSRC + 1, 50), (SSRC, 200), (SSRC, 200)]);
        assert!(!requested.load(Ordering::Acquire));
    }

    /// A stream's first frame is marked before its RTP timestamp is known: the first timestamp seen is the keyframe's.
    #[test]
    fn a_stream_s_first_keyframe_is_its_first_timestamp() {
        let shared = Arc::new(PacerShared::default());
        shared.register_track(SSRC, Arc::default());
        let mut pacer = AgedPacer::new(shared.clone());
        pacer.bucket.set_target_bitrate(1.0);
        let start = Instant::now();
        shared.mark_keyframe(SSRC, None);
        pacer.handle_write(rtp(1, 500, start)).unwrap();
        pacer.handle_write(rtp(2, 500, start)).unwrap();
        pacer.handle_write(rtp(3, 600, start)).unwrap();
        assert_eq!(*shared.keyframes.lock().get(&SSRC).unwrap(), Some(500));
        // the next frame isn't a keyframe, so nothing is flushed
        assert_eq!(pacer.queue.len(), 3);
    }
}
