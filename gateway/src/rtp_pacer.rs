//! The RTP pacer (in place of rtc-interceptor's `PacerInterceptor`, at `Slot::Pacer`): the same token bucket, rated by
//! the congestion controller's target, but no packet waits in it longer than `MAX_QUEUE_AGE`. rtc-interceptor's held
//! up to 4096 packets (5 MB: 40 s at 1 Mbit/s) with no age limit, so after the link fell (8 Mbit/s to 500 kbit/s) the
//! frames encoded for the old rate, their retransmissions and the frames after them queued there for 11 s, and video
//! stayed 11 s behind. Here a packet that waited out `MAX_QUEUE_AGE` is dropped, the track it belonged to is asked for
//! a keyframe (the frames after a hole don't decode), a retransmission of a dropped packet is dropped too, and the
//! queue's delay is published for the video send loops, which stop encoding while it is high (`VideoRate::pacer_delay`).

use parking_lot::Mutex;
use rtc::interceptor::{Attribute, Interceptor, Packet, Pacer, StreamInfo, TaggedPacket};
use rtc::shared::marshal::MarshalSize;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// The longest an RTP packet waits for the bucket before it is dropped.
pub const MAX_QUEUE_AGE: Duration = Duration::from_millis(200);
/// The rate before the congestion controller's first target (rtc-interceptor's default).
const INITIAL_BPS: f64 = 1_000_000.0;
/// Dropped packets remembered, so their retransmissions are dropped too.
const DROPPED_MEMORY: usize = 4096;

/// What the pacer shares with the gateway: its backlog, and each video track's keyframe request by SSRC.
#[derive(Default)]
pub struct PacerShared {
    /// (bits queued, the bucket's rate in bits/s, the head packet's enqueue time, as of when)
    backlog: Mutex<(f64, f64, Option<Instant>, Option<Instant>)>,
    keyframe_flags: Mutex<HashMap<u32, Arc<AtomicBool>>>,
    dropped_packets: std::sync::atomic::AtomicU64,
}

impl PacerShared {
    /// A video track's SSRC and its keyframe request, raised when the pacer drops one of its packets.
    pub fn register_track(&self, ssrc: u32, keyframe_requested: Arc<AtomicBool>) {
        self.keyframe_flags.lock().insert(ssrc, keyframe_requested);
    }

    pub fn forget_track(&self, ssrc: u32) {
        self.keyframe_flags.lock().remove(&ssrc);
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

    fn drop_stale(&mut self, now: Instant) {
        let mut asked = HashSet::new();
        while let Some(head) = self.queue.front() {
            if now.saturating_duration_since(head.since) < MAX_QUEUE_AGE {
                break;
            }
            let queued = self.queue.pop_front().expect("front just checked");
            self.queued_bits -= queued.bits;
            self.shared.dropped_packets.fetch_add(1, Ordering::Relaxed);
            if let Packet::Rtp(rtp) = &queued.packet.message.packet {
                let ssrc = rtp.header.ssrc;
                self.remember_dropped((ssrc, rtp.header.sequence_number));
                if asked.insert(ssrc)
                    && let Some(flag) = self.shared.keyframe_flags.lock().get(&ssrc)
                {
                    flag.store(true, Ordering::Release);
                }
            }
        }
        if self.queue.is_empty() {
            self.queued_bits = 0.0;
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
        self.bucket.refill(message.now);
        let bits = (rtp.marshal_size() * 8) as f64;
        self.queued_bits += bits;
        let since = message.now;
        self.queue.push_back(Queued { packet: message, bits, since });
        self.publish(since);
        Ok(())
    }

    fn poll_write(&mut self) -> Option<TaggedPacket> {
        self.write_queue.pop_front().or_else(|| self.released.pop_front())
    }

    fn handle_timeout(&mut self, now: Instant) -> Result<(), Self::Error> {
        self.bucket.refill(now);
        self.drop_stale(now);
        while let Some(head) = self.queue.front() {
            if !self.bucket.can_release(head.bits) {
                break;
            }
            let Queued { mut packet, bits, .. } = self.queue.pop_front().expect("front just checked");
            self.bucket.consume(bits);
            self.queued_bits -= bits;
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
        let stale = head.since + MAX_QUEUE_AGE;
        Some(release.map_or(stale, |release| release.min(stale)))
    }
}

impl Interceptor for AgedPacer {
    fn bind_local_stream(&mut self, _info: &StreamInfo) {}
    fn unbind_local_stream(&mut self, _info: &StreamInfo) {}
    fn bind_remote_stream(&mut self, _info: &StreamInfo) {}
    fn unbind_remote_stream(&mut self, _info: &StreamInfo) {}
}
