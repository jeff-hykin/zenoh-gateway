//! Microphone publishers: the browser's audio arrives as Opus RTP on a sendonly transceiver, is decoded with libopus to
//! PCM, and the publisher's encoding (`encode_pcm`) turns each packet into a message put on its key, with the same
//! lease, deadman and latency rules as a data-channel publisher.

use crate::encoding::{AudioPcm, MessageEncoding};
use crate::options::{Label, PubOpts};
use crate::publisher::{self, Admission, PubShared};
use anyhow::{Result, ensure};
use log::debug;
use std::sync::Arc;
use std::time::Instant;
use unsafe_libopus::{OpusDecoder, opus_decode, opus_decoder_create, opus_decoder_destroy, opus_packet_get_nb_channels, opus_packet_get_nb_samples};
use webrtc::data_channel::{DataChannel, DataChannelEvent};
use webrtc::media_stream::track_remote::{TrackRemote, TrackRemoteEvent};

/// The Opus RTP clock (RFC 7587): 48 kHz whatever rate the audio was captured at.
const OPUS_RTP_CLOCK_HZ: f64 = 48000.0;

/// libopus's decoder for one output rate and channel count.
struct Decoder {
    decoder: *mut OpusDecoder,
    sample_rate: u32,
    channels: u8,
}

// SAFETY: the decoder state is owned by this value and only used through `&mut self`
unsafe impl Send for Decoder {}

impl Decoder {
    fn new(sample_rate: u32, channels: u8) -> Result<Self> {
        let mut error = 0;
        // SAFETY: the rate is one of AudioPcm::SAMPLE_RATES (PubOpts checks it) and channels 1 or 2; the result is checked
        let decoder = unsafe { opus_decoder_create(sample_rate as i32, channels as i32, &mut error) };
        ensure!(!decoder.is_null() && error == 0, "opus_decoder_create failed ({error})");
        Ok(Decoder { decoder, sample_rate, channels })
    }

    /// One Opus packet to interleaved PCM at this decoder's rate and channel count.
    fn decode(&mut self, packet: &[u8]) -> Result<AudioPcm> {
        ensure!(!packet.is_empty(), "empty Opus packet");
        // SAFETY: `packet` is readable for its length
        let frames = unsafe { opus_packet_get_nb_samples(packet.as_ptr(), packet.len() as i32, self.sample_rate as i32) };
        ensure!(frames > 0, "malformed Opus packet ({frames})");
        let mut samples = vec![0i16; frames as usize * self.channels as usize];
        // SAFETY: `samples` holds `frames` frames of `channels` samples, as the call is told
        let decoded = unsafe { opus_decode(self.decoder, packet.as_ptr(), packet.len() as i32, samples.as_mut_ptr(), frames, 0) };
        ensure!(decoded > 0, "opus_decode failed ({decoded})");
        samples.truncate(decoded as usize * self.channels as usize);
        AudioPcm::new(self.sample_rate, self.channels, samples)
    }
}

impl Drop for Decoder {
    fn drop(&mut self) {
        // SAFETY: created by opus_decoder_create, destroyed once
        unsafe { opus_decoder_destroy(self.decoder) }
    }
}

/// How late each packet is next to the stream's least-delayed one: arrival time minus RTP time, against the smallest
/// such difference seen (the one-way delay variation; the browser's capture clock and ours needn't agree).
#[derive(Default)]
struct Lateness {
    started: Option<Instant>,
    /// the last RTP timestamp, and the unwrapped count before it
    last_rtp: Option<(u32, i64)>,
    least_ms: Option<f64>,
}

impl Lateness {
    fn of(&mut self, rtp_timestamp: u32, arrived: Instant) -> f64 {
        let started = *self.started.get_or_insert(arrived);
        let unwrapped = match self.last_rtp {
            Some((last, unwrapped)) => unwrapped + rtp_timestamp.wrapping_sub(last) as i32 as i64,
            None => 0,
        };
        self.last_rtp = Some((rtp_timestamp, unwrapped));
        let offset_ms = arrived.duration_since(started).as_secs_f64() * 1000.0 - unwrapped as f64 / OPUS_RTP_CLOCK_HZ * 1000.0;
        let least = self.least_ms.map_or(offset_ms, |least| least.min(offset_ms));
        self.least_ms = Some(least);
        offset_ms - least
    }
}

/// A browser track and the encoding that turns its audio into messages.
pub struct Microphone {
    pub track: Arc<dyn TrackRemote>,
    pub encoding: Arc<dyn MessageEncoding>,
}

/// Publishes the browser's microphone track: each Opus packet decoded, encoded by its encoding and put on the key, until
/// the channel or the track closes.
pub async fn run(dc: Arc<dyn DataChannel>, label: Label, opts: PubOpts, session: zenoh::Session, shared: Arc<PubShared>, microphone: Microphone, blocker: impl Fn() -> Option<String>) {
    let Microphone { track, encoding } = microphone;
    let Some(zenoh_publisher) = publisher::declare(&dc, &label, &opts, &session).await else { return };
    let sample_rate = opts.sample_rate.unwrap_or(OPUS_RTP_CLOCK_HZ as u32);
    let mut decoder: Option<Decoder> = None;
    let mut admission = Admission::default();
    let mut lateness = Lateness::default();
    loop {
        let packet = tokio::select! {
            event = track.poll() => match event {
                Some(TrackRemoteEvent::OnRtpPacket(packet)) => packet,
                Some(TrackRemoteEvent::OnEnded) | None => break,
                Some(_) => continue,
            },
            event = dc.poll() => match event {
                Some(DataChannelEvent::OnClose) | None => break,
                Some(_) => continue,
            },
        };
        if !admission.admit(&dc, &shared, &blocker).await {
            continue;
        }
        let late_ms = lateness.of(packet.header.timestamp, Instant::now());
        {
            let mut stats = shared.stats.lock();
            stats.last_age_ms = Some(late_ms);
            if opts.latency_limit.is_some_and(|limit| late_ms > limit) {
                stats.dropped_stale += 1;
                continue;
            }
        }
        let payload = packet.payload.to_vec();
        if payload.is_empty() {
            continue;
        }
        // SAFETY: `payload` is non-empty, and the TOC byte is all this reads
        let channels = unsafe { opus_packet_get_nb_channels(payload.as_ptr()) } as u8;
        if decoder.as_ref().is_none_or(|decoder| decoder.channels != channels) {
            match Decoder::new(sample_rate, channels) {
                Ok(created) => decoder = Some(created),
                Err(error) => {
                    malformed(&shared, &label, &error);
                    continue;
                }
            }
        }
        let pcm = match decoder.as_mut().expect("created above").decode(&payload) {
            Ok(pcm) => pcm,
            Err(error) => {
                malformed(&shared, &label, &error);
                continue;
            }
        };
        let encoder = encoding.clone();
        let message = match tokio::task::spawn_blocking(move || encoder.encode_pcm(&pcm)).await {
            Ok(Ok(message)) => message,
            Ok(Err(error)) => {
                malformed(&shared, &label, &error);
                continue;
            }
            Err(error) => {
                malformed(&shared, &label, &anyhow::anyhow!("encode_pcm task failed: {error}"));
                continue;
            }
        };
        publisher::put(&zenoh_publisher, message, &shared).await;
    }
    debug!("microphone publisher {:?} closed", label.key);
}

/// Resolves when the channel closes (a microphone's channel carries nothing from the browser).
pub async fn closed(dc: &Arc<dyn DataChannel>) {
    while let Some(event) = dc.poll().await {
        if matches!(event, DataChannelEvent::OnClose) {
            break;
        }
    }
}

fn malformed(shared: &PubShared, label: &Label, error: &anyhow::Error) {
    let mut stats = shared.stats.lock();
    stats.malformed += 1;
    if stats.malformed == 1 {
        log::warn!("microphone publisher {:?}: {error:#}", label.key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use unsafe_libopus::{OPUS_APPLICATION_VOIP, opus_encode, opus_encoder_create, opus_encoder_destroy};

    fn opus_packet(channels: u8) -> Vec<u8> {
        let mut error = 0;
        // SAFETY: valid rate and channel count; checked below
        let encoder = unsafe { opus_encoder_create(48000, channels as i32, OPUS_APPLICATION_VOIP, &mut error) };
        assert!(!encoder.is_null() && error == 0);
        let tone: Vec<i16> = (0..960 * channels as usize).map(|index| ((index as f64 * 440.0 / 48000.0 * std::f64::consts::TAU).sin() * 8000.0) as i16).collect();
        let mut packet = vec![0u8; 1500];
        // SAFETY: 960 frames of `channels` samples in, a writable buffer out
        let length = unsafe { opus_encode(encoder, tone.as_ptr(), 960, packet.as_mut_ptr(), packet.len() as i32) };
        // SAFETY: created above, destroyed once
        unsafe { opus_encoder_destroy(encoder) };
        assert!(length > 0);
        packet.truncate(length as usize);
        packet
    }

    #[test]
    fn decodes_a_packet_at_the_asked_rate() {
        let packet = opus_packet(1);
        // SAFETY: non-empty packet
        assert_eq!(unsafe { opus_packet_get_nb_channels(packet.as_ptr()) }, 1);
        let pcm = Decoder::new(48000, 1).unwrap().decode(&packet).unwrap();
        assert_eq!((pcm.sample_rate(), pcm.channels(), pcm.samples().len()), (48000, 1, 960));
        let pcm = Decoder::new(16000, 1).unwrap().decode(&packet).unwrap();
        assert_eq!(pcm.samples().len(), 320, "20 ms at 16 kHz");
        let pcm = Decoder::new(48000, 2).unwrap().decode(&opus_packet(2)).unwrap();
        assert_eq!((pcm.channels(), pcm.samples().len()), (2, 1920));
        assert!(Decoder::new(48000, 1).unwrap().decode(&[]).is_err());
    }

    #[test]
    fn lateness_is_against_the_least_delayed_packet() {
        let mut lateness = Lateness::default();
        let start = Instant::now();
        let at = |ms: u64| start + Duration::from_millis(ms);
        // RTP timestamps start anywhere (here near the wrap); 960 ticks = 20 ms
        let base = u32::MAX - 960;
        assert_eq!(lateness.of(base, at(0)), 0.0);
        assert_eq!(lateness.of(base.wrapping_add(960), at(20)), 0.0);
        assert!((lateness.of(base.wrapping_add(1920), at(140)) - 100.0).abs() < 1e-6, "a packet 100 ms behind");
        // an earlier-than-ever packet resets the baseline
        assert_eq!(lateness.of(base.wrapping_add(2880), at(50)), 0.0);
    }
}
