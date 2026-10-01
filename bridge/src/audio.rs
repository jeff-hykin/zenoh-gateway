//! Audio subscriptions: PCM decoded by an audio codec, Opus-encoded in 20 ms packets and written
//! to a WebRTC audio track (`media` negotiates it). The browser's jitter buffer smooths arrival.

use crate::codec::{AudioPcm, DecodedFrame};
use crate::media::{MediaTrack, start_decode};
use crate::subscription::{self, SubShared};
use anyhow::{Result, ensure};
use std::sync::Arc;
use std::time::{Duration, Instant};
use unsafe_libopus::{OPUS_APPLICATION_AUDIO, OpusEncoder, opus_encode, opus_encoder_create, opus_encoder_destroy};
use webrtc::data_channel::DataChannel;

const PACKET: Duration = Duration::from_millis(20);

/// libopus (transpiled to Rust, so it builds wherever Rust does) for one rate and channel count.
struct Opus {
    encoder: *mut OpusEncoder,
    sample_rate: u32,
    channels: u8,
}

// SAFETY: the encoder state is owned by this value and only used through `&mut self`
unsafe impl Send for Opus {}

impl Opus {
    fn new(sample_rate: u32, channels: u8) -> Result<Self> {
        let mut error = 0;
        // SAFETY: AudioPcm guarantees a rate and channel count libopus accepts; the result is checked
        let encoder = unsafe { opus_encoder_create(sample_rate as i32, channels as i32, OPUS_APPLICATION_AUDIO, &mut error) };
        ensure!(!encoder.is_null() && error == 0, "opus_encoder_create failed ({error})");
        Ok(Opus { encoder, sample_rate, channels })
    }

    /// Samples (all channels) in one packet.
    fn packet_samples(&self) -> usize {
        (self.sample_rate / 50) as usize * self.channels as usize
    }

    /// One packet's worth of interleaved samples to an Opus packet.
    fn encode(&mut self, samples: &[i16]) -> Result<Vec<u8>> {
        let mut packet = vec![0u8; 1500];
        // SAFETY: `samples` holds one 20 ms frame of `channels` interleaved samples; `packet` is writable for its length
        let length = unsafe { opus_encode(self.encoder, samples.as_ptr(), (samples.len() / self.channels as usize) as i32, packet.as_mut_ptr(), packet.len() as i32) };
        ensure!(length > 0, "opus_encode failed ({length})");
        packet.truncate(length as usize);
        Ok(packet)
    }
}

impl Drop for Opus {
    fn drop(&mut self) {
        // SAFETY: created by opus_encoder_create, destroyed once
        unsafe { opus_encoder_destroy(self.encoder) }
    }
}

/// Sends an audio subscription: decode each picked sample to PCM, cut it into 20 ms Opus packets
/// (a remainder waits for the next sample) and write them to the track; a small frame per sample
/// on the data channel delivers the message to the page.
pub async fn send_loop(dc: Arc<dyn DataChannel>, shared: Arc<SubShared>, track: Arc<MediaTrack>) {
    let Some(codec) = shared.codec.clone() else { return };
    let mut opus: Option<Opus> = None;
    let mut pending: Vec<i16> = Vec::new();
    let mut frame_id: u32 = 0;
    while !shared.is_closed() {
        let (picked, wake_at) = shared.pick(Instant::now());
        let Some((key, item)) = picked else {
            shared.wait_for_data(wake_at).await;
            continue;
        };
        let decoding = start_decode(&shared, &codec, key, item);
        let decoded = match decoding.task.await {
            Ok((Ok((decoded, _)), _)) => decoded,
            Ok((Err(error), _)) => {
                shared.record_codec_error(&error);
                continue;
            }
            Err(error) => {
                shared.record_codec_error(&format!("decoder task failed: {error}"));
                continue;
            }
        };
        let DecodedFrame::Audio(pcm) = &*decoded else {
            shared.record_codec_error(&format!("audio codec {:?} decoded to {:?}, not PCM", codec.name(), decoded));
            continue;
        };
        match send_pcm(&mut opus, &mut pending, pcm, &track).await {
            Ok(bytes) => shared.record_media_bytes(bytes),
            Err(error) => shared.record_codec_error(&format!("{error:#}")),
        }
        if subscription::send_small_frame(&dc, &decoding.key, &decoding.item, frame_id, &[]).await.is_err() && shared.is_closed() {
            break;
        }
        frame_id = frame_id.wrapping_add(1);
    }
    track.release();
}

/// Adds `pcm` to `pending` and writes every whole packet; returns the bytes written.
async fn send_pcm(opus: &mut Option<Opus>, pending: &mut Vec<i16>, pcm: &AudioPcm, track: &MediaTrack) -> Result<usize> {
    if opus.as_ref().is_none_or(|opus| (opus.sample_rate, opus.channels) != (pcm.sample_rate(), pcm.channels())) {
        *opus = Some(Opus::new(pcm.sample_rate(), pcm.channels())?);
        pending.clear();
    }
    let opus = opus.as_mut().expect("created above");
    pending.extend_from_slice(pcm.samples());
    let mut bytes = 0;
    let packet_samples = opus.packet_samples();
    while pending.len() >= packet_samples {
        let packet = opus.encode(&pending[..packet_samples])?;
        pending.drain(..packet_samples);
        bytes += packet.len();
        track.write(packet, PACKET).await?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_twenty_millisecond_packets() {
        let mut opus = Opus::new(48000, 1).unwrap();
        assert_eq!(opus.packet_samples(), 960);
        let tone: Vec<i16> = (0..960).map(|index| ((index as f64 * 400.0 / 48000.0 * std::f64::consts::TAU).sin() * 8000.0) as i16).collect();
        let packet = opus.encode(&tone).unwrap();
        assert!(!packet.is_empty() && packet.len() < 400, "{} bytes", packet.len());
        assert_eq!(Opus::new(16000, 2).unwrap().packet_samples(), 640);
        assert!(AudioPcm::new(44100, 1, vec![]).is_err(), "Opus can't take 44.1 kHz");
        assert!(AudioPcm::new(48000, 2, vec![0; 3]).is_err(), "half a stereo frame");
    }
}
