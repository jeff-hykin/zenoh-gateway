//! `cargo run --release --example mic_server -- <serve dir>`: an isolated zenoh-gateway server on 127.0.0.1 serving
//! `<serve dir>`, for `client/test/microphone.ts`. Prints `listening on <url>`, then a JSON line per message a microphone
//! publisher put on `mic/**`, as a plain zenoh subscriber sees it: `{"key", "sampleRate", "channels", "samples", "rms"}`.
//! Its encoding `test-pcm` puts `u32 sampleRate | u8 channels | i16 samples...` (little endian) per packet.

use anyhow::{Result, anyhow, ensure};
use zenoh_gateway::{AudioPcm, Channel, DecodedFrame, EncodingOutput, EncodingSample, MessageEncoding, Server, zenoh};

struct Pcm;

impl MessageEncoding for Pcm {
    fn name(&self) -> &str {
        "test-pcm"
    }
    fn output(&self) -> EncodingOutput {
        EncodingOutput::Audio
    }
    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        let (rate, channels, samples) = parse(sample.payload)?;
        Ok(DecodedFrame::Audio(AudioPcm::new(rate, channels, samples)?))
    }
    fn encode_pcm(&self, pcm: &AudioPcm) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(5 + pcm.samples().len() * 2);
        out.extend_from_slice(&pcm.sample_rate().to_le_bytes());
        out.push(pcm.channels());
        for sample in pcm.samples() {
            out.extend_from_slice(&sample.to_le_bytes());
        }
        Ok(out)
    }
}

fn parse(bytes: &[u8]) -> Result<(u32, u8, Vec<i16>)> {
    ensure!(bytes.len() >= 5 && (bytes.len() - 5).is_multiple_of(2), "not a test-pcm message ({} bytes)", bytes.len());
    let rate = u32::from_le_bytes(bytes[..4].try_into()?);
    let samples = bytes[5..].as_chunks::<2>().0.iter().map(|pair| i16::from_le_bytes(*pair)).collect();
    Ok((rate, bytes[4], samples))
}

#[tokio::main]
async fn main() -> Result<()> {
    let serve_dir = std::env::args().nth(1).unwrap_or_else(|| ".".into());
    let mut config = zenoh::Config::default();
    config.insert_json5("scouting/multicast/enabled", "false").map_err(anyhow::Error::msg)?;
    config.insert_json5("listen/endpoints", "[]").map_err(anyhow::Error::msg)?;
    let session = zenoh::open(config).await.map_err(anyhow::Error::msg)?;
    let subscriber = session.declare_subscriber("mic/**").await.map_err(|error| anyhow!("{error}"))?;
    let running = Server::builder().session(session.clone()).serve_dir(serve_dir).encoding(Pcm).build().await?.bind("127.0.0.1:0").await?;
    println!("listening on http://{}", running.local_addr());
    tokio::spawn(async move {
        while let Ok(sample) = subscriber.recv_async().await {
            let line = match parse(&sample.payload().to_bytes()) {
                Ok((rate, channels, samples)) => {
                    let rms = (samples.iter().map(|&sample| (sample as f64).powi(2)).sum::<f64>() / samples.len().max(1) as f64).sqrt();
                    serde_json::json!({"key": sample.key_expr().as_str(), "sampleRate": rate, "channels": channels, "samples": samples.len(), "rms": rms})
                }
                Err(error) => serde_json::json!({"key": sample.key_expr().as_str(), "error": error.to_string()}),
            };
            println!("{line}");
        }
    });
    tokio::signal::ctrl_c().await?;
    running.shutdown().await
}
