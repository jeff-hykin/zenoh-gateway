//! Embeds the zenoh-web server in an application that brings its own zenoh session and two
//! external codecs:
//!
//! - `text-uppercase` (data): UTF-8 text, upper-cased; lower quality keeps a shorter prefix. The
//!   page decodes it with `registerCodec("text-uppercase", (bytes) => new TextDecoder().decode(bytes))`.
//! - `rgb-swatch` (video): a 3-byte payload `r g b` becomes a 64x48 picture of that color (built
//!   as I420), sent as H.264 on a video track; the page needs no decoder.
//!
//! ```sh
//! cargo run --example custom_codec -- --connect tcp/127.0.0.1:7447 --serve ../examples
//! ```

use anyhow::{Context, Result, ensure};
use clap::Parser;
use std::path::PathBuf;
use zenoh_web::{Codec, CodecOutput, CodecSample, DecodedFrame, Server, VideoImage};

/// Upper-cases UTF-8 text. Quality q keeps the first `ceil(q × length)` characters.
struct TextUppercase;

impl Codec for TextUppercase {
    fn name(&self) -> &str {
        "text-uppercase"
    }

    fn output(&self) -> CodecOutput {
        CodecOutput::Data
    }

    fn decode(&self, sample: &CodecSample<'_>) -> Result<DecodedFrame> {
        let text = std::str::from_utf8(sample.payload).context("text-uppercase needs UTF-8")?;
        Ok(DecodedFrame::data(text.to_uppercase()))
    }

    fn encode(&self, frame: &DecodedFrame, quality: f64) -> Result<Vec<u8>> {
        let text = frame.downcast::<String>()?;
        let keep = (text.chars().count() as f64 * quality.clamp(0.0, 1.0)).ceil() as usize;
        Ok(text.chars().take(keep).collect::<String>().into_bytes())
    }

    fn estimated_bytes(&self, payload_bytes: usize, quality: f64) -> f64 {
        payload_bytes as f64 * quality.clamp(0.05, 1.0)
    }
}

/// A 3-byte RGB payload as a solid 64x48 picture, handed to the bridge as I420 (BT.601).
struct RgbSwatch;

impl Codec for RgbSwatch {
    fn name(&self) -> &str {
        "rgb-swatch"
    }

    fn output(&self) -> CodecOutput {
        CodecOutput::Video
    }

    fn decode(&self, sample: &CodecSample<'_>) -> Result<DecodedFrame> {
        let &[red, green, blue] = sample.payload else { anyhow::bail!("rgb-swatch needs 3 bytes, got {}", sample.payload.len()) };
        let (red, green, blue) = (red as f64, green as f64, blue as f64);
        let luma = 16.0 + 0.257 * red + 0.504 * green + 0.098 * blue;
        let u = 128.0 - 0.148 * red - 0.291 * green + 0.439 * blue;
        let v = 128.0 + 0.439 * red - 0.368 * green - 0.071 * blue;
        let (width, height) = (64usize, 48usize);
        let mut data = vec![luma.round() as u8; width * height];
        data.extend(std::iter::repeat_n(u.round() as u8, width * height / 4));
        data.extend(std::iter::repeat_n(v.round() as u8, width * height / 4));
        Ok(DecodedFrame::Video(VideoImage::i420(width as u32, height as u32, data)?))
    }
}

#[derive(Parser)]
struct Cli {
    /// HTTP port (0 = any free port).
    #[arg(long, default_value_t = zenoh_web::DEFAULT_PORT)]
    port: u16,
    /// zenoh config file (json5) for the application's session.
    #[arg(long)]
    zenoh_config: Option<PathBuf>,
    /// zenoh endpoint to connect to (repeatable).
    #[arg(long)]
    connect: Vec<String>,
    /// Directory to serve over HTTP.
    #[arg(long)]
    serve: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info,zenoh=warn,zenoh_web=info,rtc=warn,webrtc=warn")).init();
    let cli = Cli::parse();

    // the host application's own zenoh session, shared with zenoh-web
    let mut config = match &cli.zenoh_config {
        Some(path) => zenoh_web::zenoh::Config::from_file(path).map_err(|error| anyhow::anyhow!("{error}"))?,
        None => zenoh_web::zenoh::Config::default(),
    };
    if !cli.connect.is_empty() {
        config.insert_json5("connect/endpoints", &serde_json::to_string(&cli.connect)?).map_err(|error| anyhow::anyhow!("{error}"))?;
    }
    let session = zenoh_web::zenoh::open(config).await.map_err(|error| anyhow::anyhow!("{error}"))?;

    let mut builder = Server::builder().session(session.clone()).codec(TextUppercase).codec(RgbSwatch);
    if let Some(dir) = cli.serve {
        builder = builder.serve_dir(dir);
    }
    let running = builder.build().await?.bind(("0.0.0.0", cli.port)).await?;
    ensure!(running.local_addr().port() != 0, "bound to port 0");

    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
    // deadmen fire and browsers disconnect; the session is ours to close
    running.shutdown().await?;
    session.close().await.map_err(|error| anyhow::anyhow!("{error}"))?;
    Ok(())
}
