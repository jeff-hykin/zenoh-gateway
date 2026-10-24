//! `cargo run --release --example degrade_server -- <serve dir>`: an isolated zenoh-gateway server on 127.0.0.1 serving
//! `<serve dir>` with a Controller-like load that can be degraded, for `client/test/degrade.ts`. Prints `listening on <url>`.
//! - robot/cam0, robot/cam1 (CAMS of them): `test-cam`, a CAM_SIZE (640x480) moving picture at CAM_HZ (30) with per-pixel
//!   noise of CAM_NOISE levels (32: hard to compress; a real camera's is a few), for video channels;
//! - robot/cloud0, robot/cloud1: `test-cloud`, CLOUD_BYTES (200 KB) of 12-byte points at 10 Hz, quality q keeping 1 point
//!   in round(1/q) (at most 16);
//! - robot/map: raw MAP_BYTES (400 KB) at MAP_HZ (1); robot/pose 200 B at 20 Hz; robot/tf 200 B at 100 Hz; robot/cmd 64 B at
//!   20 Hz;
//! - robot/cmd_vel_echo: every robot/cmd_vel put, republished as is (the page's commands, round trip).

use anyhow::Result;

// DHAT=1 builds (`--features dhat-heap`): the heap's live bytes every 30 s on stderr, and per allocation site in
// dhat-heap.json (DHAT_FILE) on SIGINT/SIGTERM; view it at https://nnethercote.github.io/dh_view/dh_view.html
#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;
use std::time::Duration;
use zenoh_gateway::{Channel, DecodedFrame, EncodeOptions, EncodingOutput, EncodingSample, MessageEncoding, Server, VideoImage, zenoh};

struct Cam {
    width: u32,
    height: u32,
    /// noise levels added to each pixel (a power of two)
    noise: u32,
}

impl MessageEncoding for Cam {
    fn name(&self) -> &str {
        "test-cam"
    }
    fn output(&self) -> EncodingOutput {
        EncodingOutput::Video
    }
    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        let p = sample.payload;
        let frame = u32::from_le_bytes([p[0], p[1], p[2], p[3]]);
        let (width, height) = (self.width, self.height);
        let mut data = vec![0u8; (width * height * 3) as usize];
        let mut seed = frame.wrapping_mul(2654435761) | 1;
        for y in 0..height {
            for x in 0..width {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                let noise = seed & (self.noise - 1);
                let moving = ((x + frame * 4) / 32 + (y + frame * 2) / 32) % 2;
                let i = ((y * width + x) * 3) as usize;
                data[i] = ((x * 255 / width) as u32 + noise).min(255) as u8;
                data[i + 1] = ((y * 255 / height) as u32 + noise).min(255) as u8;
                data[i + 2] = (moving * 200 + noise).min(255) as u8;
            }
        }
        // (a message header as an image codec's: client/test/video_latency.ts reads it back per shown frame)
        Ok(DecodedFrame::Video(VideoImage::rgb8(width, height, data)?.with_header(frame as f64 / 30.0, "test_cam_optical")))
    }
}

struct Cloud;

impl MessageEncoding for Cloud {
    fn name(&self) -> &str {
        "test-cloud"
    }
    fn output(&self) -> EncodingOutput {
        EncodingOutput::Data
    }
    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        Ok(DecodedFrame::data(sample.payload.to_vec()))
    }
    fn encode(&self, frame: &DecodedFrame, options: &EncodeOptions) -> Result<Vec<u8>> {
        let bytes = frame.downcast::<Vec<u8>>()?;
        let keep_every = (1.0 / options.quality.max(1.0 / 16.0)).round().clamp(1.0, 16.0) as usize;
        Ok(bytes.chunks(12).step_by(keep_every).flatten().copied().collect())
    }
    fn estimated_bytes(&self, payload_bytes: usize, options: &EncodeOptions) -> f64 {
        payload_bytes as f64 / (1.0 / options.quality.max(1.0 / 16.0)).round().clamp(1.0, 16.0)
    }
}

fn publish(session: &zenoh::Session, key: &'static str, bytes: usize, hz: f64) {
    let session = session.clone();
    tokio::spawn(async move {
        let mut payload = vec![7u8; bytes.max(4)];
        let mut ticker = tokio::time::interval(Duration::from_secs_f64(1.0 / hz));
        let mut frame = 0u32;
        loop {
            ticker.tick().await;
            payload[..4].copy_from_slice(&frame.to_le_bytes());
            frame = frame.wrapping_add(1);
            let _ = session.put(key, payload.clone()).await;
        }
    });
}

/// The test camera CAM_SIZE (WxH) and CAM_NOISE say.
fn cam() -> Result<Cam> {
    let size = std::env::var("CAM_SIZE").unwrap_or_else(|_| "640x480".into());
    let (width, height) = size.split_once('x').and_then(|(w, h)| Some((w.parse().ok()?, h.parse().ok()?))).ok_or_else(|| anyhow::anyhow!("CAM_SIZE must be WxH, not {size}"))?;
    let noise: u32 = std::env::var("CAM_NOISE").ok().and_then(|value| value.parse().ok()).unwrap_or(32);
    anyhow::ensure!(noise.is_power_of_two(), "CAM_NOISE must be a power of two");
    Ok(Cam { width, height, noise })
}

#[tokio::main]
async fn main() -> Result<()> {
    let serve_dir = std::env::args().nth(1).unwrap_or_else(|| ".".into());
    let mut config = zenoh::Config::default();
    config.insert_json5("scouting/multicast/enabled", "false").map_err(anyhow::Error::msg)?;
    config.insert_json5("listen/endpoints", "[]").map_err(anyhow::Error::msg)?;
    // samples carry a zenoh timestamp, as a timestamping publisher's do (video latency is measured from it)
    config.insert_json5("timestamping/enabled", "true").map_err(anyhow::Error::msg)?;
    let session = zenoh::open(config).await.map_err(anyhow::Error::msg)?;
    let running = Server::builder().session(session.clone()).serve_dir(serve_dir).encoding(cam()?).encoding(Cloud).build().await?.bind("127.0.0.1:0").await?;
    let env = |name: &str, default: f64| std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default);
    let (cloud, map, cam_hz) = (env("CLOUD_BYTES", 200_000.0) as usize, env("MAP_BYTES", 400_000.0) as usize, env("CAM_HZ", 30.0));
    let (cams, map_hz) = (env("CAMS", 2.0) as usize, env("MAP_HZ", 1.0));
    for key in ["robot/cam0", "robot/cam1"].into_iter().take(cams) {
        publish(&session, key, 4, cam_hz);
    }
    for (key, bytes, hz) in [("robot/cloud0", cloud, 10.0), ("robot/cloud1", cloud, 10.0), ("robot/map", map, map_hz), ("robot/pose", 200, 20.0), ("robot/tf", 200, 100.0), ("robot/cmd", 64, 20.0)] {
        publish(&session, key, bytes, hz);
    }
    let echo = session.clone();
    let _cmd_vel = session
        .declare_subscriber("robot/cmd_vel")
        .callback(move |sample| {
            let (echo, payload) = (echo.clone(), sample.payload().to_bytes().to_vec());
            tokio::spawn(async move {
                let _ = echo.put("robot/cmd_vel_echo", payload).await;
            });
        })
        .await
        .map_err(anyhow::Error::msg)?;
    println!("listening on http://{}", running.local_addr());
    #[cfg(feature = "dhat-heap")]
    let profiler = dhat::Profiler::builder().file_name(std::env::var("DHAT_FILE").unwrap_or_else(|_| "dhat-heap.json".into())).build();
    #[cfg(feature = "dhat-heap")]
    tokio::spawn(async {
        let started = std::time::Instant::now();
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let stats = dhat::HeapStats::get();
            eprintln!("heap t={:.0}s live {:.1} MB in {} blocks (peak {:.1} MB)", started.elapsed().as_secs_f64(), stats.curr_bytes as f64 / 1e6, stats.curr_blocks, stats.max_bytes as f64 / 1e6);
        }
    });
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = terminate.recv() => {}
    }
    let result = running.shutdown().await;
    #[cfg(feature = "dhat-heap")]
    drop(profiler);
    result
}
