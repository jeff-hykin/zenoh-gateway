//! `cargo run --release --example degrade_server -- <serve dir>`: an isolated zenoh-gateway server on 127.0.0.1 serving
//! `<serve dir>` with a Controller-like load that can be degraded, for `client/test/degrade.ts`. Prints `listening on <url>`.
//! - robot/cam0, robot/cam1 (CAMS of them): `test-cam`, a 640x480 moving picture at CAM_HZ (30), for video channels;
//! - robot/cloud0, robot/cloud1: `test-cloud`, CLOUD_BYTES (200 KB) of 12-byte points at 10 Hz, quality q keeping 1 point
//!   in round(1/q) (at most 16);
//! - robot/map: raw MAP_BYTES (400 KB) at 1 Hz; robot/pose 200 B at 20 Hz; robot/tf 200 B at 100 Hz; robot/cmd 64 B at
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

struct Cam;

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
        let (width, height) = (640u32, 480u32);
        let mut data = vec![0u8; (width * height * 3) as usize];
        let mut seed = frame.wrapping_mul(2654435761) | 1;
        for y in 0..height {
            for x in 0..width {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                let noise = (seed & 31) as u32;
                let moving = ((x + frame * 4) / 32 + (y + frame * 2) / 32) % 2;
                let i = ((y * width + x) * 3) as usize;
                data[i] = ((x * 255 / width) as u32 + noise).min(255) as u8;
                data[i + 1] = ((y * 255 / height) as u32 + noise).min(255) as u8;
                data[i + 2] = (moving * 200 + noise).min(255) as u8;
            }
        }
        Ok(DecodedFrame::Video(VideoImage::rgb8(width, height, data)?))
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

#[tokio::main]
async fn main() -> Result<()> {
    let serve_dir = std::env::args().nth(1).unwrap_or_else(|| ".".into());
    let mut config = zenoh::Config::default();
    config.insert_json5("scouting/multicast/enabled", "false").map_err(anyhow::Error::msg)?;
    config.insert_json5("listen/endpoints", "[]").map_err(anyhow::Error::msg)?;
    let session = zenoh::open(config).await.map_err(anyhow::Error::msg)?;
    let running = Server::builder().session(session.clone()).serve_dir(serve_dir).encoding(Cam).encoding(Cloud).build().await?.bind("127.0.0.1:0").await?;
    let env = |name: &str, default: f64| std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default);
    let (cloud, map, cam_hz) = (env("CLOUD_BYTES", 200_000.0) as usize, env("MAP_BYTES", 400_000.0) as usize, env("CAM_HZ", 30.0));
    let cams = env("CAMS", 2.0) as usize;
    for key in ["robot/cam0", "robot/cam1"].into_iter().take(cams) {
        publish(&session, key, 4, cam_hz);
    }
    for (key, bytes, hz) in [("robot/cloud0", cloud, 10.0), ("robot/cloud1", cloud, 10.0), ("robot/map", map, 1.0), ("robot/pose", 200, 20.0), ("robot/tf", 200, 100.0), ("robot/cmd", 64, 20.0)] {
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
