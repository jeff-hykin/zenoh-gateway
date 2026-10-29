//! `cargo run --example load_server -- <serve dir>`: an isolated zenoh-gateway server on 127.0.0.1 serving `<serve dir>`,
//! with a Controller-like load on its own session: three point clouds (CLOUD_BYTES, default 200 KB, at 10 Hz), a map
//! (MAP_BYTES, default 400 KB, at 1 Hz), a pose (200 B at 20 Hz) and tf (200 B at 100 Hz). Prints `listening on <url>`.
//! For `client/test/backpressure.ts`.

use anyhow::Result;
use std::time::Duration;
use zenoh_gateway::{Server, zenoh};

fn publish(session: &zenoh::Session, key: &'static str, bytes: usize, hz: f64) {
    let session = session.clone();
    tokio::spawn(async move {
        let payload = vec![7u8; bytes];
        let mut ticker = tokio::time::interval(Duration::from_secs_f64(1.0 / hz));
        loop {
            ticker.tick().await;
            if let Err(error) = session.put(key, payload.clone()).await {
                eprintln!("put {key} failed: {error}");
            }
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
    let running = Server::builder().session(session.clone()).serve_dir(serve_dir).build().await?.bind("127.0.0.1:0").await?;
    let bytes = |name: &str, default: usize| std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default);
    let (cloud, map) = (bytes("CLOUD_BYTES", 200_000), bytes("MAP_BYTES", 400_000));
    for (key, bytes, hz) in [("robot/cloud0", cloud, 10.0), ("robot/cloud1", cloud, 10.0), ("robot/cloud2", cloud, 10.0), ("robot/map", map, 1.0), ("robot/pose", 200, 20.0), ("robot/tf", 200, 100.0)] {
        publish(&session, key, bytes, hz);
    }
    println!("listening on http://{}", running.local_addr());
    tokio::signal::ctrl_c().await?;
    running.shutdown().await
}
