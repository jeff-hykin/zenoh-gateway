//! `cargo run --example load_server -- <serve dir>`: an isolated zenoh-gateway server on 127.0.0.1 serving `<serve dir>`,
//! with a Controller-like load on its own session: three point clouds (200 KB at 10 Hz), a map (400 KB at 1 Hz) and a pose
//! (200 B at 20 Hz). Prints `listening on <url>`. For `client/test/backpressure.ts`.

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
    let running = Server::builder().session(session.clone()).serve_dir(serve_dir).build().await?.bind("127.0.0.1:0").await?;
    for (key, bytes, hz) in [("robot/cloud0", 200_000, 10.0), ("robot/cloud1", 200_000, 10.0), ("robot/cloud2", 200_000, 10.0), ("robot/map", 400_000, 1.0), ("robot/pose", 200, 20.0)] {
        publish(&session, key, bytes, hz);
    }
    println!("listening on http://{}", running.local_addr());
    tokio::signal::ctrl_c().await?;
    running.shutdown().await
}
