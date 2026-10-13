//! Browsers that connect and go, over and over (phones on a flaky link reconnect for hours): every connection's state
//! must go with it (`cargo test --features client --test leak`). Its own test binary: the live count is process-wide.

use anyhow::Result;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use zenoh_gateway::client::{Client, ClientOptions, Delivery, Message, PublisherOptions, SubscribeOptions};
use zenoh_gateway::{Channel, DecodedFrame, EncodingOutput, EncodingSample, MessageEncoding, Server, VideoImage, zenoh};

/// `[r, g, b, width u16, height u16]` → a solid picture.
struct SolidColor;

impl MessageEncoding for SolidColor {
    fn name(&self) -> &str {
        "test-solid"
    }

    fn output(&self) -> EncodingOutput {
        EncodingOutput::Video
    }

    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        let p = sample.payload;
        let (width, height) = (u16::from_le_bytes([p[3], p[4]]) as u32, u16::from_le_bytes([p[5], p[6]]) as u32);
        Ok(DecodedFrame::Video(VideoImage::rgb8(width, height, p[..3].repeat((width * height) as usize))?))
    }
}

fn solid(rgb: [u8; 3], width: u16, height: u16) -> Vec<u8> {
    [&rgb[..], &width.to_le_bytes(), &height.to_le_bytes()].concat()
}

/// How a page leaves.
#[derive(Debug, Clone, Copy)]
enum Leave {
    /// `close()`
    Close,
    /// dropped without a word
    Drop,
    /// heartbeats stop first (a frozen tab), then it goes
    Frozen,
}

async fn wait_for(what: &str, limit: Duration, done: impl Fn() -> bool) {
    let started = Instant::now();
    while !done() {
        assert!(started.elapsed() < limit, "{what}: still {} live peer states after {limit:?} (RSS {:.0} MB)", zenoh_gateway::live_peer_states(), rss_mb());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// This process's resident set, in MB.
fn rss_mb() -> f64 {
    let out = std::process::Command::new("ps").args(["-o", "rss=", "-p", &std::process::id().to_string()]).output().unwrap();
    String::from_utf8_lossy(&out.stdout).trim().parse::<f64>().unwrap_or(0.0) / 1024.0
}

#[tokio::test(flavor = "multi_thread")]
async fn connections_that_come_and_go_leave_nothing_behind() {
    let mut config = zenoh::Config::default();
    config.insert_json5("scouting/multicast/enabled", "false").unwrap();
    config.insert_json5("listen/endpoints", "[]").unwrap();
    let session = zenoh::open(config).await.unwrap();
    let server = Server::builder().session(session.clone()).encoding(SolidColor).build().await.unwrap();
    let running = server.bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", running.local_addr());
    let putter = {
        let session = session.clone();
        tokio::spawn(async move {
            loop {
                session.put("camera/front", solid([200, 40, 90], 320, 240)).await.unwrap();
                session.put("map/local", vec![7u8; 200_000]).await.unwrap();
                tokio::time::sleep(Duration::from_millis(33)).await;
            }
        })
    };
    // batches of connections: each one's states must all go, and memory must level off rather than climb per batch
    let batches: usize = std::env::var("LEAK_BATCHES").ok().and_then(|batches| batches.parse().ok()).unwrap_or(1);
    for batch in 0..batches {
        for round in 0..12 {
            let leave = [Leave::Close, Leave::Drop, Leave::Frozen][round % 3];
            let client = Client::connect(&url, ClientOptions { heartbeat_hz: 10.0, heartbeat_misses: 3, ..Default::default() }).await.unwrap();
            let mut camera = client.subscribe("camera/front", SubscribeOptions { encoding: Some("test-solid".into()), ..Default::default() }).await.unwrap();
            let mut map = client.subscribe("map/local", SubscribeOptions { delivery: Some(Delivery::Reliable), ..Default::default() }).await.unwrap();
            let publisher = client.publish("cmd/test", PublisherOptions::default()).await.unwrap();
            publisher.set_deadman(b"stop").await.unwrap();
            for subscription in [&mut camera, &mut map] {
                loop {
                    match timeout(Duration::from_secs(10), subscription.recv()).await.expect("a message").expect("open") {
                        Message::Video(_) | Message::Data(_) => break,
                        _ => continue,
                    }
                }
            }
            match leave {
                Leave::Close => client.close().await,
                Leave::Drop => drop((client, camera, map, publisher)),
                Leave::Frozen => {
                    client.pause_heartbeat(true);
                    tokio::time::sleep(Duration::from_millis(600)).await;
                    drop((client, camera, map, publisher));
                }
            }
        }
        // a vanished page is let go after the ICE timeouts and DISCONNECTED_GRACE
        wait_for("after 12 connections", Duration::from_secs(60), || zenoh_gateway::live_peer_states() == 0).await;
        eprintln!("after {} connections: RSS {:.0} MB, no live peer states", 12 * (batch + 1), rss_mb());
    }
    putter.abort();
    running.shutdown().await.unwrap();
}
