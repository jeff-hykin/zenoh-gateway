//! A zenoh peer for the e2e test and the viewer demo.
//!
//! - `test/cached`: AdvancedPublisher with a 1-sample cache, put once at startup ("cached-hello")
//! - `test/fast`: `--fast-hz` puts of `--fast-bytes` bytes; payload starts with f64 send time (unix ms) + u32 counter
//! - `test/jpeg`: a real JPEG at 5 Hz
//! - `test/queryable`: replies "pong"
//! - `test/frombrowser/**`: printed to stdout as `RECV <key> <utf8 payload>`
//!
//! Prints `READY` once everything is declared.

use clap::Parser;
use std::io::Write;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use zenoh::qos::CongestionControl;
use zenoh_ext::{AdvancedPublisherBuilderExt, CacheConfig};

const JPEG: &[u8] = include_bytes!("test_image.jpg");

#[derive(Parser)]
struct Cli {
    /// Endpoint to listen on, e.g. tcp/127.0.0.1:17447
    #[arg(long)]
    listen: String,
    #[arg(long, default_value_t = 200.0)]
    fast_hz: f64,
    #[arg(long, default_value_t = 16 * 1024)]
    fast_bytes: usize,
}

fn unix_ms() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64() * 1000.0
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let mut config = zenoh::Config::default();
    let insert = |config: &mut zenoh::Config, key: &str, value: &str| {
        config.insert_json5(key, value).map_err(|e| anyhow::anyhow!("{key}: {e}"))
    };
    insert(&mut config, "mode", r#""peer""#)?;
    insert(&mut config, "listen/endpoints", &serde_json::to_string(&[&cli.listen])?)?;
    insert(&mut config, "scouting/multicast/enabled", "false")?;
    insert(&mut config, "timestamping/enabled", "true")?;
    let session = zenoh::open(config).await.map_err(|e| anyhow::anyhow!("{e}"))?;

    let cached = session
        .declare_publisher("test/cached")
        .advanced()
        .cache(CacheConfig::default().max_samples(1))
        .publisher_detection()
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    cached.put("cached-hello").await.map_err(|e| anyhow::anyhow!("{e}"))?;

    let _queryable = session
        .declare_queryable("test/queryable")
        .callback(|query| {
            let key = query.key_expr().clone();
            tokio::spawn(async move {
                let _ = query.reply(key, "pong").await;
            });
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let _from_browser = session
        .declare_subscriber("test/frombrowser/**")
        .callback(|sample| {
            let text = String::from_utf8_lossy(&sample.payload().to_bytes()).into_owned();
            let mut stdout = std::io::stdout().lock();
            let _ = writeln!(stdout, "RECV {} {}", sample.key_expr(), text);
            let _ = stdout.flush();
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let jpeg_publisher = session.declare_publisher("test/jpeg").await.map_err(|e| anyhow::anyhow!("{e}"))?;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(200));
        loop {
            ticker.tick().await;
            let _ = jpeg_publisher.put(JPEG).await;
        }
    });

    let fast = session
        .declare_publisher("test/fast")
        .congestion_control(CongestionControl::Drop)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("READY");
    std::io::stdout().flush()?;

    // Fixed-rate schedule; catch up in bursts rather than drift when the timer is late.
    let period = Duration::from_secs_f64(1.0 / cli.fast_hz);
    let start = Instant::now();
    let mut counter: u32 = 0;
    loop {
        let due = start + period * counter;
        let now = Instant::now();
        if due > now {
            tokio::time::sleep(due - now).await;
        }
        let mut payload = vec![0u8; cli.fast_bytes.max(12)];
        payload[0..8].copy_from_slice(&unix_ms().to_le_bytes());
        payload[8..12].copy_from_slice(&counter.to_le_bytes());
        let _ = fast.put(payload).await;
        counter = counter.wrapping_add(1);
    }
}
