//! A zenoh peer for the e2e tests and for trying the example page without a robot.
//!
//! - `test/cached`: AdvancedPublisher with a 1-sample cache, put once at startup ("cached-hello")
//! - `test/fast`: `--fast-hz` puts of `--fast-bytes` bytes; payload starts with f64 send time (unix ms) + u32 counter
//! - `test/jpeg`: a real JPEG at 5 Hz
//! - `test/queryable`: replies "pong"
//! - `test/frombrowser/**`: printed to stdout as `RECV <key> <utf8 payload>`
//! - topic enumeration fixtures nobody subscribes to: `test/unsubscribed/declared` (declared
//!   publisher, 2 Hz), `test/unsubscribed/silent` (declared publisher, never puts),
//!   `test/unsubscribed/undeclared` (plain session.put, 2 Hz), `test/unsubscribed/token` (liveliness token)
//!
//! - `test/big`: `--big-bytes` at `--big-hz`, only while someone subscribes; byte i (i >= 8) is
//!   `(counter * 31 + i * 7) & 0xff`, bytes 0..4 the counter, 4..8 the length (u32 little endian)
//!
//! - `--publish <key>=<file>@<hz>` (repeatable): the file's exact bytes, e.g. a codec fixture
//! - `--synthetic <key>=<bytes>@<hz>` (repeatable): f64 send time (unix ms) + u32 counter + zeros
//!
//! `test/big`, `--publish` and `--synthetic` keys only put while someone subscribes.
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
    #[arg(long, default_value_t = 20.0)]
    big_hz: f64,
    #[arg(long, default_value_t = 2_500_000)]
    big_bytes: usize,
    #[arg(long)]
    publish: Vec<String>,
    #[arg(long)]
    synthetic: Vec<String>,
}

/// `<key>=<value>@<hz>`
fn split_spec(spec: &str) -> anyhow::Result<(String, String, f64)> {
    let (key, rest) = spec.split_once('=').ok_or_else(|| anyhow::anyhow!("{spec}: expected <key>=<value>@<hz>"))?;
    let (value, hz) = rest.rsplit_once('@').ok_or_else(|| anyhow::anyhow!("{spec}: expected <key>=<value>@<hz>"))?;
    Ok((key.to_owned(), value.to_owned(), hz.parse()?))
}

/// Puts `make(counter)` on `key` at `hz` while the key has a matching subscriber.
async fn publish_while_matched(session: &zenoh::Session, key: String, hz: f64, make: impl Fn(u32) -> Vec<u8> + Send + 'static) -> anyhow::Result<()> {
    let publisher = session.declare_publisher(key).congestion_control(CongestionControl::Drop).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs_f64(1.0 / hz));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut counter: u32 = 0;
        loop {
            ticker.tick().await;
            if publisher.matching_status().await.is_ok_and(|status| status.matching()) {
                let _ = publisher.put(make(counter)).await;
                counter = counter.wrapping_add(1);
            }
        }
    });
    Ok(())
}

/// The `test/big` payload for `counter`; checkable byte by byte in the browser.
fn big_payload(counter: u32, length: usize) -> Vec<u8> {
    let mut payload: Vec<u8> = (0..length).map(|i| (counter as usize * 31 + i * 7) as u8).collect();
    payload[0..4].copy_from_slice(&counter.to_le_bytes());
    payload[4..8].copy_from_slice(&(length as u32).to_le_bytes());
    payload
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

    let declared = session.declare_publisher("test/unsubscribed/declared").await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let _silent = session.declare_publisher("test/unsubscribed/silent").await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let _token = session.liveliness().declare_token("test/unsubscribed/token").await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let undeclared_session = session.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        loop {
            ticker.tick().await;
            let _ = declared.put("declared").await;
            let _ = undeclared_session.put("test/unsubscribed/undeclared", "undeclared").await;
        }
    });

    let big_publisher = session.declare_publisher("test/big").await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let (big_hz, big_bytes) = (cli.big_hz, cli.big_bytes.max(8));
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs_f64(1.0 / big_hz));
        let mut counter: u32 = 0;
        loop {
            ticker.tick().await;
            if big_publisher.matching_status().await.is_ok_and(|status| status.matching()) {
                let _ = big_publisher.put(big_payload(counter, big_bytes)).await;
                counter = counter.wrapping_add(1);
            }
        }
    });

    for spec in &cli.publish {
        let (key, path, hz) = split_spec(spec)?;
        let bytes = std::fs::read(&path).map_err(|e| anyhow::anyhow!("{path}: {e}"))?;
        publish_while_matched(&session, key, hz, move |_| bytes.clone()).await?;
    }
    for spec in &cli.synthetic {
        let (key, size, hz) = split_spec(spec)?;
        let size: usize = size.parse::<usize>()?.max(12);
        publish_while_matched(&session, key, hz, move |counter| {
            let mut payload = vec![0u8; size];
            payload[0..8].copy_from_slice(&unix_ms().to_le_bytes());
            payload[8..12].copy_from_slice(&counter.to_le_bytes());
            payload
        })
        .await?;
    }

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
