//! The `zenoh-web` command: a thin wrapper over [`zenoh_web::Server`].

use clap::Parser;
use log::info;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "zenoh-web", version, about = "Bridge zenoh to browsers over WebRTC data channels")]
struct Cli {
    /// HTTP port for signaling (POST /offer) and static files.
    #[arg(long, default_value_t = zenoh_web::DEFAULT_PORT)]
    port: u16,
    /// zenoh config file (json5).
    #[arg(long)]
    zenoh_config: Option<PathBuf>,
    /// zenoh endpoint to connect to, e.g. tcp/192.168.1.2:7447 (repeatable).
    #[arg(long)]
    connect: Vec<String>,
    /// Serve this directory over HTTP (so the UI is live-editable on disk).
    #[arg(long)]
    serve: Option<PathBuf>,
    /// Cap each frontend's bandwidth budget (bytes/s) below the estimate, e.g. for a known-slow
    /// link or to test allocation on localhost.
    #[arg(long)]
    max_bandwidth_bytes_per_sec: Option<f64>,
    /// Fraction of the estimated bandwidth the allocator hands out; the rest keeps the path's
    /// queues short for strict-priority streams. A connection may override it (connect option
    /// `bandwidthTargetFraction`).
    #[arg(long, default_value_t = 0.75)]
    bandwidth_target_fraction: f64,
    /// Subscriptions at this zenoh priority or more urgent (1 REAL_TIME .. 7 BACKGROUND) bypass
    /// allocation and pacing and preempt everything else; 0 disables. Default 2 (INTERACTIVE_HIGH).
    #[arg(long, default_value_t = 2)]
    strict_priority: u8,
}

/// Resolves on SIGINT or SIGTERM.
async fn terminated() {
    let mut terminate = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(terminate) => terminate,
        Err(error) => {
            log::warn!("no SIGTERM handler: {error}");
            let _ = tokio::signal::ctrl_c().await;
            info!("SIGINT");
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => info!("SIGINT"),
        _ = terminate.recv() => info!("SIGTERM"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info,zenoh=warn,zenoh_ext=warn,zenoh_web=info,rtc=warn,webrtc=warn")).init();
    let cli = Cli::parse();
    let mut builder = zenoh_web::Server::builder()
        .bandwidth_target_fraction(cli.bandwidth_target_fraction)
        .strict_priority(cli.strict_priority);
    if let Some(path) = &cli.zenoh_config {
        builder = builder.zenoh_config_file(path)?;
    }
    for endpoint in cli.connect {
        builder = builder.connect(endpoint);
    }
    if let Some(dir) = cli.serve {
        builder = builder.serve_dir(dir);
    }
    if let Some(cap) = cli.max_bandwidth_bytes_per_sec {
        builder = builder.max_bandwidth_bytes_per_sec(cap);
    }
    // every frontend's deadmen go out (reliably) before the zenoh session closes
    builder.build().await?.serve_with_shutdown(("0.0.0.0", cli.port), terminated()).await
}
