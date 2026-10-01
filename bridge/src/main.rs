//! zenoh-web: a dumb pipe between zenoh key expressions and browser WebRTC data channels.

mod acl;
mod frame;
mod options;
mod peer;
mod publisher;
mod subscription;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use clap::Parser;
use log::{error, info};
use peer::Bridge;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use webrtc::peer_connection::RTCSessionDescription;

#[derive(Parser, Debug)]
#[command(name = "zenoh-web", about = "Bridge zenoh to browsers over WebRTC data channels")]
struct Cli {
    /// HTTP port for signaling (POST /offer) and static files.
    #[arg(long, default_value_t = 7448)]
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
}

async fn offer(State(bridge): State<Arc<Bridge>>, Json(offer): Json<RTCSessionDescription>) -> Response {
    match bridge.answer(offer).await {
        Ok(answer) => Json(answer).into_response(),
        Err(error) => {
            error!("offer failed: {error:#}");
            (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
    }
}

fn zenoh_config(cli: &Cli) -> anyhow::Result<zenoh::Config> {
    let mut config = match &cli.zenoh_config {
        Some(path) => zenoh::Config::from_file(path).map_err(|e| anyhow::anyhow!("{e}"))?,
        None => zenoh::Config::default(),
    };
    if !cli.connect.is_empty() {
        let endpoints = serde_json::to_string(&cli.connect)?;
        config.insert_json5("connect/endpoints", &endpoints).map_err(|e| anyhow::anyhow!("{e}"))?;
    }
    Ok(config)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info,zenoh=warn,zenoh_ext=warn,zenoh_web=info,rtc=warn,webrtc=warn")).init();
    let cli = Cli::parse();
    let mut config = zenoh_config(&cli)?;
    let access_control = acl::AccessControl::from_config(&config)?;
    if access_control.enabled() {
        info!("access_control enabled: browser puts/subscribes/gets are checked against it");
    }
    // listTopics reads this bridge's own routing tables through the admin space (read-only)
    config.insert_json5("adminspace/enabled", "true").map_err(|e| anyhow::anyhow!("{e}"))?;
    config.insert_json5("adminspace/permissions", r#"{"read": true, "write": false}"#).map_err(|e| anyhow::anyhow!("{e}"))?;
    let session = zenoh::open(config).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let bridge = Bridge::new(session.clone(), access_control);

    let mut app = Router::new().route("/offer", post(offer)).with_state(bridge.clone()).layer(CorsLayer::permissive());
    if let Some(dir) = &cli.serve {
        app = app.fallback_service(ServeDir::new(dir));
    }
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", cli.port)).await?;
    info!("zenoh-web listening on http://{}", listener.local_addr()?);
    if let Some(dir) = &cli.serve {
        info!("serving {}", dir.display());
    }
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = axum::serve(listener, app) => result?,
        _ = tokio::signal::ctrl_c() => info!("SIGINT"),
        _ = terminate.recv() => info!("SIGTERM"),
    }
    // every frontend's deadmen go out (reliably) before the zenoh session closes
    bridge.shutdown().await;
    session.close().await.map_err(|e| anyhow::anyhow!("{e}"))?;
    info!("shut down");
    Ok(())
}
