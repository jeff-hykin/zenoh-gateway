//! The embeddable server: [`Server::builder`] → [`ServerBuilder::build`] → [`Server::bind`] (or
//! [`Server::serve`], or [`Server::router`] to mount it in your own axum app).

use crate::acl::AccessControl;
use crate::codec::Codec;
use crate::codec::registry::CodecRegistry;
use crate::peer::{AllocationConfig, Bridge};
use anyhow::{Context, Result, anyhow, ensure};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use log::{error, info};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::net::ToSocketAddrs;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use webrtc::peer_connection::RTCSessionDescription;

/// Default HTTP port of the `zenoh-web` command.
pub const DEFAULT_PORT: u16 = 7448;

/// `GET` this path for `{"service": "zenoh-web", "version": "<crate version>"}`: how a program
/// checks whether a zenoh-web server is already listening somewhere before starting its own.
pub const HEALTH_PATH: &str = "/zenoh-web/health";

/// Configures a [`Server`]. Start with [`Server::builder`].
///
/// ```no_run
/// # async fn run() -> anyhow::Result<()> {
/// let server = zenoh_web::Server::builder()
///     .connect("tcp/192.168.1.2:7447")
///     .serve_dir("examples")
///     .bandwidth_target_fraction(0.75)
///     .build()
///     .await?;
/// let running = server.bind(("0.0.0.0", 7448)).await?;
/// // ... later, e.g. when the host app quits: deadmen fire, then everything closes
/// running.shutdown().await?;
/// # Ok(())
/// # }
/// ```
#[must_use = "a builder does nothing until `build` is awaited"]
pub struct ServerBuilder {
    zenoh_config: Option<zenoh::Config>,
    session: Option<zenoh::Session>,
    connect: Vec<String>,
    serve_dir: Option<PathBuf>,
    max_bandwidth_bytes_per_sec: Option<f64>,
    bandwidth_target_fraction: f64,
    strict_priority: u8,
    codecs: Vec<Arc<dyn Codec>>,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        ServerBuilder {
            zenoh_config: None,
            session: None,
            connect: Vec::new(),
            serve_dir: None,
            max_bandwidth_bytes_per_sec: None,
            bandwidth_target_fraction: 0.75,
            strict_priority: 2,
            codecs: Vec::new(),
        }
    }
}

impl ServerBuilder {
    /// The zenoh configuration for the session the server opens (default: zenoh's defaults, a
    /// peer with multicast scouting). Its `access_control` section is also applied to browser
    /// puts, subscriptions and queries, so a refusal reaches the page as a `rejected` state
    /// (zenoh alone would drop them silently). With [`session`](Self::session), only that
    /// `access_control` section is used.
    pub fn zenoh_config(mut self, config: zenoh::Config) -> Self {
        self.zenoh_config = Some(config);
        self
    }

    /// Reads [`zenoh_config`](Self::zenoh_config) from a json5 file.
    pub fn zenoh_config_file(self, path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let config = zenoh::Config::from_file(path).map_err(|error| anyhow!("{}: {error}", path.display()))?;
        Ok(self.zenoh_config(config))
    }

    /// Uses a zenoh session the host application already has instead of opening one. The server
    /// never closes it. Topic listing's routing-table sources (`subscriber`, `queryable`) need the
    /// session's admin space to be readable; without it `listTopics` still sees tokens and samples.
    pub fn session(mut self, session: zenoh::Session) -> Self {
        self.session = Some(session);
        self
    }

    /// Adds a zenoh endpoint to connect to, e.g. `tcp/192.168.1.2:7447` (repeatable; replaces the
    /// config's `connect/endpoints`). Not allowed together with [`session`](Self::session).
    pub fn connect(mut self, endpoint: impl Into<String>) -> Self {
        self.connect.push(endpoint.into());
        self
    }

    /// Serves a directory over HTTP next to the signaling endpoint (`/` serves `index.html`); the
    /// files are read on every request, so the UI is live-editable on disk.
    pub fn serve_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.serve_dir = Some(dir.into());
        self
    }

    /// Caps every browser's bandwidth budget (bytes/s) below its estimate, e.g. for a known-slow
    /// link. Default: no cap.
    pub fn max_bandwidth_bytes_per_sec(mut self, bytes_per_sec: f64) -> Self {
        self.max_bandwidth_bytes_per_sec = Some(bytes_per_sec);
        self
    }

    /// Share of the estimated bandwidth the allocator hands out, in (0, 1]; the rest is headroom
    /// that keeps the path's queues short. A connection may override it (`bandwidthTargetFraction`).
    /// Default 0.75.
    pub fn bandwidth_target_fraction(mut self, fraction: f64) -> Self {
        self.bandwidth_target_fraction = fraction;
        self
    }

    /// Subscriptions at this zenoh priority or more urgent (1 REAL_TIME … 7 BACKGROUND) bypass
    /// allocation and pacing and preempt everything else; 0 disables. Default 2 (INTERACTIVE_HIGH).
    pub fn strict_priority(mut self, priority: u8) -> Self {
        self.strict_priority = priority;
        self
    }

    /// Registers an external codec next to the built-in ones. A name that is already registered
    /// (built-in or not) makes [`build`](Self::build) fail.
    pub fn codec(self, codec: impl Codec + 'static) -> Self {
        self.shared_codec(Arc::new(codec))
    }

    /// [`codec`](Self::codec) for a codec that is already shared.
    pub fn shared_codec(mut self, codec: Arc<dyn Codec>) -> Self {
        self.codecs.push(codec);
        self
    }

    /// Validates the options, registers the codecs and opens the zenoh session (unless one was
    /// given).
    pub async fn build(self) -> Result<Server> {
        ensure!(
            self.bandwidth_target_fraction > 0.0 && self.bandwidth_target_fraction <= 1.0,
            "bandwidth target fraction must be within (0, 1], got {}",
            self.bandwidth_target_fraction
        );
        if let Some(cap) = self.max_bandwidth_bytes_per_sec {
            ensure!(cap.is_finite() && cap > 0.0, "max bandwidth must be a positive number of bytes/s, got {cap}");
        }
        ensure!(self.strict_priority <= 7, "strict priority must be 0..7, got {}", self.strict_priority);
        let codecs = CodecRegistry::new(self.codecs)?;
        let mut config = self.zenoh_config.unwrap_or_default();
        let access_control = AccessControl::from_config(&config)?;
        if access_control.enabled() {
            info!("access_control enabled: browser puts/subscribes/gets are checked against it");
        }
        let (session, owns_session) = match self.session {
            Some(session) => {
                ensure!(self.connect.is_empty(), "connect endpoints only apply to a session zenoh-web opens; configure them on the session you pass instead");
                (session, false)
            }
            None => {
                if !self.connect.is_empty() {
                    config.insert_json5("connect/endpoints", &serde_json::to_string(&self.connect)?).map_err(|error| anyhow!("{error}"))?;
                }
                // listTopics reads this bridge's own routing tables through the admin space (read-only)
                config.insert_json5("adminspace/enabled", "true").map_err(|error| anyhow!("{error}"))?;
                config.insert_json5("adminspace/permissions", r#"{"read": true, "write": false}"#).map_err(|error| anyhow!("{error}"))?;
                (zenoh::open(config).await.map_err(|error| anyhow!("opening the zenoh session: {error}"))?, true)
            }
        };
        let allocation = AllocationConfig {
            max_bandwidth: self.max_bandwidth_bytes_per_sec,
            target_fraction: self.bandwidth_target_fraction,
            strict_priority: self.strict_priority,
        };
        let bridge = Bridge::new(session.clone(), access_control, codecs, allocation);
        Ok(Server { inner: Arc::new(Inner { bridge, session, owns_session, serve_dir: self.serve_dir, shut_down: AtomicBool::new(false) }) })
    }
}

struct Inner {
    bridge: Arc<Bridge>,
    session: zenoh::Session,
    owns_session: bool,
    serve_dir: Option<PathBuf>,
    shut_down: AtomicBool,
}

/// A configured zenoh-web server: the zenoh side is live, browsers connect once it is bound to an
/// HTTP address ([`bind`](Self::bind), [`serve`](Self::serve)) or mounted in a host's router
/// ([`router`](Self::router)). Cheap to clone; clones share everything.
#[derive(Clone)]
pub struct Server {
    inner: Arc<Inner>,
}

impl Server {
    /// Starts configuring a server.
    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    /// The zenoh session browsers' subscriptions, puts and queries go through.
    pub fn session(&self) -> &zenoh::Session {
        &self.inner.session
    }

    /// The HTTP routes: `POST /offer` (WebRTC signaling, CORS-permissive), `GET /zenoh-web/health`
    /// (`{"service": "zenoh-web", "version": ...}`, so another program can tell a zenoh-web server is
    /// already listening on a port) and, with [`ServerBuilder::serve_dir`], static files for
    /// everything else. Mount it in a host application's axum server; call
    /// [`shutdown`](Self::shutdown) when the host stops.
    pub fn router(&self) -> Router {
        let mut router = Router::new()
            .route("/offer", post(offer))
            .route(HEALTH_PATH, get(health))
            .with_state(self.inner.bridge.clone())
            .layer(CorsLayer::permissive());
        if let Some(dir) = &self.inner.serve_dir {
            router = router.fallback_service(ServeDir::new(dir));
        }
        router
    }

    /// Binds the HTTP listener (e.g. `("0.0.0.0", 7448)`, or port 0 for any free port) and serves
    /// on a background task until [`RunningServer::shutdown`].
    pub async fn bind(self, addr: impl ToSocketAddrs) -> Result<RunningServer> {
        let listener = tokio::net::TcpListener::bind(addr).await.context("binding the HTTP listener")?;
        let local_addr = listener.local_addr()?;
        info!("zenoh-web listening on http://{local_addr}");
        if let Some(dir) = &self.inner.serve_dir {
            info!("serving {}", dir.display());
        }
        let (stop, stopped) = oneshot::channel::<()>();
        let app = self.router();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = stopped.await;
                })
                .await
        });
        Ok(RunningServer { server: self, local_addr, stop: Some(stop), task })
    }

    /// Serves on `addr` until the HTTP server fails. To stop it cleanly (firing deadmen), use
    /// [`serve_with_shutdown`](Self::serve_with_shutdown) or [`bind`](Self::bind).
    pub async fn serve(self, addr: impl ToSocketAddrs) -> Result<()> {
        self.serve_with_shutdown(addr, std::future::pending()).await
    }

    /// Serves on `addr` until `signal` completes (e.g. Ctrl-C), then shuts down like
    /// [`RunningServer::shutdown`].
    pub async fn serve_with_shutdown(self, addr: impl ToSocketAddrs, signal: impl Future<Output = ()>) -> Result<()> {
        let mut running = self.bind(addr).await?;
        let failed = tokio::select! {
            result = &mut running.task => Some(result),
            () = signal => None,
        };
        match failed {
            Some(result) => {
                running.stop = None;
                running.server.shutdown().await?;
                result.context("HTTP server task")?.context("HTTP server")
            }
            None => running.shutdown().await,
        }
    }

    /// Fires every connected browser's deadmen (reason `"shutdown"`, published once, reliably),
    /// closes the browser connections and, if the server opened the zenoh session, closes it.
    /// Later calls do nothing.
    pub async fn shutdown(&self) -> Result<()> {
        if self.inner.shut_down.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.inner.bridge.shutdown().await;
        if self.inner.owns_session {
            self.inner.session.close().await.map_err(|error| anyhow!("closing the zenoh session: {error}"))?;
        }
        info!("shut down");
        Ok(())
    }
}

/// A server bound to an HTTP address, serving on a background task. Dropping it leaves the task
/// running; call [`shutdown`](Self::shutdown) to stop it and fire deadmen.
pub struct RunningServer {
    server: Server,
    local_addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<std::io::Result<()>>,
}

impl RunningServer {
    /// The address the HTTP listener is bound to (useful after binding port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The running [`Server`] (e.g. for its [`session`](Server::session)).
    pub fn server(&self) -> &Server {
        &self.server
    }

    /// Stops accepting HTTP requests, waits for the ones in flight, fires every browser's deadmen,
    /// closes the browser connections and (if the server opened it) the zenoh session.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        // deadmen first: a browser that is still connected keeps its HTTP side idle anyway
        self.server.shutdown().await?;
        // graceful HTTP shutdown waits for open requests; a stuck one must not keep the process alive
        match tokio::time::timeout(std::time::Duration::from_secs(3), &mut self.task).await {
            Ok(Ok(result)) => result.context("HTTP server"),
            Ok(Err(error)) => Err(anyhow!("HTTP server task: {error}")),
            Err(_) => {
                self.task.abort();
                Ok(())
            }
        }
    }
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"service": "zenoh-web", "version": env!("CARGO_PKG_VERSION")}))
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
