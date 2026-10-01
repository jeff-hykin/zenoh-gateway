//! The embeddable server: [`Server::builder`] → [`ServerBuilder::build`] → [`Server::bind`] (or [`Server::serve`], or [`Server::router`]).

use crate::auth::{Grant, Leases};
use crate::codec::Codec;
use crate::codec::registry::CodecRegistry;
use crate::ice::{self, IceServer};
use crate::peer::{AllocationConfig, Bridge, ConnectConfig};
use anyhow::{Context, Result, anyhow, ensure};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use log::{error, info};
use std::future::Future;
use std::net::SocketAddr;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::net::ToSocketAddrs;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use webrtc::peer_connection::RTCSessionDescription;

/// Default HTTP port of the `zenoh-web` command.
pub const DEFAULT_PORT: u16 = 7448;

/// `GET` this path for `{"service": "zenoh-web", "version": "<crate version>"}`, to check a zenoh-web server is listening.
pub const HEALTH_PATH: &str = "/zenoh-web/health";

/// `GET` this path for `{"iceServers": [...]}`: the STUN/TURN servers the bridge uses, with TURN credentials minted for the caller.
pub const ICE_PATH: &str = "/zenoh-web/ice";

/// The authorize hook: the bearer token of `POST /offer` (and `GET` [`ICE_PATH`]) and the request headers in,
/// a [`Grant`] or the reason for refusing (HTTP 401) out.
pub type Authorize = dyn Fn(Option<&str>, &HeaderMap) -> Result<Grant, String> + Send + Sync;

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
    codecs: Vec<Arc<dyn Codec>>,
    authorize: Option<Arc<Authorize>>,
    lease_groups: HashMap<String, Vec<String>>,
    connect_config: ConnectConfig,
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
            codecs: Vec::new(),
            authorize: None,
            lease_groups: HashMap::new(),
            connect_config: ConnectConfig::default(),
        }
    }
}

impl ServerBuilder {
    /// The zenoh configuration for the session the server opens (default: a peer with multicast scouting). Its
    /// `access_control` section applies to browsers' puts, subscriptions and queries too (dropped silently).
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

    /// Uses a zenoh session the host application already has instead of opening one; the server never closes it.
    pub fn session(mut self, session: zenoh::Session) -> Self {
        self.session = Some(session);
        self
    }

    /// Adds a zenoh endpoint to connect to, e.g. `tcp/192.168.1.2:7447` (repeatable; replaces the config's
    /// `connect/endpoints`). Not allowed together with [`session`](Self::session).
    pub fn connect(mut self, endpoint: impl Into<String>) -> Self {
        self.connect.push(endpoint.into());
        self
    }

    /// Serves a directory over HTTP (`/` serves `index.html`), read on every request so the UI is live-editable on disk.
    pub fn serve_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.serve_dir = Some(dir.into());
        self
    }

    /// Caps every browser's bandwidth budget (bytes/s) below its estimate, e.g. for a known-slow link. Default: no cap.
    pub fn max_bandwidth_bytes_per_sec(mut self, bytes_per_sec: f64) -> Self {
        self.max_bandwidth_bytes_per_sec = Some(bytes_per_sec);
        self
    }

    /// Share of the estimated bandwidth the allocator hands out, in (0, 1]; the rest keeps queues short. Default 0.75.
    pub fn bandwidth_target_fraction(mut self, fraction: f64) -> Self {
        self.bandwidth_target_fraction = fraction;
        self
    }

    /// Registers a codec. A name that is already registered makes [`build`](Self::build) fail.
    pub fn codec(self, codec: impl Codec + 'static) -> Self {
        self.shared_codec(Arc::new(codec))
    }

    /// [`codec`](Self::codec) for a codec that is already shared.
    pub fn shared_codec(mut self, codec: Arc<dyn Codec>) -> Self {
        self.codecs.push(codec);
        self
    }

    /// Decides what each connection may do, from its bearer token (`connect(url, { token })`) and headers; it runs on
    /// every offer, so a reconnect is authorized afresh. Without a hook everyone gets [`Grant::all`].
    pub fn authorize(mut self, hook: impl Fn(Option<&str>, &HeaderMap) -> Result<Grant, String> + Send + Sync + 'static) -> Self {
        self.authorize = Some(Arc::new(hook));
        self
    }

    /// Defines a lease group: while a client holds it, other clients of this bridge can't publish on these keys.
    pub fn lease_group(mut self, name: impl Into<String>, keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.lease_groups.insert(name.into(), keys.into_iter().map(Into::into).collect());
        self
    }

    /// STUN/TURN servers for the bridge's side of every connection, also handed to browsers at [`ICE_PATH`].
    pub fn ice_servers(mut self, servers: impl IntoIterator<Item = IceServer>) -> Self {
        self.connect_config.ice_servers = servers.into_iter().collect();
        self
    }

    /// coturn's `static-auth-secret`: TURN servers without a username get credentials minted per connection, valid for `ttl`.
    pub fn turn_secret(mut self, secret: impl Into<String>, ttl: Duration) -> Self {
        self.connect_config.turn_secret = Some((secret.into(), ttl));
        self
    }

    /// Binds each connection's WebRTC UDP sockets to a port from this range (one port per connection), to firewall a relay easily.
    pub fn udp_ports(mut self, ports: RangeInclusive<u16>) -> Self {
        self.connect_config.udp_ports = Some(ports);
        self
    }

    /// Validates the options, registers the codecs and opens the zenoh session (unless one was given).
    pub async fn build(self) -> Result<Server> {
        let fraction = self.bandwidth_target_fraction;
        ensure!(fraction > 0.0 && fraction <= 1.0, "bandwidth target fraction must be within (0, 1], got {fraction}");
        if let Some(cap) = self.max_bandwidth_bytes_per_sec {
            ensure!(cap.is_finite() && cap > 0.0, "max bandwidth must be a positive number of bytes/s, got {cap}");
        }
        let codecs = CodecRegistry::new(self.codecs)?;
        let mut config = self.zenoh_config.unwrap_or_default();
        let (session, owns_session) = match self.session {
            Some(session) => {
                ensure!(self.connect.is_empty(), "connect endpoints only apply to a session zenoh-web opens; configure them on the session you pass instead");
                (session, false)
            }
            None => {
                if !self.connect.is_empty() {
                    config.insert_json5("connect/endpoints", &serde_json::to_string(&self.connect)?).map_err(|error| anyhow!("{error}"))?;
                }
                (zenoh::open(config).await.map_err(|error| anyhow!("opening the zenoh session: {error}"))?, true)
            }
        };
        ensure!(self.connect_config.udp_ports.as_ref().is_none_or(|ports| !ports.is_empty() && *ports.start() > 0), "the UDP port range must be non-empty and above 0");
        let leases = Leases::new(self.lease_groups);
        let bridge = Bridge::new(session.clone(), codecs, AllocationConfig { max_bandwidth: self.max_bandwidth_bytes_per_sec, target_fraction: fraction }, leases, self.connect_config);
        Ok(Server { inner: Arc::new(Inner { bridge, session, owns_session, serve_dir: self.serve_dir, shut_down: AtomicBool::new(false), authorize: self.authorize }) })
    }
}

struct Inner {
    bridge: Arc<Bridge>,
    session: zenoh::Session,
    owns_session: bool,
    serve_dir: Option<PathBuf>,
    shut_down: AtomicBool,
    authorize: Option<Arc<Authorize>>,
}

impl Inner {
    /// The request's bearer token and what the hook grants it.
    fn authorize(&self, headers: &HeaderMap) -> Result<(Option<String>, Grant), String> {
        let token = headers.get(header::AUTHORIZATION).and_then(|value| value.to_str().ok()).and_then(|value| value.strip_prefix("Bearer ")).map(str::to_owned);
        let grant = self.authorize.as_ref().map_or_else(|| Ok(Grant::all()), |hook| hook(token.as_deref(), headers))?;
        Ok((token, grant))
    }
}

/// A configured zenoh-web server: the zenoh side is live, browsers connect once it is bound ([`bind`](Self::bind),
/// [`serve`](Self::serve)) or mounted in a host's router ([`router`](Self::router)). Clones share everything.
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

    /// Closes every live connection made with `token` (deadmen fire with reason `"revoked"`) and returns how many;
    /// the authorize hook decides whether the token may connect again.
    pub fn revoke(&self, token: &str) -> usize {
        self.inner.bridge.revoke(token)
    }

    /// The HTTP routes: `POST /offer` (WebRTC signaling, CORS-permissive), `GET` [`HEALTH_PATH`] and [`ICE_PATH`] and, with
    /// [`ServerBuilder::serve_dir`], static files. Mount it in a host's axum server; call [`shutdown`](Self::shutdown) when it stops.
    pub fn router(&self) -> Router {
        let mut router = Router::new()
            .route("/offer", post(offer))
            .route(HEALTH_PATH, get(health))
            .route(ICE_PATH, get(ice_servers))
            .with_state(self.inner.clone())
            .layer(CorsLayer::permissive());
        if let Some(dir) = &self.inner.serve_dir {
            router = router.fallback_service(ServeDir::new(dir));
        }
        router
    }

    /// Binds the HTTP listener (e.g. `("0.0.0.0", 7448)`, or port 0) and serves on a task until [`RunningServer::shutdown`].
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

    /// Serves on `addr` until `signal` completes (e.g. Ctrl-C), then shuts down like [`RunningServer::shutdown`].
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

    /// Fires every connected browser's deadmen (reason `"shutdown"`, once, reliably), closes the browser connections and,
    /// if the server opened the zenoh session, closes it. Later calls do nothing.
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

/// A server serving on a background task; dropping it leaves the task running, [`shutdown`](Self::shutdown) stops it.
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

    /// Stops accepting HTTP requests, waits for the ones in flight, then [`Server::shutdown`].
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

async fn ice_servers(State(inner): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    match inner.authorize(&headers) {
        Ok(_) => Json(serde_json::json!({"iceServers": ice::mint(&inner.bridge.connect_config.ice_servers, inner.bridge.connect_config.turn_secret.as_ref(), "browser")})).into_response(),
        Err(reason) => (StatusCode::UNAUTHORIZED, reason).into_response(),
    }
}

async fn offer(State(inner): State<Arc<Inner>>, headers: HeaderMap, Json(offer): Json<RTCSessionDescription>) -> Response {
    let (token, grant) = match inner.authorize(&headers) {
        Ok(authorized) => authorized,
        Err(reason) => {
            info!("offer refused: {reason}");
            return (StatusCode::UNAUTHORIZED, reason).into_response();
        }
    };
    match inner.bridge.answer(offer, token, grant).await {
        Ok(answer) => Json(answer).into_response(),
        Err(error) => {
            error!("offer failed: {error:#}");
            (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
        }
    }
}
