//! The embeddable server: [`Server::builder`] → [`ServerBuilder::build`] → [`Server::bind`] (or [`Server::serve`], or [`Server::router`]).

use crate::auth::{Grant, Leases};
use crate::encoding::registry::{EncodingRegistry, VideoEncoderFactory};
use crate::encoding::{MessageEncoding, VideoEncoder, VideoFormat, VideoPolicy};
use crate::ice::{self, IceHook, IceRequest, IceServer, IceSide};
use crate::peer::{AllocationConfig, Gateway, ConnectConfig};
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
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::net::ToSocketAddrs;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use webrtc::peer_connection::RTCSessionDescription;

/// Default HTTP port of the `zenoh-gateway` command.
pub const DEFAULT_PORT: u16 = 7448;

/// `GET` this path for `{"service": "zenoh-gateway", "version": "<crate version>"}`, to check a zenoh-gateway server is listening.
pub const HEALTH_PATH: &str = "/zenoh-gateway/health";

/// `GET` this path for `{"iceServers": [...]}`: the STUN/TURN servers the gateway uses, with TURN credentials minted for the caller.
pub const ICE_PATH: &str = "/zenoh-gateway/ice";

/// With [`ServerBuilder::zenoh_signalling`], the key expression prefix of a server's signalling queryables:
/// `zenoh-gateway/<name>/offer` and `zenoh-gateway/<name>/ice`.
pub const SIGNALLING_PREFIX: &str = "zenoh-gateway";

/// The authorize hook: the bearer token of `POST /offer` (and `GET` [`ICE_PATH`]) and the request headers in,
/// a [`Grant`] or the reason for refusing (HTTP 401) out.
pub type Authorize = dyn Fn(Option<&str>, &HeaderMap) -> Result<Grant, String> + Send + Sync;

/// Configures a [`Server`]. Start with [`Server::builder`].
///
/// ```no_run
/// # async fn run() -> anyhow::Result<()> {
/// let server = zenoh_gateway::Server::builder()
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
    encodings: Vec<Arc<dyn MessageEncoding>>,
    authorize: Option<Arc<Authorize>>,
    lease_groups: HashMap<String, Vec<String>>,
    connect_config: ConnectConfig,
    video_encoders: Vec<(VideoFormat, VideoEncoderFactory)>,
    video_policy: VideoPolicy,
    zenoh_signalling: Option<String>,
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
            encodings: Vec::new(),
            authorize: None,
            lease_groups: HashMap::new(),
            connect_config: ConnectConfig::default(),
            video_encoders: Vec::new(),
            video_policy: VideoPolicy::default(),
            zenoh_signalling: None,
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

    /// Registers a message encoding (what subscriptions name with `encoding`). A name that is already registered makes
    /// [`build`](Self::build) fail.
    pub fn encoding(self, encoding: impl MessageEncoding + 'static) -> Self {
        self.shared_encoding(Arc::new(encoding))
    }

    /// [`encoding`](Self::encoding) for one that is already shared.
    pub fn shared_encoding(mut self, encoding: Arc<dyn MessageEncoding>) -> Self {
        self.encodings.push(encoding);
        self
    }

    /// Decides what each connection may do, from its bearer token (`connect(url, { token })`) and headers; it runs on
    /// every offer, so a reconnect is authorized afresh. Without a hook everyone gets [`Grant::all`].
    pub fn authorize(mut self, hook: impl Fn(Option<&str>, &HeaderMap) -> Result<Grant, String> + Send + Sync + 'static) -> Self {
        self.authorize = Some(Arc::new(hook));
        self
    }

    /// Defines a lease group: while a client holds it, other clients of this gateway can't publish on these keys.
    pub fn lease_group(mut self, name: impl Into<String>, keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.lease_groups.insert(name.into(), keys.into_iter().map(Into::into).collect());
        self
    }

    /// STUN/TURN servers for the gateway's side of every connection, also handed to browsers at [`ICE_PATH`].
    pub fn ice_servers(mut self, servers: impl IntoIterator<Item = IceServer>) -> Self {
        self.connect_config.ice_servers = servers.into_iter().collect();
        self
    }

    /// coturn's `static-auth-secret`: TURN servers without a username get credentials minted per connection, valid for `ttl`.
    pub fn turn_secret(mut self, secret: impl Into<String>, ttl: Duration) -> Self {
        self.connect_config.turn_secret = Some((secret.into(), ttl));
        self
    }

    /// Mints STUN/TURN servers for each end of each connection (e.g. a TURN provider's short-lived credentials), added
    /// after [`ice_servers`](Self::ice_servers); called for every `GET` [`ICE_PATH`] and every offer the gateway answers.
    /// When it fails, or takes over [`ICE_HOOK_TIMEOUT`](crate::ICE_HOOK_TIMEOUT), that end gets only the static servers.
    pub fn ice_servers_fn<F, Fut>(mut self, hook: F) -> Self
    where
        F: Fn(IceRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<IceServer>>> + Send + 'static,
    {
        self.connect_config.ice_servers_fn = Some(IceHook(Arc::new(move |request| Box::pin(hook(request)))));
        self
    }

    /// Cloudflare TURN (feature `cloudflare`): [`ice_servers_fn`](Self::ice_servers_fn) with credentials from its API.
    #[cfg(feature = "cloudflare")]
    pub fn cloudflare_turn(self, turn: ice::CloudflareTurn) -> Self {
        let turn = Arc::new(turn);
        self.ice_servers_fn(move |_| {
            let turn = turn.clone();
            async move { turn.servers().await }
        })
    }

    /// Binds each connection's WebRTC UDP sockets to a port from this range (one port per connection), to firewall a relay easily.
    pub fn udp_ports(mut self, ports: RangeInclusive<u16>) -> Self {
        self.connect_config.udp_ports = Some(ports);
        self
    }

    /// Makes the video encoders of the format the factory's encoders report ([`VideoEncoder::format`]; it is called
    /// once here to ask), for every encoding without its own ([`MessageEncoding::video_encoder`]), e.g. a hardware
    /// one (zenoh-dimos-codecs' encoders); then once per encode session. Call it once per format; a later call for
    /// the same format replaces the earlier one. Without one: the built-in encoders (H.264: openh264; AV1: rav1e,
    /// feature `av1`); `video-vp8` and `video-vp9` need one.
    pub fn video_encoder(mut self, factory: impl Fn() -> Box<dyn VideoEncoder> + Send + Sync + 'static) -> Self {
        let format = factory().format();
        self.video_encoders.retain(|(registered, _)| *registered != format);
        self.video_encoders.push((format, Arc::new(factory)));
        self
    }

    /// How video streams spend their bandwidth by default (subscriptions override `maxBitrate`, `minResolutionScale`
    /// and `maxResolution`). Default: [`VideoPolicy::default`].
    pub fn video_policy(mut self, policy: VideoPolicy) -> Self {
        self.video_policy = policy;
        self
    }

    /// Also answers offers over zenoh, on queryables `zenoh-gateway/<name>/offer` and `zenoh-gateway/<name>/ice`, so a client with
    /// a zenoh session that reaches this one (e.g. a relay this side dialled out to) connects without HTTP; see SPEC
    /// "Signalling over zenoh". `name` is one key chunk. Authorized like HTTP, with the token the query carries.
    pub fn zenoh_signalling(mut self, name: impl Into<String>) -> Self {
        self.zenoh_signalling = Some(name.into());
        self
    }

    /// Validates the options, registers the encodings and opens the zenoh session (unless one was given).
    pub async fn build(self) -> Result<Server> {
        let fraction = self.bandwidth_target_fraction;
        ensure!(fraction > 0.0 && fraction <= 1.0, "bandwidth target fraction must be within (0, 1], got {fraction}");
        if let Some(cap) = self.max_bandwidth_bytes_per_sec {
            ensure!(cap.is_finite() && cap > 0.0, "max bandwidth must be a positive number of bytes/s, got {cap}");
        }
        self.video_policy.validate()?;
        let codecs = EncodingRegistry::new(self.encodings)?.with_video(self.video_encoders, self.video_policy);
        let mut config = self.zenoh_config.unwrap_or_default();
        let (session, owns_session) = match self.session {
            Some(session) => {
                ensure!(self.connect.is_empty(), "connect endpoints only apply to a session zenoh-gateway opens; configure them on the session you pass instead");
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
        let gateway = Gateway::new(session.clone(), codecs, AllocationConfig { max_bandwidth: self.max_bandwidth_bytes_per_sec, target_fraction: fraction }, leases, self.connect_config);
        let inner = Arc::new(Inner { gateway, session, owns_session, serve_dir: self.serve_dir, shut_down: AtomicBool::new(false), authorize: self.authorize, signalling: Mutex::new(Vec::new()) });
        if let Some(name) = self.zenoh_signalling {
            ensure!(!name.is_empty() && !name.contains(['/', '*', '$', '?', '#']), "zenoh signalling name {name:?} must be one key chunk (no '/', '*', '$')");
            let tasks = signalling::serve(&inner, &name).await?;
            *inner.signalling.lock().unwrap() = tasks;
            info!("answering offers over zenoh on {SIGNALLING_PREFIX}/{name}/offer");
        }
        Ok(Server { inner })
    }
}

struct Inner {
    gateway: Arc<Gateway>,
    session: zenoh::Session,
    owns_session: bool,
    serve_dir: Option<PathBuf>,
    shut_down: AtomicBool,
    authorize: Option<Arc<Authorize>>,
    /// the zenoh signalling queryables' tasks
    signalling: Mutex<Vec<JoinHandle<()>>>,
}

impl Inner {
    /// The request's bearer token and what the hook grants it.
    fn authorize(&self, headers: &HeaderMap) -> Result<(Option<String>, Grant), String> {
        let token = headers.get(header::AUTHORIZATION).and_then(|value| value.to_str().ok()).and_then(|value| value.strip_prefix("Bearer ")).map(str::to_owned);
        let grant = self.authorize.as_ref().map_or_else(|| Ok(Grant::all()), |hook| hook(token.as_deref(), headers))?;
        Ok((token, grant))
    }
}

/// A configured zenoh-gateway server: the zenoh side is live, browsers connect once it is bound ([`bind`](Self::bind),
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

    /// Every open subscription of every connection: its key expression and encoding (e.g. for a relay that pulls upstream
    /// only what its viewers watch). [`changes`](Self::changes) says when this or [`leases`](Self::leases) changed.
    pub fn subscriptions(&self) -> Vec<(String, Option<String>)> {
        self.inner.gateway.subscriptions()
    }

    /// The leases held now: group and keys.
    pub fn leases(&self) -> Vec<(String, Vec<String>)> {
        self.inner.gateway.leases.held()
    }

    /// A counter bumped whenever a subscription opens or closes and a lease is taken or ends.
    pub fn changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.inner.gateway.leases.changes.subscribe()
    }

    /// Ends `group`'s lease as a force-expiry would, telling its holder `reason`; false when nobody holds it.
    pub async fn expire_lease(&self, group: &str, reason: &str) -> bool {
        self.inner.gateway.expire_lease(group, reason).await
    }

    /// Closes every live connection made with `token` (deadmen fire with reason `"revoked"`) and returns how many;
    /// the authorize hook decides whether the token may connect again.
    pub fn revoke(&self, token: &str) -> usize {
        self.inner.gateway.revoke(token)
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
        info!("zenoh-gateway listening on http://{local_addr}");
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
        for task in self.inner.signalling.lock().unwrap().drain(..) {
            task.abort();
        }
        self.inner.gateway.shutdown().await;
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
    Json(serde_json::json!({"service": "zenoh-gateway", "version": env!("CARGO_PKG_VERSION")}))
}

async fn ice_servers(State(inner): State<Arc<Inner>>, headers: HeaderMap) -> Response {
    match inner.authorize(&headers) {
        Ok((token, _)) => Json(minted_ice_servers(&inner, token).await).into_response(),
        Err(reason) => (StatusCode::UNAUTHORIZED, reason).into_response(),
    }
}

async fn offer(State(inner): State<Arc<Inner>>, headers: HeaderMap, Json(offer): Json<RTCSessionDescription>) -> Response {
    match answer(&inner, &headers, offer).await {
        Ok(answer) => Json(answer).into_response(),
        Err((status, reason)) => (status, reason).into_response(),
    }
}

/// Authorizes an offer and answers it, or the HTTP status and reason for refusing.
async fn answer(inner: &Inner, headers: &HeaderMap, offer: RTCSessionDescription) -> Result<RTCSessionDescription, (StatusCode, String)> {
    let (token, grant) = inner.authorize(headers).map_err(|reason| {
        info!("offer refused: {reason}");
        (StatusCode::UNAUTHORIZED, reason)
    })?;
    inner.gateway.answer(offer, token, grant).await.map_err(|error| {
        error!("offer failed: {error:#}");
        (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    })
}

/// `{"iceServers": [...]}` for a browser connecting with `token`.
async fn minted_ice_servers(inner: &Inner, token: Option<String>) -> serde_json::Value {
    let config = &inner.gateway.connect_config;
    let servers = ice::servers(&config.ice_servers, config.turn_secret.as_ref(), config.ice_servers_fn.as_ref(), IceRequest { side: IceSide::Browser, token }).await;
    serde_json::json!({"iceServers": servers})
}

/// Signalling over zenoh: the HTTP routes' twins as queryables (SPEC "Signalling over zenoh").
mod signalling {
    use super::*;

    /// A query's payload: `{"token"?, "offer"?}`.
    #[derive(serde::Deserialize, Default)]
    struct Request {
        token: Option<String>,
        offer: Option<RTCSessionDescription>,
    }

    pub(super) async fn serve(inner: &Arc<Inner>, name: &str) -> Result<Vec<JoinHandle<()>>> {
        let mut tasks = Vec::new();
        for op in ["offer", "ice"] {
            let queryable = inner.session.declare_queryable(format!("{SIGNALLING_PREFIX}/{name}/{op}")).await.map_err(|error| anyhow!("declaring the signalling queryable: {error}"))?;
            let inner = Arc::downgrade(inner);
            tasks.push(tokio::spawn(async move {
                while let Ok(query) = queryable.recv_async().await {
                    let Some(inner) = inner.upgrade() else { break };
                    tokio::spawn(async move {
                        let reply = respond(&inner, op, query.payload().map(|payload| payload.to_bytes().to_vec()).unwrap_or_default()).await;
                        let key = query.key_expr().clone();
                        let sent = match reply {
                            Ok(body) => query.reply(key, body.to_string()).await,
                            Err((status, reason)) => query.reply_err(serde_json::json!({"status": status.as_u16(), "error": reason}).to_string()).await,
                        };
                        if let Err(error) = sent {
                            error!("signalling reply failed: {error}");
                        }
                    });
                }
            }));
        }
        Ok(tasks)
    }

    async fn respond(inner: &Inner, op: &str, payload: Vec<u8>) -> Result<serde_json::Value, (StatusCode, String)> {
        let request: Request = if payload.is_empty() { Request::default() } else { serde_json::from_slice(&payload).map_err(|error| (StatusCode::BAD_REQUEST, format!("bad signalling request: {error}")))? };
        let mut headers = HeaderMap::new();
        if let Some(token) = &request.token {
            let value = format!("Bearer {token}").parse().map_err(|_| (StatusCode::BAD_REQUEST, "bad token".to_owned()))?;
            headers.insert(header::AUTHORIZATION, value);
        }
        match (op, request.offer) {
            ("ice", _) => {
                let (token, _) = inner.authorize(&headers).map_err(|reason| (StatusCode::UNAUTHORIZED, reason))?;
                Ok(minted_ice_servers(inner, token).await)
            }
            (_, Some(offer)) => answer(inner, &headers, offer).await.map(|answer| serde_json::to_value(answer).unwrap_or_default()),
            (_, None) => Err((StatusCode::BAD_REQUEST, "no offer".to_owned())),
        }
    }
}
