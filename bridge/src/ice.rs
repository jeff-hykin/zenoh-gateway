//! STUN/TURN servers for both ends of each connection: static ones, coturn's time-limited credentials, and servers
//! an embedder mints per connection ([`IceServersFn`], e.g. [`CloudflareTurn`]).

use base64::Engine;
use futures::future::BoxFuture;
use hmac::{Hmac, Mac};
use log::warn;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A STUN or TURN server, in the browser's `RTCIceServer` shape (`GET /zenoh-web/ice` returns these).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct IceServer {
    /// e.g. `stun:stun.example.org:3478`, `turn:relay.example.org:3478?transport=udp`
    pub urls: Vec<String>,
    /// TURN user (left empty with a TURN secret: minted per connection)
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub username: String,
    /// TURN password
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub credential: String,
}

/// coturn's TURN REST API credentials (`use-auth-secret`): username `"<unix expiry>:<user>"`, credential
/// base64(HMAC-SHA1(secret, username)), valid for `ttl`.
pub fn turn_credentials(secret: &str, user: &str, ttl: Duration, now: SystemTime) -> (String, String) {
    let expiry = (now + ttl).duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
    let username = format!("{expiry}:{user}");
    let mut mac = Hmac::<sha1::Sha1>::new_from_slice(secret.as_bytes()).expect("HMAC takes any key length");
    mac.update(username.as_bytes());
    (username, base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn is_turn(server: &IceServer) -> bool {
    server.urls.iter().any(|url| url.starts_with("turn"))
}

/// The servers with credentials minted for `user` on every TURN entry that has none, when a secret is set.
pub(crate) fn mint(servers: &[IceServer], secret: Option<&(String, Duration)>, user: &str) -> Vec<IceServer> {
    servers
        .iter()
        .map(|server| match secret {
            Some((secret, ttl)) if server.username.is_empty() && is_turn(server) => {
                let (username, credential) = turn_credentials(secret, user, *ttl, SystemTime::now());
                IceServer { username, credential, ..server.clone() }
            }
            _ => server.clone(),
        })
        .collect()
}

/// Which end of a connection asks for ICE servers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IceSide {
    /// a browser or client, through `GET` [`ICE_PATH`](crate::ICE_PATH) or the zenoh `ice` queryable
    Browser,
    /// the bridge's own end of a connection it is answering
    Bridge,
}

/// What an [`IceServersFn`] is asked for: one end of one connection.
#[derive(Debug, Clone)]
pub struct IceRequest {
    /// which end
    pub side: IceSide,
    /// the connection's bearer token, if it sent one
    pub token: Option<String>,
}

/// The ICE servers hook ([`ServerBuilder::ice_servers_fn`](crate::ServerBuilder::ice_servers_fn)): servers for one end of
/// one connection, added after the static ones; an error (or no answer in [`ICE_HOOK_TIMEOUT`]) leaves just the static ones.
pub type IceServersFn = dyn Fn(IceRequest) -> BoxFuture<'static, anyhow::Result<Vec<IceServer>>> + Send + Sync;

/// How long a connection waits on the [`IceServersFn`] before going on with the static servers.
pub const ICE_HOOK_TIMEOUT: Duration = Duration::from_secs(5);

/// [`IceServersFn`] in a config that is `Debug`.
#[derive(Clone)]
pub(crate) struct IceHook(pub Arc<IceServersFn>);

impl std::fmt::Debug for IceHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("IceHook")
    }
}

/// The static servers (TURN credentials minted with `secret`), then the hook's, for `request`.
pub(crate) async fn servers(statics: &[IceServer], secret: Option<&(String, Duration)>, hook: Option<&IceHook>, request: IceRequest) -> Vec<IceServer> {
    let user = match request.side {
        IceSide::Browser => "browser",
        IceSide::Bridge => "bridge",
    };
    let mut servers = mint(statics, secret, user);
    if let Some(IceHook(hook)) = hook {
        match tokio::time::timeout(ICE_HOOK_TIMEOUT, hook(request)).await {
            Ok(Ok(minted)) => {
                // a browser refuses the whole connection over one TURN entry without credentials
                let (usable, unusable): (Vec<_>, Vec<_>) = minted.into_iter().partition(|server| !is_turn(server) || (!server.username.is_empty() && !server.credential.is_empty()));
                if !unusable.is_empty() {
                    warn!("ICE servers hook: dropped {} TURN server(s) without a username and credential", unusable.len());
                }
                servers.extend(usable)
            }
            Ok(Err(error)) => warn!("ICE servers hook failed, using the static servers: {error:#}"),
            Err(_) => warn!("ICE servers hook gave no answer in {ICE_HOOK_TIMEOUT:?}, using the static servers"),
        }
    }
    servers
}

#[cfg(feature = "cloudflare")]
pub use cloudflare::CloudflareTurn;

#[cfg(feature = "cloudflare")]
mod cloudflare {
    use super::IceServer;
    use anyhow::{Context, Result, bail};
    use std::time::{Duration, Instant};
    use tokio::sync::Mutex;

    /// Cloudflare TURN credentials from its API (feature `cloudflare`), for
    /// [`ServerBuilder::cloudflare_turn`](crate::ServerBuilder::cloudflare_turn).
    ///
    /// By default one set of credentials is shared by every connection and minted again once half its `ttl` has passed,
    /// so each connection gets credentials valid for at least `ttl / 2`; [`per_connection`](Self::per_connection) mints
    /// a set for each. Port-53 URLs are dropped (browsers block that port).
    pub struct CloudflareTurn {
        key_id: String,
        api_token: String,
        ttl: Duration,
        per_connection: bool,
        api_base: String,
        http: reqwest::Client,
        cached: Mutex<Option<(Instant, Vec<IceServer>)>>,
    }

    /// The API's reply: `iceServers` is a list (`generate-ice-servers`) or one server (the older `generate`).
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Generated {
        ice_servers: OneOrMany,
    }

    #[derive(serde::Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        Many(Vec<IceServer>),
        One(IceServer),
    }

    impl CloudflareTurn {
        /// The API's default base URL.
        pub const API_BASE: &str = "https://rtc.live.cloudflare.com/v1";

        /// A TURN key's id and API token (Cloudflare dashboard → Realtime → TURN). Credentials last 24 h by default.
        pub fn new(key_id: impl Into<String>, api_token: impl Into<String>) -> Self {
            CloudflareTurn {
                key_id: key_id.into(),
                api_token: api_token.into(),
                ttl: Duration::from_secs(24 * 3600),
                per_connection: false,
                api_base: Self::API_BASE.to_owned(),
                http: reqwest::Client::new(),
                cached: Mutex::new(None),
            }
        }

        /// How long minted credentials are valid (Cloudflare: the longest call you expect).
        pub fn ttl(mut self, ttl: Duration) -> Self {
            self.ttl = ttl;
            self
        }

        /// Mints credentials for every connection instead of sharing them.
        pub fn per_connection(mut self, per_connection: bool) -> Self {
            self.per_connection = per_connection;
            self
        }

        /// Another API base URL (e.g. a test server).
        pub fn api_base(mut self, url: impl Into<String>) -> Self {
            self.api_base = url.into();
            self
        }

        /// Mints a fresh set of servers from the API.
        pub async fn generate(&self) -> Result<Vec<IceServer>> {
            let url = format!("{}/turn/keys/{}/credentials/generate-ice-servers", self.api_base.trim_end_matches('/'), self.key_id);
            let response = self.http.post(url).bearer_auth(&self.api_token).json(&serde_json::json!({"ttl": self.ttl.as_secs()})).send().await.context("asking Cloudflare for TURN credentials")?;
            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                bail!("Cloudflare TURN credentials: HTTP {status}: {}", body.chars().take(300).collect::<String>());
            }
            let generated: Generated = response.json().await.context("reading Cloudflare's TURN credentials")?;
            let servers = match generated.ice_servers {
                OneOrMany::Many(servers) => servers,
                OneOrMany::One(server) => vec![server],
            };
            Ok(servers
                .into_iter()
                .map(|server| IceServer { urls: server.urls.into_iter().filter(|url| !is_port_53(url)).collect(), ..server })
                .filter(|server| !server.urls.is_empty())
                .collect())
        }

        /// The servers for one connection: a fresh set, or the shared set while it has at least half its `ttl` left.
        pub async fn servers(&self) -> Result<Vec<IceServer>> {
            if self.per_connection {
                return self.generate().await;
            }
            let mut cached = self.cached.lock().await;
            if let Some((minted_at, servers)) = cached.as_ref() {
                if minted_at.elapsed() < self.ttl / 2 {
                    return Ok(servers.clone());
                }
            }
            let servers = self.generate().await?;
            *cached = Some((Instant::now(), servers.clone()));
            Ok(servers)
        }
    }

    /// `turn:host:53?transport=udp`, `stun:host:53`
    fn is_port_53(url: &str) -> bool {
        url.split('?').next().is_some_and(|address| address.ends_with(":53"))
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use axum::routing::post;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// A fake API on localhost that counts its calls and answers with `reply` (status 201) or 401 when `refuse`.
        async fn fake_api(reply: serde_json::Value, refuse: bool) -> (String, Arc<AtomicUsize>, Arc<std::sync::Mutex<Option<(String, serde_json::Value)>>>) {
            let calls = Arc::new(AtomicUsize::new(0));
            let seen = Arc::new(std::sync::Mutex::new(None));
            let (calls_in, seen_in) = (calls.clone(), seen.clone());
            let app = axum::Router::new().route(
                "/v1/turn/keys/{key}/credentials/generate-ice-servers",
                post(move |headers: axum::http::HeaderMap, axum::Json(body): axum::Json<serde_json::Value>| {
                    let (calls, seen, reply) = (calls_in.clone(), seen_in.clone(), reply.clone());
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        let auth = headers.get("authorization").and_then(|value| value.to_str().ok()).unwrap_or_default().to_owned();
                        *seen.lock().unwrap() = Some((auth, body));
                        if refuse { (axum::http::StatusCode::UNAUTHORIZED, axum::Json(serde_json::json!({"error": "bad token"}))) } else { (axum::http::StatusCode::CREATED, axum::Json(reply)) }
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            (format!("http://{address}/v1"), calls, seen)
        }

        fn reply() -> serde_json::Value {
            serde_json::json!({"iceServers": [
                {"urls": ["stun:stun.cloudflare.com:3478", "stun:stun.cloudflare.com:53"]},
                {"urls": ["turn:turn.cloudflare.com:3478?transport=udp", "turn:turn.cloudflare.com:53?transport=udp", "turns:turn.cloudflare.com:443?transport=tcp"], "username": "u1", "credential": "c1"},
            ]})
        }

        #[tokio::test]
        async fn mints_with_the_token_and_ttl_and_drops_port_53() {
            let (base, _, seen) = fake_api(reply(), false).await;
            let servers = CloudflareTurn::new("key", "secret-token").ttl(Duration::from_secs(600)).api_base(base).generate().await.unwrap();
            assert_eq!(servers, vec![
                IceServer { urls: vec!["stun:stun.cloudflare.com:3478".into()], ..Default::default() },
                IceServer { urls: vec!["turn:turn.cloudflare.com:3478?transport=udp".into(), "turns:turn.cloudflare.com:443?transport=tcp".into()], username: "u1".into(), credential: "c1".into() },
            ]);
            assert_eq!(seen.lock().unwrap().clone().unwrap(), ("Bearer secret-token".to_owned(), serde_json::json!({"ttl": 600})));
        }

        #[tokio::test]
        async fn reads_the_older_single_server_reply() {
            let (base, _, _) = fake_api(serde_json::json!({"iceServers": {"urls": ["turn:t:3478"], "username": "u", "credential": "c"}}), false).await;
            let servers = CloudflareTurn::new("key", "token").api_base(base).generate().await.unwrap();
            assert_eq!(servers, vec![IceServer { urls: vec!["turn:t:3478".into()], username: "u".into(), credential: "c".into() }]);
        }

        #[tokio::test]
        async fn shares_until_half_the_ttl_or_mints_per_connection() {
            let (base, calls, _) = fake_api(reply(), false).await;
            let shared = CloudflareTurn::new("key", "token").api_base(base.clone());
            for _ in 0..3 {
                shared.servers().await.unwrap();
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            let short = CloudflareTurn::new("key", "token").ttl(Duration::from_millis(40)).api_base(base.clone());
            short.servers().await.unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            short.servers().await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 3);
            let each = CloudflareTurn::new("key", "token").per_connection(true).api_base(base);
            each.servers().await.unwrap();
            each.servers().await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 5);
        }

        #[tokio::test]
        async fn a_refusal_is_an_error_that_names_the_status() {
            let (base, _, _) = fake_api(reply(), true).await;
            let error = CloudflareTurn::new("key", "token").api_base(base).servers().await.unwrap_err();
            assert!(format!("{error:#}").contains("401"), "{error:#}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_match_coturn() {
        // `printf 1700000000:alice | openssl dgst -sha1 -hmac north -binary | base64`, what coturn checks with static-auth-secret=north
        let (username, credential) = turn_credentials("north", "alice", Duration::from_secs(3600), UNIX_EPOCH + Duration::from_secs(1_699_996_400));
        assert_eq!(username, "1700000000:alice");
        assert_eq!(credential, "Cd/49soE35ICqcJF/bCTn8Z4OyE=");
    }

    #[test]
    fn mints_only_turn_entries_without_credentials() {
        let servers = vec![
            IceServer { urls: vec!["stun:s:3478".into()], ..Default::default() },
            IceServer { urls: vec!["turn:t:3478".into()], ..Default::default() },
            IceServer { urls: vec!["turn:u:3478".into()], username: "fixed".into(), credential: "pw".into() },
        ];
        let minted = mint(&servers, Some(&("north".into(), Duration::from_secs(60))), "peer");
        assert_eq!((&minted[0], &minted[2]), (&servers[0], &servers[2]));
        assert!(minted[1].username.ends_with(":peer") && !minted[1].credential.is_empty());
        assert_eq!(mint(&servers, None, "peer"), servers);
    }

    #[tokio::test]
    async fn hook_servers_follow_the_static_ones_and_an_error_leaves_the_static_ones() {
        let statics = vec![IceServer { urls: vec!["stun:s:3478".into()], ..Default::default() }];
        let extra = IceServer { urls: vec!["turn:t:3478".into()], username: "u".into(), credential: "c".into() };
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_in = seen.clone();
        let extra_in = extra.clone();
        let good = IceHook(Arc::new(move |request: IceRequest| {
            seen_in.lock().unwrap().push((request.side, request.token));
            let extra = extra_in.clone();
            Box::pin(async move { Ok(vec![extra]) }) as BoxFuture<'static, anyhow::Result<Vec<IceServer>>>
        }));
        let request = IceRequest { side: IceSide::Bridge, token: Some("t".into()) };
        assert_eq!(servers(&statics, None, Some(&good), request).await, vec![statics[0].clone(), extra]);
        assert_eq!(seen.lock().unwrap().as_slice(), &[(IceSide::Bridge, Some("t".to_owned()))]);
        let bare_turn = IceHook(Arc::new(|_| Box::pin(async { Ok(vec![IceServer { urls: vec!["turn:t:3478".into()], ..Default::default() }]) })));
        assert_eq!(servers(&statics, None, Some(&bare_turn), IceRequest { side: IceSide::Browser, token: None }).await, statics, "a TURN entry without credentials is dropped");
        let failing = IceHook(Arc::new(|_| Box::pin(async { anyhow::bail!("down") })));
        assert_eq!(servers(&statics, None, Some(&failing), IceRequest { side: IceSide::Browser, token: None }).await, statics);
    }
}
