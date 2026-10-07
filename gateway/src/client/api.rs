//! The rest of zenoh's API through the gateway (SPEC "The rest of the zenoh API"): `put`/`delete`
//! with options, `get` with options, queryables this client answers, liveliness, matching, and
//! session info. Each call is one request on the `control` channel; queries, liveliness changes
//! and matching changes come back as events.

use super::{Client, GetReply, Inner, PING_TIMEOUT};
use anyhow::{Result, anyhow};
use base64::Engine;
use serde_json::{Value, json};
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::sync::mpsc;

/// Events routed to a handle, and the field naming the handle.
pub(super) const EVENT_IDS: &[(&str, &str)] = &[("query", "queryableId"), ("liveliness", "subId"), ("matching", "listenerId")];

fn base64() -> base64::engine::general_purpose::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

fn decoded(value: &Value) -> Result<Option<Vec<u8>>> {
    value.as_str().map(|text| base64().decode(text).map_err(|error| anyhow!("bad base64: {error}"))).transpose()
}

/// zenoh's congestion control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CongestionControl {
    /// drop the message when the queue is full
    Drop,
    /// wait for room
    Block,
}

impl CongestionControl {
    fn as_str(self) -> &'static str {
        match self {
            CongestionControl::Drop => "drop",
            CongestionControl::Block => "block",
        }
    }
}

/// Options of [`Client::put`] and [`Client::delete`].
#[derive(Debug, Clone, Default)]
pub struct PutOptions {
    /// e.g. "text/plain" (put only)
    pub encoding: Option<String>,
    /// the attachment
    pub attachment: Option<Vec<u8>>,
    /// zenoh priority, 1 (real time) to 7 (background)
    pub priority: Option<u8>,
    /// congestion control
    pub congestion_control: Option<CongestionControl>,
    /// send right away instead of batching
    pub express: bool,
    /// the sample's timestamp, unix ms on the gateway's clock (default: none)
    pub timestamp_ms: Option<f64>,
}

impl PutOptions {
    fn apply(&self, request: &mut Value) {
        if let Some(encoding) = &self.encoding {
            request["encoding"] = json!(encoding);
        }
        if let Some(attachment) = &self.attachment {
            request["attachment"] = json!(base64().encode(attachment));
        }
        if let Some(priority) = self.priority {
            request["priority"] = json!(priority);
        }
        if let Some(congestion_control) = self.congestion_control {
            request["congestionControl"] = json!(congestion_control.as_str());
        }
        if self.express {
            request["express"] = json!(true);
        }
        if let Some(timestamp_ms) = self.timestamp_ms {
            request["timestamp"] = json!(timestamp_ms);
        }
    }
}

/// Which queryables a query reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryTarget {
    /// the queryables best placed to answer
    BestMatching,
    /// every matching queryable
    All,
    /// every complete matching queryable
    AllComplete,
}

/// How replies with the same key are merged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsolidationMode {
    /// zenoh picks
    Auto,
    /// every reply as it comes
    None,
    /// drop replies older than one already delivered for the key
    Monotonic,
    /// only the latest reply per key, once all are in
    Latest,
}

/// Options of [`Client::get_with`] (and a [`Querier`]).
#[derive(Debug, Clone, Default)]
pub struct GetOptions {
    /// the selector's parameters ("a=1;b=2"); also allowed inline in the key after `?`
    pub parameters: Option<String>,
    /// the query's payload
    pub payload: Option<Vec<u8>>,
    /// the payload's encoding
    pub encoding: Option<String>,
    /// the attachment
    pub attachment: Option<Vec<u8>>,
    /// which queryables it reaches
    pub target: Option<QueryTarget>,
    /// how replies with the same key are merged
    pub consolidation: Option<ConsolidationMode>,
    /// default 5 s
    pub timeout: Option<Duration>,
    /// zenoh priority, 1 (real time) to 7 (background)
    pub priority: Option<u8>,
    /// congestion control
    pub congestion_control: Option<CongestionControl>,
    /// send right away instead of batching
    pub express: bool,
}

impl GetOptions {
    fn timeout(&self) -> Duration {
        self.timeout.unwrap_or(Duration::from_secs(5))
    }

    fn request(&self, key: &str) -> Value {
        let mut request = json!({"op": "get", "key": key, "timeoutMs": self.timeout().as_millis() as u64});
        if let Some(parameters) = &self.parameters {
            request["parameters"] = json!(parameters);
        }
        if let Some(payload) = &self.payload {
            request["payload"] = json!(base64().encode(payload));
        }
        if let Some(encoding) = &self.encoding {
            request["encoding"] = json!(encoding);
        }
        if let Some(attachment) = &self.attachment {
            request["attachment"] = json!(base64().encode(attachment));
        }
        if let Some(target) = self.target {
            request["target"] = json!(match target {
                QueryTarget::BestMatching => "bestMatching",
                QueryTarget::All => "all",
                QueryTarget::AllComplete => "allComplete",
            });
        }
        if let Some(consolidation) = self.consolidation {
            request["consolidation"] = json!(match consolidation {
                ConsolidationMode::Auto => "auto",
                ConsolidationMode::None => "none",
                ConsolidationMode::Monotonic => "monotonic",
                ConsolidationMode::Latest => "latest",
            });
        }
        if let Some(priority) = self.priority {
            request["priority"] = json!(priority);
        }
        if let Some(congestion_control) = self.congestion_control {
            request["congestionControl"] = json!(congestion_control.as_str());
        }
        if self.express {
            request["express"] = json!(true);
        }
        request
    }
}

pub(super) fn parse_reply(reply: &Value) -> Result<GetReply> {
    let encoding = reply["encoding"].as_str().map(str::to_owned);
    Ok(match reply.get("error") {
        Some(error) => {
            GetReply { key: None, bytes: decoded(error)?.unwrap_or_default(), error: true, encoding, attachment: None, delete: false, timestamp_ms: None }
        }
        None => GetReply {
            key: reply["key"].as_str().map(str::to_owned),
            bytes: decoded(&reply["bytes"])?.unwrap_or_default(),
            error: false,
            encoding,
            attachment: decoded(&reply["attachment"])?,
            delete: reply["kind"] == "delete",
            timestamp_ms: reply["timestamp"].as_f64(),
        },
    })
}

/// A connection's view of the zenoh network (see [`Client::info`]).
#[derive(Debug, Clone)]
pub struct SessionInfo {
    /// the gateway's zenoh id
    pub zid: String,
    /// the zenoh ids of the routers it is connected to
    pub routers: Vec<String>,
    /// the zenoh ids of the peers it is connected to
    pub peers: Vec<String>,
}

/// Whose presence [`Client::matching_status`] reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchingTarget {
    /// subscribers a publisher on the key would reach
    Subscribers,
    /// queryables a querier on the key would reach
    Queryables,
}

impl MatchingTarget {
    fn as_str(self) -> &'static str {
        match self {
            MatchingTarget::Subscribers => "subscribers",
            MatchingTarget::Queryables => "queryables",
        }
    }
}

/// A route for one handle's events, removed when the handle goes.
struct Route {
    client: Weak<Inner>,
    key: (&'static str, u64),
}

impl Drop for Route {
    fn drop(&mut self) {
        if let Some(inner) = self.client.upgrade() {
            inner.shared.api_routes.lock().unwrap().remove(&self.key);
        }
    }
}

impl Client {
    async fn api_request(&self, request: Value, timeout: Duration) -> Result<Value> {
        self.inner.request(request, timeout).await
    }

    fn route(&self, event: &'static str, id: u64) -> (Route, mpsc::UnboundedReceiver<Value>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut routes = self.inner.shared.api_routes.lock().unwrap();
        for message in self.inner.shared.api_early.lock().unwrap().remove(&(event, id)).unwrap_or_default() {
            let _ = sender.send(message);
        }
        routes.insert((event, id), sender);
        (Route { client: Arc::downgrade(&self.inner), key: (event, id) }, receiver)
    }

    /// A one-off zenoh put through the gateway (needs the `publish` grant).
    pub async fn put(&self, key: &str, bytes: impl AsRef<[u8]>, options: PutOptions) -> Result<()> {
        let mut request = json!({"op": "put", "key": key, "bytes": base64().encode(bytes)});
        options.apply(&mut request);
        self.api_request(request, PING_TIMEOUT).await.map(drop)
    }

    /// A zenoh delete through the gateway (needs the `publish` grant).
    pub async fn delete(&self, key: &str, options: PutOptions) -> Result<()> {
        let mut request = json!({"op": "delete", "key": key});
        options.apply(&mut request);
        self.api_request(request, PING_TIMEOUT).await.map(drop)
    }

    /// A zenoh query with options.
    pub async fn get_with(&self, key: &str, options: GetOptions) -> Result<Vec<GetReply>> {
        let reply = self.api_request(options.request(key), options.timeout() + Duration::from_secs(2)).await?;
        reply["replies"].as_array().map(Vec::as_slice).unwrap_or_default().iter().map(parse_reply).collect()
    }

    /// Queries with fixed options, like zenoh's querier.
    pub fn querier(&self, key: &str, options: GetOptions) -> Querier {
        Querier { client: Client { inner: self.inner.clone() }, key: key.to_owned(), options }
    }

    /// Answers queries on `key` from this client (needs the `queryable` grant). `complete`: it
    /// answers for every key under `key`.
    pub async fn declare_queryable(&self, key: &str, complete: bool) -> Result<Queryable> {
        let reply = self.api_request(json!({"op": "declareQueryable", "key": key, "complete": complete}), PING_TIMEOUT).await?;
        let id = reply["queryableId"].as_u64().ok_or_else(|| anyhow!("no queryableId"))?;
        let (route, receiver) = self.route("query", id);
        Ok(Queryable { client: Client { inner: self.inner.clone() }, id, receiver, _route: route })
    }

    /// A liveliness token on `key`, alive until undeclared or this client goes (needs `liveliness`).
    pub async fn declare_token(&self, key: &str) -> Result<LivelinessToken> {
        let reply = self.api_request(json!({"op": "declareToken", "key": key}), PING_TIMEOUT).await?;
        let id = reply["tokenId"].as_u64().ok_or_else(|| anyhow!("no tokenId"))?;
        Ok(LivelinessToken { client: Client { inner: self.inner.clone() }, id })
    }

    /// Liveliness tokens appearing and disappearing under `key`; `history` also reports the ones
    /// already alive.
    pub async fn liveliness_subscribe(&self, key: &str, history: bool) -> Result<LivelinessSubscriber> {
        let reply = self.api_request(json!({"op": "livelinessSubscribe", "key": key, "history": history}), PING_TIMEOUT).await?;
        let id = reply["subId"].as_u64().ok_or_else(|| anyhow!("no subId"))?;
        let (route, receiver) = self.route("liveliness", id);
        Ok(LivelinessSubscriber { client: Client { inner: self.inner.clone() }, id, receiver, _route: route })
    }

    /// The keys of the liveliness tokens alive under `key`.
    pub async fn liveliness_get(&self, key: &str, timeout: Duration) -> Result<Vec<String>> {
        let reply =
            self.api_request(json!({"op": "livelinessGet", "key": key, "timeoutMs": timeout.as_millis() as u64}), timeout + Duration::from_secs(2)).await?;
        Ok(reply["tokens"].as_array().map(Vec::as_slice).unwrap_or_default().iter().filter_map(|key| key.as_str().map(str::to_owned)).collect())
    }

    /// Whether a publisher (subscribers) or a querier (queryables) on `key` would reach anyone now.
    pub async fn matching_status(&self, key: &str, target: MatchingTarget) -> Result<bool> {
        let reply = self.api_request(json!({"op": "matchingStatus", "key": key, "matching": target.as_str()}), PING_TIMEOUT + Duration::from_secs(5)).await?;
        reply["matching"].as_bool().ok_or_else(|| anyhow!("no matching"))
    }

    /// Notified each time [`Client::matching_status`] for `key` changes.
    pub async fn matching_listener(&self, key: &str, target: MatchingTarget) -> Result<MatchingListener> {
        let reply = self.api_request(json!({"op": "declareMatchingListener", "key": key, "matching": target.as_str()}), PING_TIMEOUT).await?;
        let id = reply["listenerId"].as_u64().ok_or_else(|| anyhow!("no listenerId"))?;
        let (route, receiver) = self.route("matching", id);
        Ok(MatchingListener { client: Client { inner: self.inner.clone() }, id, receiver, _route: route })
    }

    /// The gateway's zenoh session: its id, and the routers and peers it is connected to.
    pub async fn info(&self) -> Result<SessionInfo> {
        let reply = self.api_request(json!({"op": "info"}), PING_TIMEOUT).await?;
        let strings =
            |value: &Value| value.as_array().map(Vec::as_slice).unwrap_or_default().iter().filter_map(|item| item.as_str().map(str::to_owned)).collect();
        Ok(SessionInfo { zid: reply["zid"].as_str().unwrap_or_default().to_owned(), routers: strings(&reply["routers"]), peers: strings(&reply["peers"]) })
    }
}

/// Fixed-option queries on one key ([`Client::querier`]).
pub struct Querier {
    client: Client,
    key: String,
    options: GetOptions,
}

impl Querier {
    /// Queries with the querier's options.
    pub async fn get(&self) -> Result<Vec<GetReply>> {
        self.client.get_with(&self.key, self.options.clone()).await
    }

    /// With per-call parameters, payload and attachment on top of the querier's options.
    pub async fn get_with(&self, parameters: Option<&str>, payload: Option<Vec<u8>>, attachment: Option<Vec<u8>>) -> Result<Vec<GetReply>> {
        let mut options = self.options.clone();
        options.parameters = parameters.map(str::to_owned).or(options.parameters);
        options.payload = payload.or(options.payload);
        options.attachment = attachment.or(options.attachment);
        self.client.get_with(&self.key, options).await
    }

    /// Whether a queryable would answer now.
    pub async fn matching_status(&self) -> Result<bool> {
        self.client.matching_status(&self.key, MatchingTarget::Queryables).await
    }
}

/// A query this client's [`Queryable`] received. Reply as often as needed, then [`Query::finalize`]
/// (or drop it: the gateway finalizes forgotten queries after two minutes).
pub struct Query {
    client: Client,
    /// the query's id (on this connection)
    pub id: u64,
    /// the key
    pub key: String,
    /// the selector's parameters
    pub parameters: String,
    /// the query's payload
    pub payload: Option<Vec<u8>>,
    /// the payload's encoding
    pub encoding: Option<String>,
    /// the attachment
    pub attachment: Option<Vec<u8>>,
}

impl Query {
    /// Replies with a sample on `key` (default: the query's key).
    pub async fn reply(&self, key: Option<&str>, bytes: impl AsRef<[u8]>, encoding: Option<&str>, attachment: Option<&[u8]>) -> Result<()> {
        let mut request = json!({"op": "reply", "queryId": self.id, "key": key.unwrap_or_default(), "bytes": base64().encode(bytes)});
        if let Some(encoding) = encoding {
            request["encoding"] = json!(encoding);
        }
        if let Some(attachment) = attachment {
            request["attachment"] = json!(base64().encode(attachment));
        }
        self.client.api_request(request, PING_TIMEOUT).await.map(drop)
    }

    /// Replies with an error.
    pub async fn reply_err(&self, bytes: impl AsRef<[u8]>, encoding: Option<&str>) -> Result<()> {
        let mut request = json!({"op": "replyErr", "queryId": self.id, "bytes": base64().encode(bytes)});
        if let Some(encoding) = encoding {
            request["encoding"] = json!(encoding);
        }
        self.client.api_request(request, PING_TIMEOUT).await.map(drop)
    }

    /// Replies that `key` (default: the query's key) was deleted.
    pub async fn reply_del(&self, key: Option<&str>) -> Result<()> {
        self.client.api_request(json!({"op": "replyDel", "queryId": self.id, "key": key.unwrap_or_default()}), PING_TIMEOUT).await.map(drop)
    }

    /// No more replies: the asker's get completes.
    pub async fn finalize(self) -> Result<()> {
        self.client.api_request(json!({"op": "finalizeQuery", "queryId": self.id}), PING_TIMEOUT).await.map(drop)
    }
}

/// Queries for this client to answer ([`Client::declare_queryable`]).
pub struct Queryable {
    client: Client,
    id: u64,
    receiver: mpsc::UnboundedReceiver<Value>,
    _route: Route,
}

impl Queryable {
    /// The next query (None once the connection is gone).
    pub async fn recv(&mut self) -> Option<Query> {
        let event = self.receiver.recv().await?;
        Some(Query {
            client: Client { inner: self.client.inner.clone() },
            id: event["queryId"].as_u64().unwrap_or_default(),
            key: event["key"].as_str().unwrap_or_default().to_owned(),
            parameters: event["parameters"].as_str().unwrap_or_default().to_owned(),
            payload: decoded(&event["payload"]).ok().flatten(),
            encoding: event["encoding"].as_str().map(str::to_owned),
            attachment: decoded(&event["attachment"]).ok().flatten(),
        })
    }

    /// Ends it on the gateway.
    pub async fn undeclare(self) -> Result<()> {
        self.client.api_request(json!({"op": "undeclareQueryable", "queryableId": self.id}), PING_TIMEOUT).await.map(drop)
    }
}

/// A liveliness token ([`Client::declare_token`]).
pub struct LivelinessToken {
    client: Client,
    id: u64,
}

impl LivelinessToken {
    /// Ends it on the gateway.
    pub async fn undeclare(self) -> Result<()> {
        self.client.api_request(json!({"op": "undeclareToken", "tokenId": self.id}), PING_TIMEOUT).await.map(drop)
    }
}

/// A liveliness change: a token appeared (`alive`) or went.
#[derive(Debug, Clone, PartialEq)]
pub struct LivelinessChange {
    /// the key
    pub key: String,
    /// the token appeared (true) or went (false)
    pub alive: bool,
}

/// Liveliness changes under a key ([`Client::liveliness_subscribe`]).
pub struct LivelinessSubscriber {
    client: Client,
    id: u64,
    receiver: mpsc::UnboundedReceiver<Value>,
    _route: Route,
}

impl LivelinessSubscriber {
    /// The next change (None once the connection is gone).
    pub async fn recv(&mut self) -> Option<LivelinessChange> {
        let event = self.receiver.recv().await?;
        Some(LivelinessChange { key: event["key"].as_str().unwrap_or_default().to_owned(), alive: event["kind"] == "put" })
    }

    /// Ends it on the gateway.
    pub async fn undeclare(self) -> Result<()> {
        self.client.api_request(json!({"op": "livelinessUnsubscribe", "subId": self.id}), PING_TIMEOUT).await.map(drop)
    }
}

/// Matching changes ([`Client::matching_listener`]).
pub struct MatchingListener {
    client: Client,
    id: u64,
    receiver: mpsc::UnboundedReceiver<Value>,
    _route: Route,
}

impl MatchingListener {
    /// Whether anyone matches, each time that changes.
    pub async fn recv(&mut self) -> Option<bool> {
        self.receiver.recv().await?["matching"].as_bool()
    }

    /// Ends it on the gateway.
    pub async fn undeclare(self) -> Result<()> {
        self.client.api_request(json!({"op": "undeclareMatchingListener", "listenerId": self.id}), PING_TIMEOUT).await.map(drop)
    }
}

impl super::Publisher {
    /// A zenoh delete on this publisher's key.
    pub async fn delete(&self, options: PutOptions) -> Result<()> {
        let inner = self.client.upgrade().ok_or_else(|| anyhow!("the client is gone"))?;
        Client { inner }.delete(&self.key, options).await
    }
}
