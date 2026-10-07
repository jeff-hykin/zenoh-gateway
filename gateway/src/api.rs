//! The rest of zenoh's API for a frontend, over the `control` channel: session `put`/`delete`
//! with options, `get` with options, queryables the frontend answers, liveliness, matching, and
//! session info. Payloads and attachments travel as base64.
//!
//! Requests are `{id, op, ...}` and answers `{id, ok, ...}` (see `peer::run_control`); queries,
//! liveliness changes and matching changes arrive as events: `{event: "query" | "liveliness" | "matching", ...}`.

use crate::auth::Grant;
use base64::Engine;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use zenoh::bytes::{Encoding, ZBytes};
use zenoh::liveliness::LivelinessToken;
use zenoh::matching::MatchingListener;
use zenoh::pubsub::{Publisher, Subscriber};
use zenoh::qos::{CongestionControl, Priority};
use zenoh::query::{ConsolidationMode, Querier, Query, QueryTarget, Queryable, Reply};
use zenoh::sample::{Sample, SampleKind};
use zenoh::time::{NTP64, Timestamp};
use zenoh::{Session, Wait};

/// The ops this module answers.
pub(crate) const OPS: &[&str] = &[
    "put",
    "delete",
    "get",
    "declareQueryable",
    "undeclareQueryable",
    "reply",
    "replyErr",
    "replyDel",
    "finalizeQuery",
    "declareToken",
    "undeclareToken",
    "livelinessSubscribe",
    "livelinessUnsubscribe",
    "livelinessGet",
    "matchingStatus",
    "declareMatchingListener",
    "undeclareMatchingListener",
    "info",
];

/// Ops that wait on the network, so the control loop runs them in their own task.
pub(crate) const SLOW_OPS: &[&str] = &["get", "livelinessGet", "matchingStatus"];

const DEFAULT_GET_TIMEOUT_MS: u64 = 5000;
/// A query the frontend never finalizes is answered as final after this (the asker has long given up).
const QUERY_LIFETIME: Duration = Duration::from_secs(120);

/// One frontend's declared entities.
#[derive(Default)]
pub(crate) struct ApiState {
    /// shared with queryable callbacks, which number the queries they receive
    next_id: Arc<AtomicU64>,
    queryables: Mutex<HashMap<u64, Queryable<()>>>,
    /// open queries, by id; shared with the queryable callbacks that add them
    queries: Arc<Mutex<HashMap<u64, (Query, Instant)>>>,
    tokens: Mutex<HashMap<u64, LivelinessToken>>,
    liveliness_subscribers: Mutex<HashMap<u64, Subscriber<()>>>,
    matching: Mutex<HashMap<u64, MatchingEntity>>,
}

/// What a matching listener hangs off; dropping it ends the listener.
#[allow(dead_code)]
enum MatchingEntity {
    Publisher(Publisher<'static>, MatchingListener<()>),
    Querier(Querier<'static>, MatchingListener<()>),
}

impl ApiState {
    fn next(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Finalizes queries the frontend forgot (dropping a `Query` sends the final reply).
    /// Undeclares everything (the frontend is gone).
    pub(crate) fn clear(&self) {
        self.queryables.lock().unwrap().clear();
        self.queries.lock().unwrap().clear();
        self.tokens.lock().unwrap().clear();
        self.liveliness_subscribers.lock().unwrap().clear();
        self.matching.lock().unwrap().clear();
    }

    fn reap_queries(&self) {
        self.queries.lock().unwrap().retain(|_, (_, since)| since.elapsed() < QUERY_LIFETIME);
    }
}

/// The fields any of these ops may carry (camelCase on the wire).
#[derive(Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub(crate) struct ApiRequest {
    pub id: Value,
    pub op: String,
    key: String,
    /// base64
    bytes: Option<String>,
    /// base64
    payload: Option<String>,
    encoding: Option<String>,
    /// base64
    attachment: Option<String>,
    /// zenoh priority 1 (real time) to 7 (background)
    priority: Option<u8>,
    /// "drop" | "block"
    congestion_control: Option<String>,
    express: Option<bool>,
    /// ms since the unix epoch, gateway clock
    timestamp: Option<f64>,
    parameters: Option<String>,
    /// "bestMatching" | "all" | "allComplete"
    target: Option<String>,
    /// "auto" | "none" | "monotonic" | "latest"
    consolidation: Option<String>,
    timeout_ms: Option<u64>,
    complete: Option<bool>,
    history: Option<bool>,
    /// matching: "subscribers" (a publisher's) or "queryables" (a querier's)
    matching: Option<String>,
    queryable_id: Option<u64>,
    query_id: Option<u64>,
    token_id: Option<u64>,
    sub_id: Option<u64>,
    listener_id: Option<u64>,
}

/// What an op needs from the frontend's connection.
pub(crate) struct Context<'a> {
    pub session: &'a Session,
    pub grant: &'a Grant,
    pub api: &'a ApiState,
    /// sends `{event: ...}` to the frontend
    pub events: tokio::sync::mpsc::UnboundedSender<Value>,
}

fn base64() -> base64::engine::general_purpose::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

fn decode(field: &str, value: &Option<String>) -> Result<Option<Vec<u8>>, String> {
    value.as_ref().map(|text| base64().decode(text).map_err(|error| format!("{field}: {error}"))).transpose()
}

fn encode(bytes: &ZBytes) -> String {
    base64().encode(bytes.to_bytes())
}

fn priority(value: Option<u8>) -> Result<Option<Priority>, String> {
    value.map(|value| Priority::try_from(value).map_err(|_| format!("priority must be 1-7, not {value}"))).transpose()
}

fn congestion_control(value: &Option<String>) -> Result<Option<CongestionControl>, String> {
    match value.as_deref() {
        None => Ok(None),
        Some("drop") => Ok(Some(CongestionControl::Drop)),
        Some("block") => Ok(Some(CongestionControl::Block)),
        Some(other) => Err(format!("congestionControl must be \"drop\" or \"block\", not {other:?}")),
    }
}

fn query_target(value: &Option<String>) -> Result<Option<QueryTarget>, String> {
    match value.as_deref() {
        None => Ok(None),
        Some("bestMatching") => Ok(Some(QueryTarget::BestMatching)),
        Some("all") => Ok(Some(QueryTarget::All)),
        Some("allComplete") => Ok(Some(QueryTarget::AllComplete)),
        Some(other) => Err(format!("target must be bestMatching, all or allComplete, not {other:?}")),
    }
}

fn consolidation(value: &Option<String>) -> Result<Option<ConsolidationMode>, String> {
    match value.as_deref() {
        None => Ok(None),
        Some("auto") => Ok(Some(ConsolidationMode::Auto)),
        Some("none") => Ok(Some(ConsolidationMode::None)),
        Some("monotonic") => Ok(Some(ConsolidationMode::Monotonic)),
        Some("latest") => Ok(Some(ConsolidationMode::Latest)),
        Some(other) => Err(format!("consolidation must be auto, none, monotonic or latest, not {other:?}")),
    }
}

fn timestamp(session: &Session, ms: Option<f64>) -> Option<Timestamp> {
    ms.map(|ms| Timestamp::new(NTP64::from(Duration::from_secs_f64(ms.max(0.0) / 1000.0)), session.zid().into()))
}

fn timestamp_ms(timestamp: Option<&Timestamp>) -> Option<f64> {
    timestamp.map(|timestamp| timestamp.get_time().to_duration().as_secs_f64() * 1000.0)
}

fn kind(kind: SampleKind) -> &'static str {
    match kind {
        SampleKind::Put => "put",
        SampleKind::Delete => "delete",
    }
}

fn sample_json(sample: &Sample) -> Value {
    let mut value = json!({
        "key": sample.key_expr().as_str(),
        "bytes": encode(sample.payload()),
        "encoding": sample.encoding().to_string(),
        "kind": kind(sample.kind()),
    });
    if let Some(attachment) = sample.attachment() {
        value["attachment"] = json!(encode(attachment));
    }
    if let Some(ms) = timestamp_ms(sample.timestamp()) {
        value["timestamp"] = json!(ms);
    }
    value
}

fn reply_json(reply: &Reply) -> Value {
    match reply.result() {
        Ok(sample) => sample_json(sample),
        Err(error) => {
            json!({"error": encode(error.payload()), "encoding": error.encoding().to_string()})
        }
    }
}

fn ok(id: &Value, extra: Value) -> Value {
    let mut response = json!({"id": id, "ok": true});
    if let (Some(response), Value::Object(extra)) = (response.as_object_mut(), extra) {
        response.extend(extra);
    }
    response
}

fn fail(id: &Value, error: impl std::fmt::Display) -> Value {
    json!({"id": id, "ok": false, "error": error.to_string()})
}

fn now_ms() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|elapsed| elapsed.as_secs_f64() * 1000.0).unwrap_or_default()
}

/// The `key` part of a selector (`a/b?x=1` -> `a/b`).
fn key_of(selector: &str) -> &str {
    selector.split('?').next().unwrap_or_default()
}

/// Answers one op from [`OPS`].
pub(crate) async fn handle(context: Context<'_>, request: ApiRequest) -> Value {
    let id = request.id.clone();
    match run(context, request).await {
        Ok(extra) => ok(&id, extra),
        Err(error) => fail(&id, error),
    }
}

async fn run(context: Context<'_>, request: ApiRequest) -> Result<Value, String> {
    let Context { session, grant, api, events } = context;
    api.reap_queries();
    let error = |error: zenoh::Error| error.to_string();
    match request.op.as_str() {
        "put" => {
            Grant::check(&grant.publish, "publish", &request.key)?;
            let mut builder = session.put(&request.key, decode("bytes", &request.bytes)?.unwrap_or_default());
            if let Some(encoding) = &request.encoding {
                builder = builder.encoding(Encoding::from(encoding.as_str()));
            }
            if let Some(attachment) = decode("attachment", &request.attachment)? {
                builder = builder.attachment(attachment);
            }
            if let Some(priority) = priority(request.priority)? {
                builder = builder.priority(priority);
            }
            if let Some(congestion_control) = congestion_control(&request.congestion_control)? {
                builder = builder.congestion_control(congestion_control);
            }
            builder.express(request.express.unwrap_or(false)).timestamp(timestamp(session, request.timestamp)).await.map_err(error)?;
            Ok(json!({}))
        }
        "delete" => {
            Grant::check(&grant.publish, "publish", &request.key)?;
            let mut builder = session.delete(&request.key);
            if let Some(attachment) = decode("attachment", &request.attachment)? {
                builder = builder.attachment(attachment);
            }
            if let Some(priority) = priority(request.priority)? {
                builder = builder.priority(priority);
            }
            if let Some(congestion_control) = congestion_control(&request.congestion_control)? {
                builder = builder.congestion_control(congestion_control);
            }
            builder.express(request.express.unwrap_or(false)).timestamp(timestamp(session, request.timestamp)).await.map_err(error)?;
            Ok(json!({}))
        }
        "get" => {
            Grant::check(&grant.query, "query", key_of(&request.key))?;
            let selector = match &request.parameters {
                Some(parameters) if !parameters.is_empty() => {
                    format!("{}?{}", key_of(&request.key), parameters)
                }
                _ => request.key.clone(),
            };
            let timeout = Duration::from_millis(request.timeout_ms.unwrap_or(DEFAULT_GET_TIMEOUT_MS));
            let mut builder = session.get(selector.as_str()).timeout(timeout);
            if let Some(payload) = decode("payload", &request.payload)? {
                builder = builder.payload(payload);
            }
            if let Some(encoding) = &request.encoding {
                builder = builder.encoding(Encoding::from(encoding.as_str()));
            }
            if let Some(attachment) = decode("attachment", &request.attachment)? {
                builder = builder.attachment(attachment);
            }
            if let Some(target) = query_target(&request.target)? {
                builder = builder.target(target);
            }
            if let Some(consolidation) = consolidation(&request.consolidation)? {
                builder = builder.consolidation(consolidation);
            }
            if let Some(priority) = priority(request.priority)? {
                builder = builder.priority(priority);
            }
            if let Some(congestion_control) = congestion_control(&request.congestion_control)? {
                builder = builder.congestion_control(congestion_control);
            }
            let replies = builder.express(request.express.unwrap_or(false)).await.map_err(error)?;
            let mut results = Vec::new();
            while let Ok(reply) = replies.recv_async().await {
                results.push(reply_json(&reply));
            }
            Ok(json!({"replies": results}))
        }
        "declareQueryable" => {
            Grant::check(&grant.queryable, "declare a queryable on", &request.key)?;
            let queryable_id = api.next();
            let (queries, next_query) = (api.queries.clone(), api.next_id.clone());
            let queryable = session
                .declare_queryable(&request.key)
                .complete(request.complete.unwrap_or(false))
                .callback(move |query: Query| {
                    let query_id = next_query.fetch_add(1, Ordering::Relaxed) + 1;
                    let mut event = json!({
                        "event": "query",
                        "queryableId": queryable_id,
                        "queryId": query_id,
                        "key": query.key_expr().as_str(),
                        "parameters": query.parameters().as_str(),
                    });
                    if let Some(payload) = query.payload() {
                        event["payload"] = json!(encode(payload));
                    }
                    if let Some(encoding) = query.encoding() {
                        event["encoding"] = json!(encoding.to_string());
                    }
                    if let Some(attachment) = query.attachment() {
                        event["attachment"] = json!(encode(attachment));
                    }
                    queries.lock().unwrap().insert(query_id, (query, Instant::now()));
                    let _ = events.send(event);
                })
                .await
                .map_err(error)?;
            api.queryables.lock().unwrap().insert(queryable_id, queryable);
            Ok(json!({"queryableId": queryable_id}))
        }
        "undeclareQueryable" => {
            let queryable = api.queryables.lock().unwrap().remove(&request.queryable_id.unwrap_or_default()).ok_or("no such queryable")?;
            queryable.undeclare().await.map_err(error)?;
            Ok(json!({}))
        }
        "reply" | "replyErr" | "replyDel" => {
            let query_id = request.query_id.ok_or("queryId missing")?;
            // a reply goes out while the query stays open for more (finalizeQuery ends it)
            let query = api.queries.lock().unwrap().get(&query_id).map(|(query, _)| query.clone()).ok_or("no such query (finalized or expired)")?;
            match request.op.as_str() {
                "reply" => {
                    let key = if request.key.is_empty() { query.key_expr().to_string() } else { request.key.clone() };
                    let mut builder = query.reply(key, decode("bytes", &request.bytes)?.unwrap_or_default());
                    if let Some(encoding) = &request.encoding {
                        builder = builder.encoding(Encoding::from(encoding.as_str()));
                    }
                    if let Some(attachment) = decode("attachment", &request.attachment)? {
                        builder = builder.attachment(attachment);
                    }
                    builder.timestamp(timestamp(session, request.timestamp)).await.map_err(error)?;
                }
                "replyErr" => {
                    let mut builder = query.reply_err(decode("bytes", &request.bytes)?.unwrap_or_default());
                    if let Some(encoding) = &request.encoding {
                        builder = builder.encoding(Encoding::from(encoding.as_str()));
                    }
                    builder.await.map_err(error)?;
                }
                _ => {
                    let key = if request.key.is_empty() { query.key_expr().to_string() } else { request.key.clone() };
                    let mut builder = query.reply_del(key);
                    if let Some(attachment) = decode("attachment", &request.attachment)? {
                        builder = builder.attachment(attachment);
                    }
                    builder.timestamp(timestamp(session, request.timestamp)).await.map_err(error)?;
                }
            }
            Ok(json!({}))
        }
        "finalizeQuery" => {
            // dropping the last copy of a Query sends its final reply
            api.queries.lock().unwrap().remove(&request.query_id.unwrap_or_default());
            Ok(json!({}))
        }
        "declareToken" => {
            Grant::check(&grant.liveliness, "declare a liveliness token on", &request.key)?;
            let token = session.liveliness().declare_token(&request.key).await.map_err(error)?;
            let token_id = api.next();
            api.tokens.lock().unwrap().insert(token_id, token);
            Ok(json!({"tokenId": token_id}))
        }
        "undeclareToken" => {
            let token = api.tokens.lock().unwrap().remove(&request.token_id.unwrap_or_default()).ok_or("no such token")?;
            token.undeclare().await.map_err(error)?;
            Ok(json!({}))
        }
        "livelinessSubscribe" => {
            Grant::check(&grant.liveliness, "watch liveliness of", &request.key)?;
            let sub_id = api.next();
            let subscriber = session
                .liveliness()
                .declare_subscriber(&request.key)
                .history(request.history.unwrap_or(false))
                .callback(move |sample: Sample| {
                    let _ = events
                        .send(json!({"event": "liveliness", "subId": sub_id, "key": sample.key_expr().as_str(), "kind": kind(sample.kind()), "t": now_ms()}));
                })
                .await
                .map_err(error)?;
            api.liveliness_subscribers.lock().unwrap().insert(sub_id, subscriber);
            Ok(json!({"subId": sub_id}))
        }
        "livelinessUnsubscribe" => {
            let subscriber = api.liveliness_subscribers.lock().unwrap().remove(&request.sub_id.unwrap_or_default()).ok_or("no such liveliness subscriber")?;
            subscriber.undeclare().await.map_err(error)?;
            Ok(json!({}))
        }
        "livelinessGet" => {
            Grant::check(&grant.liveliness, "query liveliness of", &request.key)?;
            let timeout = Duration::from_millis(request.timeout_ms.unwrap_or(DEFAULT_GET_TIMEOUT_MS));
            let replies = session.liveliness().get(&request.key).timeout(timeout).await.map_err(error)?;
            let mut tokens = Vec::new();
            while let Ok(reply) = replies.recv_async().await {
                if let Ok(sample) = reply.result() {
                    tokens.push(sample.key_expr().as_str().to_owned());
                }
            }
            Ok(json!({"tokens": tokens}))
        }
        "matchingStatus" | "declareMatchingListener" => {
            let queryables = match request.matching.as_deref().unwrap_or("subscribers") {
                "subscribers" => false,
                "queryables" => true,
                other => {
                    return Err(format!("matching must be \"subscribers\" or \"queryables\", not {other:?}"));
                }
            };
            if queryables {
                Grant::check(&grant.query, "query", &request.key)?;
            } else {
                Grant::check(&grant.publish, "publish", &request.key)?;
            }
            let key = request.key.clone();
            if request.op == "matchingStatus" {
                let matching = if queryables {
                    let querier = session.declare_querier(key).await.map_err(error)?;
                    querier.matching_status().await.map_err(error)?.matching()
                } else {
                    let publisher = session.declare_publisher(key).await.map_err(error)?;
                    publisher.matching_status().await.map_err(error)?.matching()
                };
                return Ok(json!({"matching": matching}));
            }
            let listener_id = api.next();
            let on_change = move |status: zenoh::matching::MatchingStatus| {
                let _ = events.send(json!({"event": "matching", "listenerId": listener_id, "matching": status.matching()}));
            };
            let entity = if queryables {
                let querier = session.declare_querier(key).wait().map_err(error)?;
                let listener = querier.matching_listener().callback(on_change).wait().map_err(error)?;
                MatchingEntity::Querier(querier, listener)
            } else {
                let publisher = session.declare_publisher(key).wait().map_err(error)?;
                let listener = publisher.matching_listener().callback(on_change).wait().map_err(error)?;
                MatchingEntity::Publisher(publisher, listener)
            };
            api.matching.lock().unwrap().insert(listener_id, entity);
            Ok(json!({"listenerId": listener_id}))
        }
        "undeclareMatchingListener" => {
            api.matching.lock().unwrap().remove(&request.listener_id.unwrap_or_default()).ok_or("no such matching listener")?;
            Ok(json!({}))
        }
        "info" => {
            let info = session.info();
            let routers: Vec<String> = info.routers_zid().await.map(|zid| zid.to_string()).collect();
            let peers: Vec<String> = info.peers_zid().await.map(|zid| zid.to_string()).collect();
            Ok(json!({"zid": info.zid().await.to_string(), "routers": routers, "peers": peers}))
        }
        other => Err(format!("unknown op {other}")),
    }
}
