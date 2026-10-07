//! The rest of zenoh's API through the gateway (SPEC "The rest of the zenoh API"), each checked
//! against a plain zenoh session on the other side.

use std::time::Duration;
use tokio::time::timeout;
use zenoh_gateway::client::{
    Client, ClientOptions, CongestionControl, ConsolidationMode, GetOptions, MatchingTarget, Message, PutOptions, QueryTarget, SubscribeOptions,
};
use zenoh_gateway::{Grant, RunningServer, Server, zenoh};

fn isolated_config() -> zenoh::Config {
    let mut config = zenoh::Config::default();
    config.insert_json5("scouting/multicast/enabled", "false").unwrap();
    config.insert_json5("listen/endpoints", "[]").unwrap();
    config
}

async fn start(grant: Option<Grant>) -> (RunningServer, zenoh::Session, String) {
    let session = zenoh::open(isolated_config()).await.unwrap();
    let mut builder = Server::builder().session(session.clone());
    if let Some(grant) = grant {
        builder = builder.authorize(move |_token, _headers| Ok(grant.clone()));
    }
    let running = builder.build().await.unwrap().bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", running.local_addr());
    (running, session, url)
}

const WAIT: Duration = Duration::from_secs(5);

#[tokio::test(flavor = "multi_thread")]
async fn put_and_delete_with_options() {
    let (running, session, url) = start(None).await;
    let subscriber = session.declare_subscriber("api/put/**").await.unwrap();
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    let options = PutOptions {
        encoding: Some("text/plain".into()),
        attachment: Some(b"meta".to_vec()),
        priority: Some(2),
        congestion_control: Some(CongestionControl::Block),
        express: true,
        timestamp_ms: Some(1_700_000_000_000.0),
    };
    client.put("api/put/a", b"hello", options).await.unwrap();
    let sample = timeout(WAIT, subscriber.recv_async()).await.unwrap().unwrap();
    assert_eq!(sample.payload().to_bytes().as_ref(), b"hello");
    assert_eq!(sample.encoding().to_string(), "text/plain");
    assert_eq!(sample.attachment().unwrap().to_bytes().as_ref(), b"meta");
    assert_eq!(sample.priority() as u8, 2);
    assert_eq!(sample.congestion_control(), zenoh::qos::CongestionControl::Block);
    assert!(sample.express());
    let seconds = sample.timestamp().unwrap().get_time().to_duration().as_secs_f64();
    assert!((seconds - 1_700_000_000.0).abs() < 0.01, "timestamp {seconds}");

    client.delete("api/put/a", PutOptions { attachment: Some(b"gone".to_vec()), ..Default::default() }).await.unwrap();
    let sample = timeout(WAIT, subscriber.recv_async()).await.unwrap().unwrap();
    assert_eq!(sample.kind(), zenoh::sample::SampleKind::Delete);
    assert_eq!(sample.attachment().unwrap().to_bytes().as_ref(), b"gone");

    let publisher = client.publish("api/put/b", Default::default()).await.unwrap();
    publisher.delete(PutOptions::default()).await.unwrap();
    let sample = timeout(WAIT, subscriber.recv_async()).await.unwrap().unwrap();
    assert_eq!((sample.key_expr().as_str(), sample.kind()), ("api/put/b", zenoh::sample::SampleKind::Delete));
    client.close().await;
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn get_with_options_and_rich_replies() {
    let (running, session, url) = start(None).await;
    let _queryable = session
        .declare_queryable("api/get/**")
        .callback(|query| {
            tokio::spawn(async move {
                // echo what arrived, then an error, then a delete
                let echo = format!(
                    "{}|{}|{}|{}",
                    query.parameters(),
                    query.payload().map(|payload| String::from_utf8_lossy(&payload.to_bytes()).into_owned()).unwrap_or_default(),
                    query.encoding().map(|encoding| encoding.to_string()).unwrap_or_default(),
                    query.attachment().map(|attachment| String::from_utf8_lossy(&attachment.to_bytes()).into_owned()).unwrap_or_default(),
                );
                query.reply("api/get/one", echo).encoding("text/plain").attachment("reply-meta").await.unwrap();
                query.reply_err("nope").encoding("text/plain").await.unwrap();
                query.reply_del("api/get/two").await.unwrap();
            });
        })
        .await
        .unwrap();
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    let options = GetOptions {
        parameters: Some("a=1".into()),
        payload: Some(b"question".to_vec()),
        encoding: Some("text/plain".into()),
        attachment: Some(b"ask-meta".to_vec()),
        target: Some(QueryTarget::All),
        consolidation: Some(ConsolidationMode::None),
        timeout: Some(Duration::from_secs(2)),
        ..Default::default()
    };
    let replies = client.get_with("api/get/*", options).await.unwrap();
    assert_eq!(replies.len(), 3, "{replies:?}");
    let ok = replies.iter().find(|reply| reply.key.as_deref() == Some("api/get/one")).unwrap();
    assert_eq!(String::from_utf8_lossy(&ok.bytes), "a=1|question|text/plain|ask-meta");
    assert_eq!((ok.encoding.as_deref(), ok.attachment.as_deref()), (Some("text/plain"), Some(&b"reply-meta"[..])));
    let error = replies.iter().find(|reply| reply.error).unwrap();
    assert_eq!((error.bytes.as_slice(), error.encoding.as_deref()), (&b"nope"[..], Some("text/plain")));
    let deleted = replies.iter().find(|reply| reply.delete).unwrap();
    assert_eq!(deleted.key.as_deref(), Some("api/get/two"));

    // a querier keeps its options; per-call parameters go on top
    let querier =
        client.querier("api/get/*", GetOptions { consolidation: Some(ConsolidationMode::None), timeout: Some(Duration::from_secs(2)), ..Default::default() });
    let replies = querier.get_with(Some("b=2"), None, None).await.unwrap();
    assert!(replies.iter().any(|reply| reply.bytes.starts_with(b"b=2|")), "{replies:?}");
    assert!(querier.matching_status().await.unwrap(), "a queryable answers api/get/*");
    client.close().await;
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_queryable_answers_a_zenoh_get() {
    let (running, session, url) = start(None).await;
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    let mut queryable = client.declare_queryable("api/served/**", false).await.unwrap();
    let answering = tokio::spawn(async move {
        let query = timeout(WAIT, queryable.recv()).await.unwrap().unwrap();
        assert_eq!((query.key.as_str(), query.parameters.as_str()), ("api/served/*", "n=3"));
        assert_eq!(
            (query.payload.as_deref(), query.encoding.as_deref(), query.attachment.as_deref()),
            (Some(&b"in"[..]), Some("text/plain"), Some(&b"att"[..]))
        );
        query.reply(Some("api/served/x"), b"first", Some("text/plain"), Some(b"r-att")).await.unwrap();
        query.reply(Some("api/served/other"), b"second", None, None).await.unwrap();
        query.reply_err(b"err", None).await.unwrap();
        query.reply_del(Some("api/served/old")).await.unwrap();
        query.finalize().await.unwrap();
        queryable
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let replies = session
        .get("api/served/*?n=3")
        .payload("in")
        .encoding("text/plain")
        .attachment("att")
        .consolidation(zenoh::query::ConsolidationMode::None)
        .timeout(Duration::from_secs(5))
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let mut seen = Vec::new();
    while let Ok(reply) = replies.recv_async().await {
        seen.push(match reply.result() {
            Ok(sample) if sample.kind() == zenoh::sample::SampleKind::Delete => {
                format!("del {}", sample.key_expr())
            }
            Ok(sample) => format!("{} {} {}", sample.key_expr(), String::from_utf8_lossy(&sample.payload().to_bytes()), sample.encoding()),
            Err(error) => format!("err {}", String::from_utf8_lossy(&error.payload().to_bytes())),
        });
    }
    assert_eq!(seen, ["api/served/x first text/plain", "api/served/other second zenoh/bytes", "err err", "del api/served/old"]);
    assert!(started.elapsed() < Duration::from_secs(4), "finalize ended the get before its timeout");
    let queryable = answering.await.unwrap();
    queryable.undeclare().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let replies = session.get("api/served/x").timeout(Duration::from_millis(500)).await.unwrap();
    assert!(replies.recv_async().await.is_err(), "no queryable after undeclare");
    client.close().await;
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn liveliness_both_ways() {
    let (running, session, url) = start(None).await;
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    // the client's token, seen from zenoh
    let watcher = session.liveliness().declare_subscriber("api/alive/browser").await.unwrap();
    let token = client.declare_token("api/alive/browser").await.unwrap();
    let sample = timeout(WAIT, watcher.recv_async()).await.unwrap().unwrap();
    assert_eq!((sample.key_expr().as_str(), sample.kind()), ("api/alive/browser", zenoh::sample::SampleKind::Put));
    // zenoh's token, seen from the client: history, then a new one, then gone
    let robot = session.liveliness().declare_token("api/alive/robot").await.unwrap();
    let mut subscriber = client.liveliness_subscribe("api/alive/robot", true).await.unwrap();
    let change = timeout(WAIT, subscriber.recv()).await.unwrap().unwrap();
    assert_eq!((change.key.as_str(), change.alive), ("api/alive/robot", true));
    let mut tokens = client.liveliness_get("api/alive/**", Duration::from_secs(1)).await.unwrap();
    tokens.sort();
    assert_eq!(tokens, ["api/alive/browser", "api/alive/robot"]);
    robot.undeclare().await.unwrap();
    let change = timeout(WAIT, subscriber.recv()).await.unwrap().unwrap();
    assert_eq!((change.key.as_str(), change.alive), ("api/alive/robot", false));
    subscriber.undeclare().await.unwrap();
    token.undeclare().await.unwrap();
    let sample = timeout(WAIT, watcher.recv_async()).await.unwrap().unwrap();
    assert_eq!(sample.kind(), zenoh::sample::SampleKind::Delete, "undeclaring the client's token");
    client.close().await;
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn matching_status_and_listeners() {
    let (running, session, url) = start(None).await;
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    assert!(!client.matching_status("api/match/k", MatchingTarget::Subscribers).await.unwrap());
    let mut listener = client.matching_listener("api/match/k", MatchingTarget::Subscribers).await.unwrap();
    let subscriber = session.declare_subscriber("api/match/**").await.unwrap();
    assert_eq!(timeout(WAIT, listener.recv()).await.unwrap(), Some(true));
    assert!(client.matching_status("api/match/k", MatchingTarget::Subscribers).await.unwrap());
    subscriber.undeclare().await.unwrap();
    assert_eq!(timeout(WAIT, listener.recv()).await.unwrap(), Some(false));
    listener.undeclare().await.unwrap();

    assert!(!client.matching_status("api/match/q", MatchingTarget::Queryables).await.unwrap());
    let _queryable = session.declare_queryable("api/match/q").await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(client.matching_status("api/match/q", MatchingTarget::Queryables).await.unwrap());
    client.close().await;
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn session_info_is_the_gateways() {
    let (running, session, url) = start(None).await;
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    let info = client.info().await.unwrap();
    assert_eq!(info.zid, session.info().zid().await.to_string());
    assert!(info.routers.is_empty() && info.peers.is_empty(), "an isolated session: {info:?}");
    client.close().await;
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn subscription_messages_carry_kind_encoding_and_attachment() {
    let (running, session, url) = start(None).await;
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    let mut subscription =
        client.subscribe("api/meta/**", SubscribeOptions { delivery: Some(zenoh_gateway::client::Delivery::Reliable), ..Default::default() }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    session.put("api/meta/a", "plain").await.unwrap();
    session.put("api/meta/a", "{\"x\":1}").encoding("application/json").attachment("att").await.unwrap();
    session.delete("api/meta/a").await.unwrap();
    let mut messages = Vec::new();
    while messages.len() < 3 {
        if let Message::Data(message) = timeout(WAIT, subscription.recv()).await.unwrap().unwrap() {
            messages.push(message);
        }
    }
    assert_eq!(
        (messages[0].bytes.as_slice(), messages[0].encoding.as_deref(), messages[0].attachment.as_deref(), messages[0].delete),
        (&b"plain"[..], None, None, false)
    );
    assert_eq!(
        (messages[1].bytes.as_slice(), messages[1].encoding.as_deref(), messages[1].attachment.as_deref()),
        (&b"{\"x\":1}"[..], Some("application/json"), Some(&b"att"[..]))
    );
    assert!(messages[2].delete && messages[2].bytes.is_empty());
    client.close().await;
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn grants_cover_the_new_capabilities() {
    let read_only = Grant { subscribe: vec!["**".into()], ..Default::default() };
    let (running, session, url) = start(Some(read_only)).await;
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    let denied = |result: anyhow::Result<()>, what: &str| {
        let error = result.expect_err(what).to_string();
        assert!(error.contains("not authorized"), "{what}: {error}");
    };
    denied(client.put("api/denied/a", b"x", Default::default()).await, "put");
    denied(client.delete("api/denied/a", Default::default()).await, "delete");
    denied(client.get_with("api/denied/a", Default::default()).await.map(drop), "get");
    denied(client.declare_queryable("api/denied/a", false).await.map(drop), "queryable");
    denied(client.declare_token("api/denied/a").await.map(drop), "liveliness token");
    denied(client.liveliness_subscribe("api/denied/a", false).await.map(drop), "liveliness subscribe");
    denied(client.liveliness_get("api/denied/a", Duration::from_millis(200)).await.map(drop), "liveliness get");
    denied(client.matching_status("api/denied/a", MatchingTarget::Subscribers).await.map(drop), "matching (publish)");
    denied(client.matching_status("api/denied/a", MatchingTarget::Queryables).await.map(drop), "matching (query)");
    client.close().await;
    // and a grant for exactly one key lets that key through
    running.shutdown().await.unwrap();
    let narrow = Grant { queryable: vec!["api/allowed/**".into()], liveliness: vec!["api/allowed/**".into()], ..Default::default() };
    let (running, _session2, url) = start(Some(narrow)).await;
    drop(session);
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    client.declare_queryable("api/allowed/q", false).await.unwrap();
    client.declare_token("api/allowed/t").await.unwrap();
    denied(client.declare_token("api/elsewhere/t").await.map(drop), "token outside the grant");
    client.close().await;
    running.shutdown().await.unwrap();
}
