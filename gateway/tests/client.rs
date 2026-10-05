//! The Rust client against an in-process server (`cargo test --features client`).

#[path = "../examples/relay_sketch.rs"]
mod relay_sketch;

use anyhow::Result;
use openh264::formats::YUVSource;
use std::time::Duration;
use tokio::time::timeout;
use zenoh_web::client::{Client, ClientOptions, Delivery, Message, PublisherOptions, SubscribeOptions, VideoFrame};
use zenoh_web::{Channel, EncodeOptions, MessageEncoding, EncodingOutput, EncodingSample, Compress, DecodedFrame, Fields, RunningServer, Server, VideoImage, zenoh};

/// `[r, g, b, width u16, height u16]` → a solid picture.
struct SolidColor;

impl MessageEncoding for SolidColor {
    fn name(&self) -> &str {
        "test-solid"
    }

    fn output(&self) -> EncodingOutput {
        EncodingOutput::Video
    }

    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        let p = sample.payload;
        let (width, height) = (u16::from_le_bytes([p[3], p[4]]) as u32, u16::from_le_bytes([p[5], p[6]]) as u32);
        Ok(DecodedFrame::Video(VideoImage::rgb8(width, height, p[..3].repeat((width * height) as usize))?))
    }
}

/// bytes → `{count, data, name}`
struct ByteFields;

impl MessageEncoding for ByteFields {
    fn name(&self) -> &str {
        "test-fields"
    }

    fn output(&self) -> EncodingOutput {
        EncodingOutput::Fields
    }

    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        Ok(DecodedFrame::data(sample.payload.to_vec()))
    }

    fn encode(&self, frame: &DecodedFrame, _options: &EncodeOptions) -> Result<Vec<u8>> {
        let bytes = frame.downcast::<Vec<u8>>()?;
        Ok(Fields::new().scalar("count", bytes.len() as u32).array("data", bytes).text("name", "bytes").build())
    }
}

fn isolated_config() -> zenoh::Config {
    let mut config = zenoh::Config::default();
    config.insert_json5("scouting/multicast/enabled", "false").unwrap();
    config.insert_json5("listen/endpoints", "[]").unwrap();
    config
}

async fn start() -> (RunningServer, zenoh::Session, String) {
    let session = zenoh::open(isolated_config()).await.unwrap();
    let server = Server::builder().session(session.clone()).encoding(SolidColor).encoding(ByteFields).build().await.unwrap();
    let running = server.bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", running.local_addr());
    (running, session, url)
}

/// Puts `payload` on `key` every 50 ms until dropped (the gateway's subscription starts a moment after it accepts).
fn keep_putting(session: &zenoh::Session, key: &str, payload: Vec<u8>) -> tokio::task::JoinHandle<()> {
    let (session, key) = (session.clone(), key.to_owned());
    tokio::spawn(async move {
        loop {
            session.put(&key, payload.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
}

async fn next_video(subscription: &mut zenoh_web::client::Subscription) -> VideoFrame {
    loop {
        match timeout(Duration::from_secs(10), subscription.recv()).await.expect("a video frame").expect("subscription open") {
            Message::Video(frame) => return frame,
            _ => continue,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn list_topics_and_get() {
    let (running, session, url) = start().await;
    let _token = session.liveliness().declare_token("listed/robot/camera").await.unwrap();
    let _queryable = session.declare_queryable("answers/42").callback(|query| {
        tokio::spawn(async move { query.reply("answers/42", "forty-two").await.unwrap() });
    }).await.unwrap();
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    assert!(client.encodings().iter().any(|encoding| encoding.name == "test-solid" && encoding.output == "video"));
    let topics = client.list_topics("listed/**", Some(0)).await.unwrap();
    assert_eq!(topics.len(), 1, "{topics:?}");
    assert_eq!((topics[0].key.as_str(), topics[0].sources.as_slice()), ("listed/robot/camera", &["token".to_owned()][..]));
    let replies = client.get("answers/*", Duration::from_secs(2)).await.unwrap();
    assert_eq!((replies[0].key.as_deref(), replies[0].bytes.as_slice()), (Some("answers/42"), &b"forty-two"[..]));
    assert!(client.clock_offset_ms().unwrap().abs() < 50.0, "same machine: offset ~0");
    client.close().await;
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_subscribe_is_byte_exact_with_zstd() {
    let (running, session, url) = start().await;
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    let options = SubscribeOptions { delivery: Some(Delivery::Reliable), compress: Some(Compress::Zstd), ..Default::default() };
    let mut subscription = client.subscribe("raw/big", options).await.unwrap();
    // 300 KB, several chunks, compressible
    let payload: Vec<u8> = (0..300_000u32).map(|index| (index / 1000) as u8).collect();
    let _putter = keep_putting(&session, "raw/big", payload.clone());
    let Message::Data(message) = timeout(Duration::from_secs(10), subscription.recv()).await.unwrap().unwrap() else { panic!("not data") };
    assert_eq!(message.key, "raw/big");
    assert!(message.bytes == payload, "byte-exact after reassembly and zstd ({} bytes)", message.bytes.len());
    let stats = client.stats().await.unwrap();
    let sent: f64 = stats["channels"].as_array().unwrap().iter().map(|channel| channel["stats"]["bytesSent"].as_f64().unwrap_or_default()).sum();
    assert!(sent > 0.0 && sent < 100_000.0, "zstd on the wire: {sent} bytes sent");
    client.close().await;
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn fields_subscribe_is_parsed() {
    let (running, session, url) = start().await;
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    let mut subscription = client.subscribe("fields/x", SubscribeOptions { encoding: Some("test-fields".into()), ..Default::default() }).await.unwrap();
    let _putter = keep_putting(&session, "fields/x", vec![3, 1, 4]);
    let Message::Data(message) = timeout(Duration::from_secs(10), subscription.recv()).await.unwrap().unwrap() else { panic!("not data") };
    let fields = message.fields.expect("fields");
    assert_eq!(fields["count"].values(), [3.0]);
    assert_eq!(fields["data"].values(), [3.0, 1.0, 4.0]);
    assert_eq!(fields["name"].text(), Some("bytes"));
    let refused = client.subscribe("fields/x", SubscribeOptions { max_hz: Some(-1.0), ..Default::default() }).await;
    assert!(refused.err().unwrap().to_string().contains("maxHz"), "the gateway's reason");
    client.close().await;
    running.shutdown().await.unwrap();
}

/// Decodes H.264 access units in order, returning the last picture's size and mean RGB.
fn decode_all(frames: &[VideoFrame]) -> ((usize, usize), [f64; 3]) {
    let mut decoder = openh264::decoder::Decoder::new().unwrap();
    let mut last = None;
    for frame in frames {
        if let Some(picture) = decoder.decode(&frame.data).unwrap() {
            let (width, height) = picture.dimensions();
            let mut rgb = vec![0u8; width * height * 3];
            picture.write_rgb8(&mut rgb);
            let mut mean = [0.0; 3];
            for pixel in rgb.as_chunks::<3>().0 {
                for (sum, value) in mean.iter_mut().zip(pixel) {
                    *sum += *value as f64 / (width * height) as f64;
                }
            }
            last = Some(((width, height), mean));
        }
    }
    last.expect("a decoded picture")
}

fn solid(rgb: [u8; 3], width: u16, height: u16) -> Vec<u8> {
    [&rgb[..], &width.to_le_bytes(), &height.to_le_bytes()].concat()
}

fn assert_color(mean: [f64; 3], expected: [u8; 3]) {
    assert!(mean.iter().zip(expected).all(|(got, want)| (got - want as f64).abs() < 12.0), "mean {mean:?}, expected {expected:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn video_arrives_as_h264_access_units_and_answers_keyframe_requests() {
    let (running, session, url) = start().await;
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    let options = SubscribeOptions { encoding: Some("test-solid".into()), min_quality: Some(1.0), ..Default::default() };
    let mut camera = client.subscribe("camera/front", options).await.unwrap();
    let _putter = keep_putting(&session, "camera/front", solid([200, 40, 90], 320, 240));
    let mut frames = vec![next_video(&mut camera).await];
    assert!(frames[0].keyframe, "a stream starts at a keyframe");
    assert_eq!(frames[0].format, zenoh_web::VideoFormat::H264);
    while frames.len() < 5 {
        frames.push(next_video(&mut camera).await);
    }
    let (size, mean) = decode_all(&frames);
    assert_eq!(size, (320, 240));
    assert_color(mean, [200, 40, 90]);
    // the gateway's own keyframes come every 3 s; one asked for comes at once
    while next_video(&mut camera).await.keyframe {}
    camera.request_keyframe().await.unwrap();
    let asked = std::time::Instant::now();
    while !next_video(&mut camera).await.keyframe {}
    assert!(asked.elapsed() < Duration::from_secs(1), "keyframe after {:?}", asked.elapsed());
    let stats = client.stats().await.unwrap();
    assert!(stats["channels"][0]["stats"]["keyframeRequests"].as_u64().unwrap() >= 1, "{}", stats["channels"][0]["stats"]);
    // a second video subscription after closing the first reuses its track
    drop(camera);
    let options = SubscribeOptions { encoding: Some("test-solid".into()), min_quality: Some(1.0), max_resolution: Some((160, 120)), ..Default::default() };
    let mut again = client.subscribe("camera/front", options).await.unwrap();
    let first = next_video(&mut again).await;
    assert!(first.keyframe);
    assert_eq!(decode_all(&[first]).0, (160, 120), "maxResolution reached the gateway");
    client.close().await;
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_arrives_in_zenoh() {
    let (running, session, url) = start().await;
    let subscriber = session.declare_subscriber("cmd/vel").await.unwrap();
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    let publisher = client.publish("cmd/vel", PublisherOptions { delivery: Some(Delivery::Reliable), latency_limit: Some(500.0), ..Default::default() }).await.unwrap();
    publisher.put(b"forward").await.unwrap();
    let sample = timeout(Duration::from_secs(5), subscriber.recv_async()).await.unwrap().unwrap();
    assert_eq!(sample.payload().to_bytes().as_ref(), b"forward");
    client.close().await;
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn deadman_fires_when_heartbeats_stop() {
    let (running, session, url) = start().await;
    let subscriber = session.declare_subscriber("cmd/stop").await.unwrap();
    let client = Client::connect(&url, ClientOptions { heartbeat_hz: 10.0, heartbeat_misses: 3, ..Default::default() }).await.unwrap();
    let publisher = client.publish("cmd/stop", PublisherOptions::default()).await.unwrap();
    publisher.set_deadman(b"halt").await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(subscriber.try_recv().unwrap().is_none(), "nothing while heartbeats flow");
    client.pause_heartbeat(true);
    let sample = timeout(Duration::from_secs(3), subscriber.recv_async()).await.unwrap().unwrap();
    assert_eq!(sample.payload().to_bytes().as_ref(), b"halt");
    assert_eq!(timeout(Duration::from_secs(2), publisher.wait_tripped()).await.unwrap(), "heartbeat");
    assert!(publisher.put(b"go").await.is_err(), "a tripped publisher refuses puts");
    client.close().await;
    running.shutdown().await.unwrap();
}

/// Frames flow A → client → decode → B → client: the relay example end to end.
#[tokio::test(flavor = "multi_thread")]
async fn relay_sketch_reserves_a_camera() {
    let (source, session, url) = start().await;
    let _putter = keep_putting(&session, "camera/rear", solid([30, 160, 220], 160, 120));
    let relay = relay_sketch::relay(&url, "camera/rear", "test-solid", "127.0.0.1:0").await.unwrap();
    let viewer = Client::connect(&format!("http://{}", relay.local_addr()), ClientOptions::default()).await.unwrap();
    let options = SubscribeOptions { encoding: Some("relay-rgb".into()), min_quality: Some(1.0), ..Default::default() };
    let mut camera = viewer.subscribe("relay/camera/rear", options).await.unwrap();
    let mut frames = Vec::new();
    while frames.len() < 3 {
        frames.push(next_video(&mut camera).await);
    }
    let (size, mean) = decode_all(&frames);
    assert_eq!(size, (160, 120));
    assert_color(mean, [30, 160, 220]);
    viewer.close().await;
    relay.shutdown().await.unwrap();
    source.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn token_and_leases() {
    let session = zenoh::open(isolated_config()).await.unwrap();
    let server = Server::builder()
        .session(session.clone())
        .authorize(|token, _headers| if token == Some("good") { Ok(zenoh_web::Grant::all()) } else { Err("unknown token".into()) })
        .lease_group("drive", ["cmd/**"])
        .build()
        .await
        .unwrap();
    let running = server.bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", running.local_addr());
    let refused = Client::connect(&url, ClientOptions { token: Some("bad".into()), ..Default::default() }).await;
    assert!(refused.err().unwrap().to_string().contains("refused the token"));
    let options = || ClientOptions { token: Some("good".into()), heartbeat_hz: 10.0, ..Default::default() };
    let (driver, other) = (Client::connect(&url, options()).await.unwrap(), Client::connect(&url, options()).await.unwrap());
    let lease = driver.lease("drive", None, None).await.unwrap();
    assert_eq!(lease.keys, ["cmd/**"]);
    let subscriber = session.declare_subscriber("cmd/vel").await.unwrap();
    let blocked = other.publish("cmd/vel", PublisherOptions { delivery: Some(Delivery::Reliable), ..Default::default() }).await.unwrap();
    blocked.put(b"other").await.unwrap();
    let mine = driver.publish("cmd/vel", PublisherOptions { delivery: Some(Delivery::Reliable), ..Default::default() }).await.unwrap();
    mine.put(b"driver").await.unwrap();
    let sample = timeout(Duration::from_secs(5), subscriber.recv_async()).await.unwrap().unwrap();
    assert_eq!(sample.payload().to_bytes().as_ref(), b"driver", "the other client's put was dropped");
    for _ in 0..100 {
        if blocked.blocked().is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(blocked.blocked().unwrap().contains("leased by another client"), "the gateway said why");
    lease.release().await.unwrap();
    assert_eq!(lease.lost().as_deref(), Some("released"));
    blocked.put(b"other again").await.unwrap();
    let sample = timeout(Duration::from_secs(5), subscriber.recv_async()).await.unwrap().unwrap();
    assert_eq!(sample.payload().to_bytes().as_ref(), b"other again");
    driver.close().await;
    other.close().await;
    running.shutdown().await.unwrap();
}

/// A zenoh session listening on a free local port, and that endpoint.
async fn listening_session() -> (zenoh::Session, String) {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let endpoint = format!("tcp/127.0.0.1:{port}");
    let mut config = isolated_config();
    config.insert_json5("mode", "\"router\"").unwrap();
    config.insert_json5("listen/endpoints", &format!("[\"{endpoint}\"]")).unwrap();
    (zenoh::open(config).await.unwrap(), endpoint)
}

/// The robot dials out to the relay's zenoh; the relay signals over that link (no HTTP on the robot).
#[tokio::test(flavor = "multi_thread")]
async fn signalling_over_zenoh() {
    let (relay_session, endpoint) = listening_session().await;
    let mut config = isolated_config();
    config.insert_json5("connect/endpoints", &format!("[\"{endpoint}\"]")).unwrap();
    let robot_session = zenoh::open(config).await.unwrap();
    let robot = Server::builder()
        .session(robot_session.clone())
        .zenoh_signalling("robot")
        .authorize(|token, _headers| if token == Some("good") { Ok(zenoh_web::Grant::all()) } else { Err("unknown token".into()) })
        .build()
        .await
        .unwrap();
    let options = |token: &str| ClientOptions { token: Some(token.into()), ..Default::default() };
    // the robot's link to the relay comes up in the background
    let mut refused = String::new();
    for _ in 0..50 {
        refused = Client::connect_zenoh(&relay_session, "robot", options("bad")).await.err().unwrap().to_string();
        if !refused.contains("no zenoh-web server answered") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(refused.contains("refused the token: unknown token"), "{refused}");
    let absent = Client::connect_zenoh(&relay_session, "nobody", options("good")).await;
    assert!(absent.err().unwrap().to_string().contains("no zenoh-web server answered"));
    let client = Client::connect_zenoh(&relay_session, "robot", options("good")).await.unwrap();
    let mut subscription = client.subscribe("robot/data", SubscribeOptions::default()).await.unwrap();
    let _putter = keep_putting(&robot_session, "robot/data", b"over webrtc".to_vec());
    let Message::Data(message) = timeout(Duration::from_secs(10), subscription.recv()).await.unwrap().unwrap() else { panic!("not data") };
    assert_eq!(message.bytes, b"over webrtc");
    assert_eq!(robot.subscriptions(), [("robot/data".to_owned(), None)]);
    client.close().await;
    robot.shutdown().await.unwrap();
}

/// `fields` under `@relay/test-prefixed`: only its subscribers see those samples, raw ones see the raw topic.
struct Prefixed;

impl MessageEncoding for Prefixed {
    fn name(&self) -> &str {
        "test-prefixed"
    }

    fn output(&self) -> EncodingOutput {
        EncodingOutput::Data
    }

    fn key_prefix(&self) -> Option<&str> {
        Some("@relay/test-prefixed")
    }

    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        Ok(DecodedFrame::data(sample.payload.to_vec()))
    }

    fn encode(&self, frame: &DecodedFrame, _options: &EncodeOptions) -> Result<Vec<u8>> {
        Ok(frame.downcast::<Vec<u8>>()?.clone())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn key_prefix_subscriptions_and_lease_hooks() {
    let session = zenoh::open(isolated_config()).await.unwrap();
    let server = Server::builder().session(session.clone()).encoding(Prefixed).build().await.unwrap();
    let running = server.clone().bind("127.0.0.1:0").await.unwrap();
    let mut changes = server.changes();
    let client = Client::connect(&format!("http://{}", running.local_addr()), ClientOptions { heartbeat_hz: 5.0, ..Default::default() }).await.unwrap();
    let mut prefixed = client.subscribe("cam/*", SubscribeOptions { encoding: Some("test-prefixed".into()), ..Default::default() }).await.unwrap();
    let mut raw = client.subscribe("cam/*", SubscribeOptions::default()).await.unwrap();
    assert!(changes.has_changed().unwrap(), "opening subscriptions bumps changes");
    let mut subscriptions = server.subscriptions();
    subscriptions.sort();
    assert_eq!(subscriptions, [("cam/*".to_owned(), None), ("cam/*".to_owned(), Some("test-prefixed".to_owned()))]);
    let _raw_putter = keep_putting(&session, "cam/a", b"raw".to_vec());
    let _prefixed_putter = keep_putting(&session, "@relay/test-prefixed/cam/a", b"prefixed".to_vec());
    for _ in 0..3 {
        let Message::Data(message) = timeout(Duration::from_secs(10), prefixed.recv()).await.unwrap().unwrap() else { panic!("not data") };
        assert_eq!((message.key.as_str(), message.bytes.as_slice()), ("cam/a", &b"prefixed"[..]));
        let Message::Data(message) = timeout(Duration::from_secs(10), raw.recv()).await.unwrap().unwrap() else { panic!("not data") };
        assert_eq!((message.key.as_str(), message.bytes.as_slice()), ("cam/a", &b"raw"[..]));
    }
    changes.mark_unchanged();
    let lease = client.lease("arm", Some(vec!["arm/**".into()]), None).await.unwrap();
    assert!(changes.has_changed().unwrap(), "a lease bumps changes");
    assert_eq!(server.leases(), [("arm".to_owned(), vec!["arm/**".to_owned()])]);
    assert!(server.expire_lease("arm", "upstream refused").await);
    assert_eq!(timeout(Duration::from_secs(2), lease.wait_lost()).await.unwrap(), "upstream refused");
    assert!(server.leases().is_empty());
    drop((prefixed, raw));
    for _ in 0..100 {
        if server.subscriptions().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(server.subscriptions().is_empty(), "closed subscriptions leave the list");
    client.close().await;
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn lease_lost_right_behind_its_grant() {
    let session = zenoh::open(isolated_config()).await.unwrap();
    let server = Server::builder().session(session.clone()).build().await.unwrap();
    let running = server.clone().bind("127.0.0.1:0").await.unwrap();
    let client = Client::connect(&format!("http://{}", running.local_addr()), ClientOptions { heartbeat_hz: 5.0, ..Default::default() }).await.unwrap();
    // ends every lease the moment it's granted, so the leaseLost event lands right behind the lease reply
    let expirer = {
        let (server, mut changes) = (server.clone(), server.changes());
        tokio::spawn(async move {
            while changes.changed().await.is_ok() {
                for (group, _) in server.leases() {
                    server.expire_lease(&group, "taken back").await;
                }
            }
        })
    };
    for round in 0..30 {
        let lease = client.lease("arm", Some(vec!["arm/**".into()]), None).await.unwrap();
        let lost = timeout(Duration::from_secs(2), lease.wait_lost()).await;
        assert_eq!(lost.as_deref(), Ok("taken back"), "round {round}: the lease never learned it ended");
    }
    expirer.abort();
    client.close().await;
    running.shutdown().await.unwrap();
}

/// `GET /zenoh-web/ice` as a browser would.
async fn browser_ice_servers(url: &str, token: Option<&str>) -> Vec<zenoh_web::IceServer> {
    let mut request = reqwest::Client::new().get(format!("{url}{}", zenoh_web::ICE_PATH));
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let body: serde_json::Value = request.send().await.unwrap().json().await.unwrap();
    serde_json::from_value(body["iceServers"].clone()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn ice_hook_adds_servers_for_both_ends_and_a_failure_leaves_the_static_ones() {
    use std::sync::{Arc, Mutex};
    use zenoh_web::{IceServer, IceSide};
    let stun = IceServer { urls: vec!["stun:127.0.0.1:3478".into()], ..Default::default() };
    // the hook's TURN server is unreachable, so the connection must still work on host candidates
    let minted = IceServer { urls: vec!["turn:127.0.0.1:9?transport=udp".into()], username: "u".into(), credential: "c".into() };
    let asked = Arc::new(Mutex::new(Vec::new()));
    let (asked_in, minted_in) = (asked.clone(), minted.clone());
    let session = zenoh::open(isolated_config()).await.unwrap();
    let server = Server::builder()
        .session(session.clone())
        .ice_servers([stun.clone()])
        .ice_servers_fn(move |request| {
            asked_in.lock().unwrap().push((request.side, request.token.clone()));
            let minted = minted_in.clone();
            async move { if request.token.as_deref() == Some("broken") { anyhow::bail!("provider down") } else { Ok(vec![minted]) } }
        })
        .build()
        .await
        .unwrap();
    let running = server.bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", running.local_addr());
    assert_eq!(browser_ice_servers(&url, Some("t1")).await, vec![stun.clone(), minted.clone()]);
    assert_eq!(browser_ice_servers(&url, Some("broken")).await, vec![stun.clone()]);
    let client = Client::connect(&url, ClientOptions { token: Some("t2".into()), ..Default::default() }).await.unwrap();
    let asked = asked.lock().unwrap().clone();
    assert!(asked.contains(&(IceSide::Gateway, Some("t2".into()))) && asked.contains(&(IceSide::Browser, Some("t2".into()))), "{asked:?}");
    client.close().await;
    running.shutdown().await.unwrap();
}

/// Real Cloudflare TURN, relay-only: `CF_TURN_KEY_ID=... CF_TURN_API_TOKEN=... cargo test --features client,cloudflare
/// --test client cloudflare -- --ignored`.
#[cfg(feature = "cloudflare")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a Cloudflare TURN key (CF_TURN_KEY_ID, CF_TURN_API_TOKEN) and the internet"]
async fn cloudflare_turn_carries_a_relay_only_connection() {
    let key_id = std::env::var("CF_TURN_KEY_ID").expect("CF_TURN_KEY_ID");
    let api_token = std::env::var("CF_TURN_API_TOKEN").expect("CF_TURN_API_TOKEN");
    let turn = zenoh_web::CloudflareTurn::new(key_id, api_token).ttl(Duration::from_secs(600));
    let generated = turn.generate().await.unwrap();
    assert!(generated.iter().any(|server| server.urls.iter().any(|url| url.starts_with("turn")) && !server.credential.is_empty()), "{generated:?}");
    assert!(generated.iter().all(|server| server.urls.iter().all(|url| !url.split('?').next().unwrap().ends_with(":53"))), "port 53 dropped");
    let session = zenoh::open(isolated_config()).await.unwrap();
    let server = Server::builder().session(session.clone()).cloudflare_turn(turn).build().await.unwrap();
    let running = server.bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", running.local_addr());
    // relay only: every packet goes through Cloudflare's TURN server, with the credentials the server handed out
    let client = timeout(Duration::from_secs(30), Client::connect(&url, ClientOptions { relay_only: true, ..Default::default() })).await.unwrap().unwrap();
    let mut subscription = client.subscribe("turn/data", SubscribeOptions { delivery: Some(Delivery::Reliable), ..Default::default() }).await.unwrap();
    let _putter = keep_putting(&session, "turn/data", b"through cloudflare".to_vec());
    for _ in 0..5 {
        let Message::Data(message) = timeout(Duration::from_secs(10), subscription.recv()).await.unwrap().unwrap() else { panic!("not data") };
        assert_eq!(message.bytes, b"through cloudflare");
    }
    client.close().await;
    running.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relay_policy_in_the_ice_reply_makes_the_connection_relay_only() {
    use axum::{Json, Router, routing::get};
    // a host app answering /zenoh-web/ice itself, the rest from zenoh-web's routes; no TURN, so relay-only can't connect
    async fn host(policy: Option<&'static str>) -> String {
        let session = zenoh::open(isolated_config()).await.unwrap();
        let server = Server::builder().session(session).build().await.unwrap();
        let ice = move || async move { Json(serde_json::json!({"iceServers": [], "iceTransportPolicy": policy})) };
        let app = Router::new().route(zenoh_web::ICE_PATH, get(ice)).fallback_service(server.router());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        url
    }
    let all = host(None).await;
    let client = timeout(Duration::from_secs(15), Client::connect(&all, ClientOptions::default())).await.unwrap().unwrap();
    client.close().await;
    let relay = host(Some("relay")).await;
    let refused = timeout(Duration::from_secs(20), Client::connect(&relay, ClientOptions::default())).await;
    let error = match refused { Ok(Ok(_)) => panic!("relay-only with no TURN server must not connect"), Ok(Err(error)) => format!("{error:#}"), Err(_) => "timed out".to_owned() };
    assert!(error.contains("Relay-only"), "refused for the relay policy: {error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn video_av1_channel_arrives_as_av1_and_vp8_without_an_encoder_is_refused() {
    let (running, session, url) = start().await;
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    let av1 = SubscribeOptions { encoding: Some("test-solid".into()), channel: Some("video-av1".into()), min_quality: Some(1.0), ..Default::default() };
    let mut camera = client.subscribe("camera/front", av1).await.unwrap();
    let _putter = keep_putting(&session, "camera/front", solid([200, 40, 90], 320, 240));
    let frame = next_video(&mut camera).await;
    assert_eq!((frame.format, frame.keyframe), (zenoh_web::VideoFormat::Av1, true));
    assert!(!next_video(&mut camera).await.data.is_empty());
    let vp8 = SubscribeOptions { encoding: Some("test-solid".into()), channel: Some("video-vp8".into()), ..Default::default() };
    let refused = client.subscribe("camera/front", vp8).await.err().unwrap().to_string();
    assert!(refused.contains("no video-vp8 encoder"), "{refused}");
    client.close().await;
    running.shutdown().await.unwrap();
}

/// Data channel only: echoes `encodeOptions.prefix` and the quality it was given, as fields when `fields` is set.
struct Echo;

impl MessageEncoding for Echo {
    fn name(&self) -> &str {
        "test-echo"
    }

    fn output(&self) -> EncodingOutput {
        EncodingOutput::Data
    }

    fn output_on(&self, channel: Channel, options: &serde_json::Map<String, serde_json::Value>) -> Result<EncodingOutput, String> {
        if channel != Channel::Data || options.keys().any(|key| key != "prefix" && key != "fields") {
            return Err("test-echo: data, with prefix and fields".into());
        }
        Ok(if options.get("fields").and_then(|value| value.as_bool()) == Some(true) { EncodingOutput::Fields } else { EncodingOutput::Data })
    }

    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        Ok(DecodedFrame::data(sample.payload.to_vec()))
    }

    fn encode(&self, frame: &DecodedFrame, options: &EncodeOptions) -> Result<Vec<u8>> {
        let text = format!("{}{} q={:.2}", options.str("prefix").unwrap_or(""), String::from_utf8_lossy(frame.downcast::<Vec<u8>>()?), options.quality);
        Ok(if options.options.contains_key("fields") { Fields::new().text("text", &text).build() } else { text.into_bytes() })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn encode_options_reach_the_encoding_and_quality_caps_it() {
    let session = zenoh::open(isolated_config()).await.unwrap();
    let server = Server::builder().session(session.clone()).encoding(Echo).build().await.unwrap();
    let running = server.bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", running.local_addr());
    let client = Client::connect(&url, ClientOptions::default()).await.unwrap();
    let options = |json: serde_json::Value| SubscribeOptions { encoding: Some("test-echo".into()), encode_options: Some(serde_json::from_value(json).unwrap()), ..Default::default() };
    let mut bytes = client.subscribe("echo/a", options(serde_json::json!({"prefix": ">", "quality": 0.25}))).await.unwrap();
    let mut fields = client.subscribe("echo/a", options(serde_json::json!({"fields": true}))).await.unwrap();
    let _putter = keep_putting(&session, "echo/a", b"hi".to_vec());
    let Message::Data(message) = timeout(Duration::from_secs(5), bytes.recv()).await.unwrap().unwrap() else { panic!("not data") };
    assert_eq!((message.bytes.as_slice(), message.fields.is_none()), (&b">hi q=0.25"[..], true), "quality capped at encodeOptions.quality");
    let Message::Data(message) = timeout(Duration::from_secs(5), fields.recv()).await.unwrap().unwrap() else { panic!("not data") };
    assert_eq!(message.fields.unwrap()["text"].text(), Some("hi q=1.00"), "fields output is decoded by the client");
    let refused = client.subscribe("echo/a", options(serde_json::json!({"colour": "red"}))).await.err().unwrap().to_string();
    assert!(refused.contains("prefix and fields"), "the encoding refuses options it doesn't know: {refused}");
    client.close().await;
    running.shutdown().await.unwrap();
}
