//! A frontend on a slow, deep-buffered link (a phone over Tailscale): bulk streams must not make
//! control traffic (heartbeat replies, control answers) or each other's data wait in a growing queue.
//! A userspace UDP shaper sits between the Rust client and the server, and a signalling proxy
//! rewrites the SDP so ICE can only use it (`cargo test --features client --test backpressure`).

use serde_json::Value;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::Instant;
use zenoh_gateway::client::{Client, ClientOptions, Delivery, Message, SubscribeOptions};
use zenoh_gateway::{RunningServer, Server, zenoh};

/// One direction of the shaped link: a drop-tail queue drained at `bytes_per_sec`, then `delay`.
#[derive(Clone, Copy)]
struct Direction {
    bytes_per_sec: f64,
    queue_limit_bytes: usize,
    delay: Duration,
}

/// Sends what is pushed to it (a packet, the socket to send it from, and where to) through a rate-limited FIFO and a
/// fixed delay; returns the push end and the bytes queued.
type Packet = (Vec<u8>, Arc<UdpSocket>, SocketAddr);

fn shaped_direction(direction: Direction) -> (mpsc::UnboundedSender<Packet>, Arc<AtomicUsize>) {
    let (push, mut queue) = mpsc::unbounded_channel::<Packet>();
    let (deliver, mut delivering) = mpsc::unbounded_channel::<(Instant, Packet)>();
    let queued = Arc::new(AtomicUsize::new(0));
    let (accepted, served) = (queued.clone(), queued.clone());
    let (admit, mut admitted) = mpsc::unbounded_channel::<(Instant, Packet)>();
    // drop-tail admission
    tokio::spawn(async move {
        while let Some(packet) = queue.recv().await {
            if accepted.load(Ordering::Relaxed) + packet.0.len() > direction.queue_limit_bytes {
                continue;
            }
            accepted.fetch_add(packet.0.len(), Ordering::Relaxed);
            let _ = admit.send((Instant::now(), packet));
        }
    });
    // the bottleneck: each packet leaves when the one before it has, plus its own serialization time
    tokio::spawn(async move {
        let mut free_at = Instant::now();
        while let Some((arrived, packet)) = admitted.recv().await {
            let start = free_at.max(arrived);
            free_at = start + Duration::from_secs_f64(packet.0.len() as f64 / direction.bytes_per_sec);
            if free_at > Instant::now() {
                tokio::time::sleep_until(free_at).await;
            }
            served.fetch_sub(packet.0.len(), Ordering::Relaxed);
            let _ = deliver.send((free_at + direction.delay, packet));
        }
    });
    tokio::spawn(async move {
        while let Some((at, (bytes, socket, to))) = delivering.recv().await {
            tokio::time::sleep_until(at).await;
            let _ = socket.send_to(&bytes, to).await;
        }
    });
    (push, queued)
}

/// client <-> [client_side | one server-side socket per client address, as a NAT maps them] <-> `target` (the server's
/// ICE port); returns the client-side port. The client checks candidate pairs from each of its sockets, so answers must
/// go back to the socket that asked.
async fn start_shaper(target: SocketAddr, down: Direction, up: Direction) -> (u16, Arc<AtomicUsize>) {
    let client_side = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let (to_client, down_queued) = shaped_direction(down);
    let (to_server, _) = shaped_direction(up);
    let port = client_side.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut mapped: std::collections::HashMap<SocketAddr, Arc<UdpSocket>> = std::collections::HashMap::new();
        let mut buffer = vec![0u8; 65536];
        while let Ok((length, from)) = client_side.recv_from(&mut buffer).await {
            let server_side = match mapped.get(&from) {
                Some(socket) => socket.clone(),
                None => {
                    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
                    mapped.insert(from, socket.clone());
                    let (socket_in, client_side, to_client) = (socket.clone(), client_side.clone(), to_client.clone());
                    tokio::spawn(async move {
                        let mut buffer = vec![0u8; 65536];
                        while let Ok((length, _)) = socket_in.recv_from(&mut buffer).await {
                            let _ = to_client.send((buffer[..length].to_vec(), client_side.clone(), from));
                        }
                    });
                    socket
                }
            };
            let _ = to_server.send((buffer[..length].to_vec(), server_side, target));
        }
    });
    (port, down_queued)
}

/// Signalling for `server_url` that routes the connection's ICE through a shaper: the offer loses its
/// candidates, the answer's are replaced by the shaper's port.
async fn start_shaped_link(server_url: String, down: Direction, up: Direction) -> (String, Arc<Mutex<Option<Arc<AtomicUsize>>>>) {
    let down_queued: Arc<Mutex<Option<Arc<AtomicUsize>>>> = Arc::default();
    let queued = down_queued.clone();
    let route = axum::routing::post(move |axum::Json(mut offer): axum::Json<Value>| {
        let (server_url, queued) = (server_url.clone(), queued.clone());
        async move {
            let without_candidates: Vec<&str> = offer["sdp"].as_str().unwrap().split("\r\n").filter(|line| !line.starts_with("a=candidate")).collect();
            offer["sdp"] = without_candidates.join("\r\n").into();
            let mut answer: Value = reqwest::Client::new().post(format!("{server_url}/offer")).json(&offer).send().await.unwrap().json().await.unwrap();
            let sdp = answer["sdp"].as_str().unwrap().to_owned();
            let loopback = sdp.split("\r\n").find(|line| line.starts_with("a=candidate") && line.contains(" 127.0.0.1 ")).expect("a loopback candidate");
            let target_port: u16 = loopback.split(' ').nth(5).unwrap().parse().unwrap();
            let (port, down_queued) = start_shaper(SocketAddr::from(([127, 0, 0, 1], target_port)), down, up).await;
            *queued.lock().unwrap() = Some(down_queued);
            let mut replaced = false;
            let lines: Vec<String> = sdp
                .split("\r\n")
                .filter_map(|line| {
                    if !line.starts_with("a=candidate") {
                        return Some(line.to_owned());
                    }
                    (!std::mem::replace(&mut replaced, true)).then(|| format!("a=candidate:1 1 udp 2130706431 127.0.0.1 {port} typ host"))
                })
                .collect();
            answer["sdp"] = lines.join("\r\n").into();
            axum::Json(answer)
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, axum::Router::new().route("/offer", route)).await.unwrap() });
    (url, down_queued)
}

fn isolated_config() -> zenoh::Config {
    let mut config = zenoh::Config::default();
    config.insert_json5("scouting/multicast/enabled", "false").unwrap();
    config.insert_json5("listen/endpoints", "[]").unwrap();
    config
}

async fn start_server() -> (RunningServer, zenoh::Session, String) {
    let session = zenoh::open(isolated_config()).await.unwrap();
    let server = Server::builder().session(session.clone()).build().await.unwrap();
    let running = server.bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", running.local_addr());
    (running, session, url)
}

/// Puts `bytes` on `key` at `hz` until dropped.
fn publish(session: &zenoh::Session, key: &str, bytes: usize, hz: f64) -> tokio::task::JoinHandle<()> {
    let (session, key) = (session.clone(), key.to_owned());
    tokio::spawn(async move {
        let payload = vec![7u8; bytes];
        let mut ticker = tokio::time::interval(Duration::from_secs_f64(1.0 / hz));
        loop {
            ticker.tick().await;
            session.put(&key, payload.clone()).await.unwrap();
        }
    })
}

fn percentile(values: &[f64], q: f64) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted.get(((q * sorted.len() as f64) as usize).min(sorted.len().saturating_sub(1))).copied().unwrap_or(f64::NAN)
}

fn now_ms() -> f64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64() * 1000.0
}

/// What one run measured, per window of the run.
#[derive(Debug)]
struct Window {
    /// heartbeat round trips (what the Controller's drive hold reads)
    heartbeat_p50: f64,
    heartbeat_p95: f64,
    /// bulk messages: receive time minus the gateway's receive time of the sample
    bulk_age_p50: f64,
    bulk_age_p95: f64,
    /// the small `pose` stream's message age
    small_age_p95: f64,
    bulk_bytes_per_sec: f64,
}

/// A Controller-like page over a 1 MB/s link with a 2 s deep buffer: three point clouds (300 KB at 10 Hz) and a map
/// (1 MB at 1 Hz), all `latest`, want ~10 MB/s; a small pose stream at 20 Hz; heartbeats at 5 Hz. `consume_bytes_per_sec`
/// is how fast the page gets through bulk messages (a phone decoding clouds), None for at once.
async fn run(seconds: u64, consume_bytes_per_sec: Option<f64>) -> Vec<Window> {
    let (_running, session, server_url) = start_server().await;
    let down = Direction { bytes_per_sec: 1_000_000.0, queue_limit_bytes: 2_000_000, delay: Duration::from_millis(30) };
    let up = Direction { bytes_per_sec: 10_000_000.0, queue_limit_bytes: 2_000_000, delay: Duration::from_millis(30) };
    let (url, _queued) = start_shaped_link(server_url, down, up).await;
    let client = Client::connect(&url, ClientOptions { ice_servers: Some(vec![]), heartbeat_hz: 5.0, heartbeat_misses: 50, ..Default::default() }).await.expect("connect through the shaper");

    let started = Instant::now();
    let window_secs = 5.0;
    let windows = (seconds as f64 / window_secs).ceil() as usize;
    let window_of = move |at: Instant| ((at - started).as_secs_f64() / window_secs) as usize;
    type Samples = Arc<Mutex<Vec<Vec<f64>>>>;
    let new_samples = || -> Samples { Arc::new(Mutex::new(vec![Vec::new(); windows + 1])) };
    let (heartbeats, bulk_ages, small_ages, bulk_bytes) = (new_samples(), new_samples(), new_samples(), new_samples());

    let mut publishers = vec![publish(&session, "robot/pose", 200, 20.0), publish(&session, "robot/map", 1_000_000, 1.0)];
    for index in 0..3 {
        publishers.push(publish(&session, &format!("robot/cloud{index}"), 300_000, 10.0));
    }
    let latest = SubscribeOptions { delivery: Some(Delivery::Latest), ..Default::default() };
    let mut readers = Vec::new();
    for key in ["robot/cloud0", "robot/cloud1", "robot/cloud2", "robot/map", "robot/pose"] {
        let mut subscription = client.subscribe(key, latest.clone()).await.unwrap();
        let small = key == "robot/pose";
        let (ages, bytes) = if small { (small_ages.clone(), bulk_bytes.clone()) } else { (bulk_ages.clone(), bulk_bytes.clone()) };
        readers.push(tokio::spawn(async move {
            while let Some(message) = subscription.recv().await {
                let Message::Data(message) = message else { continue };
                let age = now_ms() - message.timestamp_ms;
                let window = window_of(Instant::now()).min(windows);
                ages.lock().unwrap()[window].push(age);
                if !small {
                    bytes.lock().unwrap()[window].push(message.bytes.len() as f64);
                    if let Some(rate) = consume_bytes_per_sec {
                        tokio::time::sleep(Duration::from_secs_f64(message.bytes.len() as f64 / rate)).await;
                    }
                }
            }
        }));
    }
    let sampler = {
        let (client, heartbeats) = (client.clone(), heartbeats.clone());
        tokio::spawn(async move {
            let mut last = None;
            let mut ticker = tokio::time::interval(Duration::from_millis(200));
            loop {
                ticker.tick().await;
                let rtt = client.rtt_ms();
                if rtt != last && let Some(rtt) = rtt {
                    heartbeats.lock().unwrap()[window_of(Instant::now()).min(windows)].push(rtt);
                }
                last = rtt;
            }
        })
    };
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    sampler.abort();
    for task in publishers.into_iter().chain(readers) {
        task.abort();
    }
    let stats = client.stats().await.unwrap_or_default();
    eprintln!("bandwidth: {}", stats["bandwidth"]);
    client.close().await;
    let (heartbeats, bulk_ages, small_ages, bulk_bytes) = (heartbeats.lock().unwrap(), bulk_ages.lock().unwrap(), small_ages.lock().unwrap(), bulk_bytes.lock().unwrap());
    (0..windows)
        .map(|window| Window {
            heartbeat_p50: percentile(&heartbeats[window], 0.5),
            heartbeat_p95: percentile(&heartbeats[window], 0.95),
            bulk_age_p50: percentile(&bulk_ages[window], 0.5),
            bulk_age_p95: percentile(&bulk_ages[window], 0.95),
            small_age_p95: percentile(&small_ages[window], 0.95),
            bulk_bytes_per_sec: bulk_bytes[window].iter().sum::<f64>() / window_secs,
        })
        .collect()
}

fn report(name: &str, windows: &[Window]) {
    eprintln!("{name}:");
    for (index, window) in windows.iter().enumerate() {
        eprintln!(
            "  {:>3}-{:<3}s heartbeat rtt p50 {:>6.0} p95 {:>6.0} ms | bulk age p50 {:>6.0} p95 {:>6.0} ms | pose age p95 {:>6.0} ms | bulk {:>5.0} KB/s",
            index * 5,
            index * 5 + 5,
            window.heartbeat_p50,
            window.heartbeat_p95,
            window.bulk_age_p50,
            window.bulk_age_p95,
            window.small_age_p95,
            window.bulk_bytes_per_sec / 1000.0
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_link_keeps_control_and_data_fresh() {
    let windows = run(20, None).await;
    report("1 MB/s link, 2 s buffer, ~10 MB/s wanted", &windows);
    let last = windows.last().unwrap();
    assert!(last.heartbeat_p95 < 500.0, "heartbeats wait behind bulk data: {last:?}");
    assert!(last.small_age_p95 < 500.0, "a small stream waits behind bulk data: {last:?}");
    assert!(last.bulk_age_p50 < 1000.0, "bulk data arrives stale: {last:?}");
    assert!(last.bulk_bytes_per_sec > 300_000.0, "the link carries too little: {last:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_slow_page_keeps_control_and_data_fresh() {
    let windows = run(20, Some(400_000.0)).await;
    report("page consumes 400 KB/s, 1 MB/s link", &windows);
    let last = windows.last().unwrap();
    assert!(last.heartbeat_p95 < 500.0, "heartbeats wait behind bulk data: {last:?}");
    assert!(last.bulk_age_p50 < 1000.0, "bulk data arrives stale: {last:?}");
}
