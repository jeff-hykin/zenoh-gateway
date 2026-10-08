# zenoh-gateway

View and drive a [zenoh](https://zenoh.io) system from a browser over a squeezed network (a phone on
weak wifi), without a heavy middle layer: a Rust library that serves browsers over WebRTC, and a
dependency-free TypeScript client. Pictures arrive as video (H.264 by default, AV1 built in too, or any encoder the
application plugs in), sound as Opus audio, other data as bytes on per-stream data channels, and a per-browser bandwidth
allocator decides who gets what when the link is short. Message encodings (how to read each message type) are plugged in
by the application:

- [zenoh-gateway-cli](https://github.com/jeff-hykin/zenoh-gateway-cli): the `zenoh-gateway` command (install,
  flags, the example page and the end-to-end tests).
- [zenoh-dimos-codecs](https://github.com/jeff-hykin/zenoh-dimos-codecs): ROS 2 / dimos images,
  lossless depth and point clouds.

[docs/how-to.md](docs/how-to.md): how do I subscribe, publish, show video, get point clouds, ... from a page. [SPEC.md](SPEC.md) is the detailed contract; this README is the overview.

![the example page: topic list, H.264 video, point cloud, depth, a raw stream and live allocation stats](https://raw.githubusercontent.com/jeff-hykin/zenoh-gateway-cli/main/test/artifacts/example.png)

## Architecture

```
zenoh peers / routers (publishers you don't control: ROS 2 over rmw_zenoh, dimos, anything)
        │  zenoh (the gateway is a normal zenoh peer or client)
   zenoh-gateway server  (Rust, in your process: zenoh 1.6.2, webrtc-rs, tokio, axum)
        │  HTTP: POST /offer (signaling) + optional static files
        │  WebRTC: one SCTP data channel per subscription/publisher, + video and audio tracks
   browser page  (client/zenoh_gateway.ts, loaded from esm.sh or bundled)
```

- Each `subscribe` / `publisher` is its own data channel, so a slow stream never blocks another one.
  `delivery: "latest"` channels are unordered and unreliable (old frames are dropped, never queued);
  `"reliable"` ones are ordered and lossless.
- The gateway never parses payloads unless a subscription picks an encoding. Encodings run lazily inside the
  gateway, only for frames that will actually be sent.
- Every browser gets a bandwidth estimate and a budget; streams with a higher `bandwidthPriority` keep more, trading
  quality against rate per `qualityToHzTradeoff`. Strict-priority streams skip the queue.
- Heartbeat + deadman: a publisher can leave a "stop" message on the gateway that is published once if
  the page goes silent.

## Client API

```js
import { connect, Priority, registerEncoding } from "https://esm.sh/gh/jeff-hykin/zenoh-gateway@<commit or tag>/client/zenoh_gateway.ts"

const z = await connect("http://robot.local:7448", { heartbeatHz: 5, heartbeatMisses: 3 })
const sub = z.subscribe("camera/**", { encoding: "ros2_image", maxHz: 15 }, (msg) => {})
video.srcObject = sub.mediaStream
const cmd = z.publisher("cmd_vel", { priority: Priority.REAL_TIME, latencyLimit: 300 })
cmd.put(bytes)
await cmd.setDeadman(stopBytes)
```

esm.sh transpiles the TypeScript on the fly; pin a commit. Or bundle it (`deno bundle client/zenoh_gateway.ts`).
The gateway checks options: an unknown name or a bad value rejects the subscription or publisher (`ready()` rejects with the reason).

### `connect(url, options)` → `Promise<ZenohGateway>`

| option | default | |
|---|---|---|
| `heartbeatHz` | 0 (off) | heartbeats per second on their own channel; required for deadmen and leases |
| `heartbeatMisses` | 3 | silence of `misses / hz` seconds = this page is gone |
| `token` | none | sent as `Authorization: Bearer <token>` (see [Auth](#auth)); a refused token rejects `connect` and stops reconnecting |
| `iceServers` | the gateway's | `RTCIceServer[]`; by default fetched from the gateway (`GET /zenoh-gateway/ice`, TURN credentials minted per client), `[]` on a gateway without any |
| `iceTransportPolicy` | the `/zenoh-gateway/ice` reply's, else `"all"` | `"relay"` sends everything through TURN |
| `reconnect` | `true` | re-open the connection and every live channel after a loss |
| `statsIntervalMs` | 1000 | how often `z.stats` / `z.gatewayStats` refresh |
| `clock` | `performance.timeOrigin + performance.now()` | the page's clock in ms (put timestamps, clock sync) |

### `ZenohGateway`

| member | |
|---|---|
| `subscribe(key, options, callback)` → `Subscription` | `callback(msg)`: `{ key, kind, bytes, encoding?, attachment?, timestamp, seq, decoded?, video?, mediaStream? }` (`kind`: `"put"` / `"delete"`) |
| `publisher(key, options)` → `Publisher` | |
| `get(key, { parameters, payload, encoding, attachment, target, consolidation, timeoutMs = 5000, priority, congestionControl, express })` → `[{ key, bytes, kind, encoding, attachment?, timestamp? }` or `{ error: true, bytes, encoding }]` | zenoh query |
| `querier(key, getOptions)` → `Querier` | fixed-option queries: `querier.get({ parameters, payload })`, `querier.matchingStatus()` |
| `put(key, bytes, { encoding, attachment, priority, congestionControl, express, timestamp })`, `delete(key, options)` | one zenoh put / delete (also `publisher.delete()`) |
| `declareQueryable(key, { complete }, (query) => …)` → `Queryable` | the page answers zenoh queries: `query.reply(bytes, { key, encoding, attachment })`, `replyErr(bytes)`, `replyDel({ key })`, then `query.finalize()` |
| `declareToken(key)` → `LivelinessToken` | a liveliness token (`undeclare()`) |
| `livelinessSubscribe(key, { history }, ({ key, alive }) => …)`, `livelinessGet(key)` → `[key]` | watch / list liveliness tokens |
| `matchingStatus(key, "subscribers" \| "queryables")` → `boolean`, `matchingListener(key, target, (matching) => …)` | whether a publisher / querier on the key would reach anyone |
| `info()` → `{ zid, routers, peers }` | the gateway's zenoh session |
| `listTopics(filter = "**", { probeMs = 600 })` → `[{ key, sources }]` | live keys; `sources` ⊂ `token`, `advancedPublisher`, `sample` (SPEC "Topic enumeration"); `probeMs: 0` skips the `sample` probe, so publishers that only send while matched stay asleep |
| `encodings` | `[{ name, output }]`: every message encoding the gateway runs (`output` on its default channel: `"video"`, `"audio"`, `"fields"` or `"data"`), fetched on connect |
| `stats` | per key: `received`, `dropped`, `backlogBytes`, `rttMs`, `gateway` (normalized options, gateway counters, `allocation`) |
| `gatewayStats` | `clock`, `heartbeat`, `bandwidth` (estimate, cap, budget, demand, queue delay, …) |
| `rttMs`, `clockOffsetMs` | round trip and gateway-minus-page clock offset |
| `state`, `onState(fn)` | `"connecting"` / `"connected"` / `"degraded"` / `"lost"`; `onState` returns an unsubscribe function |
| `now()` | the page clock used for timestamps |
| `pollStats()` | refresh stats now |
| `lease(group, { keys?, maxSeconds? })` → `Lease` | exclusive publish rights on a group's keys among this gateway's clients ([Leases](#leases)) |
| `expireLease(group)` | end another client's lease (needs the grant's `forceExpire`) |
| `iceServers` | the ICE servers in use |
| `pauseHeartbeat()`, `resumeHeartbeat()` | stop/resume beats (to test deadman wiring) |
| `close()` | close everything |

### Subscribe options

| option | default | |
|---|---|---|
| `delivery` | `"latest"` | `"latest"`: drop old frames; `"reliable"`: lossless, ordered (not for video or audio channels) |
| `priority` | as published | zenoh priority 1–7 (`Priority.*`); ≤ INTERACTIVE_HIGH (2) makes it strict |
| `maxAge` | none | ms; drop anything older (also the SCTP packet lifetime on `"latest"`) |
| `maxHz` | none | never send a key faster |
| `bandwidthPriority` | 1 | when bandwidth is short, a higher number keeps more bandwidth and quality (each stream gives up in proportion to demand / priority); 0 gives up everything first |
| `minQuality` | 0 | the least quality the allocator picks for an encoded stream (the most is `encodeOptions.quality`) |
| `qualityToHzTradeoff` | 0.5 | 0 = keep quality, drop Hz; 1 = keep Hz, drop quality |
| `encoding` | none (raw bytes) | a name from `z.encodings` (see [Encodings and channels](#encodings-and-channels)); the gateway rejects unknown names, listing them |
| `channel` | from the encoding's output: pictures `"video-h264"`, sound `"audio-opus"`, else `"data"` | what the messages travel on: `"video-h264"`, `"video-vp8"`, `"video-vp9"`, `"video-av1"`, `"audio-opus"` or `"data"` |
| `encodeOptions` | `{}` | passed to the encoding (each documents its own); `quality` (0–1, default 1) is the most the allocator picks, lowered when bandwidth is short |
| `compress` | the encoding's (none without one) | `"zstd"` or `"none"`: zstd-compress each data-channel message (raw topics too); the client decompresses, so `msg.bytes` is always plain. Rejected on video and audio channels |
| `maxBitrate` | the server's (~0.3 bit/pixel at the source's size and rate) | video channels: most bits/s the stream asks for; it encodes at what the allocator grants |
| `minResolutionScale` | 0.25 | video channels: the picture keeps its full size unless the grant is under 0.05 bit/pixel there, and never shrinks below this share |
| `maxResolution` | none | video channels: `[width, height]` box the picture is fitted into |
| `playoutDelay` | `[0, 0]` | video channels: `[min, max]` ms the browser may hold a frame to smooth out jitter; `[0, 0]` shows each frame at once (after a non-zero min on the same track, `[10, 10]`: Firefox can't go back) |

`Subscription`: `ready()` (resolves when the gateway accepted it and the channel is open, rejects with
the gateway's reason), `state` (`"connecting"`, `"open"`, `"rejected"`, `"closed"`), `mediaStream`
(video and audio channels), `channelName` (the channel in use), `received`, `dropped`, `partialDropped`,
`decodeErrors`, `gatewayStats`, `close()`, and `update(changes)`: changes `maxHz`, `minQuality`,
`qualityToHzTradeoff`, `bandwidthPriority`, `maxBitrate`, `minResolutionScale`, `maxResolution`, `playoutDelay` or
`encodeOptions.quality` on the running subscription (same channel and track, no resubscribe; `null` puts one back
to its default). A changed picture size starts at a keyframe.

### Publisher options and methods

| option | default | |
|---|---|---|
| `delivery` | `"latest"` | `"reliable"` puts use zenoh CongestionControl Block, else Drop |
| `priority` | zenoh default | 1–7; ≤ INTERACTIVE_HIGH is sent express |
| `repeatMs` | none | re-send the last value on a timer (client side) |
| `latencyLimit` | none | ms; the gateway drops puts older than this (clock-corrected) |

`Publisher`: `put(bytes | string | ArrayBufferView, { timestamp })`, `setDeadman(bytes)`,
`clearDeadman()`, `state` (`"connecting"`, `"open"`, `"tripped"`, `"rejected"`, `"closed"`),
`onTripped(fn)`, `tripReason`, `blocked` (why the gateway drops its puts now: another client's lease;
else `null`), `sent`, `dropped`, `ready()`, `close()`.

**Fields** messages arrive decoded as `msg.decoded`, an object of numbers, strings and typed arrays (`decodeFields`,
SPEC "Fields"). `registerEncoding(name, decoder)` supplies the browser decoder of any other data-channel encoding the
gateway's host application added (see [Custom encodings](#custom-encodings)): each message then gets
`msg.decoded = decoder(msg.bytes, msg)`. Without a decoder, `msg.bytes` still carries the encoding's bytes. Video and
audio need no decoder.

Also exported: `Priority` (`REAL_TIME` 1, `INTERACTIVE_HIGH` 2, `INTERACTIVE_LOW` 3, `DATA_HIGH` 4,
`DATA` 5, `DATA_LOW` 6, `BACKGROUND` 7) and the wire helpers `decodeFrame`, `decodeFields`, `decodeVideoFrameInfo`,
`encodePut`.

## Encodings and channels

A subscription names a message **encoding** (how to read its messages), the **channel** the result travels on, and
**encodeOptions** for the encoding: `subscribe(key, { encoding: "ros2_compressed_image", channel: "video-h264",
encodeOptions: { quality: 0.8 } })`. There is no auto-detection and none is built in, and the core knows no message
types: the application registers encodings (e.g. [zenoh-dimos-codecs](https://github.com/jeff-hykin/zenoh-dimos-codecs),
one per message type). No encoding = raw bytes, rate is the only degradation.

- **video channels** (`video-h264` default, `video-av1`, `video-vp8`, `video-vp9`): the encoding hands the gateway
  pictures, which it encodes at the bitrate the allocator grants (full size unless that is under 0.05 bit/pixel) and
  sends on a video track (`sub.mediaStream`, `msg.video`). Built-in encoders: H.264 (openh264) and AV1 (rav1e, feature
  `av1`, on by default; ~3 frames of added latency). The server can plug in others per format
  (`ServerBuilder::video_encoder`, e.g. [zenoh-dimos-codecs](https://github.com/jeff-hykin/zenoh-dimos-codecs)' hardware
  encoders: VideoToolbox, or GStreamer on a Jetson / NVENC / VAAPI); VP8 and VP9 need one. The page refuses a channel its
  browser can't play.
- **audio-opus**: the encoding hands it PCM, sent as Opus on an audio track (`sub.mediaStream`).
- **data**: bytes from the encoding: **fields** (named numbers and arrays built with `zenoh_gateway::Fields`, which the
  client decodes into `msg.decoded` itself) or its own format (decoded by the page with `registerEncoding`). An encoding
  can ask for zstd by default (`default_compress`, e.g. depth and point clouds); the `compress` option overrides it.

`encodeOptions.quality` is the most the bandwidth allocator picks; it lowers the quality when the link is squeezed (down
to `minQuality`). The rest of `encodeOptions` is the encoding's (an encoding refuses options it doesn't know).

## Use as a Rust library

An application (e.g. a desktop app) runs the server in-process, hands it the zenoh session it already
has (zenoh-gateway re-exports the zenoh it is built against, `zenoh_gateway::zenoh`), and adds message encodings written in
Rust.

```toml
[dependencies]
zenoh-gateway = { git = "https://github.com/jeff-hykin/zenoh-gateway", rev = "<commit>" }
tokio = { version = "1", features = ["full"] }
```

```rust
let server = zenoh_gateway::Server::builder()
    .connect("tcp/192.168.1.2:7447")       // or .session(existing_session), or .zenoh_config(config)
    .serve_dir("ui")                       // optional static files
    .bandwidth_target_fraction(0.75)
    .encoding(TextUppercase)               // a message encoding, below
    .build()
    .await?;
let running = server.bind(("0.0.0.0", 7448)).await?;   // background task; port 0 = any free port
println!("listening on {}", running.local_addr());
// ... when the app quits: deadmen fire, browsers disconnect, the session closes (if zenoh-gateway opened it)
running.shutdown().await?;
```

Also `server.serve(addr)`, `server.serve_with_shutdown(addr, signal)`, and `server.router()` (an axum
`Router` with `POST /offer`, `GET /zenoh-gateway/health`, `GET /zenoh-gateway/ice` and the static files, to mount in your own HTTP
server; then call `server.shutdown()` yourself). `GET /zenoh-gateway/health` answers
`{"service": "zenoh-gateway", "version": "..."}`, so an application can check whether a zenoh-gateway server
is already running on a port before starting its own (`zenoh_gateway::HEALTH_PATH`). API docs:
`cargo doc --open` in `gateway/`.

### Rust client (feature `client`)

A program with no browser can connect to a server the way a page does, e.g. a relay that takes one
robot's best stream per camera and serves it again through its own server. Video and audio arrive
encoded (H.264/VP8/VP9/AV1 access units with a keyframe flag, Opus packets); decode them yourself.

```toml
zenoh-gateway = { git = "https://github.com/jeff-hykin/zenoh-gateway", rev = "<commit>", features = ["client"] }
```

```rust
use zenoh_gateway::client::{Client, ClientOptions, Message, PublisherOptions, SubscribeOptions};
let client = Client::connect("http://robot.local:7448", ClientOptions { token: None, heartbeat_hz: 5.0, ..Default::default() }).await?;
let topics = client.list_topics("robot/**", None).await?;
let mut camera = client.subscribe("camera/front", SubscribeOptions { encoding: Some("ros2_image".into()), ..Default::default() }).await?;
while let Some(message) = camera.recv().await {     // also a futures::Stream
    match message {
        Message::Video(frame) => { /* frame.format, frame.data (Annex B), frame.keyframe */ }
        Message::Data(data) => { /* data.bytes (decompressed), data.fields (fields messages) */ }
        _ => {}
    }
}
camera.request_keyframe().await?;                    // RTCP PLI
let cmd = client.publish("cmd_vel", PublisherOptions::default()).await?;
cmd.put(b"...").await?;
cmd.set_deadman(b"stop").await?;                     // needs heartbeat_hz
let lease = client.lease("drive", None, None).await?; // exclusive publishing (SPEC "Leases")
// the rest of zenoh's API (SPEC "The rest of the zenoh API")
client.put("ui/note", b"hi", PutOptions { encoding: Some("text/plain".into()), ..Default::default() }).await?;
let replies = client.get_with("robot/params/*", GetOptions { parameters: Some("depth=2".into()), ..Default::default() }).await?;
let mut queryable = client.declare_queryable("ui/answers/**", false).await?;
while let Some(query) = queryable.recv().await { query.reply(None, b"42", None, None).await?; query.finalize().await?; }
let token = client.declare_token("ui/present").await?;
let alive = client.liveliness_get("robot/**", Duration::from_secs(1)).await?;
let matching = client.matching_status("cmd_vel", MatchingTarget::Subscribers).await?;
let info = client.info().await?;                     // the gateway's zid, routers, peers
client.close().await;                                // reconnecting is the caller's job: watch client.closed()
```

A server with no inbound ports (its zenoh dials out) can also be reached by signalling over that zenoh link:
`ServerBuilder::zenoh_signalling("robot")` on its side, `Client::connect_zenoh(&session, "robot", options)` on the
client's (SPEC "Signalling over zenoh"); [zenoh-gateway-relay](https://github.com/jeff-hykin/zenoh-gateway-relay) is built on it.

`examples/relay_sketch.rs` relays a camera from one server to another, decoding with openh264
(`cargo run --example relay_sketch --features client -- <url> <key> <encoding> <port>`). SPEC.md "Rust client".

### Custom encodings

Implement `zenoh_gateway::MessageEncoding`: decode a zenoh sample (key, payload, zenoh encoding) once per channel kind, then
produce **video** (`DecodedFrame::Video(VideoImage::rgb8(..)` or `::i420(..)`, BT.601): the gateway encodes it at the
granted bitrate in the subscription's video format and sends it on a video track, so the page just shows
`sub.mediaStream`; override `video_encoder(format)` to return your own `VideoEncoder`, e.g. one that passes through H.264
a camera already made), **audio** (`DecodedFrame::Audio(AudioPcm::new(..))`, Opus on an audio track) or bytes for the
data channel from `encode(frame, &EncodeOptions { quality, options })`: **fields** (built with `zenoh_gateway::Fields`,
decoded by the client with no page code) or any format (the page decodes it with `registerEncoding`). `output()` is
what it produces by default; `output_on(channel, options)` says what it produces on another channel or with
`encodeOptions`, or refuses them. Decodes are shared across browsers per sample, data encodes per (sample, quality,
options, compression). `estimated_bytes(payload_bytes, &options)` is an optional cost model for the allocator,
`default_compress()` the compression used when a subscription sets none.

```rust
use zenoh_gateway::{Channel, DecodedFrame, EncodeOptions, EncodingOutput, EncodingSample, MessageEncoding};

struct TextUppercase;

impl MessageEncoding for TextUppercase {
    fn name(&self) -> &str { "text_uppercase" }
    fn output(&self) -> EncodingOutput { EncodingOutput::Data }
    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> anyhow::Result<DecodedFrame> {
        Ok(DecodedFrame::data(std::str::from_utf8(sample.payload)?.to_uppercase()))
    }
    fn encode(&self, frame: &DecodedFrame, options: &EncodeOptions) -> anyhow::Result<Vec<u8>> {
        let text = frame.downcast::<String>()?;   // lower quality: a shorter prefix
        Ok(text.chars().take((text.chars().count() as f64 * options.quality).ceil() as usize).collect::<String>().into_bytes())
    }
}
```

```js
import { connect, registerEncoding } from "./zenoh_gateway.ts"
registerEncoding("text_uppercase", (bytes) => new TextDecoder().decode(bytes))
const z = await connect("http://localhost:7448")
z.subscribe("chat/**", { encoding: "text_uppercase" }, (msg) => console.log(msg.decoded))
```

A name that is already registered makes `build()` fail; the gateway refuses an unknown name with the list of encodings it
has. zenoh-gateway-cli's `examples/custom_codec.rs` is a complete program (its own zenoh session, the data encoding above and
a video encoding producing I420 frames); its `test/custom_codec.js` drives it from Chrome.
[zenoh-dimos-codecs](https://github.com/jeff-hykin/zenoh-dimos-codecs) is a whole crate of them (ROS 2 / dimos images,
depth, point clouds, audio).

zenoh is pinned to 1.6.2: 1.7.0 through 1.10.1 deadlock when the admin space answers a query while
a declaration waits for the routing tables (the fix, eclipse-zenoh/zenoh branch
`bugfix/routing-deadlock`, is unreleased). A dependent uses the same zenoh. The webrtc-rs fixes
zenoh-gateway needs are published as renamed crates (`zenoh-web-webrtc`, `zenoh-web-rtc`,
`zenoh-web-rtc-datachannel`, `zenoh-web-rtc-sctp`, from
[webrtc-rs-zenoh-web](https://github.com/jeff-hykin/webrtc-rs-zenoh-web), each with a `PATCHES.md`),
so a dependent crate gets the fixed code without any `[patch]` section.

## Access control

zenoh's own `access_control` (in the zenoh config) applies to the browsers' puts, subscriptions and
queries like to any other traffic of the server's session: what it denies is dropped silently.

## Auth

```rust
use zenoh_gateway::Grant;
let server = zenoh_gateway::Server::builder()
    .authorize(|token, _headers| match token {
        Some("viewer-token") => Ok(Grant { subscribe: vec!["robot/**".into()], list_topics: vec!["robot/**".into()], ..Default::default() }),
        Some("driver-token") => Ok(Grant { publish: vec!["robot/cmd/**".into()], lease_groups: vec!["drive".into()], max_lease_secs: Some(300.0), ..Grant::all() }),
        _ => Err("unknown token".into()),   // HTTP 401 with this reason
    })
    .lease_group("drive", ["robot/cmd/**"])
    .build()
    .await?;
server.revoke("driver-token");   // closes its live connections; the hook decides if it may come back
```

The page connects with `connect(url, { token })`. A `Grant` lists key expressions per action (`subscribe`,
`publish` (also put, delete, matching for subscribers), `query` (also queriers, matching for queryables),
`queryable` (queryables the page declares), `liveliness` (tokens, liveliness watching and get), `listTopics`) plus lease rights (`leaseGroups`, `maxLeaseSecs`, `forceExpire`); a
request is allowed when one of them includes its key, otherwise the channel is rejected with
`not authorized to <action> "<key>"`. No hook = `Grant::all()` for everyone. Issuing tokens is up to
the host; zenoh-gateway-cli's `--auth-file` is a small file-based example. SPEC "Auth".

## Leases

`const lease = await z.lease("drive", { maxSeconds: 60 })` gives this page the exclusive right to
publish on the group's keys among the gateway's clients: everyone else's puts there are dropped
(`publisher.blocked` says why) until the lease ends: the holder's heartbeat stops, `maxSeconds`
passes, it disconnects, calls `lease.release()`, or a client with `forceExpire` calls
`z.expireLease("drive")`. `lease.onLost(reason)` reports which. Groups come from the server
(`lease_group`) or, for names it doesn't define, from the client (`{ keys: [...] }`), within the grant.
**Leases bind only this gateway's clients**, not native zenoh publishers; use zenoh's
`access_control` for those. SPEC "Leases".

## ICE and TURN

`.ice_servers([IceServer { urls: vec!["turn:relay.example.org:3478".into()], ..Default::default() }])`
configures the gateway's side, and browsers get the same list from `GET /zenoh-gateway/ice` (the client
fetches it by itself). With coturn's `use-auth-secret`, `.turn_secret(secret, ttl)` mints
time-limited credentials per connection (username `"<expiry>:<user>"`, HMAC-SHA1 credential).
For any other TURN provider, `.ice_servers_fn(|request| async { ... })` mints servers for each end
of each connection (`request.side`: browser or gateway, `request.token`), added after the static ones; if
it fails or takes over 5 s, that end gets just the static ones. Cloudflare TURN is built in (feature
`cloudflare`): `.cloudflare_turn(CloudflareTurn::new(key_id, api_token))`, with `.ttl(..)` (default
24 h; shared credentials are minted again after half of it) and `.per_connection(true)`.
`.udp_ports(50000..=50100)` binds each connection's WebRTC sockets to a port from that range, to
firewall a relay or robot easily. SPEC "ICE and TURN".

## Heartbeat and deadman

`connect(url, { heartbeatHz: 5, heartbeatMisses: 3 })` sends beats on an unreliable channel (they are
also clock-sync samples). `await publisher.setDeadman(stopBytes)` stores one message on the gateway per
publisher. If the beats stop for `misses / hz` seconds, the page disconnects, or the gateway shuts down
(SIGINT/SIGTERM, or `shutdown()` when embedded), the gateway publishes it **once** (REAL_TIME, reliable). The publisher is then
`"tripped"` (`onTripped(reason)` with `"heartbeat"`, `"disconnected"` or `"shutdown"`); puts throw and a
new publisher is needed. Background tabs throttle timers to ≥ 1 s, so keep `misses / hz` well above 1 s.

## Bandwidth allocation

Per browser, every 250 ms: estimate the path (delivery rate + a delay trigger from RTT samples for data
channels, GCC for video), take `bandwidth_target_fraction` of it (capped by
`max_bandwidth_bytes_per_sec`), reserve strict-priority and reliable streams, and shrink the rest like
CSS flex items by `demand / bandwidthPriority` (higher priority keeps more; priority-0 streams shrink first). An encoded stream granted a fraction r of its demand shrinks its message size by
`r^qualityToHzTradeoff` and its rate by the rest. Bulk sends are paced so queues stay short and strict
streams don't wait behind them. Each subscription's `allocation` (demand, budget, hz, quality,
constrained) is in `z.stats`. Full algorithm and measurements: SPEC.md "Bandwidth allocation".

## Nix / cross compiling

The flake builds with [crate2nix](https://github.com/nix-community/crate2nix): every crate is its own nix store
derivation, so crates are built once and shared by every flake that uses `lib.crossRust` (zenoh-gateway, zenoh-dimos-codecs,
zenoh-gateway-cli, zenoh-gateway-relay, your own) instead of each repo's `target/`. A crate is shared when it resolves to the same
version and features: Cargo unifies features per project, so e.g. a project with `env_logger` turns on extra
`portable-atomic`/`regex` features that ripple into zenoh's crates, which then build once per such feature set (tokio,
rustls, ring, openh264, zstd, libopus, ... stay shared). Linux binaries are cross
compiled from a Mac (or the other Linux arch) with zig as the C compiler and linker, for glibc 2.35 (Ubuntu 22.04,
Jetson L4T 36): no Linux VM, no GCC cross toolchain. C sources (openh264, zstd, ring) cross compile through zig; Opus is
`unsafe-libopus` (Rust); GStreamer is loaded at runtime, so nothing links it.

```sh
nix build .#zenoh-gateway-example                  # native (example/: a loopback server + the Rust client)
nix build .#zenoh-gateway-example-aarch64-linux    # ELF aarch64, glibc >= 2.35
nix build .#zenoh-gateway-example-x86_64-linux
```

Your nix may build one derivation at a time (`max-jobs = 1`); with hundreds of crates pass `--max-jobs auto`.

### A crate that depends on zenoh-gateway

zenoh 1.6.2 drops a client's puts on a key after a publisher on it was undeclared and the key's
subscribers changed. zenoh-gateway builds against a fixed zenoh
([jeff-hykin/zenoh-vendor](https://github.com/jeff-hykin/zenoh-vendor)); `[patch]` doesn't carry over to
dependents, so add the same to yours:

```toml
[patch.crates-io]
zenoh = { git = "https://github.com/jeff-hykin/zenoh-vendor", tag = "zenoh-1.6.2-patch.1" }
```

```sh
nix flake init -t github:jeff-hykin/zenoh-gateway#downstream   # Cargo.toml, src/main.rs, flake.nix
cargo generate-lockfile && nix run github:jeff-hykin/zenoh-gateway#crate2nix -- generate   # Cargo.lock -> Cargo.nix
git add -A && nix build .#my-app-aarch64-linux --max-jobs auto
```

The template's `flake.nix` is the whole recipe:

```nix
{
    inputs.zenoh-gateway.url = "github:jeff-hykin/zenoh-gateway";
    outputs = { self, zenoh-gateway }: {
        packages = zenoh-gateway.lib.eachSystem (system: zenoh-gateway.lib.crossRustPackages {
            name = "my-app";                  # packages my-app, my-app-aarch64-linux, my-app-x86_64-linux
            inherit system;
            cargoNix = ./Cargo.nix;           # from `crate2nix generate`; regenerate when Cargo.lock changes
        });
    };
}
```

`lib.crossRust { system, cargoNix, crate ? null, features ? [ "default" ], crateOverrides ? { }, glibc ? "2.35" }`
returns `{ native, aarch64-linux, x86_64-linux }` (`crate`: a workspace member, default the root crate; `crateOverrides`:
buildRustCrate overrides merged over nixpkgs' `defaultCrateOverrides`, e.g. for a `-sys` crate that needs a
library). `lib.crossRustPackages { name, ... }` names them `<name>`, `<name>-aarch64-linux`, `<name>-x86_64-linux`.
Builds use zenoh-gateway's nixpkgs and Rust toolchain pins, which is what lets crates be shared; don't make the input
follow another nixpkgs. Native macOS binaries link `/usr/lib/libiconv` (not nix's), so they run on Macs without nix.

## Tests

```sh
cd gateway && cargo test --all-features && cargo clippy --all-features --all-targets && cargo doc --no-deps   # unit, client and doc tests, lints, API docs
deno task check                            # type-check the client
```

The end-to-end suites (a real zenoh peer, the server and headless Chrome; delivery, clock sync,
deadmen, allocation, latency under load, throughput on a shaped link, video latency, encodings, auth and leases, and the rest of the
zenoh API from a page) are in
[zenoh-gateway-cli](https://github.com/jeff-hykin/zenoh-gateway-cli), which builds them against this crate.

## Known limitations

- Topic listing sees liveliness tokens (incl. AdvancedPublishers with publisher detection) and keys
  that publish during its probe; plain declarations without a token are invisible.
- The default video encoder is software H.264 (openh264). Viewers of one stream at similar grants share one encode,
  but every stream still costs a core-share unless the server has a hardware encoder (`ServerBuilder::video_encoder`).
- Audio goes to the browser only; the microphone direction is designed (SPEC "Audio") but not built.
- Signaling is plain HTTP: a bearer token crosses the network in the clear unless the gateway sits behind
  HTTPS (a reverse proxy).
- Leases bind only this gateway's clients, and aren't re-taken after a reconnect.
- Encodings are compiled into the application (encodings shipped from the browser as WASM are a later phase).
- Changing a subscription's options means closing it and subscribing again.
