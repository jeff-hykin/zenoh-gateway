# zenoh-web

View and drive a [zenoh](https://zenoh.io) system from a browser over a squeezed network (a phone on
weak wifi), without a heavy bridge: a Rust library that serves browsers over WebRTC, and a
dependency-free TypeScript client. Pictures arrive as video (H.264 by default, or any encoder a
codec brings), sound as Opus audio, other data as bytes on per-stream data channels, and a per-browser bandwidth allocator decides who gets what when
the link is short. Codecs are plugged in by the application:

- [zenoh-web-cli](https://github.com/jeff-hykin/zenoh-web-cli): the `zenoh-web` command (install,
  flags, the example page and the end-to-end tests).
- [zenoh-dimos-codecs](https://github.com/jeff-hykin/zenoh-dimos-codecs): ROS 2 / dimos images,
  lossless depth and point clouds.

[SPEC.md](SPEC.md) is the detailed contract; this README is the overview.

![the example page: topic list, H.264 video, point cloud, depth, a raw stream and live allocation stats](https://raw.githubusercontent.com/jeff-hykin/zenoh-web-cli/main/test/artifacts/example.png)

## Architecture

```
zenoh peers / routers (publishers you don't control: ROS 2 over rmw_zenoh, dimos, anything)
        │  zenoh (the bridge is a normal zenoh peer or client)
   zenoh-web server  (Rust, in your process: zenoh 1.6.2, webrtc-rs, tokio, axum)
        │  HTTP: POST /offer (signaling) + optional static files
        │  WebRTC: one SCTP data channel per subscription/publisher, + video and audio tracks
   browser page  (client/zenoh_web.ts, loaded from esm.sh or bundled)
```

- Each `subscribe` / `publisher` is its own data channel, so a slow stream never blocks another one.
  `delivery: "latest"` channels are unordered and unreliable (old frames are dropped, never queued);
  `"reliable"` ones are ordered and lossless.
- The bridge never parses payloads unless a subscription picks a codec. Codecs run lazily inside the
  bridge, only for frames that will actually be sent.
- Every browser gets a bandwidth estimate and a budget; streams with a higher `bandwidthPriority` keep more, trading
  quality against rate per `qualityToHzTradeoff`. Strict-priority streams skip the queue.
- Heartbeat + deadman: a publisher can leave a "stop" message on the bridge that is published once if
  the page goes silent.

## Client API

```js
import { connect, Priority, registerCodec } from "https://esm.sh/gh/jeff-hykin/zenoh-web@<commit or tag>/client/zenoh_web.ts"

const z = await connect("http://robot.local:7448", { heartbeatHz: 5, heartbeatMisses: 3 })
const sub = z.subscribe("camera/**", { codec: "ros2-image", maxHz: 15 }, (msg) => {})
video.srcObject = sub.mediaStream
const cmd = z.publisher("cmd_vel", { priority: Priority.REAL_TIME, latencyLimit: 300 })
cmd.put(bytes)
await cmd.setDeadman(stopBytes)
```

esm.sh transpiles the TypeScript on the fly; pin a commit. Or bundle it (`deno bundle client/zenoh_web.ts`).
The bridge checks options: an unknown name or a bad value rejects the subscription or publisher (`ready()` rejects with the reason).

### `connect(url, options)` → `Promise<ZenohWeb>`

| option | default | |
|---|---|---|
| `heartbeatHz` | 0 (off) | heartbeats per second on their own channel; required for deadmen |
| `heartbeatMisses` | 3 | silence of `misses / hz` seconds = this page is gone |
| `iceServers` | `[]` | `RTCIceServer[]` (LAN needs none) |
| `reconnect` | `true` | re-open the connection and every live channel after a loss |
| `statsIntervalMs` | 1000 | how often `z.stats` / `z.bridgeStats` refresh |
| `clock` | `performance.timeOrigin + performance.now()` | the page's clock in ms (put timestamps, clock sync) |

### `ZenohWeb`

| member | |
|---|---|
| `subscribe(key, options, callback)` → `Subscription` | `callback(msg)`: `{ key, bytes, timestamp, seq, decoded?, video?, mediaStream? }` |
| `publisher(key, options)` → `Publisher` | |
| `get(key, { timeoutMs = 5000 })` → `[{ key, bytes, error? }]` | zenoh query |
| `listTopics(filter = "**", { probeMs = 600 })` → `[{ key, sources }]` | live keys; `sources` ⊂ `token`, `advancedPublisher`, `sample` (SPEC "Topic enumeration"); `probeMs: 0` skips the `sample` probe, so publishers that only send while matched stay asleep |
| `codecs` | `[{ name, output }]`: every codec the bridge runs (`output` `"video"` or `"data"`), fetched on connect |
| `stats` | per key: `received`, `dropped`, `backlogBytes`, `rttMs`, `bridge` (normalized options, bridge counters, `allocation`) |
| `bridgeStats` | `clock`, `heartbeat`, `bandwidth` (estimate, cap, budget, demand, queue delay, …) |
| `rttMs`, `clockOffsetMs` | round trip and bridge-minus-page clock offset |
| `state`, `onState(fn)` | `"connecting"` / `"connected"` / `"degraded"` / `"lost"`; `onState` returns an unsubscribe function |
| `now()` | the page clock used for timestamps |
| `pollStats()` | refresh stats now |
| `pauseHeartbeat()`, `resumeHeartbeat()` | stop/resume beats (to test deadman wiring) |
| `close()` | close everything |

### Subscribe options

| option | default | |
|---|---|---|
| `delivery` | `"latest"` | `"latest"`: drop old frames; `"reliable"`: lossless, ordered (not for video codecs) |
| `priority` | as published | zenoh priority 1–7 (`Priority.*`); ≤ INTERACTIVE_HIGH (2) makes it strict |
| `maxAge` | none | ms; drop anything older (also the SCTP packet lifetime on `"latest"`) |
| `maxHz` | none | never send a key faster |
| `bandwidthPriority` | 1 | when bandwidth is short, a higher number keeps more bandwidth and quality (each stream gives up in proportion to demand / priority); 0 gives up everything first |
| `minQuality`, `maxQuality` | 0, 1 | quality bounds for codec streams |
| `qualityToHzTradeoff` | 0.5 | 0 = keep quality, drop Hz; 1 = keep Hz, drop quality |
| `codec` | none (raw bytes) | a name from `z.codecs` (see "Codecs"); the bridge rejects unknown names, listing its codecs |
| `compress` | the codec's (none without one) | `"zstd"` or `"none"`: zstd-compress each data-channel message (raw topics too); the client decompresses, so `msg.bytes` is always plain. Rejected on video codecs |

`Subscription`: `ready()` (resolves when the bridge accepted it and the channel is open, rejects with
the bridge's reason), `state` (`"connecting"`, `"open"`, `"rejected"`, `"closed"`), `mediaStream`
(video codecs), `codecKind` (`"video"`, `"fields"`, `"data"` or `null`), `received`, `dropped`, `partialDropped`,
`decodeErrors`, `bridgeStats`, `close()`.

### Publisher options and methods

| option | default | |
|---|---|---|
| `delivery` | `"latest"` | `"reliable"` puts use zenoh CongestionControl Block, else Drop |
| `priority` | zenoh default | 1–7; ≤ INTERACTIVE_HIGH is sent express |
| `repeatMs` | none | re-send the last value on a timer (client side) |
| `latencyLimit` | none | ms; the bridge drops puts older than this (clock-corrected) |

`Publisher`: `put(bytes | string | ArrayBufferView, { timestamp })`, `setDeadman(bytes)`,
`clearDeadman()`, `state` (`"connecting"`, `"open"`, `"tripped"`, `"rejected"`, `"closed"`),
`onTripped(fn)`, `tripReason`, `sent`, `dropped`, `ready()`, `close()`.

A **fields** codec's messages arrive decoded as `msg.decoded`, an object of numbers, strings and
typed arrays (`decodeFields`, SPEC "Fields"). `registerCodec(name, decoder)` supplies the browser decoder of a data codec the bridge's host
application added (see [Custom codecs](#custom-codecs)): each message then gets
`msg.decoded = decoder(msg.bytes, msg)`. Without a decoder, `msg.bytes` still carries the codec's
bytes (and the page warns once). Video codecs need no decoder.

Also exported: `Priority` (`REAL_TIME` 1, `INTERACTIVE_HIGH` 2, `INTERACTIVE_LOW` 3, `DATA_HIGH` 4,
`DATA` 5, `DATA_LOW` 6, `BACKGROUND` 7) and the wire helpers `decodeFrame`, `decodeFields`, `decodeVideoFrameInfo`,
`encodePut`.

## Codecs

Picked explicitly per subscription; there is no auto-detection and none is built in, and the core
knows no message types. No codec = raw bytes, rate is the only degradation. A **video** codec hands
the bridge frames for its `VideoEncoder`: by default pictures, which the bridge scales to the
allocated quality and encodes as H.264; a codec can bring its own encoder (H.264, VP8, VP9 or AV1,
e.g. a hardware one) and the bridge negotiates, packetizes and paces it on a video track
(`sub.mediaStream`, `msg.video`). An **audio** codec hands it PCM, sent as Opus on an audio track
(`sub.mediaStream`). A **fields** codec sends named numbers and arrays (built with `zenoh_web::Fields`) that the client
decodes into `msg.decoded` itself; a **data** codec sends its own bytes, which the page decodes with
`registerCodec`. A codec can ask for zstd by default (`Codec::default_compress`, e.g. depth and point
clouds); the `compress` option overrides it.

## Use as a Rust library

An application (e.g. a desktop app) runs the server in-process, hands it the zenoh session it already
has (zenoh-web re-exports the zenoh it is built against, `zenoh_web::zenoh`), and adds codecs written in Rust.

```toml
[dependencies]
zenoh-web = { git = "https://github.com/jeff-hykin/zenoh-web", rev = "<commit>" }
tokio = { version = "1", features = ["full"] }
```

```rust
let server = zenoh_web::Server::builder()
    .connect("tcp/192.168.1.2:7447")       // or .session(existing_session), or .zenoh_config(config)
    .serve_dir("ui")                       // optional static files
    .bandwidth_target_fraction(0.75)
    .codec(TextUppercase)                  // a codec, below
    .build()
    .await?;
let running = server.bind(("0.0.0.0", 7448)).await?;   // background task; port 0 = any free port
println!("listening on {}", running.local_addr());
// ... when the app quits: deadmen fire, browsers disconnect, the session closes (if zenoh-web opened it)
running.shutdown().await?;
```

Also `server.serve(addr)`, `server.serve_with_shutdown(addr, signal)`, and `server.router()` (an axum
`Router` with `POST /offer`, `GET /zenoh-web/health` and the static files, to mount in your own HTTP
server; then call `server.shutdown()` yourself). `GET /zenoh-web/health` answers
`{"service": "zenoh-web", "version": "..."}`, so an application can check whether a zenoh-web server
is already running on a port before starting its own (`zenoh_web::HEALTH_PATH`). API docs:
`cargo doc --open` in `bridge/`.

### Custom codecs

Implement `zenoh_web::Codec`: decode a zenoh sample (key, payload, encoding) once, then produce
**video** (`DecodedFrame::Video(VideoImage::rgb8(..)` or `::i420(..))`: the bridge scales it
to the allocated quality, encodes H.264 and sends it on a video track, so the page just shows
`sub.mediaStream`; override `video_encoder()` to return your own `VideoEncoder`, e.g. NVENC or a
Jetson's encoder, declaring its `VideoFormat`), **audio** (`DecodedFrame::Audio(AudioPcm::new(..))`,
Opus on an audio track) or bytes for the data channel from `encode(frame, quality)`: **fields** (built
with `zenoh_web::Fields`, decoded by the client with no page code) or **data** (any format; the page
decodes it with `registerCodec`). Decodes are shared across browsers per sample, data encodes per
(sample, quality, compression). `estimated_bytes(payload_bytes, quality)` is an optional cost model
for the allocator, `default_compress()` the compression used when a subscription sets none.

```rust
use zenoh_web::{Codec, CodecOutput, CodecSample, DecodedFrame};

struct TextUppercase;

impl Codec for TextUppercase {
    fn name(&self) -> &str { "text-uppercase" }
    fn output(&self) -> CodecOutput { CodecOutput::Data }
    fn decode(&self, sample: &CodecSample<'_>) -> anyhow::Result<DecodedFrame> {
        Ok(DecodedFrame::data(std::str::from_utf8(sample.payload)?.to_uppercase()))
    }
    fn encode(&self, frame: &DecodedFrame, quality: f64) -> anyhow::Result<Vec<u8>> {
        let text = frame.downcast::<String>()?;   // lower quality: a shorter prefix
        Ok(text.chars().take((text.chars().count() as f64 * quality).ceil() as usize).collect::<String>().into_bytes())
    }
}
```

```js
import { connect, registerCodec } from "./zenoh_web.ts"
registerCodec("text-uppercase", (bytes) => new TextDecoder().decode(bytes))
const z = await connect("http://localhost:7448")
z.subscribe("chat/**", { codec: "text-uppercase" }, (msg) => console.log(msg.decoded))
```

A name that is already registered makes `build()` fail; the bridge refuses an unknown name with the
list of codecs it has. zenoh-web-cli's `examples/custom_codec.rs` is a complete program (its own zenoh
session, the data codec above and a video codec producing I420 frames); its `test/custom_codec.js`
drives it from Chrome. [zenoh-dimos-codecs](https://github.com/jeff-hykin/zenoh-dimos-codecs) is a
whole crate of them (ROS 2 / dimos images, depth, point clouds).

zenoh is pinned to 1.6.2: 1.7.0 through 1.10.1 deadlock when the admin space answers a query while
a declaration waits for the routing tables (the fix, eclipse-zenoh/zenoh branch
`bugfix/routing-deadlock`, is unreleased). A dependent uses the same zenoh. The webrtc-rs fixes
zenoh-web needs are published as renamed crates (`zenoh-web-webrtc`, `zenoh-web-rtc`,
`zenoh-web-rtc-datachannel`, `zenoh-web-rtc-sctp`, from
[webrtc-rs-zenoh-web](https://github.com/jeff-hykin/webrtc-rs-zenoh-web), each with a `PATCHES.md`),
so a dependent crate gets the fixed code without any `[patch]` section.

## Access control

zenoh's own `access_control` (in the zenoh config) applies to the browsers' puts, subscriptions and
queries like to any other traffic of the server's session: what it denies is dropped silently.

## Heartbeat and deadman

`connect(url, { heartbeatHz: 5, heartbeatMisses: 3 })` sends beats on an unreliable channel (they are
also clock-sync samples). `await publisher.setDeadman(stopBytes)` stores one message on the bridge per
publisher. If the beats stop for `misses / hz` seconds, the page disconnects, or the bridge shuts down
(SIGINT/SIGTERM, or `shutdown()` when embedded), the bridge publishes it **once** (REAL_TIME, reliable). The publisher is then
`"tripped"` (`onTripped(reason)` with `"heartbeat"`, `"disconnected"` or `"shutdown"`); puts throw and a
new publisher is needed. Background tabs throttle timers to ≥ 1 s, so keep `misses / hz` well above 1 s.

## Bandwidth allocation

Per browser, every 250 ms: estimate the path (delivery rate + a delay trigger from RTT samples for data
channels, GCC for video), take `bandwidth_target_fraction` of it (capped by
`max_bandwidth_bytes_per_sec`), reserve strict-priority and reliable streams, and shrink the rest like
CSS flex items by `demand / bandwidthPriority` (higher priority keeps more; priority-0 streams shrink first). A codec stream granted a fraction r of its demand shrinks its message size by
`r^qualityToHzTradeoff` and its rate by the rest. Bulk sends are paced so queues stay short and strict
streams don't wait behind them. Each subscription's `allocation` (demand, budget, hz, quality,
constrained) is in `z.stats`. Full algorithm and measurements: SPEC.md "Bandwidth allocation".

## Tests

```sh
cd bridge && cargo test && cargo clippy --all-targets && cargo doc --no-deps   # unit + doc tests, lints, API docs
deno task check                            # type-check the client
```

The end-to-end suites (a real zenoh peer, the server and headless Chrome; delivery, clock sync,
deadmen, allocation, latency under load, throughput on a shaped link, video latency, codecs) are in
[zenoh-web-cli](https://github.com/jeff-hykin/zenoh-web-cli), which builds them against this crate.

## Known limitations

- Topic listing sees liveliness tokens (incl. AdvancedPublishers with publisher detection) and keys
  that publish during its probe; plain declarations without a token are invisible.
- The default video encoder is software H.264 (openh264), one per browser subscription: CPU scales
  with viewers × streams unless a codec brings a hardware encoder.
- Audio goes to the browser only; the microphone direction is designed (SPEC "Audio") but not built.
- No per-user identity or auth on `/offer`. Run it on a trusted network.
- No TURN/relay configuration on the bridge side: the browser must reach the bridge's UDP ports (LAN, VPN).
- Codecs are compiled into the application (codecs shipped from the browser as WASM are a later phase).
- Changing a subscription's options means closing it and subscribing again.
