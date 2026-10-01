# zenoh-web spec

A browser UI that views and drives a zenoh system over a squeezed network, without a heavy bridge.

## Shape

```
zenoh peers (publishers we don't control)
        │  zenoh
   zenoh-web bridge (Rust: the zenoh-web command, or a library inside another application)
        │  WebRTC data channels (UDP) + one HTTP endpoint for signaling
   browser page (plain JS client, live-editable)
```

The bridge is a dumb pipe: one data channel ↔ one zenoh key expression. It never parses payloads,
except when a subscription explicitly picks one of its codecs (see "Codecs").

## JS API

```js
import { connect, Priority, registerCodec } from "./zenoh_web.ts"   // via esm.sh, or bundled: see "Client"

const z = await connect("http://robot.local:7448", {
    heartbeatHz: 5,         // 0 (default) = no heartbeat; needed for deadmen
    heartbeatMisses: 3,     // silence of misses/heartbeatHz seconds = frontend gone
    bandwidthTargetFraction: 0.75,  // optional: overrides the bridge's --bandwidth-target-fraction here
})

const sub = z.subscribe("camera/**", {
    delivery: "latest",          // or "reliable"
    priority: Priority.DATA_LOW, // optional; defaults to the priority the message was published with
    queueSize: 1,                // pending samples per key; default 1 for latest, Infinity for reliable
    maxAge: 500,                 // ms; drop anything older
    maxHz: 20,                   // bridge never sends a key faster than this
    bandwidthPriority: 1,        // flex-shrink weight when bandwidth is short (higher shrinks more)
    dangerousMinHz: 1,           // allocation floor (may starve others)
    minQuality: 0.3,             // 0-1, transcoded streams only
    maxQuality: 1.0,
    qualityToHzTradeoff: 0.7,    // 0 = keep quality, drop hz; 1 = keep hz, drop quality
    codec: "ros2-image",         // optional transcoder, see "Codecs"; names not in z.codecs throw
}, (msg) => { msg.key, msg.bytes, msg.timestamp, msg.seq, msg.depth, msg.points, msg.decoded, msg.video, msg.mediaStream })
sub.mediaStream  // video codecs: a MediaStream for a <video> element
sub.close()

const cmd = z.publisher("cmd_vel", {
    delivery: "latest",
    priority: Priority.REAL_TIME,
    repeatMs: 100,          // client-side: re-send the last value on a timer
    latencyLimit: 300,      // ms; the bridge drops puts older than this (clock-corrected). default: none
})
cmd.put(bytes)                    // or cmd.put(bytes, { timestamp: z.now() - delay })
await cmd.setDeadman(stopBytes)   // throws if connect() had no heartbeat
await cmd.clearDeadman()
cmd.state                         // "connecting" | "open" | "tripped" | "rejected" | "closed"
cmd.onTripped((reason) => {})     // "heartbeat" | "disconnected" | "shutdown"
cmd.close()

const replies = await z.get("some/key/**")   // zenoh query, returns [{ key, bytes }]
const topics = await z.listTopics("robot/**")  // [{ key, sources }], see "Topic enumeration"

await sub.ready()      // resolves when the bridge accepted the channel, rejects with its reason
sub.state              // "connecting" | "open" | "rejected" | "closed" (publishers add "tripped")

z.stats          // per-key: received, dropped, backlogBytes, rttMs, bridge (normalized options, stats, allocation)
z.bridgeStats.bandwidth  // this frontend's estimate, cap, budget, demand (see "Bandwidth allocation")
z.clockOffsetMs  // bridge clock - browser clock
z.rttMs
z.onState(fn)    // "connecting" | "connected" | "degraded" | "lost"
z.codecs         // [{ name, output: "video" | "data" }]: the bridge's codecs, fetched on connect

registerCodec("text-uppercase", (bytes, msg) => new TextDecoder().decode(bytes))  // browser side of a data codec: msg.decoded
```

- `Priority` mirrors zenoh / zenoh-ts: REAL_TIME=1, INTERACTIVE_HIGH=2, INTERACTIVE_LOW=3, DATA_HIGH=4, DATA=5, DATA_LOW=6, BACKGROUND=7 (lower = more important).
- Options are validated in the client (unknown names and out-of-range values throw) and again in the bridge.
- `bandwidthPriority` (default 1), `dangerousMinHz` (0), `minQuality` (0), `maxQuality` (1),
  `qualityToHzTradeoff` (0.5) drive the per-frontend allocator ("Bandwidth allocation"); stats show
  them with defaults filled in.
- No `latched` flag: the bridge always subscribes with zenoh-ext AdvancedSubscriber history (max 1 sample per publisher), so publishers with a cache (e.g. rmw_zenoh transient_local like tf_static) replay their last message.

## Delivery → transport mapping

| options | data channel init | bridge behavior when the channel is backed up |
|---|---|---|
| `delivery: "reliable"` | ordered, fully reliable | queue `queueSize` per key (default unbounded) |
| `delivery: "latest"` | unordered, maxRetransmits 0 | keep at most `queueSize` (default 1) per key, drop oldest |
| `+ maxAge: M` (latest) | unordered, maxPacketLifeTime M | also drop anything older than M |

"Backed up" = the channel's unacknowledged bytes are above ~64 KB, or the page has not yet consumed
~256 KB the bridge sent (the client acks consumption with a 4-byte `u32 seq` message on each `sub`
channel, because browsers queue received messages for the page without limit; on lossy channels a
sender blocked only by that window sends one probe frame after 50 ms, doubling to 1 s, so a lost tail
can't wedge it). The bridge drains
its own queue on bufferedAmountLow / acks. Nothing piles up in kernel/wifi buffers or the browser,
so a slow phone sees fewer frames instead of stale ones.

Browser → zenoh puts: priority from the publisher options, congestion control Block for `"reliable"`,
Drop otherwise, express for priority ≤ INTERACTIVE_HIGH.

## Client

`client/zenoh_web.ts` (strict TypeScript, no dependencies). Browsers load it either from esm.sh, which
transpiles it (`https://esm.sh/gh/<owner>/zenoh-web@<tag>/client/zenoh_web.ts`), or bundled locally:
`deno task build` runs `deno bundle` (the same esbuild transform) into `build/client/zenoh_web.js` next
to copies of `examples/` and `test/`, so `zenoh-web --serve build` works offline. The e2e test does the same.

The example page (`examples/index.html`, `examples/app.js`: plain JS, no build) imports the client from
esm.sh pinned to a commit, so `zenoh-web --serve examples` serves it at `/` (directories serve their
`index.html`); `?client=<url>` swaps in another copy of the client (e.g. `/client/zenoh_web.js` from a
`--serve build` root) and `?bridge=<url>` another bridge.

## Server (Rust library)

The bridge is the `zenoh-web` crate: a library plus the `zenoh-web` command, a thin wrapper over it
(same flags). `Server::builder()` takes the zenoh config (`zenoh_config`, `zenoh_config_file`,
repeatable `connect`) or an existing `session` (never closed by the server; its admin space must be
readable for `listTopics`' routing-table sources), `serve_dir`, `max_bandwidth_bytes_per_sec`,
`bandwidth_target_fraction`, `strict_priority` and external `codec`s; `build().await` validates them
and opens the session. Then `bind(addr)` serves on a background task (`RunningServer::local_addr`,
`shutdown()`), `serve(addr)` / `serve_with_shutdown(addr, signal)` serve in place, or `router()` returns
the axum routes (`POST /offer`, static files) for the host's own HTTP server. Shutdown fires every
frontend's deadmen (reason `"shutdown"`), closes the browser connections, then the session if the
server opened it.

The webrtc-rs fixes the bridge relies on (see "Delivery → transport mapping", "Large messages") are
in renamed forks under `bridge/forks/` (`zenoh-web-webrtc` → `zenoh-web-rtc` →
`zenoh-web-rtc-datachannel`, `zenoh-web-rtc-sctp`), which zenoh-web depends on directly, so crates that
depend on zenoh-web build the fixed code; a `[patch.crates-io]` would only apply in zenoh-web's own workspace.

## Large messages

Any size: the bridge splits a message into 64 KiB chunks (one frame each, sharing the message's `seq`)
and the client reassembles. The page acks frames, not messages, so a big message never deadlocks the
consumption window. On `latest`, a message that can't arrive whole is dropped whole, never delivered
partially: the bridge finishes a chunked message it started (so big messages make progress even when
newer ones keep arriving) unless it outlives `maxAge` (`abandonedPartial`); SCTP drops lost chunks
(`maxRetransmits: 0` / `maxPacketLifeTime`); and the client discards incomplete messages once a newer
one completes or more than 8 are pending (`partialDropped`).

## Codecs

Picked explicitly per subscription with `codec`. No `codec` = raw passthrough (Hz is the only
degradation). There is no auto-detection. The bridge has a registry of codecs by name: the built-in
ones below, plus any an application embedding the bridge as a Rust library registered
(`ServerBuilder::codec`; a name registered twice fails the build). The client fetches the registry
on connect (`control` op `codecs` → `{codecs: [{name, output}]}`, as `z.codecs`); an unknown name
throws in the client and is refused by the bridge (`rejected` event), both listing the known names.

Every codec, built-in or not, implements the same Rust trait (`zenoh_web::Codec`):

- `name()`, and `output()`: **video** or **data**.
- `decode(sample)`: the sample's key, payload and zenoh encoding → a decoded frame. A video codec's
  frame is a picture (packed RGB8 or planar I420, any size).
- data codecs: `encode(frame, quality)` → the bytes sent on the data channel (quality 0..1, from the
  allocator). The page decodes them with the decoder registered for that name
  (`registerCodec(name, decoder)` → `msg.decoded`); without one it gets `msg.bytes` (and a warning).
- video codecs: the bridge scales the picture to the allocated quality, encodes H.264 and sends it on
  the subscription's video track (see "Video"); the page needs no codec code. They require
  `delivery: "latest"`.
- `estimated_bytes(payloadBytes, quality)`: optional cost model for data codecs (bytes per message),
  the allocator's prior until sizes are measured and its shape between measured qualities (default:
  10–100 % of the payload, linear in quality). Video is priced by the bridge's own model.

The built-in codecs are named `<protocol>-<input type>`; the input type decides the output:

| codec | input message | output |
|---|---|---|
| `ros2-image`, `dimos-image` | `sensor_msgs/Image`: rgb8, bgr8, rgba8, bgra8, mono8, mono16/16UC1 (top 8 bits), or `jpeg`/`png` data in an Image (dimos' jpeg-encoded Image) | H.264 video track |
| `ros2-compressed-image`, `dimos-compressed-image` | `sensor_msgs/CompressedImage`: jpeg, png, webp, jxl (magic bytes first, `format` string second) | H.264 video track |
| `ros2-depth`, `dimos-depth` | `sensor_msgs/Image`: 16UC1, 32FC1, mono16 | lossless depth, data channel |
| `ros2-compressed-depth`, `dimos-compressed-depth` | `sensor_msgs/CompressedImage`: 16-bit gray png or jxl, ROS `compressedDepth` png (12-byte header skipped; its quantized 32FC1 form is refused) | lossless depth, data channel |
| `ros2-pointcloud2`, `dimos-pointcloud2` | `sensor_msgs/PointCloud2`, any field layout | quantized points, data channel |

Inputs:
- ROS 2 over rmw_zenoh: key `<domain>/<topic>/<pkg>::msg::dds_::<Type>_/RIHS01_<hash>`, payload CDR with
  the 4-byte encapsulation header (little or big endian honored).
- dimos over zenoh: key `<topic>/<msg_name>` (e.g. `dimos/camera/color/sensor_msgs.Image`), payload in
  the dimos message format (big endian) with its 8-byte type fingerprint, which the bridge checks (a
  wrong type is an error, counted in `codecErrors` / `lastCodecError` stats).
- mono16 is ambiguous (IR intensity or depth-like); the subscriber decides: `*-image` shows its top
  8 bits as gray video, `*-depth` delivers it losslessly with encoding `mono16`.
- Decoders are pure Rust (zune-jpeg, png, image-webp, jxl-oxide); H.264 is openh264, compression zstd.
- For video, a YCbCr JPEG with even sides decodes straight to I420 (full range → BT.601 limited), never
  through RGB; everything else decodes to RGB8. Scaling is a box filter; RGB → I420 is integer BT.601.
- A video subscription decodes its next frame while it encodes the current one (two blocking-pool
  tasks), so the rate is set by the slower stage, not their sum.

Work happens lazily and on send: only messages the pacing/queues let through are transcoded, on
tokio's blocking pool. Work is shared across frontends through two small caches per bridge: decoded
frames keyed by (codec, key + payload hash), and data-channel encodes keyed by (codec, quality in
1/1000 steps, key + payload hash). Identical requests compute once (`encodes` vs `sharedEncodes` in
stats: data codecs count shared encodes, video codecs shared decodes). Each (frontend, subscription)
has its own H.264 encoder, because rate control and reference frames are per receiver.

### Video

The client adds a recvonly video transceiver and renegotiates over `control`
(`{op: "renegotiate", addVideo: true, sdp}` → `{sdp, mid}`); the bridge adds an H.264 track
(constrained baseline, `profile-level-id=42e01f`) that pairs with the new m-line, then the `sub`
channel's label names that `mid`. Renegotiations run one at a time. A closed video subscription's
transceiver (and the bridge's track) is reused by the next one instead of renegotiating again.
Each video frame also sends a 28-byte metadata frame on the `sub` channel (`msg.video`).
Quality q maps to resolution scale `0.25 + 0.75 q` (even sizes) and a target of `0.03 + 0.12 q`
bits per pixel; the encoder's bitrate is that size times the allocated Hz. Keyframes: the first
frame of every subscription, on PLI/FIR from the browser (`keyframeRequests`), and every 3 s.
Send-side congestion control: TWCC feedback into GCC (webrtc-rs interceptors), whose target feeds the
allocator.

### Depth and point clouds

Depth stays lossless: quality only lowers resolution, by an integer stride `round(1 / (1/8 + 7/8 q))`
(1 at q = 1, 2 at 0.5, 8 at 0), nearest neighbor (every value is a source value, never a blend).
`msg.depth.data` is a `Uint16Array` (16UC1, mono16) or `Float32Array` (32FC1).

Point clouds: points with a non-finite x, y or z are skipped; fields are read by name (`x`, `y`, `z`,
optional `intensity`) at their offsets with any PointField datatype, honoring `point_step`, `row_step`
and `is_bigendian`. Quality q < 1 voxel-downsamples with voxel edge `0.2 m × (1 − q)`: each occupied
voxel becomes one point at its center (mean intensity). Coordinates are int16 around a per-message
origin: `x = originX + qx × scale`. Error per axis against the source point is at most `scale / 2`
without voxels, where `scale = (largest bounding-box extent / 2) / 32767` (e.g. 0.76 mm for a
100 m wide cloud), plus f32 rounding (~1e-7 relative); with voxels, at most `voxelSize / 2`
(`msg.points.maxError`). Intensity is scaled to u8 over the message's min..max (`intensityMin`,
`intensityScale`). `msg.points.positions` is a `Float32Array` (x, y, z per point).

## Bandwidth allocation

All of a frontend's streams share one path (wifi queue, UDP, one SCTP association), so throttling
"on average" isn't enough: a burst or an overshoot builds a queue that every stream waits in. The
bridge keeps the path's queues short and lets urgent streams skip what queue remains:

- **Strict-priority tier.** A subscription whose priority (its `priority` option, else the published
  priority of its samples) is `--strict-priority` (default 2, INTERACTIVE_HIGH) or more urgent
  bypasses allocation and pacing: its measured rate is reserved off the top, and while it sends a
  message no bulk chunk starts. Everything else shares the remainder by `bandwidthPriority`.
- **Target fraction (headroom).** The allocator hands out `--bandwidth-target-fraction` (default
  0.75; per connection: connect option `bandwidthTargetFraction`) of the estimate, so bulk traffic
  runs below the path's capacity and its queue stays near empty.
- **Pacing.** Bulk streams send through a per-stream token bucket at their granted rate × 1.25, in
  chunks of 4 ms of the frontend's budget (4–64 KiB). A per-frontend gate admits a bulk chunk only
  while the bulk bytes outstanding in SCTP are under `budget × (min RTT + 5 ms)` (at least two chunks,
  about one bandwidth-delay product), counting a chunk the moment it is admitted so senders woken
  together can't burst. A strict message waits behind at most the bulk already handed to SCTP, and
  bulk never bursts whole messages into the link.
- **Delay trigger.** The browser reports its RTT with every clock-sync sample (each heartbeat, else
  the 1 s control ping: configure `heartbeatHz` for a fast trigger). When the smallest RTT of a 250 ms
  interval is more than 5 ms above the 30 s minimum, a queue is standing on the path: the data
  estimate drops 15% at once and probing pauses 1 s; then it probes up 10% per interval below 90% of
  the level that caused the queue and 2% above it (before any congestion: 50% per interval, a slow
  start). No loss is needed to react.

Measured by `test/latency.js` through real queueing: a userspace UDP shaper (in the test process)
sits between Chrome and the bridge, 2 MB/s bridge→browser with a 1 MB drop-tail queue and 5 ms each
way (browser→bridge is delayed, not rate limited); a signaling proxy rewrites the SDP so ICE can only
use the shaper. A strict-priority 200 B stream at 50 Hz, alone: p99 ≈ 7.7 ms; with five 200 KB × 10 Hz
bulk streams (10 MB/s wanted) the bulk gets ~1.7 MB/s and the strict p99 is ≈ 17 ms (+9 ms). The same
stream without strict priority under that load: p99 ≈ 0.4–0.5 s.

Per frontend, every 250 ms:
1. **Estimate.** Data channels: a delivery-rate estimator over what the `sub` channels pushed into SCTP
   and how long their senders waited on SCTP (webrtc-rs doesn't expose the congestion window), plus the
   delay trigger above: blocked on the network more than 20% of the interval → estimate = 0.9 ×
   measured rate (at most halving per step); otherwise, while streams want more, probe up (start
   1 MB/s). Video: GCC's target bitrate (TWCC feedback), counted while a video track is in use; video
   tracks are paced by the GCC pacer (at 2.5 × the GCC target, as libwebrtc paces), not the bulk gate. Budget = min(`--max-bandwidth-bytes-per-sec`
   if set, target fraction × (data estimate + video estimate)).
2. **Demand.** Each subscription wants `price(maxQuality) × Hz`, Hz being each key's measured source
   rate capped by `maxHz`, summed over its keys. Price = bytes per message: measured for raw streams
   and data-channel codecs (per quality, scaled by the codec's `estimated_bytes` between measured
   qualities, which is also the prior before anything was measured), modeled for video (resolution ×
   bits per pixel). Floor = `price(minQuality) × dangerousMinHz` (per key,
   never above the key's rate). Strict-priority and reliable streams are reserved at their measured
   rate instead.
3. **Shrink.** If demand exceeds the budget, streams shrink like CSS flex items: the deficit is split in
   proportion to `bandwidthPriority × demand`; a stream that would go below its floor freezes there and
   the rest shrink further. Weight-0 streams shrink only once nothing else can. Floors are kept even
   when they add up to more than the budget ("dangerous"). Reliable subscriptions can't drop messages,
   so their measured rate is reserved and never shrunk (they still yield to strict streams).
4. **Quality vs Hz.** A transcoded stream granted fraction r of its demand shrinks its message size by
   `r^t` (choosing the best quality among the bounds and 0.1 steps that fits) and its Hz by the rest,
   `t = qualityToHzTradeoff`: 0 keeps quality and drops Hz, 1 keeps Hz and drops quality. When one
   hits its bound (`minQuality`, or `dangerousMinHz`), the other gives.
5. **Apply.** Each key's send interval becomes `1 / (its wanted Hz × granted fraction)` (never below
   its floor or 0.05 Hz); transcoders encode at the granted quality.

Stats: each subscription's `allocation` (`demandBytesPerSec`, `floorBytesPerSec`,
`budgetBytesPerSec`, `hz`, `hzFraction`, `quality`, `constrained`) and the frontend's `bandwidth`
(`dataEstimateBytesPerSec`, `videoEstimateBytesPerSec`, `capBytesPerSec`, `budgetBytesPerSec`,
`targetFraction`, `reservedBytesPerSec`, `bulkChunkBytes`, `queueDelayMs`, `minRttMs`, `delayEvents`,
`demandBytesPerSec`, `sentBytesPerSec`, `networkBlockedFraction`, `constrained`). Per stream,
`pacedMs` is time spent waiting on the token bucket and the gate.

## Topic enumeration

`z.listTopics(filter = "**", { probeMs = 600 })` merges what the bridge can see, each key tagged with its sources:

| source | what it sees |
|---|---|
| `subscriber`, `queryable` | declarations in the bridge's zenoh routing tables (read through its own admin space) |
| `token` | liveliness tokens (`liveliness().get`) |
| `advancedPublisher` | zenoh-ext AdvancedPublishers with publisher_detection (their `@adv/pub` token) |
| `sample` | keys that published during a `probeMs` subscription on `filter`, declared or not |

`probeMs: 0` skips the `sample` probe entirely (no subscription on `filter`), so a publisher that
only sends while it has a matching subscriber (a zenoh matching listener) isn't woken by a listing.

It can't see a plain publisher that is declared but silent during the probe: zenoh peers only forward
publisher declarations to nodes that declared interest in them, which zenoh's public API doesn't expose.
rmw_zenoh liveliness tokens (`@ros2_lv/...`) only appear when the filter names them; they're returned raw.
The bridge enables its own admin space read-only for this.

## Access control

The bridge reads `access_control` from its zenoh config (`--zenoh-config`). zenoh enforces it on the
bridge's own session too (verified: an egress `put` deny stops the bridge's puts), but silently. So the
bridge also applies it before a browser's `put` (publisher channel), `declare_subscriber` (subscription)
and `query` (`get`), with zenoh's decision logic (a matching deny rule wins; otherwise
`default_permission`, or an allow rule whose key expression includes the key; only rules referenced by a
policy and covering the `egress` flow count). Browsers have no zenoh subject, so rules apply whatever
their subjects. A refused channel gets a `rejected` event with the reason (the publisher/subscription
goes to state `"rejected"`, `ready()` rejects, `put` throws), a refused `get` rejects, and
`stats.access.denied` counts refusals.

## Clock sync

Per connection, NTP-style: the browser sends `t0` (its clock), the bridge answers with `t1`/`t2` (its
receive/send times), the browser notes `t3`. `offset = ((t1-t0)+(t2-t3))/2`, `rtt = (t3-t0)-(t2-t1)`.
The browser keeps the 16 most recent samples, uses the offset of the lowest-RTT one, and reports
that estimate to the bridge on every subsequent ping. Samples come from heartbeats when configured,
otherwise from the control-channel stats ping (every second); `connect()` takes a few samples before
resolving so the bridge has an offset before the first put.

## Heartbeat and deadman

- At most one heartbeat per frontend: its own data channel (unordered, maxRetransmits 0), sending
  `heartbeatHz` beats; every beat is also a clock-sync ping.
- `publisher.setDeadman(bytes)` stores (at most) one deadman per publisher stream per frontend on the bridge.
- When that frontend's heartbeat misses `heartbeatMisses` beats, when it disconnects (control channel
  closes, connection fails, or stays disconnected 15 s), or when the bridge shuts down (SIGINT/SIGTERM,
  or the embedding application's `shutdown()`), the
  bridge publishes every armed deadman of that frontend **once**, at REAL_TIME with CongestionControl
  Block (reliable), before anything else on that stream.
- No re-arm: the stream is then *tripped*. The bridge rejects further puts on it (counted as
  `rejectedTripped`), notifies the client over `control` (`{event:"tripped", id, reason}`), and the
  client publisher goes to state `"tripped"`, calls `onTripped` listeners and throws on `put`. The
  frontend must create a new publisher.
- Browsers throttle timers in background tabs (to ≥1 s), so pick `heartbeatMisses / heartbeatHz` well above 1 s.

## Wire format

- Signaling: `POST /offer` with the browser's SDP offer (non-trickle), returns the answer.
- `GET /zenoh-web/health` returns `{"service": "zenoh-web", "version": "<crate version>"}` (detecting a running server).
- The bridge can also serve a static directory (`--serve <dir>`, `ServerBuilder::serve_dir`) so the UI is live-editable on disk.
- Each subscribe/publisher is its own data channel. Its label is JSON: `{"type":"sub"|"pub", "key":..., "id":n, "opts":{...}}`.
  The heartbeat channel is `{"type":"heartbeat", "opts":{"hz":..., "misses":...}}`.
- One extra channel labeled `control` carries JSON request/response (`get`, `listTopics`, `stats`, `ping`,
  `codecs`, `configure`, `renegotiate`, `setDeadman`, `clearDeadman`) and events: `accepted` / `rejected`
  (per sub/pub channel, by label id) and `tripped`.
- Bridge → browser frame: `u16 keyLen | key utf8 | f64 timestampMs | u32 seq | u32 frameId | u32 chunkIndex | u32 chunkCount | chunk`,
  little endian. `seq` numbers messages per channel, `frameId` numbers frames; the page acks the highest
  `frameId` it has processed with a 4-byte `u32` message on the same channel.
- All chunks of one message have the same size, at most 64 KiB (bulk streams use smaller ones, see
  "Bandwidth allocation").
- Browser → bridge put: `f64 sentAtMs (browser clock) | payload`, little endian.
- Heartbeat: browser sends `{"t0", "offsetMs", "rttMs"}` (JSON), bridge answers `{"t0", "t1", "t2"}`.
- Video `sub` label: adds `"mid"`. The payload of a codec message (little endian):
  - depth: `u8 version=1 | u8 encoding (1 16UC1, 2 32FC1, 3 mono16) | u16 stride | u32 width |
    u32 height | u32 sourceWidth | u32 sourceHeight | zstd(width × height values)`
  - point cloud: `u8 version=1 | u8 flags (bit0 intensity) | u16 0 | u32 pointCount | u32 sourcePointCount |
    f32 originX | f32 originY | f32 originZ | f32 scale | f32 voxelSize | f32 intensityMin |
    f32 intensityScale | zstd(i16 x, y, z per point, then u8 intensity per point if flagged)`
  - video metadata: `u8 version=1 | u8 flags (bit0 keyframe) | u16 0 | u32 width | u32 height |
    u32 sourceWidth | u32 sourceHeight | f32 quality | u32 encodedBytes`
- The client decodes zstd with vendored fzstd (`client/vendor/`), since `DecompressionStream("zstd")`
  isn't in every browser yet.

## Phases

1. Bridge pipe + JS client: subscribe (with history), publisher, get, delivery queues (queueSize, maxAge),
   maxHz cap, stats, clock sync, latencyLimit, heartbeat + deadman.
2. Done: bandwidth allocation and the built-in codecs above. Transcoding runs inside the bridge's
   subscription (so only while a browser subscribes), not on separate zenoh keys; decodes and encodes
   are shared through in-process caches instead.
3. Done: the bridge as a Rust library (builder, existing zenoh session, graceful shutdown that fires
   deadmen) with external codecs in Rust through the same `Codec` trait as the built-ins, and
   `registerCodec` for their browser decoders.
4. Later: WASM degrade functions shipped from the frontend.
