# zenoh-web spec

A browser UI that views and drives a zenoh system over a squeezed network, without a heavy bridge.

## Shape

```
zenoh peers (publishers we don't control)
        │  zenoh
   zenoh-web bridge (Rust library inside an application, e.g. the zenoh-web command of zenoh-web-cli)
        │  WebRTC data channels (UDP) + one HTTP endpoint for signaling
   browser page (plain JS client, live-editable)
```

The bridge is a dumb pipe: one data channel ↔ one zenoh key expression. It never parses payloads,
except when a subscription explicitly picks one of its codecs (see "Codecs").

## JS API

```js
import { connect, Priority, registerCodec } from "./zenoh_web.ts"   // via esm.sh, or bundled: see "Client"

const z = await connect("http://robot.local:7448", {
    heartbeatHz: 5,         // 0 (default) = no heartbeat; needed for deadmen and leases
    heartbeatMisses: 3,     // silence of misses/heartbeatHz seconds = frontend gone
    token: "s3cret",        // optional: `Authorization: Bearer`, see "Auth"
    iceServers: [],         // optional: default the bridge's (GET /zenoh-web/ice), see "ICE and TURN"
    iceTransportPolicy: "relay", // optional: everything through TURN
})

const sub = z.subscribe("camera/**", {
    delivery: "latest",          // or "reliable"
    priority: Priority.DATA_LOW, // optional; defaults to the priority the message was published with
    maxAge: 500,                 // ms; drop anything older
    maxHz: 20,                   // bridge never sends a key faster than this
    bandwidthPriority: 1,        // when bandwidth is short, higher keeps more (0 gives up everything first)
    minQuality: 0.3,             // 0-1, transcoded streams only
    maxQuality: 1.0,
    qualityToHzTradeoff: 0.7,    // 0 = keep quality, drop hz; 1 = keep hz, drop quality
    codec: "ros2-image",         // optional transcoder the bridge registered (z.codecs), see "Codecs"
    compress: "zstd",            // or "none"; default: the codec's (none without one), see "Compression"
    maxBitrate: 8e6,             // video codecs: most bits/s asked for (default: the server's), see "Video"
    minResolutionScale: 0.5,     // video codecs: the picture never shrinks below this share of the source's size
    maxResolution: [1280, 720],  // video codecs: box the picture is fitted into
}, (msg) => { msg.key, msg.bytes, msg.timestamp, msg.seq, msg.decoded, msg.video, msg.mediaStream })
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
cmd.onTripped((reason) => {})     // "heartbeat" | "disconnected" | "shutdown" | "revoked"
cmd.blocked                       // why the bridge drops this publisher's puts now (another client's lease), else null
cmd.close()

const lease = await z.lease("arm", { maxSeconds: 60 })  // or { keys: ["robot/arm/**"] } for a group the server doesn't define; see "Leases"
lease.onLost((reason) => {})      // "heartbeat" | "maxSeconds" | "disconnected" | "force-expired by peer N" | "released" | "renewed"
await lease.release()
await z.expireLease("arm")        // end another client's lease (needs the grant's forceExpire)

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
- Options are checked by the bridge: an unknown name or a bad value rejects the channel (`rejected`
  event with the reason: `state` becomes `"rejected"`, `ready()` rejects).
- `bandwidthPriority` (default 1), `minQuality` (0), `maxQuality` (1), `qualityToHzTradeoff` (0.5) drive the per-frontend
  allocator ("Bandwidth allocation"); stats show them with defaults filled in. `maxBitrate`, `minResolutionScale` and
  `maxResolution` override the server's video policy ("Video"); on a non-video codec they reject the channel.
- No `latched` flag: the bridge always subscribes with zenoh-ext AdvancedSubscriber history (max 1 sample per publisher), so publishers with a cache (e.g. rmw_zenoh transient_local like tf_static) replay their last message.

## Delivery → transport mapping

| options | data channel init | bridge behavior when the channel is backed up |
|---|---|---|
| `delivery: "reliable"` | ordered, fully reliable | queue every sample |
| `delivery: "latest"` | unordered, maxRetransmits 0 | keep only the newest sample per key |
| `+ maxAge: M` | latest: unordered, maxPacketLifeTime M | also drop anything older than M |

"Backed up" = the channel's unacknowledged bytes are above its send window, or the page has not yet
consumed the larger of ~256 KB and two windows the bridge sent. The window is twice the frontend's
budget × its highest RTT of the last 10 s (at most the minimum RTT + 500 ms), between 64 KB and 4 MB
(`windowBytes` in stats); sending resumes at half of it. A fixed 64 KB capped a channel at 64 KB per
round trip, ~150 KB/s on a Wi-Fi path whose RTT swings to 400 ms, and SCTP's congestion window only
grows while data waits for it, so it stayed there too. (The page acks consumption with a 4-byte `u32 seq` message on each `sub`
channel, because browsers queue received messages for the page without limit; on lossy channels a
sender blocked only by that window sends one probe frame after 50 ms, doubling to 1 s, so a lost tail
can't wedge it). The bridge drains
its own queue on bufferedAmountLow / acks. Nothing piles up in kernel/wifi buffers or the browser,
so a slow phone sees fewer frames instead of stale ones.

Browser → zenoh puts: priority from the publisher options, congestion control Block for `"reliable"`,
Drop otherwise, express for priority ≤ INTERACTIVE_HIGH.

## Client

`client/zenoh_web.ts` (strict TypeScript, no dependencies). Browsers load it from esm.sh, which
transpiles it (`https://esm.sh/gh/jeff-hykin/zenoh-web@<commit>/client/zenoh_web.ts`), or bundle it
(`deno bundle`, the same esbuild transform; zenoh-web-cli's `deno task build` bundles it with the
dimos codecs' decoders).

## Rust client

`zenoh_web::client` (cargo feature `client`, so server-only builds don't carry reqwest) connects to a
server the way the browser client does, for programs with no browser (Deno has no
RTCPeerConnection or WebCodecs), e.g. a relay that takes a robot's best stream per camera and serves
it again through its own `Server`. It is a module of this crate rather than a separate crate because
it shares the wire code: the media engine and interceptors, the frame format, `fields::parse`.

- `Client::connect(url, ClientOptions { token, ice_servers, relay_only, heartbeat_hz, heartbeat_misses })`:
  the bridge's ICE servers from `GET /zenoh-web/ice` unless given, the same non-trickle `POST /offer`
  (both with `Authorization: Bearer <token>` when set; a 401 fails the connect), `control` and
  heartbeat channels, `codecs`, and 5 clock pings before returning; then a ping a second (clock
  sync, `Degraded` when one fails). `list_topics`, `get`, `stats`, `codecs`, `clock_offset_ms`,
  `rtt_ms`, `state`, `closed()`, `close()`. Reconnecting is the caller's job: when `closed()`
  resolves, connect and subscribe again.
- `Client::connect_zenoh(&session, name, options)`: the same, signalling over zenoh (see "Signalling over zenoh").
- `subscribe(key, SubscribeOptions)` takes the browser's options (camelCase on the wire, unset ones
  left out), resolves once the bridge accepted the channel (or fails with its reason), and yields
  `Message`s (`recv()`, or as a `Stream`):
  - `Data`: raw or codec bytes, reassembled and zstd-decompressed; fields codecs add the parsed `fields`;
  - `Video`: an access unit (H.264 Annex B, VP8/VP9 frame, AV1 OBUs) with its format, keyframe flag
    and RTP timestamp, depacketized from the track (reordered and retransmitted packets included);
    after a lost frame the client sends a PLI and skips frames until a keyframe, so what it yields
    always decodes. `request_keyframe()` sends a PLI;
  - `VideoInfo`: the 28-byte metadata frame (key, size, quality), which travels on the data channel
    apart from its frame;
  - `Audio`: Opus packets.
  Messages are acked once queued (64 per subscription), so a slow consumer slows the bridge instead
  of piling up. Video and audio renegotiate a recvonly transceiver per codec and reuse a dropped
  subscription's, as the browser does.
- `publish(key, PublisherOptions)` → `put` / `put_at(bytes, timestamp_ms)` (`latest` drops a put
  while 64 KiB are unsent), `repeat_ms`, `set_deadman`, `clear_deadman`, `tripped()`, `wait_tripped()`.
  `blocked()` says why the bridge drops its puts (another client's lease). `pause_heartbeat(true)`
  stops beats (to test deadmen).
- `lease(group, keys, max_seconds)` → `Lease` (`lost()`, `wait_lost()`, `release()`), `expire_lease(group)`;
  a `closed` event (revoked token) makes the client `Lost`.
- Nothing is decoded: `examples/relay_sketch.rs` decodes H.264 with openh264 and serves the pictures
  through a second server's video codec.

## Server (Rust library)

`Server::builder()` takes the zenoh config (`zenoh_config`, `zenoh_config_file`, repeatable `connect`)
or an existing `session` (never closed by the server), `serve_dir`, `max_bandwidth_bytes_per_sec`,
`bandwidth_target_fraction`, `codec`s, `authorize`, `lease_group`, `ice_servers`, `turn_secret`, `ice_servers_fn`, `cloudflare_turn`, `udp_ports`,
`video_encoder` (a factory for every video codec without its own encoder, e.g. a hardware one from
zenoh-dimos-codecs' encoders) and `video_policy` (see "Video"); `build().await` validates them and opens the session. Then
`bind(addr)` serves on a background task (`RunningServer::local_addr`, `shutdown()`), `serve(addr)` /
`serve_with_shutdown(addr, signal)` serve in place, or `router()` returns the axum routes (`POST
/offer`, `GET /zenoh-web/health`, `GET /zenoh-web/ice`, static files) for the host's own HTTP server.
`Server::revoke(token)` closes that token's connections (see "Auth"). For relays and monitoring:
`subscriptions()` (every open subscription's key expression and codec), `leases()` (group and keys of each held
lease), `changes()` (a `watch` counter bumped when either changes) and `expire_lease(group, reason)` (ends a lease
as a force-expiry does, the holder told `reason`). Shutdown fires every frontend's deadmen
(reason `"shutdown"`), closes the browser connections, then the session if the server opened it.

zenoh is pinned to 1.6.2: 1.7.0 through 1.10.1 deadlock when the admin space answers a query while a
declaration waits for the routing tables (fixed upstream on branch `bugfix/routing-deadlock`, not
released). The webrtc-rs fixes the bridge relies on (see "Delivery → transport mapping", "Large
messages") are published as renamed crates (`zenoh-web-webrtc` → `zenoh-web-rtc` →
`zenoh-web-rtc-datachannel`, `zenoh-web-rtc-sctp`; github.com/jeff-hykin/webrtc-rs-zenoh-web), so
crates that depend on zenoh-web build the fixed code.

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
degradation). There is no auto-detection and none is built in: the application embedding the bridge
registers codecs by name (`ServerBuilder::codec`; a name registered twice fails the build), e.g. the
robotics codecs of [zenoh-dimos-codecs](https://github.com/jeff-hykin/zenoh-dimos-codecs). The core
knows no message types: its only special paths are video and audio tracks. The client fetches the
registry on connect (`control` op `codecs` → `{codecs: [{name, output}]}`, output `"video"`,
`"audio"`, `"fields"` or `"data"`, as `z.codecs`), which is how a page learns which codecs need a
track; the bridge refuses an unknown name (`rejected` event, listing the known names).

Every codec implements one Rust trait (`zenoh_web::Codec`):

- `name()`, and `output()`: **video**, **audio**, or data (**fields** or **data**).
- `decode(sample)`: the sample's key, payload and zenoh encoding → a decoded frame: a picture (packed
  RGB8 or planar I420, any size) for the default video encoder, anything a codec's own video encoder
  takes, PCM for audio, any value for data codecs.
- fields and data codecs: `encode(frame, quality)` → the bytes sent on the data channel (quality
  0..1, from the allocator). A **fields** codec builds them with `zenoh_web::Fields` (see "Fields") and
  the client decodes them into `msg.decoded` by itself. A **data** codec uses its own format, which the
  page decodes with the decoder registered for that name (`registerCodec(name, decoder)` →
  `msg.decoded`); without one it gets `msg.bytes` (and a warning). A registered decoder also overrides
  a fields codec's automatic decoding.
- `key_prefix()`: where the codec's samples live (default none). With `Some(prefix)` a subscription to `key` reads
  zenoh key `<prefix>/<key>` and its messages carry keys without the prefix, so a codec's input can sit apart from the
  raw topic: zenoh-web-relay puts its decoded frames under `@relay/<codec>/<key>`, which raw subscribers to `key`
  (and `**`, which never matches a `@` chunk) don't see.
- `default_compress()`: compression for the codec's messages when the subscription doesn't set
  `compress` (default none; zenoh-dimos-codecs' depth and point clouds use zstd).
- video codecs: optionally `video_encoder()` → the codec's own `VideoEncoder` per encode session; default the server's
  (`ServerBuilder::video_encoder`), else the bridge's software H.264 (see "Video encoders"); its frames go on the
  subscription's video track (see "Video"); the page needs no codec code. They require `delivery: "latest"`.
- audio codecs: PCM, which the bridge encodes to Opus on an audio track (see "Audio").
- `estimated_bytes(payloadBytes, quality)`: optional cost model for data codecs (bytes per message),
  the allocator's prior until sizes are measured and its shape between measured qualities (default:
  10–100 % of the payload, linear in quality). Video is priced by the video policy (see "Video").

Video: scaling is a box filter, RGB → I420 integer BT.601 limited range, tagged in the stream (BT.601 matrix, BT.709
primaries and transfer). Chrome reads untagged HD video as BT.709, which cost ~2.7 dB of PSNR; tagged BT.601 measured as
good as BT.709 through openh264 and ~0.5 dB better through VideoToolbox. H.264 openh264 by default.
- A video subscription decodes its next frame while it encodes the current one (two blocking-pool
  tasks), so the rate is set by the slower stage, not their sum.
- CPU governor (per encode session): when scale+encode takes more than 85% of the frame interval, the ceiling on the
  picture's scale drops 0.1 below the scale in use (never below `minResolutionScale`), at most once a second; it rises
  0.1 when the encode cost predicted at the next step (∝ pixels) fits in 60% of the interval and 5 s have passed since
  the last step (each resolution change restarts the encoder with a keyframe, so the ceiling must not flap). It only
  lowers the size the bitrate policy picked, never the bitrate or the frame rate, so the two never fight; a hardware
  encoder rarely moves it. Decode time does not move it (a slow decode caps the rate at any resolution). Stats:
  `decodeMs`, `encodeMs`, `cpuScaleCap`.

Work happens lazily and on send: only messages the pacing/queues let through are transcoded, on
tokio's blocking pool. Work is shared across frontends through two small caches per bridge: decoded
frames keyed by (codec, key + payload hash), and data-channel encodes keyed by (codec, quality in
1/1000 steps, key + payload hash, compression). Identical requests compute once (`encodes` vs `sharedEncodes` in
stats). Video encodes are shared through encode sessions: the viewers of one stream (codec, key and video policy)
whose grants are within 1.25× of each other share one encoder, which runs at the lowest of their grants. Every member
sends every frame of its session in order (so one reference chain serves them all), starting from a keyframe; a member
whose grant moves out of range moves to another session (a keyframe), a member that fell behind the session's last 8
frames waits for a keyframe. A sample is encoded once per session: whichever member picks it first encodes it, the
others send the result (`sharedEncodes`). Each subscription keeps its own Opus encoder.

### Fields

The format of **fields** codecs: named numbers, text and arrays the client turns into a plain object.
Little endian: `u8 version=1 | u8 fieldCount`, then per field `u8 nameLen | name utf8 | u8 dtype |
u8 components (1..4) | u8 flags | u32 count | [scaled: f64 offset[components] | f64 scale[components]] |
zero padding | count × components values`.
- dtype: 0 u8, 1 i8, 2 u16, 3 i16, 4 u32, 5 i32, 6 f32, 7 f64, 8 utf8 (text; `count` bytes).
- flags: bit0 **scaled** (integer dtypes only), bit1 **scalar** (one value).
- The padding puts the values at a multiple of the dtype's size from the message start, so the client
  views them in place (it copies a message once if its buffer is misaligned).
- Decoded: a scalar is a `number`, text a `string`, a scaled field a `Float32Array` of
  `offset[c] + value × scale[c]` (c = index mod components), anything else the dtype's typed array
  (`Uint8Array` … `Float64Array`), components interleaved.
- Rust: `Fields::new().scalar("width", w).array("data", &values).vectors("origin", 3, &xyz)
  .scaled("positions", &origin, &scale, &quantized).text("encoding", "16UC1").build()`;
  `zenoh_web::fields::parse` reads one back.

### Compression

Subscribe option `compress`: `"zstd"` or `"none"`; unset, the codec's `default_compress()` (none
without a codec). It applies to raw topics and to fields and data codecs: the bridge compresses each
message (zstd level 3, on tokio's blocking pool, shared across frontends with the encode) and the
client decompresses it before decoding or handing it on, so `msg.bytes` is always uncompressed. A
message zstd would not shrink is sent as is; each frame's `flags` says which. The price model learns
the compressed sizes from what is sent. Video codecs are already compressed (H.264): `compress: "zstd"`
on one is rejected, `"none"` accepted, their default ignored.

### Video encoders

`zenoh_web::VideoEncoder` turns a video codec's decoded frames into one WebRTC codec's frames:
`format()` declares the codec (`VideoFormat::H264`, `Vp8`, `Vp9` or `Av1`) and
`encode(frame, target)` returns an `EncodedVideo` (bitstream, size, keyframe) or `None` while a
pipelined encoder has nothing out yet. `target` carries the granted bitrate (encode at it) and frame rate, the even
output size the video policy and CPU governor picked, the allocator's quality, and whether this frame must be a
keyframe (a viewer joined or sent PLI/FIR). `VideoTarget::new(width, height, bitrate, fps)` makes one to try an
encoder. The bridge negotiates the format (it offers all four),
packetizes (webrtc-rs payloaders), paces (GCC), measures the frames for the allocator and sends the
per-frame metadata. `H264Encoder` (openh264, constrained baseline) is the default.

A hardware encoder plugs in for every video codec as `ServerBuilder::video_encoder(factory)`, or for one codec as
its `video_encoder()` (which wins). [zenoh-dimos-codecs](https://github.com/jeff-hykin/zenoh-dimos-codecs)' `encoders` module has
VideoToolbox (macOS) and GStreamer (`nvv4l2h264enc` on a Jetson, `nvh264enc`, `vah264enc` / `vaapih264enc`, loaded at
runtime) backends, probed by encoding a test frame and wrapped in a fallback to openh264; zenoh-web-cli uses it
(`--video-encoder auto|software|videotoolbox|gstreamer`). Any encoder is fed from
`VideoImage::to_i420(target.width, target.height)`
(or the codec's own frames, e.g. GPU buffers decoded into `DecodedFrame::Data`, which the encoder
downcasts), returning access units. A camera that already sends H.264 can be passed through the
same way: the codec's decode keeps the access unit and its encoder returns it (keyframes then
come from the source). zenoh-web-cli's `examples/custom_codec.rs` has an AV1 encoder (rav1e) that
`test/custom_codec.js` shows in Chrome.

### Audio

An audio codec decodes each sample to `AudioPcm` (interleaved i16, 8/12/16/24/48 kHz, mono or
stereo). The bridge encodes it with libopus (`unsafe-libopus`, libopus transpiled to Rust, so it
builds for every target without a C toolchain) in 20 ms packets, carrying any remainder into the
next sample, and writes them to an Opus track (the client adds a recvonly audio transceiver and
renegotiates with the codec's name, as for video). The browser's jitter buffer smooths arrival;
`sub.mediaStream` plays in an `<audio>` element. Audio streams are reserved at their measured rate
like reliable ones (never paced or thinned). Each sample also sends an empty frame on the `sub`
channel, so the callback runs per message.

Microphone (browser → zenoh), not implemented, fits without redesign:
- the page needs a secure context for `getUserMedia` (HTTPS, or `http://localhost`), so a robot's
  bridge would be served over TLS (a reverse proxy, or `ServerBuilder` behind one);
- the client adds a `sendonly` audio transceiver with the mic track and renegotiates
  (`{op: "renegotiate", publish: "<codec>", sdp}`), and opens a `pub` channel naming that mid, as a
  video subscription names its track's;
- the bridge's `on_track` reads the Opus RTP, decodes it with libopus (`opus_decode`) to PCM and
  hands it to the codec's inverse of `decode` (e.g. `encode_pcm(&AudioPcm) -> Vec<u8>`, a new
  `Codec` method with a default error), whose bytes are put on the key like any `pub` channel's,
  with the same latency limit and deadman rules. Only the Opus decode and the `on_track` handler are
  new; negotiation, codecs and publishing are the existing paths.

### Video

The client adds a recvonly video (or audio) transceiver and renegotiates over `control`
(`{op: "renegotiate", codec, sdp}` → `{sdp, mid}`); the bridge adds a track of that codec's format
(H.264 is constrained baseline, `profile-level-id=42e01f`; VP9 profile 0; Opus 48 kHz) that pairs
with the new m-line, then the `sub` channel's label names that `mid` (the bridge refuses a track of
another format). Renegotiations run one at a time. A closed subscription's transceiver (and the
bridge's track) is reused by the next one of the same codec instead of renegotiating again.
Each video frame also sends a 28-byte metadata frame on the `sub` channel (`msg.video`).
Bitrate and size (the video policy: `ServerBuilder::video_policy`, overridden per subscription):
- A stream asks the allocator for at most `maxBitrate` bits/s (default: `max_bits_per_pixel` × the source's pixels ×
  its rate, 0.3: ~17 Mbit/s for 720p60, ~1.4 Mbit/s for 320x240 at 60 Hz). The encoder runs at what the allocator
  actually grants.
- The picture keeps the source's size (fitted into `maxResolution`) unless the grant would leave fewer than
  `min_bits_per_pixel` (0.05) there; then it shrinks just enough, never below `minResolutionScale` (0.25). Below
  ~0.03 bit/pixel openh264 overshoots its target even at its coarsest quantizer, while above it a full-size picture
  beat every smaller one at the same bitrate on the bench scene (after upscaling).
- Quality, for video, is the share of the most bits per frame: 0 is the smallest picture at the bit-per-pixel floor,
  1 is `maxBitrate` / rate, linear between. The allocator trades it against Hz as for other codecs.
- openh264 applies bitrate changes in place (no keyframe); a size change restarts it with one.
Keyframes: the first frame each viewer gets, on PLI/FIR from the browser (`keyframeRequests`), and every 3 s.
Send-side congestion control: TWCC feedback into GCC (webrtc-rs interceptors), whose target feeds the
allocator.
Inbound PLI/FIR reach the encoder through an interceptor that marks them for the application (the
webrtc-rs chain otherwise ends every RTCP packet). Every RTP packet carries the playout-delay extension with min = max = 0, so Chrome shows each frame
as soon as it is decoded instead of holding it in its jitter buffer for smooth pacing (the client
also sets `jitterBufferTarget = 0`). Locally that took receive -> shown from ~22 ms to ~1 ms
(`test/video_latency.js`); on a jittery link, bursts of frames are shown as they come.

## Bandwidth allocation

All of a frontend's streams share one path (wifi queue, UDP, one SCTP association), so throttling
"on average" isn't enough: a burst or an overshoot builds a queue that every stream waits in. The
bridge keeps the path's queues short and lets urgent streams skip what queue remains:

- **Strict-priority tier.** A subscription whose priority (its `priority` option, else the published
  priority of its samples) is INTERACTIVE_HIGH (2) or more urgent bypasses allocation and pacing: its
  measured rate is reserved off the top, and while it sends a message no bulk chunk starts.
- **Target fraction (headroom).** The allocator hands out `bandwidth_target_fraction` (default 0.75)
  of the estimate, so bulk traffic runs below the path's capacity and its queue stays near empty.
- **Pacing.** Bulk streams send through a per-stream token bucket at their granted rate × 1.25, in
  chunks of 4 ms of the frontend's budget (4–64 KiB). A per-frontend gate admits a bulk chunk only
  while the bulk bytes outstanding in SCTP are under `budget × (min RTT + 5 ms)`, when the frontend has a strict stream
  to protect (none: no in-flight limit, `bulkInflightLimit: null` in stats; at least two chunks,
  about one bandwidth-delay product), counting a chunk the moment it is admitted so senders woken
  together can't burst. A strict message waits behind at most the bulk already handed to SCTP, and
  bulk never bursts whole messages into the link.
- **Delay trigger.** The browser reports its RTT with every clock-sync sample (each heartbeat, else
  the 1 s control ping: configure `heartbeatHz` for a fast trigger). The queue delay of a 250 ms
  interval is its smallest RTT minus the 30 s minimum. When it exceeds `5 ms + 2 × median` of the queue delays of the
  last 30 s (`delayThresholdMs` in stats) for two intervals in a row, a queue is
  standing on the path (a Wi-Fi or VPN path whose RTT swings by tens of ms with no load raises its
  own threshold; a lone spike is ignored): the data
  estimate drops 15% at once and probing pauses 1 s; then it probes up 10% per interval below 90% of
  the level that caused the queue and 2% above it, and 10% again once that level is 5 s old (on Wi-Fi
  one early stall had held probing at 2% for good; before any congestion: 50% per interval, a slow
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
   measured rate (at most 15% down per step: a long round trip blocks the first intervals before
   the window knows the RTT); otherwise, while streams want more, probe up (start
   1 MB/s). Video: GCC's target bitrate (TWCC feedback, bounded at 2 Gbit/s: a 50 Mbit/s bound had capped 8 HD cameras on
   an idle link), counted while a video track is in use; video
   tracks are paced by the GCC pacer (at 2.5 × the GCC target, as libwebrtc paces), not the bulk gate. Budget = min(`--max-bandwidth-bytes-per-sec`
   if set, target fraction × (data estimate + video estimate)).
   GCC starts at 8 Mb/s, the data estimate's starting point. Video streams together get at most
   the video estimate (GCC's target): their bytes leave through
   the GCC-paced track, so data-channel capacity is no use to them (granting it made encoders
   outrun the pacer and queue); data streams are then allocated what remains.
2. **Demand.** Each subscription wants `price(maxQuality) × Hz`, Hz being each key's measured source
   rate capped by `maxHz`, summed over its keys. Price = bytes per message: measured for raw streams
   and data-channel codecs (per quality, scaled by the codec's `estimated_bytes` between measured
   qualities, which is also the prior before anything was measured), from the video policy for video
   (`maxBitrate` / rate at quality 1, the floor picture at 0). Strict-priority and reliable streams can't drop messages: they are reserved at
   their measured rate and never shrunk.
3. **Shrink.** If the rest want more than the budget left, they shrink like CSS flex items: the deficit
   is split in proportion to `demand / bandwidthPriority` (so 10 / 10 / 0.1 means the 0.1 stream takes
   ~100x the cut); a stream that reaches 0 stops there and the rest shrink further. Priority-0 streams
   give up everything before any other stream gives up anything.
4. **Quality vs Hz.** A transcoded stream granted fraction r of its demand shrinks its message size by
   `r^t` (choosing the best quality among the bounds and 0.1 steps that fits) and its Hz by the rest,
   `t = qualityToHzTradeoff`: 0 keeps quality and drops Hz, 1 keeps Hz and drops quality. When quality
   hits `minQuality`, Hz gives.
5. **Apply.** Each key's send interval becomes `1 / (its wanted Hz × granted fraction)` (at least
   0.05 Hz); data transcoders encode at the granted quality, video encoders at the granted bitrate.

Stats: each subscription's `allocation` (`demandBytesPerSec`,
`budgetBytesPerSec`, `hz`, `hzFraction`, `quality`, `constrained`) and the frontend's `bandwidth`
(`dataEstimateBytesPerSec`, `videoEstimateBytesPerSec`, `capBytesPerSec`, `budgetBytesPerSec`,
`targetFraction`, `reservedBytesPerSec`, `bulkChunkBytes`, `queueDelayMs`, `minRttMs`, `delayEvents`,
`demandBytesPerSec`, `sentBytesPerSec`, `networkBlockedFraction`, `constrained`). Per stream,
`pacedMs` is time spent waiting on the token bucket and the gate.

## Topic enumeration

`z.listTopics(filter = "**", { probeMs = 600 })` merges what the bridge can see, each key tagged with its sources:

| source | what it sees |
|---|---|
| `token` | liveliness tokens (`liveliness().get`) |
| `advancedPublisher` | zenoh-ext AdvancedPublishers with publisher_detection (their `@adv/pub` token) |
| `sample` | keys that published during a `probeMs` subscription on `filter`, declared or not |

`probeMs: 0` skips the `sample` probe entirely (no subscription on `filter`), so a publisher that
only sends while it has a matching subscriber (a zenoh matching listener) isn't woken by a listing.

It can't see plain publishers, subscribers and queryables that hold no token (a publisher that puts
during the probe is still seen): zenoh's admin space would list the bridge's routing tables, but
answering it deadlocks zenoh 1.7.0 through 1.10.1, and remote publishers only reach it once interest
is declared. rmw_zenoh liveliness tokens (`@ros2_lv/...`) only appear when the filter names them;
they're returned raw.

## Access control

The `access_control` section of the bridge's zenoh config applies to browsers' puts, subscriptions and
queries like to any other traffic of its session: zenoh drops what it denies, silently. "Auth" is the
per-browser layer on top, with reasons.

## Auth

- `ServerBuilder::authorize(|token: Option<&str>, headers| -> Result<Grant, String>)` runs on every
  `POST /offer` and `GET /zenoh-web/ice`, with the `Authorization: Bearer <token>` the client sends
  (`connect(url, { token })`). `Err(reason)` answers HTTP 401 with the reason; the client then stops
  reconnecting (`connect` rejects with `bridge refused the token: <reason>`, state `"lost"`). Without a
  hook every connection gets `Grant::all()`, the behaviour before auth existed.
- A `Grant` (serde, camelCase) holds key-expression lists: `subscribe`, `publish` (and deadmen), `query`
  (`get`), `listTopics` (which keys a listing shows), and the lease rights `leaseGroups` (names, `"*"` =
  any), `maxLeaseSecs` and `forceExpire`. A request is allowed when one of the list's expressions
  includes its key (zenoh's `includes`: `robot/**` includes `robot/arm/cmd` and `robot/arm/**`, not
  `**`). A refused sub or pub channel is `rejected` with `not authorized to subscribe "<key>"`; a
  refused `get` fails the same way; `listTopics` leaves the keys out.
- `Server::revoke(token)` closes every live connection made with that token (deadmen fire with reason
  `"revoked"`, leases end); whether it can come back is the hook's call, since it runs on every offer.
- Token issuance is the host's business (no store, no JWT in core). zenoh-web-cli's `--auth-file` is a
  small example: a json5 map of token → `read` / `write` / `lease` / a grant object, re-read when it
  changes, revoking tokens removed or changed there.

## Leases

- A lease is one client's exclusive right to publish on a group of key expressions, among the clients
  of this bridge. Groups are defined on the server (`ServerBuilder::lease_group(name, keys)`, or the
  cli's auth file), or by the client (`z.lease(group, { keys })`) when the server doesn't define that
  name. The grant must allow the group name, and `publish` must include every key of the group.
- `z.lease(group, { maxSeconds })` needs a heartbeat (`connect(url, { heartbeatHz })`) and returns a
  `Lease` (`keys`, `expiresInMs`, `release()`, `onLost`). Leasing a group you hold renews it (the old
  handle is lost with `"renewed"`). A group whose keys intersect another client's lease is refused
  (`conflicts with "<group>", held by another client`). `maxSeconds` is capped by `maxLeaseSecs`.
- While held, every other client's puts on keys intersecting the lease are dropped by the bridge
  (`rejectedLeased` in that publisher's stats). The publisher is told why on its own channel: the bridge
  sends `{"blocked": "<key> is leased by another client (group \"<group>\")"}` when its first put is
  dropped and `{"blocked": null}` when puts go through again; the client keeps it in `publisher.blocked`.
  Other clients' deadmen on those keys don't fire either.
- It ends when the holder's heartbeat misses (`"heartbeat"`, the same deadline as deadmen), at
  `maxSeconds`, when the holder disconnects or is revoked, on `release()`, or when a client whose grant
  has `forceExpire` calls `z.expireLease(group)`. The bridge tells the holder with
  `{event: "leaseLost", group, reason}` on `control`, and the client calls `onLost`. Leases are not
  re-taken after a reconnect.
- **It binds only clients of this bridge.** Native zenoh publishers, and other bridges, publish
  regardless; zenoh's `access_control` is the tool for those.

## ICE and TURN

- `ServerBuilder::ice_servers([IceServer { urls, username, credential }])` configures the bridge's own
  side of every connection, and `GET /zenoh-web/ice` (authorized like an offer) hands the same list to
  browsers as `{"iceServers": [...]}`. The client fetches it on every (re)connect unless `connect` was
  given `iceServers` (an older bridge's 404 means none), and exposes it as `z.iceServers`. A reply with
  `"iceTransportPolicy": "relay"` (e.g. from a host app serving its own `/zenoh-web/ice`) makes the connection
  relay-only unless `connect` was given `iceTransportPolicy` (the Rust client: `relay_only` or the reply).
- `turn_secret(secret, ttl)`: coturn's TURN REST API (`use-auth-secret`, `static-auth-secret`). Every
  TURN entry without a username gets credentials minted per connection and per caller: username
  `"<unix expiry>:<user>"` (`bridge` for its own side, `browser` for `/zenoh-web/ice`), credential
  `base64(HMAC-SHA1(secret, username))`. The ttl must outlast a connection (TURN refreshes use them).
  `zenoh_web::turn_credentials` computes them.
- `ice_servers_fn(hook)`: `async fn(IceRequest { side, token }) -> Result<Vec<IceServer>>`, called for
  every `/zenoh-web/ice` (and zenoh `ice` query; side `Browser`) and every offer the bridge answers (side
  `Bridge`), with the connection's bearer token. Its servers come after the static (and minted) ones; an
  error, or no answer within `ICE_HOOK_TIMEOUT` (5 s), is logged and that end gets only the static ones. TURN
  entries it returns without a username and credential are dropped (a browser refuses the whole connection over one).
- `cloudflare_turn(CloudflareTurn)` (feature `cloudflare`) is that hook over Cloudflare's API: `POST
  https://rtc.live.cloudflare.com/v1/turn/keys/<key id>/credentials/generate-ice-servers`, `Authorization:
  Bearer <api token>`, body `{"ttl": seconds}`; the reply's `iceServers` (a list, or the older `generate`
  endpoint's single object) minus port-53 URLs, which browsers block. By default one set is shared and minted
  again once half its ttl has passed (every connection gets at least ttl/2); `per_connection(true)` mints a
  set per call.
- `udp_ports(low..=high)`: each connection's WebRTC UDP sockets bind a port from this range (one port
  per connection, on every interface), so a firewall only opens that range; the range bounds the
  number of simultaneous browsers. webrtc-rs binds sockets per connection, so there is no single
  shared mux port.

## Signalling over zenoh

`ServerBuilder::zenoh_signalling(name)` also answers signalling on two zenoh queryables of the server's session,
twins of the HTTP routes: `zenoh-web/<name>/offer` (query payload `{"token"?, "offer": <SDP>}`, reply the answer
SDP) and `zenoh-web/<name>/ice` (payload `{"token"?}`, reply `{"iceServers": [...]}`). Authorization is the same
hook, with headers holding only `Authorization: Bearer <token>`; a refusal is an error reply `{"status": 401,
"error": reason}` (400/500 for a bad request or a failed answer). `name` is one key chunk.

It is for a machine with no inbound ports: its zenoh dials out to a router (e.g. zenoh-web-relay's), and a
client on that router's side calls `zenoh_web::client::Client::connect_zenoh(&session, name, options)`, which
queries those keys instead of `POST /offer`. Only signalling uses the zenoh link: the media is the usual WebRTC
connection, and since the server answers with its candidates and also sends connectivity checks to the client's,
the server's side opens the UDP path (a client on a public address needs no STUN for the server to reach it;
behind a NAT on both sides, a TURN server). The browser client signals over HTTP only.

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

- Signaling: `POST /offer` with the browser's SDP offer (non-trickle), returns the answer (401 with a reason when the authorize hook refuses); or the zenoh queryables of "Signalling over zenoh".
- `GET /zenoh-web/ice` returns `{"iceServers": [{urls, username?, credential?}]}`, see "ICE and TURN".
- `GET /zenoh-web/health` returns `{"service": "zenoh-web", "version": "<crate version>"}` (detecting a running server).
- The bridge can also serve a static directory (`--serve <dir>`, `ServerBuilder::serve_dir`) so the UI is live-editable on disk.
- Each subscribe/publisher is its own data channel. Its label is JSON: `{"type":"sub"|"pub", "key":..., "id":n, "opts":{...}}`.
  The heartbeat channel is `{"type":"heartbeat", "opts":{"hz":..., "misses":...}}`.
- One extra channel labeled `control` carries JSON request/response (`get`, `listTopics`, `stats`, `ping`,
  `codecs`, `renegotiate`, `setDeadman`, `clearDeadman`, `lease {group, keys?, maxSeconds?}`,
  `releaseLease {group}`, `expireLease {group}`) and events: `accepted` / `rejected` (per sub/pub
  channel, by label id), `tripped` and `leaseLost {group, reason}`.
- Bridge → browser on a `pub` channel: `{"blocked": reason | null}` (JSON text), see "Leases".
- Bridge → browser frame: `u16 keyLen | key utf8 | f64 timestampMs | u32 seq | u32 frameId | u32 chunkIndex | u32 chunkCount | u8 flags | chunk`,
  little endian. `seq` numbers messages per channel, `frameId` numbers frames; the page acks the highest
  `frameId` it has processed with a 4-byte `u32` message on the same channel. `flags` bit0: the
  message (its chunks joined) is zstd-compressed.
- All chunks of one message have the same size, at most 64 KiB (bulk streams use smaller ones, see
  "Bandwidth allocation").
- Browser → bridge put: `f64 sentAtMs (browser clock) | payload`, little endian.
- Heartbeat: browser sends `{"t0", "offsetMs", "rttMs"}` (JSON), bridge answers `{"t0", "t1", "t2"}`.
- Video `sub` label: adds `"mid"`. Video metadata frame (little endian): `u8 version=1 | u8 flags
  (bit0 keyframe) | u16 0 | u32 width | u32 height | u32 sourceWidth | u32 sourceHeight | f32 quality |
  u32 encodedBytes`. A fields codec's payload is "Fields", a data codec's its own format.
