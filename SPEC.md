# zenoh-web spec

A browser UI that views and drives a zenoh system over a squeezed network, without a heavy bridge.

## Shape

```
zenoh peers (publishers we don't control)
        │  zenoh
   zenoh-web bridge (Rust, one process)
        │  WebRTC data channels (UDP) + one HTTP endpoint for signaling
   browser page (plain JS client, live-editable)
```

The bridge is a dumb pipe: one data channel ↔ one zenoh key expression. It never parses payloads,
except for the hardcoded transcoders (point clouds, video) described in phase 2.

## JS API

```js
import { connect, Priority } from "./zenoh_web.js"

const z = await connect("http://robot.local:7448", {
    heartbeatHz: 5,         // 0 (default) = no heartbeat; needed for deadmen
    heartbeatMisses: 3,     // silence of misses/heartbeatHz seconds = frontend gone
})

const sub = z.subscribe("camera/**", {
    delivery: "latest",          // or "reliable"
    priority: Priority.DATA_LOW, // optional; defaults to the priority the message was published with
    queueSize: 1,                // pending samples per key; default 1 for latest, Infinity for reliable
    maxAge: 500,                 // ms; drop anything older
    maxHz: 20,                   // bridge never sends a key faster than this
    bandwidthPriority: 1,        // phase 2: flex-shrink weight when bandwidth is short
    dangerousMinHz: 1,           // phase 2: allocation floor (may starve others)
    minQuality: 0.3,             // phase 2: 0-1, transcoded types only
    maxQuality: 1.0,             // phase 2
    qualityToHzTradeoff: 0.7,    // phase 2: 0 = keep quality, drop hz; 1 = keep hz, drop quality
}, (msg) => { msg.key, msg.bytes, msg.timestamp, msg.seq })
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
cmd.state                         // "connecting" | "open" | "tripped" | "closed"
cmd.onTripped((reason) => {})     // "heartbeat" | "disconnected" | "shutdown"
cmd.close()

const replies = await z.get("some/key/**")   // zenoh query, returns [{ key, bytes }]

z.stats          // per-key: received, dropped, backlogBytes, rttMs, bridge (incl. normalized options)
z.clockOffsetMs  // bridge clock - browser clock
z.rttMs
z.onState(fn)    // "connecting" | "connected" | "degraded" | "lost"
```

- `Priority` mirrors zenoh / zenoh-ts: REAL_TIME=1, INTERACTIVE_HIGH=2, INTERACTIVE_LOW=3, DATA_HIGH=4, DATA=5, DATA_LOW=6, BACKGROUND=7 (lower = more important).
- Options are validated in the client (unknown names and out-of-range values throw) and again in the bridge.
- `bandwidthPriority`, `dangerousMinHz`, `minQuality`, `maxQuality`, `qualityToHzTradeoff` are accepted,
  validated, carried to the bridge and shown (with defaults filled in) in stats, but have **no effect
  until phase 2** (bandwidth allocation and transcoding).
- No `latched` flag: the bridge always subscribes with zenoh-ext AdvancedSubscriber history (max 1 sample per publisher), so publishers with a cache (e.g. rmw_zenoh transient_local like tf_static) replay their last message.

## Delivery → transport mapping

| options | data channel init | bridge behavior when the channel is backed up |
|---|---|---|
| `delivery: "reliable"` | ordered, fully reliable | queue `queueSize` per key (default unbounded) |
| `delivery: "latest"` | unordered, maxRetransmits 0 | keep at most `queueSize` (default 1) per key, drop oldest |
| `+ maxAge: M` (latest) | unordered, maxPacketLifeTime M | also drop anything older than M |

"Backed up" = the channel's unacknowledged bytes are above ~64 KB, or the page has not yet consumed
~256 KB the bridge sent (the client acks consumption with a 4-byte `u32 seq` message on each `sub`
channel, because browsers queue received messages for the page without limit). The bridge drains
its own queue on bufferedAmountLow / acks. Nothing piles up in kernel/wifi buffers or the browser,
so a slow phone sees fewer frames instead of stale ones.

Browser → zenoh puts: priority from the publisher options, congestion control Block for `"reliable"`,
Drop otherwise, express for priority ≤ INTERACTIVE_HIGH.

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
  closes, connection fails, or stays disconnected 15 s), or when the bridge gets SIGINT/SIGTERM, the
  bridge publishes every armed deadman of that frontend **once**, at REAL_TIME with CongestionControl
  Block (reliable), before anything else on that stream.
- No re-arm: the stream is then *tripped*. The bridge rejects further puts on it (counted as
  `rejectedTripped`), notifies the client over `control` (`{event:"tripped", id, reason}`), and the
  client publisher goes to state `"tripped"`, calls `onTripped` listeners and throws on `put`. The
  frontend must create a new publisher.
- Browsers throttle timers in background tabs (to ≥1 s), so pick `heartbeatMisses / heartbeatHz` well above 1 s.

## Wire format

- Signaling: `POST /offer` with the browser's SDP offer (non-trickle), returns the answer.
- The bridge can also serve a static directory (`--serve <dir>`) so the UI is live-editable on disk.
- Each subscribe/publisher is its own data channel. Its label is JSON: `{"type":"sub"|"pub", "key":..., "id":n, "opts":{...}}`.
  The heartbeat channel is `{"type":"heartbeat", "opts":{"hz":..., "misses":...}}`.
- One extra channel labeled `control` carries JSON request/response (`get`, `stats`, `ping`, `setDeadman`, `clearDeadman`) and `tripped` events.
- Bridge → browser frame: `u16 keyLen | key utf8 | f64 timestampMs | u32 seq | payload`, little endian.
- Browser → bridge put: `f64 sentAtMs (browser clock) | payload`, little endian.
- Heartbeat: browser sends `{"t0", "offsetMs", "rttMs"}` (JSON), bridge answers `{"t0", "t1", "t2"}`.

## Phases

1. Bridge pipe + JS client: subscribe (with history), publisher, get, delivery queues (queueSize, maxAge),
   maxHz cap, stats, clock sync, latencyLimit, heartbeat + deadman.
2. Bandwidth allocation (`bandwidthPriority` as flex-shrink weight, `dangerousMinHz` floor, shrink by
   priority) + hardcoded transcoders for point clouds and video (`minQuality`/`maxQuality` +
   `qualityToHzTradeoff`). Lazy: a transcoder only runs while a web subscriber wants it
   (zenoh matching listener). Transcoded variants publish on quality-rounded keys so clients share encodes.
   Unknown types are hz-only.
3. Later: WASM degrade functions shipped from the frontend.
