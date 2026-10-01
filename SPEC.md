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

const z = await connect("http://robot.local:7448")

const sub = z.subscribe("camera/**", {
    delivery: { queue: 1, maxAgeMs: 500 },   // or "latest" (= queue 1) or "reliable" (= queue Infinity)
    priority: Priority.DATA_LOW,              // optional; defaults to the priority the message was published with
    hz: [1.0, 20.0],                          // floats; bridge never sends faster than max
    quality: [0.3, 1.0],                      // floats 0-1; only meaningful for transcoded types (phase 2)
    tradeoff: 0.7,                            // 0 = keep quality, drop hz; 1 = keep hz, drop quality (phase 2)
}, (msg) => { msg.key, msg.bytes, msg.timestamp, msg.seq })
sub.close()

const cmd = z.publisher("cmd_vel", { delivery: "latest", priority: Priority.REAL_TIME, repeatMs: 100 })
cmd.put(bytes)
cmd.close()

const replies = await z.get("some/key/**")   // zenoh query, returns [{ key, bytes }]

z.stats        // per-key: received, dropped, backlogBytes, rttMs
z.onState(fn)  // "connecting" | "connected" | "degraded" | "lost"
```

- `Priority` mirrors zenoh / zenoh-ts: REAL_TIME=1, INTERACTIVE_HIGH=2, INTERACTIVE_LOW=3, DATA_HIGH=4, DATA=5, DATA_LOW=6, BACKGROUND=7 (lower = more important).
- No `latched` flag: the bridge always subscribes with zenoh-ext AdvancedSubscriber history (max 1 sample per publisher), so publishers with a cache (e.g. rmw_zenoh transient_local like tf_static) replay their last message.
- `repeatMs` is client-side: re-send the last value on a timer so a robot-side deadman stays fed.

## Delivery → transport mapping

| delivery | data channel init | bridge behavior when the channel is backed up |
|---|---|---|
| `"reliable"` | ordered, fully reliable | queue without limit |
| `"latest"` / `{queue: N}` | unordered, maxRetransmits 0 | keep at most N pending, drop oldest |
| `{maxAgeMs: M}` | maxPacketLifeTime M | also drop anything older than M |

"Backed up" = the channel's bufferedAmount is above a small threshold (~64 KB). The bridge drains its
own queue on bufferedAmountLow. Nothing piles up in kernel/wifi buffers, so a slow phone sees fewer
frames instead of stale ones.

Browser → zenoh puts: priority from the publisher options, congestion control Block for `"reliable"`,
Drop otherwise, express for priority ≤ INTERACTIVE_HIGH.

## Wire format

- Signaling: `POST /offer` with the browser's SDP offer (non-trickle), returns the answer.
- The bridge can also serve a static directory (`--serve <dir>`) so the UI is live-editable on disk.
- Each subscribe/publisher is its own data channel. Its label is JSON: `{"type":"sub"|"pub", "key":..., "opts":{...}}`.
- One extra channel labeled `control` carries JSON request/response (get, stats).
- Bridge → browser frame: `u16 keyLen | key utf8 | f64 timestampMs | u32 seq | payload`, little endian.

## Phases

1. Bridge pipe + JS client: subscribe (with history), publisher, get, delivery queues, max-hz cap, stats.
2. Bandwidth allocation (min/max hz, shrink by priority) + hardcoded transcoders for point clouds and
   video (quality range + tradeoff). Lazy: a transcoder only runs while a web subscriber wants it
   (zenoh matching listener). Transcoded variants publish on quality-rounded keys so clients share encodes.
   Unknown types are hz-only.
3. Later: WASM degrade functions shipped from the frontend.
