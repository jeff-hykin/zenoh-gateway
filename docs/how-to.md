# How to

Snippets are from an app page inside dimOS Desktop, using [dim-app](https://github.com/jeff-hykin/dim-app)'s `DimApp`
(vendored at `./dim-app/`). Every page needs this in its `dimos.yaml`:

```yaml
uses:
    "@zenoh-gateway": ">=0.5.1"
    "@dimos-gateway":
        - GET /msgs.js
    "@desktop-gateway":
        - GET /api/desktop/zenoh
```

```js
import { DimApp } from "./dim-app/mod.js"
const app = new DimApp({ msgDecodeEndpoint: "../../dimos/msgs.js" })
```

## How do I subscribe to a topic?

```js
const off = app.subscribe("odom", (odom, { key, type, receivedAt }) => draw(odom.pose.pose.position))
off() // unsubscribe
```

Every message, not just the newest: `app.subscribe("events", show, { delivery: "reliable" })`.

## How do I subscribe to any zenoh key?

```js
app.zenoh.subscribe("dimos/**", { delivery: "latest" }, (msg) => console.log(msg.key, msg.bytes))
```

## How do I list the live topics?

```js
await app.zenoh.ready
const topics = await app.zenoh.client.listTopics("dimos/**") // [{ key, sources }]
```

## How do I decode a dimos message myself?

```js
const msgs = await import("../../dimos/msgs.js")
app.zenoh.subscribe("dimos/odom/*", {}, (msg) => {
    const odom = msgs.decodeChannel(msg.key, msg.bytes) // the key's last chunk names the type
})
```

## How do I publish one message?

```js
button.onclick = () => app.publish("goal", "geometry_msgs.PoseStamped", { pose: { position: { x: 1 } } })
```

## How do I drive a robot (publish with a deadman)?

```js
const app = new DimApp({ msgDecodeEndpoint: "../../dimos/msgs.js", connectOptions: { heartbeatHz: 5 } })
const drive = await app.publisher("cmd_vel", "geometry_msgs.Twist")
await drive.setDeadman({}) // sent if the page dies mid-drive
pad.onpointerdown = () => drive.put({ linear: { x: 0.3 } })
pad.onpointerup = () => drive.stop()
```

Never publish on page load or while idle: only `put()` from a user's input.

## How do I show a camera in a `<video>`?

```js
const camera = app.zenoh.subscribe("dimos/color_image/*", { encoding: "dimos_lcm_image", maxHz: 15 }, (msg) => {
    if (video.srcObject !== msg.mediaStream) {
        video.srcObject = msg.mediaStream
    }
})
```

```html
<video autoplay muted playsinline></video>
```

## How do I change a running video's quality or resolution?

```js
await camera.update({ maxHz: 30, maxResolution: [640, 480], encodeOptions: { quality: 0.6 } })
await camera.update({ maxResolution: null }) // back to the default
```

Also takes `minQuality`, `qualityToHzTradeoff`, `bandwidthPriority`, `maxBitrate`, `minResolutionScale`, `playoutDelay`.

## How do I get an image as a file instead of video?

```js
app.zenoh.subscribe("dimos/color_image/*", { encoding: "dimos_lcm_image", channel: "data", encodeOptions: { format: "jpeg" } },
    async (msg) => ctx.drawImage(await createImageBitmap(new Blob([msg.bytes])), 0, 0))
```

## How do I get a point cloud?

```js
app.zenoh.subscribe("dimos/lidar/*", { encoding: "dimos_lcm_pointcloud2", maxHz: 10 }, (msg) => {
    const { positions, count, intensity } = msg.decoded // positions: Float32Array, x y z per point
})
```

## How do I get a depth image?

```js
app.zenoh.subscribe("dimos/depth_image/*", { encoding: "dimos_lcm_depth" }, (msg) => {
    const { width, height, data, encoding } = msg.decoded
})
```

## How do I list the encodings the gateway has?

```js
await app.zenoh.ready
app.zenoh.client.encodings // [{ name, output }]
```

## How do I know the connection is alive?

```js
app.zenoh.onState((state) => badge.textContent = state) // connecting | connected | degraded | lost
app.zenoh.onReconnect(refetchEverything)
```

## How do I turn on heartbeats?

```js
const app = new DimApp({ msgDecodeEndpoint: "../../dimos/msgs.js", connectOptions: { heartbeatHz: 5, heartbeatMisses: 3 } })
```

Needed for deadmen and leases. The first `DimApp` / `getZenoh` call's options win.

## How do I use the client without dim-app?

```js
import { connect } from "https://esm.sh/gh/jeff-hykin/zenoh-gateway@74b478b/client/zenoh_gateway.ts"
const { zenohGatewayUrl = "/zenoh-gateway" } = await (await fetch("../../api/desktop/zenoh?app=my-app")).json()
const z = await connect(new URL(zenohGatewayUrl, location.href).href, { heartbeatHz: 5 })
const sub = z.subscribe("dimos/odom/*", { delivery: "latest" }, (msg) => console.log(msg.bytes))
```

Full API: [README.md](../README.md#client-api). Wire contract: [SPEC.md](../SPEC.md).
