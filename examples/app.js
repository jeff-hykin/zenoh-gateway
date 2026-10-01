// zenoh-web example: no build step, the TypeScript client comes transpiled from esm.sh.
// Query params: ?bridge=<url> (default: this page's origin), ?client=<module url> (e.g. a local bundle).

const defaultClientUrl = "https://esm.sh/gh/jeff-hykin/zenoh-web@7fb9072f9baab4916c0a67ab7fee9925ad7218bc/client/zenoh_web.ts"

const params = new URLSearchParams(location.search)
const bridgeUrl = params.get("bridge") ?? location.origin
const { connect, CODECS, Priority, codecOutput } = await import(params.get("client") ?? defaultClientUrl)

const byId = (id) => document.getElementById(id)
const connectionStateElement = byId("connection-state")
const streamsElement = byId("streams")
const streamTemplate = byId("stream-template")

/** @param {number} bytes */
function formatBytes(bytes) {
    if (bytes === null || bytes === undefined || !Number.isFinite(bytes)) {
        return "–"
    }
    if (bytes >= 1e6) {
        return `${(bytes / 1e6).toFixed(2)} MB`
    }
    if (bytes >= 1e3) {
        return `${(bytes / 1e3).toFixed(1)} kB`
    }
    return `${Math.round(bytes)} B`
}

/** @param {number | null | undefined} value @param {number} digits */
function formatNumber(value, digits = 1) {
    return value === null || value === undefined || !Number.isFinite(value) ? "–" : value.toFixed(digits)
}

/**
 * Suggests a codec from the key's type part (ROS 2 `...::msg::dds_::Image_/...`, dimos `.../sensor_msgs.Image`).
 * Depth vs color is only a guess from the topic name; change it in the select.
 * @param {string} key
 */
function guessCodec(key) {
    const protocol = key.includes("::msg::dds_::") ? "ros2" : key.includes("sensor_msgs.") ? "dimos" : null
    if (protocol === null) {
        return ""
    }
    const looksLikeDepth = /depth/i.test(key)
    if (key.includes("PointCloud2")) {
        return `${protocol}-pointcloud2`
    }
    if (key.includes("CompressedImage")) {
        return looksLikeDepth ? `${protocol}-compressed-depth` : `${protocol}-compressed-image`
    }
    if (key.includes("Image")) {
        return looksLikeDepth ? `${protocol}-depth` : `${protocol}-image`
    }
    return ""
}

// ---------------------------------------------------------------- connection

byId("bridge-url").textContent = bridgeUrl
let client
try {
    client = await connect(bridgeUrl)
} catch (error) {
    connectionStateElement.textContent = `could not connect: ${error.message}`
    connectionStateElement.dataset.state = "lost"
    throw error
}
const setConnectionState = (state) => {
    connectionStateElement.textContent = state
    connectionStateElement.dataset.state = state
}
setConnectionState(client.state)
client.onState(setConnectionState)

/** @type {Set<Stream>} */
const streams = new Set()
// for debugging from the console (and the example test)
window.zenohWebExample = { client, streams }

// ---------------------------------------------------------------- topics

const topicListElement = byId("topic-list")
const topicStatusElement = byId("topic-status")
const subscribeKeyElement = byId("subscribe-key")
const subscribeCodecElement = byId("subscribe-codec")

async function refreshTopics() {
    const filter = byId("topic-filter").value.trim() || "**"
    topicStatusElement.textContent = "listing…"
    try {
        const topics = await client.listTopics(filter)
        topics.sort((left, right) => left.key.localeCompare(right.key))
        topicListElement.replaceChildren(...topics.map((topic) => {
            const item = document.createElement("li")
            const button = document.createElement("button")
            button.className = "mono"
            button.textContent = topic.key
            const sources = document.createElement("div")
            sources.className = "sources"
            sources.textContent = topic.sources.join(", ")
            button.append(sources)
            button.addEventListener("click", () => {
                subscribeKeyElement.value = topic.key
                subscribeCodecElement.value = guessCodec(topic.key)
            })
            item.append(button)
            return item
        }))
        topicStatusElement.textContent = `${topics.length} topic${topics.length === 1 ? "" : "s"} (click one to fill the key)`
    } catch (error) {
        topicStatusElement.textContent = `listTopics failed: ${error.message}`
    }
}
byId("refresh-topics").addEventListener("click", refreshTopics)

// ---------------------------------------------------------------- subscribe form

subscribeCodecElement.append(new Option("raw (no codec)", ""), ...CODECS.map((codec) => new Option(codec, codec)))
subscribeKeyElement.addEventListener("change", () => {
    subscribeCodecElement.value = guessCodec(subscribeKeyElement.value)
})
byId("subscribe-button").addEventListener("click", () => {
    const key = subscribeKeyElement.value.trim()
    const errorElement = byId("subscribe-error")
    errorElement.textContent = ""
    if (!key) {
        errorElement.textContent = "enter a key expression"
        return
    }
    try {
        streams.add(new Stream(key, subscribeCodecElement.value || null))
    } catch (error) {
        errorElement.textContent = error.message
    }
})

// ---------------------------------------------------------------- renderers

/** Grayscale depth: near = bright, invalid (0 / non-finite) = black. */
class DepthView {
    constructor(container) {
        this.canvas = document.createElement("canvas")
        this.canvas.className = "depth"
        this.context = this.canvas.getContext("2d")
        this.caption = document.createElement("div")
        this.caption.className = "stream-stats"
        container.append(this.canvas, this.caption)
    }

    /** @param {import("../client/zenoh_web.ts").DepthImage} depth */
    draw(depth) {
        const { width, height, data } = depth
        if (this.canvas.width !== width || this.canvas.height !== height) {
            this.canvas.width = width
            this.canvas.height = height
        }
        let nearest = Infinity
        let farthest = -Infinity
        for (let index = 0; index < data.length; index++) {
            const value = data[index]
            if (value > 0 && Number.isFinite(value)) {
                nearest = Math.min(nearest, value)
                farthest = Math.max(farthest, value)
            }
        }
        const range = farthest > nearest ? farthest - nearest : 1
        const image = this.context.createImageData(width, height)
        const pixels = image.data
        for (let index = 0; index < data.length; index++) {
            const value = data[index]
            const gray = value > 0 && Number.isFinite(value) ? 255 - Math.round(((value - nearest) / range) * 215) : 0
            const offset = index * 4
            pixels[offset] = gray
            pixels[offset + 1] = gray
            pixels[offset + 2] = gray
            pixels[offset + 3] = 255
        }
        this.context.putImageData(image, 0, 0)
        const unit = depth.encoding === "32FC1" ? "m" : depth.encoding === "16UC1" ? "mm" : ""
        this.caption.textContent = `${width}×${height} (source ${depth.sourceWidth}×${depth.sourceHeight}, stride ${depth.stride}), ${depth.encoding}, ${formatNumber(nearest, 2)}–${formatNumber(farthest, 2)} ${unit}`
    }
}

/** Point cloud on a 2D canvas: perspective view around the cloud's center, drag to orbit, colored by height. */
class PointCloudView {
    constructor(container) {
        this.canvas = document.createElement("canvas")
        this.canvas.className = "pointcloud"
        this.canvas.width = 640
        this.canvas.height = 480
        this.context = this.canvas.getContext("2d")
        this.caption = document.createElement("div")
        this.caption.className = "stream-stats"
        container.append(this.canvas, this.caption)
        this.yaw = -0.6
        this.pitch = 0.5
        this.points = null
        let dragStart = null
        this.canvas.addEventListener("pointerdown", (event) => {
            dragStart = { x: event.clientX, y: event.clientY, yaw: this.yaw, pitch: this.pitch }
            this.canvas.setPointerCapture(event.pointerId)
        })
        this.canvas.addEventListener("pointermove", (event) => {
            if (dragStart) {
                this.yaw = dragStart.yaw + (event.clientX - dragStart.x) * 0.01
                this.pitch = Math.max(-1.5, Math.min(1.5, dragStart.pitch + (event.clientY - dragStart.y) * 0.01))
                this.render()
            }
        })
        this.canvas.addEventListener("pointerup", () => {
            dragStart = null
        })
    }

    /** @param {import("../client/zenoh_web.ts").PointCloud} points */
    draw(points) {
        this.points = points
        this.render()
        this.caption.textContent = `${points.count} points (source ${points.sourceCount}), 1 in ${points.keepEvery} kept, max error ${formatNumber(points.maxError, 4)}`
    }

    render() {
        const { width, height } = this.canvas
        const image = this.context.createImageData(width, height)
        const pixels = image.data
        for (let offset = 3; offset < pixels.length; offset += 4) {
            pixels[offset] = 255
        }
        const points = this.points
        if (points && points.count > 0) {
            const positions = points.positions
            const minimum = [Infinity, Infinity, Infinity]
            const maximum = [-Infinity, -Infinity, -Infinity]
            for (let index = 0; index < positions.length; index++) {
                const axis = index % 3
                minimum[axis] = Math.min(minimum[axis], positions[index])
                maximum[axis] = Math.max(maximum[axis], positions[index])
            }
            const center = minimum.map((value, axis) => (value + maximum[axis]) / 2)
            const radius = Math.max(1e-6, ...maximum.map((value, axis) => value - minimum[axis])) / 2
            const heightRange = Math.max(1e-6, maximum[2] - minimum[2])
            const [cosYaw, sinYaw, cosPitch, sinPitch] = [Math.cos(this.yaw), Math.sin(this.yaw), Math.cos(this.pitch), Math.sin(this.pitch)]
            const cameraDistance = radius * 3
            const focal = Math.min(width, height) * 1.2
            for (let index = 0; index < points.count; index++) {
                const x = positions[index * 3] - center[0]
                const y = positions[index * 3 + 1] - center[1]
                const z = positions[index * 3 + 2] - center[2]
                // z-up: yaw around z, then pitch the camera down toward the cloud
                const rotatedX = x * cosYaw - y * sinYaw
                const rotatedY = x * sinYaw + y * cosYaw
                const depth = rotatedY * cosPitch + z * sinPitch + cameraDistance
                const up = z * cosPitch - rotatedY * sinPitch
                if (depth <= 0) {
                    continue
                }
                const screenX = Math.round(width / 2 + (rotatedX / depth) * focal)
                const screenY = Math.round(height / 2 - (up / depth) * focal)
                if (screenX < 0 || screenY < 0 || screenX >= width - 1 || screenY >= height - 1) {
                    continue
                }
                // height -> blue..green..yellow
                const t = (positions[index * 3 + 2] - minimum[2]) / heightRange
                const red = Math.round(255 * Math.min(1, t * 2 - 0.5))
                const green = Math.round(80 + 175 * Math.min(1, t * 1.5))
                const blue = Math.round(255 * (1 - t))
                for (const pixelOffset of [0, 1, width, width + 1]) {
                    const offset = (screenY * width + screenX + pixelOffset) * 4
                    pixels[offset] = Math.max(0, red)
                    pixels[offset + 1] = green
                    pixels[offset + 2] = blue
                }
            }
        }
        this.context.putImageData(image, 0, 0)
    }
}

class VideoView {
    constructor(container) {
        this.video = document.createElement("video")
        this.video.autoplay = true
        this.video.muted = true
        this.video.playsInline = true
        this.caption = document.createElement("div")
        this.caption.className = "stream-stats"
        container.append(this.video, this.caption)
    }

    attach(mediaStream) {
        if (mediaStream && this.video.srcObject !== mediaStream) {
            this.video.srcObject = mediaStream
            this.video.play().catch(() => {})
        }
    }

    /** @param {import("../client/zenoh_web.ts").Message} message */
    draw(message) {
        this.attach(message.mediaStream)
        const info = message.video
        this.caption.textContent = `${info.width}×${info.height} (source ${info.sourceWidth}×${info.sourceHeight}), quality ${info.quality.toFixed(2)}, ${formatBytes(info.encodedBytes)}/frame${info.keyframe ? ", keyframe" : ""}`
    }
}

class RawView {
    constructor(container) {
        this.element = document.createElement("div")
        this.element.className = "raw"
        container.append(this.element)
    }

    update(hz, lastSize) {
        this.element.textContent = `${formatNumber(hz)} Hz · ${formatBytes(lastSize)}`
    }
}

// ---------------------------------------------------------------- streams

const priorityNames = Object.fromEntries(Object.entries(Priority).map(([name, value]) => [value, name]))

/** One subscription card; changing a control re-subscribes with the new options. */
class Stream {
    /** @param {string} key @param {string | null} codec */
    constructor(key, codec) {
        this.key = key
        this.codec = codec
        this.output = codec ? codecOutput(codec) : "raw"
        this.element = streamTemplate.content.firstElementChild.cloneNode(true)
        this.element.dataset.key = key
        this.element.dataset.output = this.output
        this.element.querySelector(".key").textContent = key
        this.element.querySelector(".codec").textContent = codec ?? "raw"
        this.errorElement = this.element.querySelector(".stream-error")
        this.statsElement = this.element.querySelector(".stream-stats")
        this.element.querySelector(".close").addEventListener("click", () => this.close())
        const view = this.element.querySelector(".stream-view")
        this.view = this.output === "video" ? new VideoView(view)
            : this.output === "depth" ? new DepthView(view)
            : this.output === "pointcloud" ? new PointCloudView(view)
            : new RawView(view)
        this.#setUpControls()
        this.messagesThisSecond = 0
        this.hz = 0
        this.lastSize = 0
        this.subscription = null
        this.#subscribe()
        streamsElement.append(this.element)
    }

    #setUpControls() {
        const prioritySelect = this.element.querySelector('select[name="priority"]')
        prioritySelect.append(new Option("as published", ""), ...Object.entries(Priority).map(([name, value]) => new Option(`${value} ${name}`, String(value))))
        this.controls = [...this.element.querySelectorAll(".controls input, .controls select")]
        for (const control of this.controls) {
            const output = control.nextElementSibling
            const show = () => {
                if (output?.tagName === "OUTPUT") {
                    output.textContent = control.name === "maxHz" && Number(control.value) === 0 ? "∞" : control.value
                }
            }
            show()
            control.addEventListener("input", show)
            control.addEventListener("change", () => this.#subscribe())
            // quality only applies to transcoded streams
            if (this.output === "raw" && control.name.endsWith("Quality")) {
                control.disabled = true
            }
        }
    }

    /** The subscribe options the controls describe. */
    options() {
        const values = Object.fromEntries(this.controls.map((control) => [control.name, control.value]))
        const options = {
            delivery: "latest",
            bandwidthPriority: Number(values.bandwidthPriority),
            qualityToHzTradeoff: Number(values.qualityToHzTradeoff),
        }
        if (this.codec) {
            options.codec = this.codec
            options.minQuality = Math.min(Number(values.minQuality), Number(values.maxQuality))
            options.maxQuality = Number(values.maxQuality)
        }
        if (Number(values.maxHz) > 0) {
            options.maxHz = Number(values.maxHz)
        }
        if (values.priority) {
            options.priority = Number(values.priority)
        }
        return options
    }

    #subscribe() {
        this.subscription?.close()
        this.errorElement.textContent = ""
        const subscription = client.subscribe(this.key, this.options(), (message) => this.#onMessage(message))
        this.subscription = subscription
        subscription.ready().then(() => {
            if (this.output === "video") {
                this.view.attach(subscription.mediaStream)
            }
        }, (error) => {
            if (subscription === this.subscription) {
                this.errorElement.textContent = error.message
            }
        })
    }

    #onMessage(message) {
        this.messagesThisSecond++
        // video pixels travel on the track; the channel only carries 28 bytes of metadata per frame
        this.lastSize = message.video ? message.video.encodedBytes : message.bytes.byteLength
        if (this.output === "video") {
            this.view.draw(message)
        } else if (this.output === "depth") {
            this.view.draw(message.depth)
        } else if (this.output === "pointcloud") {
            this.view.draw(message.points)
        }
    }

    /** Called once a second. */
    tick() {
        this.hz = this.messagesThisSecond
        this.messagesThisSecond = 0
        const subscription = this.subscription
        if (this.output === "raw") {
            this.view.update(this.hz, this.lastSize)
        }
        const allocation = subscription?.bridgeStats?.allocation
        const pieces = [
            `${this.hz} Hz`,
            `${formatBytes(this.lastSize)}/msg`,
            `received ${subscription?.received ?? 0}`,
            `dropped ${subscription?.dropped ?? 0}`,
            `state ${subscription?.state ?? "–"}`,
        ]
        if (allocation) {
            pieces.push(`granted ${formatNumber(allocation.hz)} Hz${allocation.quality === null ? "" : ` @ q${allocation.quality.toFixed(2)}`} of ${formatBytes(allocation.budgetBytesPerSec)}/s${allocation.constrained ? " (constrained)" : ""}`)
        }
        const priority = this.subscription?.options.priority
        if (priority) {
            pieces.push(priorityNames[priority])
        }
        this.statsElement.textContent = pieces.join(" · ")
    }

    close() {
        this.subscription?.close()
        this.element.remove()
        streams.delete(this)
    }
}

// ---------------------------------------------------------------- stats

const connectionStatsElement = byId("connection-stats")
const keyStatsElement = byId("key-stats")

function renderStats() {
    for (const stream of streams) {
        stream.tick()
    }
    byId("header-rtt").textContent = `rtt ${formatNumber(client.rttMs)} ms`
    const bandwidth = client.bridgeStats?.bandwidth
    const rows = [
        ["state", client.state],
        ["rtt", `${formatNumber(client.rttMs)} ms`],
        ["clock offset", `${formatNumber(client.clockOffsetMs)} ms`],
        ["budget", `${formatBytes(bandwidth?.budgetBytesPerSec)}/s`],
        ["data estimate", `${formatBytes(bandwidth?.dataEstimateBytesPerSec)}/s`],
        ["video estimate", `${formatBytes(bandwidth?.videoEstimateBytesPerSec)}/s`],
        ["cap", bandwidth?.capBytesPerSec ? `${formatBytes(bandwidth.capBytesPerSec)}/s` : "none"],
        ["demand", `${formatBytes(bandwidth?.demandBytesPerSec)}/s`],
        ["sent", `${formatBytes(bandwidth?.sentBytesPerSec)}/s`],
        ["queue delay", `${formatNumber(bandwidth?.queueDelayMs)} ms`],
        ["constrained", bandwidth ? String(bandwidth.constrained) : "–"],
        ["access denials", String(client.bridgeStats?.access?.denied ?? "–")],
    ]
    connectionStatsElement.replaceChildren(...rows.flatMap(([name, value]) => {
        const term = document.createElement("dt")
        term.textContent = name
        const definition = document.createElement("dd")
        definition.textContent = value
        return [term, definition]
    }))
    keyStatsElement.replaceChildren(...Object.entries(client.stats).map(([key, stats]) => {
        const allocation = stats.bridge?.allocation
        const row = document.createElement("tr")
        const cells = [
            key,
            String(stats.received),
            String(stats.dropped),
            formatBytes(stats.backlogBytes),
            allocation?.quality === null || allocation?.quality === undefined ? "–" : allocation.quality.toFixed(2),
            formatNumber(allocation?.hz),
            allocation ? `${formatBytes(allocation.budgetBytesPerSec)}/s` : "–",
        ]
        row.replaceChildren(...cells.map((text, index) => {
            const cell = document.createElement("td")
            cell.textContent = text
            if (index === 0) {
                cell.className = "key mono"
            }
            return cell
        }))
        return row
    }))
}
setInterval(renderStats, 1000)
renderStats()
refreshTopics()
