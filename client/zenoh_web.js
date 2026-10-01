// zenoh-web browser client: one WebRTC data channel per subscription/publisher, see SPEC.md

/** zenoh priorities (lower = more important). */
export const Priority = Object.freeze({
    REAL_TIME: 1,
    INTERACTIVE_HIGH: 2,
    INTERACTIVE_LOW: 3,
    DATA_HIGH: 4,
    DATA: 5,
    DATA_LOW: 6,
    BACKGROUND: 7,
})

/**
 * @typedef {"latest" | "reliable"} Delivery
 * @typedef {"connecting" | "connected" | "degraded" | "lost"} ConnectionState
 * @typedef {"connecting" | "open" | "tripped" | "closed"} PublisherState
 * @typedef {{ key: string, bytes: Uint8Array, timestamp: number, seq: number }} Message
 * @typedef {{
 *     delivery?: Delivery,
 *     priority?: number,
 *     bandwidthPriority?: number,
 *     queueSize?: number,
 *     maxAge?: number,
 *     maxHz?: number,
 *     dangerousMinHz?: number,
 *     minQuality?: number,
 *     maxQuality?: number,
 *     qualityToHzTradeoff?: number,
 * }} SubscribeOptions
 * @typedef {{ delivery?: Delivery, priority?: number, repeatMs?: number, latencyLimit?: number }} PublisherOptions
 * @typedef {{ received: number, dropped: number, backlogBytes: number, rttMs: number | null, bridge: object | null }} KeyStats
 * @typedef {{
 *     iceServers?: RTCIceServer[],
 *     reconnect?: boolean,
 *     statsIntervalMs?: number,
 *     heartbeatHz?: number,
 *     heartbeatMisses?: number,
 *     clock?: () => number,
 * }} ConnectOptions
 */

const backedUpBytes = 64 * 1024
const resumeBytes = 16 * 1024
const gatherTimeoutMs = 3000
const openTimeoutMs = 10000
const pingTimeoutMs = 3000
const reconnectDelayMs = 1000
// consumption acks let the bridge stop sending while this page's JS is behind
const ackEveryBytes = 16 * 1024
const ackDelayMs = 5
// clock sync keeps the lowest-RTT sample among the most recent ones
const clockWindow = 16
const initialClockPings = 5
const putHeaderBytes = 8

const subscribeOptionNames = new Set(["delivery", "priority", "bandwidthPriority", "queueSize", "maxAge", "maxHz", "dangerousMinHz", "minQuality", "maxQuality", "qualityToHzTradeoff"])
const publisherOptionNames = new Set(["delivery", "priority", "repeatMs", "latencyLimit"])

/**
 * @param {string} name
 * @param {unknown} value
 * @param {(value: number) => boolean} isValid
 * @param {string} expected
 */
function checkNumber(name, value, isValid, expected) {
    if (value === undefined) {
        return
    }
    if (typeof value !== "number" || Number.isNaN(value) || !isValid(value)) {
        throw new RangeError(`zenoh-web: ${name} must be ${expected}, got ${String(value)}`)
    }
}

/**
 * @param {Record<string, unknown>} options
 * @param {Set<string>} allowed
 * @param {string} where
 */
function checkCommon(options, allowed, where) {
    for (const name of Object.keys(options)) {
        if (!allowed.has(name)) {
            throw new TypeError(`zenoh-web: unknown ${where} option "${name}" (allowed: ${[...allowed].join(", ")})`)
        }
    }
    if (options.delivery !== undefined && options.delivery !== "latest" && options.delivery !== "reliable") {
        throw new TypeError(`zenoh-web: delivery must be "latest" or "reliable", got ${String(options.delivery)}`)
    }
    checkNumber("priority", options.priority, (v) => Number.isInteger(v) && v >= 1 && v <= 7, "an integer 1..7 (see Priority)")
}

/** @param {SubscribeOptions} options */
export function validateSubscribeOptions(options) {
    checkCommon(options, subscribeOptionNames, "subscribe")
    const isUnit = (/** @type {number} */ v) => v >= 0 && v <= 1
    checkNumber("bandwidthPriority", options.bandwidthPriority, (v) => Number.isFinite(v) && v >= 0, ">= 0")
    checkNumber("queueSize", options.queueSize, (v) => v === Infinity || (Number.isInteger(v) && v >= 1), "an integer >= 1 or Infinity")
    checkNumber("maxAge", options.maxAge, (v) => Number.isFinite(v) && v > 0, "> 0 (ms)")
    checkNumber("maxHz", options.maxHz, (v) => Number.isFinite(v) && v > 0, "> 0")
    checkNumber("dangerousMinHz", options.dangerousMinHz, (v) => Number.isFinite(v) && v >= 0 && v <= (options.maxHz ?? Infinity), ">= 0 and <= maxHz")
    checkNumber("minQuality", options.minQuality, isUnit, "within 0..1")
    checkNumber("maxQuality", options.maxQuality, isUnit, "within 0..1")
    checkNumber("qualityToHzTradeoff", options.qualityToHzTradeoff, isUnit, "within 0..1")
    if ((options.minQuality ?? 0) > (options.maxQuality ?? 1)) {
        throw new RangeError("zenoh-web: minQuality must be <= maxQuality")
    }
}

/** @param {PublisherOptions} options */
export function validatePublisherOptions(options) {
    checkCommon(options, publisherOptionNames, "publisher")
    checkNumber("repeatMs", options.repeatMs, (v) => Number.isFinite(v) && v > 0, "> 0 (ms)")
    checkNumber("latencyLimit", options.latencyLimit, (v) => Number.isFinite(v) && v > 0, "> 0 (ms)")
}

/**
 * Delivery -> data channel reliability (SPEC "Delivery -> transport mapping").
 * @param {Delivery | undefined} delivery
 * @param {number | undefined} maxAge
 * @returns {RTCDataChannelInit}
 */
function channelInit(delivery, maxAge) {
    if (delivery === "reliable") {
        return { ordered: true }
    }
    if (maxAge) {
        return { ordered: false, maxPacketLifeTime: Math.min(65535, Math.round(maxAge)) }
    }
    return { ordered: false, maxRetransmits: 0 }
}

/**
 * @param {Uint8Array | ArrayBuffer | ArrayBufferView | string} value
 * @returns {Uint8Array}
 */
function toBytes(value) {
    if (typeof value === "string") {
        return new TextEncoder().encode(value)
    }
    if (value instanceof Uint8Array) {
        return value
    }
    if (ArrayBuffer.isView(value)) {
        return new Uint8Array(value.buffer, value.byteOffset, value.byteLength)
    }
    return new Uint8Array(value)
}

/** @param {string} text */
function fromBase64(text) {
    const binary = atob(text)
    const bytes = new Uint8Array(binary.length)
    for (let index = 0; index < binary.length; index++) {
        bytes[index] = binary.charCodeAt(index)
    }
    return bytes
}

/** @param {Uint8Array} bytes */
function toBase64(bytes) {
    let binary = ""
    for (let index = 0; index < bytes.length; index++) {
        binary += String.fromCharCode(bytes[index])
    }
    return btoa(binary)
}

const keyDecoder = new TextDecoder()

/**
 * Bridge frame: u16 keyLen | key | f64 timestampMs | u32 seq | payload (little endian).
 * @param {ArrayBuffer} buffer
 * @returns {Message}
 */
export function decodeFrame(buffer) {
    const view = new DataView(buffer)
    const keyLength = view.getUint16(0, true)
    const key = keyDecoder.decode(new Uint8Array(buffer, 2, keyLength))
    const timestamp = view.getFloat64(2 + keyLength, true)
    const seq = view.getUint32(10 + keyLength, true)
    return { key, bytes: new Uint8Array(buffer, 14 + keyLength), timestamp, seq }
}

/**
 * Browser -> bridge put: f64 sentAtMs (browser clock) | payload (little endian).
 * @param {Uint8Array} payload
 * @param {number} sentAtMs
 */
export function encodePut(payload, sentAtMs) {
    const frame = new Uint8Array(putHeaderBytes + payload.length)
    new DataView(frame.buffer).setFloat64(0, sentAtMs, true)
    frame.set(payload, putHeaderBytes)
    return frame
}

/** @param {number} ms */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * @param {RTCDataChannel} channel
 * @param {number} timeoutMs
 */
function waitOpen(channel, timeoutMs) {
    if (channel.readyState === "open") {
        return Promise.resolve()
    }
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("data channel open timed out")), timeoutMs)
        channel.addEventListener("open", () => {
            clearTimeout(timer)
            resolve(undefined)
        }, { once: true })
        channel.addEventListener("close", () => {
            clearTimeout(timer)
            reject(new Error("data channel closed before opening"))
        }, { once: true })
    })
}

/** @param {RTCPeerConnection} peer */
function waitIceGathering(peer) {
    if (peer.iceGatheringState === "complete") {
        return Promise.resolve()
    }
    return new Promise((resolve) => {
        const finish = () => {
            clearTimeout(timer)
            peer.removeEventListener("icegatheringstatechange", onChange)
            resolve(undefined)
        }
        const onChange = () => {
            if (peer.iceGatheringState === "complete") {
                finish()
            }
        }
        const timer = setTimeout(finish, gatherTimeoutMs)
        peer.addEventListener("icegatheringstatechange", onChange)
    })
}

export class Subscription {
    /** @type {RTCDataChannel | null} */
    channel = null
    received = 0
    /** drops before the current channel (each new channel restarts seq at 0) */
    #droppedBefore = 0
    #firstSeq = -1
    #maxSeq = -1
    #receivedOnChannel = 0
    closed = false
    /** @type {object | null} latest bridge-side stats for this channel */
    bridgeStats = null
    /** @type {Promise<void>} */
    #ready = Promise.resolve()
    #highestConsumedSeq = -1
    #bytesSinceAck = 0
    /** @type {ReturnType<typeof setTimeout> | null} */
    #ackTimer = null

    /**
     * @param {ZenohWeb} owner
     * @param {number} id
     * @param {string} key
     * @param {SubscribeOptions} options
     * @param {(message: Message) => void} callback
     */
    constructor(owner, id, key, options, callback) {
        this.owner = owner
        this.id = id
        this.key = key
        this.options = options
        this.callback = callback
    }

    /** Resolves once the current data channel is open (rejects if it fails to open). */
    ready() {
        return this.#ready
    }

    /** samples the bridge accepted for us but we never got (queue, age, maxHz or network) */
    get dropped() {
        const span = this.#maxSeq < 0 ? 0 : this.#maxSeq - this.#firstSeq + 1
        return this.#droppedBefore + Math.max(0, span - this.#receivedOnChannel)
    }

    /** @param {RTCPeerConnection} peer */
    attach(peer) {
        this.#droppedBefore = this.dropped
        this.#firstSeq = -1
        this.#maxSeq = -1
        this.#receivedOnChannel = 0
        this.#highestConsumedSeq = -1
        this.#bytesSinceAck = 0
        // JSON turns queueSize Infinity into null, which the bridge reads as unbounded
        const label = JSON.stringify({ type: "sub", key: this.key, id: this.id, opts: this.options })
        const channel = peer.createDataChannel(label, channelInit(this.options.delivery, this.options.maxAge))
        channel.binaryType = "arraybuffer"
        channel.onmessage = (event) => {
            if (!(event.data instanceof ArrayBuffer)) {
                return
            }
            const message = decodeFrame(event.data)
            this.received++
            this.#receivedOnChannel++
            if (this.#firstSeq < 0 || message.seq < this.#firstSeq) {
                this.#firstSeq = message.seq
            }
            if (message.seq > this.#maxSeq) {
                this.#maxSeq = message.seq
            }
            try {
                this.callback(message)
            } catch (error) {
                console.error(`zenoh-web: subscriber callback for ${this.key} threw`, error)
            }
            this.#consumed(channel, message.seq, event.data.byteLength)
        }
        this.channel = channel
        this.#ready = waitOpen(channel, openTimeoutMs)
        this.#ready.catch(() => {})
    }

    /**
     * Tells the bridge we processed everything up to seq: 4 bytes, little endian.
     * @param {RTCDataChannel} channel
     * @param {number} seq
     * @param {number} byteLength
     */
    #consumed(channel, seq, byteLength) {
        if (seq > this.#highestConsumedSeq) {
            this.#highestConsumedSeq = seq
        }
        this.#bytesSinceAck += byteLength
        const sendAck = () => {
            this.#ackTimer = null
            if (channel.readyState !== "open" || channel !== this.channel) {
                return
            }
            this.#bytesSinceAck = 0
            const ack = new Uint8Array(4)
            new DataView(ack.buffer).setUint32(0, this.#highestConsumedSeq, true)
            channel.send(ack)
        }
        if (this.#bytesSinceAck >= ackEveryBytes) {
            if (this.#ackTimer) {
                clearTimeout(this.#ackTimer)
            }
            sendAck()
        } else if (!this.#ackTimer) {
            this.#ackTimer = setTimeout(sendAck, ackDelayMs)
        }
    }

    close() {
        if (this.closed) {
            return
        }
        this.closed = true
        this.channel?.close()
        this.owner._forget(this)
    }
}

export class Publisher {
    /** @type {RTCDataChannel | null} */
    channel = null
    /** @type {Uint8Array | null} */
    #last = null
    /** @type {Uint8Array[]} stamped frames waiting for the channel (reliable: all, latest: only the newest) */
    #pending = []
    /** @type {ReturnType<typeof setInterval> | null} */
    #repeatTimer = null
    sent = 0
    dropped = 0
    closed = false
    tripped = false
    /** @type {string | null} why the deadman fired: "heartbeat" | "disconnected" | "shutdown" */
    tripReason = null
    deadmanArmed = false
    /** @type {object | null} latest bridge-side stats for this channel */
    bridgeStats = null
    /** @type {Set<(reason: string) => void>} */
    #tripListeners = new Set()
    /** @type {Promise<void>} */
    #ready = Promise.resolve()

    /**
     * @param {ZenohWeb} owner
     * @param {number} id
     * @param {string} key
     * @param {PublisherOptions} options
     */
    constructor(owner, id, key, options) {
        this.owner = owner
        this.id = id
        this.key = key
        this.options = options
        if (options.repeatMs) {
            this.#repeatTimer = setInterval(() => {
                if (this.#last && !this.tripped) {
                    this.#send(encodePut(this.#last, this.owner.now()))
                }
            }, options.repeatMs)
        }
    }

    /** @returns {PublisherState} */
    get state() {
        if (this.closed) {
            return "closed"
        }
        if (this.tripped) {
            return "tripped"
        }
        return this.channel?.readyState === "open" ? "open" : "connecting"
    }

    /** Resolves once the current data channel is open (rejects if it fails to open). */
    ready() {
        return this.#ready
    }

    /** @param {(reason: string) => void} listener @returns {() => void} unsubscribe */
    onTripped(listener) {
        this.#tripListeners.add(listener)
        return () => this.#tripListeners.delete(listener)
    }

    /** @param {RTCPeerConnection} peer */
    attach(peer) {
        const { delivery, priority, latencyLimit } = this.options
        const label = JSON.stringify({ type: "pub", key: this.key, id: this.id, opts: { delivery, priority, latencyLimit } })
        const channel = peer.createDataChannel(label, channelInit(delivery, undefined))
        channel.binaryType = "arraybuffer"
        channel.bufferedAmountLowThreshold = resumeBytes
        channel.onopen = () => this.#flush()
        channel.onbufferedamountlow = () => this.#flush()
        this.channel = channel
        this.#ready = waitOpen(channel, openTimeoutMs)
        this.#ready.catch(() => {})
    }

    #checkUsable() {
        if (this.closed) {
            throw new Error(`zenoh-web: publisher ${this.key} is closed`)
        }
        if (this.tripped) {
            throw new Error(`zenoh-web: publisher ${this.key} is tripped (deadman fired: ${this.tripReason}); create a new publisher`)
        }
    }

    /**
     * @param {Uint8Array | ArrayBuffer | ArrayBufferView | string} value
     * @param {{ timestamp?: number }} options timestamp: when the value was produced, in this client's clock (`z.now()`); defaults to now
     */
    put(value, { timestamp } = {}) {
        this.#checkUsable()
        const bytes = toBytes(value)
        this.#last = bytes
        this.#send(encodePut(bytes, timestamp ?? this.owner.now()))
    }

    /** @param {Uint8Array} frame */
    #send(frame) {
        const channel = this.channel
        const backedUp = !channel || channel.readyState !== "open" || channel.bufferedAmount > backedUpBytes
        if (backedUp || this.#pending.length > 0) {
            if (this.options.delivery !== "reliable") {
                this.dropped += this.#pending.length
                this.#pending = []
            }
            this.#pending.push(frame)
            return
        }
        channel.send(frame)
        this.sent++
    }

    #flush() {
        const channel = this.channel
        while (this.#pending.length > 0 && channel?.readyState === "open" && channel.bufferedAmount <= backedUpBytes) {
            channel.send(/** @type {Uint8Array} */ (this.#pending.shift()))
            this.sent++
        }
    }

    /**
     * Stores `value` on the bridge; it is published once (REAL_TIME, reliable) if this frontend's
     * heartbeat stops, it disconnects, or the bridge shuts down. Then this publisher is tripped.
     * @param {Uint8Array | ArrayBuffer | ArrayBufferView | string} value
     * @returns {Promise<void>}
     */
    setDeadman(value) {
        if (!this.owner.options.heartbeatHz) {
            throw new Error("zenoh-web: setDeadman needs a heartbeat; connect(url, { heartbeatHz: 5, heartbeatMisses: 3 })")
        }
        this.#checkUsable()
        const bytes = toBytes(value)
        return this.ready().then(async () => {
            await this.owner._request({ op: "setDeadman", pubId: this.id, bytes: toBase64(bytes) }, pingTimeoutMs)
            this.deadmanArmed = true
        })
    }

    /** @returns {Promise<void>} */
    async clearDeadman() {
        this.#checkUsable()
        await this.owner._request({ op: "clearDeadman", pubId: this.id }, pingTimeoutMs)
        this.deadmanArmed = false
    }

    /** @param {string} reason */
    _trip(reason) {
        if (this.tripped || this.closed) {
            return
        }
        this.tripped = true
        this.tripReason = reason
        this.deadmanArmed = false
        this.#pending = []
        if (this.#repeatTimer) {
            clearInterval(this.#repeatTimer)
        }
        // a tripped stream is never re-opened; the channel stays until close() so the bridge keeps rejecting it
        this.owner._forget(this)
        for (const listener of this.#tripListeners) {
            try {
                listener(reason)
            } catch (error) {
                console.error("zenoh-web: onTripped listener threw", error)
            }
        }
    }

    close() {
        if (this.closed) {
            return
        }
        this.closed = true
        if (this.#repeatTimer) {
            clearInterval(this.#repeatTimer)
        }
        this.channel?.close()
        this.owner._forget(this)
    }
}

export class ZenohWeb {
    /** @type {ConnectionState} */
    state = "connecting"
    /** @type {Record<string, KeyStats>} per subscribed/published key expression */
    stats = {}
    /** @type {number | null} latest round trip to the bridge (heartbeat, else control ping) */
    rttMs = null
    /** @type {number | null} bridge clock minus this client's clock, from the lowest-RTT recent sample */
    clockOffsetMs = null
    /** @type {object | null} bridge-side heartbeat/clock stats */
    bridgeStats = null
    /** @type {RTCPeerConnection | null} */
    #peer = null
    /** @type {RTCDataChannel | null} */
    #control = null
    /** @type {RTCDataChannel | null} */
    #heartbeat = null
    /** @type {ReturnType<typeof setInterval> | null} */
    #heartbeatTimer = null
    #heartbeatPaused = false
    /** @type {{ offsetMs: number, rttMs: number }[]} */
    #clockSamples = []
    /** @type {Set<Subscription | Publisher>} */
    #endpoints = new Set()
    /** @type {Map<number, Publisher>} publishers that may still be tripped by the bridge */
    #publishers = new Map()
    /** @type {Map<number, { resolve: (value: any) => void, reject: (error: Error) => void, timer: ReturnType<typeof setTimeout> }>} */
    #requests = new Map()
    /** @type {Set<(state: ConnectionState) => void>} */
    #stateListeners = new Set()
    #nextId = 1
    #closed = false
    #generation = 0
    /** @type {ReturnType<typeof setInterval> | null} */
    #statsTimer = null

    /**
     * @param {string} url bridge base url, e.g. http://robot.local:7448
     * @param {ConnectOptions} options
     */
    constructor(url, options = {}) {
        this.url = url.replace(/\/+$/, "")
        this.options = { iceServers: [], reconnect: true, statsIntervalMs: 1000, heartbeatHz: 0, heartbeatMisses: 3, ...options }
        checkNumber("heartbeatHz", this.options.heartbeatHz, (v) => Number.isFinite(v) && v >= 0, ">= 0 (0 = no heartbeat)")
        checkNumber("heartbeatMisses", this.options.heartbeatMisses, (v) => Number.isInteger(v) && v >= 1, "an integer >= 1")
        /** @type {() => number} this client's clock in ms; put timestamps and clock sync use it */
        this.now = this.options.clock ?? (() => performance.timeOrigin + performance.now())
    }

    /** @param {(state: ConnectionState) => void} listener @returns {() => void} unsubscribe */
    onState(listener) {
        this.#stateListeners.add(listener)
        return () => this.#stateListeners.delete(listener)
    }

    /** @param {ConnectionState} state */
    #setState(state) {
        if (state === this.state) {
            return
        }
        this.state = state
        for (const listener of this.#stateListeners) {
            try {
                listener(state)
            } catch (error) {
                console.error("zenoh-web: state listener threw", error)
            }
        }
    }

    /**
     * NTP-style sample: t0/t3 in our clock, t1/t2 bridge receive/send in its clock.
     * @param {number} t0 @param {number} t1 @param {number} t2 @param {number} t3
     */
    #addClockSample(t0, t1, t2, t3) {
        const rttMs = (t3 - t0) - (t2 - t1)
        const offsetMs = ((t1 - t0) + (t2 - t3)) / 2
        if (!Number.isFinite(rttMs) || !Number.isFinite(offsetMs)) {
            return
        }
        this.#clockSamples.push({ offsetMs, rttMs })
        if (this.#clockSamples.length > clockWindow) {
            this.#clockSamples.shift()
        }
        const best = this.#clockSamples.reduce((a, b) => (b.rttMs < a.rttMs ? b : a))
        this.clockOffsetMs = best.offsetMs
        this.rttMs = rttMs
    }

    /** Clock-sync ping over control; also reports our current estimate to the bridge. */
    async #controlPing() {
        const t0 = this.now()
        const response = await this._request({ op: "ping", t0, offsetMs: this.clockOffsetMs, rttMs: this.rttMs }, pingTimeoutMs)
        this.#addClockSample(t0, response.t1, response.t2, this.now())
    }

    /** Opens (or re-opens) the peer connection and every channel on it. */
    async _open() {
        const generation = ++this.#generation
        this.#setState("connecting")
        this.#clockSamples = []
        const peer = new RTCPeerConnection({ iceServers: this.options.iceServers })
        const control = peer.createDataChannel("control", { ordered: true })
        this.#peer = peer
        this.#control = control
        control.onmessage = (event) => this.#onControlMessage(event.data)
        control.onclose = () => this.#onLost(generation)
        peer.onconnectionstatechange = () => {
            if (generation !== this.#generation) {
                return
            }
            const connectionState = peer.connectionState
            if (connectionState === "failed" || connectionState === "closed") {
                this.#onLost(generation)
            } else if (connectionState === "disconnected") {
                this.#setState("degraded")
            } else if (connectionState === "connected" && control.readyState === "open") {
                this.#setState("connected")
            }
        }
        if (this.options.heartbeatHz > 0) {
            this.#attachHeartbeat(peer)
        }
        for (const endpoint of this.#endpoints) {
            endpoint.attach(peer)
        }
        try {
            await peer.setLocalDescription(await peer.createOffer())
            await waitIceGathering(peer)
            const response = await fetch(`${this.url}/offer`, {
                method: "POST",
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ type: peer.localDescription?.type, sdp: peer.localDescription?.sdp }),
            })
            if (!response.ok) {
                throw new Error(`bridge refused offer: ${response.status} ${await response.text()}`)
            }
            await peer.setRemoteDescription(await response.json())
            await waitOpen(control, openTimeoutMs)
            // a few quick samples so the bridge knows the clock offset before the first put
            for (let index = 0; index < initialClockPings; index++) {
                await this.#controlPing()
            }
            await this.#controlPing()
        } catch (error) {
            this.#onLost(generation)
            throw error
        }
        this.#setState("connected")
    }

    /** @param {RTCPeerConnection} peer */
    #attachHeartbeat(peer) {
        const label = JSON.stringify({ type: "heartbeat", opts: { hz: this.options.heartbeatHz, misses: this.options.heartbeatMisses } })
        const heartbeat = peer.createDataChannel(label, { ordered: false, maxRetransmits: 0 })
        heartbeat.onmessage = (event) => {
            try {
                const reply = JSON.parse(event.data)
                this.#addClockSample(reply.t0, reply.t1, reply.t2, this.now())
            } catch {
                // a malformed reply is just a lost sample
            }
        }
        this.#heartbeat = heartbeat
        if (!this.#heartbeatTimer) {
            this.#heartbeatTimer = setInterval(() => {
                const channel = this.#heartbeat
                if (this.#heartbeatPaused || channel?.readyState !== "open") {
                    return
                }
                channel.send(JSON.stringify({ t0: this.now(), offsetMs: this.clockOffsetMs, rttMs: this.rttMs }))
            }, 1000 / this.options.heartbeatHz)
        }
    }

    /** Stops sending heartbeats (the bridge then fires this frontend's deadmen); for testing deadman wiring. */
    pauseHeartbeat() {
        this.#heartbeatPaused = true
    }

    resumeHeartbeat() {
        this.#heartbeatPaused = false
    }

    /** @param {string} reason */
    #tripArmedPublishers(reason) {
        for (const publisher of this.#publishers.values()) {
            if (publisher.deadmanArmed) {
                publisher._trip(reason)
            }
        }
    }

    /** @param {number} generation */
    #onLost(generation) {
        if (generation !== this.#generation || this.state === "lost") {
            return
        }
        this.#setState("lost")
        // the bridge fires this frontend's deadmen when it loses us
        this.#tripArmedPublishers("disconnected")
        for (const [, request] of this.#requests) {
            clearTimeout(request.timer)
            request.reject(new Error("connection lost"))
        }
        this.#requests.clear()
        this.#peer?.close()
        if (this.options.reconnect && !this.#closed) {
            setTimeout(async () => {
                while (!this.#closed && generation === this.#generation) {
                    try {
                        await this._open()
                        return
                    } catch (error) {
                        console.warn("zenoh-web: reconnect failed", error)
                        await sleep(reconnectDelayMs)
                    }
                }
            }, reconnectDelayMs)
        }
    }

    /** @param {string} text */
    #onControlMessage(text) {
        let response
        try {
            response = JSON.parse(text)
        } catch {
            return
        }
        if (response.event === "tripped") {
            this.#publishers.get(response.id)?._trip(response.reason)
            return
        }
        const request = this.#requests.get(response.id)
        if (!request) {
            return
        }
        this.#requests.delete(response.id)
        clearTimeout(request.timer)
        if (response.ok) {
            request.resolve(response)
        } else {
            request.reject(new Error(`zenoh-web bridge: ${response.error ?? "error"}`))
        }
    }

    /**
     * @param {object & { op: string }} body
     * @param {number} timeoutMs
     * @returns {Promise<any>}
     */
    _request(body, timeoutMs) {
        const control = this.#control
        if (!control || control.readyState !== "open") {
            return Promise.reject(new Error(`not connected (${this.state})`))
        }
        const id = this.#nextId++
        return new Promise((resolve, reject) => {
            const timer = setTimeout(() => {
                this.#requests.delete(id)
                reject(new Error(`${body.op} timed out`))
            }, timeoutMs)
            this.#requests.set(id, { resolve, reject, timer })
            control.send(JSON.stringify({ id, ...body }))
        })
    }

    /**
     * @param {string} key key expression
     * @param {SubscribeOptions} options
     * @param {(message: Message) => void} callback
     */
    subscribe(key, options, callback) {
        validateSubscribeOptions(options ?? {})
        const subscription = new Subscription(this, this.#nextId++, key, { ...options }, callback)
        this.#addEndpoint(subscription)
        return subscription
    }

    /**
     * @param {string} key
     * @param {PublisherOptions} options
     */
    publisher(key, options = {}) {
        validatePublisherOptions(options)
        const publisher = new Publisher(this, this.#nextId++, key, { ...options })
        this.#publishers.set(publisher.id, publisher)
        this.#addEndpoint(publisher)
        return publisher
    }

    /** @param {Subscription | Publisher} endpoint */
    #addEndpoint(endpoint) {
        this.#endpoints.add(endpoint)
        if (this.#peer && this.#peer.connectionState !== "closed") {
            endpoint.attach(this.#peer)
        }
    }

    /** @param {Subscription | Publisher} endpoint */
    _forget(endpoint) {
        this.#endpoints.delete(endpoint)
        if (endpoint instanceof Publisher && endpoint.closed) {
            this.#publishers.delete(endpoint.id)
        }
        this.#refreshStats(null)
    }

    /**
     * zenoh query.
     * @param {string} key
     * @param {{ timeoutMs?: number }} options
     * @returns {Promise<{ key: string | null, bytes: Uint8Array, error?: boolean }[]>}
     */
    async get(key, { timeoutMs = 5000 } = {}) {
        const response = await this._request({ op: "get", key, timeoutMs }, timeoutMs + 2000)
        return response.replies.map((/** @type {any} */ reply) => {
            if (reply.error !== undefined) {
                return { key: null, bytes: fromBase64(reply.error), error: true }
            }
            return { key: reply.key, bytes: fromBase64(reply.bytes) }
        })
    }

    /** Polls bridge stats (and, without a heartbeat, clock sync) once; also runs on a timer while connected. */
    async pollStats() {
        try {
            await this.#controlPing()
            if (this.state === "degraded" && this.#peer?.connectionState === "connected") {
                this.#setState("connected")
            }
        } catch {
            if (this.state === "connected") {
                this.#setState("degraded")
            }
            return
        }
        const response = await this._request({ op: "stats" }, pingTimeoutMs).catch(() => null)
        if (response) {
            this.bridgeStats = { clock: response.clock, heartbeat: response.heartbeat }
        }
        this.#refreshStats(response?.channels ?? null)
    }

    /** @param {{ id: number, stats: object }[] | null} bridgeChannels */
    #refreshStats(bridgeChannels) {
        if (bridgeChannels) {
            const byId = new Map(bridgeChannels.map((channel) => [channel.id, channel]))
            for (const endpoint of [...this.#endpoints, ...this.#publishers.values()]) {
                endpoint.bridgeStats = byId.get(endpoint.id) ?? null
            }
        }
        /** @type {Record<string, KeyStats>} */
        const stats = {}
        for (const endpoint of this.#endpoints) {
            const entry = stats[endpoint.key] ??= { received: 0, dropped: 0, backlogBytes: 0, rttMs: this.rttMs, bridge: null }
            const bridge = /** @type {any} */ (endpoint.bridgeStats)
            entry.bridge = bridge
            if (endpoint instanceof Subscription) {
                entry.received += endpoint.received
                entry.dropped += endpoint.dropped
                entry.backlogBytes += bridge ? bridge.stats.queuedBytes + bridge.stats.outstandingBytes : 0
            } else {
                entry.received += endpoint.sent
                entry.dropped += endpoint.dropped + (bridge ? bridge.stats.droppedStale : 0)
                entry.backlogBytes += endpoint.channel?.bufferedAmount ?? 0
            }
        }
        this.stats = stats
    }

    _startStats() {
        this.#statsTimer = setInterval(() => {
            if (this.state === "connected" || this.state === "degraded") {
                this.pollStats().catch(() => {})
            }
        }, this.options.statsIntervalMs)
    }

    close() {
        this.#closed = true
        if (this.#statsTimer) {
            clearInterval(this.#statsTimer)
        }
        if (this.#heartbeatTimer) {
            clearInterval(this.#heartbeatTimer)
        }
        for (const endpoint of [...this.#endpoints]) {
            endpoint.close()
        }
        this.#generation++
        this.#peer?.close()
        this.#setState("lost")
    }
}

/**
 * Connects to a zenoh-web bridge.
 * @param {string} url e.g. "http://robot.local:7448"
 * @param {ConnectOptions} options
 * @returns {Promise<ZenohWeb>}
 */
export async function connect(url, options = {}) {
    const client = new ZenohWeb(url, options)
    await client._open()
    client._startStats()
    return client
}
