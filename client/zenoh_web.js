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
 * @typedef {"latest" | "reliable" | { queue?: number, maxAgeMs?: number }} Delivery
 * @typedef {"connecting" | "connected" | "degraded" | "lost"} ConnectionState
 * @typedef {{ key: string, bytes: Uint8Array, timestamp: number, seq: number }} Message
 * @typedef {{
 *     delivery?: Delivery,
 *     priority?: number,
 *     hz?: [number, number],
 *     quality?: [number, number],
 *     tradeoff?: number,
 * }} SubscribeOptions
 * @typedef {{ delivery?: "latest" | "reliable", priority?: number, repeatMs?: number }} PublisherOptions
 * @typedef {{ received: number, dropped: number, backlogBytes: number, rttMs: number | null, bridge: object | null }} KeyStats
 * @typedef {{ iceServers?: RTCIceServer[], reconnect?: boolean, statsIntervalMs?: number }} ConnectOptions
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

/**
 * @param {Delivery | undefined} delivery
 * @returns {{ queue: number, maxAgeMs: number | null, reliable: boolean }}
 */
function normalizeDelivery(delivery) {
    if (delivery === "reliable") {
        return { queue: Infinity, maxAgeMs: null, reliable: true }
    }
    if (delivery == null || typeof delivery === "string") {
        return { queue: 1, maxAgeMs: null, reliable: false }
    }
    const maxAgeMs = delivery.maxAgeMs > 0 ? delivery.maxAgeMs : null
    const queue = delivery.queue ?? (maxAgeMs ? Infinity : 1)
    return { queue, maxAgeMs, reliable: false }
}

/**
 * Delivery mode -> data channel reliability (SPEC "Delivery -> transport mapping").
 * @param {{ maxAgeMs: number | null, reliable: boolean }} delivery
 * @returns {RTCDataChannelInit}
 */
function channelInit(delivery) {
    if (delivery.reliable) {
        return { ordered: true }
    }
    if (delivery.maxAgeMs) {
        return { ordered: false, maxPacketLifeTime: Math.min(65535, Math.round(delivery.maxAgeMs)) }
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

    /** Resolves once the current data channel is open (rejects if it fails to open). */
    ready() {
        return this.#ready
    }

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
        this.delivery = normalizeDelivery(options.delivery)
    }

    /** samples the bridge accepted for us but we never got (queue, age, hz cap or network) */
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
        const { delivery, priority, hz, quality, tradeoff } = this.options
        const label = JSON.stringify({ type: "sub", key: this.key, id: this.id, opts: { delivery, priority, hz, quality, tradeoff } })
        const channel = peer.createDataChannel(label, channelInit(this.delivery))
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
    /** @type {Uint8Array[]} values waiting for the channel (reliable: all, latest: only the newest) */
    #pending = []
    /** @type {ReturnType<typeof setInterval> | null} */
    #repeatTimer = null
    sent = 0
    dropped = 0
    closed = false
    /** @type {Promise<void>} */
    #ready = Promise.resolve()

    /** Resolves once the current data channel is open (rejects if it fails to open). */
    ready() {
        return this.#ready
    }

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
        this.delivery = normalizeDelivery(options.delivery)
        if (options.repeatMs > 0) {
            this.#repeatTimer = setInterval(() => {
                if (this.#last) {
                    this.#send(this.#last)
                }
            }, options.repeatMs)
        }
    }

    /** @param {RTCPeerConnection} peer */
    attach(peer) {
        const { delivery, priority } = this.options
        const label = JSON.stringify({ type: "pub", key: this.key, id: this.id, opts: { delivery, priority } })
        const channel = peer.createDataChannel(label, channelInit(this.delivery))
        channel.binaryType = "arraybuffer"
        channel.bufferedAmountLowThreshold = resumeBytes
        channel.onopen = () => this.#flush()
        channel.onbufferedamountlow = () => this.#flush()
        this.channel = channel
        this.#ready = waitOpen(channel, openTimeoutMs)
        this.#ready.catch(() => {})
    }

    /** @param {Uint8Array | ArrayBuffer | ArrayBufferView | string} value */
    put(value) {
        if (this.closed) {
            throw new Error(`publisher ${this.key} is closed`)
        }
        const bytes = toBytes(value)
        this.#last = bytes
        this.#send(bytes)
    }

    /** @param {Uint8Array} bytes */
    #send(bytes) {
        const channel = this.channel
        const backedUp = !channel || channel.readyState !== "open" || channel.bufferedAmount > backedUpBytes
        if (backedUp || this.#pending.length > 0) {
            if (!this.delivery.reliable) {
                this.dropped += this.#pending.length
                this.#pending = []
            }
            this.#pending.push(bytes)
            return
        }
        channel.send(bytes)
        this.sent++
    }

    #flush() {
        const channel = this.channel
        while (this.#pending.length > 0 && channel?.readyState === "open" && channel.bufferedAmount <= backedUpBytes) {
            channel.send(/** @type {Uint8Array} */ (this.#pending.shift()))
            this.sent++
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
    /** @type {number | null} round trip over the control channel */
    rttMs = null
    /** @type {RTCPeerConnection | null} */
    #peer = null
    /** @type {RTCDataChannel | null} */
    #control = null
    /** @type {Set<Subscription | Publisher>} */
    #endpoints = new Set()
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
        this.options = { iceServers: [], reconnect: true, statsIntervalMs: 1000, ...options }
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

    /** Opens (or re-opens) the peer connection and every channel on it. */
    async _open() {
        const generation = ++this.#generation
        this.#setState("connecting")
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
        } catch (error) {
            this.#onLost(generation)
            throw error
        }
        this.#setState("connected")
    }

    /** @param {number} generation */
    #onLost(generation) {
        if (generation !== this.#generation || this.state === "lost") {
            return
        }
        this.#setState("lost")
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
        const request = this.#requests.get(response.id)
        if (!request) {
            return
        }
        this.#requests.delete(response.id)
        clearTimeout(request.timer)
        if (response.ok) {
            request.resolve(response)
        } else {
            request.reject(new Error(response.error ?? "bridge error"))
        }
    }

    /**
     * @param {object} body
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
        const subscription = new Subscription(this, this.#nextId++, key, options ?? {}, callback)
        this.#addEndpoint(subscription)
        return subscription
    }

    /**
     * @param {string} key
     * @param {PublisherOptions} options
     */
    publisher(key, options = {}) {
        const publisher = new Publisher(this, this.#nextId++, key, options)
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
        return response.replies.map((reply) => {
            if (reply.error !== undefined) {
                return { key: null, bytes: fromBase64(reply.error), error: true }
            }
            return { key: reply.key, bytes: fromBase64(reply.bytes) }
        })
    }

    /** Polls bridge stats and round trip once; also runs on a timer while connected. */
    async pollStats() {
        const started = performance.now()
        try {
            await this._request({ op: "ping" }, pingTimeoutMs)
            this.rttMs = performance.now() - started
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
        this.#refreshStats(response?.channels ?? null)
    }

    /** @param {{ id: number, stats: object }[] | null} bridgeChannels */
    #refreshStats(bridgeChannels) {
        if (bridgeChannels) {
            const byId = new Map(bridgeChannels.map((channel) => [channel.id, channel.stats]))
            for (const endpoint of this.#endpoints) {
                if (endpoint instanceof Subscription) {
                    endpoint.bridgeStats = byId.get(endpoint.id) ?? null
                }
            }
        }
        /** @type {Record<string, KeyStats>} */
        const stats = {}
        for (const endpoint of this.#endpoints) {
            const entry = stats[endpoint.key] ??= { received: 0, dropped: 0, backlogBytes: 0, rttMs: this.rttMs, bridge: null }
            if (endpoint instanceof Subscription) {
                const bridge = /** @type {any} */ (endpoint.bridgeStats)
                entry.received += endpoint.received
                entry.dropped += endpoint.dropped
                entry.backlogBytes += bridge ? bridge.queuedBytes + bridge.outstandingBytes : 0
                entry.bridge = bridge
            } else {
                entry.received += endpoint.sent
                entry.dropped += endpoint.dropped
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
