/// <reference no-default-lib="true" />
/// <reference lib="dom" />
/// <reference lib="dom.iterable" />
/// <reference lib="esnext" />
// zenoh-gateway browser client: one WebRTC data channel per subscription/publisher, see SPEC.md

import { decompress as zstdDecompress } from "./vendor/fzstd.ts"

/** zenoh priorities (lower = more important). */
export const Priority = Object.freeze({
    REAL_TIME: 1,
    INTERACTIVE_HIGH: 2,
    INTERACTIVE_LOW: 3,
    DATA_HIGH: 4,
    DATA: 5,
    DATA_LOW: 6,
    BACKGROUND: 7,
} as const)

export type Delivery = "latest" | "reliable"
export type ConnectionState = "connecting" | "connected" | "degraded" | "lost"
export type PublisherState = "connecting" | "open" | "tripped" | "rejected" | "closed"
export type SubscriptionState = "connecting" | "open" | "rejected" | "closed"

/** What a message encoding produces on its default channel: pictures (`video`), sound (`audio`), or bytes on the data channel (`fields`: decoded by this client). */
export type EncodingOutput = "video" | "audio" | "data" | "fields"

/** What a subscription's messages travel on: a video track of one format, the Opus audio track, or the data channel. */
export type Channel = "video-h264" | "video-vp8" | "video-vp9" | "video-av1" | "audio-opus" | "data"

/** A message encoding the gateway has registered. */
export interface EncodingInfo {
    name: string
    output: EncodingOutput
}

/** Turns a data-channel encoding's bytes into what `msg.decoded` carries. */
export type EncodingDecoder = (bytes: Uint8Array, message: Message) => unknown

const encodingDecoders = new Map<string, EncodingDecoder>()

/**
 * Registers the browser decoder for a data-channel encoding the gateway runs (a Rust encoding the host application
 * added with `ServerBuilder::encoding`). Messages of subscriptions using that encoding on the data channel get
 * `msg.decoded = decoder(msg.bytes, msg)` (fields messages decode by themselves). Video and audio need no decoder.
 * Registering a different decoder under a name that already has one throws.
 */
export function registerEncoding(name: string, decoder: EncodingDecoder): void {
    if (typeof name !== "string" || name.length === 0) {
        throw new TypeError(`zenoh-gateway: registerEncoding needs an encoding name, got ${String(name)}`)
    }
    if (typeof decoder !== "function") {
        throw new TypeError(`zenoh-gateway: registerEncoding("${name}") needs a decoder function`)
    }
    const existing = encodingDecoders.get(name)
    if (existing !== undefined && existing !== decoder) {
        throw new Error(`zenoh-gateway: a decoder for encoding "${name}" is already registered`)
    }
    encodingDecoders.set(name, decoder)
}

/** The channel a subscription uses: its own, else where the encoding's output goes by default. */
function channelOf(options: SubscribeOptions, encodings: readonly EncodingInfo[]): Channel {
    if (options.channel !== undefined) {
        return options.channel
    }
    const output = options.encoding === undefined ? undefined : encodings.find((info) => info.name === options.encoding)?.output
    return output === "video" ? "video-h264" : output === "audio" ? "audio-opus" : "data"
}

/** The RTP mime type a media channel carries. */
const channelMimes: Record<Exclude<Channel, "data">, string> = { "video-h264": "video/H264", "video-vp8": "video/VP8", "video-vp9": "video/VP9", "video-av1": "video/AV1", "audio-opus": "audio/opus" }

/** Per-frame details of a video subscription; the pixels are on `mediaStream`. */
export interface VideoFrameInfo {
    width: number
    height: number
    sourceWidth: number
    sourceHeight: number
    quality: number
    keyframe: boolean
    encodedBytes: number
}

export interface Message {
    key: string
    /** "delete" for a delete sample (its bytes are empty) */
    kind: "put" | "delete"
    /** the payload (decompressed): raw sample bytes, or the encoding's output */
    bytes: Uint8Array
    /** the sample's encoding, when not the default (raw messages only) */
    encoding?: string
    /** the sample's attachment (raw messages only) */
    attachment?: Uint8Array
    timestamp: number
    seq: number
    /** fields messages: the decoded fields (see `decodeFields`); other data-channel encodings: what the encoding's registered decoder returned (see `registerEncoding`) */
    decoded?: unknown
    video?: VideoFrameInfo
    mediaStream?: MediaStream
}

export interface SubscribeOptions {
    delivery?: Delivery
    priority?: number
    /** when bandwidth is short, higher keeps more bandwidth and quality (default 1; 0 gives up everything first) */
    bandwidthPriority?: number
    maxAge?: number
    maxHz?: number
    minQuality?: number
    qualityToHzTradeoff?: number
    /** a message encoding the gateway registered (`ZenohGateway.encodings`); none = raw bytes */
    encoding?: string
    /** what the messages travel on (default: where the encoding's output goes: pictures on video-h264, sound on audio-opus, else data) */
    channel?: Channel
    /** passed to the encoding; `quality` (0..1, default 1) is the most the bandwidth allocator may pick */
    encodeOptions?: { quality?: number, [option: string]: unknown }
    /** data-channel compression (default: the encoding's, none without one); not for video or audio */
    compress?: "zstd" | "none"
    /** video channels: most bits/s the stream asks for (default: the server's) */
    maxBitrate?: number
    /** video channels: smallest share of the source's width and height the picture may shrink to (default 0.25) */
    minResolutionScale?: number
    /** video channels: [width, height] box the picture is fitted into */
    maxResolution?: [number, number]
    /** video channels: [min, max] ms the browser may hold a frame to smooth out jitter (default [0, 0]: show at once) */
    playoutDelay?: [number, number]
}

/** What `Subscription.update` may change on a running subscription; `null` puts an option back to its default. */
export type SubscriptionUpdate = {
    [option in "maxHz" | "minQuality" | "qualityToHzTradeoff" | "bandwidthPriority" | "maxBitrate" | "minResolutionScale" | "maxResolution" | "playoutDelay"]?:
        SubscribeOptions[option] | null
} & { encodeOptions?: { quality?: number } }

export interface PublisherOptions {
    delivery?: Delivery
    priority?: number
    repeatMs?: number
    latencyLimit?: number
}

export interface ConnectOptions {
    /** default: the gateway's (`GET /zenoh-gateway/ice`, TURN credentials minted for this client) */
    iceServers?: RTCIceServer[]
    /** "relay" forces every byte through TURN; default: the `/zenoh-gateway/ice` reply's `iceTransportPolicy`, else "all" */
    iceTransportPolicy?: RTCIceTransportPolicy
    /** sent as `Authorization: Bearer <token>`; the gateway's authorize hook turns it into a grant */
    token?: string
    reconnect?: boolean
    statsIntervalMs?: number
    heartbeatHz?: number
    heartbeatMisses?: number
    clock?: () => number
}

export interface KeyStats {
    received: number
    dropped: number
    backlogBytes: number
    rttMs: number | null
    gateway: GatewayChannelStats | null
}

/** One channel as the gateway reports it in `stats`. */
export interface GatewayChannelStats {
    id: number | null
    type: string
    key: string
    opts: Record<string, unknown>
    stats: Record<string, number | boolean | string | null>
    /** subscriptions: this stream's share of the frontend's bandwidth (SPEC "Bandwidth allocation") */
    allocation: Record<string, number | boolean | null> | null
}

/** The gateway's `clock`, `heartbeat` and `bandwidth` stats (SPEC "Stats"). */
export type GatewayStats = Record<string, Record<string, unknown> | null>

export interface GetReply {
    key: string | null
    bytes: Uint8Array
    error?: boolean
    /** a delete reply */
    kind?: "put" | "delete"
    /** the sample's (or the error's) encoding */
    encoding?: string
    attachment?: Uint8Array
    /** the sample's timestamp, unix ms */
    timestamp?: number
}

export type TopicSource = "token" | "advancedPublisher" | "sample"

export interface Topic {
    key: string
    sources: TopicSource[]
}

type Bytesish = Uint8Array | ArrayBuffer | ArrayBufferView | string

interface ControlResponse {
    id?: number
    ok?: boolean
    error?: string
    event?: "tripped" | "rejected" | "accepted" | "leaseLost" | "closed" | "query" | "liveliness" | "matching"
    reason?: string
    [field: string]: unknown
}

interface PendingRequest {
    resolve: (response: ControlResponse) => void
    reject: (error: Error) => void
    timer: ReturnType<typeof setTimeout>
}

interface ClockSample {
    offsetMs: number
    rttMs: number
}

interface PartialMessage {
    key: string
    timestamp: number
    flags: number
    chunks: (Uint8Array | undefined)[]
    receivedChunks: number
    receivedBytes: number
}

const backedUpBytes = 64 * 1024
const resumeBytes = 16 * 1024
const gatherTimeoutMs = 3000
const openTimeoutMs = 10000
const pingTimeoutMs = 3000
const reconnectDelayMs = 1000
// consumption acks let the gateway stop sending while this page's JS is behind
const ackEveryBytes = 16 * 1024
const ackDelayMs = 5
// clock sync keeps the lowest-RTT sample among the most recent ones
const clockWindow = 16
const initialClockPings = 5
const putHeaderBytes = 8
// incomplete chunked messages kept per subscription before the oldest is dropped
const maxPartialMessages = 8

/** Delivery -> data channel reliability (SPEC "Delivery -> transport mapping"). */
function channelInit(delivery: Delivery | undefined, maxAge: number | undefined): RTCDataChannelInit {
    if (delivery === "reliable") {
        return { ordered: true }
    }
    if (maxAge) {
        return { ordered: false, maxPacketLifeTime: Math.min(65535, Math.round(maxAge)) }
    }
    return { ordered: false, maxRetransmits: 0 }
}

function toBytes(value: Bytesish): Uint8Array {
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

function fromBase64(text: string): Uint8Array {
    const binary = atob(text)
    const bytes = new Uint8Array(binary.length)
    for (let index = 0; index < binary.length; index++) {
        bytes[index] = binary.charCodeAt(index)
    }
    return bytes
}

function toBase64(bytes: Uint8Array): string {
    let binary = ""
    for (let index = 0; index < bytes.length; index++) {
        binary += String.fromCharCode(bytes[index])
    }
    return btoa(binary)
}

const keyDecoder = new TextDecoder()

export interface Frame {
    key: string
    timestamp: number
    seq: number
    frameId: number
    chunkIndex: number
    chunkCount: number
    /** bit0: the whole message is zstd-compressed */
    flags: number
    chunk: Uint8Array
}

const zstdFlag = 1
/** frame flag: the message is zenoh-gateway fields */
const fieldsFlag = 2
/** frame flag: the sample is a delete */
const deleteFlag = 4
/** frame flag: the message starts with the sample's encoding and attachment (see `splitMeta`) */
const metaFlag = 8

/** u16 encodingLen | encoding | u32 attachmentLen | attachment | payload */
function splitMeta(message: Uint8Array): { encoding: string, attachment?: Uint8Array, payload: Uint8Array } {
    const view = new DataView(message.buffer, message.byteOffset, message.byteLength)
    const encodingLength = view.getUint16(0, true)
    const encoding = keyDecoder.decode(message.subarray(2, 2 + encodingLength))
    const attachmentLength = view.getUint32(2 + encodingLength, true)
    const attachmentStart = 6 + encodingLength
    const attachment = attachmentLength > 0 ? message.slice(attachmentStart, attachmentStart + attachmentLength) : undefined
    return { encoding, attachment, payload: message.subarray(attachmentStart + attachmentLength) }
}

/**
 * Gateway frame (little endian):
 * u16 keyLen | key | f64 timestampMs | u32 seq | u32 frameId | u32 chunkIndex | u32 chunkCount | u8 flags | chunk
 */
export function decodeFrame(buffer: ArrayBuffer): Frame {
    const view = new DataView(buffer)
    const keyLength = view.getUint16(0, true)
    const key = keyDecoder.decode(new Uint8Array(buffer, 2, keyLength))
    const offset = 2 + keyLength
    return {
        key,
        timestamp: view.getFloat64(offset, true),
        seq: view.getUint32(offset + 8, true),
        frameId: view.getUint32(offset + 12, true),
        chunkIndex: view.getUint32(offset + 16, true),
        chunkCount: view.getUint32(offset + 20, true),
        flags: view.getUint8(offset + 24),
        chunk: new Uint8Array(buffer, offset + 25),
    }
}

/** A decoded fields value: one number, text, or the elements' values (scaled fields: Float32Array). */
export type FieldValue = number | string | Uint8Array | Int8Array | Uint16Array | Int16Array | Uint32Array | Int32Array | Float32Array | Float64Array

const fieldArrays = [Uint8Array, Int8Array, Uint16Array, Int16Array, Uint32Array, Int32Array, Float32Array, Float64Array]

/**
 * A fields message (SPEC "Fields") as an object of named values: scalar fields are numbers, text
 * fields strings, others typed arrays viewing the message (copied once if misaligned).
 */
export function decodeFields(message: Uint8Array): Record<string, FieldValue> {
    const bytes = message.byteOffset % 8 === 0 ? message : message.slice()
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    if (bytes[0] !== 1) {
        throw new Error(`zenoh-gateway: unknown fields format version ${bytes[0]}`)
    }
    const fields: Record<string, FieldValue> = {}
    let offset = 2
    for (let field = 0; field < bytes[1]; field++) {
        const name = keyDecoder.decode(bytes.subarray(offset + 1, offset + 1 + bytes[offset]))
        offset += 1 + bytes[offset]
        const [dtype, components, flags] = [bytes[offset], bytes[offset + 1], bytes[offset + 2]]
        const length = view.getUint32(offset + 3, true) * components
        offset += 7
        if (dtype === 8) {
            fields[name] = keyDecoder.decode(bytes.subarray(offset, offset + length))
            offset += length
            continue
        }
        const TypedArray = fieldArrays[dtype]
        if (TypedArray === undefined) {
            throw new Error(`zenoh-gateway: field ${name} has unknown dtype ${dtype}`)
        }
        const scaling = (flags & 1) === 1 ? Array.from({ length: 2 * components }, (_, index) => view.getFloat64(offset + 8 * index, true)) : null
        offset += scaling === null ? 0 : 16 * components
        offset = Math.ceil(offset / TypedArray.BYTES_PER_ELEMENT) * TypedArray.BYTES_PER_ELEMENT
        let values: FieldValue = new TypedArray(bytes.buffer as ArrayBuffer, bytes.byteOffset + offset, length)
        offset += length * TypedArray.BYTES_PER_ELEMENT
        if (scaling !== null) {
            const quantized = values
            values = new Float32Array(length)
            for (let index = 0; index < length; index++) {
                const component = index % components
                values[index] = scaling[component] + quantized[index] * scaling[components + component]
            }
        }
        fields[name] = (flags & 2) === 2 ? values[0] : values
    }
    return fields
}

/** Video metadata frame (28 bytes) sent on a video subscription's channel per frame. */
export function decodeVideoFrameInfo(bytes: Uint8Array): VideoFrameInfo {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    return {
        keyframe: (bytes[1] & 1) === 1,
        width: view.getUint32(4, true),
        height: view.getUint32(8, true),
        sourceWidth: view.getUint32(12, true),
        sourceHeight: view.getUint32(16, true),
        quality: view.getFloat32(20, true),
        encodedBytes: view.getUint32(24, true),
    }
}

/** Browser -> gateway put: f64 sentAtMs (browser clock) | payload (little endian). */
export function encodePut(payload: Uint8Array, sentAtMs: number): Uint8Array<ArrayBuffer> {
    const frame = new Uint8Array(putHeaderBytes + payload.length)
    new DataView(frame.buffer).setFloat64(0, sentAtMs, true)
    frame.set(payload, putHeaderBytes)
    return frame
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

function waitOpen(channel: RTCDataChannel, timeoutMs: number): Promise<void> {
    if (channel.readyState === "open") {
        return Promise.resolve()
    }
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error("data channel open timed out")), timeoutMs)
        channel.addEventListener("open", () => {
            clearTimeout(timer)
            resolve()
        }, { once: true })
        channel.addEventListener("close", () => {
            clearTimeout(timer)
            reject(new Error("data channel closed before opening"))
        }, { once: true })
    })
}

function waitIceGathering(peer: RTCPeerConnection): Promise<void> {
    if (peer.iceGatheringState === "complete") {
        return Promise.resolve()
    }
    return new Promise((resolve) => {
        const finish = () => {
            clearTimeout(timer)
            peer.removeEventListener("icegatheringstatechange", onChange)
            resolve()
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

/**
 * Settles once the channel is usable: the gateway accepted it AND it is open in this browser.
 * Both are needed because the gateway's `accepted` (control channel) can arrive before this
 * channel's own open (its DCEP ack travels on a different SCTP stream). Rejects if the gateway
 * rejects it or it fails to open.
 */
class Acceptance {
    promise: Promise<void>
    settled = false
    #gatewayAccepted = false
    #channelOpen = false
    #resolve: () => void = () => {}
    #reject: (error: Error) => void = () => {}

    constructor() {
        this.promise = new Promise<void>((resolve, reject) => {
            this.#resolve = resolve
            this.#reject = reject
        })
        this.promise.catch(() => {})
    }

    gatewayAccepted(): void {
        this.#gatewayAccepted = true
        this.#settleIfReady()
    }

    channelOpened(): void {
        this.#channelOpen = true
        this.#settleIfReady()
    }

    #settleIfReady(): void {
        if (!this.settled && this.#gatewayAccepted && this.#channelOpen) {
            this.settled = true
            this.#resolve()
        }
    }

    reject(error: Error): void {
        if (!this.settled) {
            this.settled = true
            this.#reject(error)
        }
    }
}

/** Common to subscriptions and publishers: a channel the gateway accepts or rejects. */
abstract class Endpoint {
    channel: RTCDataChannel | null = null
    closed = false
    rejectionReason: string | null = null
    gatewayStats: GatewayChannelStats | null = null
    protected acceptance = new Acceptance()

    constructor(readonly owner: ZenohGateway, readonly id: number, readonly key: string) {}

    abstract attach(peer: RTCPeerConnection): void

    /** Resolves once the gateway accepted this channel; rejects with the gateway's reason otherwise. */
    ready(): Promise<void> {
        return this.acceptance.promise
    }

    /** Starts a new attempt to get accepted (each attach, including reconnects). */
    protected beginAttempt(): Acceptance {
        this.acceptance = new Acceptance()
        return this.acceptance
    }

    protected watchChannel(channel: RTCDataChannel, acceptance: Acceptance): void {
        waitOpen(channel, openTimeoutMs).then(() => acceptance.channelOpened(), (error: Error) => acceptance.reject(error))
    }

    _accepted(): void {
        this.acceptance.gatewayAccepted()
    }

    _rejected(reason: string): void {
        this.rejectionReason = reason
        this.acceptance.reject(new Error(`zenoh-gateway: gateway rejected ${this.key}: ${reason}`))
        this.owner._forget(this)
    }

    close(): void {
        if (this.closed) {
            return
        }
        this.closed = true
        this.channel?.close()
        this.owner._forget(this)
    }
}

export class Subscription extends Endpoint {
    received = 0
    /** chunked messages dropped incomplete (lost chunk or abandoned for a newer message) */
    partialDropped = 0
    /** encoded payloads that failed to decode in this page */
    decodeErrors = 0
    /** video and audio channels: the track (also on each message as `mediaStream`) */
    mediaStream: MediaStream | null = null
    /** what the messages travel on */
    readonly channelName: Channel
    #warnedNoDecoder = false
    #transceiver: RTCRtpTransceiver | null = null
    /** drops before the current channel (each new channel restarts seq at 0) */
    #droppedBefore = 0
    #firstSeq = -1
    #maxSeq = -1
    #receivedOnChannel = 0
    #highestConsumedFrame = -1
    #bytesSinceAck = 0
    #ackTimer: ReturnType<typeof setTimeout> | null = null
    #partials = new Map<number, PartialMessage>()

    constructor(owner: ZenohGateway, id: number, key: string, public options: SubscribeOptions, readonly callback: (message: Message) => void) {
        super(owner, id, key)
        this.channelName = channelOf(options, owner.encodings)
    }

    /**
     * Changes the running subscription's options in place: same channel and track, no resubscribe; the gateway's
     * next frame uses them. Reconnects keep them too.
     */
    async update(changes: SubscriptionUpdate): Promise<void> {
        await this.ready()
        await this.owner._request({ op: "updateSubscription", subId: this.id, opts: changes }, pingTimeoutMs)
        const options: Record<string, unknown> = { ...this.options }
        for (const [name, value] of Object.entries(changes)) {
            if (name === "encodeOptions") {
                options.encodeOptions = { ...this.options.encodeOptions, ...(value as object) }
            } else if (value === null) {
                delete options[name]
            } else {
                options[name] = value
            }
        }
        this.options = options as SubscribeOptions
    }

    get state(): SubscriptionState {
        if (this.closed) {
            return "closed"
        }
        if (this.rejectionReason !== null) {
            return "rejected"
        }
        return this.acceptance.settled ? "open" : "connecting"
    }

    /** messages the gateway accepted for us but we never got whole (queue, age, maxHz, network) */
    get dropped(): number {
        const span = this.#maxSeq < 0 ? 0 : this.#maxSeq - this.#firstSeq + 1
        return this.#droppedBefore + Math.max(0, span - this.#receivedOnChannel)
    }

    attach(peer: RTCPeerConnection): void {
        this.#droppedBefore = this.dropped
        this.#firstSeq = -1
        this.#maxSeq = -1
        this.#receivedOnChannel = 0
        this.#highestConsumedFrame = -1
        this.#bytesSinceAck = 0
        this.#partials.clear()
        const acceptance = this.beginAttempt()
        const channelName = this.channelName
        if (channelName === "data") {
            this.#openChannel(peer, acceptance, null)
            return
        }
        // a recvonly transceiver (renegotiated with the gateway for this channel's format) carries the frames
        this.#transceiver = null
        const kind = channelName === "audio-opus" ? "audio" : "video"
        const mime = channelMimes[channelName]
        const playable = globalThis.RTCRtpReceiver?.getCapabilities?.(kind)?.codecs.some((codec) => codec.mimeType.toLowerCase() === mime.toLowerCase()) ?? true
        if (!playable) {
            acceptance.reject(new Error(`zenoh-gateway: this browser can't play ${channelName} (no ${mime} decoder); pick another channel`))
            return
        }
        this.owner._acquireTransceiver(peer, kind, channelName).then((transceiver) => {
            if (this.closed || acceptance !== this.acceptance) {
                this.owner._releaseTransceiver(peer, transceiver)
                return
            }
            this.#transceiver = transceiver
            this.mediaStream = new MediaStream([transceiver.receiver.track])
            this.#openChannel(peer, acceptance, transceiver.mid)
        }, (error: Error) => acceptance.reject(new Error(`zenoh-gateway: ${channelName} renegotiation for ${this.key} failed: ${error.message}`)))
    }

    #openChannel(peer: RTCPeerConnection, acceptance: Acceptance, mid: string | null): void {
        const label = JSON.stringify({ type: "sub", key: this.key, id: this.id, opts: this.options, ...(mid === null ? {} : { mid }) })
        const channel = peer.createDataChannel(label, channelInit(this.options.delivery, this.options.maxAge))
        channel.binaryType = "arraybuffer"
        channel.onmessage = (event: MessageEvent) => {
            if (event.data instanceof ArrayBuffer) {
                this.#onFrame(channel, decodeFrame(event.data), event.data.byteLength)
            }
        }
        this.channel = channel
        this.watchChannel(channel, acceptance)
    }

    override close(): void {
        if (this.closed) {
            return
        }
        super.close()
        const peer = this.owner._peer
        if (this.#transceiver && peer) {
            this.owner._releaseTransceiver(peer, this.#transceiver)
        }
        this.#transceiver = null
    }

    #onFrame(channel: RTCDataChannel, frame: Frame, frameBytes: number): void {
        if (frame.chunkCount <= 1) {
            this.#deliver({ key: frame.key, kind: "put", bytes: frame.chunk, timestamp: frame.timestamp, seq: frame.seq }, frame.flags)
        } else {
            this.#addChunk(frame)
        }
        this.#consumed(channel, frame.frameId, frameBytes)
    }

    #addChunk(frame: Frame): void {
        let partial = this.#partials.get(frame.seq)
        if (!partial) {
            partial = { key: frame.key, timestamp: frame.timestamp, flags: frame.flags, chunks: new Array(frame.chunkCount), receivedChunks: 0, receivedBytes: 0 }
            this.#partials.set(frame.seq, partial)
            this.#evictPartials(maxPartialMessages)
        }
        if (partial.chunks[frame.chunkIndex] === undefined) {
            // the frame's buffer is reused by nothing else, but copy so a partial never pins big buffers
            partial.chunks[frame.chunkIndex] = frame.chunk.slice()
            partial.receivedChunks++
            partial.receivedBytes += frame.chunk.length
        }
        if (partial.receivedChunks < partial.chunks.length) {
            return
        }
        this.#partials.delete(frame.seq)
        const bytes = new Uint8Array(partial.receivedBytes)
        let offset = 0
        for (const chunk of partial.chunks) {
            const piece = chunk as Uint8Array
            bytes.set(piece, offset)
            offset += piece.length
        }
        // anything older that is still incomplete can only be stale now
        for (const seq of [...this.#partials.keys()]) {
            if (seq < frame.seq) {
                this.#partials.delete(seq)
                this.partialDropped++
            }
        }
        this.#deliver({ key: partial.key, kind: "put", bytes, timestamp: partial.timestamp, seq: frame.seq }, partial.flags)
    }

    #evictPartials(limit: number): void {
        while (this.#partials.size > limit) {
            const oldest = Math.min(...this.#partials.keys())
            this.#partials.delete(oldest)
            this.partialDropped++
        }
    }

    /** Adds the encoding's decoded form; false if it can't be decoded. */
    #decode(message: Message, flags: number): boolean {
        try {
            if (this.channelName !== "data") {
                message.video = this.channelName.startsWith("video-") ? decodeVideoFrameInfo(message.bytes) : undefined
                message.mediaStream = this.mediaStream ?? undefined
                return true
            }
            if ((flags & fieldsFlag) !== 0) {
                message.decoded = decodeFields(message.bytes)
                return true
            }
            const name = String(this.options.encoding)
            const decoder = encodingDecoders.get(name)
            if (decoder !== undefined) {
                message.decoded = decoder(message.bytes, message)
            } else if (!this.#warnedNoDecoder) {
                this.#warnedNoDecoder = true
                console.info(`zenoh-gateway: no decoder registered for encoding "${name}" (registerEncoding("${name}", decoder)); msg.bytes carries its bytes`)
            }
            return true
        } catch (error) {
            this.decodeErrors++
            console.error(`zenoh-gateway: ${this.options.encoding} payload on ${message.key} did not decode`, error)
            return false
        }
    }

    #deliver(message: Message, flags: number): void {
        if ((flags & zstdFlag) !== 0) {
            try {
                message.bytes = zstdDecompress(message.bytes) as Uint8Array
            } catch (error) {
                this.decodeErrors++
                console.error(`zenoh-gateway: zstd message on ${message.key} did not decompress`, error)
                return
            }
        }
        message.kind = (flags & deleteFlag) !== 0 ? "delete" : "put"
        if ((flags & metaFlag) !== 0) {
            try {
                const { encoding, attachment, payload } = splitMeta(message.bytes)
                message.encoding = encoding
                message.attachment = attachment
                message.bytes = payload
            } catch (error) {
                this.decodeErrors++
                console.error(`zenoh-gateway: bad sample header on ${message.key}`, error)
                return
            }
        }
        if (message.kind === "delete") {
            // nothing to decode
        } else if (this.options.encoding !== undefined && !this.#decode(message, flags)) {
            return
        }
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
            console.error(`zenoh-gateway: subscriber callback for ${this.key} threw`, error)
        }
    }

    /** Tells the gateway we processed every frame up to frameId: 4 bytes, little endian. */
    #consumed(channel: RTCDataChannel, frameId: number, byteLength: number): void {
        if (frameId > this.#highestConsumedFrame) {
            this.#highestConsumedFrame = frameId
        }
        this.#bytesSinceAck += byteLength
        const sendAck = () => {
            this.#ackTimer = null
            if (channel.readyState !== "open" || channel !== this.channel) {
                return
            }
            this.#bytesSinceAck = 0
            const ack = new Uint8Array(4)
            new DataView(ack.buffer).setUint32(0, this.#highestConsumedFrame, true)
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
}

export class Publisher extends Endpoint {
    sent = 0
    dropped = 0
    tripped = false
    /** why the deadman fired: "heartbeat" | "disconnected" | "shutdown" */
    tripReason: string | null = null
    deadmanArmed = false
    /** why the gateway is dropping this publisher's puts right now (another client's lease), else null */
    blocked: string | null = null
    #last: Uint8Array | null = null
    /** stamped frames waiting for the channel (reliable: all, latest: only the newest) */
    #pending: Uint8Array<ArrayBuffer>[] = []
    #repeatTimer: ReturnType<typeof setInterval> | null = null
    #tripListeners = new Set<(reason: string) => void>()

    constructor(owner: ZenohGateway, id: number, key: string, readonly options: PublisherOptions) {
        super(owner, id, key)
        if (options.repeatMs) {
            this.#repeatTimer = setInterval(() => {
                if (this.#last && !this.tripped && this.rejectionReason === null) {
                    this.#send(encodePut(this.#last, this.owner.now()))
                }
            }, options.repeatMs)
        }
    }

    get state(): PublisherState {
        if (this.closed) {
            return "closed"
        }
        if (this.rejectionReason !== null) {
            return "rejected"
        }
        if (this.tripped) {
            return "tripped"
        }
        return this.acceptance.settled ? "open" : "connecting"
    }

    onTripped(listener: (reason: string) => void): () => void {
        this.#tripListeners.add(listener)
        return () => {
            this.#tripListeners.delete(listener)
        }
    }

    attach(peer: RTCPeerConnection): void {
        const { delivery, priority, latencyLimit } = this.options
        const label = JSON.stringify({ type: "pub", key: this.key, id: this.id, opts: { delivery, priority, latencyLimit } })
        const acceptance = this.beginAttempt()
        const channel = peer.createDataChannel(label, channelInit(delivery, undefined))
        channel.binaryType = "arraybuffer"
        channel.bufferedAmountLowThreshold = resumeBytes
        channel.onbufferedamountlow = () => this.#flush()
        channel.onmessage = (event: MessageEvent) => {
            this.blocked = JSON.parse(String(event.data)).blocked ?? null
        }
        this.channel = channel
        this.watchChannel(channel, acceptance)
        // puts made before the gateway accepted the channel wait for it
        this.acceptance.promise.then(() => this.#flush(), () => {})
    }

    #checkUsable(): void {
        if (this.closed) {
            throw new Error(`zenoh-gateway: publisher ${this.key} is closed`)
        }
        if (this.rejectionReason !== null) {
            throw new Error(`zenoh-gateway: publisher ${this.key} was rejected by the gateway: ${this.rejectionReason}`)
        }
        if (this.tripped) {
            throw new Error(`zenoh-gateway: publisher ${this.key} is tripped (deadman fired: ${this.tripReason}); create a new publisher`)
        }
    }

    /** `timestamp`: when the value was produced, in this client's clock (`z.now()`); defaults to now. */
    /** A zenoh delete on this publisher's key (over the control channel; needs the `publish` grant). */
    delete(options: PutOptions = {}): Promise<void> {
        return this.owner.delete(this.key, options)
    }

    put(value: Bytesish, { timestamp }: { timestamp?: number } = {}): void {
        this.#checkUsable()
        const bytes = toBytes(value)
        this.#last = bytes
        this.#send(encodePut(bytes, timestamp ?? this.owner.now()))
    }

    #send(frame: Uint8Array<ArrayBuffer>): void {
        const channel = this.channel
        const backedUp = !channel || channel.readyState !== "open" || !this.acceptance.settled || channel.bufferedAmount > backedUpBytes
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

    #flush(): void {
        const channel = this.channel
        while (this.#pending.length > 0 && channel?.readyState === "open" && this.rejectionReason === null && channel.bufferedAmount <= backedUpBytes) {
            channel.send(this.#pending.shift() as Uint8Array<ArrayBuffer>)
            this.sent++
        }
    }

    /**
     * Stores `value` on the gateway; it is published once (REAL_TIME, reliable) if this frontend's
     * heartbeat stops, it disconnects, or the gateway shuts down. Then this publisher is tripped.
     */
    setDeadman(value: Bytesish): Promise<void> {
        if (!this.owner.options.heartbeatHz) {
            throw new Error("zenoh-gateway: setDeadman needs a heartbeat; connect(url, { heartbeatHz: 5, heartbeatMisses: 3 })")
        }
        this.#checkUsable()
        const bytes = toBytes(value)
        return this.ready().then(async () => {
            await this.owner._request({ op: "setDeadman", pubId: this.id, bytes: toBase64(bytes) }, pingTimeoutMs)
            this.deadmanArmed = true
        })
    }

    async clearDeadman(): Promise<void> {
        this.#checkUsable()
        await this.owner._request({ op: "clearDeadman", pubId: this.id }, pingTimeoutMs)
        this.deadmanArmed = false
    }

    _trip(reason: string): void {
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
        // a tripped stream is never re-opened; the channel stays until close() so the gateway keeps rejecting it
        this.owner._forget(this)
        for (const listener of this.#tripListeners) {
            try {
                listener(reason)
            } catch (error) {
                console.error("zenoh-gateway: onTripped listener threw", error)
            }
        }
    }

    override _rejected(reason: string): void {
        this.#pending = []
        if (this.#repeatTimer) {
            clearInterval(this.#repeatTimer)
        }
        super._rejected(reason)
    }

    override close(): void {
        if (this.#repeatTimer) {
            clearInterval(this.#repeatTimer)
        }
        super.close()
    }
}

/** An exclusive right to publish on a group of keys among this gateway's clients (SPEC "Leases"). */
export class Lease {
    /** why it ended: "heartbeat" | "maxSeconds" | "disconnected" | "force-expired by peer N" | "released" */
    lost: string | null = null
    #listeners = new Set<(reason: string) => void>()

    constructor(readonly owner: ZenohGateway, readonly group: string, readonly keys: string[], readonly expiresInMs: number | null) {}

    onLost(listener: (reason: string) => void): () => void {
        this.#listeners.add(listener)
        return () => {
            this.#listeners.delete(listener)
        }
    }

    async release(): Promise<void> {
        if (this.lost === null) {
            this._lose("released")
            await this.owner._request({ op: "releaseLease", group: this.group }, pingTimeoutMs)
        }
    }

    _lose(reason: string): void {
        if (this.lost !== null) {
            return
        }
        this.lost = reason
        this.owner._leases.delete(this.group)
        for (const listener of this.#listeners) {
            try {
                listener(reason)
            } catch (error) {
                console.error("zenoh-gateway: onLost listener threw", error)
            }
        }
    }
}

type ResolvedConnectOptions = Required<Omit<ConnectOptions, "clock" | "iceServers" | "iceTransportPolicy" | "token">> & Pick<ConnectOptions, "clock" | "iceServers" | "iceTransportPolicy" | "token">

export class ZenohGateway {
    state: ConnectionState = "connecting"
    /** per subscribed/published key expression */
    stats: Record<string, KeyStats> = {}
    /** latest round trip to the gateway (heartbeat, else control ping) */
    rttMs: number | null = null
    /** gateway clock minus this client's clock, from the lowest-RTT recent sample */
    clockOffsetMs: number | null = null
    /** gateway-side heartbeat, clock and bandwidth stats */
    gatewayStats: GatewayStats | null = null
    /** the message encodings the gateway runs, fetched on connect */
    encodings: readonly EncodingInfo[] = []
    /** the ICE servers in use (the gateway's unless given) */
    iceServers: RTCIceServer[] = []
    /** leases held, by group */
    readonly _leases = new Map<string, Lease>()
    readonly url: string
    readonly options: ResolvedConnectOptions
    /** this client's clock in ms; put timestamps and clock sync use it */
    readonly now: () => number
    #peer: RTCPeerConnection | null = null
    #control: RTCDataChannel | null = null
    #heartbeat: RTCDataChannel | null = null
    #heartbeatTimer: ReturnType<typeof setInterval> | null = null
    #heartbeatPaused = false
    #clockSamples: ClockSample[] = []
    #endpoints = new Set<Subscription | Publisher>()
    /** every endpoint by id, including tripped/rejected ones the gateway may still talk about */
    #endpointsById = new Map<number, Subscription | Publisher>()
    #requests = new Map<number, PendingRequest>()
    #stateListeners = new Set<(state: ConnectionState) => void>()
    #nextId = 1
    #closed = false
    #generation = 0
    #statsTimer: ReturnType<typeof setInterval> | null = null
    /** renegotiations run one at a time */
    #negotiation: Promise<unknown> = Promise.resolve()
    /** resolves once the current peer connection is up (renegotiation needs `control`) */
    #connected: Promise<void> = new Promise(() => {})
    #markConnected: () => void = () => {}

    constructor(url: string, options: ConnectOptions = {}) {
        this.url = url.replace(/\/+$/, "")
        this.options = { reconnect: true, statsIntervalMs: 1000, heartbeatHz: 0, heartbeatMisses: 3, ...options }
        this.now = this.options.clock ?? (() => performance.timeOrigin + performance.now())
    }

    onState(listener: (state: ConnectionState) => void): () => void {
        this.#stateListeners.add(listener)
        return () => {
            this.#stateListeners.delete(listener)
        }
    }

    #setState(state: ConnectionState): void {
        if (state === this.state) {
            return
        }
        this.state = state
        for (const listener of this.#stateListeners) {
            try {
                listener(state)
            } catch (error) {
                console.error("zenoh-gateway: state listener threw", error)
            }
        }
    }

    /** NTP-style sample: t0/t3 in our clock, t1/t2 gateway receive/send in its clock. */
    #addClockSample(t0: number, t1: number, t2: number, t3: number): void {
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

    /** Clock-sync ping over control; also reports our current estimate to the gateway. */
    async #controlPing(): Promise<void> {
        const t0 = this.now()
        const response = await this._request({ op: "ping", t0, offsetMs: this.clockOffsetMs, rttMs: this.rttMs }, pingTimeoutMs)
        this.#addClockSample(t0, Number(response.t1), Number(response.t2), this.now())
    }

    /** Opens (or re-opens) the peer connection and every channel on it. */
    get _peer(): RTCPeerConnection | null {
        return this.#peer
    }

    /**
     * A new recvonly transceiver bound to a gateway track of `channel`'s format, added through a renegotiation over
     * `control` (the gateway answers with a track for the new m-line). Never a reused one: Chrome stopped assembling
     * a reused receiver's frames after a few quick close-and-reopen switches.
     */
    _acquireTransceiver(peer: RTCPeerConnection, kind: "video" | "audio", channel: Channel): Promise<RTCRtpTransceiver> {
        const run = async () => {
            await this.#connected
            if (peer !== this.#peer) {
                throw new Error("connection replaced")
            }
            const transceiver = peer.addTransceiver(kind, { direction: "recvonly" })
            // video shows frames as soon as they decode (the gateway also asks for zero playout delay); audio keeps its jitter buffer
            const receiver = transceiver.receiver as unknown as { jitterBufferTarget?: number | null, playoutDelayHint?: number }
            if (kind === "video" && "jitterBufferTarget" in receiver) {
                receiver.jitterBufferTarget = 0
            } else if (kind === "video") {
                receiver.playoutDelayHint = 0
            }
            await peer.setLocalDescription(await peer.createOffer())
            const offer = peer.localDescription
            const response = await this._request({ op: "renegotiate", channel, sdp: { type: offer?.type, sdp: offer?.sdp } }, openTimeoutMs)
            await peer.setRemoteDescription(response.sdp as RTCSessionDescriptionInit)
            if (response.mid !== transceiver.mid) {
                throw new Error(`gateway bound mid ${String(response.mid)}, expected ${String(transceiver.mid)}`)
            }
            return transceiver
        }
        const result = this.#negotiation.then(run, run)
        this.#negotiation = result.catch(() => {})
        return result
    }

    /** A closed subscription's transceiver stops; the next renegotiation frees its m-line (Chrome reuses the slot). */
    _releaseTransceiver(peer: RTCPeerConnection, transceiver: RTCRtpTransceiver): void {
        if (peer.connectionState !== "closed" && transceiver.currentDirection !== "stopped") {
            transceiver.stop()
        }
    }

    async _open(): Promise<void> {
        const generation = ++this.#generation
        this.#setState("connecting")
        this.#clockSamples = []
        this.#connected = new Promise((resolve) => {
            this.#markConnected = resolve
        })
        const auth: Record<string, string> = this.options.token === undefined ? {} : { authorization: `Bearer ${this.options.token}` }
        let iceServers = this.options.iceServers
        let iceTransportPolicy = this.options.iceTransportPolicy
        if (iceServers === undefined) {
            const response = await fetch(`${this.url}/zenoh-gateway/ice`, { headers: auth }).catch(() => null)
            await this.#refuseIfUnauthorized(response)
            const ice = response?.ok ? await response.json() : {}
            iceServers = (ice.iceServers ?? []) as RTCIceServer[]
            // a server may ask for relay-only; the caller's own policy wins
            iceTransportPolicy ??= ice.iceTransportPolicy as RTCIceTransportPolicy | undefined
        }
        this.iceServers = iceServers
        const peer = new RTCPeerConnection({ iceServers, iceTransportPolicy: iceTransportPolicy ?? "all" })
        const control = peer.createDataChannel("control", { ordered: true })
        this.#peer = peer
        this.#control = control
        control.onmessage = (event: MessageEvent) => this.#onControlMessage(String(event.data))
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
                headers: { "content-type": "application/json", ...auth },
                body: JSON.stringify({ type: peer.localDescription?.type, sdp: peer.localDescription?.sdp }),
            })
            await this.#refuseIfUnauthorized(response, peer)
            if (!response.ok) {
                throw new Error(`gateway refused offer: ${response.status} ${await response.text()}`)
            }
            await peer.setRemoteDescription(await response.json())
            await waitOpen(control, openTimeoutMs)
            this.encodings = Object.freeze((await this._request({ op: "encodings" }, pingTimeoutMs)).encodings as EncodingInfo[])
            // a few quick samples so the gateway knows the clock offset before the first put
            for (let index = 0; index < initialClockPings; index++) {
                await this.#controlPing()
            }
            await this.#redeclare()
        } catch (error) {
            this.#onLost(generation)
            throw error
        }
        this.#setState("connected")
        this.#markConnected()
    }

    /** A 401 is final: no reconnecting with a token the gateway refuses (or revoked). */
    async #refuseIfUnauthorized(response: Response | null, peer?: RTCPeerConnection): Promise<void> {
        if (response?.status === 401) {
            peer?.close()
            this.close()
            throw new Error(`zenoh-gateway: gateway refused the token: ${await response.text()}`)
        }
    }

    #attachHeartbeat(peer: RTCPeerConnection): void {
        const label = JSON.stringify({ type: "heartbeat", opts: { hz: this.options.heartbeatHz, misses: this.options.heartbeatMisses } })
        const heartbeat = peer.createDataChannel(label, { ordered: false, maxRetransmits: 0 })
        heartbeat.onmessage = (event: MessageEvent) => {
            try {
                const reply = JSON.parse(String(event.data))
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

    /** Stops sending heartbeats (the gateway then fires this frontend's deadmen); for testing deadman wiring. */
    pauseHeartbeat(): void {
        this.#heartbeatPaused = true
    }

    resumeHeartbeat(): void {
        this.#heartbeatPaused = false
    }

    #tripArmedPublishers(reason: string): void {
        for (const endpoint of this.#endpointsById.values()) {
            if (endpoint instanceof Publisher && endpoint.deadmanArmed) {
                endpoint._trip(reason)
            }
        }
    }

    #onLost(generation: number): void {
        if (generation !== this.#generation || this.state === "lost") {
            return
        }
        this.#setState("lost")
        // the gateway fires this frontend's deadmen when it loses us
        this.#tripArmedPublishers("disconnected")
        for (const lease of [...this._leases.values()]) {
            lease._lose("disconnected")
        }
        for (const request of this.#requests.values()) {
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
                        console.warn("zenoh-gateway: reconnect failed", error)
                        await sleep(reconnectDelayMs)
                    }
                }
            }, reconnectDelayMs)
        }
    }

    #onControlMessage(text: string): void {
        let response: ControlResponse
        try {
            response = JSON.parse(text)
        } catch {
            return
        }
        if (response.event === "query" || response.event === "liveliness" || response.event === "matching") {
            this.#routeApiEvent(response)
            return
        }
        if (response.event !== undefined) {
            const endpoint = this.#endpointsById.get(Number(response.id))
            if (response.event === "tripped" && endpoint instanceof Publisher) {
                endpoint._trip(String(response.reason))
            } else if (response.event === "rejected") {
                endpoint?._rejected(String(response.reason))
            } else if (response.event === "accepted") {
                endpoint?._accepted()
            } else if (response.event === "closed") {
                this.#onLost(this.#generation)
            } else if (response.event === "leaseLost") {
                this._leases.get(String(response.group))?._lose(String(response.reason))
            }
            return
        }
        const request = this.#requests.get(Number(response.id))
        if (!request) {
            return
        }
        this.#requests.delete(Number(response.id))
        clearTimeout(request.timer)
        if (response.ok) {
            request.resolve(response)
        } else {
            request.reject(new Error(`zenoh-gateway: ${response.error ?? "error"}`))
        }
    }

    _request(body: { op: string, [field: string]: unknown }, timeoutMs: number): Promise<ControlResponse> {
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

    /** Options are checked by the gateway: a bad one rejects the subscription (`state`, `ready()`). */
    subscribe(key: string, options: SubscribeOptions, callback: (message: Message) => void): Subscription {
        const subscription = new Subscription(this, this.#nextId++, key, { ...options }, callback)
        this.#addEndpoint(subscription)
        return subscription
    }

    publisher(key: string, options: PublisherOptions = {}): Publisher {
        const publisher = new Publisher(this, this.#nextId++, key, { ...options })
        this.#addEndpoint(publisher)
        return publisher
    }

    #addEndpoint(endpoint: Subscription | Publisher): void {
        this.#endpoints.add(endpoint)
        this.#endpointsById.set(endpoint.id, endpoint)
        if (this.#peer && this.#peer.connectionState !== "closed") {
            endpoint.attach(this.#peer)
        }
    }

    /** Stops re-attaching an endpoint on reconnect (closed, tripped or rejected). */
    _forget(endpoint: Endpoint): void {
        this.#endpoints.delete(endpoint as Subscription | Publisher)
        if (endpoint.closed) {
            this.#endpointsById.delete(endpoint.id)
        }
        this.#refreshStats(null)
    }

    /** zenoh query: `key` may carry parameters after `?` (or pass `parameters`). */
    async get(key: string, options: GetOptions = {}): Promise<GetReply[]> {
        const timeoutMs = options.timeoutMs ?? 5000
        const response = await this._request({ op: "get", key, ...getRequestFields(options), timeoutMs }, timeoutMs + 2000)
        return (response.replies as ReplyWire[]).map(parseReply)
    }

    /** Queries with fixed options, like zenoh's querier. */
    querier(key: string, options: GetOptions = {}): Querier {
        return new Querier(this, key, options)
    }

    /** One zenoh put (needs the `publish` grant). */
    async put(key: string, value: Bytesish, options: PutOptions = {}): Promise<void> {
        await this._request({ op: "put", key, bytes: toBase64(toBytes(value)), ...putRequestFields(options, this.clockOffsetMs) }, pingTimeoutMs)
    }

    /** One zenoh delete (needs the `publish` grant). */
    async delete(key: string, options: PutOptions = {}): Promise<void> {
        await this._request({ op: "delete", key, ...putRequestFields(options, this.clockOffsetMs) }, pingTimeoutMs)
    }

    /**
     * Answers zenoh queries on `key` from this page (needs the `queryable` grant). The callback gets
     * each query; reply with `query.reply(...)` (any number of times), then `query.finalize()`.
     */
    async declareQueryable(key: string, options: { complete?: boolean }, callback: (query: Query) => void): Promise<Queryable> {
        const queryable = new Queryable(this, key, options.complete ?? false, callback)
        await queryable._declare()
        this.#apiHandles.add(queryable)
        return queryable
    }

    /** A liveliness token on `key`, alive until undeclared or this page goes (needs `liveliness`). */
    async declareToken(key: string): Promise<LivelinessToken> {
        const token = new LivelinessToken(this, key)
        await token._declare()
        this.#apiHandles.add(token)
        return token
    }

    /** Tokens appearing (`alive: true`) and going under `key`; `history` also reports the ones already alive. */
    async livelinessSubscribe(key: string, options: { history?: boolean }, callback: (change: { key: string, alive: boolean }) => void): Promise<LivelinessSubscriber> {
        const subscriber = new LivelinessSubscriber(this, key, options.history ?? false, callback)
        await subscriber._declare()
        this.#apiHandles.add(subscriber)
        return subscriber
    }

    /** The keys of the liveliness tokens alive under `key`. */
    async livelinessGet(key: string, { timeoutMs = 5000 }: { timeoutMs?: number } = {}): Promise<string[]> {
        const response = await this._request({ op: "livelinessGet", key, timeoutMs }, timeoutMs + 2000)
        return response.tokens as string[]
    }

    /** Whether a publisher ("subscribers") or a querier ("queryables") on `key` would reach anyone now. */
    async matchingStatus(key: string, target: MatchingTarget = "subscribers"): Promise<boolean> {
        const response = await this._request({ op: "matchingStatus", key, matching: target }, pingTimeoutMs + 5000)
        return Boolean(response.matching)
    }

    /** Called each time `matchingStatus(key, target)` changes. */
    async matchingListener(key: string, target: MatchingTarget, callback: (matching: boolean) => void): Promise<MatchingListener> {
        const listener = new MatchingListener(this, key, target, callback)
        await listener._declare()
        this.#apiHandles.add(listener)
        return listener
    }

    /** The gateway's zenoh session: its id, and the routers and peers it is connected to. */
    async info(): Promise<SessionInfo> {
        const response = await this._request({ op: "info" }, pingTimeoutMs)
        return { zid: String(response.zid), routers: response.routers as string[], peers: response.peers as string[] }
    }

    /** api handles by event and id, and events that came before their handle's id did */
    #apiRoutes = new Map<string, (event: ControlResponse) => void>()
    #apiEarly = new Map<string, ControlResponse[]>()
    /** declared queryables, tokens and listeners: re-declared after a reconnect */
    #apiHandles = new Set<ApiHandle>()

    _route(event: string, id: number, handler: (event: ControlResponse) => void): void {
        const routeKey = `${event}:${id}`
        this.#apiRoutes.set(routeKey, handler)
        for (const early of this.#apiEarly.get(routeKey) ?? []) {
            handler(early)
        }
        this.#apiEarly.delete(routeKey)
    }

    _unroute(event: string, id: number): void {
        this.#apiRoutes.delete(`${event}:${id}`)
    }

    _forgetHandle(handle: ApiHandle): void {
        this.#apiHandles.delete(handle)
    }

    #routeApiEvent(response: ControlResponse): void {
        const idField = { query: "queryableId", liveliness: "subId", matching: "listenerId" }[response.event as "query" | "liveliness" | "matching"]
        const routeKey = `${response.event}:${response[idField]}`
        const handler = this.#apiRoutes.get(routeKey)
        if (handler) {
            handler(response)
        } else if (this.#apiEarly.size < 1000) {
            // a liveliness history can arrive before the reply naming its subscriber
            this.#apiEarly.set(routeKey, [...(this.#apiEarly.get(routeKey) ?? []), response])
        }
    }

    /** After a reconnect the gateway has none of this page's declarations: make them again. */
    async #redeclare(): Promise<void> {
        this.#apiRoutes.clear()
        this.#apiEarly.clear()
        for (const handle of this.#apiHandles) {
            await handle._declare().catch((error) => console.error("zenoh-gateway: re-declaring after reconnect failed", error))
        }
    }

    /**
     * Keys currently live on the zenoh network under `filter`, including ones never subscribed to.
     * See SPEC.md "Topic enumeration" for which kinds of keys can and can't be seen.
     * `probeMs: 0` lists liveliness tokens only, without subscribing to `filter`.
     */
    async listTopics(filter = "**", { probeMs = 600 }: { probeMs?: number } = {}): Promise<Topic[]> {
        const response = await this._request({ op: "listTopics", key: filter, probeMs }, probeMs + 5000)
        return response.topics as Topic[]
    }

    /**
     * Takes (or renews) the exclusive right to publish on `group`'s keys among this gateway's clients: the
     * server's group, or `keys` for one it doesn't define. Needs a heartbeat; it ends when the heartbeat
     * stops, at `maxSeconds`, on disconnect, by `release()` or by force-expiry (`onLost` says which).
     */
    async lease(group: string, { keys, maxSeconds }: { keys?: string[], maxSeconds?: number } = {}): Promise<Lease> {
        if (!this.options.heartbeatHz) {
            throw new Error("zenoh-gateway: a lease needs a heartbeat; connect(url, { heartbeatHz: 5, heartbeatMisses: 3 })")
        }
        const response = await this._request({ op: "lease", group, keys, maxSeconds }, pingTimeoutMs)
        const lease = new Lease(this, group, response.keys as string[], (response.expiresInMs as number | null) ?? null)
        this._leases.get(group)?._lose("renewed")
        this._leases.set(group, lease)
        return lease
    }

    /** Ends another client's lease on `group` (needs the grant's forceExpire right). */
    async expireLease(group: string): Promise<void> {
        await this._request({ op: "expireLease", group }, pingTimeoutMs)
    }

    /** Polls gateway stats (and, without a heartbeat, clock sync) once; also runs on a timer while connected. */
    async pollStats(): Promise<void> {
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
            this.gatewayStats = { clock: response.clock, heartbeat: response.heartbeat, bandwidth: response.bandwidth ?? null } as GatewayStats
        }
        this.#refreshStats((response?.channels as GatewayChannelStats[] | undefined) ?? null)
    }

    #refreshStats(gatewayChannels: GatewayChannelStats[] | null): void {
        if (gatewayChannels) {
            const byId = new Map(gatewayChannels.map((channel) => [channel.id, channel]))
            for (const endpoint of this.#endpointsById.values()) {
                endpoint.gatewayStats = byId.get(endpoint.id) ?? null
            }
        }
        const stats: Record<string, KeyStats> = {}
        for (const endpoint of this.#endpoints) {
            const entry = stats[endpoint.key] ??= { received: 0, dropped: 0, backlogBytes: 0, rttMs: this.rttMs, gateway: null }
            const gateway = endpoint.gatewayStats
            entry.gateway = gateway
            if (endpoint instanceof Subscription) {
                entry.received += endpoint.received
                entry.dropped += endpoint.dropped
                entry.backlogBytes += gateway ? Number(gateway.stats.queuedBytes) + Number(gateway.stats.outstandingBytes) : 0
            } else {
                entry.received += endpoint.sent
                entry.dropped += endpoint.dropped + (gateway ? Number(gateway.stats.droppedStale) : 0)
                entry.backlogBytes += endpoint.channel?.bufferedAmount ?? 0
            }
        }
        this.stats = stats
    }

    _startStats(): void {
        this.#statsTimer = setInterval(() => {
            if (this.state === "connected" || this.state === "degraded") {
                this.pollStats().catch(() => {})
            }
        }, this.options.statsIntervalMs)
    }

    close(): void {
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

/** Connects to a zenoh-gateway server, e.g. `await connect("http://robot.local:7448")`. */
export async function connect(url: string, options: ConnectOptions = {}): Promise<ZenohGateway> {
    const client = new ZenohGateway(url, options)
    await client._open()
    client._startStats()
    return client
}

// ── The rest of the zenoh API (SPEC "The rest of the zenoh API") ─────────────────────────────

/** Options of `put` / `delete` (and `publisher.delete`). */
export interface PutOptions {
    /** e.g. "text/plain" (put only) */
    encoding?: string
    attachment?: Bytesish
    /** zenoh priority, 1 (real time) to 7 (background) */
    priority?: number
    congestionControl?: "drop" | "block"
    express?: boolean
    /** the sample's timestamp: unix ms on this client's clock (converted to the gateway's) */
    timestamp?: number
}

/** Options of `get` (and a `Querier`). */
export interface GetOptions {
    /** the selector's parameters ("a=1;b=2"); also allowed inline in the key after `?` */
    parameters?: string
    payload?: Bytesish
    encoding?: string
    attachment?: Bytesish
    target?: "bestMatching" | "all" | "allComplete"
    consolidation?: "auto" | "none" | "monotonic" | "latest"
    /** default 5000 */
    timeoutMs?: number
    priority?: number
    congestionControl?: "drop" | "block"
    express?: boolean
}

export type MatchingTarget = "subscribers" | "queryables"

export interface SessionInfo {
    /** the gateway's zenoh id */
    zid: string
    routers: string[]
    peers: string[]
}

interface ReplyWire {
    key?: string
    bytes?: string
    error?: string
    encoding?: string
    attachment?: string
    kind?: "put" | "delete"
    timestamp?: number
}

function parseReply(reply: ReplyWire): GetReply {
    if (reply.error !== undefined) {
        return { key: null, bytes: fromBase64(reply.error), error: true, encoding: reply.encoding }
    }
    return {
        key: reply.key ?? null,
        bytes: fromBase64(reply.bytes ?? ""),
        kind: reply.kind,
        encoding: reply.encoding,
        attachment: reply.attachment === undefined ? undefined : fromBase64(reply.attachment),
        timestamp: reply.timestamp,
    }
}

function putRequestFields(options: PutOptions, clockOffsetMs: number | null = null): Record<string, unknown> {
    return {
        encoding: options.encoding,
        attachment: options.attachment === undefined ? undefined : toBase64(toBytes(options.attachment)),
        priority: options.priority,
        congestionControl: options.congestionControl,
        express: options.express,
        timestamp: options.timestamp === undefined ? undefined : options.timestamp + (clockOffsetMs ?? 0),
    }
}

function getRequestFields(options: GetOptions): Record<string, unknown> {
    return {
        parameters: options.parameters,
        payload: options.payload === undefined ? undefined : toBase64(toBytes(options.payload)),
        encoding: options.encoding,
        attachment: options.attachment === undefined ? undefined : toBase64(toBytes(options.attachment)),
        target: options.target,
        consolidation: options.consolidation,
        priority: options.priority,
        congestionControl: options.congestionControl,
        express: options.express,
    }
}

/** Something declared on the gateway for this page; re-declared after a reconnect. */
interface ApiHandle {
    _declare(): Promise<void>
}

/** Fixed-option queries on one key (`gateway.querier(key, options)`). */
export class Querier {
    constructor(readonly owner: ZenohGateway, readonly key: string, readonly options: GetOptions) {}

    /** A query with the querier's options, plus these per-call ones. */
    get(options: Pick<GetOptions, "parameters" | "payload" | "attachment" | "encoding"> = {}): Promise<GetReply[]> {
        return this.owner.get(this.key, { ...this.options, ...options })
    }

    /** Whether a queryable would answer now. */
    matchingStatus(): Promise<boolean> {
        return this.owner.matchingStatus(this.key, "queryables")
    }

    matchingListener(callback: (matching: boolean) => void): Promise<MatchingListener> {
        return this.owner.matchingListener(this.key, "queryables", callback)
    }
}

/** A query this page's `Queryable` received. */
export class Query {
    #finalized = false

    constructor(
        readonly owner: ZenohGateway,
        readonly id: number,
        readonly key: string,
        readonly parameters: string,
        readonly payload: Uint8Array | undefined,
        readonly encoding: string | undefined,
        readonly attachment: Uint8Array | undefined,
    ) {}

    /** Replies with a sample on `key` (default: the query's key). Any number of replies, then `finalize()`. */
    async reply(value: Bytesish, options: { key?: string, encoding?: string, attachment?: Bytesish, timestamp?: number } = {}): Promise<void> {
        await this.owner._request({
            op: "reply",
            queryId: this.id,
            key: options.key ?? "",
            bytes: toBase64(toBytes(value)),
            ...putRequestFields(options, this.owner.clockOffsetMs),
        }, pingTimeoutMs)
    }

    async replyErr(value: Bytesish, options: { encoding?: string } = {}): Promise<void> {
        await this.owner._request({ op: "replyErr", queryId: this.id, bytes: toBase64(toBytes(value)), encoding: options.encoding }, pingTimeoutMs)
    }

    /** Replies that `key` (default: the query's key) was deleted. */
    async replyDel(options: { key?: string, attachment?: Bytesish } = {}): Promise<void> {
        await this.owner._request({ op: "replyDel", queryId: this.id, key: options.key ?? "", ...putRequestFields(options) }, pingTimeoutMs)
    }

    /** No more replies: the asker's get completes. (The gateway finalizes forgotten queries after two minutes.) */
    async finalize(): Promise<void> {
        if (!this.#finalized) {
            this.#finalized = true
            await this.owner._request({ op: "finalizeQuery", queryId: this.id }, pingTimeoutMs)
        }
    }
}

/** Queries for this page to answer (`gateway.declareQueryable`). */
export class Queryable implements ApiHandle {
    #id = 0

    constructor(readonly owner: ZenohGateway, readonly key: string, readonly complete: boolean, readonly callback: (query: Query) => void) {}

    async _declare(): Promise<void> {
        const response = await this.owner._request({ op: "declareQueryable", key: this.key, complete: this.complete }, pingTimeoutMs)
        this.#id = Number(response.queryableId)
        this.owner._route("query", this.#id, (event) => {
            const query = new Query(
                this.owner,
                Number(event.queryId),
                String(event.key),
                String(event.parameters ?? ""),
                event.payload === undefined ? undefined : fromBase64(String(event.payload)),
                event.encoding === undefined ? undefined : String(event.encoding),
                event.attachment === undefined ? undefined : fromBase64(String(event.attachment)),
            )
            try {
                this.callback(query)
            } catch (error) {
                console.error(`zenoh-gateway: queryable callback for ${this.key} threw`, error)
            }
        })
    }

    async undeclare(): Promise<void> {
        this.owner._forgetHandle(this)
        this.owner._unroute("query", this.#id)
        await this.owner._request({ op: "undeclareQueryable", queryableId: this.#id }, pingTimeoutMs)
    }
}

/** A liveliness token (`gateway.declareToken`). */
export class LivelinessToken implements ApiHandle {
    #id = 0

    constructor(readonly owner: ZenohGateway, readonly key: string) {}

    async _declare(): Promise<void> {
        this.#id = Number((await this.owner._request({ op: "declareToken", key: this.key }, pingTimeoutMs)).tokenId)
    }

    async undeclare(): Promise<void> {
        this.owner._forgetHandle(this)
        await this.owner._request({ op: "undeclareToken", tokenId: this.#id }, pingTimeoutMs)
    }
}

/** Liveliness changes under a key (`gateway.livelinessSubscribe`). */
export class LivelinessSubscriber implements ApiHandle {
    #id = 0

    constructor(readonly owner: ZenohGateway, readonly key: string, readonly history: boolean, readonly callback: (change: { key: string, alive: boolean }) => void) {}

    async _declare(): Promise<void> {
        this.#id = Number((await this.owner._request({ op: "livelinessSubscribe", key: this.key, history: this.history }, pingTimeoutMs)).subId)
        this.owner._route("liveliness", this.#id, (event) => {
            try {
                this.callback({ key: String(event.key), alive: event.kind === "put" })
            } catch (error) {
                console.error(`zenoh-gateway: liveliness callback for ${this.key} threw`, error)
            }
        })
    }

    async undeclare(): Promise<void> {
        this.owner._forgetHandle(this)
        this.owner._unroute("liveliness", this.#id)
        await this.owner._request({ op: "livelinessUnsubscribe", subId: this.#id }, pingTimeoutMs)
    }
}

/** Matching changes (`gateway.matchingListener`). */
export class MatchingListener implements ApiHandle {
    #id = 0

    constructor(readonly owner: ZenohGateway, readonly key: string, readonly target: MatchingTarget, readonly callback: (matching: boolean) => void) {}

    async _declare(): Promise<void> {
        this.#id = Number((await this.owner._request({ op: "declareMatchingListener", key: this.key, matching: this.target }, pingTimeoutMs)).listenerId)
        this.owner._route("matching", this.#id, (event) => {
            try {
                this.callback(Boolean(event.matching))
            } catch (error) {
                console.error(`zenoh-gateway: matching callback for ${this.key} threw`, error)
            }
        })
    }

    async undeclare(): Promise<void> {
        this.owner._forgetHandle(this)
        this.owner._unroute("matching", this.#id)
        await this.owner._request({ op: "undeclareMatchingListener", listenerId: this.#id }, pingTimeoutMs)
    }
}
