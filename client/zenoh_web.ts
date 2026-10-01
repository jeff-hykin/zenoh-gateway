/// <reference no-default-lib="true" />
/// <reference lib="dom" />
/// <reference lib="dom.iterable" />
/// <reference lib="esnext" />
// zenoh-web browser client: one WebRTC data channel per subscription/publisher, see SPEC.md

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

/** The bridge's built-in transcoders, picked with the subscribe option `codec` (SPEC "Codecs"). A bridge may register more (`ZenohWeb.codecs`). */
export const CODECS = Object.freeze([
    "ros2-image",
    "ros2-compressed-image",
    "ros2-depth",
    "ros2-compressed-depth",
    "ros2-pointcloud2",
    "dimos-image",
    "dimos-compressed-image",
    "dimos-depth",
    "dimos-compressed-depth",
    "dimos-pointcloud2",
] as const)

export type CodecName = typeof CODECS[number]
/** What a built-in codec delivers: a video track, lossless depth or a point cloud. */
export type CodecOutput = "video" | "depth" | "pointcloud"
/** Where any codec's output goes: the bridge's H.264 video track, or bytes on the data channel. */
export type CodecKind = "video" | "data"

/** A codec the bridge has registered (built-in or the host application's own). */
export interface CodecInfo {
    name: string
    output: CodecKind
}

/**
 * Turns a data codec's bytes into what `msg.decoded` carries. It may also set other message
 * fields (the built-in depth and point cloud decoders set `msg.depth` / `msg.points`).
 */
export type CodecDecoder = (bytes: Uint8Array, message: Message) => unknown

/** The output of a built-in codec. */
export function codecOutput(codec: CodecName): CodecOutput {
    if (codec.endsWith("pointcloud2")) {
        return "pointcloud"
    }
    return codec.endsWith("depth") ? "depth" : "video"
}

const builtinCodecInfos: readonly CodecInfo[] = CODECS.map((name) => ({ name, output: codecOutput(name) === "video" ? "video" : "data" }))
const codecDecoders = new Map<string, CodecDecoder>()

/**
 * Registers the browser decoder for a data codec the bridge runs (e.g. a Rust codec the host
 * application added with `ServerBuilder::codec`). Messages of subscriptions using that codec get
 * `msg.decoded = decoder(msg.bytes, msg)`. Video codecs need no decoder. Registering a different
 * decoder under a name that already has one throws.
 */
export function registerCodec(name: string, decoder: CodecDecoder): void {
    if (typeof name !== "string" || name.length === 0) {
        throw new TypeError(`zenoh-web: registerCodec needs a codec name, got ${String(name)}`)
    }
    if (typeof decoder !== "function") {
        throw new TypeError(`zenoh-web: registerCodec("${name}") needs a decoder function`)
    }
    const existing = codecDecoders.get(name)
    if (existing !== undefined && existing !== decoder) {
        throw new Error(`zenoh-web: a decoder for codec "${name}" is already registered`)
    }
    codecDecoders.set(name, decoder)
}

for (const name of CODECS) {
    if (codecOutput(name) === "depth") {
        registerCodec(name, (bytes, message) => (message.depth = decodeDepth(bytes)))
    } else if (codecOutput(name) === "pointcloud") {
        registerCodec(name, (bytes, message) => (message.points = decodePointCloud(bytes)))
    }
}

/** Lossless depth, downscaled by `stride` (nearest neighbor) at lower quality. */
export interface DepthImage {
    width: number
    height: number
    sourceWidth: number
    sourceHeight: number
    stride: number
    encoding: "16UC1" | "32FC1" | "mono16"
    /** row-major; Uint16Array for 16UC1/mono16, Float32Array for 32FC1 */
    data: Uint16Array | Float32Array
}

export interface PointCloud {
    count: number
    /** finite points in the source message, before voxel downsampling */
    sourceCount: number
    /** x, y, z per point */
    positions: Float32Array
    /** 0..255 per point; the source value is intensityMin + value * intensityScale */
    intensity: Uint8Array | null
    intensityMin: number
    intensityScale: number
    origin: [number, number, number]
    /** meters per quantization step */
    scale: number
    /** 0 = no downsampling */
    voxelSize: number
    /** largest per-axis error against a source point: voxelSize / 2, or scale / 2 without voxels */
    maxError: number
}

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
    /** the payload as sent: raw sample bytes, or the codec's wire format */
    bytes: Uint8Array
    timestamp: number
    seq: number
    depth?: DepthImage
    points?: PointCloud
    /** data codecs: what the codec's registered decoder returned (see `registerCodec`) */
    decoded?: unknown
    video?: VideoFrameInfo
    mediaStream?: MediaStream
    /**
     * video codecs with `imageTransport: "jpeg"`: the picture, decoded (`msg.bytes` is the JPEG
     * file). Draw it, then `close()` it. Absent where `createImageBitmap` is not available.
     */
    image?: ImageBitmap
}

export interface SubscribeOptions {
    delivery?: Delivery
    priority?: number
    bandwidthPriority?: number
    queueSize?: number
    maxAge?: number
    maxHz?: number
    dangerousMinHz?: number
    minQuality?: number
    maxQuality?: number
    qualityToHzTradeoff?: number
    /** a built-in codec or any other name the bridge registered (`ZenohWeb.codecs`) */
    codec?: CodecName | (string & Record<never, never>)
    /**
     * video codecs: `"video"` (default) streams H.264 on a WebRTC video track (`msg.mediaStream`);
     * `"jpeg"` sends each picture as a JPEG file on the data channel, decoded into `msg.image`
     * (lower latency where the browser's video pipeline buffers; any `delivery`).
     */
    imageTransport?: ImageTransport
}

export type ImageTransport = "video" | "jpeg"

export interface PublisherOptions {
    delivery?: Delivery
    priority?: number
    repeatMs?: number
    latencyLimit?: number
}

export interface ConnectOptions {
    iceServers?: RTCIceServer[]
    reconnect?: boolean
    statsIntervalMs?: number
    heartbeatHz?: number
    heartbeatMisses?: number
    clock?: () => number
    /** fraction of the estimated bandwidth the bridge allocates to this connection (default: the bridge's, 0.75) */
    bandwidthTargetFraction?: number
}

export interface KeyStats {
    received: number
    dropped: number
    backlogBytes: number
    rttMs: number | null
    bridge: BridgeChannelStats | null
}

/** One channel as the bridge reports it in `stats`. */
export interface BridgeChannelStats {
    id: number | null
    type: string
    key: string
    opts: Record<string, unknown>
    stats: Record<string, number | boolean | string | null>
    /** subscriptions: this stream's share of the frontend's bandwidth (SPEC "Bandwidth allocation") */
    allocation: Allocation | null
}

export interface Allocation {
    demandBytesPerSec: number
    floorBytesPerSec: number
    budgetBytesPerSec: number
    hz: number
    hzFraction: number
    quality: number | null
    constrained: boolean
}

export interface BandwidthStats {
    dataEstimateBytesPerSec: number
    videoEstimateBytesPerSec: number
    capBytesPerSec: number | null
    budgetBytesPerSec: number
    targetFraction: number
    /** latest RTT minus its 30 s minimum: the delay trigger's input */
    queueDelayMs: number | null
    minRttMs: number | null
    delayEvents: number
    reservedBytesPerSec: number
    bulkChunkBytes: number
    demandBytesPerSec: number
    sentBytesPerSec: number
    networkBlockedFraction: number
    constrained: boolean
}

export interface BridgeStats {
    clock: { offsetMs: number | null, rttMs: number | null }
    heartbeat: Record<string, unknown>
    access: { enabled: boolean, denied: number }
    bandwidth: BandwidthStats | null
}

export interface GetReply {
    key: string | null
    bytes: Uint8Array
    error?: boolean
}

export type TopicSource = "subscriber" | "queryable" | "token" | "advancedPublisher" | "sample"

export interface Topic {
    key: string
    sources: TopicSource[]
}

type Bytesish = Uint8Array | ArrayBuffer | ArrayBufferView | string

interface ControlResponse {
    id?: number
    ok?: boolean
    error?: string
    event?: "tripped" | "rejected" | "accepted"
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
// consumption acks let the bridge stop sending while this page's JS is behind
const ackEveryBytes = 16 * 1024
const ackDelayMs = 5
// clock sync keeps the lowest-RTT sample among the most recent ones
const clockWindow = 16
const initialClockPings = 5
const putHeaderBytes = 8
// incomplete chunked messages kept per subscription before the oldest is dropped
const maxPartialMessages = 8

const subscribeOptionNames = new Set(["delivery", "priority", "bandwidthPriority", "queueSize", "maxAge", "maxHz", "dangerousMinHz", "minQuality", "maxQuality", "qualityToHzTradeoff", "codec", "imageTransport"])
const publisherOptionNames = new Set(["delivery", "priority", "repeatMs", "latencyLimit"])

function checkNumber(name: string, value: unknown, isValid: (value: number) => boolean, expected: string): void {
    if (value === undefined) {
        return
    }
    if (typeof value !== "number" || Number.isNaN(value) || !isValid(value)) {
        throw new RangeError(`zenoh-web: ${name} must be ${expected}, got ${String(value)}`)
    }
}

function checkCommon(options: object, allowed: Set<string>, where: string): void {
    for (const name of Object.keys(options)) {
        if (!allowed.has(name)) {
            throw new TypeError(`zenoh-web: unknown ${where} option "${name}" (allowed: ${[...allowed].join(", ")})`)
        }
    }
    const { delivery, priority } = options as { delivery?: unknown, priority?: unknown }
    if (delivery !== undefined && delivery !== "latest" && delivery !== "reliable") {
        throw new TypeError(`zenoh-web: delivery must be "latest" or "reliable", got ${String(delivery)}`)
    }
    checkNumber("priority", priority, (v) => Number.isInteger(v) && v >= 1 && v <= 7, "an integer 1..7 (see Priority)")
}

/**
 * Throws on an invalid option. `codecs` is what the bridge has (`ZenohWeb.codecs`); without it
 * only the built-in codecs are known.
 */
export function validateSubscribeOptions(options: SubscribeOptions, codecs: readonly CodecInfo[] = builtinCodecInfos): void {
    checkCommon(options, subscribeOptionNames, "subscribe")
    const isUnit = (v: number) => v >= 0 && v <= 1
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
    if (options.imageTransport !== undefined && options.imageTransport !== "video" && options.imageTransport !== "jpeg") {
        throw new TypeError(`zenoh-web: imageTransport must be "video" or "jpeg", got ${String(options.imageTransport)}`)
    }
    if (options.codec !== undefined) {
        const codec = codecs.find((info) => info.name === options.codec)
        if (codec === undefined) {
            throw new TypeError(`zenoh-web: unknown codec "${String(options.codec)}" (the bridge has: ${codecs.map((info) => info.name).join(", ")})`)
        }
        if (codec.output !== "video" && options.imageTransport !== undefined) {
            throw new TypeError(`zenoh-web: imageTransport applies to video codecs; ${options.codec} sends data`)
        }
        if (codec.output === "video" && options.imageTransport !== "jpeg" && options.delivery === "reliable") {
            throw new TypeError(`zenoh-web: ${options.codec} is a video codec (a lossy video track); use delivery "latest" (or imageTransport "jpeg")`)
        }
    } else if (options.imageTransport !== undefined) {
        throw new TypeError("zenoh-web: imageTransport needs a video codec")
    }
}

export function validatePublisherOptions(options: PublisherOptions): void {
    checkCommon(options, publisherOptionNames, "publisher")
    checkNumber("repeatMs", options.repeatMs, (v) => Number.isFinite(v) && v > 0, "> 0 (ms)")
    checkNumber("latencyLimit", options.latencyLimit, (v) => Number.isFinite(v) && v > 0, "> 0 (ms)")
}

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
    chunk: Uint8Array
}

/**
 * Bridge frame (little endian):
 * u16 keyLen | key | f64 timestampMs | u32 seq | u32 frameId | u32 chunkIndex | u32 chunkCount | chunk
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
        chunk: new Uint8Array(buffer, offset + 24),
    }
}

// browsers are little endian, so typed arrays can view the decoded little-endian values directly
const depthEncodings = { 1: "16UC1", 2: "32FC1", 3: "mono16" } as const

/** Depth codec payload: 20-byte header + zstd(values), see SPEC "Wire formats". */
export function decodeDepth(bytes: Uint8Array): DepthImage {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    if (bytes[0] !== 1) {
        throw new Error(`zenoh-web: unknown depth format version ${bytes[0]}`)
    }
    const encoding = depthEncodings[bytes[1] as 1 | 2 | 3]
    if (!encoding) {
        throw new Error(`zenoh-web: unknown depth encoding ${bytes[1]}`)
    }
    const width = view.getUint32(4, true)
    const height = view.getUint32(8, true)
    const values = zstdDecompress(bytes.subarray(20)) as Uint8Array
    const aligned = values.byteOffset % 4 === 0 ? values : values.slice()
    const count = width * height
    const data = encoding === "32FC1" ? new Float32Array(aligned.buffer, aligned.byteOffset, count) : new Uint16Array(aligned.buffer, aligned.byteOffset, count)
    return { width, height, sourceWidth: view.getUint32(12, true), sourceHeight: view.getUint32(16, true), stride: view.getUint16(2, true), encoding, data }
}

/** Point cloud codec payload: 40-byte header + zstd(int16 xyz, u8 intensity), see SPEC "Wire formats". */
export function decodePointCloud(bytes: Uint8Array): PointCloud {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    if (bytes[0] !== 1) {
        throw new Error(`zenoh-web: unknown point cloud format version ${bytes[0]}`)
    }
    const hasIntensity = (bytes[1] & 1) === 1
    const count = view.getUint32(4, true)
    const origin: [number, number, number] = [view.getFloat32(12, true), view.getFloat32(16, true), view.getFloat32(20, true)]
    const scale = view.getFloat32(24, true)
    const voxelSize = view.getFloat32(28, true)
    const body = zstdDecompress(bytes.subarray(40)) as Uint8Array
    const quantized = new DataView(body.buffer, body.byteOffset, body.byteLength)
    const positions = new Float32Array(count * 3)
    for (let index = 0; index < count * 3; index++) {
        positions[index] = origin[index % 3] + quantized.getInt16(index * 2, true) * scale
    }
    return {
        count,
        sourceCount: view.getUint32(8, true),
        positions,
        intensity: hasIntensity ? body.slice(count * 6, count * 7) : null,
        intensityMin: view.getFloat32(32, true),
        intensityScale: view.getFloat32(36, true),
        origin,
        scale,
        voxelSize,
        maxError: voxelSize > 0 ? voxelSize / 2 : scale / 2,
    }
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

/** Browser -> bridge put: f64 sentAtMs (browser clock) | payload (little endian). */
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
 * Settles once the channel is usable: the bridge accepted it AND it is open in this browser.
 * Both are needed because the bridge's `accepted` (control channel) can arrive before this
 * channel's own open (its DCEP ack travels on a different SCTP stream). Rejects if the bridge
 * rejects it or it fails to open.
 */
class Acceptance {
    promise: Promise<void>
    settled = false
    #bridgeAccepted = false
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

    bridgeAccepted(): void {
        this.#bridgeAccepted = true
        this.#settleIfReady()
    }

    channelOpened(): void {
        this.#channelOpen = true
        this.#settleIfReady()
    }

    #settleIfReady(): void {
        if (!this.settled && this.#bridgeAccepted && this.#channelOpen) {
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

/** Common to subscriptions and publishers: a channel the bridge accepts or rejects. */
abstract class Endpoint {
    channel: RTCDataChannel | null = null
    closed = false
    rejectionReason: string | null = null
    bridgeStats: BridgeChannelStats | null = null
    protected acceptance = new Acceptance()

    constructor(readonly owner: ZenohWeb, readonly id: number, readonly key: string) {}

    abstract attach(peer: RTCPeerConnection): void

    /** Resolves once the bridge accepted this channel; rejects with the bridge's reason otherwise. */
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
        this.acceptance.bridgeAccepted()
    }

    _rejected(reason: string): void {
        this.rejectionReason = reason
        this.acceptance.reject(new Error(`zenoh-web: bridge rejected ${this.key}: ${reason}`))
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
    /** codec payloads that failed to decode in this page */
    decodeErrors = 0
    /** video codecs: the decoded video (also on each message as `mediaStream`) */
    mediaStream: MediaStream | null = null
    /** where the codec's output arrives (null: no codec, raw bytes) */
    readonly codecKind: CodecKind | null
    #warnedNoDecoder = false
    #videoTransceiver: RTCRtpTransceiver | null = null
    /** drops before the current channel (each new channel restarts seq at 0) */
    #droppedBefore = 0
    #firstSeq = -1
    #maxSeq = -1
    #receivedOnChannel = 0
    #highestConsumedFrame = -1
    #bytesSinceAck = 0
    #ackTimer: ReturnType<typeof setTimeout> | null = null
    #partials = new Map<number, PartialMessage>()
    /** a video codec sending JPEG files on the data channel */
    readonly #jpeg: boolean
    /** jpeg: the newest message waiting for its picture to decode (older ones are dropped) */
    #pendingImage: Message | null = null
    #decodingImage = false
    /** jpeg: pictures replaced by a newer one before they finished decoding */
    imagesSkipped = 0

    constructor(owner: ZenohWeb, id: number, key: string, readonly options: SubscribeOptions, readonly callback: (message: Message) => void) {
        super(owner, id, key)
        const output = options.codec === undefined ? null : owner.codecs.find((info) => info.name === options.codec)?.output ?? "data"
        this.#jpeg = output === "video" && options.imageTransport === "jpeg"
        this.codecKind = this.#jpeg ? "data" : output
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

    /** messages the bridge accepted for us but we never got whole (queue, age, maxHz, network) */
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
        if (this.codecKind !== "video") {
            this.#openChannel(peer, acceptance, null)
            return
        }
        // video: a recvonly transceiver (renegotiated with the bridge) carries the frames
        this.#videoTransceiver = null
        this.owner._acquireVideoTransceiver(peer).then((transceiver) => {
            if (this.closed || acceptance !== this.acceptance) {
                this.owner._releaseVideoTransceiver(peer, transceiver)
                return
            }
            this.#videoTransceiver = transceiver
            this.mediaStream = new MediaStream([transceiver.receiver.track])
            this.#openChannel(peer, acceptance, transceiver.mid)
        }, (error: Error) => acceptance.reject(new Error(`zenoh-web: video renegotiation for ${this.key} failed: ${error.message}`)))
    }

    #openChannel(peer: RTCPeerConnection, acceptance: Acceptance, mid: string | null): void {
        // JSON turns queueSize Infinity into null, which the bridge reads as unbounded
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
        if (this.#videoTransceiver && peer) {
            this.owner._releaseVideoTransceiver(peer, this.#videoTransceiver)
        }
        this.#videoTransceiver = null
    }

    #onFrame(channel: RTCDataChannel, frame: Frame, frameBytes: number): void {
        if (frame.chunkCount <= 1) {
            this.#deliver({ key: frame.key, bytes: frame.chunk, timestamp: frame.timestamp, seq: frame.seq })
        } else {
            this.#addChunk(frame)
        }
        this.#consumed(channel, frame.frameId, frameBytes)
    }

    #addChunk(frame: Frame): void {
        let partial = this.#partials.get(frame.seq)
        if (!partial) {
            partial = { key: frame.key, timestamp: frame.timestamp, chunks: new Array(frame.chunkCount), receivedChunks: 0, receivedBytes: 0 }
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
        this.#deliver({ key: partial.key, bytes, timestamp: partial.timestamp, seq: frame.seq })
    }

    #evictPartials(limit: number): void {
        while (this.#partials.size > limit) {
            const oldest = Math.min(...this.#partials.keys())
            this.#partials.delete(oldest)
            this.partialDropped++
        }
    }

    /** Adds the codec's decoded form; false if it can't be decoded. */
    #decode(message: Message): boolean {
        try {
            if (this.codecKind === "video") {
                message.video = decodeVideoFrameInfo(message.bytes)
                message.mediaStream = this.mediaStream ?? undefined
                return true
            }
            if (this.#jpeg) {
                return true
            }
            const name = String(this.options.codec)
            const decoder = codecDecoders.get(name)
            if (decoder !== undefined) {
                message.decoded = decoder(message.bytes, message)
            } else if (!this.#warnedNoDecoder) {
                this.#warnedNoDecoder = true
                console.warn(`zenoh-web: no decoder registered for codec "${name}" (registerCodec("${name}", decoder)); msg.bytes carries its encoded bytes`)
            }
            return true
        } catch (error) {
            this.decodeErrors++
            console.error(`zenoh-web: ${this.options.codec} payload on ${message.key} did not decode`, error)
            return false
        }
    }

    #deliver(message: Message): void {
        if (this.codecKind !== null && !this.#decode(message)) {
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
        if (this.#jpeg && typeof createImageBitmap === "function") {
            if (this.#pendingImage !== null) {
                this.imagesSkipped++
            }
            this.#pendingImage = message
            void this.#decodeImages()
            return
        }
        this.#callback(message)
    }

    #callback(message: Message): void {
        try {
            this.callback(message)
        } catch (error) {
            console.error(`zenoh-web: subscriber callback for ${this.key} threw`, error)
        }
    }

    /** One decode at a time, newest first: a picture that arrives during a decode replaces any waiting one. */
    async #decodeImages(): Promise<void> {
        if (this.#decodingImage) {
            return
        }
        this.#decodingImage = true
        while (this.#pendingImage !== null && !this.closed) {
            const message = this.#pendingImage
            this.#pendingImage = null
            try {
                message.image = await createImageBitmap(new Blob([message.bytes as Uint8Array<ArrayBuffer>], { type: "image/jpeg" }))
            } catch (error) {
                this.decodeErrors++
                console.error(`zenoh-web: JPEG on ${message.key} did not decode`, error)
                continue
            }
            if (this.closed) {
                message.image.close()
                break
            }
            this.#callback(message)
        }
        this.#decodingImage = false
    }

    /** Tells the bridge we processed every frame up to frameId: 4 bytes, little endian. */
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
    #last: Uint8Array | null = null
    /** stamped frames waiting for the channel (reliable: all, latest: only the newest) */
    #pending: Uint8Array<ArrayBuffer>[] = []
    #repeatTimer: ReturnType<typeof setInterval> | null = null
    #tripListeners = new Set<(reason: string) => void>()

    constructor(owner: ZenohWeb, id: number, key: string, readonly options: PublisherOptions) {
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
        this.channel = channel
        this.watchChannel(channel, acceptance)
        // puts made before the bridge accepted the channel wait for it
        this.acceptance.promise.then(() => this.#flush(), () => {})
    }

    #checkUsable(): void {
        if (this.closed) {
            throw new Error(`zenoh-web: publisher ${this.key} is closed`)
        }
        if (this.rejectionReason !== null) {
            throw new Error(`zenoh-web: publisher ${this.key} was rejected by the bridge: ${this.rejectionReason}`)
        }
        if (this.tripped) {
            throw new Error(`zenoh-web: publisher ${this.key} is tripped (deadman fired: ${this.tripReason}); create a new publisher`)
        }
    }

    /** `timestamp`: when the value was produced, in this client's clock (`z.now()`); defaults to now. */
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
     * Stores `value` on the bridge; it is published once (REAL_TIME, reliable) if this frontend's
     * heartbeat stops, it disconnects, or the bridge shuts down. Then this publisher is tripped.
     */
    setDeadman(value: Bytesish): Promise<void> {
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

type ResolvedConnectOptions = Required<Omit<ConnectOptions, "clock" | "bandwidthTargetFraction">> & Pick<ConnectOptions, "clock" | "bandwidthTargetFraction">

export class ZenohWeb {
    state: ConnectionState = "connecting"
    /** per subscribed/published key expression */
    stats: Record<string, KeyStats> = {}
    /** latest round trip to the bridge (heartbeat, else control ping) */
    rttMs: number | null = null
    /** bridge clock minus this client's clock, from the lowest-RTT recent sample */
    clockOffsetMs: number | null = null
    /** bridge-side heartbeat, clock and access-control stats */
    bridgeStats: BridgeStats | null = null
    /** the codecs the bridge runs (built-in and its host application's), fetched on connect */
    codecs: readonly CodecInfo[] = builtinCodecInfos
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
    /** every endpoint by id, including tripped/rejected ones the bridge may still talk about */
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
    /** video transceivers of closed subscriptions, reused before adding new ones */
    #freeVideoTransceivers = new Map<RTCPeerConnection, RTCRtpTransceiver[]>()

    constructor(url: string, options: ConnectOptions = {}) {
        this.url = url.replace(/\/+$/, "")
        this.options = { iceServers: [], reconnect: true, statsIntervalMs: 1000, heartbeatHz: 0, heartbeatMisses: 3, ...options }
        checkNumber("heartbeatHz", this.options.heartbeatHz, (v) => Number.isFinite(v) && v >= 0, ">= 0 (0 = no heartbeat)")
        checkNumber("heartbeatMisses", this.options.heartbeatMisses, (v) => Number.isInteger(v) && v >= 1, "an integer >= 1")
        checkNumber("bandwidthTargetFraction", this.options.bandwidthTargetFraction, (v) => v > 0 && v <= 1, "within (0, 1]")
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
                console.error("zenoh-web: state listener threw", error)
            }
        }
    }

    /** NTP-style sample: t0/t3 in our clock, t1/t2 bridge receive/send in its clock. */
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

    /** Clock-sync ping over control; also reports our current estimate to the bridge. */
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
     * A recvonly video transceiver bound to a bridge track: a free one, or a new one added through
     * a renegotiation over `control` (the bridge answers with a track for the new m-line).
     */
    _acquireVideoTransceiver(peer: RTCPeerConnection): Promise<RTCRtpTransceiver> {
        const free = this.#freeVideoTransceivers.get(peer)?.pop()
        if (free) {
            return Promise.resolve(free)
        }
        const run = async () => {
            await this.#connected
            if (peer !== this.#peer) {
                throw new Error("connection replaced")
            }
            const transceiver = peer.addTransceiver("video", { direction: "recvonly" })
            await peer.setLocalDescription(await peer.createOffer())
            const offer = peer.localDescription
            const response = await this._request({ op: "renegotiate", addVideo: true, sdp: { type: offer?.type, sdp: offer?.sdp } }, openTimeoutMs)
            await peer.setRemoteDescription(response.sdp as RTCSessionDescriptionInit)
            if (response.mid !== transceiver.mid) {
                throw new Error(`bridge bound mid ${String(response.mid)}, expected ${String(transceiver.mid)}`)
            }
            return transceiver
        }
        const result = this.#negotiation.then(run, run)
        this.#negotiation = result.catch(() => {})
        return result
    }

    _releaseVideoTransceiver(peer: RTCPeerConnection, transceiver: RTCRtpTransceiver): void {
        if (peer === this.#peer && peer.connectionState !== "closed") {
            const free = this.#freeVideoTransceivers.get(peer) ?? []
            free.push(transceiver)
            this.#freeVideoTransceivers.set(peer, free)
        }
    }

    async _open(): Promise<void> {
        const generation = ++this.#generation
        this.#setState("connecting")
        this.#clockSamples = []
        this.#connected = new Promise((resolve) => {
            this.#markConnected = resolve
        })
        if (this.#peer) {
            this.#freeVideoTransceivers.delete(this.#peer)
        }
        const peer = new RTCPeerConnection({ iceServers: this.options.iceServers })
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
                headers: { "content-type": "application/json" },
                body: JSON.stringify({ type: peer.localDescription?.type, sdp: peer.localDescription?.sdp }),
            })
            if (!response.ok) {
                throw new Error(`bridge refused offer: ${response.status} ${await response.text()}`)
            }
            await peer.setRemoteDescription(await response.json())
            await waitOpen(control, openTimeoutMs)
            if (this.options.bandwidthTargetFraction !== undefined) {
                await this._request({ op: "configure", bandwidthTargetFraction: this.options.bandwidthTargetFraction }, pingTimeoutMs)
            }
            this.codecs = Object.freeze((await this._request({ op: "codecs" }, pingTimeoutMs)).codecs as CodecInfo[])
            // a few quick samples so the bridge knows the clock offset before the first put
            for (let index = 0; index < initialClockPings; index++) {
                await this.#controlPing()
            }
        } catch (error) {
            this.#onLost(generation)
            throw error
        }
        this.#setState("connected")
        this.#markConnected()
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

    /** Stops sending heartbeats (the bridge then fires this frontend's deadmen); for testing deadman wiring. */
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
        // the bridge fires this frontend's deadmen when it loses us
        this.#tripArmedPublishers("disconnected")
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
                        console.warn("zenoh-web: reconnect failed", error)
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
        if (response.event !== undefined) {
            const endpoint = this.#endpointsById.get(Number(response.id))
            if (response.event === "tripped" && endpoint instanceof Publisher) {
                endpoint._trip(String(response.reason))
            } else if (response.event === "rejected") {
                endpoint?._rejected(String(response.reason))
            } else if (response.event === "accepted") {
                endpoint?._accepted()
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
            request.reject(new Error(`zenoh-web bridge: ${response.error ?? "error"}`))
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

    subscribe(key: string, options: SubscribeOptions, callback: (message: Message) => void): Subscription {
        validateSubscribeOptions(options ?? {}, this.codecs)
        const subscription = new Subscription(this, this.#nextId++, key, { ...options }, callback)
        this.#addEndpoint(subscription)
        return subscription
    }

    publisher(key: string, options: PublisherOptions = {}): Publisher {
        validatePublisherOptions(options)
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

    /** zenoh query. */
    async get(key: string, { timeoutMs = 5000 }: { timeoutMs?: number } = {}): Promise<GetReply[]> {
        const response = await this._request({ op: "get", key, timeoutMs }, timeoutMs + 2000)
        const replies = response.replies as { key?: string, bytes?: string, error?: string }[]
        return replies.map((reply) => {
            if (reply.error !== undefined) {
                return { key: null, bytes: fromBase64(reply.error), error: true }
            }
            return { key: reply.key ?? null, bytes: fromBase64(reply.bytes ?? "") }
        })
    }

    /**
     * Keys currently live on the zenoh network under `filter`, including ones never subscribed to.
     * See SPEC.md "Topic enumeration" for which kinds of keys can and can't be seen.
     * `probeMs: 0` lists declarations and tokens only, without subscribing to `filter`.
     */
    async listTopics(filter = "**", { probeMs = 600 }: { probeMs?: number } = {}): Promise<Topic[]> {
        const response = await this._request({ op: "listTopics", key: filter, probeMs }, probeMs + 5000)
        return response.topics as Topic[]
    }

    /** Polls bridge stats (and, without a heartbeat, clock sync) once; also runs on a timer while connected. */
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
            this.bridgeStats = { clock: response.clock, heartbeat: response.heartbeat, access: response.access, bandwidth: response.bandwidth ?? null } as BridgeStats
        }
        this.#refreshStats((response?.channels as BridgeChannelStats[] | undefined) ?? null)
    }

    #refreshStats(bridgeChannels: BridgeChannelStats[] | null): void {
        if (bridgeChannels) {
            const byId = new Map(bridgeChannels.map((channel) => [channel.id, channel]))
            for (const endpoint of this.#endpointsById.values()) {
                endpoint.bridgeStats = byId.get(endpoint.id) ?? null
            }
        }
        const stats: Record<string, KeyStats> = {}
        for (const endpoint of this.#endpoints) {
            const entry = stats[endpoint.key] ??= { received: 0, dropped: 0, backlogBytes: 0, rttMs: this.rttMs, bridge: null }
            const bridge = endpoint.bridgeStats
            entry.bridge = bridge
            if (endpoint instanceof Subscription) {
                entry.received += endpoint.received
                entry.dropped += endpoint.dropped
                entry.backlogBytes += bridge ? Number(bridge.stats.queuedBytes) + Number(bridge.stats.outstandingBytes) : 0
            } else {
                entry.received += endpoint.sent
                entry.dropped += endpoint.dropped + (bridge ? Number(bridge.stats.droppedStale) : 0)
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

/** Connects to a zenoh-web bridge, e.g. `await connect("http://robot.local:7448")`. */
export async function connect(url: string, options: ConnectOptions = {}): Promise<ZenohWeb> {
    const client = new ZenohWeb(url, options)
    await client._open()
    client._startStats()
    return client
}
