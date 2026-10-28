// The latency harness's page (bench/latency.ts serves it): connects to a zenoh-gateway without a lease, subscribes the
// way the Controller does, and keeps per-second counters that window.sample() hands to the harness and resets.
//
// Video frame timing, per presented frame (requestVideoFrameCallback). Gateways from 0.5.20 put each frame's media time
// on the track and its write time in its metadata message: a presented frame's rtpTimestamp then names its message
// exactly (rtp - rtp of the first frame = mediaTimeMs - mediaTimeMs of that frame's message). Older gateways:
// - the gateway sends a metadata message on the subscription's data channel right after it writes each encoded frame
//   to the track, carrying the sample's gateway-receive time: its arrival here is the frame's write time (+ a localhost
//   data-channel hop) and `timestamp` is when the gateway got the sample;
// - webrtc-rs stamps frame k with rtp0 + the durations of the frames before it (the time between writes), so a frame's
//   write time is anchor + (rtp - rtp0) / 90 ms, anchored on the first presented frame and the first metadata message;
//   each presented frame is then matched to the metadata message whose arrival is nearest that estimate.
// Stages: gateway rx -> written (decode + scale + encode + the DC hop), written -> received (pacer queue + network +
// assembly), received -> presented (jitter buffer + decode + render).
const params = new URLSearchParams(location.search)
const config = JSON.parse(params.get("config"))
const errors = []
window.addEventListener("error", (event) => errors.push(String(event.message)))
window.addEventListener("unhandledrejection", (event) => errors.push(String(event.reason?.stack ?? event.reason)))
window.harnessErrors = errors

const nowEpoch = () => performance.timeOrigin + performance.now()
const toEpoch = (perfMs) => performance.timeOrigin + perfMs

let gw = null
const streams = {}
const subscriptions = []
const video = {
    meta: [],
    window: { meta: 0, metaBytes: 0, rxToWrite: [], frames: [], pixel: [] },
    anchor: null,
    presented: 0,
    lastRtp: null,
}

function stream(key) {
    return streams[key] ??= { n: 0, bytes: 0, ages: [], points: 0 }
}

function attachVideo(mediaStream) {
    const element = document.createElement("video")
    element.muted = true
    element.autoplay = true
    element.playsInline = true
    document.body.append(element)
    element.srcObject = mediaStream
    element.play().catch((error) => errors.push(`video.play: ${error}`))
    // --pixel-stamp: the source burned its stamp into the picture (40 blocks across the top 32 rows, ms, bit 39 first):
    // read back from what is shown, it times camera stamp -> displayed with nothing of the gateway's or WebRTC's help
    const canvas = config.pixelStamp ? new OffscreenCanvas(1, 1) : null
    const context = canvas?.getContext("2d", { willReadFrequently: true })
    const readStamp = (width, height) => {
        canvas.width = width
        canvas.height = Math.max(1, Math.round((32 * height) / 720))
        context.drawImage(element, 0, 0, width, height, 0, 0, width, height)
        const row = context.getImageData(0, Math.floor(canvas.height / 2), width, 1).data
        let ms = 0
        for (let bit = 0; bit < 40; bit++) {
            const x = Math.floor(((bit + 0.5) * width) / 40)
            const luma = (row[x * 4] + row[x * 4 + 1] + row[x * 4 + 2]) / 3
            ms = ms * 2 + (luma > 127 ? 1 : 0)
        }
        return ms
    }
    const onFrame = (_now, frame) => {
        element.requestVideoFrameCallback(onFrame)
        if (canvas && frame.width) {
            const stamp = readStamp(frame.width, frame.height)
            const shown = toEpoch(frame.presentationTime)
            // the stamp is the low 40 bits of unix ms
            const full = Math.floor(shown / 2 ** 40) * 2 ** 40 + stamp
            const age = shown - full
            if (age > -1000 && age < 60000) {
                video.window.pixel.push(age)
            } else {
                video.window.pixelBad = (video.window.pixelBad ?? 0) + 1
            }
        }
        if (frame.rtpTimestamp === undefined) {
            return
        }
        const present = toEpoch(frame.presentationTime)
        const receive = frame.receiveTime === undefined ? null : toEpoch(frame.receiveTime)
        // sequential matching: frames reach the track in metadata order, so a frame is the metadata after the last
        // matched one nearest (last match's arrival + RTP ticks since it / 90); RTP ticks alone drift (webrtc-rs stamps
        // each frame with the gap since the previous write, and the first gap is 1 / the granted Hz: up to 10 s)
        if (video.meta.length === 0) {
            return
        }
        let best = null
        if (video.meta[0].mediaTimeMs !== undefined) {
            // exact: the metadata whose media time is this frame's (anchored once on the newest message)
            video.rtpBase ??= { rtp: frame.rtpTimestamp, mediaTimeMs: video.meta.at(-1).mediaTimeMs }
            let ticks = frame.rtpTimestamp - video.rtpBase.rtp
            if (ticks < -(2 ** 31)) {
                ticks += 2 ** 32
            }
            const mediaTimeMs = video.rtpBase.mediaTimeMs + ticks / 90
            for (let i = video.meta.length - 1; i >= 0; i--) {
                if (best === null || Math.abs(video.meta[i].mediaTimeMs - mediaTimeMs) < Math.abs(best.mediaTimeMs - mediaTimeMs)) {
                    best = video.meta[i]
                }
            }
            if (!best || Math.abs(best.mediaTimeMs - mediaTimeMs) > 1) {
                video.unmatched = (video.unmatched ?? 0) + 1
                // anchored on the wrong message (the first shown frame wasn't the newest written): anchor again
                video.missesInARow = (video.missesInARow ?? 0) + 1
                if (video.missesInARow >= 3) {
                    video.rtpBase = null
                    video.missesInARow = 0
                }
                return
            }
            video.missesInARow = 0
            best.estimateError = best.mediaTimeMs - mediaTimeMs
            const writeAt = best.writtenPage
            video.presented++
            video.window.frames.push({
                total: present - best.rxPage,
                rxToWrite: writeAt - best.rxPage,
                writeToReceive: receive === null ? null : receive - writeAt,
                receiveToPresent: receive === null ? null : present - receive,
                matchErrorMs: best.estimateError,
                processingMs: frame.processingDuration === undefined ? null : frame.processingDuration * 1000,
            })
            return
        }
        if (video.anchor === null) {
            best = video.meta.at(-1)
        } else {
            let ticks = frame.rtpTimestamp - video.anchor.rtp
            if (ticks < -(2 ** 31)) {
                ticks += 2 ** 32
            }
            const estimate = video.anchor.meta.arrival + ticks / 90
            const after = video.meta.filter((candidate) => candidate.index > video.anchor.meta.index)
            for (const candidate of after) {
                if (best === null || Math.abs(candidate.arrival - estimate) < Math.abs(best.arrival - estimate)) {
                    best = candidate
                }
            }
            if (best && Math.abs(best.arrival - estimate) > 300) {
                video.rtpJumps = (video.rtpJumps ?? 0) + 1
                best = after[0]
            }
            if (best) {
                best.estimateError = best.arrival - estimate
            }
        }
        if (!best) {
            return
        }
        video.anchor = { rtp: frame.rtpTimestamp, meta: best }
        const writeEstimate = best.arrival
        video.presented++
        video.window.frames.push({
            total: present - best.rxPage,
            rxToWrite: best.arrival - best.rxPage,
            writeToReceive: receive === null ? null : receive - writeEstimate,
            receiveToPresent: receive === null ? null : present - receive,
            matchErrorMs: best.estimateError ?? null,
            processingMs: frame.processingDuration === undefined ? null : frame.processingDuration * 1000,
        })
    }
    element.requestVideoFrameCallback(onFrame)
}

async function main() {
    const { connect } = await import(config.client)
    gw = await connect(config.gateway, { heartbeatHz: 10, heartbeatMisses: 30, ...(config.token ? { token: config.token } : {}) })
    window.gw = gw
    for (const { key, options, kind } of config.subscriptions) {
        let attached = false
        const subscription = gw.subscribe(key, options, (message) => {
            const offset = gw.clockOffsetMs ?? 0
            const rxPage = message.timestamp - offset
            if (kind === "video") {
                if (message.video) {
                    const arrival = nowEpoch()
                    video.meta.push({ index: video.metaIndex = (video.metaIndex ?? 0) + 1, arrival, rxPage, bytes: message.video.encodedBytes, keyframe: message.video.keyframe, mediaTimeMs: message.video.mediaTimeMs, writtenPage: message.video.writtenMs === undefined ? arrival : message.video.writtenMs - offset })
                    if (video.meta.length > 2000) {
                        video.meta.splice(0, 1000)
                    }
                    video.window.meta++
                    video.window.metaBytes += message.video.encodedBytes
                    video.window.rxToWrite.push((message.video.writtenMs === undefined ? arrival : message.video.writtenMs - offset) - rxPage)
                    video.window.size = [message.video.width, message.video.height]
                    video.window.quality = message.video.quality
                }
                if (message.mediaStream && !attached) {
                    attached = true
                    attachVideo(message.mediaStream)
                }
                return
            }
            const entry = stream(key)
            entry.n++
            entry.bytes += message.bytes.byteLength
            entry.ages.push(nowEpoch() - rxPage)
            const positions = message.decoded?.positions
            if (positions) {
                entry.points += positions.length / 3
            }
        })
        subscriptions.push({ key, subscription })
    }
    window.harnessReady = true
}

let previousInbound = null
window.sample = async () => {
    const states = subscriptions.map(({ key, subscription }) => ({ key, state: subscription.state, rejected: subscription.rejectionReason ?? null }))
    const out = { subscriptions: states, at: nowEpoch(), rttMs: gw?.rttMs ?? null, clockOffsetMs: gw?.clockOffsetMs ?? null, errors: errors.splice(0), streams: {}, video: null, bandwidth: gw?.gatewayStats?.bandwidth ?? null, congestion: gw?.congestion ?? null }
    for (const [key, entry] of Object.entries(streams)) {
        out.streams[key] = { ...entry }
        Object.assign(entry, { n: 0, bytes: 0, ages: [], points: 0 })
    }
    for (const [key, stats] of Object.entries(gw?.stats ?? {})) {
        const target = out.streams[key] ??= { n: 0, bytes: 0, ages: [], points: 0 }
        target.gateway = stats.gateway ? { stats: stats.gateway.stats, allocation: stats.gateway.allocation } : null
        target.clientDropped = stats.dropped
    }
    if (config.subscriptions.some((subscription) => subscription.kind === "video")) {
        let inbound = null
        for (const peer of window.harnessPeers) {
            const stats = await peer.getStats()
            stats.forEach((entry) => {
                if (entry.type === "inbound-rtp" && entry.kind === "video") {
                    inbound = entry
                }
            })
        }
        const delta = (name) => inbound && previousInbound ? (inbound[name] ?? 0) - (previousInbound[name] ?? 0) : null
        const emitted = delta("jitterBufferEmittedCount")
        const decoded = delta("framesDecoded")
        out.video = {
            ...video.window,
            presentedTotal: video.presented,
            metaTotal: video.metaIndex ?? 0,
            rtpJumps: video.rtpJumps ?? 0,
            unmatched: video.unmatched ?? 0,
            exact: video.meta[0]?.mediaTimeMs !== undefined,
            inbound: inbound && {
                framesReceived: inbound.framesReceived,
                framesDecoded: inbound.framesDecoded,
                framesDropped: inbound.framesDropped,
                bytesReceived: inbound.bytesReceived,
                nackCount: inbound.nackCount,
                pliCount: inbound.pliCount,
                packetsLost: inbound.packetsLost,
                width: inbound.frameWidth,
                height: inbound.frameHeight,
                jitterBufferMs: emitted ? (delta("jitterBufferDelay") * 1000) / emitted : null,
                jitterBufferTargetMs: emitted ? (delta("jitterBufferTargetDelay") * 1000) / emitted : null,
                decodeMs: decoded ? (delta("totalDecodeTime") * 1000) / decoded : null,
                processingMs: decoded ? (delta("totalProcessingDelay") * 1000) / decoded : null,
                framesReceivedDelta: delta("framesReceived"),
                framesDecodedDelta: decoded,
                framesDroppedDelta: delta("framesDropped"),
                bytesDelta: delta("bytesReceived"),
            },
        }
        previousInbound = inbound
        video.window = { meta: 0, metaBytes: 0, rxToWrite: [], frames: [], pixel: [] }
    }
    return out
}

// every peer connection the client makes, for getStats
window.harnessPeers = []
const NativePeerConnection = window.RTCPeerConnection
window.RTCPeerConnection = class extends NativePeerConnection {
    constructor(...args) {
        super(...args)
        window.harnessPeers.push(this)
    }
}

main().catch((error) => errors.push(`main: ${error?.stack ?? error}`))
