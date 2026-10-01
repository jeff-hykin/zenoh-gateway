#!/usr/bin/env -S deno run --allow-all --unstable-net
// Where a camera frame's time goes, publish -> arrival -> shown, for H.264 video and for
// imageTransport "jpeg", optionally over a shaped link (test/shaped_link.js). The test peer stamps
// each frame's send time into its pixels (--stamped-image), so every shown frame is identified
// exactly; the browser reads the stamp back from what it drew. Same machine, so one clock.
// Usage: deno run --allow-all --unstable-net test/video_latency.js [--profile direct|jitter50|wifi] [--seconds 15] [--transport video|jpeg|both]

import { $ } from "https://esm.sh/dax-sh@0.42.0"
import { parseArgs } from "jsr:@std/cli@1/parse-args"
import { buildAll, check, finish, fixturesDir, launchBrowser, machineLoad, startBridge, startPeer } from "./harness.js"
import { links, percentiles, startShapedLink } from "./shaped_link.js"

const args = parseArgs(Deno.args, { string: ["profile", "seconds", "transport", "hz"], default: { profile: "direct", seconds: "15", transport: "both", hz: "30" } })
const scratch = $.path(await Deno.makeTempDir({ prefix: "zenoh-web-video-latency-" }))
console.log(`machine load at start: ${await machineLoad()}`)

const link = args.profile === "direct" ? null : links[args.profile]
const seconds = Number(args.seconds)
const transports = args.transport === "both" ? ["video", "jpeg"] : [args.transport]
const key = "video/stamped"

try {
    const webRoot = await buildAll(scratch)
    const peer = await startPeer(["--stamped-image", `${key}=${fixturesDir.join("dimos/image_rgb8.bin")}:320@${args.hz}`])
    const bridge = await startBridge(scratch, peer.zenohPort, webRoot)
    const shaped = link ? startShapedLink(bridge.url, link) : null
    const url = shaped?.url ?? bridge.url
    const browser = await launchBrowser()
    for (const transport of transports) {
        const page = await browser.newPage(`${url}/test/blank.html`)
        $.logStep(`${transport} over ${args.profile}`)
        const result = await page.evaluate(async (url, key, transport, seconds) => {
            // keep the client's peer connection for getStats
            const peers = []
            const NativePeerConnection = window.RTCPeerConnection
            window.RTCPeerConnection = class extends NativePeerConnection {
                constructor(...rest) {
                    super(...rest)
                    peers.push(this)
                }
            }
            const { connect } = await import("/client/zenoh_web.js")
            const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
            const client = await connect(url, { heartbeatHz: 10, heartbeatMisses: 100 })
            const canvas = new OffscreenCanvas(320, 240)
            const context = /** @type {OffscreenCanvasRenderingContext2D} */ (canvas.getContext("2d", { willReadFrequently: true }))
            /** the 16-bit ms stamp in the bottom rows of what `source` shows */
            const readStamp = (source, width, height) => {
                canvas.width = width
                canvas.height = height
                context.drawImage(source, 0, 0, width, height)
                const row = context.getImageData(0, Math.floor(height * (1 - 8 / 240)), width, 1).data
                let stamp = 0
                for (let bit = 0; bit < 16; bit++) {
                    const x = Math.floor(((bit + 0.5) * width) / 16)
                    stamp = (stamp << 1) | (row[x * 4] > 127 ? 1 : 0)
                }
                return stamp
            }
            const unixAt = (performanceMs) => performance.timeOrigin + performanceMs
            const age = (atUnixMs, stamp) => {
                const wrapped = ((Math.round(atUnixMs) - stamp) % 65536 + 65536) % 65536
                return wrapped > 32768 ? wrapped - 65536 : wrapped
            }
            const frames = []
            let measuring = false
            const options = transport === "jpeg" ? { codec: "dimos-image", imageTransport: "jpeg", maxHz: 30 } : { codec: "dimos-image", maxHz: 30 }
            let video = null
            const subscription = client.subscribe(key, options, (message) => {
                if (transport === "jpeg" && message.image) {
                    const arrived = performance.now()
                    const stamp = readStamp(message.image, message.image.width, message.image.height)
                    message.image.close()
                    // shown: the next animation frame after drawing it
                    requestAnimationFrame((shownAt) => {
                        if (measuring) {
                            frames.push({ arrival: age(unixAt(arrived), stamp), shown: age(unixAt(shownAt), stamp) })
                        }
                    })
                } else if (transport === "video" && message.mediaStream && !video) {
                    video = document.createElement("video")
                    video.muted = true
                    video.autoplay = true
                    video.playsInline = true
                    document.body.append(video)
                    video.srcObject = message.mediaStream
                    const onFrame = (_now, metadata) => {
                        const stamp = readStamp(video, metadata.width, metadata.height)
                        if (measuring) {
                            // receiveTime is absent when Chrome renders without a jitter buffer (zero playout delay)
                            const arrival = metadata.receiveTime === undefined ? NaN : age(unixAt(metadata.receiveTime), stamp)
                            frames.push({ arrival, shown: age(unixAt(metadata.expectedDisplayTime), stamp) })
                        }
                        video.requestVideoFrameCallback(onFrame)
                    }
                    video.requestVideoFrameCallback(onFrame)
                    video.play().catch(() => {})
                }
            })
            await subscription.ready()
            await sleep(4000)
            const statsOf = async () => {
                const report = await peers.at(-1)?.getStats()
                let inbound = null
                report?.forEach((stat) => {
                    if (stat.type === "inbound-rtp" && stat.kind === "video" && (stat.packetsReceived ?? 0) > 0) {
                        inbound = stat
                    }
                })
                return inbound
            }
            const before = await statsOf()
            measuring = true
            await sleep(seconds * 1000)
            measuring = false
            const after = await statsOf()
            const delta = (name) => (after?.[name] ?? 0) - (before?.[name] ?? 0)
            const perFrame = (name, count) => (1000 * delta(name)) / Math.max(1, delta(count))
            await client.pollStats().catch(() => {})
            const bridgeStats = subscription.bridgeStats?.stats
            subscription.close()
            client.close()
            return {
                frames,
                fps: frames.length / seconds,
                rtp: after ? {
                    jitterBufferMs: perFrame("jitterBufferDelay", "jitterBufferEmittedCount"),
                    jitterBufferTargetMs: perFrame("jitterBufferTargetDelay", "jitterBufferEmittedCount"),
                    jitterBufferMinimumMs: perFrame("jitterBufferMinimumDelay", "jitterBufferEmittedCount"),
                    decodeMs: perFrame("totalDecodeTime", "framesDecoded"),
                    assemblyMs: perFrame("totalAssemblyTime", "framesAssembledFromMultiplePackets"),
                    processingMs: perFrame("totalProcessingDelay", "framesDecoded"),
                    framesDropped: delta("framesDropped"),
                    decoder: after.decoderImplementation,
                    packetsReceived: after.packetsReceived,
                    framesReceived: after.framesReceived,
                    framesDecoded: after.framesDecoded,
                } : null,
                picture: bridgeStats ? `${bridgeStats.videoWidth}x${bridgeStats.videoHeight} q${bridgeStats.quality?.toFixed?.(2)}` : null,
            }
        }, { args: [url, key, transport, seconds] })
        await page.close()
        const arrivals = result.frames.filter((frame) => !Number.isNaN(frame.arrival))
        const [arrival50, arrival95] = percentiles(arrivals.map((frame) => frame.arrival), [0.5, 0.95])
        const [shown50, shown95] = percentiles(result.frames.map((frame) => frame.shown), [0.5, 0.95])
        const [toShown50] = percentiles(arrivals.map((frame) => frame.shown - frame.arrival), [0.5])
        const rtp = result.rtp ? `; Chrome per frame: jitter buffer ${result.rtp.jitterBufferMs.toFixed(1)} ms (target ${result.rtp.jitterBufferTargetMs.toFixed(1)}, minimum ${result.rtp.jitterBufferMinimumMs.toFixed(1)}), assembly ${result.rtp.assemblyMs.toFixed(1)}, decode ${result.rtp.decodeMs.toFixed(1)}, receive->decoded ${result.rtp.processingMs.toFixed(1)} ms, dropped ${result.rtp.framesDropped}, packets/frames received/decoded ${result.rtp.packetsReceived}/${result.rtp.framesReceived}/${result.rtp.framesDecoded}` : ""
        console.log(`RESULT ${transport} ${args.profile}: ${result.fps.toFixed(1)} fps shown, publish->arrival p50 ${arrival50} / p95 ${arrival95} ms, publish->shown p50 ${shown50} / p95 ${shown95} ms, arrival->shown p50 ${toShown50} ms, picture ${result.picture}${rtp}`)
        check(result.frames.length > seconds * 5, `${transport}: frames shown and stamps read (${result.frames.length})`)
        check(shown50 >= 0 && shown50 < 2000, `${transport}: stamps decode to a plausible latency (${shown50} ms)`)
        if (transport === "video") {
            // the bridge asks for zero playout delay: Chrome shows a frame as soon as it is decoded
            check(toShown50 < 10, `video: shown within 10 ms of arriving (p50 ${toShown50} ms; ~22 ms with Chrome's default playout delay)`)
        }
    }
    shaped?.shapers.forEach((shaper) => shaper.stop())
} catch (error) {
    check(false, String(error))
    console.error(error)
}
await finish(scratch.toString())
