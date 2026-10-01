// A userspace shaped link between Chrome and the bridge: a UDP relay with a rate limit and drop-tail
// queue, propagation delay, delay jitter and random loss per direction, plus a signaling proxy that
// rewrites SDP so ICE can only go through the relay (strips Chrome's candidates from the offer,
// replaces the bridge's with the relay's port).

import { freePort } from "./harness.js"

/**
 * @typedef {{
 *   bytesPerSec?: number,        // rate limit (Infinity: none)
 *   queueLimitBytes?: number,    // drop-tail queue in front of the rate limit
 *   oneWayDelayMs?: number,      // fixed propagation delay
 *   jitterMs?: number,           // extra delay, uniform in [0, jitterMs]: per packet, or per `jitterPeriodMs` if set (a Wi-Fi-like stall)
 *   jitterPeriodMs?: number,
 *   spikeChance?: number,        // with jitterPeriodMs: chance a period is a stall of spikeMs (uniform in [spikeMs / 2, spikeMs]) instead
 *   spikeMs?: number,
 *   lossRate?: number,           // random loss, 0..1
 * }} Direction
 * @typedef {{ packets: number, bytes: number, dropped: number, lost: number, maxQueueBytes: number, maxQueueMs: number, delays: number[] }} DirectionStats
 */

/**
 * One direction: packets queue FIFO (jitter delays a packet and everything behind it, it never
 * reorders), then leave at the rate limit.
 * @param {Direction} options
 * @param {(data: Uint8Array) => void} deliver
 */
function shapedDirection(options, deliver) {
    const { bytesPerSec = Infinity, queueLimitBytes = 1_000_000, oneWayDelayMs = 0, jitterMs = 0, jitterPeriodMs = 0, spikeChance = 0, spikeMs = 0, lossRate = 0 } = options
    /** @type {DirectionStats} */
    const stats = { packets: 0, bytes: 0, dropped: 0, lost: 0, maxQueueBytes: 0, maxQueueMs: 0, delays: [] }
    /** @type {{ data: Uint8Array, eligibleAt: number, arrived: number }[]} */
    const queue = []
    let queuedBytes = 0
    let lastEligibleAt = 0
    let jitterLevel = 0
    let jitterDrawnAt = -Infinity
    const jitterNow = (now) => {
        if (!jitterPeriodMs) {
            return Math.random() * jitterMs
        }
        if (now - jitterDrawnAt >= jitterPeriodMs) {
            jitterDrawnAt = now
            jitterLevel = Math.random() < spikeChance ? spikeMs * (0.5 + Math.random() / 2) : Math.random() * jitterMs
        }
        return jitterLevel
    }
    const bucketDepth = Number.isFinite(bytesPerSec) ? Math.max(1500, bytesPerSec * 0.005) : Infinity
    let tokens = 0
    let last = performance.now()
    let running = true
    ;(async () => {
        while (running) {
            await new Promise((resolve) => setTimeout(resolve, 1))
            const now = performance.now()
            tokens = Math.min(tokens + ((now - last) / 1000) * bytesPerSec, bucketDepth)
            last = now
            while (queue.length > 0 && queue[0].eligibleAt <= now && tokens >= queue[0].data.length) {
                const packet = /** @type {{ data: Uint8Array, arrived: number }} */ (queue.shift())
                queuedBytes -= packet.data.length
                if (Number.isFinite(tokens)) {
                    tokens -= packet.data.length
                }
                stats.packets++
                stats.bytes += packet.data.length
                if (stats.delays.length < 200_000) {
                    stats.delays.push(now - packet.arrived)
                }
                deliver(packet.data)
            }
        }
    })()
    return {
        stats,
        stop() {
            running = false
        },
        /** @param {Uint8Array} data */
        push(data) {
            const now = performance.now()
            if (Math.random() < lossRate) {
                stats.lost++
                return
            }
            if (queuedBytes + data.length > queueLimitBytes) {
                stats.dropped++
                return
            }
            lastEligibleAt = Math.max(lastEligibleAt, now + oneWayDelayMs + jitterNow(now))
            queue.push({ data, eligibleAt: lastEligibleAt, arrived: now })
            queuedBytes += data.length
            stats.maxQueueBytes = Math.max(stats.maxQueueBytes, queuedBytes)
            if (Number.isFinite(bytesPerSec)) {
                stats.maxQueueMs = Math.max(stats.maxQueueMs, (queuedBytes / bytesPerSec) * 1000)
            }
        },
    }
}

/**
 * Chrome <-> [chromeSide | bridgeSide] <-> the UDP port `targetPort` on `host`.
 * @param {number} targetPort
 * @param {{ down: Direction, up: Direction }} link down = toward Chrome, up = toward the target
 * @param {string} host the address both sides listen on and the target's (Chrome's own sockets
 *   are bound to the machine's addresses, not loopback)
 */
export function startShaper(targetPort, link, host = "127.0.0.1") {
    const chromeSide = Deno.listenDatagram({ transport: "udp", hostname: host, port: 0 })
    const bridgeSide = Deno.listenDatagram({ transport: "udp", hostname: host, port: 0 })
    const targetAddress = { transport: /** @type {"udp"} */ ("udp"), hostname: host, port: targetPort }
    /** @type {Deno.NetAddr | null} */
    let chromeAddress = null
    const down = shapedDirection(link.down, (data) => {
        if (chromeAddress) {
            chromeSide.send(data, chromeAddress).catch(() => {})
        }
    })
    const up = shapedDirection(link.up, (data) => {
        bridgeSide.send(data, targetAddress).catch(() => {})
    })
    ;(async () => {
        for await (const [data, address] of chromeSide) {
            chromeAddress = /** @type {Deno.NetAddr} */ (address)
            up.push(data)
        }
    })().catch(() => {})
    ;(async () => {
        for await (const [data] of bridgeSide) {
            down.push(data)
        }
    })().catch(() => {})
    return {
        port: /** @type {Deno.NetAddr} */ (chromeSide.addr).port,
        down: down.stats,
        up: up.stats,
        stop() {
            down.stop()
            up.stop()
            chromeSide.close()
            bridgeSide.close()
        },
    }
}

/**
 * Serves the page and signaling for the bridge at `bridgeUrl`, routing each peer connection's ICE
 * through its own shaper.
 * @param {string} bridgeUrl
 * @param {{ down: Direction, up: Direction }} link
 */
export function startShapedLink(bridgeUrl, link) {
    const port = freePort()
    /** @type {ReturnType<typeof startShaper>[]} */
    const shapers = []
    Deno.serve({ hostname: "127.0.0.1", port, onListen() {} }, async (request) => {
        const url = new URL(request.url)
        if (url.pathname !== "/offer") {
            return fetch(`${bridgeUrl}${url.pathname}${url.search}`)
        }
        const offer = await request.json()
        offer.sdp = offer.sdp.split("\r\n").filter((line) => !line.startsWith("a=candidate")).join("\r\n")
        const response = await fetch(`${bridgeUrl}/offer`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(offer) })
        const answer = await response.json()
        const lines = answer.sdp.split("\r\n")
        const loopback = lines.find((line) => line.startsWith("a=candidate") && line.includes(" 127.0.0.1 "))
        const shaper = startShaper(Number(loopback?.split(" ")[5]), link)
        shapers.push(shaper)
        let replaced = false
        answer.sdp = lines.flatMap((line) => {
            if (!line.startsWith("a=candidate")) {
                return [line]
            }
            if (replaced) {
                return []
            }
            replaced = true
            return [`a=candidate:1 1 udp 2130706431 127.0.0.1 ${shaper.port} typ host`]
        }).join("\r\n")
        return Response.json(answer)
    })
    return { url: `http://127.0.0.1:${port}`, shapers }
}

/** bridge->browser rate limit of the named links: about a good Wi-Fi link */
const linkBytesPerSec = 3_000_000
/** @type {Record<string, { down: Direction, up: Direction }>} */
export const links = {
    // ~50 ms RTT, every packet 0-20 ms extra each way (RTT 30-70 ms)
    jitter50: { down: { bytesPerSec: linkBytesPerSec, queueLimitBytes: 500_000, oneWayDelayMs: 15, jitterMs: 20 }, up: { oneWayDelayMs: 15, jitterMs: 20 } },
    // R1's Wi-Fi + tailscale: RTT 10-430 ms, the delay level jumping every 200 ms
    wifi: { down: { bytesPerSec: linkBytesPerSec, queueLimitBytes: 500_000, oneWayDelayMs: 5, jitterMs: 210, jitterPeriodMs: 200 }, up: { oneWayDelayMs: 5, jitterMs: 210, jitterPeriodMs: 200 } },
    // mostly a quiet 10-50 ms RTT with sudden stalls to 200-430 ms (what an RTO estimate trips on), 0.5% loss
    spiky: { down: { bytesPerSec: linkBytesPerSec, queueLimitBytes: 500_000, oneWayDelayMs: 5, jitterMs: 20, jitterPeriodMs: 100, spikeChance: 0.1, spikeMs: 210, lossRate: 0.005 }, up: { oneWayDelayMs: 5, jitterMs: 20, jitterPeriodMs: 100, spikeChance: 0.1, spikeMs: 210, lossRate: 0.005 } },
}

/**
 * p-quantiles of `values`.
 * @param {number[]} values
 * @param {number[]} quantiles
 */
export function percentiles(values, quantiles) {
    const sorted = [...values].sort((a, b) => a - b)
    return quantiles.map((q) => sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] : NaN)
}
