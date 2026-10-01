#!/usr/bin/env -S deno run --allow-all --unstable-net
// Latency of a strict-priority stream under bulk load, over a real bottleneck: a userspace UDP
// shaper sits between Chrome and the bridge (2 MB/s bridge->browser, 1 MB drop-tail queue, 5 ms
// each way). A signaling proxy rewrites SDP so ICE can only go through the shaper: it strips
// Chrome's candidates from the offer and replaces the bridge's with the shaper's port.
// Usage: deno run --allow-all --unstable-net test/latency.js

import { $ } from "https://esm.sh/dax-sh@0.42.0"
import { buildAll, check, finish, freePort, launchBrowser, machineLoad, startBridge, startPeer } from "./harness.js"

const scratch = $.path(await Deno.makeTempDir({ prefix: "zenoh-web-latency-" }))
console.log(`machine load at start: ${await machineLoad()}`)

const linkBytesPerSec = 2_000_000
const queueLimitBytes = 1_000_000
const oneWayDelayMs = 5
const bulkStreams = 5

/** @type {{ downstreamBytes: number, dropped: number, maxQueueBytes: number, maxQueueMs: number }} */
const shaperStats = { downstreamBytes: 0, dropped: 0, maxQueueBytes: 0, maxQueueMs: 0 }
/** per wall-clock second: the shaper queue's peak, in ms of link time */
const queuePeakBySecond = new Map()

/**
 * One shaped path: Chrome <-> [chromeSide | bridgeSide] <-> bridge. Bridge->Chrome is rate limited
 * with a FIFO queue (real queueing delay); both directions get a fixed propagation delay.
 * @param {number} bridgePort
 */
function startShaper(bridgePort) {
    const chromeSide = Deno.listenDatagram({ transport: "udp", hostname: "127.0.0.1", port: 0 })
    const bridgeSide = Deno.listenDatagram({ transport: "udp", hostname: "127.0.0.1", port: 0 })
    const bridgeAddress = { transport: "udp", hostname: "127.0.0.1", port: bridgePort }
    /** @type {Deno.NetAddr | null} */
    let chromeAddress = null
    /** @type {{ data: Uint8Array, eligibleAt: number }[]} */
    const queue = []
    let queuedBytes = 0
    const later = (ms, action) => setTimeout(action, ms)
    ;(async () => {
        for await (const [data, address] of chromeSide) {
            chromeAddress = /** @type {Deno.NetAddr} */ (address)
            later(oneWayDelayMs, () => bridgeSide.send(data, bridgeAddress).catch(() => {}))
        }
    })()
    ;(async () => {
        for await (const [data] of bridgeSide) {
            if (queuedBytes + data.length > queueLimitBytes) {
                shaperStats.dropped++
                continue
            }
            queue.push({ data, eligibleAt: performance.now() + oneWayDelayMs })
            queuedBytes += data.length
            shaperStats.maxQueueBytes = Math.max(shaperStats.maxQueueBytes, queuedBytes)
            const queueMs = (queuedBytes / linkBytesPerSec) * 1000
            shaperStats.maxQueueMs = Math.max(shaperStats.maxQueueMs, queueMs)
            const second = Math.floor(Date.now() / 1000)
            queuePeakBySecond.set(second, Math.max(queuePeakBySecond.get(second) ?? 0, queueMs))
        }
    })()
    // token bucket drain at `linkBytesPerSec`; tokens accumulate up to 5 ms of link time, since
    // timers fire every 1-4 ms and a one-packet bucket would cap the link at a packet per tick
    const bucketDepth = Math.max(1500, linkBytesPerSec * 0.005)
    ;(async () => {
        let tokens = 0
        let last = performance.now()
        while (true) {
            await new Promise((resolve) => setTimeout(resolve, 1))
            const now = performance.now()
            tokens = Math.min(tokens + ((now - last) / 1000) * linkBytesPerSec, bucketDepth)
            last = now
            while (queue.length > 0 && queue[0].eligibleAt <= now && tokens >= queue[0].data.length && chromeAddress) {
                const packet = /** @type {{ data: Uint8Array }} */ (queue.shift())
                queuedBytes -= packet.data.length
                tokens -= packet.data.length
                shaperStats.downstreamBytes += packet.data.length
                chromeSide.send(packet.data, chromeAddress).catch(() => {})
            }
        }
    })()
    return /** @type {Deno.NetAddr} */ (chromeSide.addr).port
}

/**
 * Serves the page and signaling for the bridge at `bridgeUrl`, routing ICE through a shaper.
 * @param {string} bridgeUrl
 */
function startSignalingProxy(bridgeUrl) {
    const port = freePort()
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
        const bridgePort = Number(loopback?.split(" ")[5])
        const shaperPort = startShaper(bridgePort)
        let replaced = false
        answer.sdp = lines.flatMap((line) => {
            if (!line.startsWith("a=candidate")) {
                return [line]
            }
            if (replaced) {
                return []
            }
            replaced = true
            return [`a=candidate:1 1 udp 2130706431 127.0.0.1 ${shaperPort} typ host`]
        }).join("\r\n")
        return Response.json(answer)
    })
    return `http://127.0.0.1:${port}`
}

try {
    const webRoot = await buildAll(scratch)
    $.logStep(`test peer: an important 200 B stream at 50 Hz, ${bulkStreams} bulk streams of 200 KB at 10 Hz (${bulkStreams * 2} MB/s)`)
    const peer = await startPeer([
        "--synthetic", "latency/important=200@50",
        ...Array.from({ length: bulkStreams }, (_, index) => ["--synthetic", `latency/bulk/${index}=200000@10`]).flat(),
    ])
    const bridge = await startBridge(scratch, peer.zenohPort, webRoot)
    const proxyUrl = startSignalingProxy(bridge.url)
    await $.sleep(1000)
    const browser = await launchBrowser()
    const page = await browser.newPage(`${proxyUrl}/test/blank.html`)
    $.logStep(`shaped link: ${linkBytesPerSec / 1e6} MB/s bridge->browser, ${queueLimitBytes / 1e6} MB queue, ${oneWayDelayMs} ms each way`)
    const results = await page.evaluate(async (proxyUrl, bulkStreams) => {
        const { connect, Priority } = await import("/client/zenoh_web.js")
        const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
        const client = await connect(proxyUrl, { heartbeatHz: 20, heartbeatMisses: 100 })
        /** p50/p99/max of the important stream's latency (arrival - publisher send time, same clock) over `seconds` */
        async function measureImportant(options, seconds) {
            const latencies = []
            const worstBySecond = {}
            let measuring = false
            const subscription = client.subscribe("latency/important", options, (message) => {
                if (measuring) {
                    const latency = Date.now() - new DataView(message.bytes.buffer, message.bytes.byteOffset, 8).getFloat64(0, true)
                    latencies.push(latency)
                    const second = Math.floor(Date.now() / 1000)
                    worstBySecond[second] = Math.max(worstBySecond[second] ?? 0, Math.round(latency))
                }
            })
            const timeline = []
            const sampler = setInterval(async () => {
                await client.pollStats().catch(() => {})
                const bandwidth = client.bridgeStats?.bandwidth
                timeline.push({ second: Math.floor(Date.now() / 1000), estimate: Math.round(bandwidth?.dataEstimateBytesPerSec ?? 0), budget: Math.round(bandwidth?.budgetBytesPerSec ?? 0), queueDelayMs: bandwidth?.queueDelayMs, delayEvents: bandwidth?.delayEvents })
            }, 1000)
            await subscription.ready()
            await sleep(2000)
            measuring = true
            await sleep(seconds * 1000)
            measuring = false
            clearInterval(sampler)
            await client.pollStats()
            const bridgeSide = subscription.bridgeStats?.stats
            subscription.close()
            latencies.sort((a, b) => a - b)
            const at = (q) => latencies[Math.min(latencies.length - 1, Math.floor(q * latencies.length))]
            return { count: latencies.length, p50: at(0.5), p99: at(0.99), max: latencies.at(-1), bridgeMaxSendLagMs: bridgeSide?.maxSendLagMs, bridgeMaxReceiveLagMs: bridgeSide?.maxReceiveLagMs, worstBySecond, timeline }
        }
        const out = {}
        out.alone = await measureImportant({ priority: Priority.INTERACTIVE_HIGH }, 10)
        const bulkBytes = Array(bulkStreams).fill(0)
        const bulk = Array.from({ length: bulkStreams }, (_, index) => client.subscribe(`latency/bulk/${index}`, { delivery: "latest" }, (message) => {
            bulkBytes[index] += message.bytes.length
        }))
        await Promise.all(bulk.map((subscription) => subscription.ready()))
        await sleep(5000)
        bulkBytes.fill(0)
        const started = performance.now()
        out.loaded = await measureImportant({ priority: Priority.INTERACTIVE_HIGH }, 10)
        out.bulkBytesPerSec = bulkBytes.reduce((sum, value) => sum + value, 0) / ((performance.now() - started) / 1000)
        await client.pollStats()
        out.bandwidth = client.bridgeStats?.bandwidth
        out.bulkAllocation = bulk[0].bridgeStats?.allocation
        // contrast, not asserted: the same stream without strict priority shares the bulk path
        out.notStrict = await measureImportant({}, 8)
        bulk.forEach((subscription) => subscription.close())
        client.close()
        return out
    }, { args: [proxyUrl, bulkStreams] })
    for (const phase of ["alone", "loaded", "notStrict"]) {
        console.log(`${phase} timeline (second: worst important latency ms | shaper queue peak ms | bridge estimate, budget B/s, queue delay ms, delay events):`)
        for (const entry of results[phase].timeline) {
            console.log(`  ${entry.second % 1000}: ${results[phase].worstBySecond[entry.second] ?? "-"} | ${(queuePeakBySecond.get(entry.second) ?? 0).toFixed(1)} | ${entry.estimate} ${entry.budget} ${entry.queueDelayMs?.toFixed?.(1)} ${entry.delayEvents}`)
        }
        delete results[phase].timeline
        delete results[phase].worstBySecond
    }
    console.log(JSON.stringify({ ...results, shaper: shaperStats }, null, 1))
    const { alone, loaded, notStrict } = results
    const increase = loaded.p99 - alone.p99
    check(alone.count > 400 && loaded.count > 400, `important stream delivers through the shaper (${alone.count} alone, ${loaded.count} loaded, 50 Hz x 10 s)`)
    check(results.bulkBytesPerSec > linkBytesPerSec * 0.3, `bulk streams really load the link (${(results.bulkBytesPerSec / 1e6).toFixed(2)} MB/s of ${linkBytesPerSec / 1e6} MB/s)`)
    check(increase < 20, `strict-priority p99 latency grows < 20 ms under ${bulkStreams} bulk streams: alone p50 ${alone.p50} / p99 ${alone.p99} / max ${alone.max} ms, loaded p50 ${loaded.p50} / p99 ${loaded.p99} / max ${loaded.max} ms (+${increase} ms)`)
    console.log(`contrast (not asserted): same stream without strict priority under load: p50 ${notStrict.p50} / p99 ${notStrict.p99} / max ${notStrict.max} ms; shaper queue peaked at ${shaperStats.maxQueueMs.toFixed(0)} ms (${shaperStats.maxQueueBytes} bytes), dropped ${shaperStats.dropped} packets`)
} catch (error) {
    check(false, String(error))
    console.error(error)
}
await finish(scratch.toString())
