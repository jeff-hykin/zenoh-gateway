#!/usr/bin/env -S deno run --allow-all --unstable-net
// Data-channel throughput over a shaped link with delay jitter (test/shaped_link.js): one
// subscription to a stream that wants more than the link carries, measured as delivered bytes/s.
// Usage: deno run --allow-all --unstable-net test/throughput.js [--profile jitter50|wifi|spiky|all] [--seconds 15] [--delivery latest|reliable]

import { $ } from "https://esm.sh/dax-sh@0.42.0"
import { parseArgs } from "jsr:@std/cli@1/parse-args"
import { buildAll, check, finish, launchBrowser, machineLoad, startBridge, startPeer } from "./harness.js"
import { links, percentiles, startShapedLink } from "./shaped_link.js"

const args = parseArgs(Deno.args, { string: ["profile", "seconds", "delivery", "message-bytes"], default: { profile: "all", seconds: "15", delivery: "latest", "message-bytes": "60000" } })
const scratch = $.path(await Deno.makeTempDir({ prefix: "zenoh-web-throughput-" }))
console.log(`machine load at start: ${await machineLoad()}`)

/** 2 Mb/s: what one channel must sustain on the 50 ms link (0: report only) */
const minBytesPerSec = { jitter50: 250_000 }
const chosen = args.profile === "all" ? Object.keys(links) : [args.profile]
const seconds = Number(args.seconds)
const messageBytes = Number(args["message-bytes"])

try {
    const webRoot = await buildAll(scratch)
    // 60 KB at 30 Hz = 1.8 MB/s wanted: more than 2 Mb/s, less than the link
    const peer = await startPeer(["--synthetic", `tput/big=${messageBytes}@30`])
    const bridge = await startBridge(scratch, peer.zenohPort, webRoot)
    const browser = await launchBrowser()
    for (const name of chosen) {
        const link = links[name]
        const floor = minBytesPerSec[/** @type {keyof typeof minBytesPerSec} */ (name)] ?? 0
        const shaped = startShapedLink(bridge.url, link)
        const page = await browser.newPage(`${shaped.url}/test/blank.html`)
        $.logStep(`${name}: ${JSON.stringify(link)}`)
        const result = await page.evaluate(async (url, seconds, delivery) => {
            const { connect } = await import("/client/zenoh_web.js")
            const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
            const client = await connect(url, { heartbeatHz: 10, heartbeatMisses: 100 })
            let bytes = 0
            let messages = 0
            const latencies = []
            let measuring = false
            const subscription = client.subscribe("tput/big", { delivery }, (message) => {
                if (measuring) {
                    bytes += message.bytes.length
                    messages++
                    latencies.push(Date.now() - new DataView(message.bytes.buffer, message.bytes.byteOffset, 8).getFloat64(0, true))
                }
            })
            await subscription.ready()
            await sleep(3000)
            const timeline = []
            let lastBytes = 0
            const sampler = setInterval(() => {
                // bytes first: a poll can wait on the control channel
                const kBps = Math.round((bytes - lastBytes) / 1000)
                lastBytes = bytes
                client.pollStats().catch(() => {})
                const bandwidth = client.bridgeStats?.bandwidth
                timeline.push({ kBps, estimate: Math.round((bandwidth?.dataEstimateBytesPerSec ?? 0) / 1000), queueDelayMs: Math.round(bandwidth?.queueDelayMs ?? 0), delayEvents: bandwidth?.delayEvents, windowKB: Math.round((subscription.bridgeStats?.stats?.windowBytes ?? 0) / 1000) })
            }, 1000)
            measuring = true
            const started = performance.now()
            await sleep(seconds * 1000)
            measuring = false
            const elapsed = (performance.now() - started) / 1000
            clearInterval(sampler)
            await client.pollStats()
            const stats = subscription.bridgeStats?.stats
            subscription.close()
            client.close()
            return { bytesPerSec: bytes / elapsed, messagesPerSec: messages / elapsed, latencies, timeline, stats }
        }, { args: [shaped.url, seconds, args.delivery] })
        await page.close()
        const shaper = shaped.shapers.at(-1)
        const [p50, p95] = percentiles(result.latencies, [0.5, 0.95])
        const [delay50, delay95] = percentiles(shaper?.down.delays ?? [], [0.5, 0.95])
        console.log(`${name} timeline (delivered KB/s | bridge data estimate KB/s | queue delay ms | delay events | window KB): ${result.timeline.map((entry) => `${entry.kBps}|${entry.estimate}|${entry.queueDelayMs}|${entry.delayEvents}|${entry.windowKB}`).join(" ")}`)
        console.log(`${name} bridge stats: ${JSON.stringify(result.stats)}`)
        console.log(`${name} shaper down: ${shaper?.down.packets} packets, ${shaper?.down.dropped} dropped, ${shaper?.down.lost} lost, one-way delay p50 ${delay50?.toFixed(0)} / p95 ${delay95?.toFixed(0)} ms, max queue ${shaper?.down.maxQueueMs.toFixed(0)} ms`)
        const summary = `${name}: ${(result.bytesPerSec / 1000).toFixed(0)} KB/s (${(result.bytesPerSec * 8 / 1e6).toFixed(2)} Mb/s), ${result.messagesPerSec.toFixed(1)} msg/s of 30, latency p50 ${p50} / p95 ${p95} ms`
        if (floor) {
            check(result.bytesPerSec >= floor, `${summary} >= ${(floor * 8 / 1e6).toFixed(0)} Mb/s`)
        } else {
            console.log(`REPORT ${summary}`)
        }
        shaped.shapers.forEach((shaper) => shaper.stop())
    }
} catch (error) {
    check(false, String(error))
    console.error(error)
}
await finish(scratch.toString())
