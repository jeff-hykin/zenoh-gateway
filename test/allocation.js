#!/usr/bin/env -S deno run --allow-all
// Bandwidth allocation end-to-end, with the bridge's budget capped (--max-bandwidth-bytes-per-sec):
// flex-shrink by bandwidthPriority down to dangerousMinHz floors, and the quality/Hz tradeoff of
// a transcoded (H.264) stream.
// Usage: deno run --allow-all test/allocation.js

import { $ } from "https://esm.sh/dax-sh@0.42.0"
import { buildAll, check, finish, fixtureKey, fixturesDir, launchBrowser, loadManifest, machineLoad, startBridge, startPeer } from "./harness.js"

const scratch = $.path(await Deno.makeTempDir({ prefix: "zenoh-web-allocation-" }))
console.log(`machine load at start: ${await machineLoad()}`)

const rawBudget = 480_000
const videoBudget = 12_000
const videoEntry = loadManifest().entries.find((entry) => entry.file === "dimos/image_rgb8.bin")
if (!videoEntry) {
    throw new Error("fixture dimos/image_rgb8.bin missing from manifest")
}
const videoKey = fixtureKey(videoEntry).replace("/fixture/", "/allocation/")

try {
    const webRoot = await buildAll(scratch)
    $.logStep("test peer: three 20 KB streams at 50 Hz, one 320x240 image stream at 20 Hz")
    const peer = await startPeer([
        ...["a", "b", "c"].flatMap((name) => ["--synthetic", `alloc/${name}=20000@50`]),
        "--publish", `${videoKey}=${fixturesDir.join(videoEntry.file)}@20`,
    ])
    const rawBridge = await startBridge(scratch, peer.zenohPort, webRoot, ["--max-bandwidth-bytes-per-sec", String(rawBudget)])
    const videoBridge = await startBridge(scratch, peer.zenohPort, webRoot, ["--max-bandwidth-bytes-per-sec", String(videoBudget)])
    await $.sleep(1000)
    const browser = await launchBrowser()
    const page = await browser.newPage(`${rawBridge.url}/test/blank.html`)

    $.logStep(`flex-shrink: budget ${rawBudget} B/s for 3 x 400 KB/s`)
    const raw = await page.evaluate(async (bridgeUrl) => {
        const { connect } = await import("/client/zenoh_web.js")
        const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
        const client = await connect(bridgeUrl)
        const streams = {
            a: { bandwidthPriority: 0.1, maxHz: 20, dangerousMinHz: 2 },
            b: { bandwidthPriority: 10, maxHz: 20, dangerousMinHz: 2 },
            c: { bandwidthPriority: 10, maxHz: 20, dangerousMinHz: 2 },
        }
        const counts = { a: 0, b: 0, c: 0 }
        const bytes = { a: 0, b: 0, c: 0 }
        let measuring = false
        const subscriptions = Object.entries(streams).map(([name, options]) => client.subscribe(`alloc/${name}`, options, (message) => {
            if (measuring) {
                counts[name]++
                bytes[name] += message.bytes.length
            }
        }))
        await Promise.all(subscriptions.map((subscription) => subscription.ready()))
        // the allocator needs a few rounds to measure source rates and sizes
        await sleep(3000)
        measuring = true
        const seconds = 6
        await sleep(seconds * 1000)
        measuring = false
        await client.pollStats()
        const result = {
            hz: Object.fromEntries(Object.entries(counts).map(([name, count]) => [name, count / seconds])),
            bytesPerSec: Object.values(bytes).reduce((sum, value) => sum + value, 0) / seconds,
            allocation: Object.fromEntries(subscriptions.map((subscription, index) => [Object.keys(streams)[index], subscription.bridgeStats?.allocation])),
            bandwidth: client.bridgeStats?.bandwidth,
        }
        client.close()
        return result
    }, { args: [rawBridge.url] })
    console.log(JSON.stringify(raw, null, 1))
    const { hz, allocation } = raw
    check(hz.b >= 1.5 && hz.b <= 3 && hz.c >= 1.5 && hz.c <= 3, `high-shrink streams (bandwidthPriority 10) fall to about their floor of 2 Hz (b ${hz.b.toFixed(2)} Hz, c ${hz.c.toFixed(2)} Hz)`)
    check(hz.a >= 17, `the low-shrink stream (bandwidthPriority 0.1) keeps its rate (a ${hz.a.toFixed(2)} Hz of 20)`)
    check(allocation.b?.constrained && allocation.c?.constrained && allocation.a?.hz > 19 && allocation.b?.hz < 2.5,
        `stats show each stream's allocation (a ${allocation.a?.hz?.toFixed(2)} Hz, b ${allocation.b?.hz?.toFixed(2)} Hz, b floor ${allocation.b?.floorBytesPerSec?.toFixed(0)} B/s)`)
    check(raw.bandwidth?.budgetBytesPerSec === rawBudget && raw.bandwidth?.capBytesPerSec === rawBudget && raw.bandwidth?.constrained === true, `frontend budget is the cap (${JSON.stringify(raw.bandwidth)})`)
    check(raw.bytesPerSec <= rawBudget * 1.1, `delivered payload stays within the budget (${raw.bytesPerSec.toFixed(0)} B/s <= ${rawBudget} + 10%)`)

    $.logStep(`quality/Hz tradeoff: one H.264 stream wanting ~29 KB/s, budget ${videoBudget} B/s`)
    await page.goto(`${videoBridge.url}/test/blank.html`)
    const tradeoffs = []
    for (const tradeoff of [0, 1]) {
        tradeoffs.push(await page.evaluate(async (bridgeUrl, key, tradeoff) => {
            const { connect } = await import("/client/zenoh_web.js")
            const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
            const client = await connect(bridgeUrl)
            const frames = []
            let measuring = false
            const subscription = client.subscribe(key, { codec: "dimos-image", maxHz: 20, dangerousMinHz: 1, minQuality: 0.2, qualityToHzTradeoff: tradeoff }, (message) => {
                if (measuring) {
                    frames.push(message.video)
                }
            })
            await subscription.ready()
            const element = document.createElement("video")
            element.muted = true
            element.srcObject = subscription.mediaStream
            document.body.append(element)
            await element.play().catch(() => {})
            await sleep(3000)
            measuring = true
            const decodedBefore = element.getVideoPlaybackQuality().totalVideoFrames
            const seconds = 5
            await sleep(seconds * 1000)
            measuring = false
            const decoded = element.getVideoPlaybackQuality().totalVideoFrames - decodedBefore
            await client.pollStats()
            const result = {
                tradeoff,
                hz: frames.length / seconds,
                decodedHz: decoded / seconds,
                widths: [...new Set(frames.map((frame) => frame.width))],
                videoWidth: element.videoWidth,
                quality: frames.at(-1)?.quality,
                allocation: subscription.bridgeStats?.allocation,
                bandwidth: client.bridgeStats?.bandwidth,
            }
            element.remove()
            client.close()
            await sleep(500)
            return result
        }, { args: [videoBridge.url, videoKey, tradeoff] }))
    }
    console.log(JSON.stringify(tradeoffs, null, 1))
    const [keepQuality, keepHz] = tradeoffs
    check(keepQuality.videoWidth === 320 && keepQuality.widths.join() === "320" && keepQuality.hz < 12 && keepQuality.hz > 4,
        `tradeoff 0 keeps quality, Hz drops (${keepQuality.videoWidth}px wide, ${keepQuality.hz.toFixed(1)} Hz sent, ${keepQuality.decodedHz.toFixed(1)} Hz decoded, quality ${keepQuality.quality?.toFixed(2)})`)
    check(keepHz.videoWidth < 320 && keepHz.hz >= 16,
        `tradeoff 1 keeps Hz, quality drops (${keepHz.videoWidth}px wide, ${keepHz.hz.toFixed(1)} Hz sent, ${keepHz.decodedHz.toFixed(1)} Hz decoded, quality ${keepHz.quality?.toFixed(2)})`)
    check(keepQuality.allocation?.constrained && keepHz.allocation?.constrained && keepQuality.allocation.quality > keepHz.allocation.quality && keepQuality.allocation.hz < keepHz.allocation.hz,
        `allocations: tradeoff 0 -> q ${keepQuality.allocation?.quality} @ ${keepQuality.allocation?.hz?.toFixed(1)} Hz, tradeoff 1 -> q ${keepHz.allocation?.quality} @ ${keepHz.allocation?.hz?.toFixed(1)} Hz`)
} catch (error) {
    check(false, String(error))
    console.error(error)
}
await finish(scratch.toString())
