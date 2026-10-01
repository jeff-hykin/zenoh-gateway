#!/usr/bin/env -S deno run --allow-all
// End-to-end: zenoh test peer <-> zenoh-web bridge <-> headless Chrome over real WebRTC.
// Usage: deno run --allow-all test/e2e.js

import { $ } from "https://esm.sh/dax-sh@0.42.0"
import { launch } from "jsr:@astral/astral@0.5.6"

const repoRoot = $.path(import.meta.url).parentOrThrow().parentOrThrow()
const bridgeDir = repoRoot.join("bridge")
const scratch = $.path(await Deno.makeTempDir({ prefix: "zenoh-web-e2e-" }))

/** @returns {number} */
function freePort() {
    const listener = Deno.listen({ port: 0, hostname: "127.0.0.1" })
    const port = /** @type {Deno.NetAddr} */ (listener.addr).port
    listener.close()
    return port
}

/** @type {string[]} */
const failures = []
/**
 * @param {boolean} condition
 * @param {string} description
 */
function check(condition, description) {
    console.log(`${condition ? "PASS" : "FAIL"} ${description}`)
    if (!condition) {
        failures.push(description)
    }
}

/**
 * Collects a child's stdout lines and resolves waiters on matching lines.
 * @param {ReadableStream<Uint8Array>} stream
 * @param {string} name
 */
function lineCollector(stream, name) {
    /** @type {string[]} */
    const lines = []
    /** @type {{ test: (line: string) => boolean, resolve: (line: string) => void }[]} */
    let waiters = []
    ;(async () => {
        let buffered = ""
        for await (const chunk of stream.pipeThrough(new TextDecoderStream())) {
            buffered += chunk
            const parts = buffered.split("\n")
            buffered = parts.pop() ?? ""
            for (const line of parts) {
                lines.push(line)
                if (Deno.env.get("E2E_VERBOSE")) {
                    console.log(`[${name}] ${line}`)
                }
                waiters = waiters.filter((waiter) => {
                    if (waiter.test(line)) {
                        waiter.resolve(line)
                        return false
                    }
                    return true
                })
            }
        }
    })()
    return {
        lines,
        /**
         * @param {(line: string) => boolean} test
         * @param {number} timeoutMs
         */
        waitFor(test, timeoutMs) {
            const existing = lines.find(test)
            if (existing) {
                return Promise.resolve(existing)
            }
            return new Promise((resolve, reject) => {
                const timer = setTimeout(() => reject(new Error(`${name}: timed out waiting for line`)), timeoutMs)
                waiters.push({ test, resolve: (line) => {
                    clearTimeout(timer)
                    resolve(line)
                } })
            })
        },
    }
}

$.logStep("building bridge + test peer (release)")
await $`cargo build --release --bin zenoh-web --example test_peer`.cwd(bridgeDir)

const zenohPort = freePort()
const httpPort = freePort()
const bridgeUrl = `http://127.0.0.1:${httpPort}`

// Isolated zenoh: no multicast scouting, so the test never touches other zenoh systems on the LAN.
const zenohConfigPath = scratch.join("bridge_zenoh.json5")
zenohConfigPath.writeTextSync(JSON.stringify({ mode: "peer", scouting: { multicast: { enabled: false } }, listen: { endpoints: [] } }))

/** @type {{ kill: (signal?: Deno.Signal) => void }[]} */
const children = []
/** @type {import("jsr:@astral/astral@0.5.6").Browser | null} */
let browser = null

async function cleanup() {
    await browser?.close().catch(() => {})
    for (const child of children) {
        try {
            child.kill("SIGTERM")
        } catch {
            // already exited
        }
    }
}

try {
    $.logStep(`starting test peer on tcp/127.0.0.1:${zenohPort}`)
    const peer = $`${bridgeDir.join("target/release/examples/test_peer")} --listen tcp/127.0.0.1:${zenohPort} --fast-hz 2000 --fast-bytes 60000`
        .stdout("piped").stderr("inherit").noThrow().spawn()
    children.push(peer)
    const peerOutput = lineCollector(peer.stdout(), "peer")
    await peerOutput.waitFor((line) => line === "READY", 15000)

    $.logStep(`starting bridge on ${bridgeUrl}`)
    const bridge = $`${bridgeDir.join("target/release/zenoh-web")} --port ${httpPort} --zenoh-config ${zenohConfigPath} --connect tcp/127.0.0.1:${zenohPort} --serve ${repoRoot}`
        .env("RUST_LOG", Deno.env.get("RUST_LOG") ?? "info,zenoh=warn,zenoh_web=info")
        .stdout("inherit").stderr("piped").noThrow().spawn()
    children.push(bridge)
    const bridgeOutput = lineCollector(bridge.stderr(), "bridge")
    await bridgeOutput.waitFor((line) => line.includes("listening on"), 15000)
    // let the bridge's zenoh session reach the peer before the late-subscriber history check
    await $.sleep(1000)

    $.logStep("launching headless Chrome (own instance, random debugging port)")
    browser = await launch({ headless: true, args: ["--no-sandbox"] })
    const page = await browser.newPage(`${bridgeUrl}/examples/viewer.html`)

    $.logStep("viewer page")
    await $.sleep(3000)
    const viewer = await page.evaluate(() => ({
        state: document.getElementById("state")?.textContent,
        keys: [...document.querySelectorAll("td.key")].map((cell) => cell.textContent),
        images: [...document.querySelectorAll("#images img")].filter((image) => image.naturalWidth > 0).length,
    }))
    console.log("viewer:", JSON.stringify(viewer))
    check(viewer.state === "connected", "viewer connects")
    check(viewer.keys.includes("test/jpeg") && viewer.keys.includes("test/fast"), "viewer lists keys from a ** subscription")
    check(viewer.images >= 1, "viewer decodes the JPEG payload into an image")
    await page.screenshot().then((png) => scratch.join("viewer.png").writeSync(png))

    // fresh page so the viewer's ** subscription doesn't compete with the measurements below
    await page.goto(`${bridgeUrl}/test/blank.html`)
    const viewerGone = await bridgeOutput.waitFor((line) => line.includes("peer 1: gone"), 5000).then(() => true, () => false)
    check(viewerGone, "bridge drops the viewer's peer connection after the page navigates away")
    const results = await page.evaluate(async (bridgeUrl) => {
        const { connect, Priority, encodePut } = await import("/client/zenoh_web.js")
        const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
        const decoder = new TextDecoder()
        const out = {}
        const client = await connect(bridgeUrl)
        out.state = client.state

        // history: test/cached was put once, long before we subscribed
        out.cached = await new Promise((resolve) => {
            const timer = setTimeout(() => resolve(null), 5000)
            const subscription = client.subscribe("test/cached", { delivery: "latest" }, (message) => {
                clearTimeout(timer)
                subscription.close()
                resolve(decoder.decode(message.bytes))
            })
        })

        const replies = await client.get("test/queryable")
        out.get = replies.map((reply) => ({ key: reply.key, text: decoder.decode(reply.bytes) }))

        const command = client.publisher("test/frombrowser/cmd", { delivery: "reliable", priority: Priority.REAL_TIME })
        command.put("hello-from-browser")
        const repeater = client.publisher("test/frombrowser/repeat", { delivery: "latest", repeatMs: 100 })
        await repeater.ready()
        repeater.put("repeat")
        await sleep(1200)
        await client.pollStats()
        out.repeat = { sent: repeater.sent, dropped: repeater.dropped }
        repeater.close()
        command.close()

        // options: old names and bad values are rejected; phase-2 fields are carried to the bridge
        out.optionErrors = []
        for (const bad of [{ hz: [1, 10] }, { delivery: { queue: 1 } }, { queueSize: 0 }, { minQuality: 0.9, maxQuality: 0.1 }, { qualityToHzTradeoff: 2 }]) {
            try {
                client.subscribe("test/jpeg", bad, () => {})
                out.optionErrors.push(null)
            } catch (error) {
                out.optionErrors.push(error.message)
            }
        }
        const carried = client.subscribe("test/jpeg", { bandwidthPriority: 2, maxHz: 20, dangerousMinHz: 1, minQuality: 0.3, maxQuality: 0.9, qualityToHzTradeoff: 0.7 }, () => {})
        await carried.ready()
        await sleep(200)
        await client.pollStats()
        out.carriedOpts = carried.bridgeStats?.opts
        carried.close()
        out.clock = { offsetMs: client.clockOffsetMs, rttMs: client.rttMs, bridge: client.bridgeStats?.clock }

        /**
         * Subscribes to test/fast for a while; latency = arrival - publisher send time (same machine clock).
         * @param {object} options
         * @param {number} durationMs
         */
        async function measure(options, durationMs) {
            const samples = []
            const gaps = []
            let previousSeq = -1
            const subscription = client.subscribe("test/fast", options, (message) => {
                const view = new DataView(message.bytes.buffer, message.bytes.byteOffset, 12)
                samples.push({ at: performance.now(), latencyMs: Date.now() - view.getFloat64(0, true) })
                if (previousSeq >= 0 && message.seq !== previousSeq + 1 && gaps.length < 8) {
                    gaps.push([previousSeq, message.seq, Math.round(performance.now())])
                }
                previousSeq = message.seq
            })
            await subscription.ready()
            const started = performance.now()
            await sleep(durationMs)
            await client.pollStats()
            const bridgeStats = subscription.bridgeStats?.stats
            const dropped = subscription.dropped
            subscription.close()
            await sleep(300)
            const settled = samples.filter((sample) => sample.at - started > 1000)
            const sorted = settled.map((sample) => sample.latencyMs).sort((a, b) => a - b)
            const quantile = (q) => sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] : null
            const window = (fromMs, toMs) => {
                const values = samples.filter((sample) => sample.at - started >= fromMs && sample.at - started < toMs).map((sample) => sample.latencyMs).sort((a, b) => a - b)
                return values.length ? values[Math.floor(values.length / 2)] : null
            }
            return {
                received: samples.length,
                ratePerSec: samples.length / (durationMs / 1000),
                dropped,
                p50: quantile(0.5),
                p90: quantile(0.9),
                p99: quantile(0.99),
                medianEarly: window(1000, 3000),
                medianLate: window(durationMs - 1000, durationMs),
                slowest: settled.filter((sample) => sample.latencyMs > 50).slice(0, 10).map((sample) => [Math.round(sample.at - started), Math.round(sample.latencyMs)]),
                bridge: bridgeStats,
                gaps,
                channelId: subscription.channel?.id,
            }
        }

        out.latest = await measure({ delivery: "latest" }, 6000)
        out.maxAge = await measure({ queueSize: 50, maxAge: 10 }, 4000)
        out.hzCapped = await measure({ delivery: "latest", maxHz: 10 }, 3000)
        out.reliable = await measure({ delivery: "reliable" }, 4000)

        // setDeadman without a heartbeat is a usage error
        const noHeartbeat = client.publisher("test/frombrowser/noheartbeat", { delivery: "reliable" })
        try {
            noHeartbeat.setDeadman("x")
            out.noHeartbeatError = null
        } catch (error) {
            out.noHeartbeatError = error.message
        }
        client.close()

        // latencyLimit: this client's clock runs 30 s ahead; the offset estimate absorbs that,
        // and a put stamped 1 s in the past (a simulated delay) is dropped
        const skewed = await connect(bridgeUrl, { clock: () => performance.timeOrigin + performance.now() + 30000 })
        const limited = skewed.publisher("test/frombrowser/limited", { delivery: "reliable", latencyLimit: 200 })
        await limited.ready()
        limited.put("fresh-1")
        limited.put("stale", { timestamp: skewed.now() - 1000 })
        limited.put("fresh-2")
        await sleep(500)
        await skewed.pollStats()
        out.latency = { offsetMs: skewed.clockOffsetMs, rttMs: skewed.rttMs, stats: limited.bridgeStats?.stats }
        skewed.close()

        // deadman via heartbeat: 5 Hz, 10 misses = 2 s of silence
        const guarded = await connect(bridgeUrl, { heartbeatHz: 5, heartbeatMisses: 10 })
        const stop = guarded.publisher("test/frombrowser/stop", { delivery: "reliable" })
        const trips = []
        stop.onTripped((reason) => trips.push(reason))
        await stop.setDeadman("STOP-heartbeat")
        const cleared = guarded.publisher("test/frombrowser/cleared", { delivery: "reliable" })
        await cleared.setDeadman("STOP-cleared")
        await cleared.clearDeadman()
        stop.put("moving")
        await sleep(2500)
        stop.put("last-before-pause")
        await sleep(200)
        guarded.pauseHeartbeat()
        await sleep(4000)
        out.trip = { state: stop.state, trips, clearedState: cleared.state }
        try {
            stop.put("after-trip")
            out.putAfterTrip = null
        } catch (error) {
            out.putAfterTrip = error.message
        }
        // bypass the client-side check: the bridge must reject it too
        stop.channel.send(encodePut(new TextEncoder().encode("raw-after-trip"), guarded.now()))
        await sleep(300)
        guarded.resumeHeartbeat()
        await sleep(500)
        await guarded.pollStats()
        out.tripStats = stop.bridgeStats?.stats
        stop.close()

        const recreated = guarded.publisher("test/frombrowser/stop", { delivery: "reliable" })
        await recreated.ready()
        recreated.put("recreated-works")
        await recreated.setDeadman("STOP-close")
        await cleared.setDeadman("STOP-cleared-close")
        await cleared.clearDeadman()
        await sleep(300)
        out.recreatedState = recreated.state
        return out
    }, { args: [bridgeUrl] })
    console.log(JSON.stringify(results, null, 2))

    check(results.state === "connected", "client connects over WebRTC")
    check(results.cached === "cached-hello", "late subscriber gets the AdvancedPublisher's cached sample (history)")
    check(results.get.length === 1 && results.get[0].key === "test/queryable" && results.get[0].text === "pong", "get returns the queryable's reply")
    const commandLine = await peerOutput.waitFor((line) => line.startsWith("RECV test/frombrowser/cmd"), 3000).catch(() => null)
    check(commandLine === "RECV test/frombrowser/cmd hello-from-browser", "publisher channel put reaches a zenoh subscriber")
    // headless Chrome timers run late on a loaded machine, so compare against what the client actually sent
    const repeatCount = peerOutput.lines.filter((line) => line === "RECV test/frombrowser/repeat repeat").length
    check(results.repeat.sent >= 5 && repeatCount === results.repeat.sent, `repeatMs re-sends the last value and every repeat reaches zenoh (sent ${results.repeat.sent}, received ${repeatCount} in ~1.2s)`)

    const latest = results.latest
    check(latest.received > 100, `latest: messages arrive (${latest.received})`)
    check(latest.bridge?.droppedQueue > 0, `latest: bridge drops under backpressure (droppedQueue=${latest.bridge?.droppedQueue})`)
    // p90/p99 are reported, not asserted: a stalled headless-Chrome main thread delays every queued message at once
    console.log(`latest: p50=${latest.p50?.toFixed(1)} p90=${latest.p90?.toFixed(1)} p99=${latest.p99?.toFixed(1)} ms, slowest=${JSON.stringify(latest.slowest)}, probes=${latest.bridge?.probes}, streams=${["latest", "maxAge", "hzCapped", "reliable"].map((name) => results[name].channelId).join(",")}`)
    check(latest.p50 !== null && latest.p50 < 50, `latest: median latency stays low (${latest.p50?.toFixed(1)} ms)`)
    check(latest.bridge?.maxReceiveLagMs < 100, `latest: upstream zenoh lag into the bridge stays low (${latest.bridge?.maxReceiveLagMs?.toFixed(1)} ms)`)
    check(latest.medianEarly !== null && latest.medianLate !== null && latest.medianLate < latest.medianEarly + 50, `latest: latency does not grow (${latest.medianEarly?.toFixed(1)} -> ${latest.medianLate?.toFixed(1)} ms)`)
    check(latest.bridge?.queued <= 1, `latest: bridge queue stays at <= 1 (${latest.bridge?.queued})`)

    const maxAge = results.maxAge
    check(maxAge.received > 50, `maxAge: messages arrive (${maxAge.received})`)
    check(maxAge.bridge?.droppedAge > 0, `maxAge: bridge drops samples older than 10 ms (droppedAge=${maxAge.bridge?.droppedAge})`)
    check(maxAge.p50 !== null && maxAge.p50 < 50, `maxAge: median latency stays low (${maxAge.p50?.toFixed(1)} ms, p90 ${maxAge.p90?.toFixed(1)})`)

    const hzCapped = results.hzCapped
    check(hzCapped.ratePerSec <= 11 && hzCapped.ratePerSec >= 7, `maxHz 10: delivered rate capped (${hzCapped.ratePerSec.toFixed(1)}/s)`)

    const reliable = results.reliable
    console.log(`reliable (contrast, not asserted beyond delivery): p50=${reliable.p50?.toFixed(1)}ms, early=${reliable.medianEarly?.toFixed(1)} late=${reliable.medianLate?.toFixed(1)} queued=${reliable.bridge?.queued} dropped=${reliable.dropped} gaps=${JSON.stringify(reliable.gaps)} streams=${["latest", "maxAge", "hzCapped", "reliable"].map((name) => results[name].channelId).join(",")}`)
    check(reliable.received > 50 && reliable.dropped === 0, `reliable: arrives with no gaps (${reliable.received}, dropped ${reliable.dropped})`)
    check(reliable.medianLate > 10 * Math.max(1, latest.medianLate), `contrast: reliable queues and grows latency where latest does not (${reliable.medianLate?.toFixed(0)} vs ${latest.medianLate?.toFixed(1)} ms)`)

    check(results.optionErrors.every((message) => typeof message === "string"), `bad/old subscribe options throw (${results.optionErrors.map((m) => m?.split(":")[1]?.trim().slice(0, 40)).join(" | ")})`)
    const carried = results.carriedOpts ?? {}
    check(carried.bandwidthPriority === 2 && carried.dangerousMinHz === 1 && carried.minQuality === 0.3 && carried.maxQuality === 0.9 && carried.qualityToHzTradeoff === 0.7 && carried.maxHz === 20,
        `phase-2 options reach the bridge and show in stats (${JSON.stringify(carried)})`)
    check(Math.abs(results.clock.offsetMs) < 20 && results.clock.rttMs >= 0 && results.clock.bridge?.offsetMs !== null, `clock sync, same machine: offset ${results.clock.offsetMs?.toFixed(2)} ms, rtt ${results.clock.rttMs?.toFixed(2)} ms`)

    const recvCount = (/** @type {string} */ key, /** @type {string} */ payload) => peerOutput.lines.filter((line) => line === `RECV test/frombrowser/${key} ${payload}`).length
    const latency = results.latency
    check(Math.abs(latency.offsetMs + 30000) < 50, `clock sync absorbs a +30 s browser clock skew (offset ${latency.offsetMs?.toFixed(1)} ms)`)
    check(recvCount("limited", "fresh-1") === 1 && recvCount("limited", "fresh-2") === 1, "latencyLimit: fresh puts from a skewed clock pass")
    check(recvCount("limited", "stale") === 0 && latency.stats?.droppedStale === 1, `latencyLimit: a put 1 s old is dropped and counted (droppedStale=${latency.stats?.droppedStale})`)

    check(results.noHeartbeatError?.includes("needs a heartbeat"), `setDeadman without a heartbeat throws (${results.noHeartbeatError})`)
    const stopLines = (/** @type {string} */ payload) => peerOutput.lines.findIndex((line) => line === `RECV test/frombrowser/stop ${payload}`)
    check(recvCount("stop", "STOP-heartbeat") === 1, `deadman fires exactly once when heartbeats stop (${recvCount("stop", "STOP-heartbeat")})`)
    check(stopLines("STOP-heartbeat") > stopLines("last-before-pause") && stopLines("last-before-pause") >= 0, "deadman did not fire while heartbeats were flowing")
    check(results.trip.state === "tripped" && results.trip.trips.join() === "heartbeat", `publisher reports tripped + callback (${results.trip.state}, ${results.trip.trips})`)
    check(results.putAfterTrip?.includes("tripped") && recvCount("stop", "after-trip") === 0, "put after a trip throws in the client")
    check(recvCount("stop", "raw-after-trip") === 0 && results.tripStats?.rejectedTripped === 1, `bridge rejects puts on a tripped stream (rejectedTripped=${results.tripStats?.rejectedTripped})`)
    check(recvCount("cleared", "STOP-cleared") === 0 && results.trip.clearedState === "open", "a cleared deadman does not fire on heartbeat loss")
    check(recvCount("stop", "recreated-works") === 1 && results.recreatedState === "open", "recreated publisher works again")

    $.logStep("closing the page with an armed deadman")
    await page.close()
    const closeLine = await peerOutput.waitFor((line) => line === "RECV test/frombrowser/stop STOP-close", 20000).catch(() => null)
    await $.sleep(1000)
    check(closeLine !== null && recvCount("stop", "STOP-close") === 1, `deadman fires once on page close (${recvCount("stop", "STOP-close")})`)
    check(recvCount("cleared", "STOP-cleared-close") === 0, "a cleared deadman does not fire on page close")

    $.logStep("SIGTERM to the bridge with an armed deadman")
    const lastPage = await browser.newPage(`${bridgeUrl}/test/blank.html`)
    await lastPage.evaluate(async (bridgeUrl) => {
        const { connect } = await import("/client/zenoh_web.js")
        const client = await connect(bridgeUrl, { heartbeatHz: 5, heartbeatMisses: 10 })
        await client.publisher("test/frombrowser/stop", { delivery: "reliable" }).setDeadman("STOP-sigterm")
    }, { args: [bridgeUrl] })
    bridge.kill("SIGTERM")
    const termLine = await peerOutput.waitFor((line) => line === "RECV test/frombrowser/stop STOP-sigterm", 10000).catch(() => null)
    await bridgeOutput.waitFor((line) => line.includes("shut down"), 10000).catch(() => null)
    check(termLine !== null && recvCount("stop", "STOP-sigterm") === 1, `deadman fires once on bridge SIGTERM (${recvCount("stop", "STOP-sigterm")})`)
} catch (error) {
    failures.push(String(error))
    console.error(error)
} finally {
    await cleanup()
}

console.log(`\n${failures.length === 0 ? "ALL PASSED" : `${failures.length} FAILED:\n  ${failures.join("\n  ")}`}`)
console.log(`artifacts: ${scratch}`)
Deno.exit(failures.length === 0 ? 0 : 1)
