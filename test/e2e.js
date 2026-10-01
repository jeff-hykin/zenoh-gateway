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
        const { connect, Priority } = await import("/client/zenoh_web.js")
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
        const deadman = client.publisher("test/frombrowser/deadman", { delivery: "latest", repeatMs: 100 })
        await deadman.ready()
        deadman.put("deadman")
        await sleep(1200)
        await client.pollStats()
        out.deadman = { sent: deadman.sent, dropped: deadman.dropped }
        deadman.close()
        command.close()

        /**
         * Subscribes to test/fast for a while; latency = arrival - publisher send time (same machine clock).
         * @param {object} options
         * @param {number} durationMs
         */
        async function measure(options, durationMs) {
            const samples = []
            const subscription = client.subscribe("test/fast", options, (message) => {
                const view = new DataView(message.bytes.buffer, message.bytes.byteOffset, 12)
                samples.push({ at: performance.now(), latencyMs: Date.now() - view.getFloat64(0, true) })
            })
            await subscription.ready()
            const started = performance.now()
            await sleep(durationMs)
            await client.pollStats()
            const bridgeStats = subscription.bridgeStats
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
                medianEarly: window(1000, 2000),
                medianLate: window(durationMs - 1000, durationMs),
                slowest: settled.filter((sample) => sample.latencyMs > 50).slice(0, 10).map((sample) => [Math.round(sample.at - started), Math.round(sample.latencyMs)]),
                bridge: bridgeStats,
            }
        }

        out.latest = await measure({ delivery: "latest" }, 6000)
        out.maxAge = await measure({ delivery: { queue: 50, maxAgeMs: 10 } }, 4000)
        out.hzCapped = await measure({ delivery: "latest", hz: [1, 10] }, 3000)
        out.reliable = await measure({ delivery: "reliable" }, 4000)
        client.close()
        return out
    }, { args: [bridgeUrl] })
    console.log(JSON.stringify(results, null, 2))

    check(results.state === "connected", "client connects over WebRTC")
    check(results.cached === "cached-hello", "late subscriber gets the AdvancedPublisher's cached sample (history)")
    check(results.get.length === 1 && results.get[0].key === "test/queryable" && results.get[0].text === "pong", "get returns the queryable's reply")
    const commandLine = await peerOutput.waitFor((line) => line.startsWith("RECV test/frombrowser/cmd"), 3000).catch(() => null)
    check(commandLine === "RECV test/frombrowser/cmd hello-from-browser", "publisher channel put reaches a zenoh subscriber")
    // headless Chrome timers run late on a loaded machine, so compare against what the client actually sent
    const deadmanCount = peerOutput.lines.filter((line) => line === "RECV test/frombrowser/deadman deadman").length
    check(results.deadman.sent >= 5 && deadmanCount === results.deadman.sent, `repeatMs re-sends the last value and every repeat reaches zenoh (sent ${results.deadman.sent}, received ${deadmanCount} in ~1.2s)`)

    const latest = results.latest
    check(latest.received > 100, `latest: messages arrive (${latest.received})`)
    check(latest.bridge?.droppedQueue > 0, `latest: bridge drops under backpressure (droppedQueue=${latest.bridge?.droppedQueue})`)
    // p90/p99 are reported, not asserted: a stalled headless-Chrome main thread delays every queued message at once
    console.log(`latest: p50=${latest.p50?.toFixed(1)} p90=${latest.p90?.toFixed(1)} p99=${latest.p99?.toFixed(1)} ms, slowest=${JSON.stringify(latest.slowest)}`)
    check(latest.p50 !== null && latest.p50 < 50, `latest: median latency stays low (${latest.p50?.toFixed(1)} ms)`)
    check(latest.bridge?.maxReceiveLagMs < 100, `latest: upstream zenoh lag into the bridge stays low (${latest.bridge?.maxReceiveLagMs?.toFixed(1)} ms)`)
    check(latest.medianLate !== null && latest.medianLate < latest.medianEarly + 50, `latest: latency does not grow (${latest.medianEarly?.toFixed(1)} -> ${latest.medianLate?.toFixed(1)} ms)`)
    check(latest.bridge?.queued <= 1, `latest: bridge queue stays at <= 1 (${latest.bridge?.queued})`)

    const maxAge = results.maxAge
    check(maxAge.received > 50, `maxAgeMs: messages arrive (${maxAge.received})`)
    check(maxAge.bridge?.droppedAge > 0, `maxAgeMs: bridge drops samples older than 10 ms (droppedAge=${maxAge.bridge?.droppedAge})`)
    check(maxAge.p50 !== null && maxAge.p50 < 50, `maxAgeMs: median latency stays low (${maxAge.p50?.toFixed(1)} ms, p90 ${maxAge.p90?.toFixed(1)})`)

    const hzCapped = results.hzCapped
    check(hzCapped.ratePerSec <= 11 && hzCapped.ratePerSec >= 7, `hz [1,10]: delivered rate capped (${hzCapped.ratePerSec.toFixed(1)}/s)`)

    const reliable = results.reliable
    console.log(`reliable (contrast, not asserted beyond delivery): p50=${reliable.p50?.toFixed(1)}ms, early=${reliable.medianEarly?.toFixed(1)} late=${reliable.medianLate?.toFixed(1)} queued=${reliable.bridge?.queued} dropped=${reliable.dropped}`)
    check(reliable.received > 50 && reliable.dropped === 0, `reliable: arrives with no gaps (${reliable.received}, dropped ${reliable.dropped})`)
    check(reliable.medianLate > 10 * Math.max(1, latest.medianLate), `contrast: reliable queues and grows latency where latest does not (${reliable.medianLate?.toFixed(0)} vs ${latest.medianLate?.toFixed(1)} ms)`)
} catch (error) {
    failures.push(String(error))
    console.error(error)
} finally {
    await cleanup()
}

console.log(`\n${failures.length === 0 ? "ALL PASSED" : `${failures.length} FAILED:\n  ${failures.join("\n  ")}`}`)
console.log(`artifacts: ${scratch}`)
Deno.exit(failures.length === 0 ? 0 : 1)
