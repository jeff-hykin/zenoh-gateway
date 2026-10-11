#!/usr/bin/env -S deno run --allow-all --unstable-net
// A Controller-like page on a phone over a slow link: headless Chrome, its UDP through a userspace shaper (1 MB/s
// gateway->browser with a 2 MB drop-tail queue, 30 ms each way), the gateway's examples/load_server publishing ~6.4 MB/s
// of point clouds and maps plus a small pose stream, and a page that spends `bytes / PAGE_BYTES_PER_SEC` of main-thread
// time on each bulk message (a phone decoding clouds). Prints the link latency the Controller's drive hold reads (heartbeat RTT, or the silence since the last one)
// and message ages per 5 s window, and fails if either keeps growing.
// LINK=wifi adds Wi-Fi + Tailscale-like jitter: every 200 ms the path's delay jumps to a new level within 0-210 ms each way
// (RTT 60-480 ms; packets stay in order). CLOUD_BYTES / MAP_BYTES set the load (see examples/load_server.rs).
// Usage: deno run --allow-all --unstable-net client/test/backpressure.ts [seconds]

import { launch } from "jsr:@astral/astral@0.5.6"

const seconds = Number(Deno.args[0] ?? 30)
const pageBytesPerSec = Number(Deno.env.get("PAGE_BYTES_PER_SEC") ?? 800_000)
const wifi = Deno.env.get("LINK") === "wifi"
const link = { bytesPerSec: 1_000_000, queueLimitBytes: 2_000_000, oneWayDelayMs: 30, jitterMs: wifi ? 210 : 0, jitterPeriodMs: 200 }

const root = new URL("../..", import.meta.url).pathname
const scratch = await Deno.makeTempDir({ prefix: "zenoh-gateway-backpressure-" })
await Deno.mkdir(`${scratch}/client`)
await Deno.writeTextFile(`${scratch}/blank.html`, `<!doctype html><html><head><meta charset="utf-8"></head><body></body></html>`)
const bundled = await new Deno.Command(Deno.execPath(), { args: ["bundle", "--quiet", "--platform", "browser", "-o", `${scratch}/client/zenoh_gateway.js`, `${root}client/zenoh_gateway.ts`] }).output()
if (bundled.code !== 0) {
    throw new Error(`deno bundle failed: ${new TextDecoder().decode(bundled.stderr)}`)
}

const build = await new Deno.Command("cargo", { args: ["build", "--release", "--example", "load_server"], cwd: `${root}gateway`, stderr: "inherit" }).output()
if (build.code !== 0) {
    throw new Error("cargo build failed")
}
const targetDir = Deno.env.get("CARGO_TARGET_DIR") ?? `${root}gateway/target`
const server = new Deno.Command(`${targetDir}/release/examples/load_server`, { args: [scratch], stdout: "piped", stderr: "inherit" }).spawn()
const serverUrl = await (async () => {
    for await (const chunk of server.stdout.pipeThrough(new TextDecoderStream())) {
        const match = chunk.match(/listening on (\S+)/)
        if (match) {
            return match[1]
        }
    }
    throw new Error("load_server exited")
})()

/** A drop-tail FIFO drained at the link rate, then delayed: real queueing, as a bottleneck router does. */
function shapedDirection(bytesPerSec: number, queueLimitBytes: number, delayMs: number, deliver: (data: Uint8Array) => void) {
    const queue: { data: Uint8Array; arrived: number }[] = []
    let queuedBytes = 0
    let freeAt = 0
    let draining = false
    // the delay level of the current jitter period; a packet never overtakes the one before it
    let jitter = 0
    let jitterDrawnAt = -Infinity
    let lastDeliverAt = 0
    const delayNow = (now: number) => {
        if (now - jitterDrawnAt >= link.jitterPeriodMs) {
            jitterDrawnAt = now
            jitter = Math.random() * link.jitterMs
        }
        return delayMs + jitter
    }
    const drain = () => {
        const now = performance.now()
        while (queue.length > 0) {
            const { data, arrived } = queue[0]
            const leaves = Math.max(freeAt, arrived) + (data.length / bytesPerSec) * 1000
            if (leaves > now) {
                setTimeout(drain, leaves - now)
                return
            }
            queue.shift()
            queuedBytes -= data.length
            freeAt = leaves
            lastDeliverAt = Math.max(lastDeliverAt, leaves + delayNow(leaves))
            setTimeout(() => deliver(data), Math.max(0, lastDeliverAt - performance.now()))
        }
        draining = false
    }
    return (data: Uint8Array) => {
        if (queuedBytes + data.length > queueLimitBytes) {
            return
        }
        queue.push({ data, arrived: performance.now() })
        queuedBytes += data.length
        if (!draining) {
            draining = true
            drain()
        }
    }
}

/** Chrome <-> [chromeSide | serverSide] <-> the server's ICE port. */
function startShaper(targetPort: number) {
    const chromeSide = Deno.listenDatagram({ transport: "udp", hostname: "127.0.0.1", port: 0 })
    const serverSide = Deno.listenDatagram({ transport: "udp", hostname: "127.0.0.1", port: 0 })
    let chromeAddress: Deno.NetAddr | null = null
    const down = shapedDirection(link.bytesPerSec, link.queueLimitBytes, link.oneWayDelayMs, (data) => {
        if (chromeAddress) {
            chromeSide.send(data, chromeAddress).catch(() => {})
        }
    })
    const up = shapedDirection(Infinity, Infinity, link.oneWayDelayMs, (data) => {
        serverSide.send(data, { transport: "udp", hostname: "127.0.0.1", port: targetPort }).catch(() => {})
    })
    ;(async () => {
        for await (const [data, address] of chromeSide) {
            chromeAddress = address as Deno.NetAddr
            up(data)
        }
    })().catch(() => {})
    ;(async () => {
        for await (const [data] of serverSide) {
            down(data)
        }
    })().catch(() => {})
    return (chromeSide.addr as Deno.NetAddr).port
}

// serves the page and signalling, routing ICE through the shaper (the offer loses its candidates, the answer's
// become the shaper's port)
const proxy = Deno.serve({ hostname: "127.0.0.1", port: 0, onListen() {} }, async (request) => {
    const url = new URL(request.url)
    if (url.pathname !== "/offer") {
        return fetch(`${serverUrl}${url.pathname}${url.search}`)
    }
    const offer = await request.json()
    offer.sdp = offer.sdp.split("\r\n").filter((line: string) => !line.startsWith("a=candidate")).join("\r\n")
    const answer = await (await fetch(`${serverUrl}/offer`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(offer) })).json()
    const lines: string[] = answer.sdp.split("\r\n")
    const loopback = lines.find((line) => line.startsWith("a=candidate") && line.includes(" 127.0.0.1 "))
    const port = startShaper(Number(loopback?.split(" ")[5]))
    let replaced = false
    answer.sdp = lines.flatMap((line) => {
        if (!line.startsWith("a=candidate")) {
            return [line]
        }
        if (replaced) {
            return []
        }
        replaced = true
        return [`a=candidate:1 1 udp 2130706431 127.0.0.1 ${port} typ host`]
    }).join("\r\n")
    return Response.json(answer)
})
const proxyUrl = `http://127.0.0.1:${proxy.addr.port}`

const browser = await launch({ headless: true, args: ["--no-sandbox"] })
let failed = false
try {
    const page = await browser.newPage(`${proxyUrl}/blank.html`)
    const windows = await page.evaluate(async (url: string, seconds: number, pageBytesPerSec: number) => {
        const { connect } = await import("/client/zenoh_gateway.js")
        const client = await connect(url, { heartbeatHz: 5, heartbeatMisses: 50 })
        const started = performance.now()
        const windowOf = () => Math.floor((performance.now() - started) / 5000)
        const windows: { rtt: number[]; bulkAge: number[]; poseAge: number[]; tfAge: number[]; bulkBytes: number; dropped: number }[] = []
        const windowNow = () => (windows[windowOf()] ??= { rtt: [], bulkAge: [], poseAge: [], tfAge: [], bulkBytes: 0, dropped: 0 })
        const age = (timestamp: number) => client.now() - timestamp
        for (const key of ["robot/cloud0", "robot/cloud1", "robot/cloud2", "robot/map"]) {
            client.subscribe(key, { delivery: "latest" }, (message: { bytes: Uint8Array; timestamp: number }) => {
                const window = windowNow()
                window.bulkAge.push(age(message.timestamp))
                window.bulkBytes += message.bytes.length
                // a phone decoding the cloud: main-thread time in proportion to its size
                const until = performance.now() + (message.bytes.length / pageBytesPerSec) * 1000
                while (performance.now() < until) {
                    // busy
                }
            })
        }
        client.subscribe("robot/pose", { delivery: "latest" }, (message: { timestamp: number }) => windowNow().poseAge.push(age(message.timestamp)))
        // the Controller's tf feed is reliable
        let tfSeq = -1
        let tfOutOfOrder = 0
        client.subscribe("robot/tf", { delivery: "reliable" }, (message: { timestamp: number; seq: number }) => {
            tfOutOfOrder += message.seq <= tfSeq ? 1 : 0
            tfSeq = message.seq
            windowNow().tfAge.push(age(message.timestamp))
        })
        // the Controller's link latency: the heartbeat's round trip, or how long since the last one came back
        let lastRtt: number | null = null
        let lastRttAt = performance.now()
        const sampler = setInterval(() => {
            const now = performance.now()
            if (client.rttMs !== lastRtt) {
                lastRtt = client.rttMs
                lastRttAt = now
            }
            windowNow().rtt.push(Math.max(lastRtt ?? 0, now - lastRttAt - 200))
        }, 200)
        await new Promise((resolve) => setTimeout(resolve, seconds * 1000))
        clearInterval(sampler)
        const congestion = client.congestion
        const bandwidth = client.gatewayStats?.bandwidth ?? null
        const channels = Object.fromEntries(Object.entries(client.stats as Record<string, { gateway: { stats: unknown; allocation: unknown } | null }>).map(([key, stats]) => [key, { stats: stats.gateway?.stats, allocation: stats.gateway?.allocation }]))
        client.close()
        const percentile = (values: number[], q: number) => {
            const sorted = [...values].sort((a, b) => a - b)
            // no sample in a window: the page was too busy to run the sampler's timer at all
            return sorted.length ? Math.round(sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]) : null
        }
        // the whole run after the first window (startup)
        const steady = windows.slice(1, Math.floor(seconds / 5))
        const all = (pick: (window: typeof windows[number]) => number[]) => steady.flatMap(pick)
        return {
            congestion,
            bandwidth,
            channels,
            tfOutOfOrder,
            steady: {
                rttP50: percentile(all((window) => window.rtt), 0.5),
                rttP95: percentile(all((window) => window.rtt), 0.95),
                bulkAgeP50: percentile(all((window) => window.bulkAge), 0.5),
                bulkAgeP95: percentile(all((window) => window.bulkAge), 0.95),
                tfAgeP95: percentile(all((window) => window.tfAge), 0.95),
                bulkKBps: Math.round(steady.reduce((sum, window) => sum + window.bulkBytes, 0) / (steady.length * 5000)),
            },
            windows: windows.slice(0, Math.floor(seconds / 5)).map((window) => ({
                rttP50: percentile(window.rtt, 0.5),
                rttP95: percentile(window.rtt, 0.95),
                bulkAgeP50: percentile(window.bulkAge, 0.5),
                bulkAgeP95: percentile(window.bulkAge, 0.95),
                poseAgeP95: percentile(window.poseAge, 0.95),
                tfAgeP95: percentile(window.tfAge, 0.95),
                bulkKBps: Math.round(window.bulkBytes / 5000),
            })),
        }
    }, { args: [proxyUrl, seconds, pageBytesPerSec] })
    console.log(`link ${link.bytesPerSec / 1e6} MB/s${wifi ? " with wifi jitter" : ""}, page ${pageBytesPerSec / 1e3} KB/s of bulk:`)
    for (const [index, window] of windows.windows.entries()) {
        console.log(`  ${index * 5}-${index * 5 + 5} s  link latency p50 ${window.rttP50} p95 ${window.rttP95} ms | bulk age p50 ${window.bulkAgeP50} p95 ${window.bulkAgeP95} ms | pose age p95 ${window.poseAgeP95} ms | tf age p95 ${window.tfAgeP95} ms | bulk ${window.bulkKBps} KB/s`)
    }
    console.log(`  steady (5 s on): ${JSON.stringify(windows.steady)}, tf out of order ${windows.tfOutOfOrder}`)
    if (windows.congestion) {
        console.log(`  congestion: ${JSON.stringify(windows.congestion)}`)
    }
    if (Deno.env.get("VERBOSE")) {
        console.log(`  bandwidth: ${JSON.stringify(windows.bandwidth)}`)
        console.log(`  channels: ${JSON.stringify(windows.channels)}`)
    }
    const last = windows.windows.at(-1)!
    for (const [ok, what] of [[(last.rttP95 ?? Infinity) < 1000, "link latency p95 < 1 s (the Controller holds driving above it)"], [(last.bulkAgeP50 ?? Infinity) < 1000, "bulk age p50 < 1 s"], [(last.poseAgeP95 ?? Infinity) < 1000, "pose age p95 < 1 s"], [(last.tfAgeP95 ?? Infinity) < 2500, "tf (reliable) age p95 < 2.5 s"], [windows.tfOutOfOrder === 0, "tf in order"]] as const) {
        console.log(`${ok ? "PASS" : "FAIL"} ${what}`)
        failed ||= !ok
    }
} finally {
    await browser.close()
    server.kill("SIGTERM")
    await proxy.shutdown()
    await Deno.remove(scratch, { recursive: true })
}
Deno.exit(failed ? 1 : 0)
