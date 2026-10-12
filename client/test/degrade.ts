#!/usr/bin/env -S deno run --allow-all --unstable-net
// Graceful degradation on bad links, pass/fail. A Controller-like page in headless Chrome, its UDP through a userspace
// shaper (rate limit both ways with a drop-tail queue, delay, Wi-Fi-like jitter, loss, a rate schedule), against the
// gateway's examples/degrade_server: two video cameras at low bandwidthPriority, two thinnable point clouds at high
// bandwidthPriority, a raw map at priority 0, pose, reliable tf, a strict `cmd` stream, and the page's own cmd_vel
// (REAL_TIME publisher at 20 Hz, echoed back by the server on a strict subscription). Every link must keep:
// - cmd_vel: >= 90% of the commands make the round trip, p95 within the link's own round trip + 300 ms (+ 500 ms on the
//   250 kbit/s floor, where strict traffic is a third of the link: at 100 kbit/s it alone fills it);
// - the strict `cmd` stream and the heartbeat RTT likewise short;
// - flexbox order: when squeezed, the cameras (0.1) give up a larger share than the clouds (10), the map (0) the most;
// - the link estimate within 2.5x of what the link carries (it once read 12 MB/s on a 1 MB/s link and never squeezed);
// - after a sudden drop, latency back in bounds within 10 s; after recovery, the clouds back up within 20 s.
// Usage: deno run --allow-all --unstable-net client/test/degrade.ts [cell name ...]   (default: every cell)

import { launch } from "jsr:@astral/astral@0.5.6"

// slackMs: what the strict streams may take beyond the link's own round trip (300 ms; the floor cell's strict traffic is a
// third of its link, and its packets queue behind each other)
type Cell = { name: string; seconds: number; schedule: [number, number][]; delayMs: number; jitterMs: number; loss: number; slackMs?: number }
const cells: Cell[] = [
    { name: "20mbit", seconds: 30, schedule: [[0, 2_500_000]], delayMs: 30, jitterMs: 0, loss: 0 },
    { name: "8mbit", seconds: 30, schedule: [[0, 1_000_000]], delayMs: 30, jitterMs: 0, loss: 0 },
    { name: "2mbit", seconds: 30, schedule: [[0, 250_000]], delayMs: 30, jitterMs: 0, loss: 0 },
    { name: "500kbit", seconds: 30, schedule: [[0, 62_500]], delayMs: 30, jitterMs: 0, loss: 0 },
    // the floor: strict streams are never squeezed, and here cmd + the cmd_vel echo (40 small messages/s, ~200 B each on
    // the wire) are a third of the link; at 100 kbit/s they alone fill it
    { name: "250kbit", seconds: 30, schedule: [[0, 31_250]], delayMs: 30, jitterMs: 0, loss: 0, slackMs: 500 },
    { name: "rtt400", seconds: 30, schedule: [[0, 625_000]], delayMs: 200, jitterMs: 0, loss: 0 },
    { name: "wifi_loss2", seconds: 30, schedule: [[0, 625_000]], delayMs: 30, jitterMs: 210, loss: 0.02 },
    // at 5% loss each way the slowest 5% of messages are the ones resent: p95 lands on a retransmission
    { name: "loss5", seconds: 30, schedule: [[0, 625_000]], delayMs: 30, jitterMs: 0, loss: 0.05, slackMs: 500 },
    { name: "step", seconds: 90, schedule: [[0, 2_500_000], [30, 125_000], [60, 2_500_000]], delayMs: 30, jitterMs: 0, loss: 0 },
]
const chosen = Deno.args.length ? cells.filter((cell) => Deno.args.includes(cell.name)) : cells
const queueSeconds = 1
// DIAGNOSE=1: read the gateway's bandwidth stats every window (they otherwise come every few seconds on slow links)
const diagnose = Deno.env.get("DIAGNOSE") === "1"
const windowMs = 2000

const root = new URL("../..", import.meta.url).pathname
const scratch = await Deno.makeTempDir({ prefix: "zenoh-gateway-degrade-" })
await Deno.mkdir(`${scratch}/client`)
await Deno.writeTextFile(`${scratch}/blank.html`, `<!doctype html><html><head><meta charset="utf-8"></head><body></body></html>`)
const bundled = await new Deno.Command(Deno.execPath(), { args: ["bundle", "--quiet", "--platform", "browser", "-o", `${scratch}/client/zenoh_gateway.js`, `${root}client/zenoh_gateway.ts`] }).output()
if (bundled.code !== 0) {
    throw new Error(`deno bundle failed: ${new TextDecoder().decode(bundled.stderr)}`)
}
const build = await new Deno.Command("cargo", { args: ["build", "--release", "--example", "degrade_server"], cwd: `${root}gateway`, stderr: "inherit" }).output()
if (build.code !== 0) {
    throw new Error("cargo build failed")
}
const targetDir = Deno.env.get("CARGO_TARGET_DIR") ?? `${root}gateway/target`

/** One direction of the link: rate-limited (by the cell's schedule) drop-tail queue, then delay + jitter, with loss. */
function shapedDirection(cell: Cell, linkStart: () => number, deliver: (data: Uint8Array) => void, offered: { bytes: number; packets: number }) {
    const rateNow = () => {
        const t = (performance.now() - linkStart()) / 1000
        return cell.schedule.filter(([at]) => t >= at).at(-1)![1]
    }
    const queue: { data: Uint8Array; arrived: number }[] = []
    let queuedBytes = 0
    let freeAt = 0
    let draining = false
    let jitter = 0
    let jitterDrawnAt = -Infinity
    let lastDeliverAt = 0
    const delayNow = (now: number) => {
        if (now - jitterDrawnAt >= 200) {
            jitterDrawnAt = now
            jitter = Math.random() * cell.jitterMs
        }
        return cell.delayMs + jitter
    }
    const drain = () => {
        const now = performance.now()
        while (queue.length > 0) {
            const { data, arrived } = queue[0]
            const leaves = Math.max(freeAt, arrived) + (data.length / rateNow()) * 1000
            if (leaves > now) {
                setTimeout(drain, leaves - now)
                return
            }
            queue.shift()
            queuedBytes -= data.length
            freeAt = leaves
            // packets stay in order: a jitter drop never lets one overtake the one before it
            lastDeliverAt = Math.max(lastDeliverAt, leaves + delayNow(leaves))
            setTimeout(() => deliver(data), Math.max(0, lastDeliverAt - performance.now()))
        }
        draining = false
    }
    return (data: Uint8Array) => {
        offered.bytes += data.length
        offered.packets++
        if (Math.random() < cell.loss || queuedBytes + data.length > Math.max(20_000, rateNow() * queueSeconds)) {
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

async function runCell(cell: Cell) {
    const server = new Deno.Command(`${targetDir}/release/examples/degrade_server`, { args: [scratch], stdout: "piped", stderr: "inherit" }).spawn()
    const serverUrl = await (async () => {
        for await (const chunk of server.stdout.pipeThrough(new TextDecoderStream())) {
            const match = chunk.match(/listening on (\S+)/)
            if (match) {
                return match[1]
            }
        }
        throw new Error("degrade_server exited")
    })()
    let linkStartedAt = 0
    const sockets: Deno.DatagramConn[] = []
    // what each side tried to send, for the summary: a saturated uplink delays the downlink too (its SCTP acks wait)
    const offeredDown = { bytes: 0, packets: 0 }
    const offeredUp = { bytes: 0, packets: 0 }
    const startShaper = (targetPort: number) => {
        const chromeSide = Deno.listenDatagram({ transport: "udp", hostname: "127.0.0.1", port: 0 })
        const serverSide = Deno.listenDatagram({ transport: "udp", hostname: "127.0.0.1", port: 0 })
        sockets.push(chromeSide, serverSide)
        let chromeAddress: Deno.NetAddr | null = null
        const down = shapedDirection(cell, () => linkStartedAt, (data) => {
            if (chromeAddress) {
                chromeSide.send(data, chromeAddress).catch(() => {})
            }
        }, offeredDown)
        const up = shapedDirection(cell, () => linkStartedAt, (data) => {
            serverSide.send(data, { transport: "udp", hostname: "127.0.0.1", port: targetPort }).catch(() => {})
        }, offeredUp)
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
    // a signaling proxy rewrites the SDP so ICE can only use the shaper
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
        linkStartedAt = performance.now()
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
    const browser = await launch({ headless: true, args: ["--no-sandbox", "--autoplay-policy=no-user-gesture-required"] })
    try {
        const page = await browser.newPage(`http://127.0.0.1:${proxy.addr.port}/blank.html`)
        // astral retries evaluate 5x on a 10 s deadline: long runs need more
        ;(page as any).timeout = (cell.seconds + 60) * 1000
        const windows = await page.evaluate(async (url: string, seconds: number, windowMs: number, diagnose: boolean) => {
            const { connect, Priority } = await import("/client/zenoh_gateway.js")
            // at 5% loss the first connect can time out opening its channels; a page retries as dim-app does
            const client = await connect(url, { heartbeatHz: 5, heartbeatMisses: 50 }).catch(() => connect(url, { heartbeatHz: 5, heartbeatMisses: 50 }))
            const started = performance.now()
            type Window = { rtt: number[]; echo: number[]; echoSent: number; ages: Record<string, number[]>; bytes: Record<string, number>; count: Record<string, number>; alloc: Record<string, any>; bw: any; statsBytes?: number }
            const windows: Window[] = []
            const current = () => (windows[Math.floor((performance.now() - started) / windowMs)] ??= { rtt: [], echo: [], echoSent: 0, ages: {}, bytes: {}, count: {}, alloc: {}, bw: null })
            const record = (key: string, message: any) => {
                const window = current()
                ;(window.ages[key] ??= []).push(client.now() - message.timestamp)
                window.count[key] = (window.count[key] ?? 0) + 1
                window.bytes[key] = (window.bytes[key] ?? 0) + (message.video ? message.video.encodedBytes : message.bytes.length)
            }
            for (const key of ["robot/cam0", "robot/cam1"]) {
                let attached = false
                client.subscribe(key, { delivery: "latest", encoding: "test-cam", bandwidthPriority: 0.1 }, (message: any) => {
                    record(key, message)
                    if (!attached && message.mediaStream) {
                        attached = true
                        const page = (globalThis as any).document
                        const video = page.createElement("video")
                        video.muted = true
                        video.autoplay = true
                        video.srcObject = message.mediaStream
                        page.body.appendChild(video)
                        video.play().catch(() => {})
                    }
                })
            }
            for (const key of ["robot/cloud0", "robot/cloud1"]) {
                client.subscribe(key, { delivery: "latest", encoding: "test-cloud", minQuality: 0.0625, bandwidthPriority: 10 }, (message: any) => record(key, message))
            }
            client.subscribe("robot/map", { delivery: "latest", bandwidthPriority: 0 }, (message: any) => record("robot/map", message))
            client.subscribe("robot/pose", { delivery: "latest" }, (message: any) => record("robot/pose", message))
            client.subscribe("robot/tf", { delivery: "reliable" }, (message: any) => record("robot/tf", message))
            client.subscribe("robot/cmd", { delivery: "latest", priority: 1 }, (message: any) => record("robot/cmd", message))
            // the page's commands: sent time in the payload, echoed back by the server
            client.subscribe("robot/cmd_vel_echo", { delivery: "latest", priority: 1 }, (message: any) => {
                const sentAt = new DataView(message.bytes.buffer, message.bytes.byteOffset).getFloat64(0)
                current().echo.push(performance.now() - sentAt)
            })
            const cmdVel = client.publisher("robot/cmd_vel", { delivery: "latest", priority: Priority.REAL_TIME })
            const commander = setInterval(() => {
                const payload = new Uint8Array(48)
                new DataView(payload.buffer).setFloat64(0, performance.now())
                cmdVel.put(payload)
                current().echoSent++
            }, 50)
            // what the Controller's drive hold reads: the heartbeat RTT, or how long it has been silent
            let lastRtt: number | null = null
            let lastRttAt = performance.now()
            const sampler = setInterval(() => {
                const now = performance.now()
                if (client.rttMs !== lastRtt) {
                    lastRtt = client.rttMs
                    lastRttAt = now
                }
                current().rtt.push(Math.max(lastRtt ?? 0, now - lastRttAt - 200))
            }, 200)
            const statsTimer = setInterval(async () => {
                const window = current()
                window.bw = client.gatewayStats?.bandwidth ?? null
                if (diagnose) {
                    // every window, past the client's own stats budget (costs the link ~12 KB a window)
                    window.bw = (await client._request({ op: "stats" }, 3000).catch(() => null))?.bandwidth ?? window.bw
                }
                window.statsBytes = JSON.stringify(client.gatewayStats ?? {}).length + JSON.stringify(client.stats ?? {}).length
                for (const [key, stats] of Object.entries(client.stats as Record<string, any>)) {
                    const allocation = stats.gateway?.allocation
                    if (allocation) {
                        window.alloc[key] = { fraction: allocation.demandBytesPerSec > 0 ? allocation.budgetBytesPerSec / allocation.demandBytesPerSec : 1, quality: allocation.quality, constrained: allocation.constrained }
                    }
                }
            }, windowMs)
            await new Promise((resolve) => setTimeout(resolve, seconds * 1000))
            for (const timer of [commander, sampler, statsTimer]) {
                clearInterval(timer)
            }
            client.close()
            return windows.slice(0, Math.floor((seconds * 1000) / windowMs))
        }, { args: [`http://127.0.0.1:${proxy.addr.port}`, cell.seconds, windowMs, diagnose] })
        const seconds = (performance.now() - linkStartedAt) / 1000
        const rate = (offered: { bytes: number; packets: number }) => `${Math.round(offered.bytes / seconds / 100) / 10} KB/s in ${Math.round(offered.packets / seconds)} packets/s`
        console.log(`  offered: down ${rate(offeredDown)}, up ${rate(offeredUp)}`)
        return windows
    } finally {
        await browser.close()
        server.kill("SIGTERM")
        await server.status
        await proxy.shutdown()
        for (const socket of sockets) {
            socket.close()
        }
    }
}

const percentile = (values: number[], q: number) => {
    if (!values.length) {
        return Infinity
    }
    const sorted = [...values].sort((a, b) => a - b)
    return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]
}
const median = (values: number[]) => percentile(values.filter(Number.isFinite), 0.5)
const round = (value: number) => Number.isFinite(value) ? Math.round(value) : value

/** Checks one stretch of a run against its link; returns the failures. */
function check(cell: Cell, windows: any[], from: number, to: number, label: string, bytesPerSec: number): string[] {
    const span = windows.filter((_, index) => index * windowMs / 1000 >= from && index * windowMs / 1000 < to)
    const failures: string[] = []
    const fail = (what: string) => failures.push(`${cell.name} ${label}: ${what}`)
    const baseRtt = 2 * (cell.delayMs + cell.jitterMs)
    const slack = cell.slackMs ?? 300
    const echo = span.flatMap((window) => window.echo)
    const echoSent = span.reduce((sum, window) => sum + window.echoSent, 0)
    const echoP95 = percentile(echo, 0.95)
    const rttP95 = percentile(span.flatMap((window) => window.rtt), 0.95)
    const cmdP95 = percentile(span.flatMap((window) => window.ages["robot/cmd"] ?? []), 0.95)
    const tfP95 = percentile(span.flatMap((window) => window.ages["robot/tf"] ?? []), 0.95)
    const hz = (key: string) => span.reduce((sum, window) => sum + (window.count[key] ?? 0), 0) / (span.length * windowMs / 1000)
    const fraction = (key: string) => median(span.map((window) => window.alloc[key]?.fraction))
    const estimate = median(span.map((window) => window.bw?.dataEstimateBytesPerSec))
    const constrained = span.filter((window) => window.bw?.constrained).length
    console.log(`  ${label.padEnd(10)} cmd_vel ${echo.length}/${echoSent} p95 ${round(echoP95)} ms | cmd p95 ${round(cmdP95)} | rtt p95 ${round(rttP95)} | tf p95 ${round(tfP95)} | cam0 ${hz("robot/cam0").toFixed(1)} Hz f${fraction("robot/cam0").toFixed(2)} | cloud0 ${hz("robot/cloud0").toFixed(1)} Hz f${fraction("robot/cloud0").toFixed(2)} | map ${hz("robot/map").toFixed(1)} Hz f${fraction("robot/map").toFixed(2)} | est ${round(estimate / 1000)} KB/s on ${bytesPerSec / 1000} | constrained ${constrained}/${span.length}`)
    if (echo.length < 0.9 * echoSent) {
        fail(`cmd_vel round trips ${echo.length}/${echoSent} (< 90%)`)
    }
    if (echoP95 > baseRtt + slack) {
        fail(`cmd_vel round trip p95 ${round(echoP95)} ms > ${baseRtt + slack}`)
    }
    if (cmdP95 > baseRtt / 2 + slack) {
        fail(`strict cmd age p95 ${round(cmdP95)} ms > ${baseRtt / 2 + slack}`)
    }
    if (rttP95 > baseRtt + slack + 200) {
        fail(`heartbeat RTT p95 ${round(rttP95)} ms > ${baseRtt + slack + 200}`)
    }
    // reliable: 2 s of backlog at most, plus on a lossy link a retransmission timeout (SCTP's is at least 1 s)
    const tfLimit = cell.loss > 0 || cell.slackMs ? 4000 : 2500
    if (tfP95 > tfLimit) {
        fail(`reliable tf age p95 ${round(tfP95)} ms > ${tfLimit}`)
    }
    if (estimate > 2.5 * bytesPerSec) {
        fail(`link estimate ${round(estimate / 1000)} KB/s > 2.5x the link's ${bytesPerSec / 1000}`)
    }
    if (constrained > 0 && fraction("robot/cam0") > fraction("robot/cloud0") + 0.05) {
        fail(`camera (bandwidthPriority 0.1) kept ${fraction("robot/cam0").toFixed(2)} of its demand, more than the clouds (10) ${fraction("robot/cloud0").toFixed(2)}`)
    }
    if (constrained > 0 && fraction("robot/map") > Math.min(fraction("robot/cam0"), fraction("robot/cloud0")) + 0.05) {
        fail(`map (bandwidthPriority 0) kept ${fraction("robot/map").toFixed(2)}, more than a higher-priority stream`)
    }
    // at 2% loss and a ~300 ms RTT, SCTP itself carries ~50 KB/s, and reliable tf outranks the clouds there
    const cloudFloor = cell.loss > 0 ? 0.3 : 2
    if (bytesPerSec >= 250_000 && hz("robot/cloud0") < cloudFloor) {
        fail(`clouds starved at ${hz("robot/cloud0").toFixed(1)} Hz on ${bytesPerSec / 1000} KB/s`)
    }
    return failures
}

const failures: string[] = []
try {
    for (const cell of chosen) {
        console.log(`${cell.name}: ${JSON.stringify(cell.schedule)} B/s, ${cell.delayMs} ms each way, jitter ${cell.jitterMs} ms, loss ${cell.loss * 100}%`)
        const windows = await runCell(cell)
        await Deno.writeTextFile(`${scratch}/${cell.name}.json`, JSON.stringify({ cell, windows }))
        if (cell.name === "step") {
            const [high, low] = [cell.schedule[0][1], cell.schedule[1][1]]
            failures.push(...check(cell, windows, 10, 30, "before", high))
            failures.push(...check(cell, windows, 40, 60, "dropped", low))
            failures.push(...check(cell, windows, 70, 90, "recovered", high))
            const cloudHz = (from: number, to: number) => windows.slice(from / 2, to / 2).reduce((sum: number, window: any) => sum + (window.count["robot/cloud0"] ?? 0), 0) / (to - from)
            if (cloudHz(70, 90) < 0.5 * cloudHz(10, 30)) {
                failures.push(`step: clouds ${cloudHz(70, 90).toFixed(1)} Hz 10-30 s after recovery, under half the ${cloudHz(10, 30).toFixed(1)} Hz before the drop`)
            }
        } else {
            failures.push(...check(cell, windows, Math.max(4, cell.seconds - 16), cell.seconds, "steady", cell.schedule[0][1]))
        }
    }
} finally {
    console.log(`(per-window data: ${scratch}/*.json)`)
}
if (failures.length) {
    console.log(`FAIL\n${failures.join("\n")}`)
    Deno.exit(1)
}
console.log("PASS")
Deno.exit(0)
