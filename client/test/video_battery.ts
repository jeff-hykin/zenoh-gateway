#!/usr/bin/env -S deno run --allow-all --unstable-net
// Video on bad links, pass/fail: one 720p30 camera (the gateway's examples/degrade_server, CAM_NOISE 4: a camera-like
// picture, not static noise) through the userspace shaper (shaped_link.ts), watched by a page in headless Chrome with
// `Subscription.watchVideoLatency` (source to screen, per shown frame). Each phase reports its last `measure` seconds:
// latency p50/p95 and its stages, frames shown per second, the longest freeze, picture size, quality, and how much of
// the link video used. Each phase must keep:
// - latency p95 within the link's one-way delay + jitter + 300 ms (+ 600 ms below 500 kbit/s, where a frame alone takes
//   a while to cross);
// - frames moving: >= 10 shown/s from 500 kbit/s up, >= 3 below, no freeze over 2 s;
// - the link used: video >= 40% of a link the camera could fill (its most is ~8 Mbit/s here), on clean links;
// - pictures never growing as the link slows (fewer bits cost compression first; a picture shrinks once frames pass
//   maxQp), never under minResolution ([160, 64]: 160x90 for 16:9).
// Usage: deno run --allow-all --unstable-net client/test/video_battery.ts [cell name ...]   (default: every cell; BROWSER=firefox;
// REPEAT=3)

/// <reference lib="dom" />
import { openPage, waitForOtherBrowserTests } from "./browser.ts"
import { shapedLink } from "./shaped_link.ts"
import { type Cell, cells } from "./battery_cells.ts"

const measure = 10
// REPEAT=n: each cell n times, judged on the run with the median p95 (the same link gave 289 ms one run, 8 s the next)
const repeat = Number(Deno.env.get("REPEAT") ?? 1)
// (bin/test runs and caches each default cell on its own)
if (Deno.args[0] === "--list-cells") {
    console.log(cells.map((cell) => cell.name).join("\n"))
    Deno.exit(0)
}
const chosen = Deno.args.length ? cells.filter((cell) => Deno.args.includes(cell.name)) : cells

await waitForOtherBrowserTests()
const root = new URL("../..", import.meta.url).pathname
const scratch = await Deno.makeTempDir({ prefix: "zenoh-gateway-video-battery-" })
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
// SERVER_LOG=path: the gateway's log (RUST_LOG, default warn) appended there
const serverLog = Deno.env.get("SERVER_LOG")

type Phase = { timeline?: string[]; subscription?: unknown; counts: Record<string, number>; bandwidth?: Record<string, unknown>; pacerP95Ms?: number; label: string; latency: any; shownPerSec: number; maxGapMs: number; size: string; quality: number; videoBytesPerSec: number }

async function runCell(cell: Cell): Promise<Phase[]> {
    const server = new Deno.Command(`${targetDir}/release/examples/degrade_server`, { args: [scratch], env: { CAMS: "1", CAM_SIZE: "1280x720", CAM_NOISE: "4", CLOUD_BYTES: "0", MAP_BYTES: "0", RUST_LOG: Deno.env.get("RUST_LOG") ?? "warn" }, stdout: "piped", stderr: serverLog ? "piped" : "null" }).spawn()
    if (serverLog) {
        const file = await Deno.open(serverLog, { write: true, create: true, append: true })
        server.stderr.pipeTo(file.writable)
    }
    const serverUrl = await (async () => {
        for await (const chunk of server.stdout.pipeThrough(new TextDecoderStream())) {
            const match = chunk.match(/listening on (\S+)/)
            if (match) {
                return match[1]
            }
        }
        throw new Error("degrade_server exited")
    })()
    const link = shapedLink(serverUrl, cell)
    const page = await openPage(`${link.url}/blank.html`, (cell.seconds + 60) * 1000)
    try {
        const phases = cell.phases ?? [[0, cell.seconds, cell.name]]
        return await page.evaluate(async (url: string, phases: [number, number, string][], measure: number, diagnose: boolean) => {
            const { connect } = await import("/client/zenoh_gateway.js")
            const client = await connect(url, { heartbeatHz: 5, heartbeatMisses: 50 }).catch(() => connect(url, { heartbeatHz: 5, heartbeatMisses: 50 }))
            const started = performance.now()
            // per shown frame: (when, width x height); per metadata message: (when, encoded bytes, quality)
            const shown: { at: number; size: string }[] = []
            const sent: { at: number; bytes: number; quality: number }[] = []
            let element = null as HTMLVideoElement | null
            const subscription = client.subscribe("robot/cam0", { delivery: "latest", encoding: "test-cam" }, (message: any) => {
                if (message.video) {
                    sent.push({ at: performance.now(), bytes: message.video.encodedBytes, quality: message.video.quality })
                }
                if (element === null && message.mediaStream) {
                    element = document.createElement("video")
                    element.muted = true
                    element.autoplay = true
                    element.srcObject = message.mediaStream
                    document.body.append(element)
                    element.play().catch(() => {})
                    subscription.watchVideoLatency(element)
                    const onFrame = (_now: number, frame: VideoFrameCallbackMetadata) => {
                        shown.push({ at: performance.now(), size: `${frame.width}x${frame.height}` })
                        element!.requestVideoFrameCallback(onFrame)
                    }
                    element.requestVideoFrameCallback(onFrame)
                }
            })
            const results: any[] = []
            // counters from the gateway's stats, summed wherever they appear: keyframes sent, the browser's PLI/FIRs, RTP packets the pacer dropped
            const counters = (stats: unknown) => {
                const sums: Record<string, number> = { keyframes: 0, keyframeRequests: 0, pacerDroppedPackets: 0 }
                const walk = (value: any) => {
                    for (const [key, inner] of Object.entries(value ?? {})) {
                        if (key in sums && typeof inner === "number") {
                            sums[key] += inner
                        } else if (inner && typeof inner === "object") {
                            walk(inner)
                        }
                    }
                }
                walk(stats)
                return sums
            }
            const history: { at: number; sums: Record<string, number> }[] = []
            const timeline: string[] = []
            const sampler = setInterval(async () => {
                const stats = await client._request({ op: "stats" }, 5000).catch(() => null)
                history.push({ at: performance.now(), sums: counters(stats) })
                if (diagnose) {
                    const bandwidth = (stats as any)?.bandwidth ?? {}
                    const video = JSON.stringify(stats).match(/"videoWidth":(\d+),"videoHeight":(\d+)/)
                    timeline.push(`${((performance.now() - started) / 1000).toFixed(0)}s budget ${Math.round(bandwidth.budgetBytesPerSec / 1000)} videoEst ${Math.round(bandwidth.videoEstimateBytesPerSec / 1000)} brake ${Math.round((bandwidth.videoBrakeBytesPerSec ?? NaN) / 1000)} sent ${Math.round(bandwidth.videoSentBytesPerSec / 1000)} KB/s pacer ${Math.round(bandwidth.pacerDelayMs)} ms queue ${Math.round(bandwidth.videoQueueDelayMs)} ms ${video ? `${video[1]}x${video[2]}` : "-"} kf ${counters(stats).keyframes}`)
                }
            }, 1000)
            for (const [, end, label] of phases) {
                await new Promise((resolve) => setTimeout(resolve, started + end * 1000 - performance.now()))
                const from = performance.now() - measure * 1000
                const frames = shown.filter((frame) => frame.at >= from)
                const gaps = frames.slice(1).map((frame, i) => frame.at - frames[i].at)
                // the longest freeze includes one running into the window's start or end
                const edges = frames.length ? [frames[0].at - from, performance.now() - frames.at(-1)!.at] : [measure * 1000]
                const sizes = frames.map((frame) => frame.size)
                const messages = sent.filter((message) => message.at >= from)
                const stats = JSON.stringify(await client._request({ op: "stats" }, 5000).catch(() => null))
                const gatewayLatency = JSON.parse(stats?.match(/"videoLatency":(\{[^}]*\})/)?.[1] ?? "null")
                const bw = JSON.parse(stats ?? "null")?.bandwidth
                const now = counters(JSON.parse(stats ?? "null"))
                const before = history.find((sample) => sample.at >= from)?.sums ?? now
                results.push({
                    subscription: (() => {
                        const found: any[] = []
                        const walk = (value: any) => {
                            for (const inner of Object.values(value ?? {})) {
                                if (inner && typeof inner === "object") {
                                    if ("keyframeRequests" in (inner as any)) {
                                        found.push(inner)
                                    } else {
                                        walk(inner)
                                    }
                                }
                            }
                        }
                        walk(JSON.parse(stats ?? "null"))
                        return found.map((one) => Object.fromEntries(Object.entries(one).filter(([, value]) => typeof value !== "object")))
                    })(),
                    counts: Object.fromEntries(Object.entries(now).map(([key, value]) => [key, value - before[key]])),
                    bandwidth: bw && Object.fromEntries(Object.entries(bw).filter(([, value]) => typeof value !== "object")),
                    pacerP95Ms: gatewayLatency?.pacerP95Ms,
                    label,
                    latency: subscription.videoLatency(measure * 1000),
                    shownPerSec: frames.length / measure,
                    maxGapMs: Math.max(...gaps, ...edges),
                    size: sizes.sort()[Math.floor(sizes.length / 2)] ?? "none",
                    quality: messages.reduce((sum, message) => sum + message.quality, 0) / Math.max(1, messages.length),
                    videoBytesPerSec: messages.reduce((sum, message) => sum + message.bytes, 0) / measure,
                })
            }
            clearInterval(sampler)
            if (diagnose) {
                results[0].timeline = timeline
            }
            client.close()
            return results
        }, [link.url, phases, measure, Deno.env.get("DIAGNOSE") === "1"])
    } finally {
        await page.close()
        server.kill("SIGTERM")
        await server.status
        await link.close()
    }
}

const failures: string[] = []
const rows: string[] = []
const round = (value: number | undefined) => value === undefined || !Number.isFinite(value) ? "-" : String(Math.round(value))
let previousWidth = Infinity
for (const cell of chosen) {
    console.log(`${cell.name}: ${cell.seconds} s${repeat > 1 ? ` x ${repeat}` : ""}`)
    const runs: Phase[][] = []
    for (let run = 0; run < repeat; run++) {
        runs.push(await runCell(cell))
    }
    for (const [index, label] of runs[0].map((phase, index) => [index, phase.label] as const)) {
        // the run with the median p95 stands for the phase (a frameless run counts as the worst)
        const tries = runs.map((phases) => phases[index]).sort((a, b) => (a.latency?.total.p95 ?? Infinity) - (b.latency?.total.p95 ?? Infinity))
        const phase = tries[Math.floor((tries.length - 1) / 2)]
        if (repeat > 1) {
            const each = runs.map((phases) => phases[index])
            console.log(`  ${cell.name} ${label}: p95 per run ${tries.map((one) => round(one.latency?.total.p95)).join(" / ")} ms; per run (p95 ms, frames/s, longest freeze ms): ${each.map((one) => `${round(one.latency?.total.p95)}, ${one.shownPerSec.toFixed(1)}, ${round(one.maxGapMs)}`).join(" | ")}`)
        }
        const span = cell.phases?.find(([, , label]) => label === phase.label)
        const rate = cell.schedule.filter(([at]) => at <= (span?.[0] ?? 0)).at(-1)![1]
        const name = cell.phases ? `${cell.name} ${phase.label}` : cell.name
        const latency = phase.latency
        if (Deno.env.get("DIAGNOSE") === "1") {
            console.log(`  ${name} gateway bandwidth: ${JSON.stringify(phase.bandwidth)}`)
            console.log(`  ${name} gateway subscription: ${JSON.stringify(phase.subscription)}`)
            // the worst run's timeline (only a run's first phase carries one)
            const worst = runs.map((phases) => phases[0]).sort((a, b) => (b.latency?.total.p95 ?? Infinity) - (a.latency?.total.p95 ?? Infinity))[0]
            for (const line of (index === 0 ? worst.timeline : []) ?? []) {
                console.log(`    ${line}`)
            }
        }
        console.log(`  ${name}: ${phase.counts.keyframes} keyframes, ${phase.counts.keyframeRequests} PLI/FIR, ${phase.counts.pacerDroppedPackets} RTP packets dropped by the pacer (last ${measure} s)`)
        const stages = latency?.stages
        const use = phase.videoBytesPerSec / rate
        rows.push([name.padEnd(20), round(latency?.total.p50).padStart(6), round(latency?.total.p95).padStart(6), round(stages?.encoding.p50).padStart(5), round(stages?.trackToShown.p50).padStart(6), round(phase.pacerP95Ms).padStart(6), round(stages?.toReceived?.p95).padStart(6), round(stages?.receivedToShown?.p95).padStart(6), phase.shownPerSec.toFixed(1).padStart(5), round(phase.maxGapMs).padStart(6), phase.size.padStart(9), phase.quality.toFixed(2).padStart(5), `${Math.round(use * 100)}%`.padStart(5)].join(" "))
        const budgetMs = cell.delayMs + cell.jitterMs + (rate >= 62_500 ? 300 : 600)
        if (!latency) {
            failures.push(`${name}: no frames measured`)
            continue
        }
        if (latency.total.p95 > budgetMs) {
            failures.push(`${name}: latency p95 ${round(latency.total.p95)} ms > ${budgetMs}`)
        }
        const minFps = rate >= 62_500 ? 10 : 3
        if (phase.shownPerSec < minFps) {
            failures.push(`${name}: ${phase.shownPerSec.toFixed(1)} frames/s shown < ${minFps}`)
        }
        if (phase.maxGapMs > 2000) {
            failures.push(`${name}: froze ${round(phase.maxGapMs)} ms`)
        }
        // (the camera's most at 0.3 bit/pixel: 1280 x 720 x 30 x 0.3 / 8 = 1 MB/s): on a clean link, video either fills
        // 40% of it or shows the whole picture (a picture stuck at 160x90 used 3% of 8 Mbit/s)
        if (cell.clean && use < 0.4 && phase.size !== "1280x720") {
            failures.push(`${name}: video used ${Math.round(use * 100)}% of the link (< 40%) at ${phase.size}`)
        }
        const [width, height] = phase.size.split("x").map(Number)
        if (width < 160 && height < 64) {
            failures.push(`${name}: ${phase.size} is under minResolution [160, 64]`)
        }
        if (cell.clean) {
            if (width > previousWidth * 1.1) {
                failures.push(`${name}: ${width} wide, wider than the faster link before it (${previousWidth})`)
            }
            previousWidth = width
        }
    }
}
// toShow: written to the track -> shown (p50); pacer95: the gateway's RTP pacer (p95); toRecv95/recvShow95: written ->
// the browser has the frame, and from there to the screen (p95)
console.log(`\n${"cell".padEnd(20)}  p50ms  p95ms encode toShow pacer95 toRecv95 recvShow95 fps/s  gapms      size  qual   use`)
console.log(rows.join("\n"))
if (failures.length) {
    console.log(`\nFAIL\n${failures.join("\n")}`)
    Deno.exit(1)
}
console.log("\nPASS")
Deno.exit(0)
