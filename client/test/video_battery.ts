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
// - pictures shrinking with the link, never under minResolution ([160, 64]: 160x90 for 16:9).
// Usage: deno run --allow-all --unstable-net client/test/video_battery.ts [cell name ...]   (default: every cell)

/// <reference lib="dom" />
import { launch } from "jsr:@astral/astral@0.5.6"
import { type LinkShape, shapedLink } from "./shaped_link.ts"

// phases: [start s, end s, label] measured over their last `measure` seconds
type Cell = LinkShape & { name: string; seconds: number; phases?: [number, number, string][]; clean?: boolean }
const measure = 10
const cells: Cell[] = [
    ...([["20mbit", 2_500_000], ["8mbit", 1_000_000], ["4mbit", 500_000], ["2mbit", 250_000], ["1mbit", 125_000], ["500kbit", 62_500], ["250kbit", 31_250], ["100kbit", 12_500]] as [string, number][])
        .map(([name, rate]) => ({ name, seconds: 25, schedule: [[0, rate]] as [number, number][], delayMs: 20, jitterMs: 0, loss: 0, clean: true })),
    { name: "rtt300", seconds: 25, schedule: [[0, 500_000]], delayMs: 150, jitterMs: 0, loss: 0 },
    { name: "wifi_jitter", seconds: 25, schedule: [[0, 500_000]], delayMs: 5, jitterMs: 200, loss: 0.01 },
    { name: "loss3", seconds: 25, schedule: [[0, 500_000]], delayMs: 20, jitterMs: 0, loss: 0.03 },
    // a sudden 16x drop and back: each phase measured once it had 10 s to settle
    { name: "step", seconds: 60, schedule: [[0, 1_000_000], [20, 62_500], [40, 1_000_000]], delayMs: 20, jitterMs: 0, loss: 0, phases: [[0, 20, "8mbit"], [20, 40, "500kbit"], [40, 60, "8mbit again"]] },
]
// (bin/test runs and caches each default cell on its own)
if (Deno.args[0] === "--list-cells") {
    console.log(cells.map((cell) => cell.name).join("\n"))
    Deno.exit(0)
}
const chosen = Deno.args.length ? cells.filter((cell) => Deno.args.includes(cell.name)) : cells

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

type Phase = { bandwidth?: Record<string, unknown>; pacerP95Ms?: number; label: string; latency: any; shownPerSec: number; maxGapMs: number; size: string; quality: number; videoBytesPerSec: number }

async function runCell(cell: Cell): Promise<Phase[]> {
    const server = new Deno.Command(`${targetDir}/release/examples/degrade_server`, { args: [scratch], env: { CAMS: "1", CAM_SIZE: "1280x720", CAM_NOISE: "4", CLOUD_BYTES: "0", MAP_BYTES: "0" }, stdout: "piped", stderr: "null" }).spawn()
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
    const browser = await launch({ headless: true, args: ["--no-sandbox", "--autoplay-policy=no-user-gesture-required"] })
    try {
        const page = await browser.newPage(`${link.url}/blank.html`)
        ;(page as any).timeout = (cell.seconds + 60) * 1000
        const phases = cell.phases ?? [[0, cell.seconds, cell.name]]
        return await page.evaluate(async (url: string, phases: [number, number, string][], measure: number) => {
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
                results.push({
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
            client.close()
            return results
        }, { args: [link.url, phases, measure] })
    } finally {
        await browser.close()
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
    console.log(`${cell.name}: ${cell.seconds} s`)
    const phases = await runCell(cell)
    for (const phase of phases) {
        const span = cell.phases?.find(([, , label]) => label === phase.label)
        const rate = cell.schedule.filter(([at]) => at <= (span?.[0] ?? 0)).at(-1)![1]
        const name = cell.phases ? `${cell.name} ${phase.label}` : cell.name
        const latency = phase.latency
        if (Deno.env.get("DIAGNOSE") === "1") {
            console.log(`  ${name} gateway bandwidth: ${JSON.stringify(phase.bandwidth)}`)
        }
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
        // (the camera's most at 0.3 bit/pixel: 1280 x 720 x 30 x 0.3 / 8 = 1 MB/s)
        if (cell.clean && rate <= 1_000_000 && use < 0.4) {
            failures.push(`${name}: video used ${Math.round(use * 100)}% of the link (< 40%)`)
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
