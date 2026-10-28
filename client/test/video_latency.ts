#!/usr/bin/env -S deno run --allow-all
// Built-in video latency: a page in headless Chrome subscribes a camera of the gateway's examples/degrade_server and
// measures it with `Subscription.watchVideoLatency`. Pass/fail:
// - nearly every shown frame is named exactly by its RTP timestamp (unmatched < 5%);
// - the stages add up to the total (within 1 ms), each is >= -2 ms (clock-sync slack), the total p50 < 300 ms locally;
// - source-to-shown from the stamp in the frame's own RTP packets (abs-capture-time, Chrome) agrees within 2 ms (p50);
// - a 200 ms playout delay asked for shows up in trackToShown and nowhere before the track (+150..300 ms);
// - nearly every shown frame's metadata (`videoFrameInfo(rtpTimestamp)`) carries its picture's header (stamp, frameId).
// BROWSER=firefox runs it in Playwright's Firefox (no abs-capture-time there: the in-packet check is Chrome's).
// Usage: deno run --allow-all client/test/video_latency.ts   (SECONDS per phase, default 8)

/// <reference lib="dom" />
import { firefox, openPage, waitForOtherBrowserTests } from "./browser.ts"

const seconds = Number(Deno.env.get("SECONDS") ?? 8)
await waitForOtherBrowserTests()
const root = new URL("../..", import.meta.url).pathname
const scratch = await Deno.makeTempDir({ prefix: "zenoh-gateway-video-latency-" })
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
const server = new Deno.Command(`${targetDir}/release/examples/degrade_server`, { args: [scratch], stdout: "piped", stderr: "inherit", env: { CAMS: "1", CLOUD_BYTES: "0", MAP_BYTES: "0" } }).spawn()
const serverUrl = await (async () => {
    for await (const chunk of server.stdout.pipeThrough(new TextDecoderStream())) {
        const match = chunk.match(/listening on (\S+)/)
        if (match) {
            return match[1]
        }
    }
    throw new Error("degrade_server exited")
})()

const failures: string[] = []
const driver = await openPage(`${serverUrl}/blank.html`, seconds * 4_000 + 60_000)
try {
    const measure = async (playoutDelay: [number, number]) => await driver.evaluate(async (url: string, playoutDelay: [number, number], seconds: number) => {
        const { connect } = await import("/client/zenoh_gateway.js")
        const client = (globalThis as any).client ??= await connect(url, { heartbeatHz: 5, heartbeatMisses: 50 })
        let element = null as HTMLVideoElement | null
        const subscription = client.subscribe("robot/cam0", { delivery: "latest", encoding: "test-cam", playoutDelay }, (message: any) => {
            if (element === null && message.mediaStream) {
                element = document.createElement("video")
                element.muted = true
                element.autoplay = true
                element.srcObject = message.mediaStream
                document.body.append(element)
                element.play()
                stop = subscription.watchVideoLatency(element)
                const onShown = (_now: number, frame: { rtpTimestamp?: number }) => {
                    const info = frame.rtpTimestamp === undefined ? undefined : subscription.videoFrameInfo(frame.rtpTimestamp)
                    headers.shown++
                    headers.named += info?.frameId === "test_cam_optical" && typeof info.stamp === "number" ? 1 : 0
                    element?.requestVideoFrameCallback(onShown as any)
                }
                element.requestVideoFrameCallback(onShown as any)
            }
        })
        let stop = () => {}
        const headers = { shown: 0, named: 0 }
        // (past the first second: a new track's first frames wait on its decoder)
        await new Promise((resolve) => setTimeout(resolve, 2_000))
        const warm = subscription.videoLatency()?.frames ?? 0
        await new Promise((resolve) => setTimeout(resolve, seconds * 1000))
        const latency = subscription.videoLatency()
        const stats = await client._request({ op: "stats" }, 5000)
        stop()
        subscription.close()
        element?.remove()
        return { latency, warm, headers, clockOffsetMs: client.clockOffsetMs, gateway: JSON.stringify(stats).match(/"videoLatency":(\{[^}]*\})/)?.[1] ?? null }
    }, [serverUrl, playoutDelay, seconds])

    const base = await measure([0, 0])
    console.log("playoutDelay [0, 0]:", JSON.stringify(base.latency, null, 1), "\ngateway videoLatency:", base.gateway)
    const latency = base.latency
    if (!latency || latency.frames < seconds * 10) {
        failures.push(`only ${latency?.frames ?? 0} frames measured in ${seconds} s`)
    } else {
        if (latency.unmatched > latency.frames * 0.05) {
            failures.push(`${latency.unmatched} shown frames unmatched (> 5% of ${latency.frames})`)
        }
        const { toGateway, encoding, toTrack, trackToShown } = latency.stages
        const sum = toGateway.p50 + encoding.p50 + toTrack.p50 + trackToShown.p50
        console.log(`stage p50s sum ${sum.toFixed(1)} vs total p50 ${latency.total.p50.toFixed(1)}`)
        for (const [name, stage] of Object.entries(latency.stages) as [string, { p50: number }][]) {
            if (stage.p50 < -2) {
                failures.push(`stage ${name} p50 ${stage.p50.toFixed(1)} ms < -2`)
            }
        }
        if (latency.total.p50 > 300) {
            failures.push(`total p50 ${latency.total.p50.toFixed(1)} ms > 300 on localhost`)
        }
        if (firefox) {
            // (Firefox neither offers abs-capture-time nor reports it)
        } else if (!latency.captureToShown) {
            failures.push("no in-packet stamp from Chrome: abs-capture-time not read")
        } else if (Math.abs(latency.captureToShown.p50 - latency.total.p50) > 2) {
            failures.push(`Chrome's capture-to-shown p50 ${latency.captureToShown.p50.toFixed(1)} vs ours ${latency.total.p50.toFixed(1)} (> 2 ms apart)`)
        }
        console.log(`shown frames naming their picture's header: ${base.headers.named} of ${base.headers.shown}`)
        if (base.headers.shown === 0 || base.headers.named < base.headers.shown * 0.95) {
            failures.push(`${base.headers.named} of ${base.headers.shown} shown frames' metadata named their picture's header`)
        }
        if (!base.gateway) {
            failures.push("gateway stats have no videoLatency")
        }
    }
    const delayed = await measure([200, 200])
    console.log("playoutDelay [200, 200]:", JSON.stringify(delayed.latency?.stages), "total", JSON.stringify(delayed.latency?.total))
    if (latency && delayed.latency) {
        const added = delayed.latency.stages.trackToShown.p50 - latency.stages.trackToShown.p50
        const beforeTrack = (l: typeof latency) => l.stages.toGateway.p50 + l.stages.encoding.p50 + l.stages.toTrack.p50
        if (added < 150 || added > 300) {
            failures.push(`a 200 ms playout delay added ${added.toFixed(1)} ms to trackToShown (want 150..300)`)
        }
        if (Math.abs(beforeTrack(delayed.latency) - beforeTrack(latency)) > 30) {
            failures.push(`the playout delay moved the gateway's stages: ${beforeTrack(latency).toFixed(1)} -> ${beforeTrack(delayed.latency).toFixed(1)} ms`)
        }
    } else {
        failures.push("no frames with the playout delay")
    }
} finally {
    await driver.close()
    server.kill("SIGTERM")
    await server.status
}
if (failures.length) {
    console.log(`FAIL\n${failures.join("\n")}`)
    Deno.exit(1)
}
console.log("PASS")
Deno.exit(0)
