#!/usr/bin/env -S deno run --allow-all
// A takeover lease in a browser: page A leases the camera ({ subscribe, allowTakeover }) and watches it (a video
// track), then page B takes the same lease. Pass: A's onLost runs once with "takenOver", its camera subscription is
// "rejected" and its frames stop; B gets frames.
// Usage: deno run --allow-all client/test/lease_takeover.ts

import { launch } from "jsr:@astral/astral@0.5.6"

const root = new URL("../..", import.meta.url).pathname
const scratch = await Deno.makeTempDir({ prefix: "zenoh-gateway-lease-takeover-" })
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
const server = new Deno.Command(`${targetDir}/release/examples/degrade_server`, { args: [scratch], stdout: "piped", stderr: "inherit", env: { CAMS: "1" } }).spawn()
const serverUrl = await (async () => {
    for await (const chunk of server.stdout.pipeThrough(new TextDecoderStream())) {
        const match = chunk.match(/listening on (\S+)/)
        if (match) {
            return match[1]
        }
    }
    throw new Error("degrade_server exited")
})()

/** Connects the page, leases the camera and subscribes it; `globalThis.frames` counts its video frames. */
const subscribeCamera = (url: string) => (async () => {
    const { connect } = await import("/client/zenoh_gateway.js")
    const client = await connect(url, { heartbeatHz: 5, heartbeatMisses: 50 })
    const page = globalThis as any
    page.client = client
    page.frames = 0
    page.lost = []
    await client.lease({ subscribe: ["robot/cam0"], allowTakeover: true, onLost: (reason: string) => page.lost.push(reason) })
    page.subscription = client.subscribe("robot/cam0", { delivery: "latest", encoding: "test-cam" }, (message: any) => {
        if (message.video) {
            page.frames++
        }
    })
    const started = performance.now()
    while (page.frames < 3 && performance.now() - started < 10_000) {
        await new Promise((resolve) => setTimeout(resolve, 50))
    }
    return page.frames
})

const failures: string[] = []
const browser = await launch({ headless: true, args: ["--no-sandbox", "--autoplay-policy=no-user-gesture-required"] })
try {
    const pageA = await browser.newPage(`${serverUrl}/blank.html`)
    const pageB = await browser.newPage(`${serverUrl}/blank.html`)
    const framesA = await pageA.evaluate(`(${subscribeCamera.toString()})(${JSON.stringify(serverUrl)})()`)
    if (framesA < 3) {
        failures.push(`A: only ${framesA} frames before B subscribed`)
    }
    const framesB = await pageB.evaluate(`(${subscribeCamera.toString()})(${JSON.stringify(serverUrl)})()`)
    if (framesB < 3) {
        failures.push(`B: only ${framesB} frames`)
    }
    await new Promise((resolve) => setTimeout(resolve, 1000))
    const a = await pageA.evaluate(`(async () => {
        const page = globalThis
        const before = page.frames
        await new Promise((resolve) => setTimeout(resolve, 1500))
        return { state: page.subscription.state, reason: page.subscription.rejectionReason, lost: page.lost, framesAfter: page.frames - before }
    })()`) as { state: string, reason: string, lost: string[], framesAfter: number }
    console.log("A after B subscribed:", a)
    if (a.state !== "rejected" || !a.reason?.includes("leased by another client")) {
        failures.push(`A's camera is ${a.state} (${a.reason}), not rejected for the lease`)
    }
    if (JSON.stringify(a.lost) !== JSON.stringify(["takenOver"])) {
        failures.push(`A's onLost got ${JSON.stringify(a.lost)}`)
    }
    if (a.framesAfter !== 0) {
        failures.push(`A still got ${a.framesAfter} frames after losing the lease`)
    }
    const b = await pageB.evaluate(`(async () => {
        const page = globalThis
        const before = page.frames
        await new Promise((resolve) => setTimeout(resolve, 1500))
        const stats = await page.client._request({ op: "stats" }, 5000)
        return { state: page.subscription.state, framesAfter: page.frames - before, subscriptions: stats.memory.subscriptions }
    })()`) as { state: string, framesAfter: number, subscriptions: number }
    console.log("B:", b)
    if (b.state !== "open" || b.framesAfter < 3) {
        failures.push(`B: state ${b.state}, ${b.framesAfter} frames in 1.5 s`)
    }
} finally {
    await browser.close()
    server.kill("SIGTERM")
    await server.status
}
if (failures.length) {
    console.log(`FAIL\n${failures.join("\n")}`)
    Deno.exit(1)
}
console.log("PASS")
Deno.exit(0)
