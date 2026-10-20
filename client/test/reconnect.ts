#!/usr/bin/env -S deno run --allow-all --unstable-net
// A phone switching tabs: the page goes hidden and its link goes dark for BLACKOUT_S (40: past the 30 s ICE consent
// timeout, so the connection is gone, as a backgrounded phone tab's is) seconds, then both come back.
// Measures how long after that until the camera shows frames again, with the client's default `freshAfterMs` (2000)
// and with it off (the browser declares the old connection failed first, as before). Fails if video isn't back within
// `freshAfterMs` + 2 s (before 0.5.24 it never came back: a failed reconnect attempt held the renegotiation queue).
// Usage: deno run --allow-all --unstable-net client/test/reconnect.ts   (BROWSER=firefox for Firefox)

/// <reference lib="dom" />
/// <reference lib="deno.ns" />
import { openPage, waitForOtherBrowserTests } from "./browser.ts"
import { shapedLink } from "./shaped_link.ts"

const blackoutSeconds = Number(Deno.env.get("BLACKOUT_S") ?? 40)
const darkAt = 8
await waitForOtherBrowserTests()
const root = new URL("../..", import.meta.url).pathname
const scratch = await Deno.makeTempDir({ prefix: "zenoh-gateway-reconnect-" })
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

/** Seconds from the link coming back to the first frame shown after it, or null if none came in 30 s. */
async function resumeSeconds(freshAfterMs: number, heartbeat: boolean): Promise<number | null> {
    const { seconds, log } = await resume(freshAfterMs, heartbeat)
    if (seconds === null || Deno.env.get("VERBOSE") === "1") {
        console.log(`  freshAfterMs ${freshAfterMs} heartbeat ${heartbeat}:\n    ${log.join("\n    ")}`)
    }
    return seconds
}

async function resume(freshAfterMs: number, heartbeat: boolean): Promise<{ seconds: number | null; log: string[] }> {
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
    const link = shapedLink(serverUrl, { schedule: [[0, 1_000_000], [darkAt, 0], [darkAt + blackoutSeconds, 1_000_000]], delayMs: 20, jitterMs: 0, loss: 0 })
    const page = await openPage(`${link.url}/blank.html`, (darkAt + blackoutSeconds + 60) * 1000)
    try {
        // the page, and in it the client and the camera; `window.back` is set when it becomes visible again
        const watching = page.evaluate(async (url: string, freshAfterMs: number, heartbeat: boolean, waitSeconds: number) => {
            const page = window as any
            let visibility: DocumentVisibilityState = "visible"
            Object.defineProperty(document, "visibilityState", { get: () => visibility, configurable: true })
            page.setVisibility = (state: DocumentVisibilityState) => {
                visibility = state
                if (state === "visible") {
                    page.back = performance.now()
                }
                document.dispatchEvent(new Event("visibilitychange"))
            }
            const { connect } = await import("/client/zenoh_gateway.js")
            // (with a heartbeat the gateway drops the connection once it stops hearing it, as a controller page's)
            const client = await connect(url, heartbeat ? { freshAfterMs, heartbeatHz: 5, heartbeatMisses: 3 } : { freshAfterMs })
            const shown: number[] = []
            // (the client's states and its warnings, for a failure)
            const t0 = performance.now()
            const log: string[] = []
            client.onState((state: string) => log.push(`${((performance.now() - t0) / 1000).toFixed(1)}s ${state}`))
            for (const level of ["warn", "error"] as const) {
                const original = console[level]
                console[level] = (...args: unknown[]) => {
                    log.push(`${((performance.now() - t0) / 1000).toFixed(1)}s ${level} ${args.map(String).join(" ").slice(0, 200)}`)
                    original(...args)
                }
            }
            let element = null as HTMLVideoElement | null
            page.subscription = client.subscribe("robot/cam0", { delivery: "latest", encoding: "test-cam" }, (message: any) => {
                if (message.video && page.back !== undefined && !page.sawVideoAfter) {
                    page.sawVideoAfter = true
                    log.push(`${((performance.now() - t0) / 1000).toFixed(1)}s first video metadata after back; stream ${message.mediaStream ? (message.mediaStream === element?.srcObject ? "same" : "new") : "none"}, tracks ${message.mediaStream?.getTracks().map((track: MediaStreamTrack) => track.readyState).join(",")}`)
                }
                if (message.mediaStream && element?.srcObject !== message.mediaStream) {
                    log.push(`${((performance.now() - t0) / 1000).toFixed(1)}s attach a stream`)
                    element ??= document.body.appendChild(Object.assign(document.createElement("video"), { muted: true, autoplay: true }))
                    element.srcObject = message.mediaStream
                    element.play().catch(() => {})
                    const onFrame = () => {
                        shown.push(performance.now())
                        element!.requestVideoFrameCallback(onFrame)
                    }
                    element.requestVideoFrameCallback(onFrame)
                }
            })
            while (page.back === undefined) {
                await new Promise((resolve) => setTimeout(resolve, 100))
            }
            await new Promise((resolve) => setTimeout(resolve, 5000))
            const peer = client._peer as RTCPeerConnection | null
            log.push(`back+5s: client ${client.state}, subscription ${page.subscription.state}, channel ${page.subscription.channel?.readyState} ${page.subscription.channel?.label?.slice(0, 60)}, peer ${peer?.connectionState}, transceivers ${peer?.getTransceivers().map((t) => `${t.mid}:${t.direction}:${t.currentDirection}:${t.receiver.track.readyState}`).join(" ")}`)
            await new Promise((resolve) => setTimeout(resolve, (waitSeconds - 5) * 1000))
            client.close()
            const first = shown.find((time) => time > page.back)
            log.push(`back at ${((page.back - t0) / 1000).toFixed(1)}s`)
            return { seconds: first === undefined ? null : (first - page.back) / 1000, log }
        }, [link.url, freshAfterMs, heartbeat, 30])
        // hidden while the link is dark (by the link's own clock), visible once it is back
        const at = async (seconds: number) => {
            while (link.startedAt() === 0) {
                await new Promise((resolve) => setTimeout(resolve, 50))
            }
            await new Promise((resolve) => setTimeout(resolve, Math.max(0, link.startedAt() + seconds * 1000 - performance.now())))
        }
        await at(darkAt)
        await page.evaluate(async () => (window as any).setVisibility("hidden"), [])
        await at(darkAt + blackoutSeconds)
        await page.evaluate(async () => (window as any).setVisibility("visible"), [])
        return await watching
    } finally {
        await page.close()
        server.kill("SIGTERM")
        await server.status
        await link.close()
    }
}

const show = (seconds: number | null) => seconds === null ? "no frames in 30 s" : `${seconds.toFixed(1)} s`
let failed = false
for (const heartbeat of [false, true]) {
    const fresh = await resumeSeconds(2000, heartbeat)
    // (as before freshAfterMs: wait for the browser to call the old connection failed)
    const off = await resumeSeconds(1e9, heartbeat)
    console.log(`${heartbeat ? "heartbeat 5 Hz" : "no heartbeat"}: after a ${blackoutSeconds} s blackout while hidden, frames again: freshAfterMs 2000 ${show(fresh)}, off ${show(off)}`)
    // back within freshAfterMs + 2 s (either way; which is faster when the reconnect loop retries anyway is luck)
    failed ||= fresh === null || fresh > 4 || off === null || off > 4
}
if (failed) {
    console.log("FAIL")
    Deno.exit(1)
}
console.log("PASS")
