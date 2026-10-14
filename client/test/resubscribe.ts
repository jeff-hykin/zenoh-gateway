#!/usr/bin/env -S deno run --allow-all
// Nothing a page stops using stays behind: a page in headless Chrome subscribes a camera (a video track over a
// renegotiation), waits for frames, closes it, and does that ROUNDS times (default 30), against the gateway's
// examples/degrade_server. Every few rounds it reads the gateway's stats memory section. Pass/fail:
// - memory.tracks stays at most 2 (the open subscription's, plus one the next renegotiation hasn't dropped yet);
// - the other memory counts end where the first round left them (channels, subscriptions, unacked frames);
// - the server's RSS grows by less than RSS_SLACK_MB (default 30) from round 5 to the last.
// Usage: deno run --allow-all client/test/resubscribe.ts   (DEGRADE_FEATURES=dhat-heap DHAT_FILE=... profiles the heap)

import { launch } from "jsr:@astral/astral@0.5.6"

const rounds = Number(Deno.env.get("ROUNDS") ?? 30)
const rssSlackMb = Number(Deno.env.get("RSS_SLACK_MB") ?? 30)
const root = new URL("../..", import.meta.url).pathname
const scratch = await Deno.makeTempDir({ prefix: "zenoh-gateway-resubscribe-" })
await Deno.mkdir(`${scratch}/client`)
await Deno.writeTextFile(`${scratch}/blank.html`, `<!doctype html><html><head><meta charset="utf-8"></head><body></body></html>`)
const bundled = await new Deno.Command(Deno.execPath(), { args: ["bundle", "--quiet", "--platform", "browser", "-o", `${scratch}/client/zenoh_gateway.js`, `${root}client/zenoh_gateway.ts`] }).output()
if (bundled.code !== 0) {
    throw new Error(`deno bundle failed: ${new TextDecoder().decode(bundled.stderr)}`)
}
const build = await new Deno.Command("cargo", { args: ["build", "--release", "--example", "degrade_server", ...(Deno.env.get("DEGRADE_FEATURES") ? ["--features", Deno.env.get("DEGRADE_FEATURES")!] : [])], cwd: `${root}gateway`, stderr: "inherit" }).output()
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
const rssMb = async () => {
    const output = await new Deno.Command("ps", { args: ["-o", "rss=", "-p", String(server.pid)] }).output()
    return Number(new TextDecoder().decode(output.stdout).trim()) / 1024
}

const failures: string[] = []
const browser = await launch({ headless: true, args: ["--no-sandbox", "--autoplay-policy=no-user-gesture-required"] })
try {
    const page = await browser.newPage(`${serverUrl}/blank.html`)
    ;(page as any).timeout = rounds * 15_000 + 60_000
    await page.evaluate(async (url: string) => {
        const { connect } = await import("/client/zenoh_gateway.js")
        const client = await connect(url, { heartbeatHz: 5, heartbeatMisses: 50 })
        ;(globalThis as any).client = client
        // data streams alongside, as a real page has
        client.subscribe("robot/pose", { delivery: "latest" }, () => {})
        client.subscribe("robot/tf", { delivery: "reliable" }, () => {})
    }, { args: [serverUrl] })
    const memory = async () => await page.evaluate(async () => ((await (globalThis as any).client._request({ op: "stats" }, 5000)) as any).memory)
    const samples: { round: number; rss: number; memory: any }[] = []
    for (let round = 1; round <= rounds; round++) {
        const frames = await page.evaluate(async () => {
            const client = (globalThis as any).client
            let frames = 0
            const subscription = client.subscribe("robot/cam0", { delivery: "latest", encoding: "test-cam" }, (message: any) => {
                if (message.video) {
                    frames++
                }
            })
            const started = performance.now()
            while (frames < 3 && performance.now() - started < 10_000) {
                await new Promise((resolve) => setTimeout(resolve, 50))
            }
            subscription.close()
            return frames
        })
        if (frames < 3) {
            failures.push(`round ${round}: only ${frames} video frames in 10 s`)
        }
        if (round === 1 || round % 5 === 0 || round === rounds) {
            // (let the gateway see the close before counting)
            await new Promise((resolve) => setTimeout(resolve, 500))
            samples.push({ round, rss: await rssMb(), memory: await memory() })
            const last = samples.at(-1)!
            console.log(`round ${round}: rss ${last.rss.toFixed(1)} MB, tracks ${last.memory?.tracks}, channels ${last.memory?.channels}, subscriptions ${last.memory?.subscriptions}, unacked ${last.memory?.unacked}`)
        }
    }
    const [first, last] = [samples[0], samples.at(-1)!]
    const maxTracks = Math.max(...samples.map((sample) => sample.memory?.tracks ?? Infinity))
    if (maxTracks > 2) {
        failures.push(`memory.tracks reached ${maxTracks} (> 2): dropped video tracks are kept`)
    }
    for (const field of ["channels", "subscriptions"]) {
        if (last.memory?.[field] > first.memory?.[field]) {
            failures.push(`memory.${field} grew from ${first.memory?.[field]} to ${last.memory?.[field]}`)
        }
    }
    const fromRound5 = samples.find((sample) => sample.round >= 5) ?? first
    if (last.rss - fromRound5.rss > rssSlackMb) {
        failures.push(`RSS grew ${(last.rss - fromRound5.rss).toFixed(1)} MB from round ${fromRound5.round} to ${last.round} (> ${rssSlackMb})`)
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
