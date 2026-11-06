#!/usr/bin/env -S deno run --allow-all --unstable-net
// A page that closes and reconnects within the gateway's link memory starts from its link's last estimate (resumeLink):
// on a 1 MB/s shaped link with the degrade_server's clouds, the first connection measures the link, closes, and a second
// one with the same linkId must be past startup at its first stats with an estimate near the first's; one with a new
// linkId starts from the first guess as ever. Fails otherwise.
// Usage: deno run --allow-all --unstable-net client/test/link_seed.ts   (BROWSER=firefox for Firefox)

/// <reference lib="dom" />
/// <reference lib="deno.ns" />
import { openPage, waitForOtherBrowserTests } from "./browser.ts"
import { shapedLink } from "./shaped_link.ts"

await waitForOtherBrowserTests()
const root = new URL("../..", import.meta.url).pathname
const scratch = await Deno.makeTempDir({ prefix: "zenoh-gateway-link-seed-" })
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
const linkBytesPerSec = 1_000_000

const server = new Deno.Command(`${targetDir}/release/examples/degrade_server`, { args: [scratch], env: { CAMS: "0", CLOUD_BYTES: "100000" }, stdout: "piped", stderr: "null" }).spawn()
const serverUrl = await (async () => {
    for await (const chunk of server.stdout.pipeThrough(new TextDecoderStream())) {
        const match = chunk.match(/listening on (\S+)/)
        if (match) {
            return match[1]
        }
    }
    throw new Error("degrade_server exited")
})()
const link = shapedLink(serverUrl, { schedule: [[0, linkBytesPerSec]], delayMs: 20, jitterMs: 0, loss: 0 })
const page = await openPage(`${link.url}/blank.html`, 120_000)
const failures: string[] = []
try {
    const result = await page.evaluate(async (url: string) => {
        const { connect } = await import("/client/zenoh_gateway.js")
        // the gateway's link stats, read as soon as the connection is up and again after `seconds`
        const run = async (linkId: string, seconds: number) => {
            const client: any = await connect(url, { linkId, statsIntervalMs: 250 })
            for (const key of ["robot/cloud0", "robot/cloud1"]) {
                client.subscribe(key, { delivery: "latest", encoding: "test-cloud", minQuality: 0.0625 }, () => {})
            }
            // (a few allocation rounds in: before the first, the stats are the defaults)
            await new Promise((resolve) => setTimeout(resolve, 1000))
            await client.pollStats()
            const first = client.gatewayStats?.bandwidth ?? null
            await new Promise((resolve) => setTimeout(resolve, seconds * 1000))
            await client.pollStats()
            const last = client.gatewayStats?.bandwidth ?? null
            client.close()
            return { first, last }
        }
        const measured = await run("link-seed-test", 15)
        // (the gateway keeps the estimate once the closed connection is dropped)
        await new Promise((resolve) => setTimeout(resolve, 3000))
        const resumed = await run("link-seed-test", 1)
        const fresh = await run(`link-seed-test-${crypto.randomUUID()}`, 1)
        return { measured, resumed, fresh }
    }, [link.url])
    const kb = (bytes: number | undefined) => `${Math.round((bytes ?? NaN) / 1000)} KB/s`
    const { measured, resumed, fresh } = result as any
    console.log(`measured: ${kb(measured.last?.dataEstimateBytesPerSec)} after 15 s (starting up ${measured.last?.startingUp})`)
    console.log(`resumed:  first stats ${kb(resumed.first?.dataEstimateBytesPerSec)} starting up ${resumed.first?.startingUp}`)
    console.log(`fresh id: first stats ${kb(fresh.first?.dataEstimateBytesPerSec)} starting up ${fresh.first?.startingUp}`)
    const learned = measured.last?.dataEstimateBytesPerSec ?? 0
    if (!(learned > 0.5 * linkBytesPerSec) || measured.last?.startingUp) {
        failures.push(`the first connection didn't measure the ${kb(linkBytesPerSec)} link (${kb(learned)})`)
    }
    if (resumed.first?.startingUp !== false || !(resumed.first?.dataEstimateBytesPerSec >= 0.5 * learned)) {
        failures.push(`the reconnect didn't start from the last estimate: ${kb(resumed.first?.dataEstimateBytesPerSec)}, starting up ${resumed.first?.startingUp}`)
    }
    if (fresh.first?.startingUp !== true) {
        failures.push(`a new link id was seeded too (starting up ${fresh.first?.startingUp})`)
    }
} finally {
    await page.close()
    server.kill("SIGTERM")
    await server.status
    await link.close()
}
if (failures.length) {
    console.log(`FAIL\n${failures.join("\n")}`)
    Deno.exit(1)
}
console.log("PASS")
Deno.exit(0)
