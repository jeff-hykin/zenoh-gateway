#!/usr/bin/env -S deno run --allow-all --unstable-net
// A page that knows its link: connect(url, { linkBandwidthBytesPerSec }) makes the gateway budget exactly that (no
// startup, no probes), setLinkBandwidth changes it while connected, and setLinkBandwidth(undefined) hands it back to
// the estimate. Loopback, examples/load_server. Not timing-sensitive: it doesn't wait for other browser tests.
// Usage: deno run --allow-all --unstable-net client/test/link_bandwidth.ts

import { openPage } from "./browser.ts"

const root = new URL("../..", import.meta.url).pathname
const scratch = await Deno.makeTempDir({ prefix: "zenoh-gateway-link-bandwidth-" })
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
const server = new Deno.Command(`${targetDir}/release/examples/load_server`, { args: [scratch], env: { CLOUD_BYTES: "0", MAP_BYTES: "0" }, stdout: "piped", stderr: "inherit" }).spawn()
const url = await (async () => {
    for await (const chunk of server.stdout.pipeThrough(new TextDecoderStream())) {
        const match = chunk.match(/listening on (\S+)/)
        if (match) {
            return match[1]
        }
    }
    throw new Error("load_server exited")
})()

const known = 3_000_000
const changed = 1_500_000
const page = await openPage(`${url}/blank.html`, 60_000)
const failures: string[] = []
try {
    const result = await page.evaluate(async (url: string, known: number, changed: number) => {
        const { connect } = await import("/client/zenoh_gateway.js")
        const client = await connect(url, { linkBandwidthBytesPerSec: known })
        const bandwidth = async () => {
            // (the gateway re-allocates every 250 ms)
            await new Promise((resolve) => setTimeout(resolve, 1000))
            return (await client._request({ op: "stats" }, 5000)).bandwidth
        }
        const atConnect = await bandwidth()
        await client.setLinkBandwidth(changed)
        const afterChange = await bandwidth()
        await client.setLinkBandwidth(undefined)
        const afterReset = await bandwidth()
        client.close()
        return { atConnect, afterChange, afterReset }
    }, [url, known, changed])
    const show = (bandwidth: Record<string, unknown>) => `budget ${bandwidth.budgetBytesPerSec} estimate ${bandwidth.dataEstimateBytesPerSec} gain ${bandwidth.gain} startingUp ${bandwidth.startingUp}`
    console.log(`at connect (${known}): ${show(result.atConnect)}`)
    console.log(`after setLinkBandwidth(${changed}): ${show(result.afterChange)}`)
    console.log(`after setLinkBandwidth(undefined): ${show(result.afterReset)}`)
    for (const [name, bandwidth, want] of [["at connect", result.atConnect, known], ["after setLinkBandwidth", result.afterChange, changed]] as const) {
        if (bandwidth.budgetBytesPerSec !== want || bandwidth.dataEstimateBytesPerSec !== want || bandwidth.gain !== 1 || bandwidth.startingUp !== false) {
            failures.push(`${name}: want budget and estimate ${want} at gain 1, not starting up; got ${show(bandwidth)}`)
        }
    }
    if (result.afterReset.budgetBytesPerSec === changed) {
        failures.push(`after setLinkBandwidth(undefined): budget still the page's ${changed}`)
    }
} finally {
    await page.close()
    server.kill()
}
console.log(failures.length ? `FAIL\n${failures.join("\n")}` : "PASS")
Deno.exit(failures.length ? 1 : 0)
