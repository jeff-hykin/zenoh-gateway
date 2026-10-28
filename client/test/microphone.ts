#!/usr/bin/env -S deno run --allow-all
// Microphone publishing, pass/fail: headless Chrome's fake microphone (a beep) published with `publishAudio` through the
// gateway's examples/mic_server, whose plain zenoh subscriber on mic/** reports every message. Checks the messages
// arrive at the asked rate as whole 20 ms packets, carry sound, and stop when the publisher closes; and that an unknown
// encoding is rejected.
// Usage: deno run --allow-all client/test/microphone.ts [seconds]

import { launch } from "jsr:@astral/astral@0.5.6"

const seconds = Number(Deno.args[0] ?? 5)
const sampleRate = 16000
const root = new URL("../..", import.meta.url).pathname
const scratch = await Deno.makeTempDir({ prefix: "zenoh-gateway-microphone-" })
await Deno.mkdir(`${scratch}/client`)
await Deno.writeTextFile(`${scratch}/blank.html`, `<!doctype html><html><head><meta charset="utf-8"></head><body></body></html>`)
const bundled = await new Deno.Command(Deno.execPath(), { args: ["bundle", "--quiet", "--platform", "browser", "-o", `${scratch}/client/zenoh_gateway.js`, `${root}client/zenoh_gateway.ts`] }).output()
if (bundled.code !== 0) {
    throw new Error(`deno bundle failed: ${new TextDecoder().decode(bundled.stderr)}`)
}
const build = await new Deno.Command("cargo", { args: ["build", "--release", "--example", "mic_server"], cwd: `${root}gateway`, stderr: "inherit" }).output()
if (build.code !== 0) {
    throw new Error("cargo build failed")
}
const targetDir = Deno.env.get("CARGO_TARGET_DIR") ?? `${root}gateway/target`
const server = new Deno.Command(`${targetDir}/release/examples/mic_server`, { args: [scratch], stdout: "piped", stderr: "inherit" }).spawn()

type Report = { key: string; sampleRate?: number; channels?: number; samples?: number; rms?: number; error?: string; at: number }
const reports: Report[] = []
const lines = server.stdout.pipeThrough(new TextDecoderStream()).getReader()
let buffered = ""
let serverUrl = ""
const reading = (async () => {
    while (true) {
        const { value, done } = await lines.read()
        if (done) {
            return
        }
        buffered += value
        const complete = buffered.split("\n")
        buffered = complete.pop()!
        for (const line of complete) {
            const listening = line.match(/listening on (\S+)/)
            if (listening) {
                serverUrl = listening[1]
            } else if (line.startsWith("{")) {
                reports.push({ ...JSON.parse(line), at: performance.now() })
            }
        }
    }
})()
while (!serverUrl) {
    await new Promise((resolve) => setTimeout(resolve, 50))
}

const browser = await launch({ headless: true, args: ["--no-sandbox", "--use-fake-device-for-media-stream", "--use-fake-ui-for-media-stream"] })
const failures: string[] = []
try {
    // http://127.0.0.1 is a secure context, so getUserMedia works without TLS
    const page = await browser.newPage(`${serverUrl}/blank.html`)
    const result = await page.evaluate(async (url: string, seconds: number, sampleRate: number) => {
        const { connect } = await import("/client/zenoh_gateway.js")
        const client = await connect(url)
        // the page's navigator (Deno's types have no mediaDevices)
        const media = (navigator as unknown as { mediaDevices: { getUserMedia(constraints: { audio: boolean }): Promise<{ getAudioTracks(): { clone(): unknown }[] }> } }).mediaDevices
        const track = (await media.getUserMedia({ audio: true })).getAudioTracks()[0]
        const mic = client.publishAudio("mic/test", track, { encoding: "test-pcm", sampleRate })
        await mic.ready()
        const bogus = client.publishAudio("mic/bogus", track.clone(), { encoding: "no-such-encoding" })
        const bogusError = await bogus.ready().then(() => null, (error: Error) => error.message)
        await new Promise((resolve) => setTimeout(resolve, seconds * 1000))
        const openState = mic.state
        mic.close()
        const closedAt = performance.now()
        await new Promise((resolve) => setTimeout(resolve, 1000))
        client.close()
        return { openState, bogusError, bogusState: bogus.state, closedAgoMs: performance.now() - closedAt }
    }, { args: [serverUrl, seconds, sampleRate] })
    const closedAt = performance.now() - result.closedAgoMs
    const heard = reports.filter((report) => report.key === "mic/test")
    console.log(`publisher ${result.openState}; ${heard.length} messages in ${seconds} s; bogus: ${result.bogusState} (${result.bogusError})`)
    if (result.openState !== "open") {
        failures.push(`publisher state ${result.openState}, expected open`)
    }
    // 20 ms packets: 50 a second; allow for the track starting late
    const expected = seconds * 50
    if (heard.length < expected * 0.6) {
        failures.push(`${heard.length} messages, expected about ${expected}`)
    }
    const errors = heard.filter((report) => report.error)
    if (errors.length) {
        failures.push(`${errors.length} unreadable messages: ${errors[0].error}`)
    }
    const wrongShape = heard.filter((report) => report.sampleRate !== sampleRate || report.samples !== (sampleRate / 50) * (report.channels ?? 1))
    if (wrongShape.length) {
        failures.push(`${wrongShape.length} messages aren't 20 ms at ${sampleRate} Hz, e.g. ${JSON.stringify(wrongShape[0])}`)
    }
    const loudest = Math.max(0, ...heard.map((report) => report.rms ?? 0))
    if (loudest < 100) {
        failures.push(`silent: loudest packet rms ${loudest.toFixed(1)}`)
    }
    // a packet already on its way may land just after close
    const afterClose = heard.filter((report) => report.at > closedAt + 500)
    if (afterClose.length) {
        failures.push(`${afterClose.length} messages more than 500 ms after close`)
    }
    if (result.bogusState !== "rejected" || !String(result.bogusError).includes("no-such-encoding")) {
        failures.push(`an unknown encoding wasn't rejected: ${result.bogusState} ${result.bogusError}`)
    }
    if (reports.some((report) => report.key === "mic/bogus")) {
        failures.push("the rejected publisher put messages")
    }
} finally {
    await browser.close()
    server.kill("SIGINT")
    await server.status
    await reading.catch(() => {})
    await Deno.remove(scratch, { recursive: true })
}
if (failures.length) {
    console.error(`FAIL\n- ${failures.join("\n- ")}`)
    Deno.exit(1)
}
console.log("PASS")
