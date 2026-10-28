#!/usr/bin/env -S deno run --allow-all --unstable-net
// Microphone publishing, pass/fail: headless Chrome's fake microphone (a beep) published with `publishAudio` through the
// gateway's examples/mic_server, whose plain zenoh subscriber on mic/** reports every message. The page's UDP goes
// through a relay that can hold the page's media to the gateway back, so the audio arrives late by a known amount.
// - no delay: messages arrive at the asked rate as whole 20 ms packets, carry sound, stop when the publisher closes; the
//   clock-synced age (capture on the page's clock, from its RTCP sender reports, to arrival) stays under latencyLimit;
//   an unknown encoding is rejected;
// - DELAY_MS late, no limit: the measured age is the delay (the clocks agree);
// - DELAY_MS late, latencyLimit under it: every packet aged through the clock sync is dropped (only the unsynced ones
//   before the first sender report and clock offset go out: Chrome sends its first one ~2.5 s in).
// Usage: deno run --allow-all --unstable-net client/test/microphone.ts [seconds per phase]

import { launch } from "jsr:@astral/astral@0.5.6"

const seconds = Number(Deno.args[0] ?? 8)
const sampleRate = 16000
const latencyLimitMs = 150
const delayMs = Number(Deno.env.get("DELAY_MS") ?? 400)
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

/** Chrome <-> relay <-> the server's ICE port; the page's media to the gateway held back `upDelayMs` (order kept). */
const sockets: Deno.DatagramConn[] = []
function startRelay(targetPort: number, upDelayMs: number) {
    const chromeSide = Deno.listenDatagram({ transport: "udp", hostname: "127.0.0.1", port: 0 })
    const serverSide = Deno.listenDatagram({ transport: "udp", hostname: "127.0.0.1", port: 0 })
    sockets.push(chromeSide, serverSide)
    let chromeAddress: Deno.NetAddr | null = null
    ;(async () => {
        for await (const [data, address] of chromeSide) {
            chromeAddress = address as Deno.NetAddr
            const forward = () => serverSide.send(data, { transport: "udp", hostname: "127.0.0.1", port: targetPort }).catch(() => {})
            // only the media is late (SRTP/SRTCP: first byte 128..191, RFC 7983); ICE, DTLS and the data channels aren't,
            // so the connection still comes up within its timeouts
            if (data[0] >= 128 && data[0] <= 191) {
                setTimeout(forward, upDelayMs)
            } else {
                forward()
            }
        }
    })().catch(() => {})
    ;(async () => {
        for await (const [data] of serverSide) {
            if (chromeAddress) {
                chromeSide.send(data, chromeAddress).catch(() => {})
            }
        }
    })().catch(() => {})
    return (chromeSide.addr as Deno.NetAddr).port
}

// serves the page and signalling, routing ICE through the relay (the offer loses its candidates, the answer's become
// the relay's port); each connection gets the delay set when it offers
let upDelayMs = 0
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
    const port = startRelay(Number(loopback?.split(" ")[5]), upDelayMs)
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
const proxyUrl = `http://127.0.0.1:${proxy.addr.port}`

type PhaseResult = { state: string; stats: Record<string, number | null>; bogusState: string | null; bogusError: string | null; closedAgoMs: number }
const browser = await launch({ headless: true, args: ["--no-sandbox", "--use-fake-device-for-media-stream", "--use-fake-ui-for-media-stream"] })
const failures: string[] = []
try {
    // http://127.0.0.1 is a secure context, so getUserMedia works without TLS
    const page = await browser.newPage(`${proxyUrl}/blank.html`)
    const phase = async (key: string, latencyLimit: number | null, tryBogus: boolean): Promise<PhaseResult> =>
        (await page.evaluate(async (url: string, key: string, seconds: number, sampleRate: number, latencyLimit: number | null, tryBogus: boolean) => {
            const { connect } = await import("/client/zenoh_gateway.js")
            const client = await connect(url)
            // renegotiations go over the control channel, past the relay: without their candidates ICE keeps the relayed
            // path (with them it moved to the direct one and the delay vanished)
            const request = client._request.bind(client)
            const noCandidates = (sdp: { type?: string; sdp?: string }) => ({ ...sdp, sdp: sdp.sdp?.split("\r\n").filter((line) => !line.startsWith("a=candidate")).join("\r\n") })
            client._request = async (message: { op: string; sdp?: { type?: string; sdp?: string } }, timeoutMs: number) => {
                const response = await request(message.op === "renegotiate" && message.sdp ? { ...message, sdp: noCandidates(message.sdp) } : message, timeoutMs)
                return message.op === "renegotiate" && response.sdp ? { ...response, sdp: noCandidates(response.sdp) } : response
            }
            // the page's navigator (Deno's types have no mediaDevices)
            const media = (navigator as unknown as { mediaDevices: { getUserMedia(constraints: { audio: boolean }): Promise<{ getAudioTracks(): { clone(): unknown }[] }> } }).mediaDevices
            const track = (await media.getUserMedia({ audio: true })).getAudioTracks()[0]
            const mic = client.publishAudio(key, track, { encoding: "test-pcm", sampleRate, ...(latencyLimit === null ? {} : { latencyLimit }) })
            await mic.ready()
            let bogusState = null
            let bogusError = null
            if (tryBogus) {
                const bogus = client.publishAudio("mic/bogus", track.clone(), { encoding: "no-such-encoding" })
                bogusError = await bogus.ready().then(() => null, (error: Error) => error.message)
                bogusState = bogus.state
            }
            await new Promise((resolve) => setTimeout(resolve, seconds * 1000))
            const response = await client._request({ op: "stats" }, 5000)
            const channel = (response.channels as { id: number; stats: Record<string, number | null> }[]).find((channel) => channel.id === mic.id)
            const state = mic.state
            mic.close()
            const closedAt = performance.now()
            await new Promise((resolve) => setTimeout(resolve, 1000))
            client.close()
            return { state, stats: channel?.stats ?? {}, bogusState, bogusError, closedAgoMs: performance.now() - closedAt }
        }, { args: [proxyUrl, key, seconds, sampleRate, latencyLimit, tryBogus] })) as unknown as PhaseResult
    const describe = (name: string, result: PhaseResult, heard: number) =>
        console.log(`${name}: ${result.state}; ${heard} messages in ${seconds} s; gateway ${JSON.stringify(result.stats)}`)

    // no delay
    upDelayMs = 0
    const near = await phase("mic/near", latencyLimitMs, true)
    const closedAt = performance.now() - near.closedAgoMs
    const heard = reports.filter((report) => report.key === "mic/near")
    describe(`no delay, latencyLimit ${latencyLimitMs}`, near, heard.length)
    console.log(`  unknown encoding: ${near.bogusState} (${near.bogusError})`)
    if (near.state !== "open") {
        failures.push(`publisher state ${near.state}, expected open`)
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
    const nearAge = near.stats.lastAgeMs
    if (typeof nearAge !== "number" || nearAge < 0 || nearAge > latencyLimitMs) {
        failures.push(`no delay: clock-synced age ${nearAge}, expected 0..${latencyLimitMs} ms`)
    }
    if ((near.stats.droppedStale ?? 0) > (near.stats.received ?? 0) * 0.05) {
        failures.push(`no delay: ${near.stats.droppedStale} of ${near.stats.received} dropped as stale`)
    }
    if (near.bogusState !== "rejected" || !String(near.bogusError).includes("no-such-encoding")) {
        failures.push(`an unknown encoding wasn't rejected: ${near.bogusState} ${near.bogusError}`)
    }
    if (reports.some((report) => report.key === "mic/bogus")) {
        failures.push("the rejected publisher put messages")
    }

    // late, no limit: the age is the delay
    upDelayMs = delayMs
    const late = await phase("mic/late", null, false)
    describe(`${delayMs} ms late, no limit`, late, reports.filter((report) => report.key === "mic/late").length)
    const lateAge = late.stats.lastAgeMs
    if (typeof lateAge !== "number" || lateAge < delayMs || lateAge > delayMs + latencyLimitMs) {
        failures.push(`${delayMs} ms late: clock-synced age ${lateAge}, expected ${delayMs}..${delayMs + latencyLimitMs} ms`)
    }

    // late, limit under the delay: every synced packet dropped
    const limited = await phase("mic/limited", latencyLimitMs, false)
    describe(`${delayMs} ms late, latencyLimit ${latencyLimitMs}`, limited, reports.filter((report) => report.key === "mic/limited").length)
    const { put = 0, unsynced = 0, droppedStale = 0 } = limited.stats as Record<string, number>
    if (droppedStale === 0 || put > unsynced) {
        failures.push(`${delayMs} ms late with latencyLimit ${latencyLimitMs}: ${put} put (${unsynced} unsynced), ${droppedStale} dropped stale; expected only unsynced ones put`)
    }
} finally {
    await browser.close()
    await proxy.shutdown()
    for (const socket of sockets) {
        socket.close()
    }
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
