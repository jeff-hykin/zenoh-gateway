#!/usr/bin/env -S deno run --allow-all
// A browser that disappears without closing anything (laptop lid closed, Wi-Fi gone, crashed) must
// not leave its subscriptions running: the bridge has to notice, drop the peer, and stop transcoding.
// The browser is frozen (SIGSTOP), not killed: a killed one's closed sockets answer with ICMP
// errors that end its channels at once, which a browser on another machine never does.
// Usage: deno run --allow-all test/abandoned.js

import { $ } from "https://esm.sh/dax-sh@0.42.0"
import { launch } from "jsr:@astral/astral@0.5.6"
import { buildAll, check, finish, fixtureKey, fixturesDir, loadManifest, startBridge, startPeer } from "./harness.js"

const scratch = $.path(await Deno.makeTempDir({ prefix: "zenoh-web-abandoned-" }))
const entry = loadManifest().entries.find((candidate) => candidate.file === "dimos/image_rgb8.bin")
const key = fixtureKey(entry)

const webRoot = await buildAll(scratch)
const peer = await startPeer(["--publish", `${key}=${fixturesDir.join(entry.file)}@30`])
const bridge = await startBridge(scratch, peer.zenohPort, webRoot)

$.logStep("a page subscribes to H.264 video, then its browser freezes without closing anything")
// its own Chrome, so freezing it leaves nothing behind to send a goodbye
const chromePids = async () => new Set((await $`pgrep -f ${"Chrome for Testing"}`.noThrow().text()).split("\n").filter(Boolean))
const chromesBefore = await chromePids()
const doomed = await launch({ headless: true, args: ["--no-sandbox", "--autoplay-policy=no-user-gesture-required"] })
const doomedPids = [...await chromePids()].filter((pid) => !chromesBefore.has(pid))
const doomedPage = await doomed.newPage(`${bridge.url}/test/blank.html`)
const subscribed = await doomedPage.evaluate(async (bridgeUrl, key) => {
    const { connect } = await import("/client/zenoh_web.js")
    const client = await connect(bridgeUrl)
    let frames = 0
    const subscription = client.subscribe(key, { codec: "dimos-image" }, () => frames++)
    await subscription.ready()
    await new Promise((resolve) => setTimeout(resolve, 2000))
    return frames
}, { args: [bridge.url, key] })
check(subscribed > 10, `the doomed page receives video (${subscribed} frames in 2 s)`)
const goneBefore = bridge.output.lines.filter((line) => line.includes(": gone")).length
// SIGSTOP the whole browser: no pagehide, no RTCPeerConnection.close(), no DTLS close_notify, no ICMP
const pids = doomedPids
for (const pid of pids) {
    await $`kill -STOP ${pid}`.noThrow().quiet()
}
check(pids.length > 0, `froze the doomed browser (${pids.length} processes)`)

const deadline = Date.now() + 60_000
let dropped = false
while (Date.now() < deadline) {
    dropped = bridge.output.lines.filter((line) => line.includes(": gone")).length > goneBefore
    if (dropped) {
        break
    }
    await $.sleep(1000)
}
check(dropped, `the bridge drops the vanished peer within 60 s`)

// after the drop, nothing may still be encoding for it: the bridge's CPU falls back to idle
await $.sleep(3000)
const bridgePid = (await $`lsof -ti tcp:${new URL(bridge.url).port} -sTCP:LISTEN`.text()).split("\n")[0]
const cpu = Number((await $`ps -o %cpu= -p ${bridgePid}`.noThrow().text()).trim())
const samples = []
for (let index = 0; index < 5; index++) {
    const before = await $`ps -o time= -p ${bridgePid}`.text()
    await $.sleep(1000)
    const after = await $`ps -o time= -p ${bridgePid}`.text()
    const seconds = (text) => text.trim().split(":").reduce((total, part) => total * 60 + Number(part), 0)
    samples.push(seconds(after) - seconds(before))
}
const busy = samples.reduce((sum, value) => sum + value, 0) / samples.length
check(busy < 0.15, `the bridge is idle once the peer is gone (${(busy * 100).toFixed(0)}% of a core, ps %cpu ${cpu})`)

for (const pid of pids) {
    await $`kill -9 ${pid}`.noThrow().quiet()
}
await finish(scratch)
