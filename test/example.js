#!/usr/bin/env -S deno run --allow-all
// The example page (examples/), served by the bridge itself, driven like a user in headless Chrome:
// video, point cloud, depth and a raw stream from fixtures published by a real zenoh peer.
// Needs internet: the page imports the client from esm.sh.
// Usage: deno run --allow-all test/example.js

import { $ } from "https://esm.sh/dax-sh@0.42.0"
import { bridgeDir, check, finish, fixtureKey, fixturesDir, launchBrowser, loadManifest, machineLoad, repoRoot, startBridge, startPeer } from "./harness.js"

const scratch = $.path(await Deno.makeTempDir({ prefix: "zenoh-web-example-" }))
const screenshotPath = repoRoot.join("test/artifacts/example.png")
console.log(`machine load at start: ${await machineLoad()}`)

const manifest = loadManifest()
/** @param {string} file */
const entryFor = (file) => {
    const entry = manifest.entries.find((candidate) => candidate.file === file)
    if (!entry) {
        throw new Error(`no fixture ${file} in the manifest`)
    }
    return entry
}
const streams = [
    { name: "video", file: "dimos/image_rgb8.lcm", codec: "dimos-image" },
    { name: "pointcloud", file: "dimos/pointcloud_xyzi.lcm", codec: "dimos-pointcloud2" },
    { name: "depth", file: "dimos/depth_16UC1.lcm", codec: "dimos-depth" },
].map((stream) => ({ ...stream, key: fixtureKey(entryFor(stream.file)) }))
const rawKey = "example/raw"

try {
    $.logStep("building bridge + test peer (release)")
    await $`cargo build --release --bin zenoh-web --example test_peer`.cwd(bridgeDir)
    const peer = await startPeer([
        ...streams.flatMap((stream) => ["--publish", `${stream.key}=${fixturesDir.join(stream.file)}@10`]),
        "--synthetic", `${rawKey}=1500@20`,
    ])
    const bridge = await startBridge(scratch, peer.zenohPort, repoRoot.join("examples"))
    await $.sleep(1000)

    const browser = await launchBrowser()
    const page = await browser.newPage()
    /** @type {string[]} */
    const consoleErrors = []
    page.addEventListener("console", (event) => {
        if (event.detail.type === "error") {
            consoleErrors.push(event.detail.text)
        }
    })
    page.addEventListener("pageerror", (event) => consoleErrors.push(`uncaught: ${event.detail?.message ?? event.detail}`))
    await page.setViewportSize({ width: 1400, height: 1100 })
    $.logStep(`opening ${bridge.url}/index.html (client from esm.sh)`)
    await page.goto(`${bridge.url}/index.html`)

    const connected = await page.evaluate(async () => {
        for (let attempt = 0; attempt < 150; attempt++) {
            if (document.getElementById("connection-state")?.textContent === "connected") {
                return true
            }
            await new Promise((resolve) => setTimeout(resolve, 100))
        }
        return false
    })
    check(connected, "page loads the client from esm.sh and connects to the bridge")

    $.logStep("listing topics")
    const topics = await page.evaluate(async () => {
        document.getElementById("refresh-topics").click()
        for (let attempt = 0; attempt < 100; attempt++) {
            const keys = [...document.querySelectorAll("#topic-list li button")].map((button) => button.firstChild?.textContent)
            if (keys.length > 0) {
                return keys
            }
            await new Promise((resolve) => setTimeout(resolve, 100))
        }
        return []
    })
    console.log(`topics: ${topics.join(", ")}`)
    check(topics.includes("test/cached"), "Refresh lists topics (test/cached, an AdvancedPublisher)")

    $.logStep("adding streams through the form")
    const added = await page.evaluate((streams, rawKey) => {
        const keyInput = document.getElementById("subscribe-key")
        const codecSelect = document.getElementById("subscribe-codec")
        const guesses = {}
        for (const stream of [...streams, { name: "raw", key: rawKey, codec: "" }]) {
            keyInput.value = stream.key
            keyInput.dispatchEvent(new Event("change"))
            guesses[stream.name] = codecSelect.value
            codecSelect.value = stream.codec
            document.getElementById("subscribe-button").click()
        }
        return { guesses, cards: document.querySelectorAll("#streams .stream").length, error: document.getElementById("subscribe-error").textContent }
    }, { args: [streams, rawKey] })
    console.log(JSON.stringify(added))
    check(added.cards === 4 && added.error === "", "four stream cards added")
    check(streams.every((stream) => added.guesses[stream.name] === stream.codec) && added.guesses.raw === "", "the codec select guesses each key's codec (none for a non-image key)")

    $.logStep("waiting for video frames, points, depth and raw rate")
    const rendered = await page.evaluate(async () => {
        const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
        const card = (output) => document.querySelector(`#streams .stream[data-output="${output}"]`)
        /** brightness and non-black pixel count of a canvas (or a video drawn onto one) */
        const sample = (source, width, height) => {
            if (!width || !height) {
                return { mean: 0, litPixels: 0 }
            }
            const canvas = document.createElement("canvas")
            canvas.width = width
            canvas.height = height
            const context = canvas.getContext("2d", { willReadFrequently: true })
            context.drawImage(source, 0, 0)
            const pixels = context.getImageData(0, 0, width, height).data
            let sum = 0
            let litPixels = 0
            for (let offset = 0; offset < pixels.length; offset += 4) {
                const brightness = pixels[offset] + pixels[offset + 1] + pixels[offset + 2]
                sum += brightness
                if (brightness > 30) {
                    litPixels++
                }
            }
            return { mean: sum / (3 * width * height), litPixels }
        }
        let result = null
        for (let attempt = 0; attempt < 40; attempt++) {
            const video = card("video")?.querySelector("video")
            const pointCanvas = card("pointcloud")?.querySelector("canvas")
            const depthCanvas = card("depth")?.querySelector("canvas")
            result = {
                videoWidth: video?.videoWidth ?? 0,
                videoFrames: video?.getVideoPlaybackQuality?.().totalVideoFrames ?? 0,
                video: video ? sample(video, video.videoWidth, video.videoHeight) : null,
                points: pointCanvas ? sample(pointCanvas, pointCanvas.width, pointCanvas.height) : null,
                depth: depthCanvas ? sample(depthCanvas, depthCanvas.width, depthCanvas.height) : null,
                depthSize: depthCanvas ? `${depthCanvas.width}x${depthCanvas.height}` : null,
                raw: card("raw")?.querySelector(".raw")?.textContent ?? "",
                errors: [...document.querySelectorAll(".stream-error")].map((element) => element.textContent).filter(Boolean),
            }
            const rawHz = parseFloat(result.raw)
            if (result.videoWidth > 0 && result.video.mean > 20 && result.points?.litPixels > 500 && result.depth?.litPixels > 1000 && rawHz > 0) {
                break
            }
            await sleep(500)
        }
        return result
    })
    console.log(JSON.stringify(rendered))
    check(rendered.errors.length === 0, `no stream errors ${JSON.stringify(rendered.errors)}`)
    check(rendered.videoWidth > 0 && rendered.videoFrames > 0, `<video> decodes H.264 frames (videoWidth ${rendered.videoWidth}, ${rendered.videoFrames} frames)`)
    check(rendered.video?.mean > 20, `video frame is not black (mean ${rendered.video?.mean?.toFixed(1)})`)
    check(rendered.points?.litPixels > 500, `point cloud canvas has drawn points (${rendered.points?.litPixels} lit pixels)`)
    check(rendered.depth?.litPixels > 1000 && rendered.depthSize === "320x240", `depth canvas shows the 320x240 depth image (${rendered.depthSize}, ${rendered.depth?.litPixels} lit pixels)`)
    check(parseFloat(rendered.raw) > 0, `raw stream shows its rate and size ("${rendered.raw}")`)

    $.logStep("changing a control re-subscribes with the new options")
    const controlled = await page.evaluate(async () => {
        const card = document.querySelector('#streams .stream[data-output="pointcloud"]')
        const slider = card.querySelector('input[name="maxQuality"]')
        slider.value = "0.5"
        slider.dispatchEvent(new Event("input"))
        slider.dispatchEvent(new Event("change"))
        const stream = [...window.zenohWebExample.streams].find((candidate) => candidate.output === "pointcloud")
        const received = stream.subscription.received
        for (let attempt = 0; attempt < 30 && stream.subscription.received === received; attempt++) {
            await new Promise((resolve) => setTimeout(resolve, 200))
        }
        await new Promise((resolve) => setTimeout(resolve, 1500))
        return { maxQuality: stream.subscription.options.maxQuality, state: stream.subscription.state, received: stream.subscription.received }
    })
    console.log(JSON.stringify(controlled))
    check(controlled.maxQuality === 0.5 && controlled.state === "open" && controlled.received > 0, "maxQuality slider re-subscribes and points keep arriving")

    // the table re-renders once a second; the re-subscription briefly removed a key
    const stats = await page.evaluate(async () => {
        for (let attempt = 0; attempt < 30 && document.querySelectorAll("#key-stats tr").length !== 4; attempt++) {
            await new Promise((resolve) => setTimeout(resolve, 100))
        }
        return {
            rows: document.querySelectorAll("#key-stats tr").length,
            connection: document.getElementById("connection-stats").textContent,
        }
    })
    check(stats.rows === 4 && stats.connection.includes("budget"), "z.stats table and connection stats render")

    await page.screenshot().then((png) => {
        screenshotPath.parentOrThrow().mkdirSync({ recursive: true })
        screenshotPath.writeSync(png)
    })
    console.log(`screenshot: ${screenshotPath}`)
    check(consoleErrors.length === 0, `no console errors ${JSON.stringify(consoleErrors)}`)
} catch (error) {
    console.error(error)
    check(false, `example test threw: ${error.message}`)
}
await finish(scratch.toString())
