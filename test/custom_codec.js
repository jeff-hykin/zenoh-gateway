#!/usr/bin/env -S deno run --allow-all
// External codecs end-to-end: bridge/examples/custom_codec.rs embeds the server (library API,
// the application's own zenoh session) with a data codec and a video codec written in Rust; a
// real zenoh peer publishes, headless Chrome subscribes with `codec: "<custom>"`.
// Usage: deno run --allow-all test/custom_codec.js
// ZENOH_WEB_CUSTOM_CODEC_BIN=<path> runs another build of the same program (e.g. a downstream crate's).

import { $ } from "https://esm.sh/dax-sh@0.42.0"
import { bridgeDir, buildAll, check, finish, launchBrowser, machineLoad, startBridge, startPeer } from "./harness.js"

const scratch = $.path(await Deno.makeTempDir({ prefix: "zenoh-web-custom-codec-" }))
console.log(`machine load at start: ${await machineLoad()}`)

const text = "hello from zenoh, ünïcode too"
const swatch = [230, 120, 20]

try {
    const otherBinary = Deno.env.get("ZENOH_WEB_CUSTOM_CODEC_BIN")
    const webRoot = await buildAll(scratch, otherBinary ? [] : ["custom_codec"])
    const binary = otherBinary ?? bridgeDir.join("target/release/examples/custom_codec").toString()
    scratch.join("text.txt").writeTextSync(text)
    scratch.join("swatch.rgb").writeSync(new Uint8Array(swatch))
    const peer = await startPeer(["--publish", `demo/text=${scratch.join("text.txt")}@10`, "--publish", `demo/swatch=${scratch.join("swatch.rgb")}@10`])
    $.logStep(`starting ${binary}`)
    const bridge = await startBridge(scratch, peer.zenohPort, webRoot, [], binary)
    await $.sleep(1000)
    const browser = await launchBrowser()
    const page = await browser.newPage(`${bridge.url}/test/blank.html`)

    const result = await page.evaluate(async (bridgeUrl) => {
        const { connect, registerCodec } = await import("/client/zenoh_web.js")
        const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
        const out = {}
        const client = await connect(bridgeUrl)
        out.codecs = client.codecs.filter((codec) => !codec.name.startsWith("ros2-") && !codec.name.startsWith("dimos-"))
        out.builtinCount = client.codecs.length - out.codecs.length

        // data codec: the page supplies the decoder
        registerCodec("text-uppercase", (bytes) => new TextDecoder().decode(bytes))
        const firstMessage = (options) => new Promise((resolve) => {
            const timer = setTimeout(() => resolve({ error: "no message in 8 s" }), 8000)
            const subscription = client.subscribe("demo/text", options, (message) => {
                clearTimeout(timer)
                subscription.close()
                resolve({ decoded: message.decoded, bytes: message.bytes.length })
            })
            subscription.ready().catch((error) => resolve({ error: error.message }))
        })
        out.full = await firstMessage({ codec: "text-uppercase" })
        out.reduced = await firstMessage({ codec: "text-uppercase", maxQuality: 0.5 })
        try {
            registerCodec("text-uppercase", () => "another")
            out.duplicateDecoder = null
        } catch (error) {
            out.duplicateDecoder = error.message
        }

        // video codec: no page code, frames arrive on the subscription's MediaStream
        const frames = []
        const video = client.subscribe("demo/swatch", { codec: "rgb-swatch" }, (message) => frames.push(message.video))
        try {
            await video.ready()
            const element = document.createElement("video")
            element.muted = true
            element.playsInline = true
            element.srcObject = video.mediaStream
            document.body.append(element)
            await element.play().catch(() => {})
            for (let index = 0; index < 4; index++) {
                await Promise.race([new Promise((resolve) => element.requestVideoFrameCallback(resolve)), sleep(3000)])
            }
            const canvas = document.createElement("canvas")
            canvas.width = element.videoWidth
            canvas.height = element.videoHeight
            const context = canvas.getContext("2d", { willReadFrequently: true })
            context.drawImage(element, 0, 0)
            const { width, height } = canvas
            const pixels = width && height ? context.getImageData(0, 0, width, height).data : new Uint8ClampedArray()
            const sums = [0, 0, 0]
            let count = 0
            for (let offset = 0; offset < pixels.length; offset += 4) {
                sums[0] += pixels[offset]
                sums[1] += pixels[offset + 1]
                sums[2] += pixels[offset + 2]
                count++
            }
            await client.pollStats()
            out.video = { width, height, mean: sums.map((sum) => Math.round(sum / Math.max(1, count))), frames: frames.length, metadata: frames.at(-1) ?? null, decodeErrors: video.decodeErrors, codecErrors: video.bridgeStats?.stats?.codecErrors }
            element.remove()
        } catch (error) {
            out.video = { error: error.message }
        }
        video.close()

        // unknown names: the client lists the bridge's codecs, the bridge refuses one the client let through
        try {
            client.subscribe("demo/text", { codec: "text-lowercase" }, () => {})
            out.clientError = null
        } catch (error) {
            out.clientError = error.message
        }
        try {
            client.subscribe("demo/swatch", { codec: "rgb-swatch", delivery: "reliable" }, () => {})
            out.reliableVideoError = null
        } catch (error) {
            out.reliableVideoError = error.message
        }
        const raw = client._peer.createDataChannel(JSON.stringify({ type: "sub", key: "demo/text", id: 999999, opts: { codec: "text-lowercase" } }))
        out.rawClosed = await new Promise((resolve) => {
            raw.onclose = () => resolve(true)
            setTimeout(() => resolve(false), 5000)
        })
        client.close()
        return out
    }, { args: [bridge.url] })
    console.log(JSON.stringify(result))

    check(JSON.stringify(result.codecs) === JSON.stringify([{ name: "rgb-swatch", output: "video" }, { name: "text-uppercase", output: "data" }]) && result.builtinCount === 10,
        `client sees the external codecs next to the 10 built-ins (${JSON.stringify(result.codecs)})`)
    const upper = text.toUpperCase()
    check(result.full.decoded === upper, `data codec: registered decoder got "${result.full.decoded}" (expected "${upper}")`)
    const half = [...upper].slice(0, Math.ceil([...upper].length * 0.5)).join("")
    check(result.reduced.decoded === half, `data codec at maxQuality 0.5: "${result.reduced.decoded}" (expected "${half}")`)
    check(result.duplicateDecoder?.includes("already registered"), `client: a second decoder for the same codec throws (${result.duplicateDecoder})`)
    const video = result.video
    const worst = video.error ? Infinity : Math.max(...video.mean.map((value, channel) => Math.abs(value - swatch[channel])))
    check(!video.error && video.width === 64 && video.height === 48 && worst <= 20 && video.frames > 0 && video.decodeErrors === 0 && video.codecErrors === 0,
        `video codec (I420 frames): ${video.width}x${video.height}, mean color ${JSON.stringify(video.mean)} within 20 of ${JSON.stringify(swatch)} (${video.frames} frames${video.error ? `, error ${video.error}` : ""})`)
    check(video.metadata?.sourceWidth === 64 && video.metadata?.sourceHeight === 48, `video codec: per-frame metadata (${JSON.stringify(video.metadata)})`)
    check(result.clientError?.includes("unknown codec") && result.clientError.includes("text-uppercase") && result.clientError.includes("rgb-swatch"), `client: unknown codec names the bridge's codecs (${result.clientError})`)
    check(result.reliableVideoError?.includes("video codec"), `client: reliable delivery with an external video codec throws (${result.reliableVideoError})`)
    const bridgeRefusal = await bridge.output.waitFor((line) => line.includes("unknown codec") && line.includes("text-uppercase"), 3000).catch(() => null)
    check(result.rawClosed && bridgeRefusal !== null, `bridge: unknown codec refused, listing the external codecs (${bridgeRefusal?.replace(/.*rejected/, "rejected")})`)
} catch (error) {
    check(false, String(error))
    console.error(error)
}
await finish(scratch.toString())
