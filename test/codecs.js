#!/usr/bin/env -S deno run --allow-all
// Codecs end-to-end: the fixtures' exact bytes are published by a real zenoh peer, transcoded by
// the bridge and decoded in headless Chrome (H.264 video tracks, lossless depth, point clouds).
// Usage: deno run --allow-all test/codecs.js

import { $ } from "https://esm.sh/dax-sh@0.42.0"
import { buildAll, check, finish, fixtureKey, fixturesDir, launchBrowser, loadManifest, machineLoad, startBridge, startPeer } from "./harness.js"

const scratch = $.path(await Deno.makeTempDir({ prefix: "zenoh-web-codecs-" }))
console.log(`machine load at start: ${await machineLoad()}`)

/**
 * The codec a fixture is checked with, and what the page checks.
 * @param {import("./harness.js").FixtureEntry} entry
 * @returns {{ codec: string, kind: "video" | "depth" | "pointcloud" }[]}
 */
function plansFor(entry) {
    const protocol = entry.protocol === "dimos" ? "dimos" : "ros2"
    const type = entry.msg_type.split(/[./]/).pop()
    if (type === "PointCloud2") {
        return [{ codec: `${protocol}-pointcloud2`, kind: "pointcloud" }]
    }
    if (type === "CompressedImage") {
        return entry.file.includes("depth") ? [{ codec: `${protocol}-compressed-depth`, kind: "depth" }] : [{ codec: `${protocol}-compressed-image`, kind: "video" }]
    }
    if (entry.encoding === "16UC1" || entry.encoding === "32FC1") {
        return [{ codec: `${protocol}-depth`, kind: "depth" }]
    }
    // mono16 is ambiguous (depth-like IR or plain gray): checked both ways
    if (entry.encoding === "mono16") {
        return [{ codec: `${protocol}-depth`, kind: "depth" }, { codec: `${protocol}-image`, kind: "video" }]
    }
    return [{ codec: `${protocol}-image`, kind: "video" }]
}

/**
 * Expected quadrant means (TL, TR, BL, BR) as RGB for a video fixture.
 * @param {import("./harness.js").FixtureEntry} entry
 */
function expectedQuadrants(entry) {
    const order = ["top_left", "top_right", "bottom_left", "bottom_right"]
    const expected = entry.expected ?? {}
    if (expected.nominal_quadrant_rgb) {
        return order.map((name) => expected.nominal_quadrant_rgb[name])
    }
    if (expected.quadrant_mean_rgb) {
        return order.map((name) => expected.quadrant_mean_rgb[name])
    }
    // gray: mono16 shows its top 8 bits
    const scale = entry.encoding === "mono16" ? 1 / 257 : 1
    return order.map((name) => {
        const value = Math.round(expected.quadrant_mean[name] * scale)
        return [value, value, value]
    })
}

const manifest = loadManifest()
const cases = manifest.entries.flatMap((entry) => plansFor(entry).map((plan) => ({
    ...plan,
    file: entry.file,
    key: fixtureKey(entry),
    encoding: entry.encoding ?? null,
    quadrants: plan.kind === "video" ? expectedQuadrants(entry) : null,
    tolerance: 10,
})))

try {
    const webRoot = await buildAll(scratch)
    const keys = [...new Map(cases.map((testCase) => [testCase.key, testCase.file])).entries()]
    $.logStep(`starting test peer publishing ${keys.length} fixtures at 10 Hz`)
    const peer = await startPeer(keys.flatMap(([key, file]) => ["--publish", `${key}=${fixturesDir.join(file)}@10`]))
    const bridge = await startBridge(scratch, peer.zenohPort, webRoot)
    await $.sleep(1000)
    const browser = await launchBrowser()
    const page = await browser.newPage(`${bridge.url}/test/blank.html`)

    $.logStep(`video: ${cases.filter((c) => c.kind === "video").length} subscriptions, H.264 over video tracks`)
    const video = await page.evaluate(async (bridgeUrl, cases) => {
        const { connect } = await import("/client/zenoh_web.js")
        const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
        const client = await connect(bridgeUrl)
        const results = await Promise.all(cases.map(async (testCase) => {
            const frames = []
            const subscription = client.subscribe(testCase.key, { codec: testCase.codec, maxHz: 10 }, (message) => frames.push(message.video))
            const outcome = { file: testCase.file, codec: testCase.codec }
            try {
                await subscription.ready()
            } catch (error) {
                return { ...outcome, error: error.message }
            }
            const element = document.createElement("video")
            element.muted = true
            element.playsInline = true
            element.srcObject = subscription.mediaStream
            document.body.append(element)
            await element.play().catch(() => {})
            // a few decoded frames, so the picture has settled after the keyframe
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
            // interior of each quadrant: skip a margin at the quadrant edges (chroma subsampling, blocks)
            const means = [[0, 0], [1, 0], [0, 1], [1, 1]].map(([column, row]) => {
                const sums = [0, 0, 0]
                let count = 0
                for (let y = Math.round(height * (row * 0.5 + 0.1)); y < Math.round(height * (row * 0.5 + 0.4)); y++) {
                    for (let x = Math.round(width * (column * 0.5 + 0.1)); x < Math.round(width * (column * 0.5 + 0.4)); x++) {
                        const offset = (y * width + x) * 4
                        sums[0] += pixels[offset]
                        sums[1] += pixels[offset + 1]
                        sums[2] += pixels[offset + 2]
                        count++
                    }
                }
                return sums.map((sum) => Math.round(sum / Math.max(1, count)))
            })
            await client.pollStats()
            const stats = subscription.bridgeStats?.stats
            subscription.close()
            element.remove()
            return { ...outcome, width, height, means, metadata: frames.at(-1) ?? null, frames: frames.length, decodeErrors: subscription.decodeErrors, keyframes: stats?.keyframes, codecErrors: stats?.codecErrors, lastCodecError: stats?.lastCodecError }
        }))
        client.close()
        return results
    }, { args: [bridge.url, cases.filter((c) => c.kind === "video")] })

    for (const result of video) {
        const testCase = cases.find((c) => c.file === result.file && c.codec === result.codec)
        const label = `${result.codec} ${result.file}`
        if (result.error) {
            check(false, `${label}: ${result.error}`)
            continue
        }
        const worst = Math.max(...result.means.flatMap((mean, quadrant) => mean.map((value, channel) => Math.abs(value - testCase.quadrants[quadrant][channel]))))
        check(result.width === 320 && result.height === 240 && worst <= testCase.tolerance,
            `${label}: video ${result.width}x${result.height}, quadrant means ${JSON.stringify(result.means)} within ${testCase.tolerance} of ${JSON.stringify(testCase.quadrants)} (worst ${worst}; ${result.frames} frames, keyframes ${result.keyframes}, codec errors ${result.codecErrors}${result.lastCodecError ? ` "${result.lastCodecError}"` : ""})`)
        check(result.metadata?.width === 320 && result.metadata?.sourceWidth === 320 && result.decodeErrors === 0, `${label}: per-frame metadata (${JSON.stringify(result.metadata)})`)
    }

    $.logStep("depth: lossless over the data channel")
    const depthCases = cases.filter((c) => c.kind === "depth")
    const depth = await page.evaluate(async (bridgeUrl, cases) => {
        const { connect } = await import("/client/zenoh_web.js")
        const client = await connect(bridgeUrl)
        // value at (column, row) of each fixture, from manifest.json's "pattern"
        const formulas = {
            "16UC1": (x, y) => 1000 + x + 4 * y,
            "32FC1": (x, y) => 0.5 + x / 128 + y / 64,
            mono16: (x, y) => [[0, 21845], [43690, 65535]][y < 120 ? 0 : 1][x < 160 ? 0 : 1],
        }
        const firstMessage = (key, options) => new Promise((resolve) => {
            const timer = setTimeout(() => resolve({ error: "no message in 8 s" }), 8000)
            const subscription = client.subscribe(key, options, (message) => {
                clearTimeout(timer)
                subscription.close()
                resolve({ depth: message.depth, bytes: message.bytes.length })
            })
            subscription.ready().catch((error) => resolve({ error: error.message }))
        })
        const results = []
        for (const testCase of cases) {
            for (const maxQuality of [1, 0.5]) {
                const { depth, bytes, error } = await firstMessage(testCase.key, { codec: testCase.codec, maxQuality })
                if (error) {
                    results.push({ file: testCase.file, codec: testCase.codec, maxQuality, error })
                    continue
                }
                const formula = formulas[testCase.encoding ?? "16UC1"]
                let mismatches = 0
                let firstMismatch = null
                for (let y = 0; y < depth.height; y++) {
                    for (let x = 0; x < depth.width; x++) {
                        const expected = formula(x * depth.stride, y * depth.stride)
                        const actual = depth.data[y * depth.width + x]
                        if (actual !== expected) {
                            mismatches++
                            firstMismatch ??= { x, y, expected, actual }
                        }
                    }
                }
                results.push({ file: testCase.file, codec: testCase.codec, maxQuality, width: depth.width, height: depth.height, stride: depth.stride, encoding: depth.encoding, arrayType: depth.data.constructor.name, mismatches, firstMismatch, bytes })
            }
        }
        client.close()
        return results
    }, { args: [bridge.url, depthCases] })
    for (const result of depth) {
        const label = `${result.codec} ${result.file} maxQuality ${result.maxQuality}`
        if (result.error) {
            check(false, `${label}: ${result.error}`)
            continue
        }
        const stride = result.maxQuality === 1 ? 1 : 2
        const expectedType = result.encoding === "32FC1" ? "Float32Array" : "Uint16Array"
        check(result.stride === stride && result.width === 320 / stride && result.height === 240 / stride && result.mismatches === 0 && result.arrayType === expectedType,
            `${label}: ${result.width}x${result.height} ${result.encoding} ${result.arrayType}, every value exact (${result.mismatches} mismatches${result.firstMismatch ? ` e.g. ${JSON.stringify(result.firstMismatch)}` : ""}), ${result.bytes} bytes`)
    }

    $.logStep("point clouds: int16 quantized over the data channel")
    const cloudCases = cases.filter((c) => c.kind === "pointcloud")
    const clouds = await page.evaluate(async (bridgeUrl, cases) => {
        const { connect } = await import("/client/zenoh_web.js")
        const client = await connect(bridgeUrl)
        const firstMessage = (key, options) => new Promise((resolve) => {
            const timer = setTimeout(() => resolve({ error: "no message in 8 s" }), 8000)
            const subscription = client.subscribe(key, options, (message) => {
                clearTimeout(timer)
                subscription.close()
                resolve({ points: message.points, bytes: message.bytes.length })
            })
            subscription.ready().catch((error) => resolve({ error: error.message }))
        })
        const results = []
        for (const testCase of cases) {
            const full = await firstMessage(testCase.key, { codec: testCase.codec })
            const reduced = await firstMessage(testCase.key, { codec: testCase.codec, maxQuality: 0.5 })
            if (full.error || reduced.error) {
                results.push({ file: testCase.file, codec: testCase.codec, error: full.error ?? reduced.error })
                continue
            }
            const points = full.points
            let worstError = 0
            for (let index = 0; index < points.count; index++) {
                const expected = [(index % 200) * 0.05, Math.floor(index / 200) * 0.05, (index % 7) * 0.125]
                for (let axis = 0; axis < 3; axis++) {
                    worstError = Math.max(worstError, Math.abs(points.positions[index * 3 + axis] - expected[axis]))
                }
            }
            const intensityExact = points.intensity === null ? null : points.intensity.every((value, index) => points.intensityMin + value * points.intensityScale === (testCase.file.includes("xyzi") ? index % 256 : 0))
            // reduced: every point inside the source's bounding box grown by maxError
            const low = [0, 0, 0]
            const high = [9.95, 4.95, 0.75]
            let outside = 0
            for (let index = 0; index < reduced.points.count; index++) {
                for (let axis = 0; axis < 3; axis++) {
                    const value = reduced.points.positions[index * 3 + axis]
                    if (value < low[axis] - reduced.points.maxError - 1e-5 || value > high[axis] + reduced.points.maxError + 1e-5) {
                        outside++
                    }
                }
            }
            results.push({
                file: testCase.file, codec: testCase.codec,
                count: points.count, sourceCount: points.sourceCount, maxError: points.maxError, worstError, hasIntensity: points.intensity !== null, intensityExact, bytes: full.bytes,
                reduced: { count: reduced.points.count, bytes: reduced.bytes, voxelSize: reduced.points.voxelSize, maxError: reduced.points.maxError, outside },
            })
        }
        client.close()
        return results
    }, { args: [bridge.url, cloudCases] })
    for (const result of clouds) {
        const label = `${result.codec} ${result.file}`
        if (result.error) {
            check(false, `${label}: ${result.error}`)
            continue
        }
        // documented bound: scale / 2 per axis; scale = (largest extent / 2) / 32767 = 4.975 / 32767 here
        const bound = 4.975 / 32767 / 2
        check(result.count === 20000 && result.worstError <= result.maxError + 1e-5 && result.maxError <= bound * 1.001,
            `${label}: ${result.count} points, worst axis error ${result.worstError.toExponential(2)} m <= documented bound ${result.maxError.toExponential(2)} m (${result.bytes} bytes for ${result.sourceCount} source points)`)
        const wantsIntensity = !result.file.includes("ros2/pointcloud_xyz.")
        check(result.hasIntensity === wantsIntensity && (result.intensityExact ?? true), `${label}: intensity ${result.hasIntensity ? (result.intensityExact ? "exact" : "WRONG") : "absent"} (expected ${wantsIntensity ? "present" : "absent"})`)
        check(result.reduced.bytes < result.bytes && result.reduced.count < result.count && result.reduced.outside === 0 && result.reduced.voxelSize > 0,
            `${label}: maxQuality 0.5 sends fewer bytes (${result.reduced.bytes} < ${result.bytes}) and points (${result.reduced.count}, voxel ${result.reduced.voxelSize.toFixed(2)} m), all within the source box + ${result.reduced.maxError.toFixed(3)} m`)
    }

    $.logStep("codec selection errors and shared encodes")
    const extra = await page.evaluate(async (bridgeUrl, depthKey) => {
        const { connect } = await import("/client/zenoh_web.js")
        const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
        const out = {}
        const first = await connect(bridgeUrl)
        try {
            first.subscribe(depthKey, { codec: "ros2-jpeg" }, () => {})
            out.clientError = null
        } catch (error) {
            out.clientError = error.message
        }
        try {
            first.subscribe(depthKey, { codec: "ros2-image", delivery: "reliable" }, () => {})
            out.reliableVideoError = null
        } catch (error) {
            out.reliableVideoError = error.message
        }
        // bypass the client's check: the bridge must refuse an unknown codec too
        const rawPeer = first._peer
        const raw = rawPeer.createDataChannel(JSON.stringify({ type: "sub", key: depthKey, id: 999999, opts: { codec: "ros2-jpeg" } }))
        out.rawClosed = await new Promise((resolve) => {
            raw.onclose = () => resolve(true)
            setTimeout(() => resolve(false), 5000)
        })
        // two frontends, same codec + quality: the second reuses encodes (same payload bytes)
        const second = await connect(bridgeUrl)
        const counts = [0, 0]
        const subscriptions = [first, second].map((client, index) => client.subscribe(depthKey, { codec: "ros2-depth" }, () => counts[index]++))
        await Promise.all(subscriptions.map((subscription) => subscription.ready()))
        await sleep(2500)
        await Promise.all([first.pollStats(), second.pollStats()])
        out.shared = subscriptions.map((subscription, index) => ({ received: counts[index], encodes: subscription.bridgeStats?.stats?.encodes, sharedEncodes: subscription.bridgeStats?.stats?.sharedEncodes }))
        first.close()
        second.close()
        return out
    }, { args: [bridge.url, cases.find((c) => c.file === "ros2/depth_16UC1.cdr").key] })
    console.log(JSON.stringify(extra))
    check(extra.clientError?.includes("unknown codec"), `client: unknown codec throws (${extra.clientError})`)
    check(extra.reliableVideoError?.includes("video codec"), `client: reliable delivery with a video codec throws (${extra.reliableVideoError})`)
    const bridgeRefusal = await bridge.output.waitFor((line) => line.includes("unknown codec"), 3000).catch(() => null)
    check(extra.rawClosed && bridgeRefusal !== null, `bridge: unknown codec is refused and the channel closed (${bridgeRefusal?.replace(/.*rejected/, "rejected")})`)
    const totalShared = extra.shared.reduce((sum, entry) => sum + (entry.sharedEncodes ?? 0), 0)
    check(extra.shared.every((entry) => entry.received > 5) && totalShared > 0, `identical encodes are shared across frontends (${JSON.stringify(extra.shared)})`)
} catch (error) {
    check(false, String(error))
    console.error(error)
}
await finish(scratch.toString())
