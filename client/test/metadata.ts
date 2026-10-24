#!/usr/bin/env -S deno run --allow-all --unstable-net
// Every encoded message keeps its sample's own header: msg.header ({ stamp, sec, nanosec, frameId, seq }) on a
// data-channel encoding (test-cloud) and on video (test-cam, also msg.video.frameId/stamp), exact to the nanosecond and
// matching the message it came with; `metadata: false` sends none. Loopback, examples/degrade_server, real Chrome.
// Usage: deno run --allow-all --unstable-net client/test/metadata.ts

import { openPage } from "./browser.ts"

const root = new URL("../..", import.meta.url).pathname
const scratch = await Deno.makeTempDir({ prefix: "zenoh-gateway-metadata-" })
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
const server = new Deno.Command(`${targetDir}/release/examples/degrade_server`, { args: [scratch], env: { CAMS: "1", CAM_SIZE: "320x240", CLOUD_BYTES: "1200", MAP_BYTES: "0" }, stdout: "piped", stderr: "inherit" }).spawn()
const url = await (async () => {
    for await (const chunk of server.stdout.pipeThrough(new TextDecoderStream())) {
        const match = chunk.match(/listening on (\S+)/)
        if (match) {
            return match[1]
        }
    }
    throw new Error("degrade_server exited")
})()

type Header = { stamp: number, sec: number, nanosec: number, frameId: string, seq?: number }
type Seen = { count: number, headers: (Header | undefined)[], videoFrameIds: (string | undefined)[], firstBytes: number[], samples: { encoding?: string, attachment?: number[] }[] }

const page = await openPage(`${url}/blank.html`, 60_000)
const failures: string[] = []
try {
    const result: Record<string, Seen> = await page.evaluate(async (url: string) => {
        const { connect } = await import("/client/zenoh_gateway.js")
        const client = await connect(url)
        const seen: Record<string, Seen> = {}
        const watch = (name: string, key: string, options: Record<string, unknown>) => {
            const record: Seen = seen[name] = { count: 0, headers: [], videoFrameIds: [], firstBytes: [], samples: [] }
            return client.subscribe(key, options, (msg: { header?: Header, video?: { frameId?: string }, bytes: Uint8Array, encoding?: string, attachment?: Uint8Array }) => {
                record.count++
                if (record.headers.length < 20) {
                    record.headers.push(msg.header)
                    record.videoFrameIds.push(msg.video?.frameId)
                    record.samples.push({ encoding: msg.encoding, attachment: msg.attachment && [...msg.attachment] })
                    // test-cloud keeps every point at full quality: its first 4 bytes are the sample's frame counter
                    record.firstBytes.push(msg.bytes.byteLength >= 4 ? new DataView(msg.bytes.buffer, msg.bytes.byteOffset).getUint32(0, true) : -1)
                }
            })
        }
        const subscriptions = [
            watch("cloud", "robot/cloud0", { encoding: "test-cloud", maxHz: 10 }),
            watch("cloudZstd", "robot/cloud0", { encoding: "test-cloud", compress: "zstd" }),
            watch("cloudOff", "robot/cloud0", { encoding: "test-cloud", metadata: false }),
            watch("cam", "robot/cam0", { encoding: "test-cam" }),
            watch("camOff", "robot/cam0", { encoding: "test-cam", metadata: false }),
            watch("attached", "page/attached", { encoding: "test-cloud" }),
        ]
        // samples with a zenoh encoding and attachment (as rmw_zenoh's carry seq, timestamp and gid)
        let counter = 0
        const putting = setInterval(() => {
            const bytes = new Uint8Array(12)
            new DataView(bytes.buffer).setUint32(0, counter++, true)
            client.put("page/attached", bytes, { encoding: "application/cdr", attachment: new Uint8Array([5, 6]) }).catch(() => {})
        }, 100)
        await new Promise((resolve) => setTimeout(resolve, 4000))
        clearInterval(putting)
        for (const subscription of await Promise.all(subscriptions)) {
            subscription.close?.()
        }
        client.close()
        return { ...seen, features: { count: client.features.length, headers: [], videoFrameIds: [], firstBytes: [], samples: [] } }
    }, [url])
    for (const [name, seen] of Object.entries(result)) {
        console.log(`${name}: ${seen.count} messages; first headers ${JSON.stringify(seen.headers.slice(0, 2))}`)
    }
    const exact = (header: Header | undefined, hz: number, frameId: string) =>
        header !== undefined && header.frameId === frameId && header.seq !== undefined && header.sec === Math.floor(header.seq / hz) &&
        header.nanosec === header.seq % hz * Math.floor(1e9 / hz) + 7 && Math.abs(header.stamp - (header.sec + header.nanosec * 1e-9)) < 1e-6
    for (const name of ["cloud", "cloudZstd"]) {
        const seen = result[name]
        if (seen.count === 0) {
            failures.push(`${name}: no messages`)
        } else if (!seen.headers.every((header) => exact(header, 10, "test_lidar"))) {
            failures.push(`${name}: want every message's exact header (test_lidar, seq-derived sec/nanosec), got ${JSON.stringify(seen.headers)}`)
        } else if (!seen.headers.every((header, index) => header?.seq === seen.firstBytes[index])) {
            failures.push(`${name}: a header that isn't its own message's: seqs ${seen.headers.map((header) => header?.seq)} vs payload counters ${seen.firstBytes}`)
        }
    }
    const cam = result.cam
    if (cam.count === 0) {
        failures.push("cam: no messages")
    } else if (!cam.headers.every((header) => exact(header, 30, "test_cam_optical")) || !cam.videoFrameIds.every((frameId) => frameId === "test_cam_optical")) {
        failures.push(`cam: want every frame's exact header (test_cam_optical), got ${JSON.stringify(cam.headers)}`)
    }
    for (const name of ["cloudOff", "camOff"]) {
        const seen = result[name]
        if (seen.count === 0) {
            failures.push(`${name}: no messages`)
        } else if (seen.headers.some((header) => header != null) || seen.videoFrameIds.some((frameId) => frameId != null)) {
            failures.push(`${name}: metadata false, yet a header came: ${JSON.stringify(seen.headers)}`)
        }
    }
    const attached = result.attached
    if (attached.count === 0) {
        failures.push("attached: no messages")
    } else if (!attached.samples.every((sample) => sample.encoding === "application/cdr" && JSON.stringify(sample.attachment) === "[5,6]") || !attached.headers.every((header) => exact(header, 10, "test_lidar"))) {
        failures.push(`attached: want the sample's encoding and attachment with its header, got ${JSON.stringify(attached.samples.slice(0, 3))} ${JSON.stringify(attached.headers.slice(0, 2))}`)
    }
    if (result.features.count === 0) {
        failures.push("the gateway listed no features")
    }
} finally {
    await page.close()
    server.kill()
}
console.log(failures.length ? `FAIL\n${failures.join("\n")}` : "PASS")
Deno.exit(failures.length ? 1 : 0)
