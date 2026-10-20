#!/usr/bin/env -S deno run --allow-all --no-lock
// One-command latency/throughput harness: a headless Chrome page subscribes through a zenoh-gateway (never asking for
// a lease) the way the dimos Controller does, and this prints, per stream, what the bus carries vs what the gateway
// received, sent and the page got, plus video end-to-end latency by stage (bench/latency_page.js has the method).
//
//   deno run -A --no-lock bench/latency.ts --url http://127.0.0.1:5555/zenoh-gateway         # an existing gateway (e.g. Desktop's)
//   deno run -A --no-lock bench/latency.ts --gateway ./zenoh-gateway --zenoh-config z.json5    # starts its own on a free port
//   ... --python ~/repos/dimos/.venv/bin/python   # also probe the bus itself (rate, bytes, header-stamp lag)
//
// The bus probe is a second zenoh subscriber: the publisher sends it its own copy (a raw 720p camera is ~150 Mbit/s).
import { Command } from "jsr:@cliffy/command@1.0.0-rc.7"
import { launch } from "jsr:@astral/astral@0.5.6"

const root = new URL("..", import.meta.url).pathname

const { options } = await new Command()
    .name("latency")
    .description("Per-stage rates and video latency through a zenoh-gateway, from a headless browser without a lease.")
    .option("--url <url:string>", "Gateway base URL (the client's connect() URL), e.g. http://127.0.0.1:5555/zenoh-gateway")
    .option("--gateway <path:string>", "Start this zenoh-gateway binary on a free port instead of using --url")
    .option("--zenoh-config <path:string>", "zenoh config for --gateway")
    .option("--gateway-arg <arg:string>", "Extra argument for --gateway (repeatable), e.g. --gateway-arg=--max-bandwidth-bytes-per-sec=150000", { collect: true })
    .option("--gateway-log <path:string>", "--gateway's stderr goes here (RUST_LOG from the environment, default warn)")
    .option("--gateway-pid <pid:number>", "Sample this process's CPU (e.g. Desktop's, for --url)")
    .option("--token <token:string>", "Bearer token for the gateway")
    .option("--seconds <seconds:number>", "How long to measure", { default: 30 })
    .option("--video <key:string>", "Video key (sensor_msgs.Image); empty for none", { default: "dimos/color_image/**" })
    .option("--video-encoding <name:string>", "Video encoding", { default: "dimos_lcm_image" })
    .option("--video-options <json:string>", "Extra video subscribe options (JSON), e.g. a Controller preset", { default: "{}" })
    .option("--cloud <key:string>", "PointCloud2 keys (repeatable)", { collect: true, default: ["dimos/lidar/**", "dimos/cloud/**"] })
    .option("--cloud-options <json:string>", "Extra cloud subscribe options (JSON)", { default: '{"maxHz":20,"encodeOptions":{"quality":1}}' })
    .option("--small <key:string>", "Small state keys, reliable (repeatable)", { collect: true, default: ["dimos/tf/**", "dimos/odom/**"] })
    .option("--small-options <json:string>", "Extra options for the small keys (JSON), e.g. '{\"priority\":2}' (strict: never waits behind bulk)", { default: "{}" })
    .option("--python <path:string>", "A python with eclipse-zenoh: probe the bus itself (rate, bytes, header-stamp lag)")
    .option("--probe-config <json:string>", "zenoh config insertions for the probe (JSON object)", { default: '{"mode":"peer","listen/endpoints":["tcp/127.0.0.1:0"],"scouting/multicast/interface":"lo0"}' })
    .option("--client <url:string>", "Load the browser client from this URL instead of bundling client/zenoh_gateway.ts")
    .option("--chrome <path:string>", "Chrome/Chromium binary (default: astral's)")
    .option("--out <path:string>", "Write every per-second sample as JSON lines")
    .option("--quiet", "Only the summary")
    .parse(Deno.args)

const spawned: Deno.ChildProcess[] = []
const cleanup = () => {
    for (const child of spawned) {
        try {
            child.kill("SIGTERM")
        } catch {
            // already gone
        }
    }
}
Deno.addSignalListener("SIGINT", () => {
    cleanup()
    Deno.exit(130)
})

// ── the gateway ─────────────────────────────────────────────────────────────────────────────────────
let gatewayUrl = options.url
let gatewayPid: number | null = options.gatewayPid ?? null
if (options.gateway) {
    const listener = Deno.listen({ port: 0, hostname: "127.0.0.1" })
    const port = (listener.addr as Deno.NetAddr).port
    listener.close()
    const args = ["--port", String(port), ...(options.zenohConfig ? ["--zenoh-config", options.zenohConfig] : []), ...(options.gatewayArg ?? [])]
    const log = options.gatewayLog ? await Deno.open(options.gatewayLog, { create: true, write: true, truncate: true }) : null
    const child = new Deno.Command(options.gateway, { args, stdout: "null", stderr: log ? "piped" : "null", env: { RUST_LOG: Deno.env.get("RUST_LOG") ?? "warn" } }).spawn()
    if (log) {
        child.stderr.pipeTo(log.writable).catch(() => {})
    }
    spawned.push(child)
    gatewayPid = child.pid
    gatewayUrl = `http://127.0.0.1:${port}`
    for (let i = 0; ; i++) {
        const ok = await fetch(`${gatewayUrl}/zenoh-gateway/health`).then((response) => response.ok).catch(() => false)
        if (ok) {
            break
        }
        if (i > 100) {
            throw new Error("the gateway never answered /zenoh-gateway/health")
        }
        await new Promise((resolve) => setTimeout(resolve, 100))
    }
}
if (!gatewayUrl) {
    throw new Error("give --url or --gateway")
}
const health = await fetch(`${gatewayUrl.replace(/\/zenoh-gateway\/?$/, "")}/zenoh-gateway/health`).then((response) => response.json()).catch(() => null)

// ── the page and its client ─────────────────────────────────────────────────────────────────────────
const scratch = await Deno.makeTempDir({ prefix: "zenoh-gateway-latency-" })
let clientUrl = options.client
if (!clientUrl) {
    const bundled = await new Deno.Command(Deno.execPath(), { args: ["bundle", "--quiet", "--platform", "browser", "-o", `${scratch}/zenoh_gateway.js`, `${root}client/zenoh_gateway.ts`] }).output()
    if (bundled.code !== 0) {
        throw new Error(`deno bundle failed: ${new TextDecoder().decode(bundled.stderr)}`)
    }
    clientUrl = "./zenoh_gateway.js"
}
const pageScript = await Deno.readTextFile(`${root}bench/latency_page.js`)
const server = Deno.serve({ port: 0, hostname: "127.0.0.1", onListen: () => {} }, async (request) => {
    const path = new URL(request.url).pathname
    if (path === "/zenoh_gateway.js") {
        return new Response(await Deno.readFile(`${scratch}/zenoh_gateway.js`), { headers: { "content-type": "text/javascript" } })
    }
    if (path === "/latency_page.js") {
        return new Response(pageScript, { headers: { "content-type": "text/javascript" } })
    }
    return new Response(`<!doctype html><html><head><meta charset="utf-8"></head><body><script type="module" src="./latency_page.js"></script></body></html>`, { headers: { "content-type": "text/html" } })
})
const pageUrl = new URL(`http://127.0.0.1:${server.addr.port}/`)
const subscriptions = [
    ...(options.video ? [{ key: options.video, kind: "video", options: { delivery: "latest", encoding: options.videoEncoding, bandwidthPriority: 1, ...JSON.parse(options.videoOptions) } }] : []),
    ...options.cloud.map((key) => ({ key, kind: "cloud", options: { delivery: "latest", encoding: "dimos_lcm_pointcloud2", ...JSON.parse(options.cloudOptions) } })),
    ...options.small.map((key) => ({ key, kind: "small", options: { delivery: "reliable", bandwidthPriority: 4, ...JSON.parse(options.smallOptions) } })),
]
pageUrl.searchParams.set("config", JSON.stringify({ gateway: gatewayUrl, client: new URL(clientUrl, pageUrl).href, token: options.token, subscriptions }))

// ── the bus probe ───────────────────────────────────────────────────────────────────────────────────
// per second and key: messages, bytes, and now - the LCM header stamp (found by scanning for a sec/nsec pair near now)
const probeSource = `
import zenoh, time, sys, json, struct, threading
config = zenoh.Config()
for key, value in json.loads(sys.argv[1]).items():
    config.insert_json5(key, json.dumps(value))
keys = json.loads(sys.argv[2])
session = zenoh.open(config)
lock = threading.Lock()
counts = {}
def stamp_lag(payload, now):
    for offset in range(8, min(len(payload) - 8, 64), 4):
        sec, nsec = struct.unpack(">ii", payload[offset:offset + 8])
        if abs(sec - now) < 86400 and 0 <= nsec < 1_000_000_000:
            return (now - (sec + nsec / 1e9)) * 1000
    return None
def handler(pattern):
    def on_sample(sample):
        now = time.time()
        payload = sample.payload.to_bytes()
        lag = stamp_lag(payload, now)
        with lock:
            entry = counts.setdefault(pattern, {"n": 0, "bytes": 0, "lags": []})
            entry["n"] += 1
            entry["bytes"] += len(payload)
            if lag is not None:
                entry["lags"].append(lag)
    return on_sample
subscribers = [session.declare_subscriber(pattern, handler(pattern)) for pattern in keys]
while True:
    time.sleep(1)
    with lock:
        snapshot = dict(counts)
        counts.clear()
    print(json.dumps({"at": time.time() * 1000, "keys": snapshot}), flush=True)
`
type ProbeSample = { at: number; keys: Record<string, { n: number; bytes: number; lags: number[] }> }
const probeSamples: ProbeSample[] = []
if (options.python) {
    const keys = subscriptions.map((subscription) => subscription.key)
    const child = new Deno.Command(options.python, { args: ["-c", probeSource, options.probeConfig, JSON.stringify(keys)], stdout: "piped", stderr: "null" }).spawn()
    spawned.push(child)
    ;(async () => {
        let buffered = ""
        for await (const chunk of child.stdout.pipeThrough(new TextDecoderStream())) {
            buffered += chunk
            const lines = buffered.split("\n")
            buffered = lines.pop() ?? ""
            for (const line of lines) {
                probeSamples.push(JSON.parse(line))
            }
        }
    })()
}

// ── measure ─────────────────────────────────────────────────────────────────────────────────────────
const browser = await launch({ headless: true, ...(options.chrome ? { path: options.chrome } : {}), args: ["--autoplay-policy=no-user-gesture-required"] })
const page = await browser.newPage()
await page.goto(pageUrl.href, { waitUntil: "none" })
for (let i = 0; ; i++) {
    const state = await page.evaluate(() => ({ ready: (window as any).harnessReady === true, errors: (window as any).harnessErrors ?? [] })).catch(() => ({ ready: false, errors: [] }))
    if (state.ready) {
        break
    }
    if (state.errors.length || i > 60) {
        throw new Error(`the page didn't connect to ${gatewayUrl}: ${state.errors.join("; ") || "timed out"}`)
    }
    await new Promise((resolve) => setTimeout(resolve, 500))
}

const out = options.out ? await Deno.open(options.out, { create: true, write: true, truncate: true }) : null
const samples: any[] = []
const cpuOf = async (pid: number | null) => {
    if (pid === null) {
        return null
    }
    const output = await new Deno.Command("ps", { args: ["-o", "%cpu=", "-p", String(pid)] }).output()
    return Number(new TextDecoder().decode(output.stdout).trim()) || null
}
const percentile = (values: number[], p: number) => {
    const sorted = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b)
    return sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))] : null
}
const fmt = (value: number | null | undefined, digits = 0) => (value === null || value === undefined || !Number.isFinite(value) ? "-" : value.toFixed(digits))
const mbps = (bytes: number) => (bytes * 8) / 1e6
const short = (key: string) => key.replace(/^dimos\//, "").replace(/\/\*\*$/, "")

for (let second = 1; second <= options.seconds; second++) {
    await new Promise((resolve) => setTimeout(resolve, 1000))
    const sample: any = await page.evaluate(() => (window as any).sample())
    sample.gatewayCpu = await cpuOf(gatewayPid)
    sample.probe = probeSamples.at(-1) ?? null
    samples.push(sample)
    await out?.write(new TextEncoder().encode(JSON.stringify(sample) + "\n"))
    if (sample.errors.length) {
        console.error("page:", sample.errors.join("; "))
    }
    if (options.quiet) {
        continue
    }
    const parts = Object.entries(sample.streams).map(([key, entry]: [string, any]) => `${short(key)} ${entry.n}/s ${fmt(mbps(entry.bytes), 1)}Mb age${fmt(percentile(entry.ages, 0.5))}`)
    const v = sample.video
    const videoPart = v ? `video meta ${v.meta}/s ${fmt(mbps(v.metaBytes), 2)}Mb shown ${v.frames.length}/s e2e ${fmt(percentile(v.frames.map((f: any) => f.total), 0.5))}ms (rx>wr ${fmt(percentile(v.rxToWrite, 0.5))} wr>recv ${fmt(percentile(v.frames.map((f: any) => f.writeToReceive), 0.5))} recv>show ${fmt(percentile(v.frames.map((f: any) => f.receiveToPresent), 0.5))}) inflight ${v.inbound ? v.metaTotal - v.inbound.framesReceived : "-"}` : ""
    console.log(`${String(second).padStart(3)}s  ${videoPart}  |  ${parts.join("  ")}  |  rtt ${fmt(sample.rttMs)}ms gw cpu ${fmt(sample.gatewayCpu)}%`)
}

// ── summary ─────────────────────────────────────────────────────────────────────────────────────────
const keys = [...new Set(samples.flatMap((sample) => Object.keys(sample.streams)))]
const first = samples[0]
const last = samples.at(-1)
const seconds = samples.length
const gatewayCounter = (key: string, name: string) => {
    const end = last.streams[key]?.gateway?.stats?.[name]
    const start = first.streams[key]?.gateway?.stats?.[name]
    return typeof end === "number" && typeof start === "number" ? (end - start) / Math.max(1, seconds - 1) : null
}
console.log(`\ngateway ${gatewayUrl} (${health?.version ?? "?"}), ${seconds} s, page rtt p50 ${fmt(percentile(samples.map((s) => s.rttMs), 0.5))} ms`)
console.log("\nstream                     bus Hz  bus Mb/s  gw rx Hz  gw sent Hz  gw sent Mb/s  page Hz  page Mb/s  age p50/p95 ms  gw drop q/age /s  gw max send lag ms")
for (const key of keys) {
    const probe = probeSamples.slice(-seconds)
    const busN = probe.reduce((sum, sample) => sum + (sample.keys[key]?.n ?? 0), 0) / Math.max(1, probe.length)
    const busBytes = probe.reduce((sum, sample) => sum + (sample.keys[key]?.bytes ?? 0), 0) / Math.max(1, probe.length)
    const isVideo = key === options.video
    const pageN = isVideo ? samples.reduce((sum, sample) => sum + (sample.video?.frames.length ?? 0), 0) / seconds : samples.reduce((sum, sample) => sum + (sample.streams[key]?.n ?? 0), 0) / seconds
    const pageBytes = isVideo ? samples.reduce((sum, sample) => sum + (sample.video?.inbound?.bytesDelta ?? 0), 0) / seconds : samples.reduce((sum, sample) => sum + (sample.streams[key]?.bytes ?? 0), 0) / seconds
    const ages = isVideo ? samples.flatMap((sample) => sample.video?.frames.map((frame: any) => frame.total) ?? []) : samples.flatMap((sample) => sample.streams[key]?.ages ?? [])
    console.log(
        `${short(key).padEnd(25)}  ${fmt(options.python ? busN : null, 1).padStart(6)}  ${fmt(options.python ? mbps(busBytes) : null, 1).padStart(8)}  ${fmt(gatewayCounter(key, "received"), 1).padStart(8)}  ${fmt(gatewayCounter(key, "sent"), 1).padStart(10)}  ${fmt(gatewayCounter(key, "bytesSent") === null ? null : mbps(gatewayCounter(key, "bytesSent")!), 2).padStart(12)}  ${fmt(pageN, 1).padStart(7)}  ${fmt(mbps(pageBytes), 2).padStart(9)}  ${(fmt(percentile(ages, 0.5)) + "/" + fmt(percentile(ages, 0.95))).padStart(14)}  ${(fmt(gatewayCounter(key, "droppedQueue"), 1) + "/" + fmt(gatewayCounter(key, "droppedAge"), 1)).padStart(16)}  ${fmt(last.streams[key]?.gateway?.stats?.maxSendLagMs).padStart(18)}`,
    )
}
if (options.video) {
    const frames = samples.flatMap((sample) => sample.video?.frames ?? [])
    const stage = (name: string, values: (number | null)[]) => console.log(`  ${name.padEnd(44)} p50 ${fmt(percentile(values as number[], 0.5)).padStart(6)}  p95 ${fmt(percentile(values as number[], 0.95)).padStart(6)} ms`)
    const lags = probeSamples.slice(-seconds).flatMap((sample) => sample.keys[options.video]?.lags ?? [])
    const gateway = last.streams[options.video]?.gateway?.stats ?? {}
    console.log(`\nvideo ${options.video}: ${frames.length} frames presented; per-stage (camera stamp -> presented):`)
    stage("bus: header stamp -> probe received", options.python ? lags : [])
    stage("gateway rx -> written to track (decode+enc)", samples.flatMap((sample) => sample.video?.rxToWrite ?? []))
    console.log(`    of which gateway decode ${fmt(gateway.decodeMs, 1)} ms, scale+encode ${fmt(gateway.encodeMs, 1)} ms (smoothed)`)
    stage("written -> received (pacer + net + assembly)", frames.map((frame: any) => frame.writeToReceive))
    stage("received -> presented (jitter buf + decode)", frames.map((frame: any) => frame.receiveToPresent))
    stage("gateway rx -> presented (total)", frames.map((frame: any) => frame.total))
    const jitter = samples.map((sample) => sample.video?.inbound?.jitterBufferMs).filter((value) => value !== null && value !== undefined)
    const decode = samples.map((sample) => sample.video?.inbound?.decodeMs).filter((value) => value !== null && value !== undefined)
    console.log(`  getStats: jitter buffer p50 ${fmt(percentile(jitter, 0.5))} ms, decode p50 ${fmt(percentile(decode, 0.5), 1)} ms, framesDropped ${last.video?.inbound?.framesDropped ?? "-"}, nack ${last.video?.inbound?.nackCount ?? "-"}, pli ${last.video?.inbound?.pliCount ?? "-"}, lost ${last.video?.inbound?.packetsLost ?? "-"}, size ${last.video?.inbound?.width ?? "-"}x${last.video?.inbound?.height ?? "-"}`)
    console.log(`  frames written but not yet received at the end: ${last.video?.inbound ? last.video.metaTotal - last.video.inbound.framesReceived : "-"} (metadata seen ${last.video?.metaTotal ?? "-"}, framesReceived ${last.video?.inbound?.framesReceived ?? "-"})`)
    // drift: the total's p50 over the first and last third
    const third = Math.max(1, Math.floor(samples.length / 3))
    const early = samples.slice(0, third).flatMap((sample) => sample.video?.frames.map((frame: any) => frame.total) ?? [])
    const late = samples.slice(-third).flatMap((sample) => sample.video?.frames.map((frame: any) => frame.total) ?? [])
    console.log(`  drift: total p50 ${fmt(percentile(early, 0.5))} ms in the first third, ${fmt(percentile(late, 0.5))} ms in the last; match error p50 ${fmt(percentile(frames.map((frame: any) => Math.abs(frame.matchErrorMs ?? NaN)), 0.5), 1)} ms, ${last.video?.exact ? `exact frame matching (media time), unmatched ${last.video?.unmatched ?? 0}` : `estimated matching (RTP gaps), RTP jumps re-anchored ${last.video?.rtpJumps ?? 0}`}`)
}
console.log(`\nper-second samples: ${options.out ?? "(pass --out)"}`)

await browser.close()
await server.shutdown()
cleanup()
Deno.exit(0)
