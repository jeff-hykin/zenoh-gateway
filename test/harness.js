// Shared pieces of the end-to-end tests: real zenoh test peer, real bridge, headless Chrome.

import { $ } from "https://esm.sh/dax-sh@0.42.0"
import { launch } from "jsr:@astral/astral@0.5.6"
import { buildWeb } from "../tools/build_web.js"

export const repoRoot = $.path(import.meta.url).parentOrThrow().parentOrThrow()
export const bridgeDir = repoRoot.join("bridge")
export const fixturesDir = repoRoot.join("test/fixtures")

/** @returns {number} */
export function freePort() {
    const listener = Deno.listen({ port: 0, hostname: "127.0.0.1" })
    const port = /** @type {Deno.NetAddr} */ (listener.addr).port
    listener.close()
    return port
}

/** @type {string[]} */
export const failures = []

/**
 * @param {boolean} condition
 * @param {string} description
 */
export function check(condition, description) {
    console.log(`${condition ? "PASS" : "FAIL"} ${description}`)
    if (!condition) {
        failures.push(description)
    }
}

/**
 * Collects a child's output lines and resolves waiters on matching lines.
 * @param {ReadableStream<Uint8Array>} stream
 * @param {string} name
 */
export function lineCollector(stream, name) {
    /** @type {string[]} */
    const lines = []
    /** @type {{ test: (line: string) => boolean, resolve: (line: string) => void }[]} */
    let waiters = []
    const logFile = name === "bridge" ? Deno.env.get("BRIDGE_LOG_FILE") : undefined
    const log = logFile ? Deno.openSync(logFile, { create: true, append: true }) : null
    const encoder = new TextEncoder()
    ;(async () => {
        let buffered = ""
        for await (const chunk of stream.pipeThrough(new TextDecoderStream())) {
            buffered += chunk
            const parts = buffered.split("\n")
            buffered = parts.pop() ?? ""
            for (const line of parts) {
                if (log) {
                    log.writeSync(encoder.encode(`${line}\n`))
                    // keep memory bounded when tracing: only waiters see the line
                    if (!line.includes("listening on") && !line.includes("rejected")) {
                        continue
                    }
                }
                lines.push(line)
                if (Deno.env.get("E2E_VERBOSE")) {
                    console.log(`[${name}] ${line}`)
                }
                waiters = waiters.filter((waiter) => {
                    if (waiter.test(line)) {
                        waiter.resolve(line)
                        return false
                    }
                    return true
                })
            }
        }
    })()
    return {
        lines,
        /**
         * @param {(line: string) => boolean} test
         * @param {number} timeoutMs
         * @returns {Promise<string>}
         */
        waitFor(test, timeoutMs) {
            const existing = lines.find(test)
            if (existing) {
                return Promise.resolve(existing)
            }
            return new Promise((resolve, reject) => {
                const timer = setTimeout(() => reject(new Error(`${name}: timed out waiting for line`)), timeoutMs)
                waiters.push({ test, resolve: (line) => {
                    clearTimeout(timer)
                    resolve(line)
                } })
            })
        },
    }
}

export const machineLoad = async () => (await $`uptime`.text()).replace(/.*load averages?: /, "")

/**
 * @typedef {{ file: string, protocol: string, zenoh_key: string, msg_type: string, format?: string, encoding?: string, expected?: any, [field: string]: any }} FixtureEntry
 */

/** @returns {{ pattern: any, entries: FixtureEntry[] }} */
export function loadManifest() {
    return JSON.parse(fixturesDir.join("manifest.json").readTextSync())
}

/**
 * The manifest's key with its topic replaced by `fixture/<file stem>`, keeping the protocol's key
 * shape: dimos `<topic>/<msg_name>`, ROS 2 `<domain>/<topic>/<type>/RIHS01_<hash>`.
 * @param {FixtureEntry} entry
 */
export function fixtureKey(entry) {
    const stem = entry.file.split("/").pop()?.replace(/\.[^.]+$/, "")
    const parts = entry.zenoh_key.split("/")
    if (entry.protocol === "dimos") {
        return `dimos/fixture/${stem}/${parts.at(-1)}`
    }
    return `${parts[0]}/fixture/${stem}/${parts.at(-2)}/${parts.at(-1)}`
}

/**
 * Builds the bridge + test peer (+ `examples`, release) and the web root.
 * @param {import("https://esm.sh/dax-sh@0.42.0").Path} scratch
 * @param {string[]} examples
 */
export async function buildAll(scratch, examples = []) {
    $.logStep(`building bridge + test peer${examples.map((example) => ` + ${example}`).join("")} (release)`)
    await $`cargo build --release --bin zenoh-web --example test_peer ${examples.flatMap((example) => ["--example", example])}`.cwd(bridgeDir)
    $.logStep("building the web root")
    return await buildWeb(scratch.join("web").toString())
}

/** @type {{ kill: (signal?: Deno.Signal) => void }[]} */
const children = []
/** @type {import("jsr:@astral/astral@0.5.6").Browser[]} */
const browsers = []

/**
 * @param {string[]} extraArgs
 */
export async function startPeer(extraArgs) {
    const zenohPort = freePort()
    const peer = $`${bridgeDir.join("target/release/examples/test_peer")} --listen tcp/127.0.0.1:${zenohPort} ${extraArgs}`
        .stdout("piped").stderr("inherit").noThrow().spawn()
    children.push(peer)
    const output = lineCollector(peer.stdout(), "peer")
    await output.waitFor((line) => line === "READY", 15000)
    return { zenohPort, output, process: peer }
}

/**
 * Starts the bridge (or another binary taking the same --port/--zenoh-config/--connect/--serve flags).
 * @param {import("https://esm.sh/dax-sh@0.42.0").Path} scratch
 * @param {number} zenohPort
 * @param {import("https://esm.sh/dax-sh@0.42.0").Path} webRoot
 * @param {string[]} extraArgs
 * @param {string} binary
 */
export async function startBridge(scratch, zenohPort, webRoot, extraArgs = [], binary = bridgeDir.join("target/release/zenoh-web").toString()) {
    const httpPort = freePort()
    const configPath = scratch.join(`bridge_zenoh_${httpPort}.json5`)
    // isolated zenoh: no multicast scouting, so the test never touches other zenoh systems
    configPath.writeTextSync(JSON.stringify({ mode: "peer", scouting: { multicast: { enabled: false } }, listen: { endpoints: [] } }))
    const bridge = $`${binary} --port ${httpPort} --zenoh-config ${configPath} --connect tcp/127.0.0.1:${zenohPort} --serve ${webRoot} ${extraArgs}`
        .env("RUST_LOG", Deno.env.get("RUST_LOG") ?? "info,zenoh=warn,zenoh_web=info")
        .stdout("inherit").stderr("piped").noThrow().spawn()
    children.push(bridge)
    const output = lineCollector(bridge.stderr(), "bridge")
    await output.waitFor((line) => line.includes("listening on"), 15000)
    return { url: `http://127.0.0.1:${httpPort}`, output, process: bridge }
}

export async function launchBrowser() {
    const browser = await launch({ headless: true, args: ["--no-sandbox", "--autoplay-policy=no-user-gesture-required"] })
    browsers.push(browser)
    return browser
}

export async function cleanup() {
    for (const browser of browsers) {
        await browser.close().catch(() => {})
    }
    for (const child of children) {
        try {
            child.kill("SIGTERM")
        } catch {
            // already exited
        }
    }
}

/**
 * Prints the summary and exits with the result.
 * @param {string} scratch
 */
export async function finish(scratch) {
    await cleanup()
    console.log(`machine load at end: ${await machineLoad()}`)
    console.log(`\n${failures.length === 0 ? "ALL PASSED" : `${failures.length} FAILED:\n  ${failures.join("\n  ")}`}`)
    console.log(`artifacts: ${scratch}`)
    Deno.exit(failures.length === 0 ? 0 : 1)
}
