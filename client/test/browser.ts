// A headless page for the browser suites: Chrome through astral, or BROWSER=firefox for Playwright's Firefox (its
// browser build must be installed: `npx playwright@1.63.0 install firefox`). `evaluate` runs a function in the page
// with arguments, the same for both.

import { launch } from "jsr:@astral/astral@0.5.6"

export const firefox = Deno.env.get("BROWSER") === "firefox"

export type Page = {
    // deno-lint-ignore no-explicit-any
    evaluate: <T>(fn: (...args: any[]) => Promise<T>, args: unknown[]) => Promise<T>
    close: () => Promise<void>
}

/** Opens `url` in a headless browser; page calls may take up to `timeoutMs`. */
export async function openPage(url: string, timeoutMs: number): Promise<Page> {
    if (firefox) {
        const { firefox } = await import("npm:playwright@1.63.0")
        // loopback ICE candidates and real host addresses: the gateway and its shaper are on 127.0.0.1
        const browser = await firefox.launch({ headless: true, firefoxUserPrefs: { "media.peerconnection.ice.loopback": true, "media.peerconnection.ice.obfuscate_host_addresses": false, "media.autoplay.default": 0 } })
        const page = await browser.newPage()
        page.setDefaultTimeout(timeoutMs)
        await page.goto(url)
        // (Playwright passes one argument: the function goes as source)
        return { evaluate: (fn, args) => page.evaluate(({ source, args }) => (0, eval)(`(${source})`)(...args), { source: fn.toString(), args }), close: () => browser.close() }
    }
    const browser = await launch({ headless: true, args: ["--no-sandbox", "--autoplay-policy=no-user-gesture-required"] })
    const page = await browser.newPage(url)
    // astral retries evaluate 5x on a 10 s deadline: long runs need more
    ;(page as unknown as { timeout: number }).timeout = timeoutMs
    return { evaluate: (fn, args) => page.evaluate(fn, { args }) as never, close: () => browser.close() }
}

/** The browser suites and benches: two at once on one machine skew each other's timing (a concurrent run turned a
 * 289 ms cell into 11 s). */
const timingSensitive = /(client\/test\/(video_battery|video_latency|degrade|resubscribe|backpressure|lease_takeover|reconnect)\.ts|bench\/latency\.ts)/

/** Waits while another timing-sensitive browser run (from any checkout) is going on this machine. */
export async function waitForOtherBrowserTests(): Promise<void> {
    let announced = false
    while (true) {
        const listed = await new Deno.Command("ps", { args: ["-axo", "pid=,ppid=,command="] }).output()
        const processes = new TextDecoder().decode(listed.stdout).split("\n").map((line) => line.trim().match(/^(\d+)\s+(\d+)\s+(.*)$/)).filter((match) => match !== null)
        // this run and what started it (bin/test, caffeinate) don't count, nor a shell whose command line names a test
        // (only a deno process runs one: a shell piping a run into grep waited on itself)
        const parents = new Map(processes.map(([, pid, ppid]) => [Number(pid), Number(ppid)]))
        const ours = new Set<number>()
        for (let pid = Deno.pid; pid > 1 && !ours.has(pid); pid = parents.get(pid) ?? 1) {
            ours.add(pid)
        }
        const others = processes.filter(([, pid, , command]) => !ours.has(Number(pid)) && /^\S*deno\s+run\b/.test(command) && timingSensitive.test(command))
        if (others.length === 0) {
            return
        }
        if (!announced) {
            announced = true
            console.log(`waiting for another browser test to finish: ${others.map(([, pid, , command]) => `${pid} ${command.slice(0, 120)}`).join("; ")}`)
        }
        await new Promise((resolve) => setTimeout(resolve, 5000))
    }
}
