#!/usr/bin/env -S deno run --allow-all
// Runs every end-to-end suite (each starts its own peer, bridge and Chrome) and summarizes.
// Usage: deno run --allow-all test/all.js

const suites = ["e2e.js", "codecs.js", "allocation.js", "latency.js", "example.js"]
const results = []
for (const suite of suites) {
    console.log(`\n===== ${suite} =====`)
    const command = new Deno.Command(Deno.execPath(), { args: ["run", "--allow-all", "--unstable-net", new URL(suite, import.meta.url).pathname], stdout: "inherit", stderr: "inherit" })
    const { code } = await command.output()
    results.push({ suite, passed: code === 0 })
}
console.log(`\n===== summary =====\n${results.map((result) => `${result.passed ? "PASS" : "FAIL"} ${result.suite}`).join("\n")}`)
Deno.exit(results.every((result) => result.passed) ? 0 : 1)
