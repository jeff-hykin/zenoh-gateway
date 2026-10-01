#!/usr/bin/env -S deno run --allow-all
// Builds a servable web root: client/zenoh_web.ts bundled to client/zenoh_web.js (esbuild via
// `deno bundle`, the same transform esm.sh applies to the .ts), plus examples/ and test/ pages.
// Usage: deno run --allow-all tools/build_web.js [outDir]   (default: build)

import { $ } from "https://esm.sh/dax-sh@0.42.0"

const repoRoot = $.path(import.meta.url).parentOrThrow().parentOrThrow()

/** @param {string} outDir */
export async function buildWeb(outDir) {
    const out = $.path(outDir).resolve()
    out.join("client").mkdirSync({ recursive: true })
    // Deno.Command, not dax: inside dax `deno bundle` resolved to `deno run bundle`
    const bundle = new Deno.Command(Deno.execPath(), {
        args: ["bundle", "--quiet", "--platform", "browser", "-o", out.join("client/zenoh_web.js").toString(), repoRoot.join("client/zenoh_web.ts").toString()],
        stdout: "inherit",
        stderr: "inherit",
    })
    const { code } = await bundle.output()
    if (code !== 0) {
        throw new Error(`deno bundle failed with exit code ${code}`)
    }
    for (const pages of ["examples", "test"]) {
        await $`cp -R ${repoRoot.join(pages)} ${out}/`
    }
    return out
}

if (import.meta.main) {
    const out = await buildWeb(Deno.args[0] ?? repoRoot.join("build").toString())
    console.log(`built ${out}; serve it with: zenoh-web --serve ${out}`)
}
