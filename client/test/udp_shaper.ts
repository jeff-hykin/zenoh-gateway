#!/usr/bin/env -S deno run --allow-net --unstable-net
// One of the video battery's bad links (battery_cells.ts) in front of any UDP service (a WebTransport/QUIC relay, a
// WebRTC media port): the browser talks to 127.0.0.1:<listen port>, the shaper forwards to <target> through the same
// rate schedule, delay, jitter and loss as the battery (shaped_link.ts), both ways. The schedule starts at the first
// packet from the browser. One browser at a time (the latest sender gets the replies).
// Usage: deno run --allow-net --unstable-net client/test/udp_shaper.ts <cell> <listen port> <target host:port>
//        deno run --allow-net --unstable-net client/test/udp_shaper.ts --list

import { shapedDirection } from "./shaped_link.ts"
import { cells } from "./battery_cells.ts"

if (Deno.args[0] === "--list" || Deno.args.length < 3) {
    for (const cell of cells) {
        const steps = cell.schedule.map(([at, rate]) => `${at} s: ${(rate * 8 / 1000).toFixed(0)} kbit/s`).join(", ")
        console.log(`${cell.name.padEnd(12)} ${steps}; delay ${cell.delayMs} ms, jitter up to ${cell.jitterMs} ms, loss ${cell.loss * 100}%; ${cell.seconds} s`)
    }
    if (Deno.args[0] !== "--list") {
        console.log("\nusage: udp_shaper.ts <cell> <listen port> <target host:port>")
        Deno.exit(1)
    }
    Deno.exit(0)
}
const [cellName, listenPort, target] = Deno.args
const cell = cells.find((candidate) => candidate.name === cellName)
if (!cell) {
    throw new Error(`no cell ${cellName}: see --list`)
}
const [targetHost, targetPort] = [target.slice(0, target.lastIndexOf(":")), Number(target.slice(target.lastIndexOf(":") + 1))]

let startedAt = 0
const browserSide = Deno.listenDatagram({ transport: "udp", hostname: "0.0.0.0", port: Number(listenPort) })
const serviceSide = Deno.listenDatagram({ transport: "udp", hostname: "0.0.0.0", port: 0 })
let browser: Deno.NetAddr | null = null
const offered = { bytes: 0, packets: 0 }
const down = shapedDirection(cell, () => startedAt, (data) => {
    if (browser) {
        browserSide.send(data, browser).catch(() => {})
    }
}, offered)
const up = shapedDirection(cell, () => startedAt, (data) => {
    serviceSide.send(data, { transport: "udp", hostname: targetHost, port: targetPort }).catch(() => {})
}, { bytes: 0, packets: 0 })
console.log(`${cell.name}: 127.0.0.1:${listenPort} -> ${target}; the schedule starts at the first packet`)
;(async () => {
    for await (const [data] of serviceSide) {
        down(data)
    }
})()
for await (const [data, address] of browserSide) {
    if (startedAt === 0) {
        startedAt = performance.now()
        console.log(`${cell.name}: started`)
    }
    browser = address as Deno.NetAddr
    up(data)
}
