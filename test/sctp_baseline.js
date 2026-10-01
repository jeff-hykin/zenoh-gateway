#!/usr/bin/env -S deno run --allow-all --unstable-net
// Baseline for test/throughput.js: Chrome-to-Chrome data-channel throughput (Chrome's own SCTP,
// dcsctp, on both ends) over the same shaped links. Two peer connections in one page; the sender
// keeps 4 MB buffered, so only SCTP and the link limit it. Report only.
// Usage: deno run --allow-all --unstable-net test/sctp_baseline.js [--profile jitter50|wifi|all] [--seconds 15]

import { $ } from "https://esm.sh/dax-sh@0.42.0"
import { parseArgs } from "jsr:@std/cli@1/parse-args"
import { launch } from "jsr:@astral/astral@0.5.6"
import { links, startShaper } from "./shaped_link.js"

const args = parseArgs(Deno.args, { string: ["profile", "seconds"], default: { profile: "all", seconds: "15" } })
const seconds = Number(args.seconds)
const chosen = args.profile === "all" ? Object.keys(links) : [args.profile]
// Chrome binds its sockets to the machine's addresses (real ones, with mDNS hiding off)
const host = Deno.networkInterfaces().find((entry) => entry.family === "IPv4" && !entry.address.startsWith("127."))?.address
if (!host) {
    throw new Error("no non-loopback IPv4 address")
}
const browser = await launch({ headless: true, args: ["--no-sandbox", "--disable-features=WebRtcHideLocalIpsWithMdns"] })
const page = await browser.newPage("about:blank")
for (const name of chosen) {
    for (const reliable of [false, true]) {
        // 1. offer without candidates, answer; the answerer's host candidate is the shaper's target
        const target = await page.evaluate(async (host, reliable) => {
            const gathered = (pc) => new Promise((resolve) => {
                if (pc.iceGatheringState === "complete") {
                    resolve()
                }
                pc.onicegatheringstatechange = () => pc.iceGatheringState === "complete" && resolve()
            })
            const sender = new RTCPeerConnection()
            const receiver = new RTCPeerConnection()
            const channel = sender.createDataChannel("bulk", reliable ? { ordered: true } : { ordered: false, maxRetransmits: 0 })
            await sender.setLocalDescription(await sender.createOffer())
            await gathered(sender)
            const offer = sender.localDescription.sdp.split("\r\n").filter((line) => !line.startsWith("a=candidate")).join("\r\n")
            await receiver.setRemoteDescription({ type: "offer", sdp: offer })
            await receiver.setLocalDescription(await receiver.createAnswer())
            await gathered(receiver)
            const answer = receiver.localDescription.sdp
            const candidate = answer.split("\r\n").find((line) => line.startsWith("a=candidate") && line.includes(` ${host} `) && line.includes(" udp "))
            Object.assign(globalThis, { test: { sender, receiver, channel, answer } })
            return Number(candidate?.split(" ")[5])
        }, { args: [host, reliable] })
        const shaper = startShaper(target, links[name], host)
        // 2. the sender only learns the shaper's port; the receiver answers its checks (peer-reflexive)
        const result = await page.evaluate(async (host, port, seconds) => {
            const { sender, receiver, channel, answer } = globalThis.test
            const sdp = answer.split("\r\n").filter((line) => !line.startsWith("a=candidate")).join("\r\n").replace(/(a=mid:[^\r\n]*)/, `$1\r\na=candidate:1 1 udp 2130706431 ${host} ${port} typ host`)
            await sender.setRemoteDescription({ type: "answer", sdp })
            let received = 0
            let measuring = false
            receiver.ondatachannel = (event) => {
                event.channel.onmessage = (message) => {
                    if (measuring) {
                        received += message.data.byteLength ?? message.data.size ?? message.data.length
                    }
                }
            }
            await new Promise((resolve, reject) => {
                channel.onopen = resolve
                setTimeout(() => reject(new Error("channel did not open")), 10000)
            })
            const chunk = new Uint8Array(16 * 1024)
            channel.bufferedAmountLowThreshold = 2 * 1024 * 1024
            let running = true
            const fill = () => {
                while (running && channel.bufferedAmount < 4 * 1024 * 1024) {
                    channel.send(chunk)
                }
            }
            channel.onbufferedamountlow = fill
            fill()
            await new Promise((resolve) => setTimeout(resolve, 3000))
            const timeline = []
            let last = 0
            const sampler = setInterval(() => {
                timeline.push(Math.round((received - last) / 1000))
                last = received
            }, 1000)
            measuring = true
            const started = performance.now()
            await new Promise((resolve) => setTimeout(resolve, seconds * 1000))
            measuring = false
            clearInterval(sampler)
            running = false
            const bytesPerSec = received / ((performance.now() - started) / 1000)
            const stats = []
            ;(await sender.getStats()).forEach((stat) => stat.type === "candidate-pair" && stat.nominated && stats.push({ rttMs: stat.currentRoundTripTime * 1000 }))
            sender.close()
            receiver.close()
            return { bytesPerSec, timeline, stats }
        }, { args: [host, shaper.port, seconds] })
        shaper.stop()
        console.log(`${name} ${reliable ? "reliable" : "unordered maxRetransmits 0"} timeline (KB/s): ${result.timeline.join(" ")}`)
        console.log(`RESULT chrome->chrome ${name} ${reliable ? "reliable" : "lossy"}: ${(result.bytesPerSec / 1000).toFixed(0)} KB/s (${(result.bytesPerSec * 8 / 1e6).toFixed(2)} Mb/s), shaper down ${shaper.down.packets} packets ${shaper.down.dropped} dropped ${shaper.down.lost} lost, ${JSON.stringify(result.stats)}`)
    }
}
await browser.close()
