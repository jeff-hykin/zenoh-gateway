// A link between headless Chrome and a gateway, shaped in userspace: Chrome's UDP goes through a rate limit (both ways,
// a drop-tail queue of `queueSeconds` at the current rate, following a schedule), then delay, Wi-Fi-like jitter and loss.
// A signaling proxy in front of the gateway rewrites the SDP so ICE can only use the shaper. Used by degrade.ts and
// video_battery.ts.

/** schedule: [seconds from the link's start, bytes/s] steps (0: a blackout, everything dropped); jitterMs: extra delay
 *  up to this, redrawn every 200 ms */
export type LinkShape = { schedule: [number, number][]; delayMs: number; jitterMs: number; loss: number }

const queueSeconds = 1

/** One direction of the link: rate-limited (by the cell's schedule) drop-tail queue, then delay + jitter, with loss. */
function shapedDirection(cell: LinkShape, linkStart: () => number, deliver: (data: Uint8Array) => void, offered: { bytes: number; packets: number }) {
    const rateNow = () => {
        const t = (performance.now() - linkStart()) / 1000
        return cell.schedule.filter(([at]) => t >= at).at(-1)![1]
    }
    const queue: { data: Uint8Array; arrived: number }[] = []
    let queuedBytes = 0
    let freeAt = 0
    let draining = false
    let jitter = 0
    let jitterDrawnAt = -Infinity
    let lastDeliverAt = 0
    const delayNow = (now: number) => {
        if (now - jitterDrawnAt >= 200) {
            jitterDrawnAt = now
            jitter = Math.random() * cell.jitterMs
        }
        return cell.delayMs + jitter
    }
    const drain = () => {
        const now = performance.now()
        while (queue.length > 0) {
            const { data, arrived } = queue[0]
            const leaves = Math.max(freeAt, arrived) + (data.length / rateNow()) * 1000
            if (leaves > now) {
                setTimeout(drain, leaves - now)
                return
            }
            queue.shift()
            queuedBytes -= data.length
            freeAt = leaves
            // packets stay in order: a jitter drop never lets one overtake the one before it
            lastDeliverAt = Math.max(lastDeliverAt, leaves + delayNow(leaves))
            setTimeout(() => deliver(data), Math.max(0, lastDeliverAt - performance.now()))
        }
        draining = false
    }
    return (data: Uint8Array) => {
        offered.bytes += data.length
        offered.packets++
        if (rateNow() === 0 || Math.random() < cell.loss || queuedBytes + data.length > Math.max(20_000, rateNow() * queueSeconds)) {
            return
        }
        queue.push({ data, arrived: performance.now() })
        queuedBytes += data.length
        if (!draining) {
            draining = true
            drain()
        }
    }
}

/** Starts the shaper and its signaling proxy for the gateway at `serverUrl`: pages connect to `url`. */
export function shapedLink(serverUrl: string, cell: LinkShape) {
let linkStartedAt = 0
const sockets: Deno.DatagramConn[] = []
// what each side tried to send, for the summary: a saturated uplink delays the downlink too (its SCTP acks wait)
const offeredDown = { bytes: 0, packets: 0 }
const offeredUp = { bytes: 0, packets: 0 }
const startShaper = (targetPort: number) => {
    // (every interface: Firefox offers only its LAN address, and a socket there can't reach 127.0.0.1)
    const chromeSide = Deno.listenDatagram({ transport: "udp", hostname: "0.0.0.0", port: 0 })
    const serverSide = Deno.listenDatagram({ transport: "udp", hostname: "127.0.0.1", port: 0 })
    sockets.push(chromeSide, serverSide)
    let chromeAddress: Deno.NetAddr | null = null
    const down = shapedDirection(cell, () => linkStartedAt, (data) => {
        if (chromeAddress) {
            chromeSide.send(data, chromeAddress).catch(() => {})
        }
    }, offeredDown)
    const up = shapedDirection(cell, () => linkStartedAt, (data) => {
        serverSide.send(data, { transport: "udp", hostname: "127.0.0.1", port: targetPort }).catch(() => {})
    }, offeredUp)
    ;(async () => {
        for await (const [data, address] of chromeSide) {
            chromeAddress = address as Deno.NetAddr
            up(data)
        }
    })().catch(() => {})
    ;(async () => {
        for await (const [data] of serverSide) {
            down(data)
        }
    })().catch(() => {})
    return (chromeSide.addr as Deno.NetAddr).port
}
// a signaling proxy rewrites the SDP so ICE can only use the shaper
const proxy = Deno.serve({ hostname: "127.0.0.1", port: 0, onListen() {} }, async (request) => {
    const url = new URL(request.url)
    if (url.pathname !== "/offer") {
        return fetch(`${serverUrl}${url.pathname}${url.search}`)
    }
    // no network, no signaling either
    const t = linkStartedAt === 0 ? 0 : (performance.now() - linkStartedAt) / 1000
    if (cell.schedule.filter(([at]) => t >= at).at(-1)![1] === 0) {
        return new Response("link down", { status: 503 })
    }
    const offer = await request.json()
    // the browser's own IPv4 host address (Firefox's only candidate), offered beside loopback
    const browserAddress = (offer.sdp as string).split("\r\n").map((line) => line.match(/^a=candidate:\S+ 1 udp \d+ (\d+\.\d+\.\d+\.\d+) \d+ typ host/i)?.[1]).find((address) => address !== undefined && !address.startsWith("127."))
    offer.sdp = offer.sdp.split("\r\n").filter((line: string) => !line.startsWith("a=candidate")).join("\r\n")
    const answer = await (await fetch(`${serverUrl}/offer`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(offer) })).json()
    const lines: string[] = answer.sdp.split("\r\n")
    const loopback = lines.find((line) => line.startsWith("a=candidate") && line.includes(" 127.0.0.1 "))
    const port = startShaper(Number(loopback?.split(" ")[5]))
    // the schedule runs from the first connection: a reconnect doesn't restart it
    if (linkStartedAt === 0) {
        linkStartedAt = performance.now()
    }
    let replaced = false
    answer.sdp = lines.flatMap((line) => {
        if (!line.startsWith("a=candidate")) {
            return [line]
        }
        if (replaced) {
            return []
        }
        replaced = true
        return [`a=candidate:1 1 udp 2130706431 127.0.0.1 ${port} typ host`, ...(browserAddress ? [`a=candidate:2 1 udp 2130706430 ${browserAddress} ${port} typ host`] : [])]
    }).join("\r\n")
    return Response.json(answer)
})
    return {
        url: `http://127.0.0.1:${proxy.addr.port}`,
        /** when the link started shaping (performance.now()); 0 before the page's first offer */
        startedAt: () => linkStartedAt,
        offeredDown,
        offeredUp,
        async close() {
            await proxy.shutdown()
            for (const socket of sockets) {
                socket.close()
            }
        },
    }
}
