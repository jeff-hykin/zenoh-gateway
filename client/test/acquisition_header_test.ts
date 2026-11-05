import { assertEquals } from "jsr:@std/assert@1"
import { decodeHeader } from "../zenoh_gateway.ts"

// Wire order documented by frame::encode_header. Also covers sample metadata ahead of the extension.
function header(timeNs: bigint, sample = false): Uint8Array {
    const bytes: number[] = []
    const number = (size: number, write: (v: DataView) => void) => {
        const data = new Uint8Array(size); write(new DataView(data.buffer)); bytes.push(...data)
    }
    const string = (s: string) => {
        const data = new TextEncoder().encode(s)
        number(2, v => v.setUint16(0, data.length, true)); bytes.push(...data)
    }
    number(8, v => v.setFloat64(0, Number(timeNs) / 1e9, true))
    string("g1/camera")
    number(4, v => v.setInt32(0, 0, true)); number(4, v => v.setUint32(0, 0, true))
    bytes.push(4 | (sample ? 2 : 0)); number(4, v => v.setUint32(0, 0, true))
    if (sample) {
        string("application/lcm"); number(4, v => v.setUint32(0, 3, true)); bytes.push(1, 2, 3)
    }
    string("sim-proof")
    number(8, v => v.setBigInt64(0, 3n, true)); number(8, v => v.setBigInt64(0, timeNs, true))
    string("camera"); number(8, v => v.setBigInt64(0, 2n ** 60n + 7n, true))
    return new Uint8Array(bytes)
}

Deno.test("acquisition metadata retains zero time and exact int64s alongside sample metadata", () => {
    for (const time_ns of [0n, 1761350400123456789n]) for (const sample of [false, true]) {
        const bytes = header(time_ns, sample)
        const decoded = decodeHeader(bytes)!
        assertEquals(decoded.header.acquisition, { clock_id: "sim-proof", epoch: 3n, time_ns, source_id: "camera", sequence: 2n ** 60n + 7n })
        assertEquals(decoded.header.frameId, "g1/camera")
        assertEquals(decoded.encoding, sample ? "application/lcm" : undefined)
        assertEquals(decoded.attachment, sample ? new Uint8Array([1, 2, 3]) : undefined)
        for (let end = bytes.length - 43; end < bytes.length; end++) assertEquals(decodeHeader(bytes.subarray(0, end)), undefined)
    }
})

Deno.test("legacy header has no fabricated acquisition clock", () => {
    const bytes = header(0n).slice(0, 32)
    bytes[27] = 0 // flags after f64, u16, frameId, sec and nanosec
    assertEquals(decodeHeader(bytes)?.header.acquisition, undefined)
    assertEquals(decodeHeader(bytes)?.header.frameId, "g1/camera")
})
