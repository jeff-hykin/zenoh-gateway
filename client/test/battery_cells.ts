// The bad links of the video battery (video_battery.ts), and of udp_shaper.ts for other stacks: each a userspace
// shaper's rate schedule (bytes/s from a second on), one-way delay, jitter (up to, redrawn every 200 ms) and loss.

import type { LinkShape } from "./shaped_link.ts"

// phases: [start s, end s, label] measured over their last `measure` seconds; clean: no delay or loss beyond the rate
export type Cell = LinkShape & { name: string; seconds: number; phases?: [number, number, string][]; clean?: boolean }

export const cells: Cell[] = [
    ...([["20mbit", 2_500_000], ["8mbit", 1_000_000], ["4mbit", 500_000], ["2mbit", 250_000], ["1mbit", 125_000], ["500kbit", 62_500], ["250kbit", 31_250], ["100kbit", 12_500]] as [string, number][])
        .map(([name, rate]) => ({ name, seconds: 25, schedule: [[0, rate]] as [number, number][], delayMs: 20, jitterMs: 0, loss: 0, clean: true })),
    { name: "rtt300", seconds: 25, schedule: [[0, 500_000]], delayMs: 150, jitterMs: 0, loss: 0 },
    { name: "wifi_jitter", seconds: 25, schedule: [[0, 500_000]], delayMs: 5, jitterMs: 200, loss: 0.01 },
    { name: "loss3", seconds: 25, schedule: [[0, 500_000]], delayMs: 20, jitterMs: 0, loss: 0.03 },
    // a sudden 16x drop and back: each phase measured once it had 10 s to settle
    { name: "step", seconds: 60, schedule: [[0, 1_000_000], [20, 62_500], [40, 1_000_000]], delayMs: 20, jitterMs: 0, loss: 0, phases: [[0, 20, "8mbit"], [20, 40, "500kbit"], [40, 60, "8mbit again"]] },
]
