# zenoh-web-rtc-sctp: patches on top of rtc-sctp 0.21.0

Fork of [rtc-sctp](https://crates.io/crates/rtc-sctp) 0.21.0 (webrtc-rs, MIT OR Apache-2.0), kept in
<https://github.com/jeff-hykin/zenoh-web/tree/main/bridge/forks/rtc-sctp>. Every change is marked
`zenoh-web patch` in the source. Library name is unchanged (`rtc_sctp`).

1. **Retransmission timeout as Chrome's dcsctp computes it** (`src/association/timer.rs`). Bugs:
   `RTO_MAX` 60 s turned every tail loss into a multi-second stall of the whole association; and
   SRTT + 4 x RTTVAR with a 200 ms floor (zw.1) fired on Wi-Fi, where the RTT jumps from tens of ms
   to 400+ in one round trip with nothing lost: each such T3 timeout cut cwnd to one MTU and resent
   the whole flight (on a test link with RTT 10-50 ms, stalls to 430 ms and 0.5% loss: 5 timeouts in
   20 s, latency p95 605 ms, and seconds-long stalls of reliable channels). Change: dcsctp's values,
   `rto_min` 400 ms, `min_rtt_variance` 220 ms (RTTVAR never counts below it, so RTO >= SRTT +
   880 ms) and backoff capped at 3 s (Chrome's `max_timer_backoff_duration`). Same link: 0 timeouts,
   p95 243 ms (`test/throughput.js --profile spiky` in zenoh-web).
2. **Fragmented partially-reliable messages are abandoned whole** (`src/association/mod.rs`,
   `abandon_whole_messages`; RFC 3758 Sec 3.5 A3). Bug: only single-chunk messages were ever
   abandoned, so on a `maxRetransmits` / `maxPacketLifeTime` channel every message larger than one
   chunk was retransmitted (with T3 backoff) instead of dropped. Change: when any fragment of a fully
   in-flight message is abandoned, all its fragments are, and the fast-retransmit / T3 paths skip them.
3. **A reset stream's unsent chunks are dropped** (`src/queue/pending_queue.rs` `remove_stream`,
   called from the stream reset). Bug: chunks queued but not yet sent on a stream the peer reset got
   TSNs after the reset and landed on the next channel that reused the stream id. Change: drop them.
4. **A repeated stream reset isn't re-run** (`src/association/mod.rs`, `handle_reconfig_param`; RFC
   6525 Sec 5.2.2). Bug: a retransmitted reset request (its response crossed it) was performed again,
   by which time the peer may have reused the stream ids for new channels, silently killing them.
   Change: remember the last performed request and only re-answer duplicates.

Tests for 2–4 are in `src/endpoint/endpoint_test.rs` and `src/queue/queue_test.rs` (search `zenoh-web patch`).

Versioning: upstream version + `-zw.N` (`0.21.0-zw.2`); bump `N` for new fork fixes, reset it when rebasing on a new upstream.
