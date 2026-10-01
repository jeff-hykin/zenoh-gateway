# zenoh-web-rtc-sctp: patches on top of rtc-sctp 0.21.0

Fork of [rtc-sctp](https://crates.io/crates/rtc-sctp) 0.21.0 (webrtc-rs, MIT OR Apache-2.0), kept in
<https://github.com/jeff-hykin/zenoh-web/tree/main/bridge/forks/rtc-sctp>. Every change is marked
`zenoh-web patch` in the source. Library name is unchanged (`rtc_sctp`).

1. **Retransmission timeout floor and cap** (`src/association/timer.rs`). Bug: `RTO_MIN` 1 s and
   `RTO_MAX` 60 s turned every tail loss into a multi-second stall of the whole association. Change:
   200 ms / 3 s, matching Chrome's dcsctp data-channel settings.
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

Versioning: upstream version + `-zw.N` (`0.21.0-zw.1`); bump `N` for new fork fixes, reset it when rebasing on a new upstream.
