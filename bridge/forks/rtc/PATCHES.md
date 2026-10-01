# zenoh-web-rtc: patches on top of rtc 0.21.0

Fork of [rtc](https://crates.io/crates/rtc) 0.21.0 (webrtc-rs, MIT OR Apache-2.0), kept in
<https://github.com/jeff-hykin/zenoh-web/tree/main/bridge/forks/rtc>. Library name is unchanged (`rtc`).

1. **Peer-opened data channels honor their reliability** (`src/peer_connection/handler/sctp.rs`, marked
   `zenoh-web patch`). Bug: the DCEP reliability (unordered, `maxRetransmits`, `maxPacketLifeTime`) was
   only applied to channels this side opened, so every channel a browser opened stayed ordered and fully
   reliable in the server's send direction. Change: apply the peer's DATA_CHANNEL_OPEN parameters to
   the stream when the open message arrives.
2. **Dependencies point at the forks**: `sctp` is `zenoh-web-rtc-sctp`, `datachannel` is
   `zenoh-web-rtc-datachannel` (which uses the same SCTP fork, so their types match).

Manifest: dev-dependencies, examples and tests removed (not vendored).
Versioning: upstream version + `-zw.N` (`0.21.0-zw.2`: no new change of its own, it follows the
SCTP fork's zw.2); bump `N` for new fork fixes, reset it when rebasing on a new upstream.
