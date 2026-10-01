# Forked webrtc-rs crates

webrtc-rs 0.21.0 crates with small fixes, renamed so that every crate depending on zenoh-web gets
the fixed code (a `[patch.crates-io]` section only applies in the top-level workspace, so
dependents would silently build the unpatched upstream crates). Each directory's `PATCHES.md` lists
its changes; license files are upstream's (MIT OR Apache-2.0, © WebRTC.rs).

| package | upstream | |
|---|---|---|
| `zenoh-web-rtc-sctp` | `rtc-sctp` | the SCTP fixes |
| `zenoh-web-rtc-datachannel` | `rtc-datachannel` | unchanged, on the SCTP fork |
| `zenoh-web-rtc` | `rtc` | the data channel reliability fix, on the two forks above |
| `zenoh-web-webrtc` | `webrtc` | unchanged, on the rtc fork |

zenoh-web depends on them by version and path, so a git dependency on zenoh-web builds them from
this directory. Publish order (each needs the previous ones on crates.io): rtc-sctp, rtc-datachannel, rtc, webrtc.
