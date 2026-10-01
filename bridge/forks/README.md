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

All four are published on crates.io as `0.21.0-zw.1` (upstream version + `-zw.N`; bump `N` for new
fork fixes). zenoh-web depends on them by exact version and path: inside this repository (and for a
git dependency on it) the path is used, and the published zenoh-web uses the crates.io copies.
Publish order (each needs the previous ones on crates.io): rtc-sctp, rtc-datachannel, rtc, webrtc;
run `cargo package --list` and `cargo publish --dry-run` in each directory first.

## zenoh (a patch, not a renamed fork)

`zenoh/` is zenoh 1.10.1 with the admin-space deadlock fixed (`zenoh/PATCHES.md`; upstream PR
eclipse-zenoh/zenoh#2619). zenoh-web passes zenoh types across its API (`Server::session`), so a
renamed package would be a different, incompatible crate; it is applied with `[patch.crates-io]`
instead, which only the top-level workspace honours. Every crate that depends on zenoh-web needs:

```toml
[patch.crates-io]
zenoh = { git = "https://github.com/jeff-hykin/zenoh-web", rev = "<the zenoh-web rev you use>" }
```

zenoh-web reads `zenoh::ZENOH_WEB_PATCHES`, so forgetting it is a compile error, not a hang.
