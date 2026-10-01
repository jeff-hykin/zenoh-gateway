# zenoh-web-rtc-datachannel: rtc-datachannel 0.21.0 on the SCTP fork

Fork of [rtc-datachannel](https://crates.io/crates/rtc-datachannel) 0.21.0 (webrtc-rs, MIT OR
Apache-2.0), kept in <https://github.com/jeff-hykin/zenoh-web/tree/main/bridge/forks/rtc-datachannel>.
Library name is unchanged (`rtc_datachannel`).

No source changes. Its `sctp` dependency is `zenoh-web-rtc-sctp` instead of `rtc-sctp`, so the
SCTP types it shares with `zenoh-web-rtc` come from the same (patched) crate.

Manifest: dev-dependencies removed. Versioning: upstream version + `-zw.N` (`0.21.0-zw.1`).
