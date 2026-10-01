# zenoh-web-webrtc: webrtc 0.21.0 on the rtc fork

Fork of [webrtc](https://crates.io/crates/webrtc) 0.21.0 (webrtc-rs, MIT OR Apache-2.0), kept in
<https://github.com/jeff-hykin/zenoh-web/tree/main/bridge/forks/webrtc>. Library name is unchanged (`webrtc`).

No source changes. Its `rtc` dependency is `zenoh-web-rtc` instead of `rtc`, so applications built
on it (zenoh-web) get the patched SCTP and data channel code.

Manifest: dev-dependencies, examples and tests removed (not vendored). Versioning: upstream version + `-zw.N` (`0.21.0-zw.1`).
