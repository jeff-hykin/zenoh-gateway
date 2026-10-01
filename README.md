# zenoh-web

View and drive a zenoh system from a browser over a squeezed network: one small Rust bridge, WebRTC data channels, plain JS client. See [SPEC.md](SPEC.md).

```sh
nix develop            # rust toolchain + deno (optional)
cd bridge && cargo run --release -- --serve .. --connect tcp/robot.local:7447
# open http://localhost:7448/examples/viewer.html
```

Flags: `--port` (7448), `--zenoh-config <file>`, `--connect <endpoint>` (repeatable), `--serve <dir>`.

Client: `client/zenoh_web.js` (no build, no deps). Viewer demo: `examples/viewer.html` (`?bridge=<url>&key=<keyexpr>`).

End-to-end test (real zenoh peer, bridge, headless Chrome over WebRTC): `deno run --allow-all test/e2e.js`.

`bridge/vendor/` holds webrtc-rs crates with small fixes (each marked `zenoh-web patch`), applied via `[patch.crates-io]`:
- `rtc`: a channel the browser opens now sends with the reliability the browser asked for; upstream left every accepted channel ordered + fully reliable in the bridge's send direction.
- `rtc-sctp`: when the browser resets a stream, its unsent chunks are dropped instead of being sent after the reset, where they landed on the next channel that reused the stream id; retransmission timeout floor 1 s → 200 ms and backoff cap 60 s → 3 s (matching Chrome).
