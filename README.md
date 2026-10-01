# zenoh-web

View and drive a zenoh system from a browser over a squeezed network: one small Rust bridge, WebRTC data channels, plain JS client. See [SPEC.md](SPEC.md).

```sh
nix develop            # rust toolchain + deno (optional)
deno task build       # bundles client/zenoh_web.ts into build/ next to examples/ and test/
cd bridge && cargo run --release -- --serve ../build --connect tcp/robot.local:7447
# open http://localhost:7448/examples/viewer.html
```

Flags: `--port` (7448), `--zenoh-config <file>`, `--connect <endpoint>` (repeatable), `--serve <dir>`.

Client: `client/zenoh_web.ts` (strict TypeScript, no deps; `deno task check`). Pages load it from esm.sh (`https://esm.sh/gh/<owner>/zenoh-web@<tag>/client/zenoh_web.ts`, transpiled on the fly) or from the local bundle that `deno task build` writes to `build/client/zenoh_web.js`. Viewer demo: `examples/viewer.html` (`?bridge=<url>&key=<keyexpr>`).

End-to-end test (real zenoh peer, bridge, headless Chrome over WebRTC): `deno task e2e`.

`bridge/vendor/` holds webrtc-rs crates with small fixes (each marked `zenoh-web patch`), applied via `[patch.crates-io]`:
- `rtc`: a channel the browser opens now sends with the reliability the browser asked for; upstream left every accepted channel ordered + fully reliable in the bridge's send direction.
- `rtc-sctp`:
  - when the browser resets a stream, its unsent chunks are dropped instead of being sent after the reset, where they landed on the next channel that reused the stream id;
  - a partially reliable message larger than one SCTP chunk is abandoned whole (RFC 3758 §3.5 A3); upstream never abandoned fragmented messages, so `latest`/`maxAge` channels retransmitted stale frames with T3 backoff (the 200 ms and 2.2 s latency outliers);
  - a retransmitted stream-reset request that was already performed is answered, not re-run (RFC 6525 §5.2.2), so it can't reset a stream id the browser has since reused;
  - retransmission timeout floor 1 s → 200 ms and backoff cap 60 s → 3 s (matching Chrome).
