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
