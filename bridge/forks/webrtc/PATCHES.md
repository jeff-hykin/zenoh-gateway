# zenoh-web-webrtc: webrtc 0.21.0 on the rtc fork

Fork of [webrtc](https://crates.io/crates/webrtc) 0.21.0 (webrtc-rs, MIT OR Apache-2.0), kept in
<https://github.com/jeff-hykin/zenoh-web/tree/main/bridge/forks/webrtc>. Library name is unchanged (`webrtc`).

Its `rtc` dependency is `zenoh-web-rtc` instead of `rtc`, so applications built on it (zenoh-web)
get the patched SCTP and data channel code.

1. **A bound track learns the negotiated header extension ids** (`src/media_stream/track_local/`:
   `TrackLocal::update_parameters`, implemented by `TrackLocalStaticRTP` / `TrackLocalStaticSample`;
   `src/peer_connection/mod.rs` `refresh_track_parameters`, run after every applied local or remote
   description; marked `zenoh-web patch`). Bug: `add_track` binds the track at once, before
   negotiation, so its context kept the media engine's provisional extension ids; after the answer
   the sender validates packets against the negotiated ids, and every packet carrying an extension
   written by URI (e.g. `SampleWriter::with_extension(PlayoutDelay)`) was refused with "extension
   not found": no video at all. Change: hand the track its sender's current parameters after each
   description is applied.

Manifest: dev-dependencies, examples and tests removed (not vendored). Versioning: upstream version + `-zw.N` (`0.21.0-zw.2`).
