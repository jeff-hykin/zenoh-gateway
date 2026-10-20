//! zenoh-gateway: view and drive a [zenoh](https://zenoh.io) system from a browser over WebRTC.
//!
//! One process gateways zenoh key expressions to browser data channels (one per subscription or
//! publisher) and H.264 video tracks, with a per-browser bandwidth allocator, heartbeats and
//! deadmen. The browser side is `client/zenoh_gateway.ts` in the repository.
//!
//! This crate is the library for embedding the server in an application; the `zenoh-gateway` command
//! (with a set of robotics encodings) is [zenoh-gateway-cli](https://github.com/jeff-hykin/zenoh-gateway-cli):
//!
//! ```no_run
//! # async fn run() -> anyhow::Result<()> {
//! let server = zenoh_gateway::Server::builder()
//!     .connect("tcp/127.0.0.1:7447")
//!     .serve_dir("examples")
//!     .build()
//!     .await?;
//! let running = server.bind(("0.0.0.0", 7448)).await?;
//! println!("open http://{}/", running.local_addr());
//! tokio::signal::ctrl_c().await?;
//! running.shutdown().await?; // fires deadmen, closes browsers and the zenoh session
//! # Ok(())
//! # }
//! ```
//!
//! A host that already has a zenoh session passes it with [`ServerBuilder::session`]; one that
//! already runs an axum server mounts [`Server::router`].
//!
//! # Encodings and channels
//!
//! A subscription may pick a message encoding by name (`encoding: "<name>"`), the [`Channel`] its output travels on
//! (`channel`) and options for it (`encodeOptions`). The server has none built in: implement [`MessageEncoding`] and pass
//! it to [`ServerBuilder::encoding`] (e.g. the robotics ones of
//! [zenoh-dimos-codecs](https://github.com/jeff-hykin/zenoh-dimos-codecs)). An encoding produces [`VideoImage`]s, which
//! the gateway encodes (H.264 or AV1 built in, others through [`ServerBuilder::video_encoder`]) on a video track (no
//! browser code needed), PCM for an Opus track, or bytes for the data channel: a [`Fields`] message the client decodes
//! by itself, or its own format, which the page decodes with a decoder registered through the client's
//! `registerEncoding(name, decoder)`. Data channel messages can be zstd-compressed per subscription
//! (`compress: "zstd"`, or the encoding's [`MessageEncoding::default_compress`]).

#![warn(missing_docs)]

mod allocator;
mod api;
mod audio;
mod auth;
#[cfg(feature = "client")]
pub mod client;
mod encoding;
pub mod fields;
mod frame;
mod ice;
mod media;
mod options;
mod pacing;
mod peer;
mod publisher;
mod rtp_pacer;
mod server;
mod subscription;

pub use encoding::{AudioPcm, Channel, EncodeOptions, MessageEncoding, EncodingOutput, EncodingSample, Compress, DecodedFrame, EncodedVideo, H264Encoder, PixelFormat, VideoEncoder, VideoFormat, VideoImage, VideoPolicy, VideoTarget};
pub use auth::{Acl, AclMessage, AclRule, Flow, Grant, OnRefused, Permission};
pub use fields::Fields;
pub use ice::{ICE_HOOK_TIMEOUT, IceRequest, IceServer, IceServersFn, IceSide, turn_credentials};
#[cfg(feature = "cloudflare")]
pub use ice::CloudflareTurn;
#[doc(hidden)]
pub use peer::live_peer_states;
pub use server::{Authorize, DEFAULT_PORT, HEALTH_PATH, ICE_PATH, RunningServer, SIGNALLING_PREFIX, Server, ServerBuilder};
/// The zenoh version this crate is built against (for [`ServerBuilder::session`] and
/// [`ServerBuilder::zenoh_config`]).
pub use zenoh;

/// Spawns `task`; a panic in it is logged and runs `on_panic` (closing only what the task served) instead of
/// leaving that part of the gateway dead. The returned handle finishes after both.
pub(crate) fn spawn_isolated(what: String, task: impl Future<Output = ()> + Send + 'static, on_panic: impl Future<Output = ()> + Send + 'static) -> tokio::task::JoinHandle<()> {
    let handle = tokio::spawn(task);
    tokio::spawn(async move {
        if let Err(error) = handle.await && error.is_panic() {
            log::error!("{what} panicked, closing it: {error}");
            on_panic.await;
        }
    })
}

/// Seconds as a `Duration` that cannot panic: NaN or negative is zero, beyond a day is a day (so `Instant + it` cannot overflow either).
pub(crate) fn secs(seconds: f64) -> std::time::Duration {
    std::time::Duration::from_secs_f64(if seconds > 0.0 { seconds.min(86_400.0) } else { 0.0 })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[test]
    fn secs_never_panics() {
        assert_eq!(super::secs(f64::NAN), Duration::ZERO);
        assert_eq!(super::secs(-1.0), Duration::ZERO);
        assert_eq!(super::secs(1.0 / 1e-30), Duration::from_secs(86_400));
        assert_eq!(super::secs(f64::INFINITY), Duration::from_secs(86_400));
        assert_eq!(super::secs(0.25), Duration::from_millis(250));
    }

    #[tokio::test]
    async fn a_panicked_task_runs_only_its_own_cleanup() {
        let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
        super::spawn_isolated("test".into(), async { panic!("boom") }, async move { closed_tx.send(()).unwrap() }).await.unwrap();
        closed_rx.await.expect("on_panic ran");
        let (unused_tx, unused_rx) = tokio::sync::oneshot::channel::<()>();
        super::spawn_isolated("test".into(), async {}, async move { drop(unused_tx) }).await.unwrap();
        drop(unused_rx);
    }
}
