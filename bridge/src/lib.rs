//! zenoh-web: view and drive a [zenoh](https://zenoh.io) system from a browser over WebRTC.
//!
//! One process bridges zenoh key expressions to browser data channels (one per subscription or
//! publisher) and H.264 video tracks, with a per-browser bandwidth allocator, heartbeats and
//! deadmen. The browser side is `client/zenoh_web.ts` in the repository.
//!
//! This crate is both the `zenoh-web` command and a library for embedding the server in another
//! application:
//!
//! ```no_run
//! # async fn run() -> anyhow::Result<()> {
//! let server = zenoh_web::Server::builder()
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
//! # Codecs
//!
//! A subscription may pick a transcoder by name (`codec: "ros2-image"`). The built-in `ros2-*` and
//! `dimos-*` codecs are registered by default; add your own by implementing [`Codec`] and passing
//! it to [`ServerBuilder::codec`]. A codec either produces [`VideoImage`]s, which the bridge turns
//! into H.264 on a video track (no browser code needed), or bytes for the data channel, which the
//! page decodes with a decoder registered through the client's `registerCodec(name, decoder)`.

#![warn(missing_docs)]

// zenoh's admin space deadlocked against concurrent declarations (eclipse-zenoh/zenoh#2619, unmerged);
// zenoh-web needs its fork, `bridge/forks/zenoh`. A crate depending on zenoh-web must add
// `[patch.crates-io] zenoh = { git = "https://github.com/jeff-hykin/zenoh-web", rev = "<same rev>" }`.
const _: u32 = zenoh::ZENOH_WEB_PATCHES;

mod acl;
mod allocator;
mod codec;
mod frame;
mod options;
mod pacing;
mod peer;
mod publisher;
mod server;
mod subscription;
mod video;

pub use codec::{Codec, CodecOutput, CodecSample, DecodedFrame, PixelFormat, VideoImage};
pub use server::{DEFAULT_PORT, HEALTH_PATH, RunningServer, Server, ServerBuilder};
/// The zenoh version this crate is built against (for [`ServerBuilder::session`] and
/// [`ServerBuilder::zenoh_config`]).
pub use zenoh;
