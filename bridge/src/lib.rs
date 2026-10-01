//! zenoh-web: view and drive a [zenoh](https://zenoh.io) system from a browser over WebRTC.
//!
//! One process bridges zenoh key expressions to browser data channels (one per subscription or
//! publisher) and H.264 video tracks, with a per-browser bandwidth allocator, heartbeats and
//! deadmen. The browser side is `client/zenoh_web.ts` in the repository.
//!
//! This crate is the library for embedding the server in an application; the `zenoh-web` command
//! (with the ROS 2 / dimos codecs) is [zenoh-web-cli](https://github.com/jeff-hykin/zenoh-web-cli):
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
//! A subscription may pick a transcoder by name (`codec: "<name>"`). The server has none built in:
//! implement [`Codec`] and pass it to [`ServerBuilder::codec`] (the ROS 2 / dimos ones are
//! [zenoh-dimos-codecs](https://github.com/jeff-hykin/zenoh-dimos-codecs)). A codec either produces [`VideoImage`]s, which the bridge turns
//! into H.264 on a video track (no browser code needed), or bytes for the data channel, which the
//! page decodes with a decoder registered through the client's `registerCodec(name, decoder)`.

#![warn(missing_docs)]

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
