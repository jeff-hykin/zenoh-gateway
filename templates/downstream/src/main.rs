//! `my-app [seconds]`: picks a video encoder (hardware first), serves zenoh-web on 127.0.0.1 with it and the dimos /
//! ROS 2 codecs (isolated zenoh session), connects the Rust client to list topics, serves for `seconds` (default 0) and exits.

use anyhow::Result;
use std::time::Duration;
use zenoh_web::client::{Client, ClientOptions};
use zenoh_web::{Server, zenoh};

#[tokio::main]
async fn main() -> Result<()> {
    let seconds: u64 = std::env::args().nth(1).map(|text| text.parse()).transpose()?.unwrap_or(0);
    let encoder = zenoh_dimos_codecs::encoders::select(zenoh_dimos_codecs::encoders::Backend::Auto)?;
    println!("video encoder: {}", encoder.name);

    let mut config = zenoh::Config::default();
    config.insert_json5("scouting/multicast/enabled", "false").map_err(anyhow::Error::msg)?;
    config.insert_json5("listen/endpoints", "[]").map_err(anyhow::Error::msg)?;
    let mut builder = Server::builder().zenoh_config(config);
    for codec in zenoh_dimos_codecs::all() {
        builder = builder.shared_codec(codec);
    }
    if let Some(factory) = encoder.factory {
        builder = builder.video_encoder(factory);
    }
    let running = builder.build().await?.bind("127.0.0.1:0").await?;
    let url = format!("http://{}", running.local_addr());
    println!("serving on {url}");

    let client = Client::connect(&url, ClientOptions::default()).await?;
    println!("topics: {}", client.list_topics("**", None).await?.len());
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    client.close().await;
    running.shutdown().await?;
    Ok(())
}
