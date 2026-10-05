//! `zenoh-gateway-example [seconds]`: an isolated zenoh session (no scouting, no listeners), a zenoh-gateway server on
//! 127.0.0.1, a Rust client that lists the topics, then it serves for `seconds` (default 0) and shuts down.

use anyhow::Result;
use std::time::Duration;
use zenoh_gateway::client::{Client, ClientOptions};
use zenoh_gateway::{Server, zenoh};

#[tokio::main]
async fn main() -> Result<()> {
    let argument = std::env::args().nth(1);
    if matches!(argument.as_deref(), Some("-h" | "--help")) {
        println!("usage: zenoh-gateway-example [seconds]  (loopback zenoh-gateway server + Rust client smoke test)");
        return Ok(());
    }
    let seconds: u64 = argument.map(|text| text.parse()).transpose()?.unwrap_or(0);

    let mut config = zenoh::Config::default();
    config.insert_json5("scouting/multicast/enabled", "false").map_err(anyhow::Error::msg)?;
    config.insert_json5("listen/endpoints", "[]").map_err(anyhow::Error::msg)?;
    let session = zenoh::open(config).await.map_err(anyhow::Error::msg)?;
    let running = Server::builder().session(session.clone()).build().await?.bind("127.0.0.1:0").await?;
    let url = format!("http://{}", running.local_addr());
    println!("serving on {url}");

    let _hello = session.declare_publisher("example/hello").await.map_err(anyhow::Error::msg)?;
    let client = Client::connect(&url, ClientOptions::default()).await?;
    let topics = client.list_topics("example/**", None).await?;
    println!("client sees: {:?}", topics.iter().map(|topic| &topic.key).collect::<Vec<_>>());

    tokio::time::sleep(Duration::from_secs(seconds)).await;
    client.close().await;
    running.shutdown().await?;
    println!("ok");
    Ok(())
}
