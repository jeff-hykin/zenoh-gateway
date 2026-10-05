//! A relay: connect to bridge A as a client, take one camera at full quality, decode it, and serve it
//! again through this process's own zenoh-web server B (whose allocator then adapts it to B's viewers).
//!
//! `cargo run --example relay_sketch --features client -- http://robot.local:7448 camera/front ros2-image 7449`,
//! then subscribe on B to `relay/camera/front` with encoding `relay-rgb`.
//!
//! Decoding is only needed because B re-encodes per viewer; a relay that forwards A's stream as is
//! would instead give B an encoding whose `video_encoder` returns the access units it was handed.

use anyhow::{Context, Result};
use zenoh_web::client::{Client, ClientOptions, Message, SubscribeOptions};
use zenoh_web::{Channel, MessageEncoding, EncodingOutput, EncodingSample, DecodedFrame, RunningServer, Server, VideoImage};

/// B's encoding: `u32 width | u32 height | RGB8` → a picture for B's H.264 encoder.
pub struct RelayRgb;

impl MessageEncoding for RelayRgb {
    fn name(&self) -> &str {
        "relay-rgb"
    }

    fn output(&self) -> EncodingOutput {
        EncodingOutput::Video
    }

    fn decode(&self, sample: &EncodingSample<'_>, _channel: Channel) -> Result<DecodedFrame> {
        let size = |at: usize| u32::from_le_bytes(sample.payload[at..at + 4].try_into().unwrap());
        Ok(DecodedFrame::Video(VideoImage::rgb8(size(0), size(4), sample.payload[8..].to_vec())?))
    }
}

/// Starts B on `bind` and relays `camera_key` (through `codec`, a video encoding of A) from A to B's `relay/<camera_key>`.
pub async fn relay(source_url: &str, camera_key: &str, codec: &str, bind: &str) -> Result<RunningServer> {
    let source = Client::connect(source_url, ClientOptions::default()).await?;
    let options = SubscribeOptions { encoding: Some(codec.to_owned()), ..Default::default() };
    let mut camera = source.subscribe(camera_key, options).await?;
    let mut config = zenoh_web::zenoh::Config::default();
    config.insert_json5("scouting/multicast/enabled", "false").map_err(|error| anyhow::anyhow!("{error}"))?;
    let server = Server::builder().zenoh_config(config).encoding(RelayRgb).build().await?;
    let session = server.session().clone();
    let key = format!("relay/{camera_key}");
    // openh264's decoder stays on one thread; frames go to it, pictures come back
    let (frames_tx, frames_rx) = std::sync::mpsc::channel::<Vec<u8>>();
    let (pictures_tx, mut pictures_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(2);
    std::thread::spawn(move || -> Result<()> {
        let mut decoder = openh264::decoder::Decoder::new()?;
        for access_unit in frames_rx {
            let Ok(Some(picture)) = decoder.decode(&access_unit) else { continue };
            let (width, height) = openh264::formats::YUVSource::dimensions(&picture);
            let mut payload = [(width as u32).to_le_bytes(), (height as u32).to_le_bytes()].concat();
            payload.resize(8 + width * height * 3, 0);
            picture.write_rgb8(&mut payload[8..]);
            if pictures_tx.blocking_send(payload).is_err() {
                break;
            }
        }
        Ok(())
    });
    tokio::spawn(async move {
        while let Some(message) = camera.recv().await {
            if let Message::Video(frame) = message
                && frames_tx.send(frame.data).is_err()
            {
                break;
            }
        }
        source.close().await;
    });
    tokio::spawn(async move {
        while let Some(payload) = pictures_rx.recv().await {
            let _ = session.put(&key, payload).await;
        }
    });
    server.bind(bind).await
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [source, camera, codec, port] = &args[..] else {
        anyhow::bail!("usage: relay_sketch <bridge A url> <camera key> <A's video encoding> <port for B>");
    };
    let running = relay(source, camera, codec, &format!("0.0.0.0:{port}")).await.context("starting the relay")?;
    println!("relaying {camera} on http://{} as relay/{camera} (encoding relay-rgb)", running.local_addr());
    tokio::signal::ctrl_c().await?;
    running.shutdown().await
}
