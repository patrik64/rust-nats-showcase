//! JetStream object store: blobs of any size, split into chunks that each fit in a message.

use std::io::Cursor;

use anyhow::{Context as _, Result};
use async_nats::Client;
use async_nats::jetstream::object_store::{self, ObjectMetadata};
use async_nats::jetstream::{self};
use futures::StreamExt;
use tokio::io::AsyncReadExt;

use super::{detail, section, step};

const BUCKET: &str = "showcase_blobs";
const OBJECT: &str = "report-2026-09.bin";
const CHUNK: usize = 256 * 1024;
/// Larger than the 1 MiB default max message size, so it cannot be a single message.
const SIZE: usize = 1_536_000;

pub async fn run(client: &Client) -> Result<()> {
    section("JetStream object store");
    let js = jetstream::new(client.clone());

    let store = js
        .create_object_store(object_store::Config {
            bucket: BUCKET.to_owned(),
            description: Some("Blobs stored by rust-nats-showcase".to_owned()),
            ..Default::default()
        })
        .await
        .context(
            "creating an object store needs JetStream; start the server with `nats-server -js`",
        )?;
    step(format!("object store '{BUCKET}' ready"));

    let data: Vec<u8> = (0..SIZE).map(|i| (i % 251) as u8).collect();
    let meta = ObjectMetadata {
        name: OBJECT.to_owned(),
        description: Some("Monthly report".to_owned()),
        chunk_size: Some(CHUNK),
        ..Default::default()
    };
    // Anything that implements AsyncRead can be uploaded: a file, a socket, an in-memory buffer.
    let info = store.put(meta, &mut Cursor::new(&data)).await?;
    step(format!(
        "put '{}': {} bytes stored as {} chunks of {} KiB, digest {}",
        info.name,
        info.size,
        info.chunks,
        CHUNK / 1024,
        info.digest.as_deref().unwrap_or("-")
    ));

    // Downloads stream chunk by chunk through AsyncRead; the digest is verified on the way.
    let mut object = store.get(OBJECT).await?;
    let mut downloaded = Vec::with_capacity(SIZE);
    object.read_to_end(&mut downloaded).await?;
    step(format!(
        "get '{OBJECT}': read {} bytes back, identical to the upload: {}",
        downloaded.len(),
        downloaded == data
    ));

    step("objects in the bucket:");
    let mut list = store.list().await?;
    while let Some(object) = list.next().await {
        let object = object?;
        detail(format!(
            "{:<24} {:>9} bytes  {}",
            object.name,
            object.size,
            object.description.as_deref().unwrap_or("")
        ));
    }

    store.delete(OBJECT).await?;
    step(format!("deleted '{OBJECT}'"));
    Ok(())
}
