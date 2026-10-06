//! nats-showcase: a guided tour of NATS from Rust with the official `async-nats` client.
//!
//! Every subcommand is one self-contained demo; `all` (the default) runs them in sequence.

mod demos;
mod domain;

use std::time::Duration;

use anyhow::{Context as _, Result};
use async_nats::{Client, ConnectOptions, Event};
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(
    name = "nats-showcase",
    version,
    about = "A guided tour of NATS from Rust"
)]
struct Cli {
    /// NATS server URL(s), comma separated
    #[arg(long, env = "NATS_URL", default_value = "nats://127.0.0.1:4222")]
    url: String,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug, Clone, Copy, PartialEq, Eq)]
enum Command {
    /// Core publish/subscribe with subject wildcards and headers
    #[command(name = "pubsub")]
    PubSub,
    /// Request/reply: inboxes, timeouts, no-responders, scatter-gather
    RequestReply,
    /// Queue groups: load-balance one subject across several workers
    QueueGroup,
    /// JetStream streams: acked publishes, dedup, durable pull consumers, NAK redelivery
    #[command(name = "jetstream")]
    JetStream,
    /// JetStream key-value store: put/get, compare-and-swap, watch, history
    Kv,
    /// JetStream object store: chunked blobs
    ObjectStore,
    /// NATS micro-services: endpoints, error responses, discovery, stats
    Service,
    /// Run every demo in sequence (default)
    All,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            // The client logs its own connection handling at INFO; keep that quiet by default.
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,async_nats=warn".into()),
        )
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let client = connect(&cli.url).await?;

    match cli.command.unwrap_or(Command::All) {
        Command::PubSub => demos::pubsub::run(&client).await?,
        Command::RequestReply => demos::request_reply::run(&client).await?,
        Command::QueueGroup => demos::queue_group::run(&client).await?,
        Command::JetStream => demos::jetstream::run(&client).await?,
        Command::Kv => demos::kv::run(&client).await?,
        Command::ObjectStore => demos::object_store::run(&client).await?,
        Command::Service => demos::service::run(&client).await?,
        Command::All => {
            demos::pubsub::run(&client).await?;
            demos::request_reply::run(&client).await?;
            demos::queue_group::run(&client).await?;
            demos::jetstream::run(&client).await?;
            demos::kv::run(&client).await?;
            demos::object_store::run(&client).await?;
            demos::service::run(&client).await?;
        }
    }

    // Drain flushes pending messages, unsubscribes everything and then closes the connection.
    client.drain().await?;
    println!("\nconnection drained, bye");
    Ok(())
}

async fn connect(url: &str) -> Result<Client> {
    let client = ConnectOptions::new()
        .name("nats-showcase")
        .connection_timeout(Duration::from_secs(5))
        .request_timeout(Some(Duration::from_secs(5)))
        .max_reconnects(10)
        // The client reconnects on its own; the callback just lets us observe it.
        .event_callback(|event| async move {
            match event {
                Event::Connected => tracing::info!("connection event: {event}"),
                other => tracing::warn!("connection event: {other}"),
            }
        })
        .connect(url)
        .await
        .with_context(|| {
            format!("could not connect to NATS at {url}; start a server with `docker compose up -d` or `nats-server -js`")
        })?;

    let info = client.server_info();
    tracing::info!(
        "connected to {url}: nats-server {} '{}', max payload {} bytes",
        info.version,
        info.server_name,
        info.max_payload
    );
    Ok(client)
}
