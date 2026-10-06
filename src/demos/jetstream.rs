//! JetStream: persistence on top of core NATS. A stream captures messages on disk, and
//! consumers read them with acknowledgements, so nothing is lost while a consumer is offline.

use std::time::Duration;

use anyhow::{Context as _, Result};
use async_nats::Client;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, PullConsumer, pull};
use async_nats::jetstream::message::PublishMessage;
use async_nats::jetstream::stream::{self, StorageType};
use async_nats::jetstream::{self, AckKind};
use futures::StreamExt;

use super::{boxed, detail, next_within, section, step};
use crate::domain::Order;

const STREAM: &str = "SHOWCASE_ORDERS";
const PREFIX: &str = "showcase.js.orders";
const CONSUMER: &str = "order-processor";

pub async fn run(client: &Client) -> Result<()> {
    section("JetStream: streams, acknowledged publishes and pull consumers");
    let js = jetstream::new(client.clone());

    // Drop any leftovers from a previous run, so sequences, consumers and the duplicate
    // detection window all start fresh and every run tells the same story.
    let _ = js.delete_stream(STREAM).await;

    // A stream is a durable, ordered log of every message published to its subjects.
    let mut stream = js
        .create_stream(stream::Config {
            name: STREAM.to_owned(),
            description: Some("Orders captured by rust-nats-showcase".to_owned()),
            subjects: vec![format!("{PREFIX}.>")],
            storage: StorageType::File,
            max_messages: 10_000,
            duplicate_window: Duration::from_secs(120),
            allow_direct: true, // lets clients read messages straight from the stream
            ..Default::default()
        })
        .await
        .context("creating a stream needs JetStream; start the server with `nats-server -js`")?;
    step(format!("stream '{STREAM}' captures `{PREFIX}.>` on disk"));

    // Publishing through the JetStream context returns an ack once the message is stored.
    step("publishing 5 orders and waiting for the storage acks:");
    for id in 1..=5 {
        let order = Order::sample(id);
        let publish = PublishMessage::build()
            .payload(order.to_bytes())
            .message_id(format!("order-{id}"));
        let ack = js
            .send_publish(order.subject(PREFIX), publish)
            .await?
            .await?;
        detail(format!(
            "{:<32} → stream={} seq={} duplicate={}",
            order.describe(),
            ack.stream,
            ack.sequence,
            ack.duplicate
        ));
    }

    // Idempotent publish: a repeated Nats-Msg-Id inside the duplicate window is dropped by the server.
    let first = Order::sample(1);
    let publish = PublishMessage::build()
        .payload(first.to_bytes())
        .message_id("order-1");
    let dup = js
        .send_publish(first.subject(PREFIX), publish)
        .await?
        .await?;
    step(format!(
        "re-publishing order-1 with the same message id → duplicate={} (server points at seq {} again)",
        dup.duplicate, dup.sequence
    ));

    let info = stream.info().await?;
    step(format!(
        "stream state: {} messages, sequences {}..={}, {} bytes on disk",
        info.state.messages, info.state.first_sequence, info.state.last_sequence, info.state.bytes
    ));

    // Direct get reads straight from the stream by subject, no consumer needed.
    let last_eu = stream
        .direct_get_last_for_subject(format!("{PREFIX}.eu.created"))
        .await?;
    detail(format!(
        "direct get, last EU message: seq {} {}",
        last_eu.sequence,
        Order::from_bytes(&last_eu.payload)?.describe()
    ));

    // A durable pull consumer keeps its position on the server; a restarted process resumes where it left off.
    let mut consumer: PullConsumer = stream
        .create_consumer(pull::Config {
            durable_name: Some(CONSUMER.to_owned()),
            ack_policy: AckPolicy::Explicit,
            ack_wait: Duration::from_secs(5),
            max_deliver: 5,
            ..Default::default()
        })
        .await?;
    step(format!(
        "durable pull consumer '{CONSUMER}' fetches a batch of up to 10:"
    ));
    let mut batch = consumer
        .fetch()
        .max_messages(10)
        .expires(Duration::from_secs(2))
        .messages()
        .await?;
    while let Some(msg) = batch.next().await {
        let msg = msg.map_err(boxed)?;
        let order = Order::from_bytes(&msg.payload)?;
        let meta = msg.info().map_err(boxed)?;
        if order.id == 3 {
            // NAK asks for redelivery, here after a short delay: at-least-once delivery in action.
            msg.ack_with(AckKind::Nak(Some(Duration::from_millis(100))))
                .await
                .map_err(boxed)?;
            detail(format!(
                "seq {:<2} {:<32} NAK  (simulated failure, redeliver in 100 ms)",
                meta.stream_sequence,
                order.describe()
            ));
        } else {
            msg.ack().await.map_err(boxed)?;
            detail(format!(
                "seq {:<2} {:<32} ACK",
                meta.stream_sequence,
                order.describe()
            ));
        }
    }

    tokio::time::sleep(Duration::from_millis(250)).await;
    step("fetching again: only the NAK'd message comes back, now on its second delivery:");
    let mut batch = consumer
        .fetch()
        .max_messages(10)
        .expires(Duration::from_secs(1))
        .messages()
        .await?;
    while let Some(msg) = batch.next().await {
        let msg = msg.map_err(boxed)?;
        let meta = msg.info().map_err(boxed)?;
        detail(format!(
            "seq {:<2} {:<32} delivered {} times → ACK",
            meta.stream_sequence,
            Order::from_bytes(&msg.payload)?.describe(),
            meta.delivered
        ));
        msg.ack().await.map_err(boxed)?;
    }
    let consumer_info = consumer.info().await?;
    detail(format!(
        "consumer '{}': pending={} ack_pending={} redelivered={}",
        consumer_info.name,
        consumer_info.num_pending,
        consumer_info.num_ack_pending,
        consumer_info.num_redelivered
    ));

    // An ephemeral consumer that only wants future messages, consumed as an endless stream.
    let live: PullConsumer = stream
        .create_consumer(pull::Config {
            deliver_policy: DeliverPolicy::New,
            ..Default::default()
        })
        .await?;
    let mut live_messages = live.messages().await?;
    step("ephemeral consumer with DeliverPolicy::New only sees what is published from now on:");
    for id in 6..=7 {
        let order = Order::sample(id);
        js.publish(order.subject(PREFIX), order.to_bytes())
            .await?
            .await?;
    }
    for _ in 0..2 {
        let msg = next_within(&mut live_messages, Duration::from_secs(2)).await??;
        detail(format!(
            "live ← seq {} {}",
            msg.info().map_err(boxed)?.stream_sequence,
            Order::from_bytes(&msg.payload)?.describe()
        ));
        msg.ack().await.map_err(boxed)?;
    }

    let info = stream.info().await?;
    step(format!(
        "stream '{STREAM}' now holds {} messages and has {} consumers",
        info.state.messages, info.state.consumer_count
    ));
    Ok(())
}
