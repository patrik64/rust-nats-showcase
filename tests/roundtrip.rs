//! Integration tests against a live NATS server with JetStream.
//!
//! They skip themselves (pass with a note on stderr) when no server is reachable at
//! `NATS_URL` (default `nats://127.0.0.1:4222`), so `cargo test` works without one.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_nats::jetstream::consumer::{PullConsumer, pull};
use async_nats::jetstream::{self, stream};
use async_nats::{Client, ConnectOptions};
use bytes::Bytes;
use futures::StreamExt;

async fn connect() -> Option<Client> {
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_owned());
    match ConnectOptions::new()
        .connection_timeout(Duration::from_secs(1))
        .connect(&url)
        .await
    {
        Ok(client) => Some(client),
        Err(err) => {
            eprintln!("skipping: no NATS server reachable at {url} ({err})");
            None
        }
    }
}

/// A subject nobody else is using, so parallel tests do not see each other's messages.
fn unique(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}.{nanos}")
}

#[tokio::test]
async fn core_publish_subscribe_round_trip() {
    let Some(client) = connect().await else {
        return;
    };
    let subject = unique("showcase.test.pubsub");

    let mut sub = client.subscribe(subject.clone()).await.unwrap();
    client
        .publish(subject.clone(), Bytes::from_static(b"ping"))
        .await
        .unwrap();
    client.flush().await.unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("message within 2 s")
        .expect("subscription still open");
    assert_eq!(msg.subject.as_str(), subject);
    assert_eq!(msg.payload.as_ref(), b"ping");
}

#[tokio::test]
async fn request_reply_round_trip() {
    let Some(client) = connect().await else {
        return;
    };
    let subject = unique("showcase.test.echo");

    let mut sub = client.subscribe(subject.clone()).await.unwrap();
    let responder = {
        let client = client.clone();
        tokio::spawn(async move {
            let msg = sub.next().await.expect("a request");
            client
                .publish(
                    msg.reply.expect("requests carry a reply subject"),
                    msg.payload,
                )
                .await
                .unwrap();
        })
    };

    let reply = client
        .request(subject, Bytes::from_static(b"echo"))
        .await
        .unwrap();
    assert_eq!(reply.payload.as_ref(), b"echo");
    responder.await.unwrap();
}

#[tokio::test]
async fn jetstream_publish_then_fetch_in_order() {
    let Some(client) = connect().await else {
        return;
    };
    let js = jetstream::new(client);
    let name = "SHOWCASE_TEST";
    let _ = js.delete_stream(name).await;

    let stream = js
        .create_stream(stream::Config {
            name: name.to_owned(),
            subjects: vec!["showcase.test.js.>".to_owned()],
            ..Default::default()
        })
        .await
        .unwrap();
    for i in 0..3 {
        let ack = js
            .publish(
                format!("showcase.test.js.{i}"),
                Bytes::from(format!("m{i}")),
            )
            .await
            .unwrap()
            .await
            .unwrap();
        assert_eq!(ack.sequence, i + 1);
    }

    let consumer: PullConsumer = stream
        .create_consumer(pull::Config {
            durable_name: Some("test-consumer".to_owned()),
            ..Default::default()
        })
        .await
        .unwrap();
    let mut batch = consumer
        .fetch()
        .max_messages(3)
        .expires(Duration::from_secs(2))
        .messages()
        .await
        .unwrap();
    let mut received = Vec::new();
    while let Some(msg) = batch.next().await {
        let msg = msg.unwrap();
        received.push(String::from_utf8_lossy(&msg.payload).into_owned());
        msg.ack().await.unwrap();
    }
    assert_eq!(received, ["m0", "m1", "m2"]);

    js.delete_stream(name).await.unwrap();
}
