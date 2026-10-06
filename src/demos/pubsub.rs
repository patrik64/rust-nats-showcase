//! Core NATS: fire-and-forget publish/subscribe with subject hierarchies, wildcards and headers.

use std::time::Duration;

use anyhow::Result;
use async_nats::{Client, HeaderMap};

use super::{detail, next_within, section, step};
use crate::domain::Order;

const PREFIX: &str = "showcase.orders";

pub async fn run(client: &Client) -> Result<()> {
    section("Core publish / subscribe");

    // Subjects are dot-separated tokens. `>` matches one or more trailing tokens,
    // `*` matches exactly one token. A subscription is just an interest in a pattern;
    // the server fans every matching message out to every matching subscriber.
    let mut everything = client.subscribe(format!("{PREFIX}.>")).await?;
    let mut eu_only = client.subscribe(format!("{PREFIX}.eu.*")).await?;
    step(format!(
        "subscribed to `{PREFIX}.>` (all regions) and `{PREFIX}.eu.*` (EU only)"
    ));

    let orders: Vec<Order> = (1..=4).map(Order::sample).collect();
    for order in &orders {
        // Headers are optional key/value metadata that travel with the message.
        let mut headers = HeaderMap::new();
        headers.insert("X-Trace-Id", format!("trace-{:04}", order.id));
        headers.insert("Content-Type", "application/json");
        client
            .publish_with_headers(order.subject(PREFIX), headers, order.to_bytes())
            .await?;
    }
    // Publishes are buffered on the client; flush pushes them to the server now.
    client.flush().await?;
    step(format!(
        "published {} orders on region-specific subjects",
        orders.len()
    ));

    step(format!("`{PREFIX}.>` receives every order:"));
    for _ in 0..orders.len() {
        let msg = next_within(&mut everything, Duration::from_secs(2)).await?;
        let order = Order::from_bytes(&msg.payload)?;
        let trace = msg
            .headers
            .as_ref()
            .and_then(|h| h.get("X-Trace-Id"))
            .map(|v| v.as_str())
            .unwrap_or("-");
        detail(format!(
            "{:<30} {}  [{trace}]",
            msg.subject,
            order.describe()
        ));
    }

    step(format!("`{PREFIX}.eu.*` receives only the EU ones:"));
    let mut eu_seen = 0;
    while let Ok(msg) = next_within(&mut eu_only, Duration::from_millis(250)).await {
        eu_seen += 1;
        detail(format!(
            "{:<30} {}",
            msg.subject,
            Order::from_bytes(&msg.payload)?.describe()
        ));
    }
    detail(format!(
        "{eu_seen} of {} messages matched the narrower subscription",
        orders.len()
    ));

    everything.unsubscribe().await?;
    eu_only.unsubscribe().await?;
    Ok(())
}
