//! Request/reply on top of plain pub/sub: inboxes, timeouts, "no responders" and scatter-gather.

use std::time::Duration;

use anyhow::{Result, bail};
use async_nats::{Client, Request, RequestErrorKind};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{BackgroundTask, detail, next_within, section, step};
use crate::domain::to_json_bytes;

const QUOTE_SUBJECT: &str = "showcase.pricing.quote";
const BID_SUBJECT: &str = "showcase.pricing.bid";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuoteRequest {
    sku: String,
    quantity: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct Quote {
    sku: String,
    quantity: u32,
    total_cents: u64,
    quoted_by: String,
}

/// Deterministic pseudo price derived from the SKU, so the demo output is stable.
fn unit_price_cents(sku: &str) -> u64 {
    500 + sku.bytes().map(u64::from).sum::<u64>() % 1_000
}

fn quote(req: QuoteRequest, quoted_by: &str, markup_cents: u64) -> Quote {
    Quote {
        total_cents: (unit_price_cents(&req.sku) + markup_cents) * u64::from(req.quantity),
        sku: req.sku,
        quantity: req.quantity,
        quoted_by: quoted_by.to_owned(),
    }
}

/// Starts a responder on `subject` that answers every request with a quote.
async fn spawn_responder(
    client: &Client,
    subject: &str,
    name: &'static str,
    markup_cents: u64,
) -> Result<BackgroundTask> {
    let sub = client.subscribe(subject.to_owned()).await?;
    let client = client.clone();
    Ok(BackgroundTask::spawn(sub, move |msg| {
        let client = client.clone();
        async move {
            // A plain publish carries no reply subject; only requests do.
            let Some(reply) = msg.reply else { return };
            let Ok(req) = serde_json::from_slice::<QuoteRequest>(&msg.payload) else {
                return;
            };
            if req.sku == "SLOW-1" {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let _ = client
                .publish(reply, to_json_bytes(&quote(req, name, markup_cents)))
                .await;
        }
    }))
}

pub async fn run(client: &Client) -> Result<()> {
    section("Request / reply");

    // 1. Nobody is listening yet. The server tells us so immediately instead of
    //    letting the request sit until it times out.
    match client.request(QUOTE_SUBJECT, Bytes::new()).await {
        Err(err) if err.kind() == RequestErrorKind::NoResponders => {
            step("request with no responder → server answered 'no responders' right away");
        }
        Ok(_) => bail!("expected a no-responders error"),
        Err(err) => return Err(err.into()),
    }

    // 2. Start a responder. It is an ordinary subscriber that publishes to `msg.reply`.
    let responder = spawn_responder(client, QUOTE_SUBJECT, "pricing-1", 0).await?;

    // 3. A plain request: the client creates a unique inbox, subscribes to it,
    //    publishes with that inbox as reply-to and waits for exactly one answer.
    let req = QuoteRequest {
        sku: "WIDGET-1".into(),
        quantity: 3,
    };
    let reply = client.request(QUOTE_SUBJECT, to_json_bytes(&req)).await?;
    let quote: Quote = serde_json::from_slice(&reply.payload)?;
    step(format!(
        "request {} x{} → {} answered: total {}.{:02} EUR",
        req.sku,
        req.quantity,
        quote.quoted_by,
        quote.total_cents / 100,
        quote.total_cents % 100
    ));

    // 4. Per-request timeout. The responder exists but is too slow for our budget.
    let slow = Request::new()
        .payload(to_json_bytes(&QuoteRequest {
            sku: "SLOW-1".into(),
            quantity: 1,
        }))
        .timeout(Some(Duration::from_millis(300)));
    match client.send_request(QUOTE_SUBJECT, slow).await {
        Err(err) if err.kind() == RequestErrorKind::TimedOut => {
            step(
                "slow responder → request timed out after 300 ms (the responder is alive, just slow)",
            );
        }
        Ok(_) => bail!("expected a timeout"),
        Err(err) => return Err(err.into()),
    }

    // 5. Scatter-gather: one request, many replies. We manage the inbox ourselves.
    let bidders = vec![
        spawn_responder(client, BID_SUBJECT, "bidder-alpha", 120).await?,
        spawn_responder(client, BID_SUBJECT, "bidder-beta", 40).await?,
        spawn_responder(client, BID_SUBJECT, "bidder-gamma", 85).await?,
    ];
    let inbox = client.new_inbox();
    let mut replies = client.subscribe(inbox.clone()).await?;
    client
        .publish_with_reply(BID_SUBJECT, inbox, to_json_bytes(&req))
        .await?;

    let mut bids: Vec<Quote> = Vec::new();
    while let Ok(msg) = next_within(&mut replies, Duration::from_millis(300)).await {
        bids.push(serde_json::from_slice(&msg.payload)?);
    }
    bids.sort_by_key(|b| b.total_cents);
    step(format!(
        "scatter-gather: one request on `{BID_SUBJECT}` collected {} bids",
        bids.len()
    ));
    for bid in &bids {
        detail(format!(
            "{:<13} {}.{:02} EUR",
            bid.quoted_by,
            bid.total_cents / 100,
            bid.total_cents % 100
        ));
    }
    if let Some(best) = bids.first() {
        detail(format!("cheapest: {}", best.quoted_by));
    }

    responder.stop().await;
    for bidder in bidders {
        bidder.stop().await;
    }
    Ok(())
}
