//! NATS micro-services: request/reply endpoints with built-in discovery, statistics and
//! structured error responses, all using nothing but subjects.

use anyhow::Result;
use async_nats::Client;
use async_nats::service::error::Error as ServiceError;
use async_nats::service::{self, NATS_SERVICE_ERROR, NATS_SERVICE_ERROR_CODE, ServiceExt};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{BackgroundTask, boxed, detail, section, step};
use crate::domain::to_json_bytes;

const NAME: &str = "calculator";
const ADD: &str = "showcase.calc.add";
const DIV: &str = "showcase.calc.div";

#[derive(Deserialize)]
struct Operands {
    a: f64,
    b: f64,
}

#[derive(Serialize)]
struct Answer {
    result: f64,
}

fn parse(req: &service::Request) -> Result<Operands, ServiceError> {
    serde_json::from_slice(&req.message.payload).map_err(|err| ServiceError {
        code: 400,
        status: format!("invalid request: {err}"),
    })
}

fn add(req: &service::Request) -> Result<Bytes, ServiceError> {
    let o = parse(req)?;
    Ok(to_json_bytes(&Answer { result: o.a + o.b }))
}

fn divide(req: &service::Request) -> Result<Bytes, ServiceError> {
    let o = parse(req)?;
    if o.b == 0.0 {
        return Err(ServiceError {
            code: 422,
            status: "division by zero".to_owned(),
        });
    }
    Ok(to_json_bytes(&Answer { result: o.a / o.b }))
}

/// Returns the payload, or the service error headers if the service answered with an error.
fn describe_reply(reply: &async_nats::Message) -> String {
    let headers = reply.headers.as_ref();
    match headers.and_then(|h| h.get(NATS_SERVICE_ERROR_CODE)) {
        Some(code) => {
            let status = headers
                .and_then(|h| h.get(NATS_SERVICE_ERROR))
                .map(|v| v.as_str())
                .unwrap_or("-");
            format!(
                "error {NATS_SERVICE_ERROR_CODE}={} {NATS_SERVICE_ERROR}={status:?}",
                code.as_str()
            )
        }
        None => String::from_utf8_lossy(&reply.payload).into_owned(),
    }
}

async fn call(client: &Client, subject: &str, payload: Bytes) -> Result<()> {
    let shown = String::from_utf8_lossy(&payload).into_owned();
    let reply = client.request(subject.to_owned(), payload).await?;
    detail(format!(
        "{subject} {shown:<16} → {}",
        describe_reply(&reply)
    ));
    Ok(())
}

async fn discover(client: &Client, verb: &str) -> Result<Value> {
    let reply = client
        .request(format!("$SRV.{verb}.{NAME}"), Bytes::new())
        .await?;
    Ok(serde_json::from_slice(&reply.payload)?)
}

pub async fn run(client: &Client) -> Result<()> {
    section("NATS micro-services");

    let svc = client
        .service_builder()
        .description("Arithmetic as a service")
        .start(NAME, "1.0.0")
        .await
        .map_err(boxed)?;
    // Endpoints live under a group prefix: showcase.calc.add and showcase.calc.div.
    let group = svc.group("showcase.calc");
    let add_task = BackgroundTask::spawn(
        group.endpoint("add").await.map_err(boxed)?,
        |req| async move {
            let _ = req.respond(add(&req)).await;
        },
    );
    let div_task = BackgroundTask::spawn(
        group.endpoint("div").await.map_err(boxed)?,
        |req| async move {
            let _ = req.respond(divide(&req)).await;
        },
    );
    step(format!(
        "service '{NAME}' v1.0.0 is up with endpoints `{ADD}` and `{DIV}`"
    ));

    step("calling the endpoints like any request/reply subject:");
    call(client, ADD, to_json_bytes(&json!({"a": 2, "b": 3.5}))).await?;
    call(client, DIV, to_json_bytes(&json!({"a": 9, "b": 4}))).await?;
    call(client, DIV, to_json_bytes(&json!({"a": 1, "b": 0}))).await?;
    call(client, ADD, Bytes::from_static(b"not json")).await?;

    // Every service answers on well-known $SRV subjects, so tooling can find it without config.
    step("discovery on the `$SRV.*` subjects:");
    let ping = discover(client, "PING").await?;
    detail(format!(
        "$SRV.PING.{NAME}  → name={} version={} id={}",
        ping["name"], ping["version"], ping["id"]
    ));
    let info = discover(client, "INFO").await?;
    let endpoints: Vec<&str> = info["endpoints"]
        .as_array()
        .map(|eps| eps.iter().filter_map(|e| e["subject"].as_str()).collect())
        .unwrap_or_default();
    detail(format!(
        "$SRV.INFO.{NAME}  → description={} endpoints={}",
        info["description"],
        endpoints.join(", ")
    ));
    let stats = discover(client, "STATS").await?;
    if let Some(eps) = stats["endpoints"].as_array() {
        for ep in eps {
            detail(format!(
                "$SRV.STATS.{NAME} → {:<4} requests={} errors={} avg={}ns",
                ep["name"], ep["num_requests"], ep["num_errors"], ep["average_processing_time"]
            ));
        }
    }

    step("the same statistics, straight from the Service handle:");
    let mut stats: Vec<_> = svc.stats().await.into_iter().collect();
    stats.sort_by(|a, b| a.0.cmp(&b.0));
    for (name, s) in stats {
        detail(format!(
            "{name:<4} requests={} errors={} last_error={:?}",
            s.requests, s.errors, s.last_error
        ));
    }

    svc.stop().await.map_err(boxed)?;
    add_task.stop().await;
    div_task.stop().await;
    step(format!("service '{NAME}' stopped"));
    Ok(())
}
