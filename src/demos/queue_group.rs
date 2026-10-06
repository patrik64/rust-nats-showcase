//! Queue groups: subscribers that share a group name split the messages between them
//! instead of each getting a copy. That is NATS' built-in work distribution.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Result;
use async_nats::Client;
use bytes::Bytes;

use super::{BackgroundTask, detail, section, step, wait_until};

const SUBJECT: &str = "showcase.jobs";
const QUEUE: &str = "job-workers";
const WORKERS: usize = 3;
const JOBS: usize = 12;

pub async fn run(client: &Client) -> Result<()> {
    section("Queue groups (load-balanced workers)");

    let counters: Vec<Arc<AtomicUsize>> = (0..WORKERS)
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();
    let mut workers = Vec::with_capacity(WORKERS);
    for (id, counter) in counters.iter().enumerate() {
        // Every member of the queue group gets a share of the messages, not a copy.
        let sub = client.queue_subscribe(SUBJECT, QUEUE.to_owned()).await?;
        let counter = Arc::clone(counter);
        workers.push(BackgroundTask::spawn(sub, move |msg| {
            let counter = Arc::clone(&counter);
            async move {
                tokio::time::sleep(Duration::from_millis(15)).await; // pretend to work
                counter.fetch_add(1, Ordering::Relaxed);
                detail(format!(
                    "worker-{id} finished {}",
                    String::from_utf8_lossy(&msg.payload)
                ));
            }
        }));
    }

    // A plain subscriber outside the group still receives every message, e.g. an audit log.
    let audited = Arc::new(AtomicUsize::new(0));
    let audit = {
        let audited = Arc::clone(&audited);
        BackgroundTask::spawn(client.subscribe(SUBJECT).await?, move |_msg| {
            let audited = Arc::clone(&audited);
            async move {
                audited.fetch_add(1, Ordering::Relaxed);
            }
        })
    };
    step(format!(
        "{WORKERS} workers joined queue group '{QUEUE}' on `{SUBJECT}`, plus one audit subscriber outside the group"
    ));

    for n in 1..=JOBS {
        client
            .publish(SUBJECT, Bytes::from(format!("job-{n:02}")))
            .await?;
    }
    client.flush().await?;
    step(format!("published {JOBS} jobs:"));

    let processed = || {
        counters
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .sum::<usize>()
    };
    wait_until(Duration::from_secs(5), || {
        processed() == JOBS && audited.load(Ordering::Relaxed) == JOBS
    })
    .await?;

    step("distribution:");
    for (id, counter) in counters.iter().enumerate() {
        detail(format!(
            "worker-{id}: {:>2} jobs",
            counter.load(Ordering::Relaxed)
        ));
    }
    detail(format!(
        "audit   : {:>2} jobs (all of them, because it is not in the group)",
        audited.load(Ordering::Relaxed)
    ));

    for worker in workers {
        worker.stop().await;
    }
    audit.stop().await;
    Ok(())
}
