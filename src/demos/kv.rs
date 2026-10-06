//! JetStream key-value store: a bucket of keys with revisions, history, watches and
//! compare-and-swap semantics, built on a stream that keeps the last N messages per subject.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use async_nats::Client;
use async_nats::jetstream::{self, kv};
use futures::StreamExt;

use super::{BackgroundTask, detail, section, step};

const BUCKET: &str = "showcase_config";
const FLAG: &str = "feature.dark_mode";
const BANNER: &str = "feature.beta_banner";

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

pub async fn run(client: &Client) -> Result<()> {
    section("JetStream key-value store");
    let js = jetstream::new(client.clone());

    // Drop the bucket from a previous run so revisions start at 1 and the output is reproducible.
    let _ = js.delete_key_value(BUCKET).await;
    let store = js
        .create_key_value(kv::Config {
            bucket: BUCKET.to_owned(),
            description: "Feature flags for nats-showcase".to_owned(),
            history: 10, // keep the last 10 revisions of every key
            ..Default::default()
        })
        .await
        .context("creating a KV bucket needs JetStream; start the server with `nats-server -js`")?;
    step(format!("bucket '{BUCKET}' ready (history depth 10)"));

    // A watcher is notified of every change to matching keys; we collect what it sees.
    let observed = Arc::new(Mutex::new(Vec::<String>::new()));
    let watcher = {
        let observed = Arc::clone(&observed);
        BackgroundTask::spawn(store.watch("feature.>").await?, move |entry| {
            let observed = Arc::clone(&observed);
            async move {
                let line = match entry {
                    Ok(e) => format!(
                        "{:<6} {:<20} rev={} value={:?}",
                        format!("{:?}", e.operation),
                        e.key,
                        e.revision,
                        text(&e.value)
                    ),
                    Err(err) => format!("watch error: {err}"),
                };
                observed.lock().expect("watch log lock").push(line);
            }
        })
    };

    let rev1 = store.put(FLAG, "off".into()).await?;
    step(format!("put {FLAG}=off → revision {rev1}"));
    let value = store.get(FLAG).await?.context("key vanished")?;
    detail(format!("get {FLAG} → {:?}", text(&value)));

    // Compare-and-swap: update only succeeds if the key is still at the revision we last saw.
    let rev2 = store.update(FLAG, "on".into(), rev1).await?;
    step(format!(
        "update {FLAG}=on expecting revision {rev1} → ok, now revision {rev2}"
    ));
    match store.update(FLAG, "auto".into(), rev1).await {
        Err(err) => step(format!(
            "update {FLAG}=auto expecting stale revision {rev1} → rejected: {:?}",
            err.kind()
        )),
        Ok(rev) => bail!("stale update unexpectedly succeeded with revision {rev}"),
    }

    // create() only succeeds if the key does not exist yet.
    let rev = store.create(BANNER, "true".into()).await?;
    step(format!("create {BANNER}=true → revision {rev}"));
    match store.create(BANNER, "false".into()).await {
        Err(err) => step(format!(
            "create {BANNER} again → rejected: {:?}",
            err.kind()
        )),
        Ok(rev) => bail!("second create unexpectedly succeeded with revision {rev}"),
    }

    let entry = store.entry(FLAG).await?.context("key vanished")?;
    step(format!(
        "entry {FLAG}: value={:?} revision={} operation={:?} created={}",
        text(&entry.value),
        entry.revision,
        entry.operation,
        entry.created
    ));

    step(format!("history of {FLAG}, oldest first:"));
    let mut history = store.history(FLAG).await?;
    while let Some(entry) = history.next().await {
        let entry = entry?;
        detail(format!(
            "rev {} {:?} {:?}",
            entry.revision,
            entry.operation,
            text(&entry.value)
        ));
    }

    store.delete(BANNER).await?;
    let after = store.get(BANNER).await?;
    step(format!(
        "delete {BANNER} → get now returns {after:?}; a delete marker is kept in the history"
    ));

    step("keys in the bucket (deleted keys are hidden):");
    let mut keys = store.keys().await?;
    while let Some(key) = keys.next().await {
        detail(key?);
    }

    tokio::time::sleep(Duration::from_millis(200)).await; // let the watcher catch up
    watcher.stop().await;
    step("the watcher on `feature.>` observed, in order:");
    for line in observed.lock().expect("watch log lock").iter() {
        detail(line);
    }
    Ok(())
}
