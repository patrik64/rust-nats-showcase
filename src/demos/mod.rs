//! Each sub-module is one self-contained demo of a NATS feature.
//! This module holds the small helpers they share.

pub mod jetstream;
pub mod kv;
pub mod object_store;
pub mod pubsub;
pub mod queue_group;
pub mod request_reply;
pub mod service;

use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use futures::{Stream, StreamExt};
use tokio::sync::watch;
use tokio::task::JoinHandle;

/// `async-nats` reports some errors as a boxed `dyn Error`, which `?` cannot turn into an
/// `anyhow::Error` by itself; `.map_err(boxed)?` does the conversion.
pub fn boxed(err: async_nats::Error) -> anyhow::Error {
    anyhow::anyhow!(err)
}

/// Prints a section header for a demo.
pub fn section(title: &str) {
    println!("\n━━━ {title} ━━━");
}

/// Prints one step of a demo.
pub fn step(text: impl AsRef<str>) {
    println!("▸ {}", text.as_ref());
}

/// Prints a detail line under a step.
pub fn detail(text: impl AsRef<str>) {
    println!("    {}", text.as_ref());
}

/// Waits for the next item on a stream, failing if it takes longer than `timeout`.
pub async fn next_within<S: Stream + Unpin>(stream: &mut S, timeout: Duration) -> Result<S::Item> {
    tokio::time::timeout(timeout, stream.next())
        .await
        .with_context(|| format!("no message arrived within {timeout:?}"))?
        .context("stream ended")
}

/// Polls `condition` every few milliseconds until it holds or `timeout` passes.
pub async fn wait_until(timeout: Duration, mut condition: impl FnMut() -> bool) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    while !condition() {
        ensure!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

/// A background task that feeds every item of a stream (a subscription, a KV watch, a
/// service endpoint, ...) through `handler`, and that can be stopped gracefully.
///
/// Dropping the stream at the end is what unsubscribes: `async-nats` subscriptions send
/// an UNSUB to the server when they go out of scope.
pub struct BackgroundTask {
    stop: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

impl BackgroundTask {
    pub fn spawn<S, F, Fut>(mut stream: S, mut handler: F) -> Self
    where
        S: Stream + Unpin + Send + 'static,
        S::Item: Send,
        F: FnMut(S::Item) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (stop, mut stop_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop_rx.changed() => break,
                    item = stream.next() => match item {
                        Some(item) => handler(item).await,
                        None => break,
                    },
                }
            }
        });
        Self { stop, handle }
    }

    /// Signals the task to stop and waits for it to finish.
    pub async fn stop(self) {
        let _ = self.stop.send(true);
        let _ = self.handle.await;
    }
}
