# nats-showcase

A guided tour of the [NATS](https://nats.io) messaging system from Rust, built on the
official [`async-nats`](https://crates.io/crates/async-nats) client. Every subcommand is a
small, self-contained demo that prints what it does step by step, so the source and the
output can be read side by side.

| Command         | What it shows                                                                                   | `async-nats` API used                                                        |
|-----------------|-------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| `pubsub`        | Fire-and-forget publish/subscribe, subject hierarchies, `*` and `>` wildcards, message headers  | `Client::subscribe`, `publish_with_headers`, `flush`, `HeaderMap`            |
| `request-reply` | Request/reply, "no responders" detection, per-request timeouts, scatter-gather over an inbox     | `Client::request`, `send_request`, `new_inbox`, `publish_with_reply`          |
| `queue-group`   | Load-balancing one subject across several workers while a plain subscriber still sees everything | `Client::queue_subscribe`                                                    |
| `jetstream`     | Durable streams, acknowledged publishes, message-id dedup, direct get, durable pull consumers, NAK redelivery, `DeliverPolicy::New` | `jetstream::Context`, `Stream`, `PullConsumer::fetch`/`messages`, `Message::ack` |
| `kv`            | Key-value bucket: put/get, compare-and-swap with revisions, create-if-absent, history, delete, live watch | `jetstream::kv::Store`                                             |
| `object-store`  | Storing and retrieving a blob larger than the max message size, chunked transparently            | `jetstream::object_store::ObjectStore`, `tokio::io::AsyncRead`               |
| `service`       | NATS micro-services: grouped endpoints, structured error responses, `$SRV.*` discovery, stats     | `service::ServiceExt`, `Service::group`, `Request::respond`                  |
| `all`           | Runs every demo above in sequence (the default)                                                  |                                                                              |

## Prerequisites

- Rust 1.85 or newer (the crate uses edition 2024).
- A NATS server with JetStream enabled. Any of these works:
  - `docker compose up -d` (uses the bundled [`docker-compose.yml`](docker-compose.yml), monitoring UI on <http://localhost:8222>)
  - `brew install nats-server && nats-server -js`
  - a [release binary](https://github.com/nats-io/nats-server/releases) run as `nats-server --jetstream`

## Running

```sh
cargo run                      # the whole tour
cargo run -- jetstream         # a single demo
cargo run -- --help            # list of demos

NATS_URL=nats://other-host:4222 cargo run -- kv   # or --url
RUST_LOG=debug cargo run -- pubsub                 # client-level logging on stderr
```

The demos delete and recreate the JetStream resources they own (`SHOWCASE_ORDERS`,
`showcase_config`) at the start of each run so the output is reproducible. They only
touch subjects under `showcase.>` and the `$SRV.*` discovery subjects of the `calculator`
service.

## What a run looks like

```text
━━━ JetStream: streams, acknowledged publishes and pull consumers ━━━
▸ stream 'SHOWCASE_ORDERS' captures `showcase.js.orders.>` on disk
▸ publishing 5 orders and waiting for the storage acks:
    order #1 acme/eu 27.49 EUR       → stream=SHOWCASE_ORDERS seq=1 duplicate=false
    ...
▸ re-publishing order-1 with the same message id → duplicate=true (server points at seq 1 again)
▸ durable pull consumer 'order-processor' fetches a batch of up to 10:
    seq 1  order #1 acme/eu 27.49 EUR       ACK
    seq 2  order #2 globex/us 34.99 EUR     ACK
    seq 3  order #3 initech/apac 42.49 EUR  NAK  (simulated failure, redeliver in 100 ms)
    ...
▸ fetching again: only the NAK'd message comes back, now on its second delivery:
    seq 3  order #3 initech/apac 42.49 EUR  delivered 2 times → ACK
```

## Project layout

```text
src/main.rs                 CLI (clap), connection setup with an event callback, dispatch
src/domain.rs               the Order type used as JSON payload by several demos
src/demos/mod.rs            shared helpers: BackgroundTask, next_within, printing
src/demos/pubsub.rs         core pub/sub
src/demos/request_reply.rs  request/reply patterns
src/demos/queue_group.rs    queue groups
src/demos/jetstream.rs      streams and consumers
src/demos/kv.rs             key-value store
src/demos/object_store.rs   object store
src/demos/service.rs        micro-services
tests/roundtrip.rs          integration tests; they skip themselves when no server is reachable
docker-compose.yml          single-node NATS server with JetStream
```

## Testing

```sh
cargo test          # unit tests always run; integration tests need a server at NATS_URL
```

## Things worth noticing in the code

- **Connection handling is automatic.** `ConnectOptions::event_callback` in `main.rs` only
  observes reconnects; the client reconnects and resubscribes on its own.
- **Publishes are buffered.** `Client::flush` is what guarantees they have reached the server,
  which matters in the pub/sub demo where we publish and then immediately read.
- **JetStream acks are futures.** `Context::publish(...).await?` sends the message,
  `.await?` on the result waits for the server to confirm it was stored.
- **Message-id dedup lives on the server.** Re-publishing with the same `Nats-Msg-Id` inside
  the stream's duplicate window is acknowledged but not stored (`PublishAck::duplicate`).
- **Pull consumers are explicit about flow.** `fetch()` asks for a bounded batch and ends;
  `messages()` keeps pulling in the background and behaves like an endless stream.
- **KV is a stream in disguise.** Revisions are stream sequence numbers, history is
  `max_messages_per_subject`, and compare-and-swap is an expected-last-sequence publish.
- **Services are plain subjects with conventions.** Errors travel as headers
  (`Nats-Service-Error-Code`), and `$SRV.PING|INFO|STATS.<name>` answer discovery requests.
