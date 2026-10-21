# tinyeventqueues

External event management and queuing. A TinyBus module that receives noisy
inbound events — web chat, Telegram, Discord and Slack messages, X replies via
Composio triggers, webhooks, notifications — queues them durably, and filters
them the way a person filters noise before anything reaches an agent.

```text
ingress → normalize → gate → coalesce → salience → attention lanes
        → cadence / ingestion budget → dispatch (host Deliver callback)
```

Every ingested event ends in exactly one terminal disposition — delivered,
context, digest, dead, or dropped with a reason — so nothing is lost silently.
The pipeline runs over the storage ports of
[tinystoragedrivers](https://github.com/tinyhumansai/tinystoragedrivers)
(`QueueStore` and `DocumentStore`), so it works on memory, SQLite, file,
MongoDB or a broker, and survives restarts: all state lives in storage.

The full contract is [`docs/specs/event-pipeline.md`](docs/specs/event-pipeline.md);
delivery order is in [`docs/plans/event-pipeline.md`](docs/plans/event-pipeline.md).
All seven stages are implemented: ingress with completion tombstones, the
gate, coalescing, salience with habituation and borderline host
classification, attention lanes with digest spill, the cadence scheduler with
ingestion budgets, and dispatch to the host with retries, backoff and dead
letters (`ListDead` / `Redrive`). A property test checks that every ingested
event ends exactly once, and a 10,000-event flood test checks order, thread
separation and interactive latency.

## Configuration

Two editable JSON files at the repository root. Both are strict: an unknown
or misspelt field is an error naming its JSON path.

| File | What it holds | Schema |
| --- | --- | --- |
| [`queues.json`](queues.json) | Every queue the pipeline uses (ingress, the four lanes, dead letters) with `max_attempts`, `dedupe_window_ms` and `visibility_ms` — a tinystoragedrivers `QueueManifest`, applied at construction | `schemas/queues.schema.json` in tinystoragedrivers |
| [`pipeline.json`](pipeline.json) | The pipeline's tunables: which queue plays which role, lane concurrency, per-source ack policy, tombstone TTL, batch size, gate (sender lists, DM/group policy, bot-loop guard, rate limits, ambient buffer) coalescing (debounce, max wait, control prefixes), salience (weights, keywords, known contacts, lane thresholds, borderline band, habituation), lane backlog limits, cadences and ingestion budgets per lane or source, and the digest | [`schemas/pipeline.schema.json`](schemas/pipeline.schema.json) |

No queue name or queue spec is written in Rust. To add or tune a queue, edit
`queues.json`; to route a role to it, edit `pipeline.json`.

## Usage

```rust,ignore
use std::sync::Arc;
use tinyeventqueues::{EventKind, InboundEvent, Pipeline, Sender};

// `storage` is the host's scope-bound v2 Storage bundle.
let pipeline = Pipeline::builder(Arc::clone(storage.queues()), Arc::clone(storage.documents()))
    .manifest_path("queues.json")
    .config_path("pipeline.json")
    .build()
    .await?;

let event = InboundEvent::new("channel:telegram", "1001", "chat-42",
    Sender::new("user-7"), EventKind::Message, received_at_ms);
let response = pipeline.ingest(vec![event]).await?;
// Acknowledge the platform only now, for results with `ack_platform`.
```

Over the bus, hosts call `ai.tinyhumans.tinyeventqueues.EventQueues`
(`Ingest`, `Configure`, `Stats`, `ListDead`, `Redrive`, `Drain`) and serve
`ai.tinyhumans.tinyeventqueues.Host` (`ClassifySalience`, `Deliver`). The
payload types are in the dependency-light
[`tinyeventqueues-bus`](crates/tinyeventqueues-bus/README.md) crate.

## Layout

```text
queues.json             # the queue manifest (editable)
pipeline.json           # the pipeline configuration (editable)
schemas/                # JSON Schema for pipeline.json
crates/
├── tinyeventqueues-bus/   # the wire contract: envelope, payloads, outcomes, config
└── tinyeventqueues/       # the pipeline, the TinyBus adapter, the cdylib
    └── src/
        ├── config/        # loading and validating the two files
        ├── ingress/       # validate, persist, recognize redeliveries
        ├── gate/          # deterministic checks, token buckets, ambient context
        ├── coalesce/      # the durable debounce buffer
        ├── salience/      # rule scores, SimHash habituation, lane thresholds
        ├── digest/        # the digest accumulator
        ├── cadence/       # cadence specs, the persisted tick schedule, staging
        ├── turn/          # pending turns and the lane writer
        ├── host/          # the `Host` callback trait
        ├── store/         # collections, tombstones, the outcome ledger
        ├── dead/          # parking, listing and redriving dead work
        ├── pipeline/      # `Pipeline`, its builder, the pass and dispatch
        ├── observe/       # outcome sinks
        ├── clock/         # system and manual clocks
        └── tinybus_module/
vendor/tinybus/         # pinned TinyBus SDK submodule
docs/specs, docs/plans  # the contract and the delivery plan
```

Unit tests sit beside their module as `mod_tests.rs` (or `<module>_tests.rs`),
wired with `#[cfg(test)] #[path = "mod_tests.rs"] mod tests;`, because
OpenHuman vendors this repository and its layout check rejects `test.rs` files
and inline test modules.

## Development

```sh
git submodule update --init --recursive
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo build --all-targets --all-features
cargo test --all-features
cargo run -p tinyeventqueues --example basic
.github/scripts/check-file-coverage.sh 90 coverage.json   # needs cargo-llvm-cov
```

`tinystoragedrivers-core` is a git dependency pinned to a tinyhumansai
revision (the one that merged the queue port, tinyhumansai/tinystoragedrivers#71).

## Releasing

Run the **Release** workflow with a `patch`, `minor` or `major` bump. It
revalidates, versions and tags the workspace, builds `crates/tinyeventqueues`
as a TinyBus `cdylib` for every supported platform, publishes archives named
`tinyeventqueues-<version>-<platform>.<tar.gz|zip>` with a `checksum.toml`, and
loads the published Ubuntu archive through TinyBus before declaring success.
Do not hand-edit the version in the root `Cargo.toml`.

## Documentation

- [`AGENTS.md`](AGENTS.md) — repository guidelines for humans and agents
- [`MODULE.md`](MODULE.md) — what the installable module archive contains
- [`docs/specs/`](docs/specs/README.md) and [`docs/plans/`](docs/plans/README.md)
- [`SECURITY.md`](SECURITY.md) — how to report a vulnerability

## License

GPL-3.0-only. See [LICENSE](LICENSE).
