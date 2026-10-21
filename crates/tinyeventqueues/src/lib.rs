//! External event management and queuing: durably queue noisy inbound events
//! and filter them the way a person filters noise, before they reach an agent.
//!
//! Events arrive from chat channels (web chat, Telegram, Discord, Slack),
//! connector triggers (X replies through Composio), webhooks and
//! notifications. The [`Pipeline`] moves each one through
//!
//! ```text
//! ingress → normalize → gate → coalesce → salience → attention lanes
//!         → cadence / ingestion budget → dispatch (host callback)
//! ```
//!
//! and every ingested event ends in exactly one terminal [`Disposition`]:
//! delivered, context, digest, dead, or dropped with a [`Reason`]. The full
//! contract is `docs/specs/event-pipeline.md`.
//!
//! # Storage
//!
//! The pipeline runs over the storage ports of `tinystoragedrivers`: an
//! `Arc<dyn QueueStore>` for work and an `Arc<dyn DocumentStore>` for state,
//! so a host can provide memory, `SQLite`, file, `MongoDB` or broker engines.
//! Time comes from an injectable [`Clock`], so tests drive it with a
//! [`ManualClock`].
//!
//! # Configuration
//!
//! Two editable JSON files, both strict: `queues.json` declares every queue
//! (a `tinystoragedrivers` `QueueManifest`) and `pipeline.json` holds the
//! pipeline's tunables ([`PipelineConfig`]), referring to queues by name. See
//! [`config`].
//!
//! # Example
//!
//! ```
//! # tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
//! use std::sync::Arc;
//! use tinyeventqueues::{Disposition, EventKind, InboundEvent, Pipeline, Sender};
//! use tinystoragedrivers_testkit::memory_storage;
//! use tinystoragedrivers_types::Scope;
//!
//! let storage = memory_storage(&Scope::new("example")?);
//! let pipeline = Pipeline::builder(
//!     Arc::clone(storage.queues()),
//!     Arc::clone(storage.documents()),
//! ).build().await?;
//!
//! let event = InboundEvent::new(
//!     "channel:telegram", "1001", "chat-42", Sender::new("user-7"), EventKind::Message, 0,
//! );
//! let first = pipeline.ingest(vec![event.clone()]).await?;
//! assert_eq!(first.results[0].disposition, Disposition::Accepted);
//! let again = pipeline.ingest(vec![event]).await?;
//! assert_eq!(again.results[0].disposition, Disposition::Duplicate);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # }).unwrap();
//! ```

pub mod cadence;
pub mod clock;
mod coalesce;
pub mod config;
mod dead;
mod digest;
mod error;
mod gate;
pub mod host;
pub mod ingress;
pub mod observe;
mod pipeline;
mod ports;
pub mod salience;
mod store;
mod tinybus_module;
mod turn;

#[cfg(test)]
mod test_support;

/// Entry points for a host that links the module into its own executable
/// (a host's `linked` module source) instead of loading the `cdylib`.
#[cfg(feature = "static-link")]
pub mod linked {
    pub use crate::tinybus_module::exports::{
        TINYBUS_MODULE_ABI_V1, tinybus_module_init_v1, tinybus_module_manifest_v1,
    };
}

#[cfg(feature = "static-link")]
pub use tinybus_module::exports::linked_module;

pub use clock::{Clock, ManualClock, system_clock};
pub use error::{Error, Result};
pub use host::Host;
pub use observe::{CollectingSink, NoopSink, OutcomeSink};
pub use pipeline::{Pipeline, PipelineBuilder};

// The wire contract, re-exported by module and by item so every path through
// this crate resolves to the same definitions the contract crate publishes.
pub use tinyeventqueues_bus;
pub use tinyeventqueues_bus::{
    AckPolicy, AmbientConfig, Borderline, BotLoopConfig, CONTRACT_VERSION, ClassifySalienceRequest,
    ClassifySalienceResponse, CoalesceConfig, ConfigureRequest, ConfigureResponse, DeadItem,
    DeadLetterConfig, DeliverRequest, DeliverResponse, DeliverStatus, DigestConfig, Disposition,
    DrainReport, DrainRequest, DrainResponse, EventKind, EventRef, GateConfig, HabituationConfig,
    INTERFACE, InboundEvent, IngestRequest, IngestResponse, IngestResult, IngressConfig, Lane,
    LaneConfig, LanesConfig, ListDeadRequest, ListDeadResponse, METHODS, MediaRef, OBJECT_PATH,
    Outcome, OutcomeCounts, PayloadRef, PipelineConfig, QueueDepth, RateLimit, RateLimits, Reason,
    RedriveRequest, RedriveResponse, SalienceClass, SalienceConfig, SalienceThresholds,
    SalienceWeights, Sender, SourcePolicy, StatsRequest, StatsResponse, Turn, is_compatible, names,
    version,
};
