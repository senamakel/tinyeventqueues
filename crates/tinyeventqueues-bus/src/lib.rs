//! Every type that crosses the tinyeventqueues module's `TinyBus` boundary,
//! and the names of the members that carry them.
//!
//! tinyeventqueues receives noisy inbound events (chat channels, connector
//! triggers, webhooks, notifications), queues them durably, and filters them
//! the way a person filters noise before anything reaches an agent. The module
//! ships as a loadable `TinyBus` module: `crates/tinyeventqueues` is built as a
//! `cdylib`. A host that loads it cannot `use` anything out of it, so the
//! vocabulary is published here as an ordinary library.
//!
//! # What is here
//!
//! - [`event`] — the [`InboundEvent`] envelope every source is normalized into.
//! - [`ops`] — the request and response payloads of the module's members
//!   (`Ingest`, `Configure`, `Stats`, `ListDead`, `Redrive`, `Drain`).
//! - [`host`] — the payloads of the host callbacks the module calls
//!   (`ClassifySalience`, `Deliver`), including the [`Turn`].
//! - [`outcome`] — [`Disposition`] and [`Reason`]: what happened to an event
//!   and why. Every ingested event ends in exactly one terminal disposition.
//! - [`config`] — [`PipelineConfig`], the wire form of `pipeline.json`.
//! - [`names`] — interface names, object paths, one constant per member.
//! - [`version`] — [`CONTRACT_VERSION`] and the [`is_compatible`] bind rule.
//!
//! # What is deliberately not here
//!
//! **No behavior** and **no transport**. The pipeline lives in
//! `crates/tinyeventqueues`, which depends on this crate and re-exports it, so
//! `tinyeventqueues::InboundEvent` and `tinyeventqueues_bus::InboundEvent` are
//! the same type. This crate depends on `serde` and `serde_json` only; CI
//! asserts it stays that way.
//!
//! # Example
//!
//! ```
//! use tinyeventqueues_bus::{names, EventKind, InboundEvent, IngestRequest, Sender};
//!
//! let event = InboundEvent::new(
//!     "channel:telegram", "1001", "chat-42", Sender::new("user-7"), EventKind::Message, 0,
//! )
//! .text("are you there?")
//! .direct(true);
//! assert_eq!(event.dedupe_key(), "channel:telegram:1001");
//!
//! let body = serde_json::to_value([IngestRequest::new(vec![event])])?;
//! assert_eq!(names::methods::INGEST, "Ingest");
//! assert!(body[0]["events"].is_array());
//! # Ok::<(), serde_json::Error>(())
//! ```

pub mod config;
pub mod event;
pub mod host;
pub mod names;
pub mod ops;
pub mod outcome;
#[cfg(feature = "storage")]
pub mod storage;
pub mod version;

pub use config::{
    AckPolicy, AmbientConfig, Borderline, BotLoopConfig, Budget, CoalesceConfig, DeadLetterConfig,
    DigestConfig, DispatchConfig, GateConfig, HabituationConfig, IngressConfig, Lane, LaneConfig,
    LanesConfig, PipelineConfig, RateLimit, RateLimits, SalienceConfig, SalienceThresholds,
    SalienceWeights, SourceCoalesce, SourcePolicy,
};
pub use event::{EventKind, EventRef, InboundEvent, MediaRef, PayloadRef, Sender};
pub use host::{
    ClassifySalienceRequest, ClassifySalienceResponse, DeliverRequest, DeliverResponse,
    DeliverStatus, SalienceClass, Turn,
};
pub use names::{INTERFACE, METHODS, OBJECT_PATH};
pub use ops::{
    ConfigureRequest, ConfigureResponse, DeadItem, DrainReport, DrainRequest, DrainResponse,
    IngestRequest, IngestResponse, IngestResult, ListDeadRequest, ListDeadResponse, QueueDepth,
    RedriveRequest, RedriveResponse, StatsRequest, StatsResponse,
};
pub use outcome::{Disposition, Outcome, OutcomeCounts, Reason};
pub use version::{CONTRACT_VERSION, is_compatible};
