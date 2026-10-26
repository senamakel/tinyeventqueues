//! What the module stores, as a schema a host composes.
//!
//! The pipeline keeps its state in `teq.*` document collections. Hosts compose
//! [`collection_specs`] with their own storage declarations.
//!
//! The queues themselves are not collections of this owner. They are declared
//! in `queues.json` (a `tinystoragedrivers` `QueueManifest`, shipped as
//! [`SHIPPED_QUEUES_JSON`]) and served by the host's queue port, whose own
//! bookkeeping lives under the reserved `tsd.` owner.
//!
//! [`collection_specs`] is the complete, pure collection declaration used by
//! both the host and this module.

use tinystoragedrivers_types::{CollectionSpec, IndexSpec};

/// Terminal outcomes, one per event.
pub const OUTCOMES: &str = "teq.outcomes";
/// Ingress tombstones, for queue backends without dedupe.
pub const TOMBSTONES: &str = "teq.tombstones";
/// Dead letters already copied back to their origin by a redrive.
pub const REDRIVEN: &str = "teq.redriven";
/// Token buckets and bot-loop windows.
pub const BUCKETS: &str = "teq.buckets";
/// Ambient context per conversation.
pub const AMBIENT: &str = "teq.ambient";
/// The debounce buffer.
pub const COALESCE: &str = "teq.coalesce";
/// Habituation fingerprints per source.
pub const HABITUATION: &str = "teq.habituation";
/// The digest accumulator.
pub const DIGEST: &str = "teq.digest";
/// Turns staged for a cadence tick.
pub const STAGED: &str = "teq.staged";
/// Each cadence's schedule.
pub const CADENCE: &str = "teq.cadence";
/// Per-conversation turn order, on queue backends without groups.
pub const CONVERSATIONS: &str = "teq.conversations";
/// Queued, unsettled turns per thread.
pub const THREADS: &str = "teq.threads";

/// The shipped queue manifest (`queues.json`). A host that serves the module's
/// queues declares exactly these unless its operator overrides the file.
pub const SHIPPED_QUEUES_JSON: &str = include_str!("../../../../queues.json");

/// The shipped pipeline configuration (`pipeline.json`).
pub const SHIPPED_PIPELINE_JSON: &str = include_str!("../../../../pipeline.json");

/// The collections the pipeline uses, with their indexes.
#[must_use]
pub fn collection_specs() -> Vec<CollectionSpec> {
    vec![
        CollectionSpec::new(OUTCOMES)
            .index(IndexSpec::new("by_disposition", ["disposition"]))
            .index(IndexSpec::new("by_expiry", ["expires_ms"])),
        CollectionSpec::new(TOMBSTONES).index(IndexSpec::new("by_expiry", ["expires_ms"])),
        CollectionSpec::new(REDRIVEN),
        CollectionSpec::new(BUCKETS),
        CollectionSpec::new(AMBIENT),
        CollectionSpec::new(HABITUATION),
        CollectionSpec::new(DIGEST),
        CollectionSpec::new(STAGED)
            .index(IndexSpec::new("by_due", ["due_ms"]))
            .index(IndexSpec::new("by_cadence", ["cadence_key"])),
        CollectionSpec::new(CADENCE),
        CollectionSpec::new(CONVERSATIONS),
        CollectionSpec::new(THREADS),
        CollectionSpec::new(COALESCE)
            .index(IndexSpec::new("by_due", ["due_ms"]))
            .index(IndexSpec::new("by_thread", ["thread_group"])),
    ]
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
