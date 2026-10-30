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

use serde::{Deserialize, Serialize};

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

/// A collection declaration that the host can translate to its storage API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionDeclaration {
    /// Stable collection name.
    pub name: String,
    /// Secondary indexes used by the pipeline.
    #[serde(default)]
    pub indexes: Vec<IndexDeclaration>,
}

impl CollectionDeclaration {
    /// Declare a collection by name.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            indexes: Vec::new(),
        }
    }

    /// Add an index declaration.
    #[must_use]
    pub fn index(mut self, index: IndexDeclaration) -> Self {
        self.indexes.push(index);
        self
    }
}

/// A secondary index declaration expressed as bus-owned data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDeclaration {
    /// Index name, unique within the collection.
    pub name: String,
    /// Dotted field paths, in key order.
    pub fields: Vec<String>,
    /// Whether equal field values are rejected.
    #[serde(default)]
    pub unique: bool,
}

impl IndexDeclaration {
    /// Declare a non-unique index over `fields`.
    #[must_use]
    pub fn new<S: Into<String>>(
        name: impl Into<String>,
        fields: impl IntoIterator<Item = S>,
    ) -> Self {
        Self {
            name: name.into(),
            fields: fields.into_iter().map(Into::into).collect(),
            unique: false,
        }
    }

    /// Make the index unique.
    #[must_use]
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

/// The collections the pipeline uses, with their indexes.
#[must_use]
pub fn collection_specs() -> Vec<CollectionDeclaration> {
    vec![
        CollectionDeclaration::new(OUTCOMES)
            .index(IndexDeclaration::new("by_disposition", ["disposition"]))
            .index(IndexDeclaration::new("by_expiry", ["expires_ms"])),
        CollectionDeclaration::new(TOMBSTONES)
            .index(IndexDeclaration::new("by_expiry", ["expires_ms"])),
        CollectionDeclaration::new(REDRIVEN),
        CollectionDeclaration::new(BUCKETS),
        CollectionDeclaration::new(AMBIENT),
        CollectionDeclaration::new(HABITUATION),
        CollectionDeclaration::new(DIGEST),
        CollectionDeclaration::new(STAGED)
            .index(IndexDeclaration::new("by_due", ["due_ms"]))
            .index(IndexDeclaration::new("by_cadence", ["cadence_key"])),
        CollectionDeclaration::new(CADENCE),
        CollectionDeclaration::new(CONVERSATIONS),
        CollectionDeclaration::new(THREADS),
        CollectionDeclaration::new(COALESCE)
            .index(IndexDeclaration::new("by_due", ["due_ms"]))
            .index(IndexDeclaration::new("by_thread", ["thread_group"])),
    ]
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
