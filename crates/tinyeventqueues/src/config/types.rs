//! Resolved queue references.

use crate::ports::{QueueSpec, QueueStore};
use serde::Deserialize;
use tinyeventqueues_bus::Lane;

/// A queue declaration consumed by this module's pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueDeclaration {
    /// Queue name.
    pub name: String,
    /// Human-readable purpose retained from `queues.json`.
    pub description: String,
    /// Declared owner retained from `queues.json`.
    pub owner: String,
    /// Maximum delivery attempts before dead-lettering.
    pub max_attempts: u32,
    /// Queue deduplication window in milliseconds.
    pub dedupe_window_ms: u64,
    /// Receive lease duration in milliseconds.
    pub visibility_ms: u64,
}

/// The module's queue declarations. This is an owner-local config value, not
/// a global host queue manifest or storage grant.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueManifest {
    /// Editor schema URI, retained for compatibility with the shipped file.
    #[serde(rename = "$schema", default)]
    pub schema: Option<String>,
    /// Queue declaration format version.
    pub version: u32,
    /// Queues consumed by the pipeline.
    pub queues: Vec<QueueDeclaration>,
}

impl QueueManifest {
    pub(crate) fn validate(&self) -> crate::Result<()> {
        if self.version != 1 {
            return Err(crate::Error::manifest("version must be 1"));
        }
        if self.queues.is_empty() {
            return Err(crate::Error::manifest(
                "queues must contain at least one queue",
            ));
        }
        let mut names = std::collections::HashSet::new();
        for (index, queue) in self.queues.iter().enumerate() {
            if queue.owner != "tinyeventqueues" {
                return Err(crate::Error::manifest(format!(
                    "queues[{index}].owner must be tinyeventqueues"
                )));
            }
            if queue.description.trim().is_empty() {
                return Err(crate::Error::manifest(format!(
                    "queues[{index}].description must not be empty"
                )));
            }
            tinystoragedrivers_types::validate_queue_name(&queue.name).map_err(|error| {
                crate::Error::manifest(format!("queues[{index}].name: {error}"))
            })?;
            if !names.insert(&queue.name) {
                return Err(crate::Error::manifest(format!(
                    "queues[{index}].name duplicates {}",
                    queue.name
                )));
            }
            if queue.max_attempts == 0 {
                return Err(crate::Error::manifest(format!(
                    "queues[{index}].max_attempts must be positive"
                )));
            }
            if queue.visibility_ms == 0 {
                return Err(crate::Error::manifest(format!(
                    "queues[{index}].visibility_ms must be positive"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn get(&self, name: &str) -> Option<&QueueDeclaration> {
        self.queues.iter().find(|queue| queue.name == name)
    }

    pub(crate) async fn apply(&self, queues: &dyn QueueStore) -> crate::Result<()> {
        self.validate()?;
        for declaration in &self.queues {
            queues
                .ensure_queue(
                    &declaration.name,
                    &QueueSpec {
                        max_attempts: declaration.max_attempts,
                        dedupe_window_ms: declaration.dedupe_window_ms,
                    },
                )
                .await?;
        }
        Ok(())
    }
}

/// One manifest queue the pipeline uses, resolved from its declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueRef {
    /// The queue name.
    pub name: String,
    /// The lease the pipeline asks for when it receives.
    pub visibility_ms: u64,
    /// Deliveries a message gets before the queue dead-letters it.
    pub max_attempts: u32,
}

/// Every queue the pipeline uses, by role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedQueues {
    /// Raw inbound events.
    pub ingress: QueueRef,
    /// Parked dead work.
    pub dead: QueueRef,
    /// One queue per lane, in [`Lane::ALL`] order.
    pub lanes: [QueueRef; 4],
}

impl ResolvedQueues {
    /// The queue of `lane`.
    #[must_use]
    pub fn lane(&self, lane: Lane) -> &QueueRef {
        let [interactive, actionable, background, digest] = &self.lanes;
        match lane {
            Lane::Interactive => interactive,
            Lane::Actionable => actionable,
            Lane::Background => background,
            Lane::Digest => digest,
        }
    }

    /// Every queue: ingress, the lanes, then the dead queue.
    #[must_use]
    pub fn all(&self) -> Vec<&QueueRef> {
        let mut all = vec![&self.ingress];
        all.extend(self.lanes.iter());
        all.push(&self.dead);
        all
    }

    /// The queue named `name`, if the pipeline uses one.
    #[must_use]
    pub fn by_name(&self, name: &str) -> Option<&QueueRef> {
        self.all().into_iter().find(|queue| queue.name == name)
    }
}
