//! The [`Pipeline`]: the stages wired over one queue store and one document
//! store.
//!
//! A pipeline is built once per process with [`PipelineBuilder`], which loads
//! the queue manifest and the pipeline configuration (the shipped
//! `queues.json` and `pipeline.json` unless told otherwise), checks every
//! queue the configuration names against the manifest, declares the queues
//! with [`QueueManifest::apply`], and declares its document collections.
//!
//! Everything durable lives in the two stores, so a new pipeline over the same
//! storage — after a restart — picks up exactly where the old one stopped.
//! Work advances in passes: [`Pipeline::drain`] runs one.

mod builder;
mod dispatch;
mod pass;
mod wake;

use std::sync::{Arc, RwLock};

use crate::config::QueueManifest;
use crate::ports::{Clock, DocumentStore, QueueStore};
use tinyeventqueues_bus::{
    DeadItem, DrainReport, InboundEvent, IngestResponse, PipelineConfig, QueueDepth, StatsResponse,
};

use crate::Result;
use crate::cadence::Scheduler;
use crate::coalesce::Coalescer;
use crate::config::{ResolvedQueues, check_values, resolve_queues};
use crate::dead::DeadLetters;
use crate::digest::Digest;
use crate::gate::{Ambient, Gate};
use crate::host::Host;
use crate::ingress::Ingress;
use crate::salience::Habituation;
use crate::store::{Ledger, Tombstones};
use crate::turn::{Conversations, Lanes, Threads};

pub use builder::PipelineBuilder;

/// The configuration in effect and the queues it resolved to.
#[derive(Debug)]
struct Running {
    config: PipelineConfig,
    queues: ResolvedQueues,
}

/// The inbound event pipeline. Cheap to clone; clones share everything.
#[derive(Debug, Clone)]
pub struct Pipeline {
    inner: Arc<Inner>,
}

struct Inner {
    queues: Arc<dyn QueueStore>,
    clock: Clock,
    manifest: QueueManifest,
    running: RwLock<Arc<Running>>,
    ledger: Ledger,
    tombstones: Tombstones,
    ingress: Ingress,
    dead: DeadLetters,
    gate: Gate,
    ambient: Ambient,
    coalescer: Coalescer,
    lanes: Lanes,
    habituation: Habituation,
    digest: Digest,
    scheduler: Scheduler,
    conversations: Conversations,
    threads: Threads,
    host: Option<Arc<dyn Host>>,
    consumer: String,
    /// Held for the whole of a pass. Passes come from the module's timer and
    /// from `Drain`; two at once would each dispatch up to a lane's
    /// concurrency, so a lane of one could deliver two turns together.
    pass: tokio::sync::Mutex<()>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pipeline")
            .field("queues", &self.queues)
            .field("running", &self.running)
            .finish_non_exhaustive()
    }
}

impl Pipeline {
    /// Start building a pipeline over `queues` and `docs`.
    #[must_use]
    pub fn builder(queues: Arc<dyn QueueStore>, docs: Arc<dyn DocumentStore>) -> PipelineBuilder {
        PipelineBuilder::new(queues, docs)
    }

    pub(crate) fn now(&self) -> u64 {
        (self.inner.clock)()
    }

    fn running(&self) -> Arc<Running> {
        let guard = self
            .inner
            .running
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(&guard)
    }

    /// The configuration in effect.
    #[must_use]
    pub fn config(&self) -> PipelineConfig {
        self.running().config.clone()
    }

    /// The queues the configuration in effect resolved to.
    #[must_use]
    pub fn queues(&self) -> ResolvedQueues {
        self.running().queues.clone()
    }

    /// Durably queue `events` and report what happened to each, in order.
    ///
    /// Returns only after every accepted event is stored, so the caller may
    /// acknowledge the platform for each result whose `ack_platform` is set.
    ///
    /// # Errors
    ///
    /// A storage error; events before the failing write may already be
    /// stored, and ingesting them again is the safe retry (they come back as
    /// duplicates).
    pub async fn ingest(&self, events: Vec<InboundEvent>) -> Result<IngestResponse> {
        let running = self.running();
        self.inner
            .ingress
            .ingest(events, &running.queues.ingress, &running.config, self.now())
            .await
    }

    /// Replace the configuration in effect.
    ///
    /// The new configuration is checked against the queue manifest the
    /// pipeline was built with; on any error the old one stays in effect.
    ///
    /// # Errors
    ///
    /// [`Error::Config`](crate::Error::Config) naming the offending field.
    pub fn configure(&self, config: PipelineConfig) -> Result<()> {
        check_values(&config)?;
        let queues = resolve_queues(&config, &self.inner.manifest)?;
        let mut guard = self
            .inner
            .running
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = Arc::new(Running { config, queues });
        Ok(())
    }

    /// Queue depths and terminal outcome counts.
    ///
    /// # Errors
    ///
    /// A storage error.
    pub async fn stats(&self) -> Result<StatsResponse> {
        let running = self.running();
        let mut depths = Vec::new();
        for queue in running.queues.all() {
            let stats = self.inner.queues.stats(&queue.name).await?;
            depths.push(QueueDepth::new(
                queue.name.clone(),
                stats.ready,
                stats.delayed,
                stats.leased,
                stats.dead,
            ));
        }
        Ok(StatsResponse::new(
            depths,
            self.inner.ledger.counts().await?,
        ))
    }

    /// Up to `limit` dead items, oldest parked work first.
    ///
    /// # Errors
    ///
    /// A storage error.
    pub async fn list_dead(&self, limit: usize) -> Result<Vec<DeadItem>> {
        let running = self.running();
        self.inner.dead.list(&running.queues, limit).await
    }

    /// Put the dead item `id` back into the pipeline. Returns `false` when no
    /// dead item has that id.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownDeadItem`](crate::Error::UnknownDeadItem) for an id
    /// that names no pipeline queue, or a storage error.
    pub async fn redrive(&self, id: &str) -> Result<bool> {
        let running = self.running();
        self.inner.dead.redrive(&running.queues, id).await
    }

    /// Run one pass of the pipeline: take events off the ingress queue
    /// through the gate into the debounce buffer, flush due buffers into
    /// lane queues, settle parked dead letters, record dead outcomes for work
    /// its queues dead-lettered, and prune expired tombstones and outcome
    /// records.
    ///
    /// Passes are serialized: a call made while another pass runs (the
    /// module's timer and an on-demand `Drain`) waits for it, so a lane's
    /// concurrency bounds deliveries across every caller.
    ///
    /// # Errors
    ///
    /// A storage error; the pass can be run again.
    pub async fn drain(&self) -> Result<DrainReport> {
        let _pass = self.inner.pass.lock().await;
        let running = self.running();
        let now = self.now();
        let ttl = running.config.ingress.tombstone_ttl_ms;
        let mut report = DrainReport::default();
        report.ingested = self.take_ingress(&running, now).await?;
        report.flushed = self.flush_due(&running, now).await?;
        report.scheduled = self.release_staged(&running, now).await?;
        self.release_digest(&running, now).await?;
        let (dispatched, dead) = self.dispatch(&running, now).await?;
        report.dispatched = dispatched;
        self.inner.dead.settle(&running.queues.dead).await?;
        report.dead = dead + self.inner.dead.sweep(&running.queues, now, ttl).await?;
        report.pruned =
            self.inner.tombstones.prune(now).await? + self.inner.ledger.prune(now).await?;
        Ok(report)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "invariants_tests.rs"]
mod invariants;
