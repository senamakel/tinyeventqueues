//! [`PipelineBuilder`]: loads the editable files and wires the stages.

use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::config::QueueManifest;
use crate::ports::{Clock, DocumentStore, QueueStore};
use tinyeventqueues_bus::PipelineConfig;

use super::{Inner, Pipeline, Running};
use crate::Result;
use crate::cadence::Scheduler;
use crate::clock::system_clock;
use crate::coalesce::Coalescer;
use crate::config::{
    check_values, load_manifest, load_pipeline_config, resolve_queues, shipped_manifest,
    shipped_pipeline_config,
};
use crate::dead::DeadLetters;
use crate::digest::Digest;
use crate::gate::{Ambient, Gate};
use crate::host::Host;
use crate::ingress::Ingress;
use crate::observe::{NoopSink, OutcomeSink};
use crate::salience::Habituation;
use crate::store::{self, Ledger, Tombstones};
use crate::turn::{Conversations, Lanes, Threads};

/// Where a file-backed setting comes from.
#[derive(Debug, Clone)]
enum Source<T> {
    Shipped,
    Path(PathBuf),
    Value(T),
}

/// Builds a [`Pipeline`]: loads the queue manifest and the configuration,
/// checks them against each other, and declares queues and collections.
pub struct PipelineBuilder {
    queues: Arc<dyn QueueStore>,
    docs: Arc<dyn DocumentStore>,
    manifest: Source<QueueManifest>,
    config: Source<PipelineConfig>,
    clock: Option<Clock>,
    sink: Arc<dyn OutcomeSink>,
    host: Option<Arc<dyn Host>>,
    consumer: String,
}

impl std::fmt::Debug for PipelineBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineBuilder")
            .field("manifest", &self.manifest)
            .field("config", &self.config)
            .field("consumer", &self.consumer)
            .finish_non_exhaustive()
    }
}

impl PipelineBuilder {
    pub(super) fn new(queues: Arc<dyn QueueStore>, docs: Arc<dyn DocumentStore>) -> Self {
        Self {
            queues,
            docs,
            manifest: Source::Shipped,
            config: Source::Shipped,
            clock: None,
            sink: Arc::new(NoopSink),
            host: None,
            consumer: "tinyeventqueues".to_owned(),
        }
    }

    /// Use this queue manifest instead of the shipped `queues.json`.
    #[must_use]
    pub fn manifest(mut self, manifest: QueueManifest) -> Self {
        self.manifest = Source::Value(manifest);
        self
    }

    /// Load the queue manifest from `path` when building.
    #[must_use]
    pub fn manifest_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.manifest = Source::Path(path.into());
        self
    }

    /// Use this configuration instead of the shipped `pipeline.json`.
    #[must_use]
    pub fn config(mut self, config: PipelineConfig) -> Self {
        self.config = Source::Value(config);
        self
    }

    /// Load the pipeline configuration from `path` when building.
    #[must_use]
    pub fn config_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.config = Source::Path(path.into());
        self
    }

    /// Read time from `clock` instead of the system clock. Give the queue
    /// store the same clock.
    #[must_use]
    pub fn clock(mut self, clock: Clock) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Report outcomes to `sink`.
    #[must_use]
    pub fn sink(mut self, sink: Arc<dyn OutcomeSink>) -> Self {
        self.sink = sink;
        self
    }

    /// Call `host` back for salience classification.
    #[must_use]
    pub fn host(mut self, host: Arc<dyn Host>) -> Self {
        self.host = Some(host);
        self
    }

    /// The consumer name the pipeline leases queue messages as.
    #[must_use]
    pub fn consumer(mut self, consumer: impl Into<String>) -> Self {
        self.consumer = consumer.into();
        self
    }

    /// Load and check the files, declare the queues and collections, and
    /// return the pipeline.
    ///
    /// # Errors
    ///
    /// [`crate::Error::Manifest`] or [`crate::Error::Config`] naming the
    /// offending field, [`crate::Error::ConfigFile`] for an unreadable file,
    /// or a storage error while declaring queues and collections.
    pub async fn build(self) -> Result<Pipeline> {
        let manifest = match self.manifest {
            Source::Shipped => shipped_manifest()?,
            Source::Path(path) => load_manifest(path)?,
            Source::Value(manifest) => {
                manifest.validate()?;
                manifest
            }
        };
        let config = match self.config {
            Source::Shipped => shipped_pipeline_config()?,
            Source::Path(path) => load_pipeline_config(path)?,
            Source::Value(config) => {
                check_values(&config)?;
                config
            }
        };
        let resolved = resolve_queues(&config, &manifest)?;
        manifest.apply(self.queues.as_ref()).await?;
        store::ensure(self.docs.as_ref()).await?;

        let ledger = Ledger::new(Arc::clone(&self.docs), self.sink);
        let tombstones = Tombstones::new(Arc::clone(&self.docs));
        let ingress = Ingress::new(Arc::clone(&self.queues), tombstones.clone(), ledger.clone());
        let dead = DeadLetters::new(
            Arc::clone(&self.queues),
            Arc::clone(&self.docs),
            ledger.clone(),
            self.consumer.clone(),
        );
        let gate = Gate::new(&self.docs);
        let ambient = Ambient::new(&self.docs);
        let coalescer = Coalescer::new(&self.docs);
        let conversations = Conversations::new(&self.docs);
        let lanes = Lanes::new(
            Arc::clone(&self.queues),
            tombstones.clone(),
            conversations.clone(),
        );
        let habituation = Habituation::new(&self.docs);
        let digest = Digest::new(&self.docs);
        let scheduler = Scheduler::new(&self.docs);
        Ok(Pipeline {
            inner: Arc::new(Inner {
                queues: self.queues,
                clock: self.clock.unwrap_or_else(system_clock),
                manifest,
                running: RwLock::new(Arc::new(Running {
                    config,
                    queues: resolved,
                })),
                ledger,
                tombstones,
                ingress,
                dead,
                gate,
                ambient,
                coalescer,
                lanes,
                habituation,
                digest,
                scheduler,
                conversations,
                threads: Threads::new(&self.docs),
                host: self.host,
                consumer: self.consumer,
                pass: tokio::sync::Mutex::new(()),
            }),
        })
    }
}
