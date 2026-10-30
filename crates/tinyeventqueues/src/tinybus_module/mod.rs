//! `TinyBus` module entrypoint and bus-facing interface.
//!
//! The adapter serves [`Pipeline`] at [`names::OBJECT_PATH`]. The names and
//! payload types come from [`tinyeventqueues_bus`], so a host spells them from
//! the contract crate instead of repeating string literals.
//!
//! * **Storage** ([`storage`]): the host's, over the storage bus — documents
//!   for the pipeline's `teq.*` state and the queue port for the queues the
//!   manifest declares, in the one scope the host bound this module to. An
//!   explicit invocation address is never substituted, and setup requires
//!   host-backed storage rather than an in-memory fallback.
//! * **Configuration** ([`settings`]): the host's private configuration blob
//!   may carry an invocation-scoped storage address, a `queues.json` and a
//!   `pipeline.json` document, and timer bounds; absent, the shipped files run.
//!   The manifest is applied through the queue port at setup.
//! * **Host callbacks** ([`host`]): `ClassifySalience` and `Deliver` on the
//!   host's optional `ai.tinyhumans.tinyeventqueues.Host` interface.
//! * **Passes** ([`ticker`]): the module runs passes on its own timer, waking
//!   at the next debounce flush or cadence tick and right after an `Ingest`.
//!   `Drain` runs one on demand.

mod host;
mod settings;
mod storage;
mod ticker;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tinybus::{Connection, Result as TinyBusResult};
use tinyeventqueues_bus::{
    ConfigureRequest, ConfigureResponse, DrainRequest, DrainResponse, IngestRequest,
    IngestResponse, ListDeadRequest, ListDeadResponse, RedriveRequest, RedriveResponse,
    StatsRequest, StatsResponse, names,
};

use self::host::BusHost;
use self::settings::Settings;
use self::storage::Backing;
use self::ticker::{Slot, Wake};
use crate::{Host, Pipeline};

struct EventQueuesService {
    pipeline: Pipeline,
    wake: Wake,
}

fn failed(error: &crate::Error) -> tinybus::Error {
    tinybus::Error::failed(error.to_string())
}

#[tinybus::interface(name = "ai.tinyhumans.tinyeventqueues.EventQueues")]
impl EventQueuesService {
    async fn ingest(&self, request: IngestRequest) -> TinyBusResult<IngestResponse> {
        let ingested = self
            .pipeline
            .ingest(request.events)
            .await
            .map_err(|error| failed(&error))?;
        self.wake.now();
        Ok(ingested)
    }

    async fn configure(&self, request: ConfigureRequest) -> TinyBusResult<ConfigureResponse> {
        std::future::ready(self.pipeline.configure(request.config))
            .await
            .map(|()| ConfigureResponse::new(true))
            .map_err(|error| failed(&error))
    }

    async fn stats(&self, _request: StatsRequest) -> TinyBusResult<StatsResponse> {
        self.pipeline.stats().await.map_err(|error| failed(&error))
    }

    async fn list_dead(&self, request: ListDeadRequest) -> TinyBusResult<ListDeadResponse> {
        let limit = usize::try_from(request.limit).unwrap_or(usize::MAX);
        self.pipeline
            .list_dead(limit)
            .await
            .map(ListDeadResponse::new)
            .map_err(|error| failed(&error))
    }

    async fn redrive(&self, request: RedriveRequest) -> TinyBusResult<RedriveResponse> {
        let redriven = self
            .pipeline
            .redrive(&request.id)
            .await
            .map_err(|error| failed(&error))?;
        if redriven {
            self.wake.now();
        }
        Ok(RedriveResponse::new(redriven))
    }

    async fn drain(&self, _request: DrainRequest) -> TinyBusResult<DrainResponse> {
        self.pipeline
            .drain()
            .await
            .map(DrainResponse::new)
            .map_err(|error| failed(&error))
    }
}

/// The pipeline `settings` describe over `backing`, calling `host` back.
async fn pipeline(
    backing: Backing,
    settings: &Settings,
    host: Option<Arc<dyn Host>>,
) -> crate::Result<Pipeline> {
    let builder = Pipeline::builder(backing.queues, backing.docs)
        .manifest(settings.manifest()?)
        .config(settings.pipeline()?);
    match host {
        Some(host) => builder.host(host).build().await,
        None => builder.build().await,
    }
}

/// One `setup` call of a module instance.
type SetupFuture = Pin<Box<dyn Future<Output = TinyBusResult<()>> + Send>>;

/// Runs once per module instance: every setup of the instance (the first and
/// each reinitialization) shares its timer slot.
fn setup_factory() -> impl Fn(Connection, serde_json::Value) -> SetupFuture + Clone + Send + Sync {
    let slot = Slot::default();
    move |connection, config| Box::pin(setup(connection, config, slot.clone()))
}

async fn setup(connection: Connection, config: serde_json::Value, slot: Slot) -> TinyBusResult<()> {
    let settings = Settings::parse(config).map_err(|error| failed(&error))?;
    let backing = if let Some(peer) = settings
        .storage_callback()
        .map_err(|error| failed(&error))?
    {
        storage::open_callback(connection.clone(), peer).await
    } else {
        let address = settings.storage_address().map_err(|error| failed(&error))?;
        storage::open(connection.clone(), address).await
    }
    .map_err(|error| failed(&error))?;
    install(connection, settings, slot, backing).await
}

#[cfg(test)]
async fn setup_with_test_backing(
    connection: Connection,
    config: serde_json::Value,
    slot: Slot,
    backing: Backing,
) -> TinyBusResult<()> {
    let settings = Settings::parse(config).map_err(|error| failed(&error))?;
    install(connection, settings, slot, backing).await
}

async fn install(
    connection: Connection,
    settings: Settings,
    slot: Slot,
    backing: Backing,
) -> TinyBusResult<()> {
    let host: Arc<dyn Host> = Arc::new(BusHost {
        connection: connection.clone(),
    });
    let pipeline = pipeline(backing, &settings, Some(host))
        .await
        .map_err(|error| failed(&error))?;
    let wake = ticker::start(&slot, pipeline.clone(), settings.timer);
    connection
        .serve_at(
            names::OBJECT_PATH.try_into()?,
            EventQueuesService { pipeline, wake },
        )
        .await?;
    connection.request_name(names::INTERFACE).await?;
    log::info!("[tinyeventqueues] module ready with host storage");
    Ok(())
}

// Process-global state audit (static-link / co-linking): this module keeps no
// process-wide statics. The pass timer lives in a slot the setup factory
// captures per instance, so another module linked into the same executable
// (or a second instance) can neither observe nor stop it.
#[allow(
    missing_docs,
    reason = "the export macro emits the ABI descriptor, manifest and init entry points without doc comments; nothing else may live in this module"
)]
pub(crate) mod exports {
    tinybus_module::module_export_optional_static! {
        setup_factory = super::setup_factory,
        config = serde_json::Value,
        worker_threads = 2,
        provides = ["ai.tinyhumans.tinyeventqueues.EventQueues"],
        methods = ["Ingest", "Configure", "Stats", "ListDead", "Redrive", "Drain"],
        signals = [],
        requires = [],
        optional = ["ai.tinyhumans.tinyeventqueues.Host"],
        lazy = false,
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
