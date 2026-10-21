//! The module's storage: the host's, over the storage bus, or a loud fallback.
//!
//! The module owns no storage backend, file or path. It connects to the host's
//! `ai.tinyhumans.tinystorage.Storage` objects and takes the one scope the
//! host bound it to: documents for the pipeline's `teq.*` state and the queue
//! port for its queues, so queued events survive a restart and live wherever
//! the host keeps its data (`SQLite`, `MongoDB`, a broker).
//!
//! With a host-issued descriptor, the module uses only that lease and fails
//! setup when it cannot be reached. Without a descriptor, a host that serves no
//! storage at all (nobody owns the storage name on the bus: an older host, a
//! bare test broker) leaves the module on in-process memory. That still runs
//! the contract end to end, but nothing queued survives the process, so it is
//! logged as an error. When a storage host *is* present and connecting to it
//! fails, setup fails: falling back to memory then would acknowledge events
//! that vanish on restart while durable storage exists, so the failure is
//! surfaced for the host to retry the load instead.

use std::sync::Arc;

use tinybus::Connection;
use tinystoragedrivers_busproxy::{BusTransport, StorageDescriptor, module_scope_label};
use tinystoragedrivers_client::BusStorage;
use tinystoragedrivers_core::queue::QueueStore;
use tinystoragedrivers_core::{
    DocumentStore, MemoryStorage, Scope, ScopedStorage, StorageBackend, StorageError,
};

use crate::Result;

/// Where the pipeline keeps its work and state.
#[derive(Debug, Clone)]
pub(crate) struct Backing {
    pub(crate) queues: Arc<dyn QueueStore>,
    pub(crate) docs: Arc<dyn DocumentStore>,
    /// Whether the backing is the host's storage (durable) or memory.
    pub(crate) durable: bool,
}

impl Backing {
    fn of(storage: &ScopedStorage, durable: bool) -> Self {
        Self {
            queues: Arc::clone(storage.queues()),
            docs: Arc::clone(storage.documents()),
            durable,
        }
    }

    /// Fresh in-process memory storage: not durable.
    ///
    /// # Errors
    ///
    /// Never in practice: the local scope always opens on memory.
    pub(crate) fn memory() -> Result<Self> {
        let storage = MemoryStorage::new().for_scope(&Scope::local())?;
        Ok(Self::of(&storage, false))
    }
}

/// The host's storage for this module, or memory when the host serves none.
///
/// When a descriptor is supplied, its lease is mandatory; setup never falls
/// back to the legacy root or in-process memory. Without one, the legacy
/// well-known-root probe and memory fallback remain in effect.
///
/// # Errors
///
/// [`crate::Error::Storage`] when the bus cannot say whether a storage host
/// exists, or a storage host exists but connecting to it or binding the
/// module's scope fails.
pub(crate) async fn open(
    connection: Connection,
    descriptor: Option<StorageDescriptor>,
) -> Result<Backing> {
    if let Some(descriptor) = descriptor {
        let scoped = connect_bound(connection, descriptor).await;
        if let Err(error) = &scoped {
            log::error!("[tinyeventqueues] configured storage lease is unusable: {error}");
        }
        let scoped = scoped?;
        log::info!("[tinyeventqueues] pipeline on host storage lease (durable)");
        return Ok(Backing::of(&scoped, true));
    }

    let host = connection
        .name_owner(tinystoragedrivers_busproxy::HOST_NAME)
        .await
        .map_err(|error| StorageError::unavailable(format!("storage host lookup: {error}")))?;
    if host.is_none() {
        log::error!(
            "[tinyeventqueues] host serves no storage; \
             running on IN-MEMORY storage, queued events are NOT durable"
        );
        return Backing::memory();
    }
    let scoped = connect(connection).await;
    if let Err(error) = &scoped {
        log::error!("[tinyeventqueues] host storage present but unusable: {error}");
    }
    let scoped = scoped?;
    log::info!("[tinyeventqueues] pipeline on host storage (durable)");
    Ok(Backing::of(&scoped, true))
}

/// The module's scope of the host's storage.
async fn connect(connection: Connection) -> tinystoragedrivers_core::Result<ScopedStorage> {
    tinystoragedrivers_busproxy::connect_storage(connection)
        .await?
        .for_scope(&tinystoragedrivers_busproxy::module_scope_label())
}

/// Connect only to the TSD lease selected by the host.
async fn connect_bound(
    connection: Connection,
    descriptor: StorageDescriptor,
) -> tinystoragedrivers_core::Result<ScopedStorage> {
    let transport = BusTransport::connect_bound(connection, descriptor)
        .await
        .map_err(|error| StorageError::unavailable(error.to_string()))?;
    let storage = BusStorage::connect(Arc::new(transport), module_scope_label()).await?;
    storage.for_scope(&module_scope_label())
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
