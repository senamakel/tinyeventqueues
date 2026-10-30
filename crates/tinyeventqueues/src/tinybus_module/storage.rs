//! The module's host-owned storage ports.

use std::sync::Arc;

use tinybus::{BusName, Connection};
use tinystoragedrivers_api::{DocumentStore, QueueStore, Storage};
use tinystoragedrivers_busproxy::StorageAddress;

use crate::{Error, Result};

/// The host-provided storage handles used by the pipeline.
#[derive(Debug, Clone)]
pub(crate) struct Backing {
    pub(crate) queues: Arc<dyn QueueStore>,
    pub(crate) docs: Arc<dyn DocumentStore>,
}

impl Backing {
    pub(crate) fn of(storage: &Storage) -> Self {
        Self {
            queues: Arc::clone(storage.queues()),
            docs: Arc::clone(storage.documents()),
        }
    }
}

/// Connect to one explicitly admitted host callback peer at the standard ports.
/// Invalid, missing, or unavailable peers never fall back to global storage.
///
/// # Errors
/// Returns a configuration error for a non-unique peer, or the storage error
/// when the admitted callback cannot provide its ports.
pub(crate) async fn open_callback(connection: Connection, peer: BusName) -> Result<Backing> {
    if !peer.is_unique() {
        return Err(Error::config(
            "storage_callback",
            "expected a unique host peer",
        ));
    }
    let storage = tinystoragedrivers_busproxy::connect_storage_at(connection, peer)
        .await
        .map_err(Error::from)?;
    Ok(Backing::of(&storage))
}

/// Connect to the host's v2 storage ports. An explicit invocation address is
/// used as-is; the default uses the host's ordinary caller-scoped endpoint.
/// Missing host storage is an error, so the module never acknowledges work
/// against an ephemeral engine.
pub(crate) async fn open(
    connection: Connection,
    address: Option<StorageAddress>,
) -> Result<Backing> {
    let storage = match address {
        Some(address) => {
            tinystoragedrivers_busproxy::connect_storage_at_address(connection, address).await
        }
        None => tinystoragedrivers_busproxy::connect_storage(connection).await,
    }
    .map_err(Error::from)?;
    Ok(Backing::of(&storage))
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
