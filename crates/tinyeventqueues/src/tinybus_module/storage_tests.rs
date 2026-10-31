//! Tests for the module's host-backed TSD v2 storage routes.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tinybus::Connection;
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tinyeventqueues_bus::storage::OUTCOMES;
use tinystoragedrivers_api::StorageBackend;
use tinystoragedrivers_busproxy::{
    PerCallerEndpoint, StorageAddress, StorageServer, connect_storage_at_address, serve_at_address,
};
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::{Precondition, Scope};

use super::*;
use crate::store::{collection_spec, collection_specs};
use crate::tinybus_module::tests::rig;

#[tokio::test]
async fn the_default_route_uses_the_host_scoped_storage_ports() {
    let rig = rig(true).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let backing = open(module, None).await.unwrap();

    for declaration in collection_specs() {
        let spec = collection_spec(declaration);
        backing.docs.ensure_collection(&spec).await.unwrap();
    }
    backing
        .docs
        .put(
            OUTCOMES,
            "x",
            serde_json::json!({"stored": true}),
            Precondition::None,
        )
        .await
        .unwrap();
    let host = rig
        .backend
        .for_scope(&Scope::new("local").unwrap())
        .unwrap();
    assert!(host.documents().get(OUTCOMES, "x").await.unwrap().is_some());
}

#[tokio::test]
async fn missing_or_unusable_global_storage_fails_setup() {
    let rig = rig(false).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    assert!(open(module, None).await.is_err());

    // A host name without the storage objects is unavailable, not permission
    // to acknowledge work in memory.
    rig.host
        .request_name(tinystoragedrivers_busproxy::HOST_NAME)
        .await
        .unwrap();
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    assert!(open(module, None).await.is_err());
}

#[tokio::test]
async fn an_explicit_invocation_address_never_falls_back_to_global_storage() {
    let rig = rig(true).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let host = rig.host.unique_name().unwrap().clone();
    let address = StorageAddress::try_new(
        host,
        format!(
            "{}/missing",
            tinystoragedrivers_busproxy::INVOCATION_OBJECT_ROOT
        )
        .parse()
        .unwrap(),
    )
    .unwrap();

    assert!(open(module, Some(address)).await.is_err());
}

#[tokio::test]
async fn an_issued_invocation_address_is_caller_bound_and_revocable() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let module = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let module_peer = module.unique_name().unwrap().clone();
    let address = StorageAddress::try_new(
        host.unique_name().unwrap().clone(),
        format!(
            "{}/event_queues_1",
            tinystoragedrivers_busproxy::INVOCATION_OBJECT_ROOT
        )
        .parse()
        .unwrap(),
    )
    .unwrap();
    let backend = Arc::new(MemoryBackend::new());
    let scope = Scope::new("event-queues-bound").unwrap();
    let admitted_calls = Arc::new(AtomicUsize::new(0));
    let endpoint_backend = Arc::clone(&backend);
    let endpoint_scope = scope.clone();
    let endpoint_peer = module_peer.clone();
    let endpoint_calls = Arc::clone(&admitted_calls);
    let endpoint = PerCallerEndpoint::new(move |sender| {
        if sender != &endpoint_peer {
            return None;
        }
        endpoint_calls.fetch_add(1, Ordering::Relaxed);
        let storage = endpoint_backend.for_scope(&endpoint_scope).ok()?;
        Some(StorageServer::new(storage))
    });
    let served = serve_at_address(&host, address.clone(), Arc::new(endpoint))
        .await
        .unwrap();

    let backing = open(module.clone(), Some(address.clone())).await.unwrap();
    for declaration in collection_specs() {
        let spec = collection_spec(declaration);
        backing.docs.ensure_collection(&spec).await.unwrap();
    }
    backing
        .docs
        .put(
            OUTCOMES,
            "bound",
            serde_json::json!({"stored": true}),
            Precondition::None,
        )
        .await
        .unwrap();
    let scoped = backend.for_scope(&scope).unwrap();
    assert!(
        scoped
            .documents()
            .get(OUTCOMES, "bound")
            .await
            .unwrap()
            .is_some()
    );

    let outsider = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    assert!(connect_storage_at_address(outsider, address).await.is_err());
    assert!(admitted_calls.load(Ordering::Relaxed) > 0);

    served.shutdown().await.unwrap();
    assert!(backing.docs.get(OUTCOMES, "bound").await.is_err());
}

#[tokio::test]
async fn an_unattested_callback_is_refused_even_when_it_serves_storage() {
    let rig = rig(true).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let result = open_callback(module, rig.host.unique_name().unwrap().clone()).await;
    assert!(
        result.is_err(),
        "ordinary peers must not acquire callback authority"
    );
}

#[tokio::test]
async fn a_missing_callback_never_uses_working_global_storage() {
    let rig = rig(true).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let missing = tinybus::BusName::new(":1.999999").unwrap();
    assert!(open_callback(module, missing).await.is_err());
}

#[tokio::test]
async fn a_well_known_callback_name_cannot_borrow_working_global_storage() {
    let rig = rig(true).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let peer = BusName::new("ai.tinyhumans.tinystorage").unwrap();
    let error = open_callback(module, peer).await.unwrap_err();
    let message = error.to_string();
    assert!(message.contains("storage_callback"), "{message}");
    assert!(message.contains("expected a unique host peer"), "{message}");
}
