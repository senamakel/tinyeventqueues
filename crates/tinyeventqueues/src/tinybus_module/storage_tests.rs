//! Tests for opening the module's storage: the host's over the bus, and the
//! memory fallback.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use tinyeventqueues_bus::storage::OUTCOMES;
use tinystoragedrivers_core::{Precondition, Scope, StorageBackend};

use super::*;
use crate::tinybus_module::tests::rig;
use tinybus::{Connection, broker::Broker, transport::memory::MemoryBus};

#[path = "storage_bound_tests.rs"]
mod bound_tests;

#[tokio::test]
async fn a_host_that_serves_storage_backs_the_pipeline() {
    let rig = rig(true).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let backing = open(module, None).await.unwrap();
    assert!(backing.durable);
    assert!(format!("{backing:?}").contains("durable: true"));

    // A write through the module lands in the host's backend.
    for spec in tinyeventqueues_bus::storage::collection_specs() {
        backing.docs.ensure_collection(&spec).await.unwrap();
    }
    backing
        .docs
        .put(OUTCOMES, "x", serde_json::json!({}), Precondition::None)
        .await
        .unwrap();
    let host = rig.backend.for_scope(&Scope::local()).unwrap();
    assert!(host.documents().get(OUTCOMES, "x").await.unwrap().is_some());
}

#[tokio::test]
async fn a_host_without_storage_leaves_the_pipeline_on_memory() {
    let rig = rig(false).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let backing = open(module, None).await.unwrap();
    assert!(!backing.durable);
}

#[tokio::test]
async fn a_storage_host_that_cannot_be_reached_fails_setup_instead_of_using_memory() {
    let rig = rig(false).await;
    // Something owns the storage name but serves no storage objects: a host
    // whose storage is present but failing.
    rig.host
        .request_name(tinystoragedrivers_busproxy::HOST_NAME)
        .await
        .unwrap();
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let refused = open(module, None).await;
    assert!(refused.is_err(), "fell back to memory: {refused:?}");
}

#[tokio::test]
async fn an_explicit_bound_descriptor_never_falls_back_when_its_route_is_unavailable() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let module = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    assert!(
        module
            .name_owner(tinystoragedrivers_busproxy::HOST_NAME)
            .await
            .unwrap()
            .is_none(),
        "the fixture has no legacy well-known storage root"
    );
    let descriptor = serde_json::from_value(serde_json::json!({
        "service_peer": ":1.404",
        "registration_id": 404,
        "object_root": "/ai/tinyhumans/tinystorage/Leases/404",
    }))
    .unwrap();

    let refused = open(module, Some(descriptor)).await;
    assert!(
        refused.is_err(),
        "an explicit but unavailable lease must fail instead of using memory: {refused:?}"
    );
}
