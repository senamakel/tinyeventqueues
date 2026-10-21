//! Integration tests for the public crate surface.
//!
//! These tests link against the crate as a downstream consumer would: they can
//! only use what `src/lib.rs` re-exports, plus the storage crate a host already
//! has. A change that breaks a test here is a breaking change for users.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use tinyeventqueues::config::{SHIPPED_PIPELINE_JSON, parse_pipeline_config, shipped_manifest};
use tinyeventqueues::{
    CollectingSink, Disposition, Error, EventKind, InboundEvent, ManualClock, Pipeline, Reason,
    Sender, tinyeventqueues_bus,
};
use tinystoragedrivers_testkit::memory_storage;
use tinystoragedrivers_types::Scope;

fn event(id: &str) -> InboundEvent {
    InboundEvent::new(
        "webhook:github",
        id,
        "repo-1",
        Sender::new("octocat"),
        EventKind::Trigger,
        0,
    )
}

#[tokio::test]
async fn a_restarted_pipeline_rejects_a_redelivery() {
    let clock = ManualClock::new(5_000);
    let storage = memory_storage(&Scope::new("public-api").unwrap());
    let docs = Arc::clone(storage.documents());
    let queues = Arc::clone(storage.queues());
    let build = || async {
        Pipeline::builder(Arc::clone(&queues), Arc::clone(&docs))
            .clock(clock.clock())
            .build()
            .await
            .unwrap()
    };

    let sink = Arc::new(CollectingSink::new());
    let first = Pipeline::builder(Arc::clone(&queues), Arc::clone(&docs))
        .clock(clock.clock())
        .sink(sink.clone())
        .build()
        .await
        .unwrap();
    let accepted = first.ingest(vec![event("delivery-1")]).await.unwrap();
    assert_eq!(accepted.results[0].disposition, Disposition::Accepted);
    assert_eq!(sink.outcomes()[0].reason, Reason::Ingested);

    let restarted = build().await;
    let again = restarted.ingest(vec![event("delivery-1")]).await.unwrap();
    assert_eq!(again.results[0].disposition, Disposition::Duplicate);
    assert_eq!(again.results[0].reason, Some(Reason::DuplicateRedelivery));
}

#[test]
fn the_shipped_files_are_public_and_valid() {
    assert_ne!(shipped_manifest().unwrap().queues.len(), 0);
    let config = parse_pipeline_config(SHIPPED_PIPELINE_JSON).unwrap();
    assert_eq!(config.version, 1);
}

#[test]
fn config_errors_are_matchable() {
    let error = parse_pipeline_config("{\"version\": 1}").unwrap_err();
    assert!(matches!(error, Error::Config { .. }));
}

#[test]
fn the_contract_is_reexported_as_the_same_types() {
    let event: tinyeventqueues_bus::InboundEvent = event("1");
    let _: InboundEvent = event;
    assert!(tinyeventqueues::is_compatible(
        tinyeventqueues::CONTRACT_VERSION
    ));
}

/// A host as a downstream crate writes one.
#[derive(Debug, Default)]
struct Agent {
    turns: std::sync::Mutex<Vec<tinyeventqueues::Turn>>,
}

#[async_trait::async_trait]
impl tinyeventqueues::Host for Agent {
    async fn classify_salience(
        &self,
        _request: tinyeventqueues::ClassifySalienceRequest,
    ) -> tinyeventqueues::Result<tinyeventqueues::ClassifySalienceResponse> {
        Ok(tinyeventqueues::ClassifySalienceResponse::new(
            tinyeventqueues::SalienceClass::Fyi,
        ))
    }

    async fn deliver(
        &self,
        request: tinyeventqueues::DeliverRequest,
    ) -> tinyeventqueues::Result<tinyeventqueues::DeliverResponse> {
        self.turns.lock().unwrap().push(request.turn);
        Ok(tinyeventqueues::DeliverResponse::new(
            tinyeventqueues::DeliverStatus::Accepted,
        ))
    }
}

#[tokio::test]
async fn a_host_receives_turns_from_drain() {
    let clock = ManualClock::new(5_000);
    let storage = memory_storage(&Scope::new("public-api-host").unwrap());
    let docs = Arc::clone(storage.documents());
    let queues = Arc::clone(storage.queues());
    let agent = Arc::new(Agent::default());
    let pipeline = Pipeline::builder(queues, Arc::clone(&docs))
        .clock(clock.clock())
        .host(agent.clone())
        .build()
        .await
        .unwrap();
    let dm = InboundEvent::new(
        "web:chat",
        "1",
        "visitor-1",
        Sender::new("v"),
        EventKind::Message,
        5_000,
    )
    .direct(true)
    .text("hi there");
    pipeline.ingest(vec![dm]).await.unwrap();
    pipeline.drain().await.unwrap();
    clock.advance(2_000);
    let report = pipeline.drain().await.unwrap();
    assert_eq!(report.dispatched, 1);
    assert_eq!(agent.turns.lock().unwrap()[0].events[0].event_id, "1");
    assert_eq!(pipeline.stats().await.unwrap().outcomes.delivered, 1);
}
