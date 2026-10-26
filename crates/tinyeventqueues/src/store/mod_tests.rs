//! Unit tests for the tombstones and the outcome ledger.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use tinyeventqueues_bus::{Disposition, EventRef, Outcome, Reason};
use tinystoragedrivers_api::DocumentStore;
use tinystoragedrivers_testkit::memory_storage;
use tinystoragedrivers_types::Scope;

use super::*;
use crate::observe::CollectingSink;

async fn docs() -> Arc<dyn DocumentStore> {
    let storage = memory_storage(&Scope::new("test-store").unwrap());
    let docs = Arc::clone(storage.documents());
    ensure(docs.as_ref()).await.unwrap();
    docs
}

#[tokio::test]
async fn tombstones_live_until_they_expire_and_then_prune() {
    let tombstones = Tombstones::new(docs().await);
    assert!(!tombstones.live("s", "1", 0).await.unwrap());
    tombstones.put("s", "1", 100).await.unwrap();
    assert!(tombstones.live("s", "1", 99).await.unwrap());
    assert!(!tombstones.live("s", "1", 100).await.unwrap());
    assert_eq!(tombstones.prune(100).await.unwrap(), 1);
    assert_eq!(tombstones.prune(100).await.unwrap(), 0);
}

#[tokio::test]
async fn an_event_ends_at_most_once() {
    let sink = Arc::new(CollectingSink::new());
    let ledger = Ledger::new(docs().await, sink.clone());
    let event = EventRef::new("s", "1");
    let delivered =
        Outcome::new(event.clone(), Disposition::Delivered, Reason::Turn, 10).in_turn("t");
    assert!(ledger.record(delivered.clone(), 50).await.unwrap());
    let again = Outcome::new(event.clone(), Disposition::Dropped, Reason::BotLoop, 11);
    assert!(!ledger.record(again, 50).await.unwrap());
    assert_eq!(
        ledger.terminal(&event).await.unwrap(),
        Some(delivered.clone())
    );
    assert_eq!(sink.outcomes(), vec![delivered]);
}

#[tokio::test]
async fn non_terminal_outcomes_only_reach_the_sink() {
    let sink = Arc::new(CollectingSink::new());
    let ledger = Ledger::new(docs().await, sink.clone());
    let accepted = Outcome::new(
        EventRef::new("s", "1"),
        Disposition::Accepted,
        Reason::Ingested,
        1,
    );
    assert!(ledger.record(accepted.clone(), 50).await.unwrap());
    ledger.notify(&accepted);
    assert_eq!(ledger.terminal(&accepted.event).await.unwrap(), None);
    assert_eq!(sink.outcomes().len(), 2);
}

#[tokio::test]
async fn counts_forget_and_prune() {
    let ledger = Ledger::new(docs().await, Arc::new(crate::observe::NoopSink));
    for (id, disposition) in [
        ("1", Disposition::Delivered),
        ("2", Disposition::Delivered),
        ("3", Disposition::Dead),
    ] {
        let outcome = Outcome::new(EventRef::new("s", id), disposition, Reason::Turn, 0);
        ledger.record(outcome, 100).await.unwrap();
    }
    let counts = ledger.counts().await.unwrap();
    assert_eq!((counts.delivered, counts.dead, counts.total()), (2, 1, 3));
    assert!(ledger.forget(&EventRef::new("s", "3")).await.unwrap());
    assert_eq!(ledger.counts().await.unwrap().dead, 0);
    assert_eq!(ledger.prune(100).await.unwrap(), 2);
    assert_eq!(ledger.counts().await.unwrap().total(), 0);
}

#[test]
fn event_ids_escape_their_parts() {
    assert_ne!(event_id("a/b", "c").unwrap(), event_id("a", "b/c").unwrap());
}
