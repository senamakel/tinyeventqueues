//! Unit tests for pending turns.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use tinyeventqueues_bus::{EventRef, Lane, Reason};

use super::*;
use crate::test_support::{Fixture, event};

#[test]
fn a_turn_takes_its_keys_and_id_from_its_first_event() {
    let first = event("s", "1", "c").thread("t");
    let turn = PendingTurn::new(
        Lane::Interactive,
        Reason::Turn,
        vec![first, event("s", "2", "c")],
        vec![EventRef::new("s", "3")],
        5,
    )
    .unwrap();
    assert_eq!(turn.conversation_key, "c");
    assert_eq!(turn.thread_key.as_deref(), Some("t"));
    assert_eq!(turn.absorbed, [EventRef::new("s", "3")]);
    let same = PendingTurn::new(
        Lane::Interactive,
        Reason::Turn,
        vec![event("s", "1", "c").thread("t")],
        vec![],
        9,
    )
    .unwrap();
    assert_eq!(turn.turn_id, same.turn_id);
    let other_lane = PendingTurn::new(
        Lane::Digest,
        Reason::Turn,
        vec![event("s", "1", "c").thread("t")],
        vec![],
        9,
    )
    .unwrap();
    assert_ne!(turn.turn_id, other_lane.turn_id);
}

#[tokio::test]
async fn enqueueing_a_turn_twice_stores_it_once_in_its_conversation_group() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let queue = pipeline.queues().lane(Lane::Actionable).clone();
    let lanes = Lanes::new(
        Arc::clone(&fixture.queues),
        crate::store::Tombstones::new(Arc::clone(&fixture.docs)),
        Conversations::new(&fixture.docs),
    );
    let turn = PendingTurn::new(
        Lane::Actionable,
        Reason::Turn,
        vec![event("s", "1", "c")],
        vec![],
        0,
    )
    .unwrap();
    assert!(lanes.enqueue(&queue, &turn, 0, 1_000).await.unwrap());
    assert!(!lanes.enqueue(&queue, &turn, 0, 1_000).await.unwrap());
    let got = fixture
        .queues
        .receive(&queue.name, 10, 1_000, "t")
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].group.as_deref(), Some("c"));
    let decoded: PendingTurn = serde_json::from_value(got[0].payload.clone()).unwrap();
    assert_eq!(decoded, turn);
}

#[tokio::test]
async fn an_empty_turn_has_no_group() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let queue = pipeline.queues().lane(Lane::Digest).clone();
    let lanes = Lanes::new(
        Arc::clone(&fixture.queues),
        crate::store::Tombstones::new(Arc::clone(&fixture.docs)),
        Conversations::new(&fixture.docs),
    );
    let turn = PendingTurn::new(Lane::Digest, Reason::LaneOverflow, vec![], vec![], 0).unwrap();
    assert!(lanes.enqueue(&queue, &turn, 0, 1_000).await.unwrap());
    let got = fixture
        .queues
        .receive(&queue.name, 10, 1_000, "t")
        .await
        .unwrap();
    assert_eq!(got[0].group, None);
}

#[tokio::test]
async fn without_dedupe_or_groups_a_turn_is_stored_once_and_ungrouped() {
    let fixture = Fixture::with_native(false);
    let pipeline = fixture.pipeline().await;
    let queue = pipeline.queues().lane(Lane::Actionable).clone();
    let lanes = Lanes::new(
        Arc::clone(&fixture.queues),
        crate::store::Tombstones::new(Arc::clone(&fixture.docs)),
        Conversations::new(&fixture.docs),
    );
    let turn = PendingTurn::new(
        Lane::Actionable,
        Reason::Turn,
        vec![event("s", "1", "c")],
        vec![],
        0,
    )
    .unwrap();
    assert!(lanes.enqueue(&queue, &turn, 0, 1_000).await.unwrap());
    assert!(!lanes.enqueue(&queue, &turn, 500, 1_000).await.unwrap());
    let got = fixture
        .queues
        .receive(&queue.name, 10, 1_000, "t")
        .await
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].group, None);
}

#[tokio::test]
async fn conversation_order_without_groups() {
    let fixture = Fixture::new();
    crate::store::ensure(fixture.docs.as_ref()).await.unwrap();
    let conversations = Conversations::new(&fixture.docs);
    assert_eq!(conversations.assign("c").await.unwrap(), 1);
    assert_eq!(conversations.assign("c").await.unwrap(), 2);
    assert_eq!(
        conversations.check("c", 2, 100, 0).await.unwrap(),
        Turnstile::Wait
    );
    assert_eq!(
        conversations.check("c", 1, 100, 0).await.unwrap(),
        Turnstile::Go
    );
    // Seen and backing off: a later turn waits past the gap timeout.
    assert_eq!(
        conversations.check("c", 1, 100, 10).await.unwrap(),
        Turnstile::Go
    );
    assert_eq!(
        conversations.check("c", 2, 100, 500).await.unwrap(),
        Turnstile::Wait
    );
    conversations.finish("c", 1).await.unwrap();
    conversations.finish("c", 1).await.unwrap();
    assert_eq!(
        conversations.check("c", 1, 100, 600).await.unwrap(),
        Turnstile::Done
    );
    assert_eq!(
        conversations.check("c", 2, 100, 600).await.unwrap(),
        Turnstile::Go
    );
    // A conversation never seen starts at the turn that arrives.
    assert_eq!(
        conversations.check("new", 7, 100, 0).await.unwrap(),
        Turnstile::Go
    );
    conversations.finish("none", 1).await.unwrap();
}
