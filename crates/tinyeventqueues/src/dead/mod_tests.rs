//! Unit tests for dead-letter parking, listing and redrive.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use crate::ports::NewMessage;
use serde_json::json;
use tinyeventqueues_bus::{Disposition, EventRef};

use super::*;
use crate::store::Ledger;
use crate::test_support::{Fixture, event};

fn parts(fixture: &Fixture) -> (DeadLetters, Ledger) {
    let ledger = Ledger::new(Arc::clone(&fixture.docs), Arc::new(crate::NoopSink));
    let dead = DeadLetters::new(
        Arc::clone(&fixture.queues),
        Arc::clone(&fixture.docs),
        ledger.clone(),
        "test".to_owned(),
    );
    (dead, ledger)
}

fn parked(origin: &str, id: &str) -> Parked {
    Parked {
        origin: origin.to_owned(),
        reason: "host rejected".to_owned(),
        attempt: 3,
        group: Some("c".to_owned()),
        turn_id: Some("turn-1".to_owned()),
        events: vec![EventRef::new("s", id)],
        payload: json!({"events": [{"source": "s", "event_id": id}]}),
    }
}

#[test]
fn payload_events_reads_envelopes_and_turns() {
    let envelope = json!({"event": {"source": "s", "event_id": "1"}});
    assert_eq!(payload_events(&envelope), [EventRef::new("s", "1")]);
    let turn = json!({"events": [{"source": "s", "event_id": "1"}, {"source": "s"}]});
    assert_eq!(payload_events(&turn), [EventRef::new("s", "1")]);
    assert_eq!(payload_events(&json!({"other": 1})).len(), 0);
}

#[tokio::test]
async fn parked_work_settles_dead_lists_and_redrives_to_its_origin() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let resolved = pipeline.queues();
    let (dead, ledger) = parts(&fixture);
    let lane = resolved
        .lane(tinyeventqueues_bus::Lane::Interactive)
        .name
        .clone();

    let message = NewMessage::new(serde_json::to_value(parked(&lane, "1")).unwrap());
    fixture
        .queues
        .enqueue(&resolved.dead.name, &[message])
        .await
        .unwrap();
    assert_eq!(dead.settle(&resolved.dead).await.unwrap(), 1);

    let items = dead.list(&resolved, 10).await.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].origin, lane);
    assert_eq!(items[0].reason, "host rejected");
    assert_eq!(items[0].attempt, 3);
    assert_eq!(items[0].events, [EventRef::new("s", "1")]);

    let outcome = tinyeventqueues_bus::Outcome::new(
        EventRef::new("s", "1"),
        Disposition::Dead,
        tinyeventqueues_bus::Reason::HostRejected,
        0,
    );
    ledger.record(outcome, 1_000).await.unwrap();

    assert!(dead.redrive(&resolved, &items[0].id).await.unwrap());
    assert_eq!(dead.list(&resolved, 10).await.unwrap().len(), 0);
    assert_eq!(
        ledger.terminal(&EventRef::new("s", "1")).await.unwrap(),
        None
    );
    let back = fixture.queues.receive(&lane, 10, 1_000, "t").await.unwrap();
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].group.as_deref(), Some("c"));
    assert_eq!(
        fixture
            .queues
            .stats(&resolved.dead.name)
            .await
            .unwrap()
            .dead,
        0
    );
    assert!(!dead.redrive(&resolved, &items[0].id).await.unwrap());
}

#[tokio::test]
async fn work_a_queue_dead_lettered_itself_is_swept_listed_and_redriven() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let resolved = pipeline.queues();
    let (dead, ledger) = parts(&fixture);
    let ingress = resolved.ingress.clone();
    pipeline.ingest(vec![event("s", "1", "c")]).await.unwrap();

    // A consumer that crashes on the message every time.
    for _ in 0..ingress.max_attempts {
        let got = fixture
            .queues
            .receive(&ingress.name, 1, 10, "crashy")
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        fixture.clock.advance(11);
    }
    assert_eq!(
        fixture
            .queues
            .receive(&ingress.name, 1, 10, "crashy")
            .await
            .unwrap()
            .len(),
        0
    );

    assert_eq!(dead.sweep(&resolved, 0, 1_000).await.unwrap(), 1);
    assert_eq!(dead.sweep(&resolved, 0, 1_000).await.unwrap(), 0);
    let reference = EventRef::new("s", "1");
    assert_eq!(
        ledger
            .terminal(&reference)
            .await
            .unwrap()
            .unwrap()
            .disposition,
        Disposition::Dead
    );

    let items = dead.list(&resolved, 10).await.unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].origin, ingress.name);
    assert!(items[0].id.starts_with(&format!("{}/", ingress.name)));
    assert!(dead.redrive(&resolved, &items[0].id).await.unwrap());
    assert_eq!(ledger.terminal(&reference).await.unwrap(), None);
    assert_eq!(fixture.queues.stats(&ingress.name).await.unwrap().ready, 1);
    assert!(!dead.redrive(&resolved, &items[0].id).await.unwrap());
}

#[tokio::test]
async fn listing_respects_the_limit() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let resolved = pipeline.queues();
    let (dead, _) = parts(&fixture);
    let lane = resolved
        .lane(tinyeventqueues_bus::Lane::Background)
        .name
        .clone();
    let messages: Vec<_> = (0..3)
        .map(|i| NewMessage::new(serde_json::to_value(parked(&lane, &i.to_string())).unwrap()))
        .collect();
    fixture
        .queues
        .enqueue(&resolved.dead.name, &messages)
        .await
        .unwrap();
    dead.settle(&resolved.dead).await.unwrap();
    assert_eq!(dead.list(&resolved, 2).await.unwrap().len(), 2);
    assert_eq!(dead.list(&resolved, 5).await.unwrap().len(), 3);
}

#[tokio::test]
async fn unreadable_parked_messages_are_still_listed() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let resolved = pipeline.queues();
    let (dead, _) = parts(&fixture);
    fixture
        .queues
        .enqueue(
            &resolved.dead.name,
            &[NewMessage::new(json!({"junk": true}))],
        )
        .await
        .unwrap();
    dead.settle(&resolved.dead).await.unwrap();
    let items = dead.list(&resolved, 10).await.unwrap();
    assert_eq!(items[0].reason, "unreadable parked message");
    assert_eq!(items[0].origin, resolved.dead.name);
    assert_eq!(items[0].events.len(), 0);
    let error = dead.redrive(&resolved, &items[0].id).await.unwrap_err();
    assert!(matches!(error, crate::Error::Encoding(_)));
}

#[tokio::test]
async fn malformed_ids_and_unknown_origins_are_errors() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let resolved = pipeline.queues();
    let (dead, _) = parts(&fixture);
    for id in ["no-slash", "teq.unknown/1"] {
        assert!(matches!(
            dead.redrive(&resolved, id).await.unwrap_err(),
            crate::Error::UnknownDeadItem(_)
        ));
    }
    let message = NewMessage::new(serde_json::to_value(parked("teq.elsewhere", "1")).unwrap());
    fixture
        .queues
        .enqueue(&resolved.dead.name, &[message])
        .await
        .unwrap();
    dead.settle(&resolved.dead).await.unwrap();
    let items = dead.list(&resolved, 10).await.unwrap();
    assert!(matches!(
        dead.redrive(&resolved, &items[0].id).await.unwrap_err(),
        crate::Error::UnknownDeadItem(_)
    ));
}

#[tokio::test]
async fn a_redrive_on_a_backend_without_groups_drops_the_group() {
    let fixture = Fixture::with_native(false);
    let pipeline = fixture.pipeline().await;
    let resolved = pipeline.queues();
    let (dead, _) = parts(&fixture);
    let lane = resolved
        .lane(tinyeventqueues_bus::Lane::Interactive)
        .name
        .clone();
    let message = NewMessage::new(serde_json::to_value(parked(&lane, "1")).unwrap());
    fixture
        .queues
        .enqueue(&resolved.dead.name, &[message])
        .await
        .unwrap();
    dead.settle(&resolved.dead).await.unwrap();
    let items = dead.list(&resolved, 10).await.unwrap();
    assert!(dead.redrive(&resolved, &items[0].id).await.unwrap());
    let back = fixture.queues.receive(&lane, 10, 1_000, "t").await.unwrap();
    assert_eq!(back[0].group, None);
}
