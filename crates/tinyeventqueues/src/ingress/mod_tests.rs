//! Unit tests for the ingress stage.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use serde_json::json;
use tinyeventqueues_bus::{AckPolicy, Disposition, Reason, SourcePolicy};

use super::*;
use crate::observe::CollectingSink;
use crate::ports::{ErrorKind, NewMessage};
use crate::test_support::{Fixture, event};

#[test]
fn envelopes_with_empty_or_oversized_keys_are_invalid() {
    assert_eq!(invalid_field(&event("s", "1", "c")), None);
    assert_eq!(invalid_field(&event("s", "", "c")), Some("event_id"));
    assert_eq!(invalid_field(&event("", "1", "c")), Some("source"));
    assert_eq!(
        invalid_field(&event("s", "1", "")),
        Some("conversation_key")
    );
    let long = "x".repeat(MAX_KEY_LEN + 1);
    assert_eq!(invalid_field(&event("s", &long, "c")), Some("event_id"));
    let mut anonymous = event("s", "1", "c");
    anonymous.sender.id.clear();
    assert_eq!(invalid_field(&anonymous), Some("sender.id"));
    assert_eq!(
        invalid_field(&event("s", "1", "c").thread("")),
        Some("thread_key")
    );
    assert_eq!(invalid_field(&event("s", "1", "c").thread("t")), None);
}

#[tokio::test]
async fn accepted_events_are_persisted_with_their_raw_payload() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let raw = event("channel:telegram", "1", "c").raw(json!({"update_id": 9}));
    let response = pipeline.ingest(vec![raw.clone()]).await.unwrap();
    assert_eq!(response.results[0].disposition, Disposition::Accepted);
    assert!(response.results[0].ack_platform);

    let queue = pipeline.queues().ingress;
    let stored = fixture
        .queues
        .receive(&queue.name, 10, 1_000, "test")
        .await
        .unwrap();
    let envelope: IngressEnvelope = serde_json::from_value(stored[0].payload.clone()).unwrap();
    assert_eq!(envelope.event, raw);
    assert_eq!(envelope.ingested_at_ms, fixture.clock.now());
}

#[tokio::test]
async fn invalid_events_are_dropped_visibly_and_the_rest_accepted() {
    let fixture = Fixture::new();
    let sink = Arc::new(CollectingSink::new());
    let pipeline = fixture.builder().sink(sink.clone()).build().await.unwrap();
    let response = pipeline
        .ingest(vec![event("s", "", "c"), event("s", "2", "c")])
        .await
        .unwrap();
    assert_eq!(response.results[0].disposition, Disposition::Dropped);
    assert_eq!(response.results[0].reason, Some(Reason::InvalidEnvelope));
    assert!(response.results[0].ack_platform);
    assert_eq!(response.results[1].disposition, Disposition::Accepted);
    let seen: Vec<_> = sink.outcomes().iter().map(|o| o.disposition).collect();
    assert_eq!(seen, [Disposition::Dropped, Disposition::Accepted]);
}

#[tokio::test]
async fn the_ack_policy_of_the_source_decides_platform_acknowledgement() {
    let fixture = Fixture::new();
    let mut config = crate::config::shipped_pipeline_config().unwrap();
    let mut slow = SourcePolicy::new("composio:*");
    slow.ack = AckPolicy::AfterTurn;
    config.sources.insert(0, slow);
    let pipeline = fixture.builder().config(config).build().await.unwrap();
    let response = pipeline
        .ingest(vec![
            event("composio:twitter", "1", "c"),
            event("web:chat", "1", "c"),
        ])
        .await
        .unwrap();
    assert!(!response.results[0].ack_platform);
    assert!(response.results[1].ack_platform);
}

#[tokio::test]
async fn a_repeat_within_one_request_is_a_duplicate_on_both_backends() {
    for dedupe in [true, false] {
        let fixture = Fixture::with_native(dedupe);
        let pipeline = fixture.pipeline().await;
        let response = pipeline
            .ingest(vec![event("s", "1", "c"), event("s", "1", "c")])
            .await
            .unwrap();
        let dispositions: Vec<_> = response.results.iter().map(|r| r.disposition).collect();
        assert_eq!(
            dispositions,
            [Disposition::Accepted, Disposition::Duplicate],
            "native={dedupe}"
        );
        let stats = fixture
            .queues
            .stats(&pipeline.queues().ingress.name)
            .await
            .unwrap();
        assert_eq!(stats.ready, 1, "native={dedupe}");
    }
}

#[tokio::test]
async fn without_native_dedupe_a_completed_event_is_recognized_by_its_outcome() {
    let fixture = Fixture::with_native(false);
    let pipeline = fixture.pipeline().await;
    let ttl = pipeline.config().ingress.tombstone_ttl_ms;
    let done = event("s", "1", "c");
    let ledger = crate::store::Ledger::new(Arc::clone(&fixture.docs), Arc::new(crate::NoopSink));
    ledger
        .record(
            tinyeventqueues_bus::Outcome::new(
                done.reference(),
                Disposition::Delivered,
                Reason::Turn,
                0,
            ),
            ttl,
        )
        .await
        .unwrap();
    let response = pipeline.ingest(vec![done]).await.unwrap();
    assert_eq!(response.results[0].disposition, Disposition::Duplicate);
}

#[tokio::test]
async fn an_empty_request_stores_nothing() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    assert_eq!(pipeline.ingest(Vec::new()).await.unwrap().results.len(), 0);
}

#[tokio::test]
async fn the_broker_double_passes_every_call_through() {
    use crate::ports::Retry;

    let fixture = Fixture::with_native(false);
    let pipeline = fixture.pipeline().await;
    let queue = pipeline.queues().ingress.name;
    pipeline.ingest(vec![event("s", "1", "c")]).await.unwrap();
    let got = fixture.queues.receive(&queue, 1, 100, "t").await.unwrap();
    assert!(
        fixture
            .queues
            .extend(&queue, &got[0].receipt, 100)
            .await
            .unwrap()
    );
    assert!(
        fixture
            .queues
            .nack(&queue, &got[0].receipt, Retry::DeadLetter("x".into()))
            .await
            .unwrap()
    );
    let dead = fixture.queues.dead_letters(&queue, 10).await.unwrap();
    assert!(fixture.queues.redrive(&queue, &dead[0].id).await.unwrap());
    assert_eq!(fixture.queues.purge(&queue).await.unwrap(), 1);
}

#[tokio::test]
async fn the_broker_double_refuses_groups_and_dedupe_keys() {
    let fixture = Fixture::with_native(false);
    let pipeline = fixture.pipeline().await;
    let queue = pipeline.queues().ingress.name;
    let features = fixture.queues.features();
    assert!(!features.dedupe && !features.groups);
    for message in [
        NewMessage::new(json!({})).group("g"),
        NewMessage::new(json!({})).dedupe_key("k"),
    ] {
        let error = fixture
            .queues
            .enqueue(&queue, &[message])
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}

#[tokio::test]
async fn the_broker_double_passes_the_queue_conformance_suite() {
    let fixture = Fixture::with_native(false);
    tinystoragedrivers_testkit::conformance::queue::run(fixture.queues.as_ref()).await;
}
