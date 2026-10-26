//! Tests of dispatch: delivery, retries with backoff, dead letters and
//! redrive, ack policies, and conversation order without groups.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::ports::NewMessage;
use serde_json::json;
use tinyeventqueues_bus::{
    AckPolicy, DeliverStatus, DispatchConfig, Disposition, EventRef, InboundEvent, Lane, Reason,
    SourcePolicy,
};

use super::backoff;
use crate::config::shipped_pipeline_config;
use crate::test_support::{Fixture, ScriptedHost, event};
use crate::{CollectingSink, Error, Pipeline};

fn mention(id: &str, conversation: &str) -> InboundEvent {
    event("channel:slack", id, conversation).addressed(true)
}

async fn run(fixture: &Fixture, pipeline: &Pipeline, events: Vec<InboundEvent>) {
    pipeline.ingest(events).await.unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();
}

#[test]
fn backoff_doubles_up_to_the_cap() {
    let config = DispatchConfig::default();
    assert_eq!(backoff(&config, 1), 1_000);
    assert_eq!(backoff(&config, 2), 2_000);
    assert_eq!(backoff(&config, 4), 8_000);
    assert_eq!(backoff(&config, 30), 300_000);
    assert_eq!(backoff(&config, u32::MAX), 300_000);
}

#[tokio::test]
async fn accepted_turns_end_delivered_and_leave_the_lane() {
    let fixture = Fixture::new();
    let host = ScriptedHost::accepting();
    let pipeline = fixture.builder().host(host.clone()).build().await.unwrap();
    run(
        &fixture,
        &pipeline,
        vec![mention("1", "c"), mention("2", "c")],
    )
    .await;
    let turns = host.turns();
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].lane, Lane::Interactive);
    assert_eq!(turns[0].attempt, 1);
    assert_eq!(turns[0].events.len(), 2);
    let stats = pipeline.stats().await.unwrap();
    assert_eq!(stats.outcomes.delivered, 2);
    assert!(
        stats
            .queues
            .iter()
            .all(|q| q.ready + q.leased + q.delayed == 0)
    );
}

#[tokio::test]
async fn without_a_host_turns_wait_in_their_lanes() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    run(&fixture, &pipeline, vec![mention("1", "c")]).await;
    assert_eq!(pipeline.drain().await.unwrap().dispatched, 0);
    let interactive = pipeline.queues().lane(Lane::Interactive).name.clone();
    let stats = fixture.queues.stats(&interactive).await.unwrap();
    assert_eq!(stats.ready, 1);
}

#[tokio::test]
async fn a_digest_turn_ends_its_events_as_digest() {
    let fixture = Fixture::new();
    let host = ScriptedHost::accepting();
    let mut config = shipped_pipeline_config().unwrap();
    config.gate.rate_limits.per_sender = Some(tinyeventqueues_bus::RateLimit::new(1, 1));
    let pipeline = fixture
        .builder()
        .config(config)
        .host(host.clone())
        .build()
        .await
        .unwrap();
    run(
        &fixture,
        &pipeline,
        vec![mention("1", "c"), mention("2", "c")],
    )
    .await;
    fixture.clock.advance(pipeline.config().digest.max_wait_ms);
    pipeline.drain().await.unwrap();
    let outcomes = pipeline.stats().await.unwrap().outcomes;
    assert_eq!((outcomes.delivered, outcomes.digest), (1, 1));
    assert!(
        host.turns()
            .iter()
            .any(tinyeventqueues_bus::Turn::is_digest)
    );
}

#[tokio::test]
async fn retries_back_off_and_then_deliver() {
    let fixture = Fixture::new();
    let sink = Arc::new(CollectingSink::new());
    let calls = Arc::new(AtomicU32::new(0));
    let seen = Arc::clone(&calls);
    let host = ScriptedHost::new(move |_| match seen.fetch_add(1, Ordering::SeqCst) {
        0 => Ok(DeliverStatus::Retry { after_ms: None }),
        1 => Err(Error::Host {
            method: "Deliver",
            message: "down".into(),
        }),
        2 => Ok(DeliverStatus::Retry {
            after_ms: Some(50_000),
        }),
        _ => Ok(DeliverStatus::Accepted),
    });
    let pipeline = fixture
        .builder()
        .host(host.clone())
        .sink(sink.clone())
        .build()
        .await
        .unwrap();
    run(&fixture, &pipeline, vec![mention("1", "c")]).await;
    // First backoff: one second; second: two; then the host's own 50 s.
    for (wait, calls_after) in [(999, 1), (1, 2), (2_000, 3), (49_999, 3), (1, 4)] {
        fixture.clock.advance(wait);
        pipeline.drain().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), calls_after);
    }
    let attempts: Vec<_> = host.turns().iter().map(|t| t.attempt).collect();
    assert_eq!(attempts, [1, 2, 3, 4]);
    assert_eq!(pipeline.stats().await.unwrap().outcomes.delivered, 1);
    let retried = sink
        .outcomes()
        .iter()
        .filter(|o| o.reason == Reason::RetryRequested)
        .count();
    assert_eq!(retried, 3);
}

#[tokio::test]
async fn a_turn_out_of_attempts_is_dead_lettered_listed_and_redriven() {
    let fixture = Fixture::new();
    let healthy = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&healthy);
    let host = ScriptedHost::new(move |_| {
        Ok(if flag.load(Ordering::SeqCst) {
            DeliverStatus::Accepted
        } else {
            DeliverStatus::Retry { after_ms: Some(1) }
        })
    });
    let pipeline = fixture.builder().host(host.clone()).build().await.unwrap();
    run(&fixture, &pipeline, vec![mention("1", "c")]).await;
    for _ in 0..10 {
        fixture.clock.advance(10);
        pipeline.drain().await.unwrap();
    }
    let max = pipeline.queues().lane(Lane::Interactive).max_attempts;
    assert_eq!(host.turns().len(), usize::try_from(max).unwrap());
    let stats = pipeline.stats().await.unwrap();
    assert_eq!(stats.outcomes.dead, 1);
    let dead = pipeline.list_dead(10).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].reason, "max attempts exceeded");
    assert_eq!(dead[0].events, [EventRef::new("channel:slack", "1")]);

    healthy.store(true, Ordering::SeqCst);
    assert!(pipeline.redrive(&dead[0].id).await.unwrap());
    pipeline.drain().await.unwrap();
    let outcomes = pipeline.stats().await.unwrap().outcomes;
    assert_eq!((outcomes.dead, outcomes.delivered), (0, 1));
}

#[tokio::test]
async fn a_rejected_turn_is_dead_lettered_at_once() {
    let fixture = Fixture::new();
    let host = ScriptedHost::new(|_| {
        Ok(DeliverStatus::Rejected {
            detail: "not for me".into(),
        })
    });
    let pipeline = fixture.builder().host(host.clone()).build().await.unwrap();
    run(&fixture, &pipeline, vec![mention("1", "c")]).await;
    assert_eq!(host.turns().len(), 1);
    let dead = pipeline.list_dead(10).await.unwrap();
    assert_eq!(dead[0].reason, "not for me");
    assert_eq!(dead[0].attempt, 1);
    assert_eq!(pipeline.stats().await.unwrap().outcomes.dead, 1);
}

#[tokio::test]
async fn deferred_acknowledgements_ride_with_the_turn() {
    let fixture = Fixture::new();
    let host = ScriptedHost::accepting();
    let mut config = shipped_pipeline_config().unwrap();
    let mut slow = SourcePolicy::new("channel:slack");
    slow.ack = AckPolicy::AfterTurn;
    config.sources.insert(0, slow);
    let pipeline = fixture
        .builder()
        .config(config)
        .host(host.clone())
        .build()
        .await
        .unwrap();
    let response = pipeline.ingest(vec![mention("1", "c")]).await.unwrap();
    assert!(!response.results[0].ack_platform);
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();
    assert_eq!(
        host.turns()[0].platform_acks,
        [EventRef::new("channel:slack", "1")]
    );
}

#[tokio::test]
async fn an_unreadable_turn_is_dead_lettered() {
    let fixture = Fixture::new();
    let host = ScriptedHost::accepting();
    let pipeline = fixture.builder().host(host.clone()).build().await.unwrap();
    let lane = pipeline.queues().lane(Lane::Actionable).name.clone();
    fixture
        .queues
        .enqueue(&lane, &[NewMessage::new(json!({"junk": 1}))])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    assert_eq!(host.turns().len(), 0);
    let dead = pipeline.list_dead(10).await.unwrap();
    assert_eq!(dead[0].reason, "unreadable pending turn");
}

#[tokio::test]
async fn without_groups_a_conversation_keeps_one_turn_in_flight_in_order() {
    let fixture = Fixture::with_native(false);
    let calls = Arc::new(AtomicU32::new(0));
    let seen = Arc::clone(&calls);
    // The first delivery fails once; the second turn must wait for it.
    let host = ScriptedHost::new(move |_| {
        Ok(if seen.fetch_add(1, Ordering::SeqCst) == 0 {
            DeliverStatus::Retry {
                after_ms: Some(5_000),
            }
        } else {
            DeliverStatus::Accepted
        })
    });
    let pipeline = fixture.builder().host(host.clone()).build().await.unwrap();
    pipeline.ingest(vec![mention("1", "c")]).await.unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(2_000);
    pipeline.drain().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    pipeline
        .ingest(vec![mention("2", "c").text("/now")])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    // Turn 1 was retried; turn 2 is due but must not overtake it.
    for _ in 0..8 {
        fixture.clock.advance(1_000);
        pipeline.drain().await.unwrap();
    }
    let order: Vec<_> = host
        .turns()
        .iter()
        .map(|t| t.events[0].event_id.clone())
        .collect();
    assert_eq!(order, ["1", "1", "2"]);
    assert_eq!(pipeline.stats().await.unwrap().outcomes.delivered, 2);
}

#[tokio::test]
async fn without_groups_a_lost_turn_is_skipped_after_the_gap_timeout() {
    let fixture = Fixture::with_native(false);
    let host = ScriptedHost::accepting();
    let pipeline = fixture.builder().host(host.clone()).build().await.unwrap();
    // A turn number handed out but never enqueued (a crash in between).
    let conversations = crate::turn::Conversations::new(&fixture.docs);
    let lost = crate::turn::PendingTurn::new(
        Lane::Interactive,
        Reason::Turn,
        vec![mention("0", "c")],
        vec![],
        0,
    )
    .unwrap();
    let lanes = crate::turn::Lanes::new(
        Arc::clone(&fixture.queues),
        crate::store::Tombstones::new(Arc::clone(&fixture.docs)),
        conversations.clone(),
    );
    let mut assigned = lost.clone();
    lanes
        .enqueue(pipeline.queues().lane(Lane::Interactive), &assigned, 0, 1)
        .await
        .unwrap();
    let ghost = fixture
        .queues
        .receive(
            &pipeline.queues().lane(Lane::Interactive).name,
            1,
            1_000,
            "x",
        )
        .await
        .unwrap();
    fixture
        .queues
        .ack(
            &pipeline.queues().lane(Lane::Interactive).name,
            &ghost[0].receipt,
        )
        .await
        .unwrap();
    assigned.seq = Some(1);
    run(&fixture, &pipeline, vec![mention("1", "c")]).await;
    assert_eq!(host.turns().len(), 0);
    let gap = pipeline.config().dispatch.gap_timeout_ms;
    for _ in 0..3 {
        fixture.clock.advance(gap / 2);
        pipeline.drain().await.unwrap();
    }
    assert_eq!(host.turns().len(), 1);
    // A redelivery of a finished turn whose events all ended is
    // acknowledged, not delivered again.
    let ledger = crate::store::Ledger::new(Arc::clone(&fixture.docs), Arc::new(crate::NoopSink));
    let ended = tinyeventqueues_bus::Outcome::new(
        EventRef::new("channel:slack", "0"),
        Disposition::Delivered,
        Reason::Turn,
        0,
    );
    ledger.record(ended, 1_000_000).await.unwrap();
    let queue = pipeline.queues().lane(Lane::Interactive).name.clone();
    fixture
        .queues
        .enqueue(
            &queue,
            &[NewMessage::new(serde_json::to_value(&assigned).unwrap())],
        )
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    assert_eq!(host.turns().len(), 1);
}
