//! End-to-end tests of one pipeline pass: ingress through the gate and the
//! debounce buffer into the lane queues.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use crate::ports::NewMessage;
use serde_json::json;
use tinyeventqueues_bus::{
    Disposition, EventKind, EventRef, InboundEvent, Lane, MediaRef, PipelineConfig, RateLimit,
    Reason, SalienceClass, Sender, SourcePolicy,
};

use crate::CollectingSink;
use crate::config::shipped_pipeline_config;
use crate::test_support::{Fixture, event, take_turns};

fn ids(events: &[InboundEvent]) -> Vec<&str> {
    events.iter().map(|e| e.event_id.as_str()).collect()
}

fn mention(id: &str, conversation: &str) -> InboundEvent {
    event("channel:slack", id, conversation).addressed(true)
}

#[tokio::test]
async fn unaddressed_chatter_becomes_context_for_the_next_turn() {
    let fixture = Fixture::new();
    let sink = Arc::new(CollectingSink::new());
    let pipeline = fixture.builder().sink(sink.clone()).build().await.unwrap();
    pipeline
        .ingest(vec![
            event("channel:slack", "1", "general"),
            event("channel:slack", "2", "general"),
            mention("3", "general"),
        ])
        .await
        .unwrap();
    let report = pipeline.drain().await.unwrap();
    assert_eq!(report.ingested, 3);
    assert_eq!(report.flushed, 0);

    fixture.clock.advance(1_500);
    assert_eq!(pipeline.drain().await.unwrap().flushed, 1);
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    assert_eq!(turns.len(), 1);
    assert_eq!(ids(&turns[0].events), ["3"]);
    assert_eq!(ids(&turns[0].context), ["1", "2"]);

    let context: Vec<_> = sink
        .outcomes()
        .into_iter()
        .filter(|o| o.disposition == Disposition::Context)
        .map(|o| (o.event.event_id, o.reason))
        .collect();
    assert_eq!(
        context,
        [
            ("1".into(), Reason::Unaddressed),
            ("2".into(), Reason::Unaddressed)
        ]
    );
    assert_eq!(pipeline.stats().await.unwrap().outcomes.context, 2);

    // The context was handed over: the next turn does not repeat it.
    pipeline
        .ingest(vec![mention("4", "general")])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(1_500);
    pipeline.drain().await.unwrap();
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    assert_eq!(turns[0].context.len(), 0);
}

#[tokio::test]
async fn bursts_coalesce_per_thread_without_merging_threads_and_keep_order() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let events = vec![
        mention("1", "c").thread("a"),
        mention("2", "c").thread("b"),
        mention("3", "c").thread("a"),
        mention("4", "c").thread("b"),
        mention("5", "c").thread("a"),
    ];
    pipeline.ingest(events).await.unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    assert_eq!(pipeline.drain().await.unwrap().flushed, 2);
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    let mut by_thread: Vec<_> = turns
        .iter()
        .map(|t| (t.thread_key.clone().unwrap(), ids(&t.events).join(",")))
        .collect();
    by_thread.sort();
    assert_eq!(
        by_thread,
        [("a".into(), "1,3,5".into()), ("b".into(), "2,4".into())]
    );
}

#[tokio::test]
async fn media_and_commands_flush_in_the_same_pass() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    pipeline
        .ingest(vec![
            mention("1", "photos").with_media(MediaRef::new("image", "blob://1")),
            mention("2", "ops").text("/status"),
            mention("3", "slow"),
        ])
        .await
        .unwrap();
    assert_eq!(pipeline.drain().await.unwrap().flushed, 2);
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    let mut conversations: Vec<_> = turns.iter().map(|t| t.conversation_key.as_str()).collect();
    conversations.sort_unstable();
    assert_eq!(conversations, ["ops", "photos"]);
}

#[tokio::test]
async fn a_looping_bot_is_dropped_with_a_visible_reason() {
    let fixture = Fixture::new();
    let mut config = shipped_pipeline_config().unwrap();
    config.gate.bot_loop.max_events = 2;
    let pipeline = fixture.builder().config(config).build().await.unwrap();
    let bot = |id: &str| {
        let mut event = mention(id, "c");
        event.sender = Sender::new("other-bot").as_bot();
        event
    };
    pipeline
        .ingest(vec![bot("1"), bot("2"), bot("3"), bot("4")])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    let outcomes = pipeline.stats().await.unwrap().outcomes;
    assert_eq!(outcomes.dropped, 2);
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    assert_eq!(ids(&turns[0].events), ["1", "2"]);
}

#[tokio::test]
async fn self_echoes_and_denied_senders_never_reach_a_lane() {
    let fixture = Fixture::new();
    let mut config = shipped_pipeline_config().unwrap();
    config.gate.deny_senders = vec!["spammer".into()];
    let pipeline = fixture.builder().config(config).build().await.unwrap();
    let mut echo = mention("1", "c");
    echo.sender = Sender::new("agent").as_self();
    let mut spam = mention("2", "c");
    spam.sender = Sender::new("spammer");
    pipeline.ingest(vec![echo, spam]).await.unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    assert_eq!(pipeline.drain().await.unwrap().flushed, 0);
    assert_eq!(pipeline.stats().await.unwrap().outcomes.dropped, 2);
}

#[tokio::test]
async fn rate_limited_events_spill_to_the_digest_lane() {
    let fixture = Fixture::new();
    let sink = Arc::new(CollectingSink::new());
    let mut config = shipped_pipeline_config().unwrap();
    config.gate.rate_limits.per_sender = Some(RateLimit::new(1, 1));
    let pipeline = fixture
        .builder()
        .config(config)
        .sink(sink.clone())
        .build()
        .await
        .unwrap();
    pipeline
        .ingest(vec![mention("1", "c"), mention("2", "c")])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    let early = take_turns(&fixture, &pipeline, Lane::Digest).await;
    assert_eq!(early.len(), 0, "{early:?}");
    fixture.clock.advance(pipeline.config().digest.max_wait_ms);
    pipeline.drain().await.unwrap();
    let digest = take_turns(&fixture, &pipeline, Lane::Digest).await;
    assert_eq!(digest.len(), 1);
    assert_eq!(ids(&digest[0].events), ["2"]);
    assert!(
        sink.outcomes()
            .iter()
            .any(|o| o.disposition == Disposition::Deferred && o.reason == Reason::RateLimited)
    );
}

#[tokio::test]
async fn a_delete_before_delivery_retracts_the_message() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let mut delete = mention("2", "c").target("1");
    delete.kind = EventKind::Delete;
    let mut edit = event("channel:slack", "3", "c").target("4").text("fixed");
    edit.kind = EventKind::Edit;
    pipeline
        .ingest(vec![
            mention("1", "c"),
            delete,
            mention("4", "c").text("tpyo"),
            edit,
        ])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();
    assert_eq!(pipeline.stats().await.unwrap().outcomes.dropped, 2);
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    assert_eq!(ids(&turns[0].events), ["4"]);
    assert_eq!(turns[0].events[0].text, "fixed");
    assert_eq!(turns[0].absorbed, [EventRef::new("channel:slack", "3")]);
}

#[tokio::test]
async fn salience_picks_the_lane() {
    let fixture = Fixture::new();
    let mut config = shipped_pipeline_config().unwrap();
    config.salience.keywords = vec!["failed".into()];
    let pipeline = fixture.builder().config(config).build().await.unwrap();
    let trigger = |id: &str, conversation: &str, text: &str| {
        let mut event = event("composio:github", id, conversation).text(text);
        event.kind = EventKind::Trigger;
        event
    };
    pipeline
        .ingest(vec![
            trigger("1", "ci", "build failed on main"),
            trigger("2", "stars", "someone starred the repo"),
            mention("3", "chat"),
        ])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();
    let lane_of = |turns: &[crate::turn::PendingTurn]| {
        turns
            .iter()
            .map(|t| t.conversation_key.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        lane_of(&take_turns(&fixture, &pipeline, Lane::Interactive).await),
        ["chat"]
    );
    assert_eq!(
        lane_of(&take_turns(&fixture, &pipeline, Lane::Actionable).await),
        ["ci"]
    );
    assert_eq!(
        lane_of(&take_turns(&fixture, &pipeline, Lane::Background).await),
        ["stars"]
    );
}

#[tokio::test]
async fn a_source_that_keeps_repeating_itself_fades_into_the_digest() {
    let fixture = Fixture::new();
    let sink = Arc::new(CollectingSink::new());
    let mut config = shipped_pipeline_config().unwrap();
    config.lanes.background.cadence = None;
    let pipeline = fixture
        .builder()
        .config(config)
        .sink(sink.clone())
        .build()
        .await
        .unwrap();
    for i in 0..4 {
        let mut ping = event("monitor:disk", &i.to_string(), &format!("alert-{i}"))
            .text("disk usage above 90 percent on host 7");
        ping.kind = EventKind::Notification;
        ping.received_at_ms = fixture.clock.now();
        pipeline.ingest(vec![ping]).await.unwrap();
        pipeline.drain().await.unwrap();
        fixture.clock.advance(2_000);
        pipeline.drain().await.unwrap();
    }
    // 25, then 15, then 9 and 5: two repeats fall below the background
    // threshold and fade into the digest.
    assert_eq!(
        take_turns(&fixture, &pipeline, Lane::Background)
            .await
            .len(),
        2
    );
    let habituated = sink
        .outcomes()
        .into_iter()
        .filter(|o| o.reason == Reason::Habituated)
        .count();
    assert_eq!(habituated, 2);
}

#[tokio::test]
async fn a_full_lane_spills_to_the_digest_instead_of_dropping() {
    let fixture = Fixture::new();
    let sink = Arc::new(CollectingSink::new());
    let mut config = shipped_pipeline_config().unwrap();
    config.lanes.interactive.max_backlog = 2;
    let pipeline = fixture
        .builder()
        .config(config)
        .sink(sink.clone())
        .build()
        .await
        .unwrap();
    let events: Vec<_> = (0..5)
        .map(|i| mention(&i.to_string(), &format!("c{i}")))
        .collect();
    pipeline.ingest(events).await.unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();
    assert_eq!(
        take_turns(&fixture, &pipeline, Lane::Interactive)
            .await
            .len(),
        2
    );
    let overflow = sink
        .outcomes()
        .into_iter()
        .filter(|o| o.reason == Reason::LaneOverflow)
        .count();
    assert_eq!(overflow, 3);
    fixture.clock.advance(600_000);
    pipeline.drain().await.unwrap();
    let digest = take_turns(&fixture, &pipeline, Lane::Digest).await;
    assert_eq!(digest[0].events.len(), 3);
}

#[derive(Debug)]
struct Classifier(
    SalienceClass,
    std::sync::atomic::AtomicU32,
    std::sync::Mutex<Vec<Lane>>,
);

impl Classifier {
    fn new(class: SalienceClass) -> Arc<Self> {
        Arc::new(Self(
            class,
            std::sync::atomic::AtomicU32::new(0),
            std::sync::Mutex::new(Vec::new()),
        ))
    }

    fn delivered(&self, lane: Lane) -> usize {
        self.2
            .lock()
            .unwrap()
            .iter()
            .filter(|l| **l == lane)
            .count()
    }
}

#[async_trait::async_trait]
impl crate::Host for Classifier {
    async fn classify_salience(
        &self,
        _request: tinyeventqueues_bus::ClassifySalienceRequest,
    ) -> crate::Result<tinyeventqueues_bus::ClassifySalienceResponse> {
        self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.0 == SalienceClass::Noise {
            return Err(crate::Error::Host {
                method: "ClassifySalience",
                message: "down".into(),
            });
        }
        Ok(tinyeventqueues_bus::ClassifySalienceResponse::new(self.0))
    }

    async fn deliver(
        &self,
        request: tinyeventqueues_bus::DeliverRequest,
    ) -> crate::Result<tinyeventqueues_bus::DeliverResponse> {
        self.2.lock().unwrap().push(request.turn.lane);
        Ok(tinyeventqueues_bus::DeliverResponse::new(
            tinyeventqueues_bus::DeliverStatus::Accepted,
        ))
    }
}

#[tokio::test]
async fn borderline_scores_are_classified_by_the_host_within_budget() {
    let fixture = Fixture::new();
    let host = Classifier::new(SalienceClass::Urgent);
    let mut config = shipped_pipeline_config().unwrap();
    config.salience.borderline = Some(tinyeventqueues_bus::Borderline::new(20, 30, 2));
    let pipeline = fixture
        .builder()
        .config(config)
        .host(host.clone())
        .build()
        .await
        .unwrap();
    // Plain chatter in a DM-less group scores 25: borderline. Triggers pass the gate.
    let events: Vec<_> = (0..3)
        .map(|i| {
            let mut e = event("composio:x", &i.to_string(), &format!("t{i}"));
            e.kind = EventKind::Trigger;
            e
        })
        .collect();
    pipeline.ingest(events).await.unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();
    assert_eq!(host.1.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(host.delivered(Lane::Interactive), 2);
    assert_eq!(host.delivered(Lane::Background), 1);
}

#[tokio::test]
async fn a_failing_classifier_leaves_the_rule_score_standing() {
    let fixture = Fixture::new();
    let host = Classifier::new(SalienceClass::Noise);
    let mut config = shipped_pipeline_config().unwrap();
    config.salience.borderline = Some(tinyeventqueues_bus::Borderline::new(20, 30, 0));
    let pipeline = fixture
        .builder()
        .config(config.clone())
        .host(host.clone())
        .build()
        .await
        .unwrap();
    let mut trigger = event("composio:x", "1", "t");
    trigger.kind = EventKind::Trigger;
    pipeline.ingest(vec![trigger.clone()]).await.unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();
    // A zero budget never calls the host.
    assert_eq!(host.1.load(std::sync::atomic::Ordering::SeqCst), 0);
    config.salience.borderline = Some(tinyeventqueues_bus::Borderline::new(20, 30, 10));
    pipeline.configure(config).unwrap();
    trigger = event("composio:x", "2", "t");
    trigger.kind = EventKind::Trigger;
    trigger.received_at_ms = fixture.clock.now();
    pipeline.ingest(vec![trigger]).await.unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    pipeline.drain().await.unwrap();
    assert_eq!(host.1.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(host.delivered(Lane::Background), 2);
}

#[tokio::test]
async fn an_unreadable_ingress_message_is_dead_lettered() {
    let fixture = Fixture::new();
    let pipeline = fixture.pipeline().await;
    let ingress = pipeline.queues().ingress.name;
    fixture
        .queues
        .enqueue(&ingress, &[NewMessage::new(json!({"not": "an envelope"}))])
        .await
        .unwrap();
    assert_eq!(pipeline.drain().await.unwrap().ingested, 1);
    let dead = pipeline.list_dead(10).await.unwrap();
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].reason, "unreadable ingress envelope");
}

#[tokio::test]
async fn a_restart_mid_debounce_loses_nothing() {
    let fixture = Fixture::new();
    let first = fixture.pipeline().await;
    first.ingest(vec![mention("1", "c")]).await.unwrap();
    first.drain().await.unwrap();
    drop(first);
    let restarted = fixture.pipeline().await;
    restarted.ingest(vec![mention("2", "c")]).await.unwrap();
    restarted.drain().await.unwrap();
    fixture.clock.advance(10_000);
    restarted.drain().await.unwrap();
    let turns = take_turns(&fixture, &restarted, Lane::Interactive).await;
    assert_eq!(ids(&turns[0].events), ["1", "2"]);
}

#[tokio::test]
async fn the_pass_runs_on_a_backend_without_dedupe_or_groups() {
    let fixture = Fixture::with_native(false);
    let pipeline = fixture.pipeline().await;
    pipeline
        .ingest(vec![
            mention("1", "a"),
            mention("2", "a"),
            mention("3", "b"),
        ])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(10_000);
    assert_eq!(pipeline.drain().await.unwrap().flushed, 2);
    let mut turns: Vec<_> = take_turns(&fixture, &pipeline, Lane::Interactive)
        .await
        .into_iter()
        .map(|t| (t.conversation_key, ids(&t.events).join(",")))
        .collect();
    turns.sort();
    assert_eq!(
        turns,
        [("a".into(), "1,2".into()), ("b".into(), "3".into())]
    );
    // A redelivery is still recognized.
    let again = pipeline.ingest(vec![mention("1", "a")]).await.unwrap();
    assert_eq!(again.results[0].disposition, Disposition::Duplicate);
}

/// The shipped pipeline with `coalesce` as `x:mentions`' own debounce.
fn with_source_debounce(coalesce: tinyeventqueues_bus::SourceCoalesce) -> PipelineConfig {
    let mut config = shipped_pipeline_config().unwrap();
    let mut own = SourcePolicy::new("x:mentions");
    own.weight = 50;
    own.coalesce = Some(coalesce);
    config.sources.insert(0, own);
    config
}

fn x_mention(id: &str) -> InboundEvent {
    let mut event = event("x:mentions", id, "mentions");
    event.kind = EventKind::Trigger;
    event.sender = Sender::new("x:mentions");
    event
}

#[tokio::test]
async fn a_source_debounce_replaces_the_pipeline_one_for_that_source_only() {
    let fixture = Fixture::new();
    let config = with_source_debounce(
        tinyeventqueues_bus::SourceCoalesce::new()
            .debounce_ms(30_000)
            .max_wait_ms(300_000),
    );
    let pipeline = fixture.builder().config(config).build().await.unwrap();
    pipeline
        .ingest(vec![x_mention("1"), x_mention("2"), mention("3", "c")])
        .await
        .unwrap();
    pipeline.drain().await.unwrap();
    // The pipeline's own 10 s max wait flushes the other source only.
    fixture.clock.advance(10_000);
    assert_eq!(pipeline.drain().await.unwrap().flushed, 1);
    pipeline.ingest(vec![x_mention("4")]).await.unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(29_999);
    assert_eq!(pipeline.drain().await.unwrap().flushed, 0);
    fixture.clock.advance(1);
    assert_eq!(pipeline.drain().await.unwrap().flushed, 1);
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    let batches: Vec<_> = turns.iter().map(|t| ids(&t.events).join(",")).collect();
    assert_eq!(batches, ["3", "1,2,4"]);
}

#[tokio::test]
async fn a_full_source_buffer_flushes_at_once_in_turns_of_at_most_max_events() {
    let fixture = Fixture::new();
    let config = with_source_debounce(
        tinyeventqueues_bus::SourceCoalesce::new()
            .debounce_ms(30_000)
            .max_events(2),
    );
    let pipeline = fixture.builder().config(config).build().await.unwrap();
    pipeline.ingest(vec![x_mention("1")]).await.unwrap();
    assert_eq!(pipeline.drain().await.unwrap().flushed, 0);
    // The second event fills the buffer: it goes out without the debounce.
    pipeline.ingest(vec![x_mention("2")]).await.unwrap();
    assert_eq!(pipeline.drain().await.unwrap().flushed, 1);
    // A burst taken in one pass is split rather than carried in one turn.
    let burst: Vec<_> = (3..=7).map(|i| x_mention(&i.to_string())).collect();
    pipeline.ingest(burst).await.unwrap();
    pipeline.drain().await.unwrap();
    fixture.clock.advance(30_000);
    pipeline.drain().await.unwrap();
    let turns = take_turns(&fixture, &pipeline, Lane::Interactive).await;
    let batches: Vec<_> = turns.iter().map(|t| ids(&t.events).join(",")).collect();
    assert_eq!(batches, ["1,2", "3,4", "5,6", "7"]);
}
