//! Whole-pipeline properties: no event is lost, and a flood keeps order and
//! interactive latency.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use tinyeventqueues_bus::{
    DeliverStatus, Disposition, EventKind, EventRef, InboundEvent, Lane, MediaRef, RateLimit,
    Sender,
};

use crate::config::shipped_pipeline_config;
use crate::store::Ledger;
use crate::test_support::{Fixture, ScriptedHost};
use crate::{CollectingSink, Error, NoopSink, Pipeline};

/// A small deterministic generator (xorshift64*).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[usize::try_from(self.below(items.len() as u64)).unwrap()]
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

const SOURCES: [&str; 6] = [
    "web:chat",
    "channel:slack",
    "channel:telegram",
    "composio:twitter",
    "webhook:ci",
    "monitor:disk",
];
const WORDS: [&str; 12] = [
    "deploy", "failed", "lunch", "invoice", "urgent", "ping", "status", "hello", "build", "error",
    "thanks", "meeting",
];
const KINDS: [EventKind; 8] = [
    EventKind::Message,
    EventKind::Reply,
    EventKind::Mention,
    EventKind::Reaction,
    EventKind::Edit,
    EventKind::Delete,
    EventKind::Trigger,
    EventKind::Notification,
];

fn random_event(rng: &mut Rng, id: u64, now: u64, earlier: &[InboundEvent]) -> InboundEvent {
    let source = *rng.pick(&SOURCES);
    let conversation = format!("conv-{}", rng.below(15));
    let sender = match rng.below(20) {
        0 => Sender::new("agent").as_self(),
        1 | 2 => Sender::new("helper-bot").as_bot(),
        n => Sender::new(format!("user-{}", n % 5)),
    };
    let mut event = InboundEvent::new(
        source,
        id.to_string(),
        conversation,
        sender,
        *rng.pick(&KINDS),
        now,
    )
    .addressed(rng.chance(30))
    .direct(rng.chance(20))
    .text(
        (0..=rng.below(6))
            .map(|_| *rng.pick(&WORDS))
            .collect::<Vec<_>>()
            .join(" "),
    );
    if rng.chance(30) {
        event = event.thread(format!("t{}", rng.below(3)));
    }
    if rng.chance(5) {
        event = event.with_media(MediaRef::new("image", "blob://x"));
    }
    if matches!(
        event.kind,
        EventKind::Edit | EventKind::Delete | EventKind::Reply | EventKind::Reaction
    ) && !earlier.is_empty()
    {
        let target = rng.pick(earlier).clone();
        event.target_event_id = Some(target.event_id.clone());
        event.source = target.source.clone();
        event.conversation_key = target.conversation_key.clone();
        event.thread_key = target.thread_key.clone();
        event.sender = target.sender.clone();
    }
    event
}

/// Every turn the host sees is accepted, retried, rejected or fails, at
/// random but reproducibly (by turn id and attempt).
fn chaotic_host() -> Arc<ScriptedHost> {
    ScriptedHost::new(|turn| {
        let mut rng = Rng(turn
            .turn_id
            .bytes()
            .fold(u64::from(turn.attempt) | 1, |h, b| {
                h.rotate_left(5) ^ u64::from(b)
            })
            .wrapping_mul(0x9e37_79b9_7f4a_7c15)
            | 1);
        Ok(match rng.below(100) {
            0..70 => DeliverStatus::Accepted,
            70..88 => DeliverStatus::Retry { after_ms: None },
            88..94 => DeliverStatus::Rejected {
                detail: "no".into(),
            },
            _ => {
                return Err(Error::Host {
                    method: "Deliver",
                    message: "flaky".into(),
                });
            }
        })
    })
}

async fn quiesce(fixture: &Fixture, pipeline: &Pipeline) {
    for _ in 0..60 {
        fixture.clock.advance(600_000);
        pipeline.drain().await.unwrap();
    }
}

/// Every accepted event ends in exactly one of delivered, context, digest,
/// dead or dropped — on a full-featured backend and on a broker without
/// dedupe or groups.
#[tokio::test]
async fn no_ingested_event_is_lost_or_ends_twice() {
    // Verified locally over 80 seeds; CI runs a handful.
    let seeds: Vec<(bool, u64)> = (1..=3)
        .flat_map(|s| [(true, s * 7_919), (false, s * 7_919)])
        .collect();
    for (native, seed) in seeds {
        let fixture = Fixture::with_native(native);
        let sink = Arc::new(CollectingSink::new());
        let mut config = shipped_pipeline_config().unwrap();
        config.gate.rate_limits.per_sender = Some(RateLimit::new(10, 20));
        config.gate.bot_loop.max_events = 4;
        let pipeline = fixture
            .builder()
            .config(config)
            .host(chaotic_host())
            .sink(sink.clone())
            .build()
            .await
            .unwrap();
        let mut rng = Rng(seed);
        let mut sent: Vec<InboundEvent> = Vec::new();
        let mut accepted: HashSet<EventRef> = HashSet::new();
        let mut next_id = 0;
        for _ in 0..40 {
            let mut batch = Vec::new();
            for _ in 0..=rng.below(15) {
                let now = fixture.clock.now();
                let event = if !sent.is_empty() && rng.chance(8) {
                    rng.pick(&sent).clone()
                } else if rng.chance(2) {
                    let mut bad = random_event(&mut rng, next_id, now, &sent);
                    bad.event_id.clear();
                    bad
                } else {
                    next_id += 1;
                    random_event(&mut rng, next_id, now, &sent)
                };
                batch.push(event);
            }
            let response = pipeline.ingest(batch.clone()).await.unwrap();
            for (event, result) in batch.into_iter().zip(response.results) {
                if result.disposition == Disposition::Accepted {
                    assert!(accepted.insert(event.reference()), "accepted twice");
                    sent.push(event);
                }
            }
            pipeline.drain().await.unwrap();
            fixture.clock.advance(rng.below(5_000));
        }
        quiesce(&fixture, &pipeline).await;

        let ledger = Ledger::new(Arc::clone(&fixture.docs), Arc::new(NoopSink));
        for event in &accepted {
            assert!(
                ledger.terminal(event).await.unwrap().is_some(),
                "native={native} seed={seed}: {event:?} never ended"
            );
        }
        let mut ended: HashMap<EventRef, usize> = HashMap::new();
        for outcome in sink.outcomes() {
            // Invalid envelopes are dropped at ingest without being accepted.
            if outcome.disposition.is_terminal() && accepted.contains(&outcome.event) {
                *ended.entry(outcome.event).or_default() += 1;
            }
        }
        assert!(
            ended.values().all(|count| *count == 1),
            "an event ended twice"
        );
        assert_eq!(ended.len(), accepted.len(), "native={native} seed={seed}");
        let stats = pipeline.stats().await.unwrap();
        assert_eq!(
            stats.outcomes.total(),
            u64::try_from(accepted.len()).unwrap()
        );
        for queue in &stats.queues {
            assert_eq!(queue.ready + queue.delayed + queue.leased, 0, "{queue:?}");
        }
    }
}

/// Ten thousand mixed events over a hundred simulated seconds: a background
/// webhook flood (90 a second) and interactive web chat (10 a second).
/// Every event ends exactly once, conversations keep their order, threads
/// never merge, and interactive latency stays bounded while the background
/// floods.
#[tokio::test]
async fn a_ten_thousand_event_flood_keeps_order_and_interactive_latency() {
    for native in [true, false] {
        flood(Fixture::with_native(native)).await;
    }
}

/// The shipped manifest without dedupe windows: acknowledged messages leave
/// no tombstones behind, which keeps the in-memory queue small enough for a
/// ten-thousand-event test to run quickly. Redelivery is not what this test
/// is about.
fn lean_manifest() -> crate::config::QueueManifest {
    let mut manifest = crate::config::shipped_manifest().unwrap();
    for queue in &mut manifest.queues {
        queue.dedupe_window_ms = 0;
    }
    manifest
}

/// One simulated second of the flood: nine webhook notifications for every
/// web-chat message.
fn flood_second(rng: &mut Rng, next_id: &mut u64, now: u64) -> Vec<InboundEvent> {
    let mut batch = Vec::new();
    for i in 0..100 {
        *next_id += 1;
        let id = next_id.to_string();
        let event = if i % 10 == 0 {
            let conversation = format!("chat-{}", rng.below(50));
            let mut event = InboundEvent::new(
                "web:chat",
                &id,
                conversation,
                Sender::new("visitor"),
                EventKind::Message,
                now,
            )
            .direct(true)
            .text(
                (0..8)
                    .map(|_| format!("w{}", rng.below(4_096)))
                    .collect::<Vec<_>>()
                    .join(" "),
            );
            if rng.chance(50) {
                event = event.thread(format!("t{}", rng.below(2)));
            }
            event
        } else {
            let conversation = format!("repo-{}", rng.below(100));
            InboundEvent::new(
                "webhook:ci",
                &id,
                conversation,
                Sender::new("ci"),
                EventKind::Notification,
                now,
            )
            .text(format!("build {id} finished"))
        };
        batch.push(event);
    }
    batch
}

async fn flood(fixture: Fixture) {
    let delivered: Arc<std::sync::Mutex<Vec<(tinyeventqueues_bus::Turn, u64)>>> = Arc::default();
    let record = Arc::clone(&delivered);
    let clock = fixture.clock.clone();
    let host = ScriptedHost::new(move |turn| {
        record.lock().unwrap().push((turn.clone(), clock.now()));
        Ok(DeliverStatus::Accepted)
    });
    let mut config = shipped_pipeline_config().unwrap();
    config.gate.rate_limits = tinyeventqueues_bus::RateLimits::default();
    config.lanes.interactive.concurrency = 64;
    config.lanes.background.concurrency = 32;
    let pipeline = fixture
        .builder()
        .manifest(lean_manifest())
        .config(config)
        .host(host)
        .build()
        .await
        .unwrap();
    let mut rng = Rng(42);
    let mut next_id: u64 = 0;
    for _second in 0..100 {
        let batch = flood_second(&mut rng, &mut next_id, fixture.clock.now());
        let response = pipeline.ingest(batch).await.unwrap();
        assert!(
            response
                .results
                .iter()
                .all(|r| r.disposition == Disposition::Accepted)
        );
        pipeline.drain().await.unwrap();
        fixture.clock.advance(1_000);
    }
    // Passes keep running once a second for a while, then the clock jumps
    // ahead so every cadence, backoff and digest comes due.
    for step in 0..230 {
        fixture
            .clock
            .advance(if step < 30 { 1_000 } else { 60_000 });
        pipeline.drain().await.unwrap();
    }

    let delivered = delivered.lock().unwrap().clone();
    let mut last: BTreeMap<(String, Option<String>), u64> = BTreeMap::new();
    let mut worst_interactive_ms = 0;
    let mut seen = 0usize;
    for (turn, at) in &delivered {
        seen += turn.events.len();
        if turn.is_digest() {
            continue;
        }
        let key = (turn.conversation_key.clone(), turn.thread_key.clone());
        for event in &turn.events {
            assert_eq!(
                (event.conversation_key.as_str(), &event.thread_key),
                (key.0.as_str(), &key.1),
                "a turn merged threads"
            );
            let id: u64 = event.event_id.parse().unwrap();
            let previous = last.insert(key.clone(), id).unwrap_or(0);
            assert!(previous < id, "{key:?} reordered: {previous} then {id}");
        }
        // The newest event of an interactive turn is the one that made it
        // interactive; older ones may have waited on a slower lane first.
        if let Some(newest) = turn
            .events
            .last()
            .filter(|_| turn.lane == Lane::Interactive)
        {
            worst_interactive_ms = worst_interactive_ms.max(at - newest.received_at_ms);
        }
    }
    assert_eq!(seen, 10_000);
    // Interactive work waits out its debounce and a pass or two, however
    // deep the background flood is.
    assert!(
        worst_interactive_ms <= 5_000,
        "interactive latency {worst_interactive_ms} ms"
    );
    let outcomes = pipeline.stats().await.unwrap().outcomes;
    assert_eq!(outcomes.total(), 10_000);
    assert_eq!(outcomes.dead + outcomes.dropped, 0);
}
