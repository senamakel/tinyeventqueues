//! Unit tests for cadence resolution and the persisted schedule.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use tinyeventqueues_bus::{Budget, Lane, SourcePolicy};

use super::*;
use crate::config::shipped_pipeline_config;
use crate::test_support::{Fixture, event};

fn config() -> PipelineConfig {
    let mut config = shipped_pipeline_config().unwrap();
    let mut x = SourcePolicy::new("composio:*");
    x.cadence = Some("every(10m)".into());
    x.budget = Some(Budget::new(5));
    config.sources.insert(0, x);
    config.lanes.background.cadence = Some("cron(\"0 * * * *\")".into());
    config
}

#[test]
fn a_source_cadence_wins_over_its_lanes() {
    let config = config();
    let source = resolve(&config, "composio:twitter", Lane::Interactive);
    assert_eq!(source.key, "source:composio:*");
    assert_eq!(source.cadence, Cadence::Every(600_000));
    assert_eq!(source.budget, Some(Budget::new(5)));
    let lane = resolve(&config, "channel:slack", Lane::Background);
    assert_eq!(lane.key, "lane:background");
    assert!(matches!(lane.cadence, Cadence::Cron(_)));
    assert_eq!(
        resolve(&config, "channel:slack", Lane::Actionable).cadence,
        Cadence::Instant
    );
}

#[test]
fn keys_resolve_back_to_their_cadence_or_to_instant() {
    let config = config();
    assert_eq!(
        by_key(&config, "source:composio:*").cadence,
        Cadence::Every(600_000)
    );
    assert!(matches!(
        by_key(&config, "lane:background").cadence,
        Cadence::Cron(_)
    ));
    for gone in ["source:removed:*", "lane:nowhere", "other"] {
        let resolved = by_key(&config, gone);
        assert_eq!(
            (resolved.cadence, resolved.budget),
            (Cadence::Instant, None)
        );
    }
    let mut broken = config.clone();
    broken.lanes.background.cadence = Some("whenever".into());
    assert_eq!(by_key(&broken, "lane:background").cadence, Cadence::Instant);
}

async fn scheduler(fixture: &Fixture) -> Scheduler {
    crate::store::ensure(fixture.docs.as_ref()).await.unwrap();
    Scheduler::new(&fixture.docs)
}

#[tokio::test]
async fn the_schedule_is_created_once_and_survives_a_restart() {
    let fixture = Fixture::new();
    let scheduler = scheduler(&fixture).await;
    let resolved = resolve(&config(), "composio:x", Lane::Background);
    assert_eq!(
        scheduler.next_due(&resolved, 1).await.unwrap(),
        Some(600_000)
    );
    let restarted = Scheduler::new(&fixture.docs);
    assert_eq!(
        restarted.next_due(&resolved, 599_999).await.unwrap(),
        Some(600_000)
    );
    let never = Resolved {
        key: "lane:x".into(),
        cadence: Cadence::parse("cron(0 0 31 2 *)").unwrap(),
        budget: None,
    };
    assert_eq!(scheduler.next_due(&never, 0).await.unwrap(), None);
}

#[tokio::test]
async fn a_tick_resumes_with_what_is_left_of_its_budget() {
    let fixture = Fixture::new();
    let scheduler = scheduler(&fixture).await;
    let resolved = resolve(&config(), "composio:x", Lane::Background);
    assert_eq!(scheduler.begin_tick(&resolved, 0).await.unwrap(), None);
    scheduler.next_due(&resolved, 0).await.unwrap();
    assert_eq!(
        scheduler.begin_tick(&resolved, 599_999).await.unwrap(),
        None
    );
    assert_eq!(
        scheduler.begin_tick(&resolved, 600_000).await.unwrap(),
        Some(5)
    );
    scheduler.count_release(&resolved.key).await.unwrap();
    scheduler.count_release(&resolved.key).await.unwrap();
    // A crash here: the next pass resumes the same tick.
    assert_eq!(
        scheduler.begin_tick(&resolved, 600_100).await.unwrap(),
        Some(3)
    );
    assert_eq!(
        scheduler.end_tick(&resolved, 600_100).await.unwrap(),
        Some(1_200_000)
    );
    assert_eq!(
        scheduler.begin_tick(&resolved, 600_200).await.unwrap(),
        None
    );
    assert_eq!(
        scheduler.begin_tick(&resolved, 1_200_000).await.unwrap(),
        Some(5)
    );
    let unlimited = Resolved {
        budget: None,
        ..resolved
    };
    assert_eq!(
        scheduler.begin_tick(&unlimited, 1_200_000).await.unwrap(),
        Some(u32::MAX)
    );
}

#[tokio::test]
async fn staging_folds_a_threads_events_into_one_entry() {
    let fixture = Fixture::new();
    let scheduler = scheduler(&fixture).await;
    let resolved = resolve(&config(), "composio:x", Lane::Background);
    let one = event("composio:x", "1", "c");
    assert!(
        scheduler
            .stage(
                &resolved,
                Lane::Background,
                std::slice::from_ref(&one),
                &[],
                600_000,
                5
            )
            .await
            .unwrap()
    );
    let two = event("composio:x", "2", "c");
    let edit = tinyeventqueues_bus::EventRef::new("composio:x", "3");
    assert!(
        scheduler
            .stage(
                &resolved,
                Lane::Background,
                &[two, one.clone()],
                &[edit],
                900_000,
                6
            )
            .await
            .unwrap()
    );
    assert!(
        !scheduler
            .stage(
                &resolved,
                Lane::Background,
                std::slice::from_ref(&one),
                &[],
                1,
                7
            )
            .await
            .unwrap()
    );
    assert!(
        !scheduler
            .stage(&resolved, Lane::Background, &[], &[], 1, 7)
            .await
            .unwrap()
    );
    // The same thread on a more urgent lane folds in and lifts the entry.
    let three = event("composio:x", "4", "c");
    assert!(
        scheduler
            .stage(&resolved, Lane::Actionable, &[three], &[], 900_000, 8)
            .await
            .unwrap()
    );
    let waiting = scheduler.waiting(&resolved.key).await.unwrap();
    assert_eq!(waiting.len(), 1);
    assert_eq!(waiting[0].doc.lane, Lane::Actionable);
    let entry = &waiting[0].doc;
    assert_eq!(entry.events.len(), 3);
    assert_eq!(entry.refs().len(), 4);
    assert_eq!((entry.first_ms, entry.due_ms), (5, 600_000));
    assert_eq!(scheduler.due(599_999).await.unwrap().len(), 0);
    assert_eq!(scheduler.due(600_000).await.unwrap().len(), 1);
    scheduler.defer(&waiting[0], 700_000).await.unwrap();
    assert_eq!(scheduler.due(600_000).await.unwrap().len(), 0);
    assert!(scheduler.entry_of(&one).await.unwrap().is_some());
    scheduler.remove(&waiting[0]).await.unwrap();
    assert!(scheduler.entry_of(&one).await.unwrap().is_none());
}

#[test]
fn an_instant_budget_is_per_second() {
    let limit = Scheduler::instant_limit(Budget::new(3));
    assert_eq!((limit.capacity, limit.refill_per_minute), (3, 180));
}
