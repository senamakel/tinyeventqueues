//! Unit tests for salience scoring and habituation.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use tinyeventqueues_bus::{
    EventKind, HabituationConfig, Lane, SalienceClass, SalienceConfig, SalienceThresholds, Sender,
};

use super::*;
use crate::test_support::{Fixture, event};

fn config() -> SalienceConfig {
    let mut config = SalienceConfig::default();
    config.keywords = vec!["Outage".into()];
    config.known_contacts = vec!["channel:slack/boss".into()];
    config
}

#[test]
fn features_add_up_and_clamp() {
    let config = config();
    let plain = event("channel:slack", "1", "c");
    assert_eq!(rule_score(&plain, &config, 0, 0), 25);
    assert_eq!(
        rule_score(&plain.clone().addressed(true), &config, 0, 0),
        75
    );
    let mut mention = plain.clone();
    mention.kind = EventKind::Mention;
    assert_eq!(rule_score(&mention, &config, 0, 0), 75);
    assert_eq!(
        rule_score(&plain.clone().direct(true).addressed(true), &config, 0, 0),
        100
    );
    assert_eq!(
        rule_score(&plain.clone().text("total OUTAGE now"), &config, 0, 0),
        45
    );
    let mut boss = plain.clone();
    boss.sender = Sender::new("boss");
    assert_eq!(rule_score(&boss, &config, 0, 0), 40);
    assert_eq!(rule_score(&plain, &config, 10, 0), 35);
    assert_eq!(rule_score(&plain, &config, -100, 0), 0);
}

#[test]
fn old_events_lose_points_up_to_a_cap() {
    let config = config();
    let plain = event("s", "1", "c");
    let at = plain.received_at_ms;
    assert_eq!(rule_score(&plain, &config, 0, at + 5 * 60_000), 20);
    assert_eq!(rule_score(&plain, &config, 0, at + 600 * 60_000), 5);
}

#[test]
fn thresholds_and_classes_map_to_lanes() {
    let thresholds = SalienceThresholds::default();
    assert_eq!(lane_for(100, &thresholds), Some(Lane::Interactive));
    assert_eq!(lane_for(70, &thresholds), Some(Lane::Interactive));
    assert_eq!(lane_for(69, &thresholds), Some(Lane::Actionable));
    assert_eq!(lane_for(40, &thresholds), Some(Lane::Actionable));
    assert_eq!(lane_for(15, &thresholds), Some(Lane::Background));
    assert_eq!(lane_for(14, &thresholds), None);
    assert_eq!(
        lane_for_class(SalienceClass::Urgent),
        Some(Lane::Interactive)
    );
    assert_eq!(
        lane_for_class(SalienceClass::Actionable),
        Some(Lane::Actionable)
    );
    assert_eq!(lane_for_class(SalienceClass::Fyi), Some(Lane::Background));
    assert_eq!(lane_for_class(SalienceClass::Noise), None);
}

#[test]
fn near_identical_texts_have_near_fingerprints() {
    let a = simhash("Your order 1234 has shipped and is on its way").unwrap();
    let b = simhash("your order 1234 has shipped and is on its way!").unwrap();
    let c = simhash("Lunch at noon tomorrow? The usual place").unwrap();
    assert_eq!(distance(a, b), 0);
    assert!(distance(a, c) > 3);
    assert_eq!(simhash("  ... !!! "), None);
}

#[test]
fn decay_compounds_and_stops_at_zero() {
    assert_eq!(decay(100, 0, 40), 100);
    assert_eq!(decay(100, 1, 40), 60);
    assert_eq!(decay(100, 2, 40), 36);
    assert_eq!(decay(100, 50, 100), 0);
}

#[tokio::test]
async fn a_repeating_source_habituates_and_recovers_after_the_window() {
    let fixture = Fixture::new();
    crate::store::ensure(fixture.docs.as_ref()).await.unwrap();
    let habituation = Habituation::new(&fixture.docs);
    let config = HabituationConfig::default();
    let ping = |id: &str| event("webhook:monitor", id, "c").text("CPU usage above 90% on host-7");
    let first = habituation.apply(&ping("1"), 50, &config, 0).await.unwrap();
    assert_eq!(
        first,
        Scored {
            score: 50,
            habituated: false
        }
    );
    let second = habituation.apply(&ping("2"), 50, &config, 1).await.unwrap();
    assert_eq!(
        second,
        Scored {
            score: 30,
            habituated: true
        }
    );
    let third = habituation.apply(&ping("3"), 50, &config, 2).await.unwrap();
    assert_eq!(third.score, 18);
    // Another source is not affected.
    let other = event("webhook:other", "4", "c").text("CPU usage above 90% on host-7");
    assert_eq!(
        habituation
            .apply(&other, 50, &config, 3)
            .await
            .unwrap()
            .score,
        50
    );
    // An hour later the source is fresh again.
    let later = habituation
        .apply(&ping("5"), 50, &config, config.window_ms + 10)
        .await
        .unwrap();
    assert_eq!(later.score, 50);
    // Empty text never habituates.
    let silent = event("webhook:monitor", "6", "c").text("");
    assert!(
        !habituation
            .apply(&silent, 50, &config, 0)
            .await
            .unwrap()
            .habituated
    );
}

#[tokio::test]
async fn the_fingerprint_window_is_bounded() {
    let fixture = Fixture::new();
    crate::store::ensure(fixture.docs.as_ref()).await.unwrap();
    let habituation = Habituation::new(&fixture.docs);
    let mut config = HabituationConfig::default();
    config.window = 1;
    let a = event("s", "1", "c").text("alpha beta gamma");
    let b = event("s", "2", "c").text("totally different words here");
    habituation.apply(&a, 50, &config, 0).await.unwrap();
    habituation.apply(&b, 50, &config, 1).await.unwrap();
    // `a` was pushed out by `b`, so a repeat of `a` is fresh.
    assert!(
        !habituation
            .apply(&a, 50, &config, 2)
            .await
            .unwrap()
            .habituated
    );
}
