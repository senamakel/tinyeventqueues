//! Wire-form tests for the pipeline configuration.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde_json::json;

use super::{AckPolicy, Lane, PipelineConfig, SourceCoalesce, SourcePolicy};

fn minimal() -> serde_json::Value {
    json!({
        "version": 1,
        "ingress": {"queue": "teq.ingress"},
        "dead_letter": {"queue": "teq.dead"},
        "lanes": {
            "interactive": {"queue": "teq.lane.interactive"},
            "actionable": {"queue": "teq.lane.actionable"},
            "background": {"queue": "teq.lane.background"},
            "digest": {"queue": "teq.lane.digest", "concurrency": 1}
        }
    })
}

#[test]
fn a_minimal_file_fills_in_defaults() {
    let config: PipelineConfig = serde_json::from_value(minimal()).unwrap();
    assert_eq!(config.ingress.tombstone_ttl_ms, 2_592_000_000);
    assert_eq!(config.ingress.batch_size, 64);
    assert_eq!(config.lanes.get(Lane::Interactive).concurrency, 4);
    assert_eq!(config.lanes.get(Lane::Digest).concurrency, 1);
    assert_eq!(
        config.lanes.get(Lane::Actionable).queue,
        "teq.lane.actionable"
    );
    assert_eq!(
        config.lanes.get(Lane::Background).queue,
        "teq.lane.background"
    );
    assert_eq!(config.sources.len(), 0);
    assert!(config.schema.is_none());
}

#[test]
fn unknown_fields_are_rejected_at_every_level() {
    let mut top = minimal();
    top["surprise"] = json!(1);
    assert!(serde_json::from_value::<PipelineConfig>(top).is_err());

    let mut nested = minimal();
    nested["ingress"]["batchsize"] = json!(1);
    let error = serde_json::from_value::<PipelineConfig>(nested).unwrap_err();
    assert!(error.to_string().contains("batchsize"));
}

#[test]
fn the_schema_key_is_accepted() {
    let mut with_schema = minimal();
    with_schema["$schema"] = json!("./schemas/pipeline.schema.json");
    let config: PipelineConfig = serde_json::from_value(with_schema).unwrap();
    assert_eq!(
        config.schema.as_deref(),
        Some("./schemas/pipeline.schema.json")
    );
}

#[test]
fn source_patterns_match_exactly_by_prefix_or_everything() {
    assert!(SourcePolicy::new("channel:telegram").matches("channel:telegram"));
    assert!(!SourcePolicy::new("channel:telegram").matches("channel:discord"));
    assert!(SourcePolicy::new("channel:*").matches("channel:discord"));
    assert!(!SourcePolicy::new("channel:*").matches("webhook:x"));
    assert!(SourcePolicy::new("*").matches("anything"));
}

#[test]
fn the_first_matching_source_policy_wins() {
    let mut value = minimal();
    value["sources"] = json!([
        {"match": "channel:telegram", "ack": "after_turn"},
        {"match": "channel:*", "ack": "after_dispatch"}
    ]);
    let config: PipelineConfig = serde_json::from_value(value).unwrap();
    assert_eq!(
        config.source_policy("channel:telegram").ack,
        AckPolicy::AfterTurn
    );
    assert_eq!(
        config.source_policy("channel:slack").ack,
        AckPolicy::AfterDispatch
    );
    assert_eq!(
        config.source_policy("webhook:x").ack,
        AckPolicy::AfterIngest
    );
}

#[test]
fn lanes_have_stable_wire_names() {
    for lane in Lane::ALL {
        assert_eq!(serde_json::to_value(lane).unwrap(), json!(lane.as_str()));
    }
}

#[test]
fn gate_and_coalesce_sections_default_when_absent() {
    let config: PipelineConfig = serde_json::from_value(minimal()).unwrap();
    assert!(config.gate.direct_messages && config.gate.groups);
    assert_eq!(config.gate.bot_loop.max_events, 20);
    assert_eq!(config.gate.bot_loop.window_ms, 60_000);
    assert_eq!(config.gate.ambient.max_items, 50);
    assert_eq!(config.gate.rate_limits.per_sender, None);
    assert_eq!(config.coalesce.debounce_ms, 1_500);
    assert_eq!(config.coalesce.max_wait_ms, 10_000);
    assert_eq!(config.coalesce.control_prefixes, ["/"]);
}

#[test]
fn rate_limits_round_trip_and_reject_unknown_fields() {
    let mut value = minimal();
    value["gate"] =
        json!({"rate_limits": {"per_source": {"capacity": 5, "refill_per_minute": 60}}});
    let config: PipelineConfig = serde_json::from_value(value).unwrap();
    assert_eq!(
        config.gate.rate_limits.per_source,
        Some(super::RateLimit::new(5, 60))
    );
    let encoded = serde_json::to_value(&config).unwrap();
    assert_eq!(
        serde_json::from_value::<PipelineConfig>(encoded).unwrap(),
        config
    );

    let mut typo = minimal();
    typo["coalesce"] = json!({"debounce": 5});
    assert!(serde_json::from_value::<PipelineConfig>(typo).is_err());
}

#[test]
fn salience_and_digest_sections_default_when_absent() {
    let config: PipelineConfig = serde_json::from_value(minimal()).unwrap();
    let salience = &config.salience;
    assert_eq!(salience.weights.base, 25);
    assert_eq!(salience.weights.addressed, 50);
    assert_eq!(salience.weights.direct, 50);
    assert_eq!(salience.weights.known_contact, 15);
    assert_eq!(salience.weights.keyword, 20);
    assert_eq!(salience.weights.age_penalty_per_minute, 1);
    assert_eq!(salience.weights.max_age_penalty, 20);
    assert_eq!(salience.thresholds.interactive, 70);
    assert_eq!(salience.thresholds.actionable, 40);
    assert_eq!(salience.thresholds.background, 15);
    assert_eq!(salience.borderline, None);
    assert_eq!(salience.habituation.window, 64);
    assert_eq!(salience.habituation.window_ms, 3_600_000);
    assert_eq!(salience.habituation.max_distance, 3);
    assert_eq!(salience.habituation.decay_percent, 40);
    assert_eq!(config.digest.max_items, 200);
    assert_eq!(config.digest.max_wait_ms, 600_000);
    assert_eq!(config.lanes.get(Lane::Digest).max_backlog, 1_000);
    assert_eq!(config.source_policy("x").weight, 0);
}

#[test]
fn a_borderline_band_round_trips() {
    let mut value = minimal();
    value["salience"] = json!({"borderline": {"low": 30, "high": 50, "budget_per_hour": 60}});
    let config: PipelineConfig = serde_json::from_value(value).unwrap();
    assert_eq!(
        config.salience.borderline,
        Some(super::Borderline::new(30, 50, 60))
    );
}

#[test]
fn cadences_and_budgets_are_optional() {
    let config: PipelineConfig = serde_json::from_value(minimal()).unwrap();
    assert_eq!(config.lanes.get(Lane::Background).cadence, None);
    assert_eq!(config.source_policy("x").budget, None);
    let mut value = minimal();
    value["lanes"]["background"]["cadence"] = json!("every(10m)");
    value["lanes"]["background"]["budget"] = json!({"max_turns": 5});
    let config: PipelineConfig = serde_json::from_value(value).unwrap();
    let background = config.lanes.get(Lane::Background);
    assert_eq!(background.cadence.as_deref(), Some("every(10m)"));
    assert_eq!(background.budget, Some(super::Budget::new(5)));
    let budget = background.budget.unwrap();
    assert_eq!(
        (budget.backlog_ticks, budget.degrade_debounce_ms),
        (3, 5_000)
    );
}

#[test]
fn dispatch_defaults_when_absent() {
    let config: PipelineConfig = serde_json::from_value(minimal()).unwrap();
    assert_eq!(config.dispatch.backoff_base_ms, 1_000);
    assert_eq!(config.dispatch.backoff_max_ms, 300_000);
    assert_eq!(config.dispatch.gap_timeout_ms, 120_000);
}

#[test]
fn a_source_may_carry_its_own_debounce() {
    let mut value = minimal();
    value["sources"] = json!([
        {"match": "x:mentions", "coalesce": {"debounce_ms": 30_000, "max_wait_ms": 300_000, "max_events": 20}},
        {"match": "x:*", "coalesce": {"max_events": 5}},
        {"match": "*"}
    ]);
    let config: PipelineConfig = serde_json::from_value(value).unwrap();
    let own = config.source_policy("x:mentions").coalesce.unwrap();
    assert_eq!(
        (own.debounce_ms, own.max_wait_ms, own.max_events),
        (Some(30_000), Some(300_000), Some(20))
    );
    let partial = config.source_policy("x:dms").coalesce.unwrap();
    assert_eq!(
        (partial.debounce_ms, partial.max_wait_ms, partial.max_events),
        (None, None, Some(5))
    );
    assert_eq!(config.source_policy("web:chat").coalesce, None);
    assert_eq!(SourcePolicy::default().coalesce, None);
    let encoded = serde_json::to_value(&config).unwrap();
    assert!(encoded["sources"][2].get("coalesce").is_none());
    assert_eq!(
        serde_json::from_value::<PipelineConfig>(encoded).unwrap(),
        config
    );

    let mut typo = minimal();
    typo["sources"] = json!([{"match": "x:*", "coalesce": {"debounce": 5}}]);
    assert!(serde_json::from_value::<PipelineConfig>(typo).is_err());
}

#[test]
fn a_source_debounce_builds_from_its_settings() {
    let own = SourceCoalesce::new()
        .debounce_ms(10)
        .max_wait_ms(50)
        .max_events(3);
    assert_eq!(
        (own.debounce_ms, own.max_wait_ms, own.max_events),
        (Some(10), Some(50), Some(3))
    );
    assert_eq!(SourceCoalesce::default(), SourceCoalesce::new());
}
