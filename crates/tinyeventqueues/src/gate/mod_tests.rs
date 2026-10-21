//! Unit tests for the gate, its buckets and the ambient buffer.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use tinyeventqueues_bus::{
    AmbientConfig, EventKind, GateConfig, InboundEvent, RateLimit, Reason, Sender,
};

use super::buckets::Buckets;
use super::*;
use crate::test_support::{Fixture, event};

fn addressed(id: &str) -> InboundEvent {
    event("channel:slack", id, "general").addressed(true)
}

async fn gate(fixture: &Fixture) -> Gate {
    crate::store::ensure(fixture.docs.as_ref()).await.unwrap();
    Gate::new(&fixture.docs)
}

#[tokio::test]
async fn self_echo_and_sender_lists_drop_with_their_reason() {
    let fixture = Fixture::new();
    let gate = gate(&fixture).await;
    let mut config = GateConfig::default();
    let mut echo = addressed("1");
    echo.sender = Sender::new("agent").as_self();
    assert_eq!(
        gate.check(&echo, &config, 0).await.unwrap(),
        Verdict::Drop(Reason::SelfEcho)
    );

    config.deny_senders = vec!["channel:slack/user-1".into()];
    assert_eq!(
        gate.check(&addressed("2"), &config, 0).await.unwrap(),
        Verdict::Drop(Reason::SenderDenied)
    );
    config.deny_senders = vec!["channel:discord/user-1".into()];
    assert_eq!(
        gate.check(&addressed("3"), &config, 0).await.unwrap(),
        Verdict::Pass
    );

    config.allow_senders = vec!["someone-else".into()];
    assert_eq!(
        gate.check(&addressed("4"), &config, 0).await.unwrap(),
        Verdict::Drop(Reason::SenderNotAllowed)
    );
    config.allow_senders = vec!["user-1".into()];
    assert_eq!(
        gate.check(&addressed("5"), &config, 0).await.unwrap(),
        Verdict::Pass
    );
}

#[tokio::test]
async fn direct_and_group_policy_apply() {
    let fixture = Fixture::new();
    let gate = gate(&fixture).await;
    let mut config = GateConfig::default();
    config.direct_messages = false;
    let dm = event("channel:telegram", "1", "dm").direct(true);
    assert_eq!(
        gate.check(&dm, &config, 0).await.unwrap(),
        Verdict::Drop(Reason::DirectMessagesDisabled)
    );
    let mut config = GateConfig::default();
    config.groups = false;
    assert_eq!(
        gate.check(&addressed("2"), &config, 0).await.unwrap(),
        Verdict::Drop(Reason::GroupsDisabled)
    );
    assert_eq!(gate.check(&dm, &config, 0).await.unwrap(), Verdict::Pass);
}

#[tokio::test]
async fn unaddressed_group_chatter_is_context_but_triggers_pass() {
    let fixture = Fixture::new();
    let gate = gate(&fixture).await;
    let config = GateConfig::default();
    let chatter = event("channel:slack", "1", "general");
    assert_eq!(
        gate.check(&chatter, &config, 0).await.unwrap(),
        Verdict::Context
    );
    for kind in [
        EventKind::Mention,
        EventKind::Trigger,
        EventKind::Notification,
    ] {
        let mut aimed = event("composio:twitter", "2", "feed");
        aimed.kind = kind;
        assert_eq!(gate.check(&aimed, &config, 0).await.unwrap(), Verdict::Pass);
    }
}

#[tokio::test]
async fn a_looping_bot_is_cut_off_until_its_window_passes() {
    let fixture = Fixture::new();
    let gate = gate(&fixture).await;
    let mut config = GateConfig::default();
    config.bot_loop.max_events = 3;
    config.bot_loop.window_ms = 1_000;
    let from_bot = |id: &str| {
        let mut event = addressed(id);
        event.sender = Sender::new("echo-bot").as_bot();
        event
    };
    for i in 0..3 {
        assert_eq!(
            gate.check(&from_bot(&i.to_string()), &config, 100)
                .await
                .unwrap(),
            Verdict::Pass
        );
    }
    assert_eq!(
        gate.check(&from_bot("x"), &config, 500).await.unwrap(),
        Verdict::Drop(Reason::BotLoop)
    );
    // A restart does not reset the window.
    let restarted = Gate::new(&fixture.docs);
    assert_eq!(
        restarted.check(&from_bot("y"), &config, 900).await.unwrap(),
        Verdict::Drop(Reason::BotLoop)
    );
    assert_eq!(
        restarted
            .check(&from_bot("z"), &config, 1_100)
            .await
            .unwrap(),
        Verdict::Pass
    );
}

#[tokio::test]
async fn token_buckets_spill_over_budget_and_refill_across_restarts() {
    let fixture = Fixture::new();
    let gate = gate(&fixture).await;
    let mut config = GateConfig::default();
    config.rate_limits.per_sender = Some(RateLimit::new(2, 60));
    config.rate_limits.per_conversation = Some(RateLimit::new(100, 60));
    config.rate_limits.per_source = Some(RateLimit::new(100, 60));
    assert_eq!(
        gate.check(&addressed("1"), &config, 0).await.unwrap(),
        Verdict::Pass
    );
    assert_eq!(
        gate.check(&addressed("2"), &config, 0).await.unwrap(),
        Verdict::Pass
    );
    assert_eq!(
        gate.check(&addressed("3"), &config, 0).await.unwrap(),
        Verdict::Spill(Reason::RateLimited)
    );
    let restarted = Gate::new(&fixture.docs);
    assert_eq!(
        restarted
            .check(&addressed("4"), &config, 999)
            .await
            .unwrap(),
        Verdict::Spill(Reason::RateLimited)
    );
    // One token a second.
    assert_eq!(
        restarted
            .check(&addressed("5"), &config, 1_999)
            .await
            .unwrap(),
        Verdict::Pass
    );
}

#[test]
fn buckets_refill_proportionally_and_cap_at_capacity() {
    use super::buckets::refilled;
    let limit = RateLimit::new(5, 60);
    assert_eq!(refilled(0, 1_000, limit), 1_000);
    assert_eq!(refilled(0, 500, limit), 500);
    assert_eq!(refilled(4_500, 10_000, limit), 5_000);
    assert_eq!(
        refilled(0, u64::MAX, RateLimit::new(u32::MAX, u32::MAX)),
        u64::from(u32::MAX) * 1_000
    );
}

#[tokio::test]
async fn bucket_state_ignores_a_clock_that_moves_backwards() {
    let fixture = Fixture::new();
    crate::store::ensure(fixture.docs.as_ref()).await.unwrap();
    let buckets = Buckets::new(&fixture.docs);
    let limit = RateLimit::new(1, 60);
    assert!(
        buckets
            .take("sender", "s", "u", limit, 5_000)
            .await
            .unwrap()
    );
    assert!(
        !buckets
            .take("sender", "s", "u", limit, 4_000)
            .await
            .unwrap()
    );
    assert!(
        buckets
            .take("sender", "s", "u", limit, 6_000)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn ambient_context_is_bounded_deduplicated_and_released() {
    let fixture = Fixture::new();
    crate::store::ensure(fixture.docs.as_ref()).await.unwrap();
    let ambient = Ambient::new(&fixture.docs);
    let config = AmbientConfig::default();
    let small =
        serde_json::from_value::<AmbientConfig>(serde_json::json!({"max_items": 2})).unwrap();
    for id in ["1", "2", "3"] {
        ambient.keep(&event("s", id, "c"), &small).await.unwrap();
    }
    ambient.keep(&event("s", "3", "c"), &config).await.unwrap();
    let kept = ambient.peek("s", "c").await.unwrap();
    let ids: Vec<_> = kept.iter().map(|e| e.event_id.as_str()).collect();
    assert_eq!(ids, ["2", "3"]);
    ambient
        .release("s", "c", &[kept[0].reference()])
        .await
        .unwrap();
    ambient.release("s", "c", &[]).await.unwrap();
    ambient
        .release("s", "other", &[kept[0].reference()])
        .await
        .unwrap();
    assert_eq!(ambient.peek("s", "c").await.unwrap().len(), 1);
    assert_eq!(ambient.peek("s", "none").await.unwrap().len(), 0);
}
