//! The gate: deterministic checks that decide, without a model, whether an
//! event may go on towards the agent.
//!
//! In order, first verdict wins:
//!
//! 1. the agent's own echo → dropped (`self_echo`);
//! 2. the deny list → dropped (`sender_denied`); a non-empty allow list that
//!    does not name the sender → dropped (`sender_not_allowed`);
//! 3. direct-message and group policy → dropped
//!    (`direct_messages_disabled`, `groups_disabled`);
//! 4. the bot-loop guard: a bot sending more than `max_events` into one
//!    conversation within `window_ms` → dropped (`bot_loop`);
//! 5. group chatter not addressed to the agent → [`Verdict::Context`]: kept as
//!    ambient context for the conversation's next real turn;
//! 6. token buckets per sender, conversation and source → over any of them,
//!    [`Verdict::Spill`] to the digest (`rate_limited`), never dropped.
//!
//! The bot-loop windows and the token buckets live in the document store, so
//! a restart neither resets a flood's budget nor forgets a loop.

mod ambient;
mod buckets;

use std::sync::Arc;

use crate::ports::DocumentStore;
use tinyeventqueues_bus::{EventKind, GateConfig, InboundEvent, RateLimit, Reason};

use crate::Result;
use crate::salience::names_sender;

pub(crate) use ambient::Ambient;
use buckets::Buckets;

/// What the gate decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Go on to coalescing.
    Pass,
    /// Keep as ambient context.
    Context,
    /// Spill to the digest, with why.
    Spill(Reason),
    /// Drop, with why.
    Drop(Reason),
}

/// Chat traffic that only matters to the agent when aimed at it. Triggers and
/// notifications are addressed to the agent by nature.
fn is_chatter(event: &InboundEvent) -> bool {
    !event.is_direct
        && !event.addressed_to_agent
        && !matches!(
            event.kind,
            EventKind::Mention | EventKind::Trigger | EventKind::Notification
        )
}

/// The gate over one document store.
#[derive(Debug, Clone)]
pub(crate) struct Gate {
    buckets: Buckets,
}

impl Gate {
    pub(crate) fn new(docs: &Arc<dyn DocumentStore>) -> Self {
        Self {
            buckets: Buckets::new(docs),
        }
    }

    /// Take one host classification from an hourly budget of
    /// `budget_per_hour`. Returns `false` when the budget is spent.
    pub(crate) async fn classify_budget(&self, budget_per_hour: u32, now: u64) -> Result<bool> {
        if budget_per_hour == 0 {
            return Ok(false);
        }
        let limit = RateLimit::new(budget_per_hour, (budget_per_hour / 60).max(1));
        self.buckets.take("classify", "", "", limit, now).await
    }

    /// Take one turn from an instant cadence's per-second budget. Returns
    /// `false` when the cadence is over budget.
    pub(crate) async fn turn_budget(&self, key: &str, limit: RateLimit, now: u64) -> Result<bool> {
        self.buckets.take("cadence", "", key, limit, now).await
    }

    /// Judge `event` under `config` at `now`.
    pub(crate) async fn check(
        &self,
        event: &InboundEvent,
        config: &GateConfig,
        now: u64,
    ) -> Result<Verdict> {
        if event.sender.is_self {
            return Ok(Verdict::Drop(Reason::SelfEcho));
        }
        if names_sender(&config.deny_senders, event) {
            return Ok(Verdict::Drop(Reason::SenderDenied));
        }
        if !config.allow_senders.is_empty() && !names_sender(&config.allow_senders, event) {
            return Ok(Verdict::Drop(Reason::SenderNotAllowed));
        }
        if event.is_direct && !config.direct_messages {
            return Ok(Verdict::Drop(Reason::DirectMessagesDisabled));
        }
        if !event.is_direct && !config.groups {
            return Ok(Verdict::Drop(Reason::GroupsDisabled));
        }
        if event.sender.is_bot && self.buckets.bot_loop(event, &config.bot_loop, now).await? {
            return Ok(Verdict::Drop(Reason::BotLoop));
        }
        if is_chatter(event) {
            return Ok(Verdict::Context);
        }
        let limits = &config.rate_limits;
        let scopes = [
            ("sender", limits.per_sender, event.sender.id.as_str()),
            (
                "conversation",
                limits.per_conversation,
                event.conversation_key.as_str(),
            ),
            ("source", limits.per_source, ""),
        ];
        let mut limited = false;
        for (scope, limit, key) in scopes {
            if let Some(limit) = limit {
                limited |= !self
                    .buckets
                    .take(scope, &event.source, key, limit, now)
                    .await?;
            }
        }
        Ok(if limited {
            Verdict::Spill(Reason::RateLimited)
        } else {
            Verdict::Pass
        })
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
