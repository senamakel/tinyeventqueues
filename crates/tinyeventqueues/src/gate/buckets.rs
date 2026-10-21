//! Durable token buckets and bot-loop windows.

use std::sync::Arc;

use crate::ports::{DocumentStore, key_of};
use serde::{Deserialize, Serialize};
use tinyeventqueues_bus::{BotLoopConfig, InboundEvent, RateLimit};

use crate::Result;
use crate::store::BUCKETS;
use crate::store::Repo;

/// One token, in the milli-tokens buckets count in.
const TOKEN: u64 = 1_000;

/// A token bucket's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Bucket {
    milli_tokens: u64,
    updated_ms: u64,
}

/// A fixed counting window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Window {
    start_ms: u64,
    count: u32,
}

/// The milli-tokens a bucket holds after `elapsed_ms` of refill from `held`.
pub(super) fn refilled(held: u64, elapsed_ms: u64, limit: RateLimit) -> u64 {
    let capacity = u64::from(limit.capacity) * TOKEN;
    // tokens/minute * TOKEN milli-tokens / 60_000 ms = refill/60 per ms.
    let added = u128::from(elapsed_ms) * u128::from(limit.refill_per_minute) / 60;
    let total = u128::from(held) + added;
    u64::try_from(total.min(u128::from(capacity))).unwrap_or(capacity)
}

#[derive(Debug, Clone)]
pub(super) struct Buckets {
    buckets: Repo<Bucket>,
    windows: Repo<Window>,
}

impl Buckets {
    pub(super) fn new(docs: &Arc<dyn DocumentStore>) -> Self {
        Self {
            buckets: Repo::new(docs, BUCKETS),
            windows: Repo::new(docs, BUCKETS),
        }
    }

    /// Take one token from the `scope` bucket of `key` within `source`.
    /// Returns `false` when the bucket is empty; the refill is kept either way.
    pub(super) async fn take(
        &self,
        scope: &str,
        source: &str,
        key: &str,
        limit: RateLimit,
        now: u64,
    ) -> Result<bool> {
        let id = key_of(&["bucket", scope, source, key])?;
        let mut granted = false;
        self.buckets
            .update(id.as_str(), |current| {
                let capacity = u64::from(limit.capacity) * TOKEN;
                let (held, last) =
                    current.map_or((capacity, now), |b| (b.milli_tokens, b.updated_ms));
                let mut tokens = refilled(held, now.saturating_sub(last), limit);
                granted = tokens >= TOKEN;
                if granted {
                    tokens -= TOKEN;
                }
                Ok(Some(Bucket {
                    milli_tokens: tokens,
                    updated_ms: now.max(last),
                }))
            })
            .await?;
        Ok(granted)
    }

    /// Count one event from a bot into its conversation, and report whether
    /// the bot is now over the loop guard.
    pub(super) async fn bot_loop(
        &self,
        event: &InboundEvent,
        config: &BotLoopConfig,
        now: u64,
    ) -> Result<bool> {
        let id = key_of(&[
            "botloop",
            event.source.as_str(),
            event.conversation_key.as_str(),
            event.sender.id.as_str(),
        ])?;
        let mut count = 0;
        self.windows
            .update(id.as_str(), |current| {
                let window = match current {
                    Some(window) if now.saturating_sub(window.start_ms) < config.window_ms => {
                        Window {
                            count: window.count.saturating_add(1),
                            ..window
                        }
                    }
                    _ => Window {
                        start_ms: now,
                        count: 1,
                    },
                };
                count = window.count;
                Ok(Some(window))
            })
            .await?;
        Ok(count > config.max_events)
    }
}
