//! The digest accumulator: events that spill (over a rate limit, a full lane)
//! or score as noise gather here, and go out as one digest turn on the
//! digest lane once there are `max_items` of them or the first has waited
//! `max_wait_ms`. The host summarizes a digest turn instead of answering each
//! event, the way a person skims a backlog.

use std::sync::Arc;

use crate::ports::DocumentStore;
use serde::{Deserialize, Serialize};
use tinyeventqueues_bus::{DigestConfig, EventRef, InboundEvent, Lane, Reason};

use crate::Result;
use crate::store::DIGEST;
use crate::store::Repo;
use crate::turn::PendingTurn;

/// The id of the one accumulator document.
const ID: &str = "digest";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Accumulated {
    events: Vec<InboundEvent>,
    #[serde(default)]
    absorbed: Vec<EventRef>,
    first_ms: u64,
}

impl Accumulated {
    fn holds(&self, event: &EventRef) -> bool {
        self.events.iter().any(|e| e.reference() == *event) || self.absorbed.contains(event)
    }

    fn is_due(&self, config: &DigestConfig, now: u64) -> bool {
        let count = u64::try_from(self.events.len()).unwrap_or(u64::MAX);
        !self.events.is_empty()
            && (count >= u64::from(config.max_items)
                || now >= self.first_ms.saturating_add(config.max_wait_ms))
    }
}

/// The digest accumulator over one document store.
#[derive(Debug, Clone)]
pub(crate) struct Digest {
    repo: Repo<Accumulated>,
}

impl Digest {
    pub(crate) fn new(docs: &Arc<dyn DocumentStore>) -> Self {
        Self {
            repo: Repo::new(docs, DIGEST),
        }
    }

    /// Add `events` (and the `absorbed` events that share their fate).
    /// Adding an event again is a no-op.
    pub(crate) async fn add(
        &self,
        events: &[InboundEvent],
        absorbed: &[EventRef],
        now: u64,
    ) -> Result<()> {
        self.repo
            .update(ID, |current| {
                let mut digest = current.unwrap_or_default();
                if digest.events.is_empty() {
                    digest.first_ms = now;
                }
                let before = (digest.events.len(), digest.absorbed.len());
                for event in events {
                    if !digest.holds(&event.reference()) {
                        digest.events.push(event.clone());
                    }
                }
                for event in absorbed {
                    if !digest.holds(event) {
                        digest.absorbed.push(event.clone());
                    }
                }
                Ok((before != (digest.events.len(), digest.absorbed.len())).then_some(digest))
            })
            .await?;
        Ok(())
    }

    /// The digest turn due at `now`, if any. The caller enqueues it, then
    /// [`release`](Self::release)s it.
    pub(crate) async fn due(&self, config: &DigestConfig, now: u64) -> Result<Option<PendingTurn>> {
        let Some(found) = self.repo.get(ID).await? else {
            return Ok(None);
        };
        let digest = found.doc;
        if !digest.is_due(config, now) {
            return Ok(None);
        }
        let take = usize::try_from(config.max_items).unwrap_or(usize::MAX);
        let events: Vec<InboundEvent> = digest.events.into_iter().take(take).collect();
        let absorbed = digest.absorbed;
        let mut turn = PendingTurn::new(Lane::Digest, Reason::Turn, events, absorbed, now)?;
        turn.conversation_key.clear();
        turn.thread_key = None;
        Ok(Some(turn))
    }

    /// Forget what `turn` carried.
    pub(crate) async fn release(&self, turn: &PendingTurn, now: u64) -> Result<()> {
        let taken: Vec<EventRef> = turn
            .events
            .iter()
            .map(InboundEvent::reference)
            .chain(turn.absorbed.iter().cloned())
            .collect();
        self.repo
            .update(ID, |current| {
                let Some(mut digest) = current else {
                    return Ok(None);
                };
                digest.events.retain(|e| !taken.contains(&e.reference()));
                digest.absorbed.retain(|e| !taken.contains(e));
                digest.first_ms = now;
                Ok(Some(digest))
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
