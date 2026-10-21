//! Dead letters: parking work that failed for good, listing it, and putting
//! it back.
//!
//! Work dies two ways. A stage that gives up on a message *parks* it: the
//! message is copied into the dead-letter queue the configuration names as a
//! [`Parked`] record, with where it came from and why, and acknowledged at its
//! origin. And a message
//! whose leases kept lapsing (a consumer crashing on it) is dead-lettered by
//! its own queue once it runs out of manifest `max_attempts`. Both are listed
//! by [`DeadLetters::list`] with an id of the form `<queue>/<message id>`, and
//! both come back with [`DeadLetters::redrive`].
//!
//! A parked message is enqueued ready and then *settled*: the stage receives
//! it from the dead-letter queue and nacks it with `Retry::DeadLetter`, which
//! leaves it in the queue's dead state where `dead_letters` lists it. A redrive
//! copies the parked payload back to its origin, marks the parked id as
//! redriven, and redrives it in the dead-letter queue; the next settle sees the
//! marker and acknowledges it away instead of parking it again.

mod types;

use std::sync::Arc;

use crate::ports::{DeadLetter, DocumentStore, NewMessage, Precondition, QueueStore, Retry};
use serde_json::Value;
use tinyeventqueues_bus::{DeadItem, Disposition, EventRef, Outcome, Reason};

use crate::config::{QueueRef, ResolvedQueues};
use crate::store::{Ledger, REDRIVEN};
use crate::{Error, Result};

pub(crate) use types::Parked;

/// The most dead letters one listing or sweep reads per queue.
const SCAN_LIMIT: usize = 10_000;

/// How many parked messages one settle receive takes.
const SETTLE_BATCH: u32 = 64;

/// The events a queued payload carries: an ingress envelope's one event, or
/// every event of a turn.
pub(crate) fn payload_events(payload: &Value) -> Vec<EventRef> {
    let refer = |event: &Value| {
        Some(EventRef::new(
            event.get("source")?.as_str()?,
            event.get("event_id")?.as_str()?,
        ))
    };
    if let Some(event) = payload.get("event") {
        return refer(event).into_iter().collect();
    }
    ["events", "absorbed"]
        .iter()
        .filter_map(|field| payload.get(field).and_then(Value::as_array))
        .flat_map(|events| events.iter().filter_map(refer))
        .collect()
}

/// The dead-letter handling of one pipeline.
#[derive(Debug, Clone)]
pub(crate) struct DeadLetters {
    queues: Arc<dyn QueueStore>,
    docs: Arc<dyn DocumentStore>,
    ledger: Ledger,
    consumer: String,
}

impl DeadLetters {
    pub(crate) fn new(
        queues: Arc<dyn QueueStore>,
        docs: Arc<dyn DocumentStore>,
        ledger: Ledger,
        consumer: String,
    ) -> Self {
        Self {
            queues,
            docs,
            ledger,
            consumer,
        }
    }

    /// Park `parked` in `dead` and record its events as dead. The caller
    /// acknowledges the origin message afterwards.
    pub(crate) async fn park(
        &self,
        dead: &QueueRef,
        parked: Parked,
        reason: Reason,
        now: u64,
        ttl_ms: u64,
    ) -> Result<()> {
        let events = parked.events.clone();
        let turn = parked.turn_id.clone();
        self.queues
            .enqueue(
                &dead.name,
                &[NewMessage::new(serde_json::to_value(parked)?)],
            )
            .await?;
        for event in events {
            let mut outcome = Outcome::new(event, Disposition::Dead, reason, now);
            outcome.turn_id.clone_from(&turn);
            self.ledger.record(outcome, ttl_ms).await?;
        }
        self.settle(dead).await?;
        Ok(())
    }

    /// Move every ready message of the dead-letter queue to its final state:
    /// dead, or gone if it was redriven.
    pub(crate) async fn settle(&self, dead: &QueueRef) -> Result<u64> {
        let mut settled = 0;
        loop {
            let batch = self
                .queues
                .receive(&dead.name, SETTLE_BATCH, dead.visibility_ms, &self.consumer)
                .await?;
            if batch.is_empty() {
                return Ok(settled);
            }
            for delivery in batch {
                let id = &delivery.receipt.id;
                if self.docs.get(REDRIVEN, id).await?.is_some() {
                    self.queues.ack(&dead.name, &delivery.receipt).await?;
                    self.docs.delete(REDRIVEN, id, Precondition::None).await?;
                } else {
                    let reason = serde_json::from_value::<Parked>(delivery.payload)
                        .map_or_else(|_| "unreadable parked message".to_owned(), |p| p.reason);
                    self.queues
                        .nack(&dead.name, &delivery.receipt, Retry::DeadLetter(reason))
                        .await?;
                }
                settled += 1;
            }
        }
    }

    /// Record a dead outcome for every event of every message a pipeline
    /// queue dead-lettered on its own (leases that kept lapsing). Returns how
    /// many outcomes were new.
    pub(crate) async fn sweep(
        &self,
        resolved: &ResolvedQueues,
        now: u64,
        ttl_ms: u64,
    ) -> Result<u64> {
        let mut recorded = 0;
        for queue in resolved.all() {
            if queue.name == resolved.dead.name {
                continue;
            }
            for letter in self.queues.dead_letters(&queue.name, SCAN_LIMIT).await? {
                for event in payload_events(&letter.payload) {
                    let outcome = Outcome::new(event, Disposition::Dead, Reason::MaxAttempts, now);
                    if self.ledger.record(outcome, ttl_ms).await? {
                        recorded += 1;
                    }
                }
            }
        }
        Ok(recorded)
    }

    /// Up to `limit` dead items: parked work first, then what each pipeline
    /// queue dead-lettered on its own.
    pub(crate) async fn list(
        &self,
        resolved: &ResolvedQueues,
        limit: usize,
    ) -> Result<Vec<DeadItem>> {
        let mut items = Vec::new();
        for letter in self.queues.dead_letters(&resolved.dead.name, limit).await? {
            items.push(parked_item(&resolved.dead.name, letter));
        }
        for queue in resolved.all() {
            if items.len() >= limit {
                break;
            }
            if queue.name == resolved.dead.name {
                continue;
            }
            let room = limit - items.len();
            for letter in self.queues.dead_letters(&queue.name, room).await? {
                items.push(DeadItem::new(
                    format!("{}/{}", queue.name, letter.id),
                    queue.name.clone(),
                    letter.reason.clone(),
                    letter.attempt,
                    payload_events(&letter.payload),
                ));
            }
        }
        items.truncate(limit);
        Ok(items)
    }

    /// Put the dead item `id` back where it came from. Returns `false` when
    /// no dead item has that id.
    pub(crate) async fn redrive(&self, resolved: &ResolvedQueues, id: &str) -> Result<bool> {
        let Some((queue, message)) = id.split_once('/') else {
            return Err(Error::UnknownDeadItem(id.to_owned()));
        };
        if resolved.by_name(queue).is_none() {
            return Err(Error::UnknownDeadItem(id.to_owned()));
        }
        if queue != resolved.dead.name {
            let Some(letter) = self.find(queue, message).await? else {
                return Ok(false);
            };
            self.forget(&payload_events(&letter.payload)).await?;
            return Ok(self.queues.redrive(queue, message).await?);
        }
        let Some(letter) = self.find(queue, message).await? else {
            return Ok(false);
        };
        let parked: Parked = serde_json::from_value(letter.payload)?;
        if resolved.by_name(&parked.origin).is_none() {
            return Err(Error::UnknownDeadItem(id.to_owned()));
        }
        let mut copy = NewMessage::new(parked.payload.clone());
        if self.queues.features().groups {
            copy.group.clone_from(&parked.group);
        }
        self.queues.enqueue(&parked.origin, &[copy]).await?;
        self.forget(&parked.events).await?;
        self.docs
            .put(REDRIVEN, message, serde_json::json!({}), Precondition::None)
            .await?;
        self.queues.redrive(queue, message).await?;
        self.settle(&resolved.dead).await?;
        Ok(true)
    }

    async fn find(&self, queue: &str, id: &str) -> Result<Option<DeadLetter>> {
        Ok(self
            .queues
            .dead_letters(queue, SCAN_LIMIT)
            .await?
            .into_iter()
            .find(|letter| letter.id == id))
    }

    async fn forget(&self, events: &[EventRef]) -> Result<()> {
        for event in events {
            self.ledger.forget(event).await?;
        }
        Ok(())
    }
}

fn parked_item(dead_queue: &str, letter: DeadLetter) -> DeadItem {
    let id = format!("{dead_queue}/{}", letter.id);
    match serde_json::from_value::<Parked>(letter.payload) {
        Ok(parked) => DeadItem::new(
            id,
            parked.origin,
            parked.reason,
            parked.attempt,
            parked.events,
        ),
        Err(_) => DeadItem::new(id, dead_queue, letter.reason, letter.attempt, Vec::new()),
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
