//! The ingress stage: validate an event, persist it with its raw payload, and
//! recognize redeliveries.
//!
//! `Ingest` returns only after the durable write, so the caller acknowledges
//! the platform (a webhook's 200, a poll cursor's advance) after the event is
//! safe. Each event is enqueued on the ingress queue with the dedupe key
//! `source:event_id`:
//!
//! - when the queue backend dedupes (`QueueFeatures::dedupe`), the queue
//!   itself rejects a redelivery while the event is held and for its manifest
//!   `dedupe_window_ms` after it was processed — the completion tombstone that
//!   survives a restart because it lives in the queue's storage;
//! - when it does not, the stage keeps its own tombstones in the document
//!   store, written *after* the enqueue, and also consults the outcome ledger.
//!   A crash between the two writes can let one redelivery through (at least
//!   once); it can never lose an event.

mod types;

use std::collections::HashSet;
use std::sync::Arc;

use crate::ports::{NewMessage, QueueStore};
use tinyeventqueues_bus::{
    AckPolicy, Disposition, InboundEvent, IngestResponse, IngestResult, Outcome, PipelineConfig,
    Reason,
};

use crate::Result;
use crate::config::QueueRef;
use crate::store::{Ledger, Tombstones};

pub(crate) use types::IngressEnvelope;

/// The longest routing key (`event_id`, `source`, `conversation_key`,
/// `thread_key`, sender id) the stage accepts, in bytes.
pub const MAX_KEY_LEN: usize = 512;

/// Check an envelope's routing keys.
///
/// Returns the name of the first field that is empty (where required) or
/// longer than [`MAX_KEY_LEN`].
#[must_use]
pub fn invalid_field(event: &InboundEvent) -> Option<&'static str> {
    let required = [
        ("event_id", event.event_id.as_str()),
        ("source", event.source.as_str()),
        ("conversation_key", event.conversation_key.as_str()),
        ("sender.id", event.sender.id.as_str()),
    ];
    for (field, value) in required {
        if value.is_empty() || value.len() > MAX_KEY_LEN {
            return Some(field);
        }
    }
    match &event.thread_key {
        Some(thread) if thread.is_empty() || thread.len() > MAX_KEY_LEN => Some("thread_key"),
        _ => None,
    }
}

/// The ingress stage over one queue.
#[derive(Debug, Clone)]
pub(crate) struct Ingress {
    queues: Arc<dyn QueueStore>,
    tombstones: Tombstones,
    ledger: Ledger,
}

impl Ingress {
    pub(crate) fn new(queues: Arc<dyn QueueStore>, tombstones: Tombstones, ledger: Ledger) -> Self {
        Self {
            queues,
            tombstones,
            ledger,
        }
    }

    /// Persist `events` on `queue` and report what happened to each.
    pub(crate) async fn ingest(
        &self,
        events: Vec<InboundEvent>,
        queue: &QueueRef,
        config: &PipelineConfig,
        now: u64,
    ) -> Result<IngestResponse> {
        let native_dedupe = self.queues.features().dedupe;
        let mut results: Vec<Option<IngestResult>> = vec![None; events.len()];
        let mut pending: Vec<(usize, InboundEvent)> = Vec::new();
        let mut seen = HashSet::new();
        for (index, event) in events.into_iter().enumerate() {
            if invalid_field(&event).is_some() {
                let outcome = Outcome::new(
                    event.reference(),
                    Disposition::Dropped,
                    Reason::InvalidEnvelope,
                    now,
                );
                self.ledger.notify(&outcome);
                results[index] = Some(IngestResult::new(
                    event.reference(),
                    Disposition::Dropped,
                    Some(Reason::InvalidEnvelope),
                    true,
                ));
                continue;
            }
            if !native_dedupe && self.seen_before(&event, &mut seen, now).await? {
                results[index] = Some(self.duplicate(&event, now));
                continue;
            }
            pending.push((index, event));
        }

        let messages = pending
            .iter()
            .map(|(_, event)| {
                let envelope = IngressEnvelope::new(event.clone(), now);
                let message = NewMessage::new(serde_json::to_value(envelope)?);
                Ok(if native_dedupe {
                    message.dedupe_key(event.dedupe_key())
                } else {
                    message
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let enqueued = if messages.is_empty() {
            Vec::new()
        } else {
            self.queues.enqueue(&queue.name, &messages).await?
        };

        let expires = now.saturating_add(config.ingress.tombstone_ttl_ms);
        for ((index, event), stored) in pending.into_iter().zip(enqueued) {
            if stored.duplicate {
                results[index] = Some(self.duplicate(&event, now));
                continue;
            }
            if !native_dedupe {
                self.tombstones
                    .put(&event.source, &event.event_id, expires)
                    .await?;
            }
            let ack = config.source_policy(&event.source).ack == AckPolicy::AfterIngest;
            self.ledger.notify(&Outcome::new(
                event.reference(),
                Disposition::Accepted,
                Reason::Ingested,
                now,
            ));
            results[index] = Some(IngestResult::new(
                event.reference(),
                Disposition::Accepted,
                None,
                ack,
            ));
        }
        Ok(IngestResponse::new(results.into_iter().flatten().collect()))
    }

    /// Whether a backend without dedupe has seen `event`: earlier in this
    /// request, as a live tombstone, or as a completed outcome.
    async fn seen_before(
        &self,
        event: &InboundEvent,
        seen: &mut HashSet<String>,
        now: u64,
    ) -> Result<bool> {
        if !seen.insert(event.dedupe_key()) {
            return Ok(true);
        }
        if self
            .tombstones
            .live(&event.source, &event.event_id, now)
            .await?
        {
            return Ok(true);
        }
        Ok(self.ledger.terminal(&event.reference()).await?.is_some())
    }

    fn duplicate(&self, event: &InboundEvent, now: u64) -> IngestResult {
        self.ledger.notify(&Outcome::new(
            event.reference(),
            Disposition::Duplicate,
            Reason::DuplicateRedelivery,
            now,
        ));
        IngestResult::new(
            event.reference(),
            Disposition::Duplicate,
            Some(Reason::DuplicateRedelivery),
            true,
        )
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
