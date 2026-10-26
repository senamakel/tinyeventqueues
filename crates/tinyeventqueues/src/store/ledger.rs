//! The outcome ledger: one durable record per event that reached a terminal
//! disposition.

use std::sync::Arc;

use crate::ports::{DocumentStore, ErrorKind, Filter, Precondition};
use serde::{Deserialize, Serialize};
use tinyeventqueues_bus::{Disposition, EventRef, Outcome, OutcomeCounts, Reason};

use super::{OUTCOMES, event_id};
use crate::Result;
use crate::observe::OutcomeSink;

/// A stored terminal outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    source: String,
    event_id: String,
    disposition: Disposition,
    reason: Reason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    turn_id: Option<String>,
    at_ms: u64,
    expires_ms: u64,
}

impl Record {
    fn outcome(self) -> Outcome {
        let mut outcome = Outcome::new(
            EventRef::new(self.source, self.event_id),
            self.disposition,
            self.reason,
            self.at_ms,
        );
        outcome.turn_id = self.turn_id;
        outcome
    }
}

/// Records outcomes: every one goes to the sink, terminal ones also to the
/// ledger, at most once per event.
#[derive(Debug, Clone)]
pub(crate) struct Ledger {
    docs: Arc<dyn DocumentStore>,
    sink: Arc<dyn OutcomeSink>,
}

impl Ledger {
    pub(crate) fn new(docs: Arc<dyn DocumentStore>, sink: Arc<dyn OutcomeSink>) -> Self {
        Self { docs, sink }
    }

    /// Report `outcome`. A terminal outcome is written to the ledger with a
    /// create-only precondition and kept for `ttl_ms`; it returns `false`, and
    /// reports nothing, when the event already ended (a replay after a
    /// crash). A non-terminal outcome only goes to the sink.
    pub(crate) async fn record(&self, outcome: Outcome, ttl_ms: u64) -> Result<bool> {
        if outcome.disposition.is_terminal() {
            let id = event_id(&outcome.event.source, &outcome.event.event_id)?;
            let record = Record {
                source: outcome.event.source.clone(),
                event_id: outcome.event.event_id.clone(),
                disposition: outcome.disposition,
                reason: outcome.reason,
                turn_id: outcome.turn_id.clone(),
                at_ms: outcome.at_ms,
                expires_ms: outcome.at_ms.saturating_add(ttl_ms),
            };
            let written = self
                .docs
                .put(
                    OUTCOMES,
                    &id,
                    serde_json::to_value(record)?,
                    Precondition::Absent,
                )
                .await;
            match written {
                Ok(_) => {}
                Err(error)
                    if matches!(error.kind(), ErrorKind::Conflict | ErrorKind::AlreadyExists) =>
                {
                    return Ok(false);
                }
                Err(error) => return Err(error.into()),
            }
        }
        self.sink.observe(&outcome);
        Ok(true)
    }

    /// Report a non-terminal outcome to the sink.
    pub(crate) fn notify(&self, outcome: &Outcome) {
        self.sink.observe(outcome);
    }

    /// The terminal outcome of `event`, if it ended and the record has not
    /// been pruned.
    pub(crate) async fn terminal(&self, event: &EventRef) -> Result<Option<Outcome>> {
        let id = event_id(&event.source, &event.event_id)?;
        let Some(found) = self.docs.get(OUTCOMES, &id).await? else {
            return Ok(None);
        };
        let record: Record = serde_json::from_value(found.doc)?;
        Ok(Some(record.outcome()))
    }

    /// Forget the terminal outcome of `event`, so a redriven event can end
    /// again.
    pub(crate) async fn forget(&self, event: &EventRef) -> Result<bool> {
        let id = event_id(&event.source, &event.event_id)?;
        Ok(self.docs.delete(OUTCOMES, &id, Precondition::None).await?)
    }

    /// How many unexpired records each terminal disposition has.
    pub(crate) async fn counts(&self) -> Result<OutcomeCounts> {
        let mut counts = OutcomeCounts::default();
        for disposition in Disposition::TERMINAL {
            let name = serde_json::to_value(disposition)?;
            let count = self
                .docs
                .count(OUTCOMES, &Filter::eq("disposition", name))
                .await?;
            counts.set(disposition, count);
        }
        Ok(counts)
    }

    /// Remove records expired at `now`.
    pub(crate) async fn prune(&self, now: u64) -> Result<u64> {
        Ok(self
            .docs
            .delete_where(OUTCOMES, &Filter::lte("expires_ms", now))
            .await?)
    }
}
