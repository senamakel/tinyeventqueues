//! The thread registry: which turns of a conversation thread are queued in a
//! lane and not yet settled.
//!
//! Lane queues keep order within a lane, but a thread's turns can land on
//! different lanes (a habituated message on background, the next one
//! interactive). To keep a thread's order across lanes, a new turn goes to the
//! most urgent lane its thread has pending work on, and pending turns on less
//! urgent lanes are *superseded*: their events are folded, in order, into the
//! new turn, and the superseded turns are acknowledged unseen when dispatch
//! reaches them. The new turn is enqueued before the old ones are marked
//! superseded, so a crash in between can deliver an event twice (at least
//! once) but never lose one.

use std::sync::Arc;

use crate::ports::{DocumentStore, key_of};
use serde::{Deserialize, Serialize};
use tinyeventqueues_bus::{EventRef, InboundEvent, Lane};

use super::PendingTurn;
use crate::Result;
use crate::store::Repo;
use crate::store::THREADS;

/// One queued, unsettled turn of a thread.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Queued {
    pub(crate) turn_id: String,
    pub(crate) lane: Lane,
    pub(crate) events: Vec<InboundEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) absorbed: Vec<EventRef>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Thread {
    queued: Vec<Queued>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    superseded: Vec<String>,
}

fn thread_id(first: &InboundEvent) -> Result<String> {
    Ok(key_of(&[
        "thread",
        first.source.as_str(),
        first.conversation_key.as_str(),
        first.thread_key.as_deref().unwrap_or(""),
    ])?
    .as_str()
    .to_owned())
}

/// The registry over one document store.
#[derive(Debug, Clone)]
pub(crate) struct Threads {
    repo: Repo<Thread>,
}

impl Threads {
    pub(crate) fn new(docs: &Arc<dyn DocumentStore>) -> Self {
        Self {
            repo: Repo::new(docs, THREADS),
        }
    }

    /// The lane a new turn of `first`'s thread wanting `lane` must take, and
    /// the queued turns it supersedes (those on less urgent lanes), oldest
    /// first.
    pub(crate) async fn plan(
        &self,
        first: &InboundEvent,
        lane: Lane,
    ) -> Result<(Lane, Vec<Queued>)> {
        let Some(found) = self.repo.get(&thread_id(first)?).await? else {
            return Ok((lane, Vec::new()));
        };
        let queued = found.doc.queued;
        let lane = queued.iter().map(|q| q.lane).fold(lane, Lane::min);
        let superseded = queued.into_iter().filter(|q| q.lane > lane).collect();
        Ok((lane, superseded))
    }

    /// Record `turn` as queued, and `superseded` as replaced by it.
    pub(crate) async fn record(&self, turn: &PendingTurn, superseded: &[Queued]) -> Result<()> {
        let Some(first) = turn.events.first() else {
            return Ok(());
        };
        let entry = Queued {
            turn_id: turn.turn_id.clone(),
            lane: turn.lane,
            events: turn.events.clone(),
            absorbed: turn.absorbed.clone(),
        };
        self.repo
            .update(&thread_id(first)?, |current| {
                let mut thread = current.unwrap_or_default();
                let gone: Vec<&str> = superseded.iter().map(|q| q.turn_id.as_str()).collect();
                thread
                    .queued
                    .retain(|q| !gone.contains(&q.turn_id.as_str()));
                thread
                    .superseded
                    .extend(gone.iter().map(|id| (*id).to_owned()));
                if !thread.queued.iter().any(|q| q.turn_id == entry.turn_id) {
                    thread.queued.push(entry.clone());
                }
                Ok(Some(thread))
            })
            .await?;
        Ok(())
    }

    /// Whether `turn` was superseded by a later turn of its thread.
    pub(crate) async fn is_superseded(&self, turn: &PendingTurn) -> Result<bool> {
        let Some(first) = turn.events.first() else {
            return Ok(false);
        };
        Ok(self
            .repo
            .get(&thread_id(first)?)
            .await?
            .is_some_and(|found| found.doc.superseded.contains(&turn.turn_id)))
    }

    /// `turn` is settled (delivered, dead, or skipped as superseded).
    pub(crate) async fn settle(&self, turn: &PendingTurn) -> Result<()> {
        let Some(first) = turn.events.first() else {
            return Ok(());
        };
        self.repo
            .update(&thread_id(first)?, |current| {
                let Some(mut thread) = current else {
                    return Ok(None);
                };
                thread.queued.retain(|q| q.turn_id != turn.turn_id);
                thread.superseded.retain(|id| *id != turn.turn_id);
                Ok(Some(thread))
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "threads_tests.rs"]
mod tests;
