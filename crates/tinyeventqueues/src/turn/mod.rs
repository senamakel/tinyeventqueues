//! Pending turns: what waits in a lane queue for dispatch.
//!
//! A [`PendingTurn`] is the unit the lanes hold and the host eventually
//! receives as a [`Turn`](tinyeventqueues_bus::Turn): the events of one
//! conversation thread (or a digest), the events absorbed into them (edits
//! that amended a pending message), and the ambient context collected since
//! the conversation's last turn. Its id is derived from its first event, so
//! re-enqueueing the same turn after a crash is a duplicate, not a second
//! turn.

use std::sync::Arc;

use crate::ports::{DocumentStore, NewMessage, QueueStore, key_of};
use serde::{Deserialize, Serialize};
use tinyeventqueues_bus::{EventRef, InboundEvent, Lane, Reason};

use crate::Result;
use crate::config::QueueRef;
use crate::store::Repo;
use crate::store::{CONVERSATIONS, Tombstones};

/// A turn waiting in a lane queue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PendingTurn {
    pub(crate) turn_id: String,
    pub(crate) lane: Lane,
    /// Why the turn is on its lane: an ordinary [`Reason::Turn`], or a
    /// spill such as [`Reason::RateLimited`].
    pub(crate) reason: Reason,
    pub(crate) conversation_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) thread_key: Option<String>,
    pub(crate) events: Vec<InboundEvent>,
    /// Events folded into `events` (edits) that share the turn's fate.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) absorbed: Vec<EventRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) context: Vec<InboundEvent>,
    pub(crate) created_ms: u64,
    /// The turn's place in its conversation, on a queue backend without
    /// groups (which orders conversations itself).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) seq: Option<u64>,
}

impl PendingTurn {
    /// A turn of `events` on `lane`, which must not be empty.
    pub(crate) fn new(
        lane: Lane,
        reason: Reason,
        events: Vec<InboundEvent>,
        absorbed: Vec<EventRef>,
        created_ms: u64,
    ) -> Result<Self> {
        let first = events.first();
        let conversation_key = first
            .map(|e| e.conversation_key.clone())
            .unwrap_or_default();
        let thread_key = first.and_then(|e| e.thread_key.clone());
        let (source, id) = first.map_or((String::new(), String::new()), |e| {
            (e.source.clone(), e.event_id.clone())
        });
        let turn_id = key_of(&[
            "turn",
            lane.as_str(),
            conversation_key.as_str(),
            thread_key.as_deref().unwrap_or(""),
            source.as_str(),
            id.as_str(),
        ])?
        .as_str()
        .to_owned();
        Ok(Self {
            turn_id,
            lane,
            reason,
            conversation_key,
            thread_key,
            events,
            absorbed,
            context: Vec::new(),
            created_ms,
            seq: None,
        })
    }
}

/// Writes pending turns to lane queues.
///
/// On a backend with dedupe the turn id is the message's dedupe key; without
/// it, a tombstone per turn id in the document store (written after the
/// enqueue) recognizes a re-enqueue. On a backend with groups the
/// conversation is the message's group; without them the dispatcher
/// serializes conversations itself.
#[derive(Debug, Clone)]
pub(crate) struct Lanes {
    queues: Arc<dyn QueueStore>,
    tombstones: Tombstones,
    conversations: Conversations,
}

/// The tombstone namespace of turn ids.
const TURN_TOMBSTONES: &str = "turn";

impl Lanes {
    pub(crate) fn new(
        queues: Arc<dyn QueueStore>,
        tombstones: Tombstones,
        conversations: Conversations,
    ) -> Self {
        Self {
            queues,
            tombstones,
            conversations,
        }
    }

    /// Enqueue `turn` on `queue`. Returns `false` when the turn was already
    /// queued. Without native dedupe, the turn's tombstone lives `ttl_ms`.
    pub(crate) async fn enqueue(
        &self,
        queue: &QueueRef,
        turn: &PendingTurn,
        now: u64,
        ttl_ms: u64,
    ) -> Result<bool> {
        let features = self.queues.features();
        if !features.dedupe
            && self
                .tombstones
                .live(TURN_TOMBSTONES, &turn.turn_id, now)
                .await?
        {
            return Ok(false);
        }
        let mut turn = turn.clone();
        if !features.groups && !turn.conversation_key.is_empty() && turn.seq.is_none() {
            turn.seq = Some(self.conversations.assign(&turn.conversation_key).await?);
        }
        let mut message = NewMessage::new(serde_json::to_value(&turn)?);
        if features.dedupe {
            message = message.dedupe_key(&turn.turn_id);
        }
        if features.groups && !turn.conversation_key.is_empty() {
            message = message.group(&turn.conversation_key);
        }
        let stored = self.queues.enqueue(&queue.name, &[message]).await?;
        if !features.dedupe {
            self.tombstones
                .put(TURN_TOMBSTONES, &turn.turn_id, now.saturating_add(ttl_ms))
                .await?;
        }
        Ok(stored.first().is_some_and(|stored| !stored.duplicate))
    }
}

/// A conversation's order on a queue backend without groups.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Conversation {
    /// The last sequence number handed out.
    last_seq: u64,
    /// The sequence number of the next turn to deliver.
    next_seq: u64,
    /// Since when a later turn has been waiting for a missing one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    waiting_since_ms: Option<u64>,
    /// The turn numbered `next_seq` has been seen (it is in flight or
    /// backing off), so it is not missing and must not be skipped.
    #[serde(default)]
    next_seen: bool,
}

/// Whether a turn may be delivered now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Turnstile {
    /// It is the conversation's next turn.
    Go,
    /// An earlier turn of the conversation has not finished.
    Wait,
    /// It already finished (a redelivery).
    Done,
}

/// Per-conversation ordering for queue backends without groups: each turn
/// gets the next sequence number of its conversation when it is enqueued, and
/// only the conversation's next number is delivered, so a conversation has
/// one turn in flight and its turns keep their order. A number lost to a
/// crash between assigning it and enqueueing is skipped after a gap timeout.
#[derive(Debug, Clone)]
pub(crate) struct Conversations {
    repo: Repo<Conversation>,
}

impl Conversations {
    pub(crate) fn new(docs: &Arc<dyn DocumentStore>) -> Self {
        Self {
            repo: Repo::new(docs, CONVERSATIONS),
        }
    }

    async fn assign(&self, conversation: &str) -> Result<u64> {
        let id = key_of(&["conversation", conversation])?;
        let written = self
            .repo
            .update(id.as_str(), |current| {
                let mut state = current.unwrap_or(Conversation {
                    next_seq: 1,
                    ..Conversation::default()
                });
                state.last_seq += 1;
                Ok(Some(state))
            })
            .await?;
        Ok(written.map_or(1, |(state, _)| state.last_seq))
    }

    /// Whether turn `seq` of `conversation` may go now.
    pub(crate) async fn check(
        &self,
        conversation: &str,
        seq: u64,
        gap_timeout_ms: u64,
        now: u64,
    ) -> Result<Turnstile> {
        let id = key_of(&["conversation", conversation])?;
        let mut verdict = Turnstile::Go;
        self.repo
            .update(id.as_str(), |current| {
                let mut state = current.unwrap_or(Conversation {
                    next_seq: seq,
                    last_seq: seq,
                    waiting_since_ms: None,
                    next_seen: false,
                });
                if seq < state.next_seq {
                    verdict = Turnstile::Done;
                    return Ok(None);
                }
                if seq == state.next_seq {
                    verdict = Turnstile::Go;
                    if state.next_seen {
                        return Ok(None);
                    }
                    state.next_seen = true;
                    state.waiting_since_ms = None;
                    return Ok(Some(state));
                }
                match state.waiting_since_ms {
                    Some(since)
                        if !state.next_seen && now.saturating_sub(since) >= gap_timeout_ms =>
                    {
                        verdict = Turnstile::Go;
                        state.next_seq = seq;
                        state.next_seen = true;
                        state.waiting_since_ms = None;
                    }
                    Some(_) => {
                        verdict = Turnstile::Wait;
                        return Ok(None);
                    }
                    _ => {
                        verdict = Turnstile::Wait;
                        state.waiting_since_ms = Some(now);
                    }
                }
                Ok(Some(state))
            })
            .await?;
        Ok(verdict)
    }

    /// Turn `seq` of `conversation` finished: the next one may go.
    pub(crate) async fn finish(&self, conversation: &str, seq: u64) -> Result<()> {
        let id = key_of(&["conversation", conversation])?;
        self.repo
            .update(id.as_str(), |current| {
                let Some(mut state) = current else {
                    return Ok(None);
                };
                if state.next_seq > seq {
                    return Ok(None);
                }
                state.next_seq = seq + 1;
                state.waiting_since_ms = None;
                state.next_seen = false;
                Ok(Some(state))
            })
            .await?;
        Ok(())
    }
}

mod threads;

pub(crate) use threads::Threads;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
