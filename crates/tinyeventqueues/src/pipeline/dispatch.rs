//! Dispatch: hand due turns to the host, lane by lane.
//!
//! Each pass visits the lanes most urgent first — interactive, actionable,
//! background, digest — and takes at most the lane's `concurrency` turns, so
//! interactive work is never stuck behind a background flood. A lane queue
//! with groups keeps one turn per conversation in flight; on a backend
//! without groups the conversation's sequence numbers do (see
//! [`Conversations`](crate::turn::Conversations)).
//!
//! The host's verdict settles the turn:
//!
//! - **accepted**: its events end `delivered` (or `digest` on the digest
//!   lane) and the message is acknowledged;
//! - **retry**, or a failed call: nacked with exponential backoff (or the
//!   host's `after_ms`); once the lane queue's manifest `max_attempts` is
//!   used up, the turn is dead-lettered with `max_attempts`;
//! - **rejected**: dead-lettered at once with `host_rejected`.
//!
//! Dead-lettered turns are parked in the dead-letter queue, where `ListDead`
//! shows them and `Redrive` puts them back.

use crate::ports::{Delivery, NewMessage, Retry};
use tinyeventqueues_bus::{
    AckPolicy, DeliverRequest, DeliverStatus, DispatchConfig, Disposition, InboundEvent, Lane,
    Outcome, Reason, Turn,
};

use super::{Pipeline, Running};
use crate::Result;
use crate::config::QueueRef;
use crate::dead::Parked;
use crate::turn::{PendingTurn, Turnstile};

/// The delay before retry `attempt` (1-based), doubling from the base.
pub(crate) fn backoff(config: &DispatchConfig, attempt: u32) -> u64 {
    let doublings = attempt.saturating_sub(1).min(32);
    config
        .backoff_base_ms
        .saturating_mul(1u64 << doublings)
        .min(config.backoff_max_ms)
}

/// One delivered turn being settled.
struct Settling<'a> {
    pending: &'a PendingTurn,
    delivery: &'a Delivery,
    queue: &'a QueueRef,
}

/// How long a turn waiting for an earlier one of its conversation is held.
const WAIT_MS: u64 = 1_000;

impl Pipeline {
    /// Deliver due turns. Returns how many turns were handed to the host and
    /// how many were dead-lettered.
    pub(super) async fn dispatch(&self, running: &Running, now: u64) -> Result<(u64, u64)> {
        let Some(host) = self.inner.host.clone() else {
            return Ok((0, 0));
        };
        let mut dispatched = 0;
        let mut dead = 0;
        for lane in Lane::ALL {
            let queue = running.queues.lane(lane);
            let max = running.config.lanes.get(lane).concurrency;
            let batch = self
                .inner
                .queues
                .receive(&queue.name, max, queue.visibility_ms, &self.inner.consumer)
                .await?;
            for delivery in batch {
                let Ok(pending) = serde_json::from_value::<PendingTurn>(delivery.payload.clone())
                else {
                    let reason = Retry::DeadLetter("unreadable pending turn".to_owned());
                    self.inner
                        .queues
                        .nack(&queue.name, &delivery.receipt, reason)
                        .await?;
                    continue;
                };
                if self.inner.threads.is_superseded(&pending).await? {
                    self.inner
                        .queues
                        .ack(&queue.name, &delivery.receipt)
                        .await?;
                    self.inner.threads.settle(&pending).await?;
                    if let Some(seq) = pending.seq {
                        self.inner
                            .conversations
                            .finish(&pending.conversation_key, seq)
                            .await?;
                    }
                    continue;
                }
                if let Some(seq) = pending.seq {
                    let gate = self
                        .inner
                        .conversations
                        .check(
                            &pending.conversation_key,
                            seq,
                            running.config.dispatch.gap_timeout_ms,
                            now,
                        )
                        .await?;
                    match gate {
                        Turnstile::Wait => {
                            // Waiting is not a delivery attempt: requeue a
                            // fresh copy rather than spend one of the turn's
                            // attempts with a nack.
                            let copy = NewMessage::new(delivery.payload.clone()).delay_ms(WAIT_MS);
                            self.inner.queues.enqueue(&queue.name, &[copy]).await?;
                            self.inner
                                .queues
                                .ack(&queue.name, &delivery.receipt)
                                .await?;
                            continue;
                        }
                        // A redelivery of a finished turn: acknowledge it, unless
                        // some of its events never ended (then deliver it out
                        // of order rather than lose it).
                        Turnstile::Done if self.all_ended(&pending).await? => {
                            self.inner
                                .queues
                                .ack(&queue.name, &delivery.receipt)
                                .await?;
                            self.inner.threads.settle(&pending).await?;
                            continue;
                        }
                        Turnstile::Go | Turnstile::Done => {}
                    }
                }
                let turn = Self::turn_for(&pending, delivery.attempt, running);
                let status = match host.deliver(DeliverRequest::new(turn)).await {
                    Ok(reply) => reply.status,
                    Err(_) => DeliverStatus::Retry { after_ms: None },
                };
                dispatched += 1;
                let settling = Settling {
                    pending: &pending,
                    delivery: &delivery,
                    queue,
                };
                if self.settle(&settling, status, running, now).await? {
                    dead += 1;
                }
            }
        }
        Ok((dispatched, dead))
    }

    async fn all_ended(&self, pending: &PendingTurn) -> Result<bool> {
        for event in pending
            .events
            .iter()
            .map(InboundEvent::reference)
            .chain(pending.absorbed.iter().cloned())
        {
            if self.inner.ledger.terminal(&event).await?.is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn turn_for(pending: &PendingTurn, attempt: u32, running: &Running) -> Turn {
        let mut turn = Turn::new(
            pending.turn_id.clone(),
            pending.lane,
            pending.conversation_key.clone(),
            pending.events.clone(),
        );
        turn.thread_key.clone_from(&pending.thread_key);
        turn.context.clone_from(&pending.context);
        turn.attempt = attempt;
        turn.platform_acks = pending
            .events
            .iter()
            .filter(|e| running.config.source_policy(&e.source).ack != AckPolicy::AfterIngest)
            .map(InboundEvent::reference)
            .collect();
        turn
    }

    /// Settle one delivered turn. Returns whether it was dead-lettered.
    async fn settle(
        &self,
        settling: &Settling<'_>,
        status: DeliverStatus,
        running: &Running,
        now: u64,
    ) -> Result<bool> {
        let Settling {
            pending,
            delivery,
            queue,
        } = settling;
        let ttl = running.config.ingress.tombstone_ttl_ms;
        let refs = pending
            .events
            .iter()
            .map(InboundEvent::reference)
            .chain(pending.absorbed.iter().cloned());
        match status {
            DeliverStatus::Accepted => {
                let disposition = if pending.lane == Lane::Digest {
                    Disposition::Digest
                } else {
                    Disposition::Delivered
                };
                for event in refs {
                    let outcome = Outcome::new(event, disposition, Reason::Turn, now)
                        .in_turn(&pending.turn_id);
                    self.inner.ledger.record(outcome, ttl).await?;
                }
                self.finish(settling).await?;
                Ok(false)
            }
            DeliverStatus::Retry { after_ms } if delivery.attempt < queue.max_attempts => {
                let wait =
                    after_ms.unwrap_or_else(|| backoff(&running.config.dispatch, delivery.attempt));
                for event in refs {
                    self.inner.ledger.notify(&Outcome::new(
                        event,
                        Disposition::Deferred,
                        Reason::RetryRequested,
                        now,
                    ));
                }
                self.inner
                    .queues
                    .nack(&queue.name, &delivery.receipt, Retry::After(wait))
                    .await?;
                Ok(false)
            }
            DeliverStatus::Rejected { detail } => {
                self.park(settling, Reason::HostRejected, detail, running, now)
                    .await?;
                Ok(true)
            }
            _ => {
                let detail = "max attempts exceeded".to_owned();
                self.park(settling, Reason::MaxAttempts, detail, running, now)
                    .await?;
                Ok(true)
            }
        }
    }

    /// Dead-letter a turn: park it, record its events as dead, and move its
    /// conversation on.
    async fn park(
        &self,
        settling: &Settling<'_>,
        reason: Reason,
        detail: String,
        running: &Running,
        now: u64,
    ) -> Result<()> {
        let Settling {
            pending,
            delivery,
            queue,
        } = settling;
        let parked = Parked {
            origin: queue.name.clone(),
            reason: detail,
            attempt: delivery.attempt,
            group: delivery.group.clone(),
            turn_id: Some(pending.turn_id.clone()),
            events: pending
                .events
                .iter()
                .map(InboundEvent::reference)
                .chain(pending.absorbed.iter().cloned())
                .collect(),
            payload: delivery.payload.clone(),
        };
        self.inner
            .dead
            .park(
                &running.queues.dead,
                parked,
                reason,
                now,
                running.config.ingress.tombstone_ttl_ms,
            )
            .await?;
        self.finish(settling).await
    }

    async fn finish(&self, settling: &Settling<'_>) -> Result<()> {
        self.inner
            .queues
            .ack(&settling.queue.name, &settling.delivery.receipt)
            .await?;
        self.inner.threads.settle(settling.pending).await?;
        if let Some(seq) = settling.pending.seq {
            self.inner
                .conversations
                .finish(&settling.pending.conversation_key, seq)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "dispatch_tests.rs"]
mod tests;
