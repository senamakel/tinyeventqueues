//! One pass of the pipeline's stages, in order.

use crate::ports::{Retry, Versioned};
use tinyeventqueues_bus::{
    Budget, ClassifySalienceRequest, Disposition, EventRef, InboundEvent, Lane, Outcome, Reason,
    SalienceClass,
};

use super::{Pipeline, Running};
use crate::Result;
use crate::cadence::{self, Cadence, Resolved, Scheduler, Staged};
use crate::coalesce::{Added, amends};
use crate::gate::Verdict;
use crate::ingress::IngressEnvelope;
use crate::salience::{lane_for, lane_for_class, rule_score};
use crate::turn::PendingTurn;

/// The most ingress batches one pass takes, so a pass is bounded.
const MAX_INGRESS_BATCHES: u32 = 64;

/// The most threads one pass flushes.
const MAX_FLUSH_THREADS: usize = 1_024;

impl Pipeline {
    /// Take events off the ingress queue, gate them, and buffer the ones that
    /// pass. Returns how many ingress messages were taken.
    pub(super) async fn take_ingress(&self, running: &Running, now: u64) -> Result<u64> {
        let queue = &running.queues.ingress;
        let mut taken = 0;
        for _ in 0..MAX_INGRESS_BATCHES {
            let batch = self
                .inner
                .queues
                .receive(
                    &queue.name,
                    running.config.ingress.batch_size,
                    queue.visibility_ms,
                    &self.inner.consumer,
                )
                .await?;
            if batch.is_empty() {
                break;
            }
            for delivery in batch {
                taken += 1;
                if let Ok(envelope) = serde_json::from_value::<IngressEnvelope>(delivery.payload) {
                    self.admit(envelope, running, now).await?;
                } else {
                    let reason = Retry::DeadLetter("unreadable ingress envelope".to_owned());
                    self.inner
                        .queues
                        .nack(&queue.name, &delivery.receipt, reason)
                        .await?;
                    continue;
                }
                self.inner
                    .queues
                    .ack(&queue.name, &delivery.receipt)
                    .await?;
            }
        }
        Ok(taken)
    }

    /// Gate one event and act on the verdict.
    async fn admit(&self, envelope: IngressEnvelope, running: &Running, now: u64) -> Result<()> {
        let config = &running.config;
        let ttl = config.ingress.tombstone_ttl_ms;
        let event = envelope.event;
        let verdict = self.inner.gate.check(&event, &config.gate, now).await?;
        match verdict {
            Verdict::Drop(reason) => {
                self.end(&event, Disposition::Dropped, reason, now, ttl)
                    .await
            }
            Verdict::Context => {
                if amends(&event)
                    && let Some(added) = self.inner.coalescer.try_amend(&event).await?
                {
                    return self.settle_added(added, now, ttl).await;
                }
                self.inner
                    .ambient
                    .keep(&event, &config.gate.ambient)
                    .await?;
                self.end(&event, Disposition::Context, Reason::Unaddressed, now, ttl)
                    .await
            }
            Verdict::Spill(reason) => {
                self.spill(std::slice::from_ref(&event), &[], reason, running, now)
                    .await
            }
            Verdict::Pass => {
                let added = self
                    .inner
                    .coalescer
                    .add(&event, envelope.ingested_at_ms, &config.coalesce, now)
                    .await?;
                self.settle_added(added, now, ttl).await
            }
        }
    }

    async fn settle_added(&self, added: Added, now: u64, ttl: u64) -> Result<()> {
        if let Added::Retracted(events) = added {
            for event in events {
                let outcome =
                    Outcome::new(event, Disposition::Dropped, Reason::RetractedBySender, now);
                self.inner.ledger.record(outcome, ttl).await?;
            }
        }
        Ok(())
    }

    async fn end(
        &self,
        event: &InboundEvent,
        disposition: Disposition,
        reason: Reason,
        now: u64,
        ttl: u64,
    ) -> Result<()> {
        let outcome = Outcome::new(event.reference(), disposition, reason, now);
        self.inner.ledger.record(outcome, ttl).await?;
        Ok(())
    }

    /// Flush every due buffer: score it, pick its lane, and enqueue it as a
    /// pending turn with the conversation's ambient context (or spill it to
    /// the digest). Returns how many buffers flushed.
    pub(super) async fn flush_due(&self, running: &Running, now: u64) -> Result<u64> {
        let mut flushed = 0;
        for due in self.inner.coalescer.due(now, MAX_FLUSH_THREADS).await? {
            let buffer = &due.found.doc;
            if !buffer.events.is_empty() {
                let absorbed: Vec<EventRef> =
                    buffer.absorbed.iter().map(|a| a.event.clone()).collect();
                self.route(&buffer.events, &absorbed, running, now).await?;
                flushed += 1;
            }
            self.inner.coalescer.remove(&due).await?;
        }
        Ok(flushed)
    }

    /// Score a flushed batch and send it to its lane, or spill it.
    async fn route(
        &self,
        events: &[InboundEvent],
        absorbed: &[EventRef],
        running: &Running,
        now: u64,
    ) -> Result<()> {
        let config = &running.config;
        let mut best: Option<(u32, &InboundEvent)> = None;
        let mut habituated = false;
        for event in events {
            let weight = config.source_policy(&event.source).weight;
            let rule = rule_score(event, &config.salience, weight, now);
            let scored = self
                .inner
                .habituation
                .apply(event, rule, &config.salience.habituation, now)
                .await?;
            habituated |= scored.habituated;
            if best.is_none_or(|(score, _)| scored.score > score) {
                best = Some((scored.score, event));
            }
        }
        let Some((score, top)) = best else {
            return Ok(());
        };
        let lane = match self.classify(top, score, running, now).await? {
            Some(class) => lane_for_class(class),
            None => lane_for(score, &config.salience.thresholds),
        };
        let Some(lane) = lane else {
            let reason = if habituated {
                Reason::Habituated
            } else {
                Reason::Noise
            };
            return self.spill(events, absorbed, reason, running, now).await;
        };
        let queue = running.queues.lane(lane);
        let stats = self.inner.queues.stats(&queue.name).await?;
        if lane != Lane::Digest && stats.ready + stats.delayed >= config.lanes.get(lane).max_backlog
        {
            return self
                .spill(events, absorbed, Reason::LaneOverflow, running, now)
                .await;
        }
        self.schedule(lane, events, absorbed, score, running, now)
            .await
    }

    /// Hand a routed batch to its cadence: release it now, or stage it for
    /// a tick (or, over an instant budget, for a short debounce).
    async fn schedule(
        &self,
        lane: Lane,
        events: &[InboundEvent],
        absorbed: &[EventRef],
        score: u32,
        running: &Running,
        now: u64,
    ) -> Result<()> {
        let config = &running.config;
        let Some(first) = events.first() else {
            return Ok(());
        };
        let resolved = cadence::resolve(config, &first.source, lane);
        let waiting = self.inner.scheduler.entry_of(first).await?;
        if let Some(entry) = &waiting
            && score >= config.salience.thresholds.interactive
        {
            // Promoted: the thread's waiting entry goes out now, with
            // this turn folded in, whatever cadence it waited for.
            self.inner
                .scheduler
                .stage(&resolved, lane, events, absorbed, now, now)
                .await?;
            let entry = self
                .inner
                .scheduler
                .entry_of(first)
                .await?
                .unwrap_or_else(|| entry.clone());
            return self.release_entry(&entry, running, now).await;
        }
        let due = match &resolved.cadence {
            Cadence::Instant => {
                let over_budget = match resolved.budget {
                    Some(budget) => {
                        !self
                            .inner
                            .gate
                            .turn_budget(&resolved.key, Scheduler::instant_limit(budget), now)
                            .await?
                    }
                    None => false,
                };
                match (resolved.budget, over_budget, &waiting) {
                    // A thread with a staged entry folds into it, so a newer
                    // turn never overtakes an older one.
                    (_, _, Some(entry)) => Some(entry.doc.due_ms),
                    (Some(budget), true, None) => {
                        Some(now.saturating_add(budget.degrade_debounce_ms))
                    }
                    _ => None,
                }
            }
            _ => {
                if score >= config.salience.thresholds.interactive {
                    // Promoted: interactive work does not wait for a tick.
                    None
                } else {
                    match waiting {
                        Some(entry) => Some(entry.doc.due_ms),
                        None => self.inner.scheduler.next_due(&resolved, now).await?,
                    }
                }
            }
        };
        let Some(due) = due else {
            return self
                .enqueue_turn(lane, events.to_vec(), absorbed.to_vec(), running, now)
                .await;
        };
        self.inner
            .scheduler
            .stage(&resolved, lane, events, absorbed, due, now)
            .await?;
        let reason = if resolved.cadence == Cadence::Instant {
            Reason::OverBudget
        } else {
            Reason::ScheduledCadence
        };
        self.defer_notice(events, absorbed, reason, now);
        Ok(())
    }

    fn defer_notice(
        &self,
        events: &[InboundEvent],
        absorbed: &[EventRef],
        reason: Reason,
        now: u64,
    ) {
        for event in events
            .iter()
            .map(InboundEvent::reference)
            .chain(absorbed.iter().cloned())
        {
            self.inner
                .ledger
                .notify(&Outcome::new(event, Disposition::Deferred, reason, now));
        }
    }

    /// Enqueue a turn on its lane with the conversation's ambient context.
    async fn enqueue_turn(
        &self,
        lane: Lane,
        events: Vec<InboundEvent>,
        absorbed: Vec<EventRef>,
        running: &Running,
        now: u64,
    ) -> Result<()> {
        let Some(first) = events.first() else {
            return Ok(());
        };
        let (source, conversation) = (first.source.clone(), first.conversation_key.clone());
        // Keep the thread's order across lanes: take the most urgent lane it
        // has work queued on, folding in queued turns of less urgent lanes.
        let (lane, superseded) = self.inner.threads.plan(first, lane).await?;
        let mut all_events: Vec<InboundEvent> = Vec::new();
        let mut all_absorbed: Vec<EventRef> = Vec::new();
        for queued in &superseded {
            all_events.extend(queued.events.iter().cloned());
            all_absorbed.extend(queued.absorbed.iter().cloned());
        }
        all_events.extend(events);
        all_absorbed.extend(absorbed);
        let mut turn = PendingTurn::new(lane, Reason::Turn, all_events, all_absorbed, now)?;
        turn.context = self.inner.ambient.peek(&source, &conversation).await?;
        self.inner
            .lanes
            .enqueue(
                running.queues.lane(lane),
                &turn,
                now,
                running.config.ingress.tombstone_ttl_ms,
            )
            .await?;
        self.inner.threads.record(&turn, &superseded).await?;
        let taken: Vec<_> = turn.context.iter().map(InboundEvent::reference).collect();
        self.inner
            .ambient
            .release(&source, &conversation, &taken)
            .await
    }

    /// Enqueue a staged entry as a turn and forget it.
    async fn release_entry(
        &self,
        entry: &Versioned<Staged>,
        running: &Running,
        now: u64,
    ) -> Result<()> {
        let staged = &entry.doc;
        self.enqueue_turn(
            staged.lane,
            staged.events.clone(),
            staged.absorbed.clone(),
            running,
            now,
        )
        .await?;
        self.inner.scheduler.remove(entry).await
    }

    /// Release what the cadences owe at `now`: due degraded entries of
    /// instant cadences within their budget, and each due tick of a
    /// scheduled cadence up to its budget. Waiting entries past the backlog
    /// fold into the digest. Returns how many turns were released.
    pub(super) async fn release_staged(&self, running: &Running, now: u64) -> Result<u64> {
        let mut keys: Vec<String> = Vec::new();
        for entry in self.inner.scheduler.due(now).await? {
            if !keys.contains(&entry.doc.cadence_key) {
                keys.push(entry.doc.cadence_key);
            }
        }
        let mut released = 0;
        for key in keys {
            let resolved = cadence::by_key(&running.config, &key);
            released += if resolved.cadence == Cadence::Instant {
                self.release_degraded(&resolved, running, now).await?
            } else {
                self.fire_tick(&resolved, running, now).await?
            };
        }
        Ok(released)
    }

    async fn release_degraded(
        &self,
        resolved: &Resolved,
        running: &Running,
        now: u64,
    ) -> Result<u64> {
        let mut released = 0;
        let mut held = Vec::new();
        for entry in self.inner.scheduler.due(now).await? {
            if entry.doc.cadence_key != resolved.key {
                continue;
            }
            let granted = match resolved.budget {
                Some(budget) => {
                    self.inner
                        .gate
                        .turn_budget(&resolved.key, Scheduler::instant_limit(budget), now)
                        .await?
                }
                None => true,
            };
            if granted {
                self.release_entry(&entry, running, now).await?;
                released += 1;
            } else {
                held.push(entry);
            }
        }
        if let Some(budget) = resolved.budget {
            let next = now.saturating_add(budget.degrade_debounce_ms);
            self.hold(held, budget, next, running, now).await?;
        }
        Ok(released)
    }

    async fn fire_tick(&self, resolved: &Resolved, running: &Running, now: u64) -> Result<u64> {
        let scheduler = &self.inner.scheduler;
        let Some(allowed) = scheduler.begin_tick(resolved, now).await? else {
            // Due by its own clock but not by the schedule (the cadence
            // changed): wait for the schedule's tick.
            if let Some(next) = scheduler.next_due(resolved, now).await? {
                for entry in scheduler.due(now).await? {
                    if entry.doc.cadence_key == resolved.key {
                        scheduler.defer(&entry, next).await?;
                    }
                }
            }
            return Ok(0);
        };
        let mut released = 0;
        let mut held = Vec::new();
        for entry in scheduler.waiting(&resolved.key).await? {
            if released < u64::from(allowed) {
                scheduler.count_release(&resolved.key).await?;
                self.release_entry(&entry, running, now).await?;
                released += 1;
            } else {
                held.push(entry);
            }
        }
        let next = scheduler.end_tick(resolved, now).await?.unwrap_or(u64::MAX);
        match resolved.budget {
            Some(budget) => self.hold(held, budget, next, running, now).await?,
            None => {
                for entry in held {
                    scheduler.defer(&entry, next).await?;
                }
            }
        }
        Ok(released)
    }

    /// Keep over-budget entries waiting until `next`, oldest first, folding
    /// those beyond the backlog into the digest.
    async fn hold(
        &self,
        held: Vec<Versioned<Staged>>,
        budget: Budget,
        next: u64,
        running: &Running,
        now: u64,
    ) -> Result<()> {
        let backlog = usize::try_from(budget.max_turns.saturating_mul(budget.backlog_ticks))
            .unwrap_or(usize::MAX);
        let excess = held.len().saturating_sub(backlog);
        for (index, entry) in held.into_iter().enumerate() {
            let staged = &entry.doc;
            if index < excess {
                self.spill(
                    &staged.events,
                    &staged.absorbed,
                    Reason::OverBudget,
                    running,
                    now,
                )
                .await?;
                self.inner.scheduler.remove(&entry).await?;
            } else {
                self.inner.scheduler.defer(&entry, next).await?;
                self.defer_notice(&staged.events, &staged.absorbed, Reason::OverBudget, now);
            }
        }
        Ok(())
    }

    /// Ask the host to classify a borderline score, within budget.
    async fn classify(
        &self,
        event: &InboundEvent,
        score: u32,
        running: &Running,
        now: u64,
    ) -> Result<Option<SalienceClass>> {
        let (Some(host), Some(band)) = (&self.inner.host, running.config.salience.borderline)
        else {
            return Ok(None);
        };
        if !(band.low..band.high).contains(&score)
            || !self
                .inner
                .gate
                .classify_budget(band.budget_per_hour, now)
                .await?
        {
            return Ok(None);
        }
        let request = ClassifySalienceRequest::new(event.clone(), score);
        Ok(host
            .classify_salience(request)
            .await
            .ok()
            .map(|reply| reply.class))
    }

    /// Fold events into the digest, reporting each as deferred with
    /// `reason`, and release a digest turn if one is due.
    async fn spill(
        &self,
        events: &[InboundEvent],
        absorbed: &[EventRef],
        reason: Reason,
        running: &Running,
        now: u64,
    ) -> Result<()> {
        self.inner.digest.add(events, absorbed, now).await?;
        for event in events
            .iter()
            .map(InboundEvent::reference)
            .chain(absorbed.iter().cloned())
        {
            self.inner
                .ledger
                .notify(&Outcome::new(event, Disposition::Deferred, reason, now));
        }
        self.release_digest(running, now).await?;
        Ok(())
    }

    /// Enqueue every digest turn due at `now`. Returns how many.
    pub(super) async fn release_digest(&self, running: &Running, now: u64) -> Result<u64> {
        let mut released = 0;
        while let Some(turn) = self.inner.digest.due(&running.config.digest, now).await? {
            self.inner
                .lanes
                .enqueue(
                    running.queues.lane(Lane::Digest),
                    &turn,
                    now,
                    running.config.ingress.tombstone_ttl_ms,
                )
                .await?;
            self.inner.digest.release(&turn, now).await?;
            released += 1;
        }
        Ok(released)
    }
}

#[cfg(test)]
#[path = "pass_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "cadence_tests.rs"]
mod cadence_tests;
