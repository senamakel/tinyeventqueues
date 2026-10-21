//! The cadence scheduler: when turns are released, and how many per tick.
//!
//! Every turn belongs to a cadence: its source's (`sources[].cadence`) or
//! else its lane's (`lanes.<lane>.cadence`), `instant` by default.
//!
//! - **Instant**: released at once, within a per-second budget. Over budget,
//!   the cadence *degrades*: new turns are staged for `degrade_debounce_ms`,
//!   folding each conversation's events into one entry, and released when
//!   due; once a token is available again the cadence is instant again.
//! - **Scheduled** (`every(..)`, `cron(..)`): turns are staged until the next
//!   tick, one entry per conversation thread (whatever its cadence: a later
//!   turn of the thread folds into the waiting entry) (so a conversation's events fold
//!   into one pending entry). A tick releases at most `max_turns`, oldest
//!   first; the rest wait for the next tick, and waiting entries beyond
//!   `max_turns * backlog_ticks` fold into the digest.
//!
//! Each cadence's next tick lives in the document store. Firing a tick first
//! records which tick is firing, and every released turn is counted against
//! that tick before it is enqueued, so a crash mid-tick resumes the same
//! tick with what is left of its budget: a restart neither skips a tick nor
//! fires one twice. Ticks missed while the process was down collapse into
//! one.

mod spec;

use std::sync::Arc;

use crate::ports::{DocumentStore, Filter, Precondition, Query, Sort, Versioned, key_of};
use serde::{Deserialize, Serialize};
use tinyeventqueues_bus::{Budget, EventRef, InboundEvent, Lane, PipelineConfig, RateLimit};

use crate::Result;
use crate::store::Repo;
use crate::store::{CADENCE, STAGED};

pub use spec::{Cadence, CadenceError, Cron};

/// A cadence a turn belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Resolved {
    /// `source:<pattern>` or `lane:<lane>`.
    pub(crate) key: String,
    pub(crate) cadence: Cadence,
    pub(crate) budget: Option<Budget>,
}

fn parse(text: Option<&String>) -> Cadence {
    text.map_or(Cadence::Instant, |t| {
        Cadence::parse(t).unwrap_or(Cadence::Instant)
    })
}

/// The cadence of a turn from `source` on `lane`. A cadence string that does
/// not parse (the configuration checks prevent it) counts as instant.
pub(crate) fn resolve(config: &PipelineConfig, source: &str, lane: Lane) -> Resolved {
    if let Some(policy) = config
        .sources
        .iter()
        .find(|policy| policy.matches(source))
        .filter(|policy| policy.cadence.is_some())
    {
        return Resolved {
            key: format!("source:{}", policy.pattern),
            cadence: parse(policy.cadence.as_ref()),
            budget: policy.budget,
        };
    }
    let lane_config = config.lanes.get(lane);
    Resolved {
        key: format!("lane:{}", lane.as_str()),
        cadence: parse(lane_config.cadence.as_ref()),
        budget: lane_config.budget,
    }
}

/// The cadence named by `key`, as the configuration now has it. A key the
/// configuration no longer names is instant, so its entries are released.
pub(crate) fn by_key(config: &PipelineConfig, key: &str) -> Resolved {
    let found = if let Some(pattern) = key.strip_prefix("source:") {
        config
            .sources
            .iter()
            .find(|policy| policy.pattern == pattern && policy.cadence.is_some())
            .map(|policy| (parse(policy.cadence.as_ref()), policy.budget))
    } else {
        Lane::ALL
            .into_iter()
            .find(|lane| key.strip_prefix("lane:") == Some(lane.as_str()))
            .map(|lane| {
                let lane = config.lanes.get(lane);
                (parse(lane.cadence.as_ref()), lane.budget)
            })
    };
    let (cadence, budget) = found.unwrap_or((Cadence::Instant, None));
    Resolved {
        key: key.to_owned(),
        cadence,
        budget,
    }
}

/// One staged entry: a conversation thread's pending turn on a cadence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct Staged {
    pub(crate) cadence_key: String,
    pub(crate) lane: Lane,
    pub(crate) events: Vec<InboundEvent>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) absorbed: Vec<EventRef>,
    pub(crate) first_ms: u64,
    pub(crate) due_ms: u64,
}

impl Staged {
    pub(crate) fn refs(&self) -> Vec<EventRef> {
        self.events
            .iter()
            .map(InboundEvent::reference)
            .chain(self.absorbed.iter().cloned())
            .collect()
    }
}

/// A cadence's persisted schedule.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct State {
    /// The schedule the ticks follow; a different one restarts them.
    #[serde(default)]
    pub(crate) schedule: String,
    /// The next tick.
    pub(crate) next_due_ms: u64,
    /// The tick being fired, while one is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) firing_ms: Option<u64>,
    /// Turns released in the firing tick.
    #[serde(default)]
    pub(crate) released: u32,
}

/// The scheduler over one document store.
#[derive(Debug, Clone)]
pub(crate) struct Scheduler {
    docs: Arc<dyn DocumentStore>,
    staged: Repo<Staged>,
    states: Repo<State>,
}

/// One entry per thread, whatever its cadence or lane: a thread's later
/// turns fold into its waiting entry, so a turn never overtakes an older one.
fn staged_id(first: &InboundEvent) -> Result<String> {
    Ok(key_of(&[
        "staged",
        first.source.as_str(),
        first.conversation_key.as_str(),
        first.thread_key.as_deref().unwrap_or(""),
    ])?
    .as_str()
    .to_owned())
}

impl Scheduler {
    pub(crate) fn new(docs: &Arc<dyn DocumentStore>) -> Self {
        Self {
            docs: Arc::clone(docs),
            staged: Repo::new(docs, STAGED),
            states: Repo::new(docs, CADENCE),
        }
    }

    /// The per-second bucket of an instant cadence's budget.
    pub(crate) fn instant_limit(budget: Budget) -> RateLimit {
        RateLimit::new(budget.max_turns, budget.max_turns.saturating_mul(60))
    }

    /// The next tick of a scheduled cadence, creating its schedule on first
    /// use. `None` for a cron that never fires.
    pub(crate) async fn next_due(&self, resolved: &Resolved, now: u64) -> Result<Option<u64>> {
        let Some(first) = resolved.cadence.next_tick(now) else {
            return Ok(None);
        };
        let schedule = resolved.cadence.fingerprint();
        let written = self
            .states
            .update(&resolved.key, |current| match current {
                Some(state) if state.schedule == schedule => Ok(None),
                _ => Ok(Some(State {
                    schedule: schedule.clone(),
                    next_due_ms: first,
                    ..State::default()
                })),
            })
            .await?;
        if let Some((state, _)) = written {
            return Ok(Some(state.next_due_ms));
        }
        Ok(self
            .states
            .get(&resolved.key)
            .await?
            .map(|found| found.doc.next_due_ms))
    }

    /// Stage a pending turn for `resolved` until `due_ms`, folding it into
    /// the entry of its conversation thread. Returns `false` when every event
    /// was already staged.
    pub(crate) async fn stage(
        &self,
        resolved: &Resolved,
        lane: Lane,
        events: &[InboundEvent],
        absorbed: &[EventRef],
        due_ms: u64,
        now: u64,
    ) -> Result<bool> {
        let Some(first) = events.first() else {
            return Ok(false);
        };
        let id = staged_id(first)?;
        let written = self
            .staged
            .update(&id, |current| {
                let mut entry = current.unwrap_or_else(|| Staged {
                    cadence_key: resolved.key.clone(),
                    lane,
                    events: Vec::new(),
                    absorbed: Vec::new(),
                    first_ms: now,
                    due_ms,
                });
                let held = entry.refs();
                let before = (entry.events.len(), entry.absorbed.len());
                entry.events.extend(
                    events
                        .iter()
                        .filter(|e| !held.contains(&e.reference()))
                        .cloned(),
                );
                entry
                    .absorbed
                    .extend(absorbed.iter().filter(|e| !held.contains(e)).cloned());
                entry.due_ms = entry.due_ms.min(due_ms);
                // The folded entry goes out on the more urgent lane.
                entry.lane = entry.lane.min(lane);
                Ok((before != (entry.events.len(), entry.absorbed.len())).then_some(entry))
            })
            .await?;
        Ok(written.is_some())
    }

    /// The staged entry of `events`' conversation thread, if one waits.
    pub(crate) async fn entry_of(&self, first: &InboundEvent) -> Result<Option<Versioned<Staged>>> {
        let id = staged_id(first)?;
        self.staged.get(&id).await
    }

    /// When the earliest staged entry is due, or `None` when nothing is
    /// staged.
    pub(crate) async fn earliest_due(&self) -> Result<Option<u64>> {
        let query = Query::all().sort(Sort::asc("due_ms")).limit(1);
        let first = self.staged.query(&query).await?.items.into_iter().next();
        Ok(first.map(|found| found.doc.due_ms))
    }

    /// Staged entries due at `now`, oldest first.
    pub(crate) async fn due(&self, now: u64) -> Result<Vec<Versioned<Staged>>> {
        let query = Query::filter(Filter::lte("due_ms", now)).sort(Sort::asc("first_ms"));
        Ok(self.staged.query(&query).await?.items)
    }

    /// Every staged entry of a cadence, oldest first.
    pub(crate) async fn waiting(&self, key: &str) -> Result<Vec<Versioned<Staged>>> {
        let query = Query::filter(Filter::eq("cadence_key", key)).sort(Sort::asc("first_ms"));
        Ok(self.staged.query(&query).await?.items)
    }

    /// Hold a staged entry until `due_ms`.
    pub(crate) async fn defer(&self, entry: &Versioned<Staged>, due_ms: u64) -> Result<()> {
        self.staged
            .update(&entry.id, |current| {
                Ok(current.map(|mut staged| {
                    staged.due_ms = due_ms;
                    staged
                }))
            })
            .await?;
        Ok(())
    }

    /// Remove a released entry.
    pub(crate) async fn remove(&self, entry: &Versioned<Staged>) -> Result<()> {
        self.docs
            .delete(STAGED, &entry.id, Precondition::None)
            .await?;
        Ok(())
    }

    /// Begin (or resume) firing the tick due at or before `now`. Returns the
    /// turns the tick may still release (`u32::MAX` without a budget), or
    /// `None` when no tick is due.
    pub(crate) async fn begin_tick(&self, resolved: &Resolved, now: u64) -> Result<Option<u32>> {
        let Some(state) = self.states.get(&resolved.key).await?.map(|f| f.doc) else {
            return Ok(None);
        };
        if now < state.next_due_ms || state.schedule != resolved.cadence.fingerprint() {
            return Ok(None);
        }
        let tick = state.next_due_ms;
        let written = self
            .states
            .update(&resolved.key, |current| {
                let mut state = current.unwrap_or_default();
                if state.firing_ms != Some(tick) {
                    state.firing_ms = Some(tick);
                    state.released = 0;
                }
                Ok(Some(state))
            })
            .await?;
        let released = written.map_or(0, |(state, _)| state.released);
        Ok(Some(resolved.budget.map_or(u32::MAX, |budget| {
            budget.max_turns.saturating_sub(released)
        })))
    }

    /// Count one release against the firing tick.
    pub(crate) async fn count_release(&self, key: &str) -> Result<()> {
        self.states
            .update(key, |current| {
                Ok(current.map(|mut state| {
                    state.released = state.released.saturating_add(1);
                    state
                }))
            })
            .await?;
        Ok(())
    }

    /// Finish the firing tick: schedule the next one after `now`. Returns it.
    pub(crate) async fn end_tick(&self, resolved: &Resolved, now: u64) -> Result<Option<u64>> {
        let next = resolved.cadence.next_tick(now);
        self.states
            .update(&resolved.key, |current| {
                Ok(current.map(|state| State {
                    schedule: state.schedule,
                    next_due_ms: next.unwrap_or(u64::MAX),
                    firing_ms: None,
                    released: 0,
                }))
            })
            .await?;
        Ok(next)
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
