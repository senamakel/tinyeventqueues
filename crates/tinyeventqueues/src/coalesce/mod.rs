//! The coalescer: a durable debounce buffer that gathers a burst of messages
//! into one turn.
//!
//! Events are buffered per `(source, conversation_key, thread_key, sender)`.
//! A buffer is due `debounce_ms` after its last event or `max_wait_ms` after
//! its first, whichever is earlier; an event with media or a control command
//! makes it due at once, and so does reaching its source's `max_events`. A
//! source's own `coalesce` policy replaces the pipeline's timings for its
//! buffers ([`Window`]). Buffers of different threads are never merged, and
//! events keep their arrival order within a buffer.
//!
//! When any buffer of a thread is due, every buffer of that thread is flushed,
//! oldest first, so a flush can never overtake an earlier message of another
//! sender in the same thread.
//!
//! An `edit` whose target is still buffered amends it in place (and shares its
//! fate); a `delete` whose target is still buffered retracts it.

mod types;

use std::collections::HashSet;
use std::sync::Arc;

use crate::ports::{
    DocumentStore, ErrorKind, Filter, Precondition, Query, Sort, Versioned, key_of,
};
use tinyeventqueues_bus::{CoalesceConfig, EventKind, EventRef, InboundEvent, PipelineConfig};

use crate::Result;
use crate::store::COALESCE;
use crate::store::Repo;

pub(crate) use types::Buffer;

/// What adding an event did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Added {
    /// Buffered (or amended a buffered event).
    Buffered,
    /// Already buffered: a replay.
    Duplicate,
    /// A delete retracted buffered events; these events end here.
    Retracted(Vec<EventRef>),
}

/// A due buffer, read at a version.
#[derive(Debug, Clone)]
pub(crate) struct Due {
    pub(crate) id: String,
    pub(crate) found: Versioned<Buffer>,
}

/// The debounce one event's buffer runs under: the pipeline's
/// [`CoalesceConfig`], with its source's own `coalesce` overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Window {
    pub(crate) debounce_ms: u64,
    pub(crate) max_wait_ms: u64,
    /// A buffer holding this many events is due at once; a flush hands at
    /// most this many to one turn.
    pub(crate) max_events: Option<u32>,
    pub(crate) control_prefixes: Vec<String>,
}

impl From<&CoalesceConfig> for Window {
    fn from(config: &CoalesceConfig) -> Self {
        Self {
            debounce_ms: config.debounce_ms,
            max_wait_ms: config.max_wait_ms,
            max_events: None,
            control_prefixes: config.control_prefixes.clone(),
        }
    }
}

impl Window {
    /// The window of `source`'s buffers under `config`.
    pub(crate) fn of(config: &PipelineConfig, source: &str) -> Self {
        let mut window = Self::from(&config.coalesce);
        if let Some(own) = config.source_policy(source).coalesce {
            window.debounce_ms = own.debounce_ms.unwrap_or(window.debounce_ms);
            window.max_wait_ms = own.max_wait_ms.unwrap_or(window.max_wait_ms);
            window.max_events = own.max_events;
        }
        window
    }

    /// Whether `held` events fill a buffer.
    fn is_full(&self, held: usize) -> bool {
        self.max_events
            .is_some_and(|max| held >= usize::try_from(max).unwrap_or(usize::MAX))
    }
}

/// Whether `event` flushes its buffer at once.
fn flushes_now(event: &InboundEvent, config: &Window) -> bool {
    let text = event.text.trim_start();
    !event.media.is_empty()
        || config
            .control_prefixes
            .iter()
            .any(|prefix| !prefix.is_empty() && text.starts_with(prefix.as_str()))
}

/// The thread a buffer belongs to.
fn thread_group(event: &InboundEvent) -> Result<String> {
    Ok(key_of(&[
        event.source.as_str(),
        event.conversation_key.as_str(),
        event.thread_key.as_deref().unwrap_or(""),
    ])?
    .as_str()
    .to_owned())
}

fn buffer_id(event: &InboundEvent) -> Result<String> {
    Ok(key_of(&[
        "buffer",
        event.source.as_str(),
        event.conversation_key.as_str(),
        event.thread_key.as_deref().unwrap_or(""),
        event.sender.id.as_str(),
    ])?
    .as_str()
    .to_owned())
}

/// The debounce buffer over one document store.
#[derive(Debug, Clone)]
pub(crate) struct Coalescer {
    docs: Arc<dyn DocumentStore>,
    repo: Repo<Buffer>,
}

impl Coalescer {
    pub(crate) fn new(docs: &Arc<dyn DocumentStore>) -> Self {
        Self {
            docs: Arc::clone(docs),
            repo: Repo::new(docs, COALESCE),
        }
    }

    /// Add `event`, which arrived at `arrival_ms`, to its buffer.
    pub(crate) async fn add(
        &self,
        event: &InboundEvent,
        arrival_ms: u64,
        config: &Window,
        now: u64,
    ) -> Result<Added> {
        let id = buffer_id(event)?;
        let group = thread_group(event)?;
        let mut added = Added::Buffered;
        let written = self
            .repo
            .update(&id, |current| {
                let mut buffer = current.unwrap_or_else(|| Buffer::empty(&group, arrival_ms, now));
                if buffer.holds(event) {
                    added = Added::Duplicate;
                    return Ok(None);
                }
                added = buffer.add(event);
                buffer.last_ms = now;
                buffer.urgent |= flushes_now(event, config) || config.is_full(buffer.events.len());
                buffer.due_ms = if buffer.urgent {
                    buffer.due_ms.min(now)
                } else {
                    buffer
                        .first_ms
                        .saturating_add(config.max_wait_ms)
                        .min(now.saturating_add(config.debounce_ms))
                };
                Ok(Some(buffer))
            })
            .await?;
        if let Some((buffer, version)) = written
            && buffer.is_empty()
        {
            self.delete_at(&id, Precondition::Version(version)).await?;
        }
        Ok(added)
    }

    /// Apply the amendment `event` (an edit or delete) if its target is still
    /// buffered. Returns `None`, writing nothing, when it is not.
    pub(crate) async fn try_amend(&self, event: &InboundEvent) -> Result<Option<Added>> {
        if !amends(event) {
            return Ok(None);
        }
        let id = buffer_id(event)?;
        let mut added = None;
        let written = self
            .repo
            .update(&id, |current| {
                let Some(mut buffer) = current else {
                    return Ok(None);
                };
                if buffer.holds(event) {
                    added = Some(Added::Duplicate);
                    return Ok(None);
                }
                if !buffer.targets(event) {
                    return Ok(None);
                }
                added = Some(buffer.add(event));
                Ok(Some(buffer))
            })
            .await?;
        if let Some((buffer, version)) = written
            && buffer.is_empty()
        {
            self.delete_at(&id, Precondition::Version(version)).await?;
        }
        Ok(added)
    }

    /// Every buffer that should flush at `now`, with the rest of its thread,
    /// oldest arrival first within each thread. At most `limit` threads.
    /// When the earliest buffer flushes, or `None` when nothing is buffered.
    pub(crate) async fn earliest_due(&self) -> Result<Option<u64>> {
        let query = Query::all().sort(Sort::asc("due_ms")).limit(1);
        let first = self.repo.query(&query).await?.items.into_iter().next();
        Ok(first
            .map(|found| found.doc.due_ms)
            .filter(|due| *due != u64::MAX))
    }

    pub(crate) async fn due(&self, now: u64, limit: usize) -> Result<Vec<Due>> {
        let query = Query::filter(Filter::lte("due_ms", now))
            .sort(Sort::asc("due_ms"))
            .limit(limit);
        let mut groups: Vec<String> = Vec::new();
        for found in self.repo.query(&query).await?.items {
            if !groups.contains(&found.doc.thread_group) {
                groups.push(found.doc.thread_group);
            }
        }
        groups.truncate(limit);
        let mut due = Vec::new();
        let mut seen = HashSet::new();
        for group in groups {
            let query = Query::filter(Filter::eq("thread_group", group.as_str()));
            let mut thread: Vec<Due> = self
                .repo
                .query(&query)
                .await?
                .items
                .into_iter()
                .map(|found| Due {
                    id: found.id.clone(),
                    found,
                })
                .collect();
            thread.sort_by_key(|due| {
                (
                    due.found.doc.arrival_ms,
                    due.found.doc.first_ms,
                    due.id.clone(),
                )
            });
            for item in thread {
                if seen.insert(item.id.clone()) {
                    due.push(item);
                }
            }
        }
        Ok(due)
    }

    /// Remove what `flushed` held. Events that arrived after it was read stay.
    pub(crate) async fn remove(&self, flushed: &Due) -> Result<()> {
        if self
            .delete_at(&flushed.id, Precondition::Version(flushed.found.version))
            .await?
        {
            return Ok(());
        }
        let taken: HashSet<EventRef> = flushed.found.doc.refs().into_iter().collect();
        let written = self
            .repo
            .update(&flushed.id, |current| {
                let Some(mut buffer) = current else {
                    return Ok(None);
                };
                buffer.events.retain(|e| !taken.contains(&e.reference()));
                buffer.absorbed.retain(|a| !taken.contains(&a.event));
                Ok(Some(buffer))
            })
            .await?;
        if let Some((buffer, version)) = written
            && buffer.is_empty()
        {
            self.delete_at(&flushed.id, Precondition::Version(version))
                .await?;
        }
        Ok(())
    }

    /// Delete a buffer if it is still at `precondition`. Returns `false` when
    /// it changed in between.
    async fn delete_at(&self, id: &str, precondition: Precondition) -> Result<bool> {
        match self.docs.delete(COALESCE, id, precondition).await {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == ErrorKind::Conflict => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}

/// Whether an event is an amendment of an earlier one.
pub(crate) fn amends(event: &InboundEvent) -> bool {
    matches!(event.kind, EventKind::Edit | EventKind::Delete) && event.target_event_id.is_some()
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
