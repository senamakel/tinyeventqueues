//! The ambient buffer: unaddressed group chatter kept per conversation and
//! handed to the conversation's next real turn as context.

use std::sync::Arc;

use crate::ports::{DocumentStore, Key, key_of};
use serde::{Deserialize, Serialize};
use tinyeventqueues_bus::{AmbientConfig, EventRef, InboundEvent};

use crate::Result;
use crate::store::AMBIENT;
use crate::store::Repo;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Buffer {
    events: Vec<InboundEvent>,
}

/// Ambient context per conversation.
#[derive(Debug, Clone)]
pub(crate) struct Ambient {
    repo: Repo<Buffer>,
}

fn id(source: &str, conversation: &str) -> Result<Key> {
    Ok(key_of(&["ambient", source, conversation])?)
}

impl Ambient {
    pub(crate) fn new(docs: &Arc<dyn DocumentStore>) -> Self {
        Self {
            repo: Repo::new(docs, AMBIENT),
        }
    }

    /// Keep `event` for its conversation, evicting the oldest beyond
    /// `config.max_items`. Keeping the same event twice is a no-op.
    pub(crate) async fn keep(&self, event: &InboundEvent, config: &AmbientConfig) -> Result<()> {
        let key = id(&event.source, &event.conversation_key)?;
        let max = usize::try_from(config.max_items).unwrap_or(usize::MAX);
        self.repo
            .update(key.as_str(), |current| {
                let mut buffer = current.unwrap_or_default();
                if buffer.events.iter().any(|e| e.event_id == event.event_id) {
                    return Ok(None);
                }
                buffer.events.push(event.clone());
                let excess = buffer.events.len().saturating_sub(max);
                buffer.events.drain(..excess);
                Ok(Some(buffer))
            })
            .await?;
        Ok(())
    }

    /// The context kept for a conversation, oldest first.
    pub(crate) async fn peek(&self, source: &str, conversation: &str) -> Result<Vec<InboundEvent>> {
        let key = id(source, conversation)?;
        Ok(self
            .repo
            .get(key.as_str())
            .await?
            .map(|found| found.doc.events)
            .unwrap_or_default())
    }

    /// Forget `taken` once a turn carries them.
    pub(crate) async fn release(
        &self,
        source: &str,
        conversation: &str,
        taken: &[EventRef],
    ) -> Result<()> {
        if taken.is_empty() {
            return Ok(());
        }
        let key = id(source, conversation)?;
        self.repo
            .update(key.as_str(), |current| {
                let Some(mut buffer) = current else {
                    return Ok(None);
                };
                buffer.events.retain(|e| !taken.contains(&e.reference()));
                Ok(Some(buffer))
            })
            .await?;
        Ok(())
    }
}
