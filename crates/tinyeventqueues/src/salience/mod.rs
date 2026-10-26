//! Salience: how much an event deserves the agent's attention, from 0 to
//! 100, and the lane that earns it.
//!
//! The score is a sum of rule features — a base, addressed to the agent (or a
//! mention), a direct message, a known contact, a keyword, the source's
//! weight — minus an age penalty, clamped to `0..=100`. Then **habituation**:
//! each recent near-duplicate of the text from the same source (a 64-bit
//! `SimHash` within `max_distance` bits, among the source's last `window`
//! fingerprints within `window_ms`) takes `decay_percent` off the score, so a
//! source that keeps saying the same thing fades the way a ticking clock
//! does. Fingerprints live in the document store, so habituation survives a
//! restart.
//!
//! Scores map to lanes through the configured thresholds; below the
//! background threshold an event is noise and goes to the digest.

mod simhash;

use std::sync::Arc;

use crate::ports::{DocumentStore, key_of};
use serde::{Deserialize, Serialize};
use tinyeventqueues_bus::{
    EventKind, HabituationConfig, InboundEvent, Lane, SalienceClass, SalienceConfig,
    SalienceThresholds,
};

use crate::Result;
use crate::store::HABITUATION;
use crate::store::Repo;

pub use simhash::{distance, simhash};

/// The highest score.
pub const MAX_SCORE: u32 = 100;

/// Whether `entries` names `event`'s sender, as an id or `<source>/<id>`.
pub(crate) fn names_sender(entries: &[String], event: &InboundEvent) -> bool {
    entries.iter().any(|entry| match entry.split_once('/') {
        Some((source, id)) if source == event.source => id == event.sender.id,
        _ => *entry == event.sender.id,
    })
}

/// The rule score of `event` at `now`, before habituation.
#[must_use]
pub fn rule_score(
    event: &InboundEvent,
    config: &SalienceConfig,
    source_weight: i32,
    now: u64,
) -> u32 {
    let weights = &config.weights;
    let mut score = i64::from(weights.base) + i64::from(source_weight);
    if event.addressed_to_agent || event.kind == EventKind::Mention {
        score += i64::from(weights.addressed);
    }
    if event.is_direct {
        score += i64::from(weights.direct);
    }
    if names_sender(&config.known_contacts, event) {
        score += i64::from(weights.known_contact);
    }
    let text = event.text.to_lowercase();
    if config
        .keywords
        .iter()
        .any(|keyword| !keyword.is_empty() && text.contains(&keyword.to_lowercase()))
    {
        score += i64::from(weights.keyword);
    }
    let minutes = now.saturating_sub(event.received_at_ms) / 60_000;
    let penalty = minutes
        .saturating_mul(u64::from(weights.age_penalty_per_minute))
        .min(u64::from(weights.max_age_penalty));
    score -= i64::try_from(penalty).unwrap_or(i64::MAX);
    u32::try_from(score.clamp(0, i64::from(MAX_SCORE))).unwrap_or(0)
}

/// The lane `score` earns, or `None` for noise.
#[must_use]
pub fn lane_for(score: u32, thresholds: &SalienceThresholds) -> Option<Lane> {
    if score >= thresholds.interactive {
        Some(Lane::Interactive)
    } else if score >= thresholds.actionable {
        Some(Lane::Actionable)
    } else if score >= thresholds.background {
        Some(Lane::Background)
    } else {
        None
    }
}

/// The lane a host classification asks for, or `None` for noise.
#[must_use]
pub fn lane_for_class(class: SalienceClass) -> Option<Lane> {
    match class {
        SalienceClass::Urgent => Some(Lane::Interactive),
        SalienceClass::Actionable => Some(Lane::Actionable),
        SalienceClass::Fyi => Some(Lane::Background),
        _ => None,
    }
}

/// Take `decay_percent` off `score` once per near-duplicate.
fn decay(mut score: u32, repeats: usize, decay_percent: u32) -> u32 {
    let keep = 100u32.saturating_sub(decay_percent.min(100));
    for _ in 0..repeats {
        score = score * keep / 100;
        if score == 0 {
            break;
        }
    }
    score
}

/// One source's recent fingerprints.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Fingerprints {
    entries: Vec<(u64, u64)>,
}

/// A score after habituation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Scored {
    pub(crate) score: u32,
    /// Near-duplicates took something off the score.
    pub(crate) habituated: bool,
}

/// Habituation state per source.
#[derive(Debug, Clone)]
pub(crate) struct Habituation {
    repo: Repo<Fingerprints>,
}

impl Habituation {
    pub(crate) fn new(docs: &Arc<dyn DocumentStore>) -> Self {
        Self {
            repo: Repo::new(docs, HABITUATION),
        }
    }

    /// Apply habituation to `score` for `event`, and remember its fingerprint.
    pub(crate) async fn apply(
        &self,
        event: &InboundEvent,
        score: u32,
        config: &HabituationConfig,
        now: u64,
    ) -> Result<Scored> {
        let Some(fingerprint) = simhash(&event.text) else {
            return Ok(Scored {
                score,
                habituated: false,
            });
        };
        let id = key_of(&["habituation", event.source.as_str()])?;
        let window = usize::try_from(config.window).unwrap_or(usize::MAX);
        let mut repeats = 0;
        self.repo
            .update(id.as_str(), |current| {
                let mut prints = current.unwrap_or_default();
                prints
                    .entries
                    .retain(|(_, at)| now.saturating_sub(*at) < config.window_ms);
                repeats = prints
                    .entries
                    .iter()
                    .filter(|(hash, _)| distance(*hash, fingerprint) <= config.max_distance)
                    .count();
                prints.entries.push((fingerprint, now));
                let excess = prints.entries.len().saturating_sub(window);
                prints.entries.drain(..excess);
                Ok(Some(prints))
            })
            .await?;
        Ok(Scored {
            score: decay(score, repeats, config.decay_percent),
            habituated: repeats > 0,
        })
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
