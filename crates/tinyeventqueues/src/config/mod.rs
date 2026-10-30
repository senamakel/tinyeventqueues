//! Loading and validating the two editable files the pipeline runs from.
//!
//! - `queues.json`, a private owner config: every queue the pipeline uses,
//!   with its attempts, dedupe window and lease length. Host grants remain
//!   host-owned; this declaration only configures the queue port.
//! - `pipeline.json`, a [`PipelineConfig`]: the pipeline's own tunables, which
//!   refer to manifest queues by name.
//!
//! Both are strict. Unknown fields are rejected, and every error names the
//! offending field by its JSON path (`lanes.digest.concurrency`,
//! `sources[2].match`), so whoever edits the file — a person or an agent — is
//! told exactly what to fix. The repository root ships both files, with JSON
//! Schemas in `schemas/`; [`shipped_manifest`] and
//! [`shipped_pipeline_config`] return the copies compiled into this crate.

mod types;

use std::path::Path;

use tinyeventqueues_bus::{Budget, Lane, PipelineConfig};

use crate::cadence::Cadence;
use crate::salience::MAX_SCORE;
use crate::{Error, Result};

pub use types::{QueueDeclaration, QueueManifest, QueueRef, ResolvedQueues};

/// The `queues.json` shipped at the repository root (the contract's copy, so a
/// host and this crate read one file).
pub use tinyeventqueues_bus::storage::SHIPPED_QUEUES_JSON;

/// The `pipeline.json` shipped at the repository root.
pub use tinyeventqueues_bus::storage::SHIPPED_PIPELINE_JSON;

/// The pipeline configuration format version this crate reads.
pub const PIPELINE_CONFIG_VERSION: u32 = 1;

/// The most ingress messages one pass may take.
const MAX_BATCH_SIZE: u32 = 10_000;

/// The most turns of one lane that may be in flight at once.
const MAX_CONCURRENCY: u32 = 1_024;

/// Parse the shipped `queues.json`.
///
/// # Errors
///
/// [`Error::Manifest`] if the shipped file does not validate.
pub fn shipped_manifest() -> Result<QueueManifest> {
    parse_manifest(SHIPPED_QUEUES_JSON)
}

/// Parse and check the shipped `pipeline.json`.
///
/// # Errors
///
/// [`Error::Config`] if the shipped file does not validate.
pub fn shipped_pipeline_config() -> Result<PipelineConfig> {
    parse_pipeline_config(SHIPPED_PIPELINE_JSON)
}

/// Parse a queue manifest.
///
/// # Errors
///
/// [`Error::Manifest`] naming the offending field or queue.
pub fn parse_manifest(text: &str) -> Result<QueueManifest> {
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let manifest: QueueManifest = serde_path_to_error::deserialize(&mut deserializer)
        .map_err(|error| Error::manifest(format!("{}: {}", error.path(), error.inner())))?;
    deserializer
        .end()
        .map_err(|error| Error::manifest(error.to_string()))?;
    manifest.validate()?;
    Ok(manifest)
}

/// Read and parse the queue manifest at `path`.
///
/// # Errors
///
/// [`Error::Manifest`] when the file is missing, unreadable or invalid.
pub fn load_manifest(path: impl AsRef<Path>) -> Result<QueueManifest> {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path).map_err(|error| {
        Error::Manifest(crate::ports::StorageError::invalid_input(format!(
            "{}: {error}",
            path.display()
        )))
    })?;
    parse_manifest(&text)
}

/// Parse a pipeline configuration strictly and check its values.
///
/// This checks everything that does not need the queue manifest; the queue
/// references are checked by [`resolve_queues`].
///
/// # Errors
///
/// [`Error::Config`] with the JSON path of the first offending field: an
/// unknown field, a missing one, a wrong type, or a value out of range.
pub fn parse_pipeline_config(text: &str) -> Result<PipelineConfig> {
    let deserializer = &mut serde_json::Deserializer::from_str(text);
    let config: PipelineConfig =
        serde_path_to_error::deserialize(deserializer).map_err(|error| {
            let path = error.path().to_string();
            let field = if matches!(path.as_str(), "." | "?") {
                "(root)".to_owned()
            } else {
                path
            };
            Error::config(field, error.into_inner().to_string())
        })?;
    check_values(&config)?;
    Ok(config)
}

/// Read and parse the pipeline configuration at `path`.
///
/// # Errors
///
/// [`Error::ConfigFile`] when the file cannot be read, otherwise as
/// [`parse_pipeline_config`].
pub fn load_pipeline_config(path: impl AsRef<Path>) -> Result<PipelineConfig> {
    let path = path.as_ref();
    let text = std::fs::read_to_string(path).map_err(|source| Error::ConfigFile {
        path: path.display().to_string(),
        source,
    })?;
    parse_pipeline_config(&text)
}

/// Check the values of a configuration that already decoded.
///
/// # Errors
///
/// [`Error::Config`] naming the first offending field.
pub fn check_values(config: &PipelineConfig) -> Result<()> {
    if config.version != PIPELINE_CONFIG_VERSION {
        return Err(Error::config(
            "version",
            format!(
                "version {} is not supported, expected {PIPELINE_CONFIG_VERSION}",
                config.version
            ),
        ));
    }
    if config.ingress.tombstone_ttl_ms == 0 {
        return Err(Error::config(
            "ingress.tombstone_ttl_ms",
            "must be at least 1",
        ));
    }
    if !(1..=MAX_BATCH_SIZE).contains(&config.ingress.batch_size) {
        return Err(Error::config(
            "ingress.batch_size",
            format!("must be 1 to {MAX_BATCH_SIZE}"),
        ));
    }
    for lane in Lane::ALL {
        if !(1..=MAX_CONCURRENCY).contains(&config.lanes.get(lane).concurrency) {
            return Err(Error::config(
                format!("lanes.{}.concurrency", lane.as_str()),
                format!("must be 1 to {MAX_CONCURRENCY}"),
            ));
        }
        if config.lanes.get(lane).max_backlog == 0 {
            return Err(Error::config(
                format!("lanes.{}.max_backlog", lane.as_str()),
                "must be at least 1",
            ));
        }
        let at = format!("lanes.{}", lane.as_str());
        check_cadence(
            &at,
            config.lanes.get(lane).cadence.as_deref(),
            config.lanes.get(lane).budget,
        )?;
    }
    check_gate(config)?;
    check_salience(config)?;
    check_digest(config)?;
    check_dispatch(config)?;
    for (index, prefix) in config.coalesce.control_prefixes.iter().enumerate() {
        if prefix.is_empty() {
            return Err(Error::config(
                format!("coalesce.control_prefixes[{index}]"),
                "must not be empty",
            ));
        }
    }
    for (index, source) in config.sources.iter().enumerate() {
        let pattern = source.pattern.as_str();
        let body = pattern.strip_suffix('*').unwrap_or(pattern);
        if pattern.is_empty() || body.contains('*') {
            return Err(Error::config(
                format!("sources[{index}].match"),
                "must be a source, a prefix ending in `*`, or `*`",
            ));
        }
        check_cadence(
            &format!("sources[{index}]"),
            source.cadence.as_deref(),
            source.budget,
        )?;
        if !(-100..=100).contains(&source.weight) {
            return Err(Error::config(
                format!("sources[{index}].weight"),
                "must be -100 to 100",
            ));
        }
        if source
            .coalesce
            .is_some_and(|coalesce| coalesce.max_events == Some(0))
        {
            return Err(Error::config(
                format!("sources[{index}].coalesce.max_events"),
                "must be at least 1",
            ));
        }
    }
    Ok(())
}

/// The most ambient events kept per conversation.
const MAX_AMBIENT_ITEMS: u32 = 10_000;

fn check_gate(config: &PipelineConfig) -> Result<()> {
    let gate = &config.gate;
    for (list, entries) in [
        ("allow_senders", &gate.allow_senders),
        ("deny_senders", &gate.deny_senders),
    ] {
        if let Some(index) = entries.iter().position(String::is_empty) {
            return Err(Error::config(
                format!("gate.{list}[{index}]"),
                "must not be empty",
            ));
        }
    }
    if gate.bot_loop.max_events == 0 {
        return Err(Error::config(
            "gate.bot_loop.max_events",
            "must be at least 1",
        ));
    }
    if gate.bot_loop.window_ms == 0 {
        return Err(Error::config(
            "gate.bot_loop.window_ms",
            "must be at least 1",
        ));
    }
    let limits = &gate.rate_limits;
    for (scope, limit) in [
        ("per_sender", limits.per_sender),
        ("per_conversation", limits.per_conversation),
        ("per_source", limits.per_source),
    ] {
        let Some(limit) = limit else { continue };
        if limit.capacity == 0 {
            return Err(Error::config(
                format!("gate.rate_limits.{scope}.capacity"),
                "must be at least 1",
            ));
        }
        if limit.refill_per_minute == 0 {
            return Err(Error::config(
                format!("gate.rate_limits.{scope}.refill_per_minute"),
                "must be at least 1",
            ));
        }
    }
    if !(1..=MAX_AMBIENT_ITEMS).contains(&gate.ambient.max_items) {
        return Err(Error::config(
            "gate.ambient.max_items",
            format!("must be 1 to {MAX_AMBIENT_ITEMS}"),
        ));
    }
    Ok(())
}

fn check_cadence(at: &str, cadence: Option<&str>, budget: Option<Budget>) -> Result<()> {
    if let Some(text) = cadence {
        Cadence::parse(text)
            .map_err(|error| Error::config(format!("{at}.cadence"), error.to_string()))?;
    }
    if let Some(budget) = budget {
        for (name, value) in [
            ("max_turns", u64::from(budget.max_turns)),
            ("backlog_ticks", u64::from(budget.backlog_ticks)),
            ("degrade_debounce_ms", budget.degrade_debounce_ms),
        ] {
            if value == 0 {
                return Err(Error::config(
                    format!("{at}.budget.{name}"),
                    "must be at least 1",
                ));
            }
        }
    }
    Ok(())
}

fn check_salience(config: &PipelineConfig) -> Result<()> {
    let salience = &config.salience;
    let weights = &salience.weights;
    for (name, value) in [
        ("base", weights.base),
        ("addressed", weights.addressed),
        ("direct", weights.direct),
        ("known_contact", weights.known_contact),
        ("keyword", weights.keyword),
        ("age_penalty_per_minute", weights.age_penalty_per_minute),
        ("max_age_penalty", weights.max_age_penalty),
    ] {
        if value > MAX_SCORE {
            return Err(Error::config(
                format!("salience.weights.{name}"),
                format!("must be 0 to {MAX_SCORE}"),
            ));
        }
    }
    for (list, entries) in [
        ("keywords", &salience.keywords),
        ("known_contacts", &salience.known_contacts),
    ] {
        if let Some(index) = entries.iter().position(String::is_empty) {
            return Err(Error::config(
                format!("salience.{list}[{index}]"),
                "must not be empty",
            ));
        }
    }
    let thresholds = &salience.thresholds;
    if thresholds.interactive > MAX_SCORE {
        return Err(Error::config(
            "salience.thresholds.interactive",
            format!("must be 0 to {MAX_SCORE}"),
        ));
    }
    if thresholds.actionable > thresholds.interactive {
        return Err(Error::config(
            "salience.thresholds.actionable",
            "must not exceed salience.thresholds.interactive",
        ));
    }
    if thresholds.background > thresholds.actionable {
        return Err(Error::config(
            "salience.thresholds.background",
            "must not exceed salience.thresholds.actionable",
        ));
    }
    if let Some(band) = salience.borderline
        && (band.low >= band.high || band.high > MAX_SCORE + 1)
    {
        return Err(Error::config(
            "salience.borderline",
            format!("needs low < high <= {}", MAX_SCORE + 1),
        ));
    }
    let habituation = &salience.habituation;
    if !(1..=MAX_HABITUATION_WINDOW).contains(&habituation.window) {
        return Err(Error::config(
            "salience.habituation.window",
            format!("must be 1 to {MAX_HABITUATION_WINDOW}"),
        ));
    }
    if habituation.window_ms == 0 {
        return Err(Error::config(
            "salience.habituation.window_ms",
            "must be at least 1",
        ));
    }
    if habituation.max_distance > 64 {
        return Err(Error::config(
            "salience.habituation.max_distance",
            "must be 0 to 64",
        ));
    }
    if habituation.decay_percent > 100 {
        return Err(Error::config(
            "salience.habituation.decay_percent",
            "must be 0 to 100",
        ));
    }
    Ok(())
}

fn check_dispatch(config: &PipelineConfig) -> Result<()> {
    let dispatch = &config.dispatch;
    if dispatch.backoff_base_ms == 0 {
        return Err(Error::config(
            "dispatch.backoff_base_ms",
            "must be at least 1",
        ));
    }
    if dispatch.backoff_max_ms < dispatch.backoff_base_ms {
        return Err(Error::config(
            "dispatch.backoff_max_ms",
            "must be at least dispatch.backoff_base_ms",
        ));
    }
    if dispatch.gap_timeout_ms == 0 {
        return Err(Error::config(
            "dispatch.gap_timeout_ms",
            "must be at least 1",
        ));
    }
    Ok(())
}

fn check_digest(config: &PipelineConfig) -> Result<()> {
    if !(1..=MAX_DIGEST_ITEMS).contains(&config.digest.max_items) {
        return Err(Error::config(
            "digest.max_items",
            format!("must be 1 to {MAX_DIGEST_ITEMS}"),
        ));
    }
    if config.digest.max_wait_ms == 0 {
        return Err(Error::config("digest.max_wait_ms", "must be at least 1"));
    }
    Ok(())
}

/// The most fingerprints habituation keeps per source.
const MAX_HABITUATION_WINDOW: u32 = 1_024;

/// The most events one digest turn carries.
const MAX_DIGEST_ITEMS: u32 = 10_000;

/// Resolve every queue the configuration names against the manifest.
///
/// # Errors
///
/// [`Error::Config`] naming the field whose queue is not declared, has no
/// `visibility_ms`, or is already used by another role.
pub fn resolve_queues(config: &PipelineConfig, manifest: &QueueManifest) -> Result<ResolvedQueues> {
    let mut used: Vec<(String, &str)> = Vec::new();
    let mut resolve = |field: String, name: &str| -> Result<QueueRef> {
        let Some(declared) = manifest.get(name) else {
            return Err(Error::config(
                field,
                format!("queue {name:?} is not declared in the queue manifest"),
            ));
        };
        let visibility_ms = declared.visibility_ms;
        if visibility_ms == 0 {
            return Err(Error::config(
                field,
                "queue must declare a positive visibility_ms",
            ));
        }
        if let Some((other, _)) = used.iter().find(|(_, queue)| *queue == name) {
            return Err(Error::config(
                field,
                format!("queue {name:?} is already used by {other}"),
            ));
        }
        used.push((field, &declared.name));
        Ok(QueueRef {
            name: declared.name.clone(),
            visibility_ms,
            max_attempts: declared.max_attempts,
        })
    };
    let ingress = resolve("ingress.queue".to_owned(), &config.ingress.queue)?;
    let dead = resolve("dead_letter.queue".to_owned(), &config.dead_letter.queue)?;
    let mut lane = |lane: Lane| {
        let field = format!("lanes.{}.queue", lane.as_str());
        resolve(field, &config.lanes.get(lane).queue)
    };
    let lanes = [
        lane(Lane::Interactive)?,
        lane(Lane::Actionable)?,
        lane(Lane::Background)?,
        lane(Lane::Digest)?,
    ];
    Ok(ResolvedQueues {
        ingress,
        dead,
        lanes,
    })
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
