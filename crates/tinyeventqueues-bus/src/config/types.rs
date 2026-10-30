//! Configuration types.

use serde::{Deserialize, Serialize};

/// The whole pipeline configuration (`pipeline.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PipelineConfig {
    /// The JSON Schema the file declares, for editors. Ignored by the module.
    #[serde(rename = "$schema", default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// The file format version. Only `1` exists.
    pub version: u32,
    /// The ingress stage.
    pub ingress: IngressConfig,
    /// Where dead-lettered work is parked.
    pub dead_letter: DeadLetterConfig,
    /// The attention lanes.
    pub lanes: LanesConfig,
    /// Per-source policies, first match wins. A source no entry matches gets
    /// [`SourcePolicy::default`].
    #[serde(default)]
    pub sources: Vec<SourcePolicy>,
    /// The deterministic gate.
    #[serde(default)]
    pub gate: GateConfig,
    /// The debounce buffer.
    #[serde(default)]
    pub coalesce: CoalesceConfig,
    /// Salience scoring and habituation.
    #[serde(default)]
    pub salience: SalienceConfig,
    /// The digest accumulator.
    #[serde(default)]
    pub digest: DigestConfig,
    /// Delivery to the host.
    #[serde(default)]
    pub dispatch: DispatchConfig,
}

/// Delivery to the host: retries back off exponentially from
/// `backoff_base_ms` to `backoff_max_ms`; a turn that uses up its lane
/// queue's manifest `max_attempts` is dead-lettered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DispatchConfig {
    /// The first retry delay, in milliseconds.
    #[serde(default = "default_backoff_base_ms")]
    pub backoff_base_ms: u64,
    /// The longest retry delay, in milliseconds.
    #[serde(default = "default_backoff_max_ms")]
    pub backoff_max_ms: u64,
    /// On a queue backend without groups, how long a conversation waits for
    /// a missing turn (one whose enqueue was lost to a crash) before
    /// skipping it, in milliseconds.
    #[serde(default = "default_gap_timeout_ms")]
    pub gap_timeout_ms: u64,
}

fn default_backoff_base_ms() -> u64 {
    1_000
}
fn default_backoff_max_ms() -> u64 {
    300_000
}
fn default_gap_timeout_ms() -> u64 {
    120_000
}

impl Default for DispatchConfig {
    fn default() -> Self {
        Self {
            backoff_base_ms: default_backoff_base_ms(),
            backoff_max_ms: default_backoff_max_ms(),
            gap_timeout_ms: default_gap_timeout_ms(),
        }
    }
}

/// Salience: how much an event deserves the agent's attention, from 0 to
/// 100, and which lane that earns it.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SalienceConfig {
    /// Points per feature.
    #[serde(default)]
    pub weights: SalienceWeights,
    /// Words that mark an event as worth attention (case-insensitive).
    #[serde(default)]
    pub keywords: Vec<String>,
    /// Senders the agent knows: sender ids or `<source>/<sender id>`.
    #[serde(default)]
    pub known_contacts: Vec<String>,
    /// Score thresholds for each lane.
    #[serde(default)]
    pub thresholds: SalienceThresholds,
    /// The band of scores the host's `ClassifySalience` decides; absent, the
    /// rule score always stands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub borderline: Option<Borderline>,
    /// Decay for content a source keeps repeating.
    #[serde(default)]
    pub habituation: HabituationConfig,
}

/// Points each salience feature adds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SalienceWeights {
    /// Base points every event starts with.
    #[serde(default = "default_base")]
    pub base: u32,
    /// The event is addressed to the agent, or a mention.
    #[serde(default = "default_addressed")]
    pub addressed: u32,
    /// The conversation is a direct message.
    #[serde(default = "default_addressed")]
    pub direct: u32,
    /// The sender is a known contact.
    #[serde(default = "default_known_contact")]
    pub known_contact: u32,
    /// The text holds a keyword.
    #[serde(default = "default_keyword")]
    pub keyword: u32,
    /// Points lost per minute between receipt and scoring.
    #[serde(default = "default_recency")]
    pub age_penalty_per_minute: u32,
    /// The most points age can cost.
    #[serde(default = "default_max_age_penalty")]
    pub max_age_penalty: u32,
}

fn default_base() -> u32 {
    25
}
fn default_addressed() -> u32 {
    50
}
fn default_known_contact() -> u32 {
    15
}
fn default_keyword() -> u32 {
    20
}
fn default_recency() -> u32 {
    1
}
fn default_max_age_penalty() -> u32 {
    20
}

impl Default for SalienceWeights {
    fn default() -> Self {
        Self {
            base: default_base(),
            addressed: default_addressed(),
            direct: default_addressed(),
            known_contact: default_known_contact(),
            keyword: default_keyword(),
            age_penalty_per_minute: default_recency(),
            max_age_penalty: default_max_age_penalty(),
        }
    }
}

/// The lowest score of each lane. Below `background`, an event is noise and
/// goes to the digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SalienceThresholds {
    /// The lowest interactive score.
    #[serde(default = "default_interactive")]
    pub interactive: u32,
    /// The lowest actionable score.
    #[serde(default = "default_actionable")]
    pub actionable: u32,
    /// The lowest background score.
    #[serde(default = "default_background")]
    pub background: u32,
}

fn default_interactive() -> u32 {
    70
}
fn default_actionable() -> u32 {
    40
}
fn default_background() -> u32 {
    15
}

impl Default for SalienceThresholds {
    fn default() -> Self {
        Self {
            interactive: default_interactive(),
            actionable: default_actionable(),
            background: default_background(),
        }
    }
}

/// Scores in `low..high` are borderline: the host's `ClassifySalience`
/// decides them, at most `budget_per_hour` times an hour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Borderline {
    /// The lowest borderline score.
    pub low: u32,
    /// The first score above the band.
    pub high: u32,
    /// Host classifications allowed per hour.
    pub budget_per_hour: u32,
}

impl Borderline {
    /// A band `low..high` with `budget_per_hour` classifications.
    #[must_use]
    pub fn new(low: u32, high: u32, budget_per_hour: u32) -> Self {
        Self {
            low,
            high,
            budget_per_hour,
        }
    }
}

/// Habituation: content a source repeats loses salience. Each recent
/// near-duplicate (`SimHash` within `max_distance` bits) in the source's last
/// `window` events within `window_ms` takes `decay_percent` off the score.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct HabituationConfig {
    /// Fingerprints kept per source.
    #[serde(default = "default_habituation_window")]
    pub window: u32,
    /// How long a fingerprint counts, in milliseconds.
    #[serde(default = "default_habituation_window_ms")]
    pub window_ms: u64,
    /// The most differing bits for two texts to count as near-duplicates.
    #[serde(default = "default_max_distance")]
    pub max_distance: u32,
    /// The share of the score each near-duplicate takes, in percent.
    #[serde(default = "default_decay_percent")]
    pub decay_percent: u32,
}

fn default_habituation_window() -> u32 {
    64
}
fn default_habituation_window_ms() -> u64 {
    3_600_000
}
fn default_max_distance() -> u32 {
    3
}
fn default_decay_percent() -> u32 {
    40
}

impl Default for HabituationConfig {
    fn default() -> Self {
        Self {
            window: default_habituation_window(),
            window_ms: default_habituation_window_ms(),
            max_distance: default_max_distance(),
            decay_percent: default_decay_percent(),
        }
    }
}

/// The digest accumulator: spilled, overflowing and noisy events gather here
/// and go out as one digest turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DigestConfig {
    /// Release a digest turn once it holds this many events.
    #[serde(default = "default_digest_items")]
    pub max_items: u32,
    /// Release a digest turn this long after its first event at the latest.
    #[serde(default = "default_digest_wait_ms")]
    pub max_wait_ms: u64,
}

fn default_digest_items() -> u32 {
    200
}
fn default_digest_wait_ms() -> u64 {
    600_000
}

impl Default for DigestConfig {
    fn default() -> Self {
        Self {
            max_items: default_digest_items(),
            max_wait_ms: default_digest_wait_ms(),
        }
    }
}

/// The deterministic gate: who may reach the agent, and how often.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct GateConfig {
    /// If not empty, only these senders pass. An entry is a sender id, or
    /// `<source>/<sender id>` to name one source's sender.
    #[serde(default)]
    pub allow_senders: Vec<String>,
    /// These senders never pass; same entry forms as `allow_senders`. The
    /// deny list wins over the allow list.
    #[serde(default)]
    pub deny_senders: Vec<String>,
    /// Direct messages may reach the agent.
    #[serde(default = "enabled")]
    pub direct_messages: bool,
    /// Group conversations may reach the agent.
    #[serde(default = "enabled")]
    pub groups: bool,
    /// The bot-loop guard.
    #[serde(default)]
    pub bot_loop: BotLoopConfig,
    /// Token buckets; an event over any of them spills to the digest.
    #[serde(default)]
    pub rate_limits: RateLimits,
    /// The per-conversation buffer of unaddressed chatter.
    #[serde(default)]
    pub ambient: AmbientConfig,
}

fn enabled() -> bool {
    true
}

impl Default for GateConfig {
    fn default() -> Self {
        Self {
            allow_senders: Vec::new(),
            deny_senders: Vec::new(),
            direct_messages: true,
            groups: true,
            bot_loop: BotLoopConfig::default(),
            rate_limits: RateLimits::default(),
            ambient: AmbientConfig::default(),
        }
    }
}

/// The bot-loop guard: a bot that sends more than `max_events` events into
/// one conversation within `window_ms` is looping with the agent, and the
/// excess is dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct BotLoopConfig {
    /// Events allowed per window.
    #[serde(default = "default_bot_events")]
    pub max_events: u32,
    /// The window, in milliseconds.
    #[serde(default = "default_bot_window_ms")]
    pub window_ms: u64,
}

fn default_bot_events() -> u32 {
    20
}

fn default_bot_window_ms() -> u64 {
    60_000
}

impl Default for BotLoopConfig {
    fn default() -> Self {
        Self {
            max_events: default_bot_events(),
            window_ms: default_bot_window_ms(),
        }
    }
}

/// Token buckets by scope. A missing bucket is unlimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RateLimits {
    /// One bucket per sender (per source).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_sender: Option<RateLimit>,
    /// One bucket per conversation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_conversation: Option<RateLimit>,
    /// One bucket per source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_source: Option<RateLimit>,
}

/// A token bucket: `capacity` events at once, refilled at
/// `refill_per_minute`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct RateLimit {
    /// The burst size.
    pub capacity: u32,
    /// Tokens added per minute.
    pub refill_per_minute: u32,
}

impl RateLimit {
    /// A bucket of `capacity` refilled at `refill_per_minute`.
    #[must_use]
    pub fn new(capacity: u32, refill_per_minute: u32) -> Self {
        Self {
            capacity,
            refill_per_minute,
        }
    }
}

/// The ambient buffer of unaddressed group chatter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct AmbientConfig {
    /// The most events kept per conversation; the oldest are evicted first.
    #[serde(default = "default_ambient_items")]
    pub max_items: u32,
}

fn default_ambient_items() -> u32 {
    50
}

impl Default for AmbientConfig {
    fn default() -> Self {
        Self {
            max_items: default_ambient_items(),
        }
    }
}

/// The debounce buffer keyed by conversation, thread and sender.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct CoalesceConfig {
    /// Flush this long after a key's last event, in milliseconds.
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    /// Flush this long after a key's first event at the latest.
    #[serde(default = "default_max_wait_ms")]
    pub max_wait_ms: u64,
    /// Text prefixes that mark a control command, which flushes at once.
    #[serde(default = "default_control_prefixes")]
    pub control_prefixes: Vec<String>,
}

fn default_debounce_ms() -> u64 {
    1_500
}

fn default_max_wait_ms() -> u64 {
    10_000
}

fn default_control_prefixes() -> Vec<String> {
    vec!["/".to_owned()]
}

impl Default for CoalesceConfig {
    fn default() -> Self {
        Self {
            debounce_ms: default_debounce_ms(),
            max_wait_ms: default_max_wait_ms(),
            control_prefixes: default_control_prefixes(),
        }
    }
}

/// The ingress stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct IngressConfig {
    /// The manifest queue raw events are persisted to.
    pub queue: String,
    /// How long a completed event keeps rejecting redeliveries, in
    /// milliseconds. Used when the queue backend cannot dedupe and the module
    /// keeps its own tombstones.
    #[serde(default = "default_tombstone_ttl_ms")]
    pub tombstone_ttl_ms: u64,
    /// How many ingress messages one pass takes.
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
}

/// Thirty days.
fn default_tombstone_ttl_ms() -> u64 {
    30 * 24 * 60 * 60 * 1000
}

fn default_batch_size() -> u32 {
    64
}

/// Where dead-lettered work is parked for `ListDead` and `Redrive`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct DeadLetterConfig {
    /// The manifest queue dead work is parked in.
    pub queue: String,
}

/// One attention lane per field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct LanesConfig {
    /// Direct messages, mentions and web chat.
    pub interactive: LaneConfig,
    /// Work the agent should act on, not urgently.
    pub actionable: LaneConfig,
    /// Low-salience work.
    pub background: LaneConfig,
    /// Overflow and backlog, summarized into one turn.
    pub digest: LaneConfig,
}

impl LanesConfig {
    /// The configuration of `lane`.
    #[must_use]
    pub fn get(&self, lane: Lane) -> &LaneConfig {
        match lane {
            Lane::Interactive => &self.interactive,
            Lane::Actionable => &self.actionable,
            Lane::Background => &self.background,
            Lane::Digest => &self.digest,
        }
    }
}

/// One attention lane.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct LaneConfig {
    /// The manifest queue the lane's turns wait in.
    pub queue: String,
    /// How many turns of this lane may be in flight at once, across
    /// conversations. A conversation never has more than one.
    #[serde(default = "default_concurrency")]
    pub concurrency: u32,
    /// The most turns the lane may hold waiting; past it, new work spills to
    /// the digest (`lane_overflow`). Ignored for the digest lane itself.
    #[serde(default = "default_max_backlog")]
    pub max_backlog: u64,
    /// When the lane's turns are released: `instant` (the default),
    /// `every(<n><ms|s|m|h|d>)`, or `cron("<min> <hour> <day> <month>
    /// <weekday>")` in UTC. A source's own cadence takes precedence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cadence: Option<String>,
    /// How many turns the cadence releases per tick (per second for
    /// `instant`). Absent, unlimited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<Budget>,
}

/// An ingestion budget for a cadence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Budget {
    /// Turns released per tick; for an `instant` cadence, per second.
    pub max_turns: u32,
    /// Waiting turns beyond `max_turns * backlog_ticks` fold into the
    /// digest instead of waiting for later ticks.
    #[serde(default = "default_backlog_ticks")]
    pub backlog_ticks: u32,
    /// An `instant` cadence over budget holds new turns this long, folding
    /// each conversation's events into one, until it is under budget again.
    #[serde(default = "default_degrade_debounce_ms")]
    pub degrade_debounce_ms: u64,
}

fn default_backlog_ticks() -> u32 {
    3
}

fn default_degrade_debounce_ms() -> u64 {
    5_000
}

impl Budget {
    /// A budget of `max_turns` with the default backlog and debounce.
    #[must_use]
    pub fn new(max_turns: u32) -> Self {
        Self {
            max_turns,
            backlog_ticks: default_backlog_ticks(),
            degrade_debounce_ms: default_degrade_debounce_ms(),
        }
    }
}

fn default_max_backlog() -> u64 {
    1_000
}

fn default_concurrency() -> u32 {
    4
}

/// An attention lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lane {
    /// Direct messages, mentions and web chat.
    Interactive,
    /// Work the agent should act on, not urgently.
    Actionable,
    /// Low-salience work.
    Background,
    /// Overflow and backlog, summarized into one turn.
    Digest,
}

impl Lane {
    /// Every lane, highest priority first.
    pub const ALL: [Self; 4] = [
        Self::Interactive,
        Self::Actionable,
        Self::Background,
        Self::Digest,
    ];

    /// The lane's wire name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Actionable => "actionable",
            Self::Background => "background",
            Self::Digest => "digest",
        }
    }
}

/// When the platform may be acknowledged for an event of a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AckPolicy {
    /// As soon as `Ingest` returns: the event is durable.
    #[default]
    AfterIngest,
    /// Once the turn carrying it was handed to the host.
    AfterDispatch,
    /// Once the host reports the agent's turn finished (the host acks the
    /// platform when its `Deliver` handler completes the turn).
    AfterTurn,
}

/// The policy for the sources one pattern matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SourcePolicy {
    /// The sources this entry covers: an exact source (`channel:telegram`), a
    /// prefix ending in `*` (`channel:*`), or `*` for every source.
    #[serde(rename = "match")]
    pub pattern: String,
    /// When the platform may be acknowledged.
    #[serde(default)]
    pub ack: AckPolicy,
    /// Salience points the source adds (or, negative, takes away).
    #[serde(default)]
    pub weight: i32,
    /// The source's cadence, overriding its lane's; same forms as
    /// [`LaneConfig::cadence`]. Every source this entry matches shares it
    /// and its budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cadence: Option<String>,
    /// The budget of the source's cadence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<Budget>,
    /// The source's own debounce, overriding the pipeline's
    /// [`CoalesceConfig`] timings for its buffers and capping their size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coalesce: Option<SourceCoalesce>,
}

/// One source's debounce. A field left out keeps the pipeline's
/// [`CoalesceConfig`] value; `max_events` has no pipeline-wide counterpart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SourceCoalesce {
    /// Flush this long after a buffer's last event, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub debounce_ms: Option<u64>,
    /// Flush this long after a buffer's first event at the latest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wait_ms: Option<u64>,
    /// A buffer holding this many events flushes at once, and one flushed
    /// turn carries at most this many. Absent, a buffer is unbounded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_events: Option<u32>,
}

impl SourceCoalesce {
    /// A debounce that keeps every pipeline value.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Flush `debounce_ms` after a buffer's last event.
    #[must_use]
    pub fn debounce_ms(mut self, debounce_ms: u64) -> Self {
        self.debounce_ms = Some(debounce_ms);
        self
    }

    /// Flush `max_wait_ms` after a buffer's first event at the latest.
    #[must_use]
    pub fn max_wait_ms(mut self, max_wait_ms: u64) -> Self {
        self.max_wait_ms = Some(max_wait_ms);
        self
    }

    /// Flush a buffer once it holds `max_events`, and never carry more in one
    /// turn.
    #[must_use]
    pub fn max_events(mut self, max_events: u32) -> Self {
        self.max_events = Some(max_events);
        self
    }
}

impl Default for SourcePolicy {
    fn default() -> Self {
        Self {
            pattern: "*".to_owned(),
            ack: AckPolicy::default(),
            weight: 0,
            cadence: None,
            budget: None,
            coalesce: None,
        }
    }
}

impl SourcePolicy {
    /// A policy for `pattern` with every setting at its default.
    #[must_use]
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            ..Self::default()
        }
    }

    /// Whether this entry covers `source`.
    #[must_use]
    pub fn matches(&self, source: &str) -> bool {
        match self.pattern.strip_suffix('*') {
            Some(prefix) => source.starts_with(prefix),
            None => self.pattern == source,
        }
    }
}

impl PipelineConfig {
    /// The policy for `source`: the first entry that matches it, or the
    /// default policy.
    #[must_use]
    pub fn source_policy(&self, source: &str) -> SourcePolicy {
        self.sources
            .iter()
            .find(|policy| policy.matches(source))
            .cloned()
            .unwrap_or_default()
    }
}
