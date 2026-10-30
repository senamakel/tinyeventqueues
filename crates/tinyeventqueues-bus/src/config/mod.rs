//! The pipeline configuration: the wire form of `pipeline.json` and of the
//! `Configure` member.
//!
//! Every struct rejects unknown fields, so a misspelt key in an edited file is
//! an error rather than a silently ignored setting. Queue *names* appear here
//! only as references: the queues themselves (attempts, dedupe windows,
//! visibility) are declared in the queue manifest (`queues.json`), and the
//! module checks every reference against it.
//!
//! This crate holds the shape only. Range checks with errors that name the
//! offending field live in the module crate.

mod types;

pub use types::{
    AckPolicy, AmbientConfig, Borderline, BotLoopConfig, Budget, CoalesceConfig, DeadLetterConfig,
    DigestConfig, DispatchConfig, GateConfig, HabituationConfig, IngressConfig, Lane, LaneConfig,
    LanesConfig, PipelineConfig, RateLimit, RateLimits, SalienceConfig, SalienceThresholds,
    SalienceWeights, SourceCoalesce, SourcePolicy,
};

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
