//! The private configuration a host hands the loaded module.
//!
//! Everything is optional; an empty blob (`{}`) runs the shipped files with
//! the default timer:
//!
//! ```json
//! {
//!   "storage_address": {
//!     "host": ":1.42",
//!     "object_root": "/ai/tinyhumans/tinystorage/Invocation/abc123"
//!   },
//!   "queues":   { "version": 1, "queues": [ ... ] },
//!   "pipeline": { "version": 1, ... },
//!   "timer":    { "enabled": true, "min_interval_ms": 100, "max_interval_ms": 5000 }
//! }
//! ```
//!
//! A `storage_callback` unique peer selects the host-bound callback endpoint.
//! It is mutually exclusive with `storage_address`; an explicit route never
//! falls back to global storage.
//!
//! `storage_address` identifies the host's invocation-scoped storage endpoint.
//! Its unique peer and invocation root must be issued by the host. An explicit
//! address never falls back to another storage route. `queues` is a
//! `queues.json` document and `pipeline` a `pipeline.json` document, both parsed
//! as strictly as the files are. A host that serves the module's queues should
//! declare the same manifest it passes here: its queue port admits an
//! `EnsureQueue` only for the spec it declared.

use crate::config::{
    QueueManifest, parse_manifest, parse_pipeline_config, shipped_manifest, shipped_pipeline_config,
};
use crate::{Error, Result};
use serde::Deserialize;
use serde_json::Value;
use tinybus::{BusName, ObjectPath};
use tinyeventqueues_bus::PipelineConfig;
use tinystoragedrivers_busproxy::StorageAddress;

/// The module configuration.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Settings {
    /// A host-bound storage callback peer; never a module-selected alias.
    #[serde(default)]
    storage_callback: Option<String>,
    /// Host-issued route for this module instance's scoped storage.
    #[serde(default)]
    storage_address: Option<StorageAddressConfig>,
    /// A `queues.json` document; the shipped one when absent.
    #[serde(default)]
    queues: Option<Value>,
    /// A `pipeline.json` document; the shipped one when absent.
    #[serde(default)]
    pipeline: Option<Value>,
    /// The module-owned pass timer.
    #[serde(default)]
    pub(crate) timer: TimerSettings,
}

/// The JSON form of a host-issued invocation storage address.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageAddressConfig {
    host: String,
    object_root: String,
}

/// The pass timer: how often the module runs a pass on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub(crate) struct TimerSettings {
    /// Whether the module runs passes on its own. Off, only `Drain` does.
    pub(crate) enabled: bool,
    /// The shortest sleep between passes, so ready work that cannot move
    /// (a busy conversation, a full lane) does not spin.
    pub(crate) min_interval_ms: u64,
    /// The longest sleep between passes, which bounds how late work delayed
    /// inside a queue (a retry back-off) is picked up.
    pub(crate) max_interval_ms: u64,
}

impl Default for TimerSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            min_interval_ms: 100,
            max_interval_ms: 5_000,
        }
    }
}

impl Settings {
    /// Parse the configuration blob. `null` is the default.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] naming the field that failed to decode, or a timer
    /// whose bounds are inverted or zero.
    pub(crate) fn parse(value: Value) -> Result<Self> {
        if value.is_null() {
            return Ok(Self::default());
        }
        let settings: Self = serde_path_to_error::deserialize(value)
            .map_err(|error| Error::config(error.path().to_string(), error.inner().to_string()))?;
        let timer = settings.timer;
        if timer.min_interval_ms == 0 || timer.min_interval_ms > timer.max_interval_ms {
            return Err(Error::config(
                "timer",
                "min_interval_ms must be at least 1 and at most max_interval_ms",
            ));
        }
        Ok(settings)
    }

    /// Resolve the exact host callback peer without an invocation-route fallback.
    ///
    /// # Errors
    /// Returns a configuration error for a non-unique peer or conflicting routes.
    pub(crate) fn storage_callback(&self) -> Result<Option<BusName>> {
        let Some(callback) = &self.storage_callback else {
            return Ok(None);
        };
        if self.storage_address.is_some() {
            return Err(Error::config(
                "storage_callback",
                "cannot combine callback and invocation storage routes",
            ));
        }
        let peer = BusName::new(callback)
            .map_err(|_| Error::config("storage_callback", "expected a unique host peer"))?;
        if !peer.is_unique() {
            return Err(Error::config(
                "storage_callback",
                "expected a unique host peer",
            ));
        }
        Ok(Some(peer))
    }

    /// Validate and build the host-issued invocation storage address, if set.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the peer is not unique or the object root is
    /// outside the invocation-scoped namespace.
    pub(crate) fn storage_address(&self) -> Result<Option<StorageAddress>> {
        let Some(config) = &self.storage_address else {
            return Ok(None);
        };
        let host = BusName::new(&config.host)
            .map_err(|_| Error::config("storage_address.host", "expected a unique bus name"))?;
        let root = ObjectPath::new(&config.object_root).map_err(|_| {
            Error::config(
                "storage_address.object_root",
                "expected an invocation object path",
            )
        })?;
        StorageAddress::try_new(host, root)
            .map(Some)
            .map_err(|_| Error::config("storage_address", "invalid invocation storage address"))
    }

    /// The queue manifest to run.
    ///
    /// # Errors
    ///
    /// [`Error::Manifest`] when the supplied manifest does not validate.
    pub(crate) fn manifest(&self) -> Result<QueueManifest> {
        match &self.queues {
            Some(queues) => parse_manifest(&queues.to_string()),
            None => shipped_manifest(),
        }
    }

    /// The pipeline configuration to run.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] when the supplied configuration does not validate.
    pub(crate) fn pipeline(&self) -> Result<PipelineConfig> {
        match &self.pipeline {
            Some(pipeline) => parse_pipeline_config(&pipeline.to_string()),
            None => shipped_pipeline_config(),
        }
    }
}

#[cfg(test)]
#[path = "settings_tests.rs"]
mod tests;
