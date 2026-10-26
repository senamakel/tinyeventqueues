//! Crate-wide error and result types.
//!
//! Every fallible public function in this crate returns [`Result`], and every
//! failure mode is a distinct [`Error`] variant. Callers match on variants;
//! message text is not a stable API.

use crate::ports::StorageError;

/// Errors returned by this crate.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A storage port failed.
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    /// The pipeline configuration is invalid. `field` is the JSON path of the
    /// offending field, such as `lanes.digest.concurrency`.
    #[error("pipeline config: {field}: {message}")]
    Config {
        /// The JSON path of the offending field.
        field: String,
        /// What is wrong with it.
        message: String,
    },
    /// The pipeline configuration file could not be read.
    #[error("pipeline config {path}: {source}")]
    ConfigFile {
        /// The file.
        path: String,
        /// The read error.
        source: std::io::Error,
    },
    /// The queue manifest is invalid or could not be read.
    #[error("queue manifest: {0}")]
    Manifest(StorageError),
    /// A stored record could not be encoded or decoded.
    #[error("record encoding: {0}")]
    Encoding(#[from] serde_json::Error),
    /// A host callback failed.
    #[error("host callback {method}: {message}")]
    Host {
        /// The callback.
        method: &'static str,
        /// What went wrong.
        message: String,
    },
    /// A redrive id is malformed.
    #[error("unknown dead item id {0:?}")]
    UnknownDeadItem(String),
}

impl Error {
    pub(crate) fn manifest(message: impl Into<String>) -> Self {
        Self::Manifest(StorageError::invalid_input(message))
    }
    /// A configuration error at `field`.
    pub(crate) fn config(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Config {
            field: field.into(),
            message: message.into(),
        }
    }
}

/// The crate's standard result type.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
