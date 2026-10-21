//! The v2 storage contracts consumed by the queue owner.

pub(crate) use crate::clock::Clock;
pub(crate) use tinystoragedrivers_api::{DocumentStore, QueueStore};
pub(crate) use tinystoragedrivers_types::{
    DeadLetter, Delivery, ErrorKind, Filter, Key, NewMessage, Precondition, Query, QueueSpec,
    Retry, Sort, StorageError, Version, Versioned,
};
#[cfg(test)]
pub(crate) use tinystoragedrivers_types::{Enqueued, QueueFeatures, QueueStats, Receipt, Scope};

/// Builds the legacy reversible composite key and hashes oversized values.
///
/// The v2 storage contract intentionally accepts only encoded keys, so this
/// owner-local helper retains the previous TQ key layout.
pub(crate) fn key_of(parts: &[&str]) -> Result<Key, StorageError> {
    use sha2::{Digest, Sha256};

    let mut encoded = String::new();
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            encoded.push('/');
        }
        for (char_index, ch) in part.chars().enumerate() {
            match ch {
                '%' => encoded.push_str("%25"),
                '/' => encoded.push_str("%2F"),
                '\0' => encoded.push_str("%00"),
                '~' if index == 0 && char_index == 0 => encoded.push_str("%7E"),
                _ => encoded.push(ch),
            }
        }
    }
    if encoded.len() > tinystoragedrivers_types::MAX_KEY_LEN {
        encoded = format!(
            "~{}",
            data_encoding::BASE32_NOPAD
                .encode(&Sha256::digest(encoded.as_bytes()))
                .to_ascii_lowercase()
        );
    }
    Key::new(encoded)
}

#[cfg(test)]
#[path = "ports_tests.rs"]
mod tests;
