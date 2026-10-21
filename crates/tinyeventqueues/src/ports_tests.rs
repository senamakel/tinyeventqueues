//! Tests for the owner-local storage key encoding.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::key_of;

#[test]
fn composite_keys_escape_delimiters_and_reserved_prefixes() {
    let key = key_of(&["a/b%", "~lead", "tail~", "\0x"]).unwrap();
    assert_eq!(key.as_str(), "a%2Fb%25/~lead/tail~/%00x");
    assert_eq!(key_of(&["~lead", "tail"]).unwrap().as_str(), "%7Elead/tail");
}

#[test]
fn oversized_composite_keys_are_bounded_and_deterministic() {
    let long = "x".repeat(tinystoragedrivers_types::MAX_KEY_LEN + 1);
    let first = key_of(&[&long]).unwrap();
    let second = key_of(&[&long]).unwrap();
    assert_eq!(first, second);
    assert!(first.as_str().starts_with('~'));
    assert!(first.as_str().len() <= tinystoragedrivers_types::MAX_KEY_LEN);
}
