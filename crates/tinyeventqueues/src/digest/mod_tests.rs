//! Unit tests for the digest accumulator.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use tinyeventqueues_bus::{DigestConfig, EventRef, Lane};

use super::*;
use crate::test_support::{Fixture, event};

async fn digest(fixture: &Fixture) -> Digest {
    crate::store::ensure(fixture.docs.as_ref()).await.unwrap();
    Digest::new(&fixture.docs)
}

fn small() -> DigestConfig {
    serde_json::from_value(serde_json::json!({"max_items": 2, "max_wait_ms": 100})).unwrap()
}

#[tokio::test]
async fn nothing_is_due_until_full_or_old() {
    let fixture = Fixture::new();
    let digest = digest(&fixture).await;
    assert!(digest.due(&small(), 0).await.unwrap().is_none());
    digest.add(&[event("s", "1", "a")], &[], 10).await.unwrap();
    assert!(digest.due(&small(), 109).await.unwrap().is_none());
    let turn = digest.due(&small(), 110).await.unwrap().unwrap();
    assert_eq!(turn.lane, Lane::Digest);
    assert_eq!(turn.conversation_key, "");
    assert_eq!(turn.thread_key, None);
}

#[tokio::test]
async fn a_full_digest_releases_max_items_and_keeps_the_rest() {
    let fixture = Fixture::new();
    let digest = digest(&fixture).await;
    let absorbed = [EventRef::new("s", "edit")];
    digest
        .add(
            &[
                event("s", "1", "a"),
                event("s", "2", "b"),
                event("s", "3", "c"),
            ],
            &absorbed,
            0,
        )
        .await
        .unwrap();
    // Adding the same events again changes nothing.
    digest
        .add(&[event("s", "1", "a")], &absorbed, 5)
        .await
        .unwrap();
    let turn = digest.due(&small(), 0).await.unwrap().unwrap();
    let ids: Vec<_> = turn.events.iter().map(|e| e.event_id.as_str()).collect();
    assert_eq!(ids, ["1", "2"]);
    assert_eq!(turn.absorbed, absorbed);
    digest.release(&turn, 50).await.unwrap();
    // One left, waiting from the release.
    assert!(digest.due(&small(), 149).await.unwrap().is_none());
    let rest = digest.due(&small(), 150).await.unwrap().unwrap();
    assert_eq!(rest.events[0].event_id, "3");
    digest.release(&rest, 150).await.unwrap();
    assert!(digest.due(&small(), u64::MAX).await.unwrap().is_none());
}

#[tokio::test]
async fn releasing_without_a_digest_is_harmless() {
    let fixture = Fixture::new();
    let digest = digest(&fixture).await;
    let turn = crate::turn::PendingTurn::new(
        Lane::Digest,
        tinyeventqueues_bus::Reason::Turn,
        vec![],
        vec![],
        0,
    )
    .unwrap();
    digest.release(&turn, 0).await.unwrap();
}
