//! Tests for typed document reads and compare-and-swap updates.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::json;

use super::Repo;
use crate::ports::{DocumentStore, Precondition, Query};
use crate::store::THREADS;
use crate::test_support::Fixture;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Counter {
    value: u64,
}

async fn prepared_docs() -> Arc<dyn DocumentStore> {
    let fixture = Fixture::new();
    fixture.pipeline().await;
    fixture.docs
}

#[tokio::test]
async fn typed_repo_reads_queries_and_conditionally_updates_documents() {
    let docs = prepared_docs().await;
    let repo = Repo::<Counter>::new(&docs, THREADS);
    assert!(format!("{repo:?}").contains(THREADS));
    assert!(repo.get("missing").await.unwrap().is_none());

    let (created, _) = repo
        .update("counter", |current| {
            assert!(current.is_none());
            Ok(Some(Counter { value: 1 }))
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(created, Counter { value: 1 });

    let (updated, _) = repo
        .update("counter", |current| {
            let current = current.unwrap();
            Ok(Some(Counter {
                value: current.value + 1,
            }))
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated, Counter { value: 2 });

    let found = repo.get("counter").await.unwrap().unwrap();
    assert_eq!(found.id, "counter");
    assert_eq!(found.doc, Counter { value: 2 });
    let page = repo.query(&Query::all()).await.unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].doc, found.doc);
    assert!(page.next.is_none());

    assert!(
        repo.update("counter", |_| Ok(None))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_repo_retries_conflicts_without_losing_concurrent_updates() {
    let docs = prepared_docs().await;
    let repo = Arc::new(Repo::<Counter>::new(&docs, THREADS));
    let rendezvous = Arc::new(Barrier::new(2));
    let mut updates = Vec::new();
    for _ in 0..2 {
        let repo = Arc::clone(&repo);
        let rendezvous = Arc::clone(&rendezvous);
        updates.push(tokio::spawn(async move {
            let first_attempt = AtomicBool::new(true);
            repo.update("contended", move |current| {
                if first_attempt.swap(false, Ordering::SeqCst) {
                    rendezvous.wait();
                }
                Ok(Some(Counter {
                    value: current.map_or(1, |value| value.value + 1),
                }))
            })
            .await
        }));
    }
    for update in updates {
        assert!(update.await.unwrap().unwrap().is_some());
    }
    assert_eq!(
        repo.get("contended").await.unwrap().unwrap().doc,
        Counter { value: 2 }
    );
}

#[tokio::test]
async fn typed_repo_rejects_documents_with_an_invalid_stored_shape() {
    let docs = prepared_docs().await;
    docs.put(
        THREADS,
        "malformed",
        json!({ "value": "not-a-counter" }),
        Precondition::Absent,
    )
    .await
    .unwrap();
    let repo = Repo::<Counter>::new(&docs, THREADS);
    assert!(repo.get("malformed").await.is_err());
    assert!(repo.query(&Query::all()).await.is_err());
}
