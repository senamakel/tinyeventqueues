//! Unit tests for the thread registry.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use tinyeventqueues_bus::{EventRef, Lane, Reason};

use super::*;
use crate::test_support::{Fixture, event};

async fn threads(fixture: &Fixture) -> Threads {
    crate::store::ensure(fixture.docs.as_ref()).await.unwrap();
    Threads::new(&fixture.docs)
}

fn turn(lane: Lane, id: &str) -> PendingTurn {
    PendingTurn::new(
        lane,
        Reason::Turn,
        vec![event("s", id, "c")],
        vec![EventRef::new("s", "edit")],
        0,
    )
    .unwrap()
}

#[tokio::test]
async fn a_new_turn_takes_the_most_urgent_pending_lane_and_supersedes_slower_ones() {
    let fixture = Fixture::new();
    let threads = threads(&fixture).await;
    let first = event("s", "x", "c");
    assert_eq!(
        threads.plan(&first, Lane::Background).await.unwrap(),
        (Lane::Background, vec![])
    );

    let background = turn(Lane::Background, "1");
    threads.record(&background, &[]).await.unwrap();
    // Recording the same turn again does not duplicate it.
    threads.record(&background, &[]).await.unwrap();
    let actionable = turn(Lane::Actionable, "2");
    threads.record(&actionable, &[]).await.unwrap();

    // A background turn now joins the actionable lane, superseding the older
    // background one.
    let (lane, superseded) = threads.plan(&first, Lane::Background).await.unwrap();
    assert_eq!(lane, Lane::Actionable);
    assert_eq!(superseded.len(), 1);
    assert_eq!(superseded[0].turn_id, background.turn_id);
    assert_eq!(superseded[0].absorbed, [EventRef::new("s", "edit")]);

    // An interactive turn supersedes both.
    let (lane, superseded) = threads.plan(&first, Lane::Interactive).await.unwrap();
    assert_eq!(lane, Lane::Interactive);
    assert_eq!(superseded.len(), 2);

    let interactive = turn(Lane::Interactive, "3");
    threads.record(&interactive, &superseded).await.unwrap();
    assert!(threads.is_superseded(&background).await.unwrap());
    assert!(threads.is_superseded(&actionable).await.unwrap());
    assert!(!threads.is_superseded(&interactive).await.unwrap());

    threads.settle(&background).await.unwrap();
    assert!(!threads.is_superseded(&background).await.unwrap());
    threads.settle(&actionable).await.unwrap();
    threads.settle(&interactive).await.unwrap();
    assert_eq!(
        threads.plan(&first, Lane::Digest).await.unwrap(),
        (Lane::Digest, vec![])
    );
}

#[tokio::test]
async fn turns_without_events_or_records_are_harmless() {
    let fixture = Fixture::new();
    let threads = threads(&fixture).await;
    let empty = PendingTurn::new(Lane::Digest, Reason::Turn, vec![], vec![], 0).unwrap();
    threads.record(&empty, &[]).await.unwrap();
    threads.settle(&empty).await.unwrap();
    assert!(!threads.is_superseded(&empty).await.unwrap());
    let unknown = turn(Lane::Background, "9");
    threads.settle(&unknown).await.unwrap();
    assert!(!threads.is_superseded(&unknown).await.unwrap());
}
