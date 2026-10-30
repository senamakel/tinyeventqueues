//! Unit tests for the debounce buffer.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use tinyeventqueues_bus::{CoalesceConfig, EventKind, EventRef, InboundEvent, MediaRef, Sender};

use super::*;
use crate::test_support::{Fixture, event};

async fn coalescer(fixture: &Fixture) -> Coalescer {
    crate::store::ensure(fixture.docs.as_ref()).await.unwrap();
    Coalescer::new(&fixture.docs)
}

fn config() -> Window {
    Window::from(&CoalesceConfig::default())
}

fn ids(due: &Due) -> Vec<String> {
    due.found
        .doc
        .events
        .iter()
        .map(|e| e.event_id.clone())
        .collect()
}

fn from(sender: &str, id: &str) -> InboundEvent {
    let mut event = event("s", id, "c");
    event.sender = Sender::new(sender);
    event
}

#[tokio::test]
async fn a_burst_flushes_after_the_debounce_in_order() {
    let fixture = Fixture::new();
    let coalescer = coalescer(&fixture).await;
    coalescer
        .add(&event("s", "1", "c"), 0, &config(), 0)
        .await
        .unwrap();
    coalescer
        .add(&event("s", "2", "c"), 0, &config(), 1_000)
        .await
        .unwrap();
    assert_eq!(coalescer.due(2_499, 10).await.unwrap().len(), 0);
    let due = coalescer.due(2_500, 10).await.unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(ids(&due[0]), ["1", "2"]);
}

#[tokio::test]
async fn a_steady_trickle_still_flushes_at_max_wait() {
    let fixture = Fixture::new();
    let coalescer = coalescer(&fixture).await;
    for (i, at) in (0..12u64).map(|i| (i, i * 1_000)) {
        coalescer
            .add(&event("s", &i.to_string(), "c"), 0, &config(), at)
            .await
            .unwrap();
    }
    assert_eq!(coalescer.due(10_000, 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn media_and_control_commands_flush_at_once() {
    let fixture = Fixture::new();
    let coalescer = coalescer(&fixture).await;
    let photo = event("s", "1", "c").with_media(MediaRef::new("image", "blob://1"));
    coalescer.add(&photo, 0, &config(), 50).await.unwrap();
    assert_eq!(coalescer.due(50, 10).await.unwrap().len(), 1);

    let command = event("s", "2", "other").text("  /stop");
    coalescer.add(&command, 0, &config(), 60).await.unwrap();
    assert_eq!(coalescer.due(60, 10).await.unwrap().len(), 2);

    let mut odd = config();
    odd.control_prefixes = vec![String::new()];
    coalescer
        .add(&event("s", "3", "third"), 0, &odd, 70)
        .await
        .unwrap();
    assert_eq!(coalescer.due(70, 10).await.unwrap().len(), 2);
}

#[tokio::test]
async fn threads_are_never_merged() {
    let fixture = Fixture::new();
    let coalescer = coalescer(&fixture).await;
    coalescer
        .add(&event("s", "1", "c").thread("a"), 0, &config(), 0)
        .await
        .unwrap();
    coalescer
        .add(&event("s", "2", "c").thread("b"), 0, &config(), 0)
        .await
        .unwrap();
    coalescer
        .add(&event("s", "3", "c").thread("a"), 0, &config(), 0)
        .await
        .unwrap();
    let due = coalescer.due(10_000, 10).await.unwrap();
    assert_eq!(due.len(), 2);
    let mut threads: Vec<_> = due.iter().map(ids).collect();
    threads.sort();
    assert_eq!(threads, [vec!["1", "3"], vec!["2"]]);
}

#[tokio::test]
async fn a_due_thread_flushes_every_sender_in_arrival_order() {
    let fixture = Fixture::new();
    let coalescer = coalescer(&fixture).await;
    coalescer
        .add(&from("ann", "1"), 10, &config(), 0)
        .await
        .unwrap();
    let photo = from("bob", "2").with_media(MediaRef::new("image", "u"));
    coalescer.add(&photo, 20, &config(), 1).await.unwrap();
    // Bob's photo is due at once; Ann's earlier text goes first.
    let due = coalescer.due(1, 10).await.unwrap();
    assert_eq!(
        due.iter().map(ids).collect::<Vec<_>>(),
        [vec!["1"], vec!["2"]]
    );
}

#[tokio::test]
async fn replays_are_ignored_and_edits_amend_their_target() {
    let fixture = Fixture::new();
    let coalescer = coalescer(&fixture).await;
    let original = event("s", "1", "c").text("helo");
    assert_eq!(
        coalescer.add(&original, 0, &config(), 0).await.unwrap(),
        Added::Buffered
    );
    assert_eq!(
        coalescer.add(&original, 0, &config(), 0).await.unwrap(),
        Added::Duplicate
    );
    let mut edit = event("s", "2", "c")
        .text("hello")
        .target("1")
        .with_media(MediaRef::new("image", "u"));
    edit.kind = EventKind::Edit;
    assert_eq!(
        coalescer.add(&edit, 0, &config(), 1).await.unwrap(),
        Added::Buffered
    );
    assert_eq!(
        coalescer.try_amend(&edit).await.unwrap(),
        Some(Added::Duplicate)
    );
    let due = coalescer.due(u64::MAX, 10).await.unwrap();
    assert_eq!(ids(&due[0]), ["1"]);
    assert_eq!(due[0].found.doc.events[0].text, "hello");
    assert_eq!(
        due[0].found.doc.refs(),
        [EventRef::new("s", "1"), EventRef::new("s", "2")]
    );
}

#[tokio::test]
async fn a_delete_retracts_its_target_and_the_edits_folded_into_it() {
    let fixture = Fixture::new();
    let coalescer = coalescer(&fixture).await;
    coalescer
        .add(&event("s", "1", "c"), 0, &config(), 0)
        .await
        .unwrap();
    let mut edit = event("s", "2", "c").target("1");
    edit.kind = EventKind::Edit;
    coalescer.add(&edit, 0, &config(), 0).await.unwrap();
    let mut delete = event("s", "3", "c").target("1");
    delete.kind = EventKind::Delete;
    let added = coalescer.try_amend(&delete).await.unwrap();
    assert_eq!(
        added,
        Some(Added::Retracted(vec![
            EventRef::new("s", "1"),
            EventRef::new("s", "3"),
            EventRef::new("s", "2"),
        ]))
    );
    // The emptied buffer is gone.
    assert_eq!(coalescer.due(u64::MAX, 10).await.unwrap().len(), 0);
}

#[tokio::test]
async fn amendments_without_a_buffered_target_are_not_applied() {
    let fixture = Fixture::new();
    let coalescer = coalescer(&fixture).await;
    let mut edit = event("s", "2", "c").target("missing");
    edit.kind = EventKind::Edit;
    assert_eq!(coalescer.try_amend(&edit).await.unwrap(), None);
    assert_eq!(
        coalescer.try_amend(&event("s", "3", "c")).await.unwrap(),
        None
    );
    coalescer
        .add(&event("s", "1", "c"), 0, &config(), 0)
        .await
        .unwrap();
    assert_eq!(coalescer.try_amend(&edit).await.unwrap(), None);
    // Added normally, an untargeted amendment is just another event.
    assert_eq!(
        coalescer.add(&edit, 0, &config(), 0).await.unwrap(),
        Added::Buffered
    );
    let mut delete = event("s", "4", "c").target("1");
    delete.kind = EventKind::Delete;
    assert!(matches!(
        coalescer.add(&delete, 0, &config(), 0).await.unwrap(),
        Added::Retracted(_)
    ));
    assert_eq!(ids(&coalescer.due(u64::MAX, 10).await.unwrap()[0]), ["2"]);
}

#[tokio::test]
async fn removing_a_flushed_buffer_keeps_events_that_arrived_since() {
    let fixture = Fixture::new();
    let coalescer = coalescer(&fixture).await;
    coalescer
        .add(&event("s", "1", "c"), 0, &config(), 0)
        .await
        .unwrap();
    let due = coalescer.due(u64::MAX, 10).await.unwrap();
    coalescer
        .add(&event("s", "2", "c"), 0, &config(), 5)
        .await
        .unwrap();
    coalescer.remove(&due[0]).await.unwrap();
    let left = coalescer.due(u64::MAX, 10).await.unwrap();
    assert_eq!(ids(&left[0]), ["2"]);
    coalescer.remove(&left[0]).await.unwrap();
    assert_eq!(coalescer.due(u64::MAX, 10).await.unwrap().len(), 0);
    // Removing again, after it is gone, is harmless.
    coalescer.remove(&left[0]).await.unwrap();
    coalescer.remove(&due[0]).await.unwrap();
}
