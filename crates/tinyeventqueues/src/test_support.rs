//! Shared fixtures for the unit tests: storage, a queue backend without
//! dedupe, and events.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use crate::ports::{
    DeadLetter, Delivery, DocumentStore, Enqueued, NewMessage, QueueFeatures, QueueSpec,
    QueueStats, QueueStore, Receipt, Retry, Scope, StorageError,
};
use async_trait::async_trait;
use tinyeventqueues_bus::{EventKind, InboundEvent, Sender};
use tinystoragedrivers_testkit::memory_storage;

use crate::{ManualClock, Pipeline, PipelineBuilder};

/// One storage backend, shared by every pipeline built over it, so a new
/// pipeline is a restart.
pub(crate) struct Fixture {
    pub(crate) clock: ManualClock,
    pub(crate) docs: Arc<dyn DocumentStore>,
    pub(crate) queues: Arc<dyn QueueStore>,
}

impl Fixture {
    /// A queue backend with every feature (`DocumentQueue`).
    pub(crate) fn new() -> Self {
        Self::with_native(true)
    }

    /// With `native`, a `DocumentQueue`; without, a [`Broker`] that has
    /// neither dedupe nor groups and refuses messages that use them, the way
    /// the `RabbitMQ` driver does.
    pub(crate) fn with_native(native: bool) -> Self {
        let clock = ManualClock::new(START_MS);
        let scope_text = format!("tinyeventqueues-test-{START_MS}");
        let scope = Scope::new(&scope_text).unwrap();
        let storage = memory_storage(&scope);
        let docs = Arc::clone(storage.documents());
        let engine: Arc<dyn QueueStore> = Arc::new(
            tinystoragedrivers_queue::MemoryQueue::with_clock(clock.clock()),
        );
        let queues: Arc<dyn QueueStore> = if native {
            engine
        } else {
            Arc::new(Broker(engine))
        };
        Self {
            clock,
            docs,
            queues,
        }
    }

    pub(crate) fn builder(&self) -> PipelineBuilder {
        Pipeline::builder(Arc::clone(&self.queues), Arc::clone(&self.docs))
            .clock(self.clock.clock())
    }

    /// A pipeline with the shipped files: building one again is a restart.
    pub(crate) async fn pipeline(&self) -> Pipeline {
        self.builder().build().await.unwrap()
    }
}

/// Where every [`Fixture`] clock starts.
pub(crate) const START_MS: u64 = 1_000_000;

/// An event from a human in a group conversation, received at [`START_MS`].
pub(crate) fn event(source: &str, id: &str, conversation: &str) -> InboundEvent {
    InboundEvent::new(
        source,
        id,
        conversation,
        Sender::new("user-1"),
        EventKind::Message,
        START_MS,
    )
    .text(format!("message {id} {}", words(id)))
}

/// A queue backend like the `RabbitMQ` driver: no dedupe and no groups, and
/// a message that sets `group` or `dedupe_key` is refused.
#[derive(Debug)]
pub(crate) struct Broker(pub(crate) Arc<dyn QueueStore>);

#[async_trait]
impl QueueStore for Broker {
    fn features(&self) -> QueueFeatures {
        QueueFeatures {
            dedupe: false,
            groups: false,
            ..self.0.features()
        }
    }
    async fn ensure_queue(
        &self,
        queue: &str,
        spec: &QueueSpec,
    ) -> std::result::Result<(), StorageError> {
        self.0.ensure_queue(queue, spec).await
    }
    async fn enqueue(
        &self,
        queue: &str,
        messages: &[NewMessage],
    ) -> std::result::Result<Vec<Enqueued>, StorageError> {
        if messages
            .iter()
            .any(|message| message.group.is_some() || message.dedupe_key.is_some())
        {
            return Err(StorageError::invalid_input(
                "this queue backend supports neither groups nor dedupe keys",
            ));
        }
        self.0.enqueue(queue, messages).await
    }
    async fn receive(
        &self,
        queue: &str,
        max: u32,
        visibility_ms: u64,
        consumer: &str,
    ) -> std::result::Result<Vec<Delivery>, StorageError> {
        self.0.receive(queue, max, visibility_ms, consumer).await
    }
    async fn ack(&self, queue: &str, receipt: &Receipt) -> std::result::Result<bool, StorageError> {
        self.0.ack(queue, receipt).await
    }
    async fn nack(
        &self,
        queue: &str,
        receipt: &Receipt,
        retry: Retry,
    ) -> std::result::Result<bool, StorageError> {
        self.0.nack(queue, receipt, retry).await
    }
    async fn extend(
        &self,
        queue: &str,
        receipt: &Receipt,
        visibility_ms: u64,
    ) -> std::result::Result<bool, StorageError> {
        self.0.extend(queue, receipt, visibility_ms).await
    }
    async fn stats(&self, queue: &str) -> std::result::Result<QueueStats, StorageError> {
        self.0.stats(queue).await
    }
    async fn dead_letters(
        &self,
        queue: &str,
        limit: usize,
    ) -> std::result::Result<Vec<DeadLetter>, StorageError> {
        self.0.dead_letters(queue, limit).await
    }
    async fn redrive(&self, queue: &str, id: &str) -> std::result::Result<bool, StorageError> {
        self.0.redrive(queue, id).await
    }
    async fn purge(&self, queue: &str) -> std::result::Result<u64, StorageError> {
        self.0.purge(queue).await
    }
}

/// Take every turn waiting on `lane`, in delivery order, acknowledging each
/// so the next of its conversation becomes deliverable.
pub(crate) async fn take_turns(
    fixture: &Fixture,
    pipeline: &Pipeline,
    lane: tinyeventqueues_bus::Lane,
) -> Vec<crate::turn::PendingTurn> {
    let queue = pipeline.queues().lane(lane).name.clone();
    let mut turns = Vec::new();
    loop {
        let batch = fixture
            .queues
            .receive(&queue, 100, 60_000, "inspect")
            .await
            .unwrap();
        if batch.is_empty() {
            return turns;
        }
        for delivery in batch {
            turns.push(serde_json::from_value(delivery.payload).unwrap());
            fixture.queues.ack(&queue, &delivery.receipt).await.unwrap();
        }
    }
}

/// A few words derived from `id`, so test events' texts are not
/// near-duplicates of each other (which would habituate their scores).
fn words(id: &str) -> String {
    const WORDS: [&str; 16] = [
        "amber", "birch", "cobalt", "delta", "ember", "fjord", "garnet", "harbor", "iris",
        "juniper", "kelp", "lumen", "maple", "nectar", "onyx", "prism",
    ];
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in id.bytes() {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3);
    }
    (0..6)
        .map(|_| {
            hash = hash
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            WORDS[usize::try_from(hash >> 60).unwrap()]
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// How a [`ScriptedHost`] answers a turn.
type Decide = dyn Fn(&tinyeventqueues_bus::Turn) -> crate::Result<tinyeventqueues_bus::DeliverStatus>
    + Send
    + Sync;

/// A host whose verdict on each turn is scripted, and which records every
/// turn it is handed.
pub(crate) struct ScriptedHost {
    decide: Box<Decide>,
    pub(crate) delivered: std::sync::Mutex<Vec<tinyeventqueues_bus::Turn>>,
}

impl std::fmt::Debug for ScriptedHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptedHost").finish_non_exhaustive()
    }
}

impl ScriptedHost {
    pub(crate) fn new(
        decide: impl Fn(&tinyeventqueues_bus::Turn) -> crate::Result<tinyeventqueues_bus::DeliverStatus>
        + Send
        + Sync
        + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            decide: Box::new(decide),
            delivered: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// A host that accepts everything.
    pub(crate) fn accepting() -> Arc<Self> {
        Self::new(|_| Ok(tinyeventqueues_bus::DeliverStatus::Accepted))
    }

    pub(crate) fn turns(&self) -> Vec<tinyeventqueues_bus::Turn> {
        self.delivered.lock().unwrap().clone()
    }
}

#[async_trait]
impl crate::Host for ScriptedHost {
    async fn classify_salience(
        &self,
        _request: tinyeventqueues_bus::ClassifySalienceRequest,
    ) -> crate::Result<tinyeventqueues_bus::ClassifySalienceResponse> {
        Ok(tinyeventqueues_bus::ClassifySalienceResponse::new(
            tinyeventqueues_bus::SalienceClass::Fyi,
        ))
    }

    async fn deliver(
        &self,
        request: tinyeventqueues_bus::DeliverRequest,
    ) -> crate::Result<tinyeventqueues_bus::DeliverResponse> {
        self.delivered.lock().unwrap().push(request.turn.clone());
        (self.decide)(&request.turn).map(tinyeventqueues_bus::DeliverResponse::new)
    }
}
