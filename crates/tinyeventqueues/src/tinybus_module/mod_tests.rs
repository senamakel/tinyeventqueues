//! Tests for the `TinyBus` module adapter and its declared surface.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};
use tinybus::broker::Broker;
use tinybus::transport::memory::MemoryBus;
use tinybus::{Connection, Interface, Proxy};
use tinyeventqueues_bus::{
    ConfigureRequest, ConfigureResponse, Disposition, DrainRequest, DrainResponse, EventKind,
    InboundEvent, IngestRequest, IngestResponse, ListDeadRequest, ListDeadResponse, RedriveRequest,
    RedriveResponse, Sender, StatsRequest, StatsResponse, names,
};
use tinystoragedrivers_api::StorageBackend;
use tinystoragedrivers_busproxy::StorageServer;
use tinystoragedrivers_busproxy::{PerCallerEndpoint, ServedStorage, serve};
#[cfg(all(target_os = "linux", not(feature = "static-link")))]
use tinystoragedrivers_busproxy::{prepare_storage_host_callbacks, serve_with_host_callbacks};
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::Scope;

use super::host::BusHost;
use super::settings::Settings;
use super::storage::Backing;
use super::ticker::{Slot, Wake};
use super::{EventQueuesService, pipeline, setup_factory, setup_with_test_backing};
use crate::Host;

/// A module configuration with the pass timer off, so a test's `Drain` is
/// the only thing that runs passes.
pub(crate) fn manual() -> Value {
    json!({ "timer": { "enabled": false } })
}

/// A set-up module instance: what a loaded module keeps alive.
pub(crate) struct Instance {
    pub(crate) connection: Connection,
    pub(crate) slot: Slot,
}

/// One bus with a host connection that may serve storage.
pub(crate) struct Rig {
    pub(crate) bus: MemoryBus,
    pub(crate) host: Connection,
    pub(crate) backend: Arc<MemoryBackend>,
    _served: Option<ServedStorage>,
}

/// A bus whose host serves a memory-backed test storage bundle when requested.
pub(crate) async fn rig(serve_storage: bool) -> Rig {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let backend = Arc::new(MemoryBackend::new());
    let host = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let served = if serve_storage {
        let bound = Arc::clone(&backend);
        let endpoint = PerCallerEndpoint::new(move |_sender| {
            let storage = bound.for_scope(&Scope::new("local").ok()?).ok()?;
            Some(StorageServer::new(storage))
        });
        Some(serve(&host, Arc::new(endpoint)).await.unwrap())
    } else {
        None
    };
    Rig {
        bus,
        host,
        backend,
        _served: served,
    }
}

impl Rig {
    fn test_backing(&self) -> Backing {
        let storage = self
            .backend
            .for_scope(&Scope::new("local").unwrap())
            .unwrap();
        Backing::of(&storage)
    }

    /// Set a module instance up on a fresh connection with `config`, and a
    /// proxy to it from the host connection. The returned instance (its
    /// connection and timer slot) must outlive the calls.
    pub(crate) async fn module(&self, config: Value) -> (Proxy, Instance) {
        let connection = Connection::connect(self.bus.connect().await.unwrap())
            .await
            .unwrap();
        let slot = Slot::default();
        setup_with_test_backing(
            connection.clone(),
            config,
            slot.clone(),
            self.test_backing(),
        )
        .await
        .unwrap();
        let proxy = self
            .host
            .proxy(names::INTERFACE, names::OBJECT_PATH, names::INTERFACE)
            .unwrap();
        (proxy, Instance { connection, slot })
    }
}

#[tokio::test]
async fn declared_methods_match_the_dispatch_table() {
    let settings = Settings::default();
    let service = EventQueuesService {
        pipeline: pipeline(
            Backing::of(&tinystoragedrivers_testkit::memory_storage(
                &Scope::new("test-module").unwrap(),
            )),
            &settings,
            None,
        )
        .await
        .unwrap(),
        wake: Wake::default(),
    };
    let methods = service
        .members()
        .into_iter()
        .map(|member| member.to_string())
        .collect::<Vec<_>>();
    assert_eq!(methods, names::METHODS.to_vec());
    assert_eq!(service.name().to_string(), names::INTERFACE);
}

async fn proxy() -> (Proxy, Instance, Rig) {
    let rig = rig(true).await;
    let (proxy, module) = rig.module(manual()).await;
    (proxy, module, rig)
}

#[tokio::test]
async fn the_module_serves_every_member_over_a_real_bus() {
    let (proxy, _service, _rig) = proxy().await;
    let event = InboundEvent::new(
        "web:chat",
        "1",
        "c",
        Sender::new("u"),
        EventKind::Message,
        0,
    );

    let ingested: IngestResponse = proxy
        .call(
            names::methods::INGEST,
            (IngestRequest::new(vec![event.clone()]),),
        )
        .await
        .unwrap();
    assert_eq!(ingested.results[0].disposition, Disposition::Accepted);
    let again: IngestResponse = proxy
        .call(names::methods::INGEST, (IngestRequest::new(vec![event]),))
        .await
        .unwrap();
    assert_eq!(again.results[0].disposition, Disposition::Duplicate);

    let stats: StatsResponse = proxy
        .call(names::methods::STATS, (StatsRequest::new(),))
        .await
        .unwrap();
    assert_eq!(stats.queues[0].ready, 1);

    let config = crate::config::shipped_pipeline_config().unwrap();
    let configured: ConfigureResponse = proxy
        .call(names::methods::CONFIGURE, (ConfigureRequest::new(config),))
        .await
        .unwrap();
    assert!(configured.applied);

    let dead: ListDeadResponse = proxy
        .call(names::methods::LIST_DEAD, (ListDeadRequest::new(10),))
        .await
        .unwrap();
    assert_eq!(dead.dead.len(), 0);

    let drained: DrainResponse = proxy
        .call(names::methods::DRAIN, (DrainRequest::new(),))
        .await
        .unwrap();
    assert_eq!(drained.report.dead, 0);

    let missing = proxy
        .call::<RedriveResponse>(names::methods::REDRIVE, (RedriveRequest::new("nope"),))
        .await;
    assert!(
        missing
            .unwrap_err()
            .to_string()
            .contains("unknown dead item")
    );
    let absent: RedriveResponse = proxy
        .call(
            names::methods::REDRIVE,
            (RedriveRequest::new("teq.dead/none"),),
        )
        .await
        .unwrap();
    assert!(!absent.redriven);
}

#[tokio::test]
async fn an_invalid_configuration_is_refused_over_the_bus() {
    let (proxy, _service, _rig) = proxy().await;
    let mut config = crate::config::shipped_pipeline_config().unwrap();
    config.lanes.digest.queue = "teq.missing".to_owned();
    let refused = proxy
        .call::<ConfigureResponse>(names::methods::CONFIGURE, (ConfigureRequest::new(config),))
        .await;
    assert!(
        refused
            .unwrap_err()
            .to_string()
            .contains("lanes.digest.queue")
    );
}

/// A host serving the callback interface on the bus, accepting every turn.
#[derive(Clone, Default)]
struct FakeHost {
    turns: std::sync::Arc<std::sync::Mutex<Vec<tinyeventqueues_bus::Turn>>>,
}

#[tinybus::interface(name = "ai.tinyhumans.tinyeventqueues.Host")]
impl FakeHost {
    async fn classify_salience(
        &self,
        _request: tinyeventqueues_bus::ClassifySalienceRequest,
    ) -> tinybus::Result<tinyeventqueues_bus::ClassifySalienceResponse> {
        std::future::ready(Ok(tinyeventqueues_bus::ClassifySalienceResponse::new(
            tinyeventqueues_bus::SalienceClass::Urgent,
        )))
        .await
    }

    async fn deliver(
        &self,
        request: tinyeventqueues_bus::DeliverRequest,
    ) -> tinybus::Result<tinyeventqueues_bus::DeliverResponse> {
        self.turns.lock().unwrap().push(request.turn);
        std::future::ready(Ok(tinyeventqueues_bus::DeliverResponse::new(
            tinyeventqueues_bus::DeliverStatus::Accepted,
        )))
        .await
    }
}

#[test]
fn the_host_interface_matches_the_contract() {
    let host = FakeHost::default();
    let methods: Vec<_> = host.members().into_iter().map(|m| m.to_string()).collect();
    assert_eq!(methods, names::host::METHODS.to_vec());
    assert_eq!(host.name().to_string(), names::host::INTERFACE);
}

#[tokio::test]
async fn turns_are_delivered_to_the_host_over_the_bus() {
    let rig = rig(true).await;
    let bus = rig.bus.clone();
    let host_side = rig.host.clone();
    let host = FakeHost::default();
    host_side
        .serve_at(names::host::OBJECT_PATH.try_into().unwrap(), host.clone())
        .await
        .unwrap();
    host_side
        .request_name(names::host::INTERFACE)
        .await
        .unwrap();
    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    setup_with_test_backing(
        service.clone(),
        manual(),
        Slot::default(),
        rig.test_backing(),
    )
    .await
    .unwrap();
    let proxy = host_side
        .proxy(names::INTERFACE, names::OBJECT_PATH, names::INTERFACE)
        .unwrap();

    let event = InboundEvent::new(
        "web:chat",
        "1",
        "c",
        Sender::new("u"),
        EventKind::Message,
        0,
    )
    .direct(true)
    .text("/help");
    let _: IngestResponse = proxy
        .call(names::methods::INGEST, (IngestRequest::new(vec![event]),))
        .await
        .unwrap();
    let drained: DrainResponse = proxy
        .call(names::methods::DRAIN, (DrainRequest::new(),))
        .await
        .unwrap();
    assert_eq!(drained.report.dispatched, 1);
    let turns = host.turns.lock().unwrap().clone();
    assert_eq!(turns.len(), 1);
    assert_eq!(turns[0].events[0].event_id, "1");
}

#[tokio::test]
async fn an_absent_host_makes_delivery_back_off() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let service = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let bus_host = BusHost {
        connection: service.clone(),
    };
    assert!(format!("{bus_host:?}").contains("BusHost"));
    let request = tinyeventqueues_bus::ClassifySalienceRequest::new(
        InboundEvent::new("s", "1", "c", Sender::new("u"), EventKind::Message, 0),
        30,
    );
    let error = bus_host.classify_salience(request).await.unwrap_err();
    assert!(error.to_string().contains("ClassifySalience"));
}

#[tokio::test]
async fn the_setup_factory_refuses_a_missing_storage_callback() {
    let rig = rig(true).await;
    let connection = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let setup = setup_factory();
    let error = setup(connection, manual()).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("host callback destination is required")
    );
}

#[tokio::test]
async fn a_malformed_configuration_fails_setup() {
    let rig = rig(true).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let refused = setup_with_test_backing(
        module,
        json!({ "timer": { "bogus": 1 } }),
        Slot::default(),
        rig.test_backing(),
    )
    .await;
    assert!(refused.unwrap_err().to_string().contains("timer"));
}

#[tokio::test]
async fn a_configuration_that_names_a_missing_queue_fails_setup() {
    let rig = rig(true).await;
    let module = Connection::connect(rig.bus.connect().await.unwrap())
        .await
        .unwrap();
    let mut pipeline: Value = serde_json::from_str(crate::config::SHIPPED_PIPELINE_JSON).unwrap();
    pipeline["dead_letter"]["queue"] = json!("teq.nowhere");
    let refused = setup_with_test_backing(
        module,
        json!({
            "pipeline": pipeline,
            "timer": { "enabled": false },
        }),
        Slot::default(),
        rig.test_backing(),
    )
    .await;
    assert!(refused.unwrap_err().to_string().contains("teq.nowhere"));
}

#[tokio::test]
async fn a_pipeline_on_host_storage_survives_a_module_restart() {
    let rig = rig(true).await;
    let event = InboundEvent::new(
        "composio:github",
        "evt-1",
        "repo",
        Sender::new("u"),
        EventKind::Trigger,
        0,
    );
    let (proxy, first_instance) = rig.module(manual()).await;
    let first: IngestResponse = proxy
        .call(
            names::methods::INGEST,
            (IngestRequest::new(vec![event.clone()]),),
        )
        .await
        .unwrap();
    assert_eq!(first.results[0].disposition, Disposition::Accepted);
    // The process ends: its connection closes and its bus name is released.
    first_instance.connection.close().await.unwrap();

    // The queued event and the module's state are on the host's backend.
    let scoped = rig
        .backend
        .for_scope(&Scope::new("local").unwrap())
        .unwrap();
    let ingress = scoped.queues().stats("teq.ingress").await.unwrap();
    assert_eq!(ingress.ready, 1);

    // A new module instance over the same host storage: a restart.
    let (proxy, _module) = rig.module(manual()).await;
    let again: IngestResponse = proxy
        .call(names::methods::INGEST, (IngestRequest::new(vec![event]),))
        .await
        .unwrap();
    assert_eq!(again.results[0].disposition, Disposition::Duplicate);
    let stats: StatsResponse = proxy
        .call(names::methods::STATS, (StatsRequest::new(),))
        .await
        .unwrap();
    assert_eq!(stats.queues[0].ready, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_module_timer_delivers_without_a_drain() {
    let rig = rig(true).await;
    let host = FakeHost::default();
    rig.host
        .serve_at(names::host::OBJECT_PATH.try_into().unwrap(), host.clone())
        .await
        .unwrap();
    rig.host.request_name(names::host::INTERFACE).await.unwrap();
    let (proxy, instance) = rig
        .module(json!({
            "timer": { "min_interval_ms": 5, "max_interval_ms": 50 },
        }))
        .await;
    assert!(instance.slot.running());

    // A direct control command flushes at once: the turn needs one pass.
    let event = InboundEvent::new(
        "web:chat",
        "1",
        "c",
        Sender::new("u"),
        EventKind::Message,
        0,
    )
    .direct(true)
    .text("/help");
    let _: IngestResponse = proxy
        .call(names::methods::INGEST, (IngestRequest::new(vec![event]),))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while host.turns.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the timer delivered the turn");
    assert_eq!(host.turns.lock().unwrap()[0].events[0].event_id, "1");
}

/// A statically linked module without an admitted storage callback fails
/// closed instead of borrowing the process-global storage service.
#[cfg(feature = "static-link")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_linked_module_without_a_storage_callback_fails_closed() {
    use tinybus::module::ModuleHost;
    use tinybus::module::ModuleState;

    let linked = crate::linked_module().unwrap();
    assert_eq!(linked.manifest.module.name, "tinyeventqueues");
    assert_eq!(linked.manifest.bus_name.as_str(), names::INTERFACE);
    let bus = MemoryBus::new();
    let broker = Broker::new();
    broker.clone().spawn(bus.clone());
    let module_host = ModuleHost::new(broker);
    module_host
        .attach_linked_with_config(linked, manual())
        .unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let state = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(info) = module_host
                .list()
                .into_iter()
                .find(|info| info.name == "tinyeventqueues")
            {
                match &info.state {
                    ModuleState::Failed { .. } | ModuleState::Faulted { .. } => {
                        break info.state;
                    }
                    _ => {}
                }
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the linked module reaches a terminal setup failure");
    assert!(matches!(
        state,
        ModuleState::Failed { .. } | ModuleState::Faulted { .. }
    ));
    assert!(
        client
            .list_names()
            .await
            .unwrap()
            .iter()
            .all(|name| name.as_str() != names::INTERFACE)
    );
}

/// The shipped cdylib uses the unique host callback destination admitted by
/// TinyBus and reads/writes the host's storage bundle through that binding.
#[cfg(all(target_os = "linux", not(feature = "static-link")))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_loaded_module_uses_only_its_admitted_host_storage_callback() {
    let source = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("libtinyeventqueues.so");
    assert!(
        source.is_file(),
        "test cdylib is missing: {}",
        source.display()
    );
    let dir = std::env::current_dir()
        .unwrap()
        .join("target")
        .join(format!("callback-module-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let artifact = dir.join("libtinyeventqueues.so");
    std::fs::copy(&source, &artifact).unwrap();
    let hash = tinybus::module::sha256_file(&artifact).unwrap();
    std::fs::write(
        dir.join("modules.toml"),
        format!("\"libtinyeventqueues.so\" = \"{hash}\"\n"),
    )
    .unwrap();

    let bus = MemoryBus::new();
    let broker = Broker::new();
    let _broker_task = broker.clone().spawn(bus.clone());
    let modules = tinybus::module::ModuleHost::new(broker);
    let binding = prepare_storage_host_callbacks(&modules).await.unwrap();
    let backend = Arc::new(MemoryBackend::new());
    let bound = Arc::clone(&backend);
    let endpoint = PerCallerEndpoint::new(move |_sender| {
        let storage = bound.for_scope(&Scope::new("local").ok()?).ok()?;
        Some(StorageServer::new(storage))
    });
    let _served = serve_with_host_callbacks(binding.clone(), Arc::new(endpoint))
        .await
        .unwrap();

    modules
        .load_file_with_host_callbacks(
            &artifact,
            json!({
                "storage_callback": binding.unique_name(),
                "timer": { "enabled": false }
            }),
            &binding,
        )
        .await
        .unwrap();
    let client = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let proxy = client
        .proxy(names::INTERFACE, names::OBJECT_PATH, names::INTERFACE)
        .unwrap();
    let event = InboundEvent::new(
        "callback-test",
        "stored",
        "conversation",
        Sender::new("user"),
        EventKind::Message,
        1,
    );
    let response: IngestResponse = proxy
        .call(names::methods::INGEST, (IngestRequest::new(vec![event]),))
        .await
        .unwrap();
    assert_eq!(response.results[0].disposition, Disposition::Accepted);
    let stats: StatsResponse = proxy
        .call(names::methods::STATS, (StatsRequest::new(),))
        .await
        .unwrap();
    assert_eq!(stats.queues.iter().map(|queue| queue.ready).sum::<u64>(), 1);

    let outsider = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    assert!(
        tinystoragedrivers_busproxy::connect_storage_at(outsider, binding.unique_name().clone(),)
            .await
            .is_err()
    );
    std::fs::remove_dir_all(dir).unwrap();
}
