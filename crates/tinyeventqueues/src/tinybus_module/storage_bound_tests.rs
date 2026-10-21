//! Exercise the module's explicit TSD lease route through a governed host.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tinybus::{
    Connection,
    broker::Broker,
    module::ModuleHost,
    transport::memory::{MemoryBus, MemoryTransport},
};
use tinyeventqueues_bus::{
    Disposition, EventKind, InboundEvent, IngestRequest, IngestResponse, Sender, names,
};
use tinystoragedrivers_bus::StorageNeeds;
use tinystoragedrivers_busproxy::{
    AuditEvent, AuditSink, Environment, GovernedEndpointLimits, GovernedStorage, GrantPolicy,
    HostContext, HostFailure, ResolvedStorage, StorageDescriptor, StorageGrant, StorageLease,
};
use tinystoragedrivers_client::StorageServer;
use tinystoragedrivers_core::{MemoryStorage, Scope, StorageBackend};
use tinystoragedrivers_tinybus::{StorageRegistration, attach_storage};

use crate::config::shipped_manifest;
use crate::tinybus_module::{setup, storage, ticker::Slot};

struct QueueHostContext;

impl HostContext for QueueHostContext {}

struct QueueHostEnvironment {
    backend: Arc<MemoryStorage>,
    scope: Scope,
    manifest: Arc<tinystoragedrivers_core::queue::QueueManifest>,
}

#[async_trait]
impl Environment<QueueHostContext> for QueueHostEnvironment {
    async fn resolve(
        &self,
        _context: &QueueHostContext,
        _module: &tinybus::module::ModuleId,
    ) -> std::result::Result<ResolvedStorage, HostFailure> {
        let storage = self
            .backend
            .for_scope(&self.scope)
            .map_err(|_| HostFailure::Denied("test-storage-unavailable"))?;
        let collections: Vec<tinystoragedrivers_bus::CollectionSpec> = serde_json::from_value(
            serde_json::to_value(tinyeventqueues_bus::storage::collection_specs())
                .map_err(|_| HostFailure::Denied("event-queue-schema-serialization-failed"))?,
        )
        .map_err(|_| HostFailure::Denied("event-queue-schema-decode-failed"))?;
        let grant = StorageGrant::from_needs(
            StorageNeeds {
                collections: &collections,
                ..StorageNeeds::default()
            },
            &GrantPolicy {
                writable: true,
                ..GrantPolicy::default()
            },
        )
        .map_err(|_| HostFailure::Denied("event-queue-storage-grant-invalid"))?;
        let grant = grant
            .with_queue_manifest(Arc::clone(&self.manifest))
            .map_err(|_| HostFailure::Denied("event-queue-manifest-invalid"))?;
        let server = StorageServer::new(storage).with_queue_manifest(Arc::clone(&self.manifest));
        Ok(ResolvedStorage {
            backend_identity: "event-queues-bound-test".to_owned(),
            binding_epoch: 1,
            canonical_scope: "bound-queue-tenant".to_owned(),
            grant,
            server: Arc::new(server),
        })
    }

    async fn admit_write(
        &self,
        _context: &QueueHostContext,
        _module: &tinybus::module::ModuleId,
    ) -> std::result::Result<(), HostFailure> {
        Ok(())
    }
}

struct QueueHostAudit;

impl AuditSink for QueueHostAudit {
    fn record(&self, _event: AuditEvent) {}
}

type QueueHostRegistration =
    StorageRegistration<QueueHostEnvironment, QueueHostAudit, QueueHostContext>;

struct SetupFixture {
    registration: Arc<QueueHostRegistration>,
}

static SETUP_FIXTURE: std::sync::OnceLock<std::sync::Mutex<Option<SetupFixture>>> =
    std::sync::OnceLock::new();

fn setup_fixture() -> &'static std::sync::Mutex<Option<SetupFixture>> {
    SETUP_FIXTURE.get_or_init(|| std::sync::Mutex::new(None))
}

type ObservedLease = (StorageLease, StorageDescriptor, Connection);

static OBSERVED_LEASE: std::sync::OnceLock<std::sync::Mutex<Option<ObservedLease>>> =
    std::sync::OnceLock::new();

fn observed_lease() -> &'static std::sync::Mutex<Option<ObservedLease>> {
    OBSERVED_LEASE.get_or_init(|| std::sync::Mutex::new(None))
}

mod bound_module_export {
    use super::*;

    async fn setup_with_context(
        context: tinybus_module::ModuleSetupContext,
        _config: Option<serde_json::Value>,
    ) -> tinybus::Result<()> {
        let fixture = setup_fixture()
            .lock()
            .map_err(|_| tinybus::Error::failed("bound storage fixture lock poisoned"))?
            .take()
            .ok_or_else(|| tinybus::Error::failed("bound storage fixture missing"))?;
        let connection = context.connection().clone();
        let peer = connection
            .unique_name()
            .ok_or_else(|| tinybus::Error::failed("event-queues module has no unique peer"))?;
        let (lease, descriptor) = fixture
            .registration
            .register_lease(
                tinystoragedrivers_busproxy::HostStorageContext::new(QueueHostContext),
                peer,
            )
            .await
            .map_err(|_| tinybus::Error::failed("event-queues storage lease was refused"))?;
        *observed_lease()
            .lock()
            .map_err(|_| tinybus::Error::failed("bound storage lease lock poisoned"))? =
            Some((lease, descriptor.clone(), connection.clone()));

        let mut config = serde_json::json!({ "timer": { "enabled": false } });
        config["storage_descriptor"] = serde_json::to_value(descriptor)
            .map_err(|_| tinybus::Error::failed("storage descriptor serialization failed"))?;
        setup(connection, config, Slot::default()).await
    }

    tinybus_module::module_export_static! {
        setup_with_context = setup_with_context,
        config = Option<serde_json::Value>,
        worker_threads = 2,
        provides = ["ai.tinyhumans.tinyeventqueues.EventQueues"],
        methods = ["Ingest", "Configure", "Stats", "ListDead", "Redrive", "Drain"],
        signals = [],
        requires = [],
        optional = [],
        lazy = false,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_host_issued_lease_supplies_scoped_queues_and_rejects_other_callers()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    let bus = MemoryBus::new();
    let broker = Broker::new_restricted_module_control();
    let (broker_transport, controller_transport) = MemoryTransport::pair();
    broker.attach_module_controller(Arc::new(broker_transport))?;
    let controller = Connection::connect(Box::new(controller_transport)).await?;
    let broker_task = broker.spawn(bus.clone());
    let host = ModuleHost::new(broker);
    let backend = Arc::new(MemoryStorage::new());
    let scope = Scope::new("bound-queue-tenant")?;
    let manifest = Arc::new(shipped_manifest()?);
    let registration: Arc<QueueHostRegistration> = Arc::new(
        attach_storage(
            &host,
            &controller,
            GovernedStorage::new(
                Arc::new(QueueHostEnvironment {
                    backend: Arc::clone(&backend),
                    scope: scope.clone(),
                    manifest,
                }),
                Arc::new(QueueHostAudit),
                GovernedEndpointLimits::default(),
            ),
            tokio::runtime::Handle::current(),
        )
        .await
        .map_err(|_| std::io::Error::other("governed storage attachment failed"))?,
    );
    *setup_fixture()
        .lock()
        .map_err(|_| std::io::Error::other("bound storage fixture lock poisoned"))? =
        Some(SetupFixture {
            registration: Arc::clone(&registration),
        });

    let info = host
        .attach_linked_with_config(bound_module_export::linked_module()?, serde_json::json!({}))?;
    if let Err(error) = host.ensure_ready(info.manifest.bus_name.as_str()).await {
        return Err(
            std::io::Error::other(format!("bound event-queues setup failed: {error}")).into(),
        );
    }
    let (lease, descriptor, module_connection) = observed_lease()
        .lock()
        .map_err(|_| std::io::Error::other("bound storage lease lock poisoned"))?
        .take()
        .ok_or_else(|| std::io::Error::other("bound storage lease was not captured"))?;

    let client = Connection::connect(bus.connect().await?).await?;
    let proxy = client.proxy(names::INTERFACE, names::OBJECT_PATH, names::INTERFACE)?;
    let event = InboundEvent::new(
        "web:chat",
        "bound-1",
        "conversation",
        Sender::new("user"),
        EventKind::Message,
        1,
    );
    let response: IngestResponse = proxy
        .call(names::methods::INGEST, (IngestRequest::new(vec![event]),))
        .await?;
    assert_eq!(response.results[0].disposition, Disposition::Accepted);

    let scoped = backend.for_scope(&scope)?;
    assert_eq!(scoped.queues().stats("teq.ingress").await?.ready, 1);

    let wrong_peer = storage::open(client.clone(), Some(descriptor.clone())).await;
    assert!(
        wrong_peer.is_err(),
        "a different bus peer must not use the module's lease: {wrong_peer:?}"
    );

    assert!(registration.revoke(&lease).await);
    let revoked = storage::open(module_connection, Some(descriptor)).await;
    assert!(
        revoked.is_err(),
        "a revoked lease must fail rather than falling back to memory: {revoked:?}"
    );

    let registration = Arc::try_unwrap(registration)
        .map_err(|_| std::io::Error::other("storage registration remained in use"))?;
    registration.shutdown().await?;
    host.shutdown(Duration::from_secs(2)).await;
    broker_task.abort();
    Ok(())
}
