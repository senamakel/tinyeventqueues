//! Downloads a tagged release asset and calls the loaded `TinyBus` module.
//!
//! Run it with the release tag URL, platform archive, and archive SHA-256:
//!
//! ```text
//! cargo run --example verify_github_release -- \
//!   https://github.com/tinyhumansai/tinyeventqueues/releases/tag/v0.1.0 \
//!   tinyeventqueues-0.1.0-ubuntu-24.04-x86_64.tar.gz \
//!   <sha256>
//! ```

use std::io;
use std::sync::Arc;
use std::time::Duration;

use tinybus::Connection;
use tinybus::broker::Broker;
use tinybus::module::ModuleHost;
use tinybus::transport::memory::MemoryBus;
use tinyeventqueues::{StatsRequest, StatsResponse, names};
use tinystoragedrivers_api::StorageBackend;
use tinystoragedrivers_busproxy::{HOST_NAME, PerCallerEndpoint, StorageServer, serve};
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::Scope;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (release_url, archive, sha256) = arguments()?;
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.spawn(bus.clone());
    // The module refuses to start without host storage. Serve memory storage
    // under the shared storage service name, as a host that serves one storage
    // service for every module does.
    let host = Connection::connect(bus.connect().await?).await?;
    let backend = Arc::new(MemoryBackend::new());
    let endpoint = PerCallerEndpoint::new(move |_sender| {
        let storage = backend.for_scope(&Scope::new("local").ok()?).ok()?;
        Some(StorageServer::new(storage))
    });
    let _served = serve(&host, Arc::new(endpoint)).await?;
    let module_host = ModuleHost::new(broker);
    let info = module_host.load_github_release(
        &release_url,
        &archive,
        Some(&sha256),
        serde_json::json!({ "storage_callback": HOST_NAME }),
    )?;

    if info.name != env!("CARGO_PKG_NAME") {
        return Err(io::Error::other(format!(
            "loaded module `{}` instead of `{}`",
            info.name,
            env!("CARGO_PKG_NAME")
        ))
        .into());
    }

    let client = Connection::connect(bus.connect().await?).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let claimed = client.list_names().await?;
            if claimed.iter().any(|name| name.as_str() == names::INTERFACE) {
                return tinybus::Result::Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;

    let proxy = client.proxy(names::INTERFACE, names::OBJECT_PATH, names::INTERFACE)?;
    let stats: StatsResponse = proxy
        .call(names::methods::STATS, (StatsRequest::new(),))
        .await?;
    if stats.queues.is_empty() {
        return Err(io::Error::other("module reported no queues").into());
    }

    println!(
        "verified {archive} from {release_url} as TinyBus module `{}`",
        info.name
    );
    broker_task.abort();
    Ok(())
}

fn arguments() -> Result<(String, String, String), io::Error> {
    let mut args = std::env::args().skip(1);
    let usage = "usage: cargo run --example verify_github_release -- \
                 <release-tag-url> <archive-name> <sha256>";
    let release_url = args
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, usage))?;
    let archive = args
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, usage))?;
    let sha256 = args
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, usage))?;
    if args.next().is_some() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, usage));
    }
    Ok((release_url, archive, sha256))
}
