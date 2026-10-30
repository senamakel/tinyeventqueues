//! Loads a built module through the real `TinyBus` dynamic loader.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tinybus::Connection;
use tinybus::broker::Broker;
use tinybus::module::ModuleHost;
use tinybus::transport::memory::MemoryBus;
use tinyeventqueues::{StatsRequest, StatsResponse, names};
use tinystoragedrivers_api::StorageBackend;
use tinystoragedrivers_busproxy::{
    PerCallerEndpoint, StorageServer, prepare_storage_host_callbacks, serve_with_host_callbacks,
};
use tinystoragedrivers_testkit::MemoryBackend;
use tinystoragedrivers_types::Scope;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let module = module_argument()?;
    let bus = MemoryBus::new();
    let broker = Broker::new();
    let broker_task = broker.clone().spawn(bus.clone());
    let module_host = ModuleHost::new(broker);
    let binding = prepare_storage_host_callbacks(&module_host).await?;
    let backend = Arc::new(MemoryBackend::new());
    let bound = Arc::clone(&backend);
    let endpoint = PerCallerEndpoint::new(move |_sender| {
        let storage = bound.for_scope(&Scope::new("local").ok()?).ok()?;
        Some(StorageServer::new(storage))
    });
    let _served = serve_with_host_callbacks(binding.clone(), Arc::new(endpoint)).await?;
    write_module_manifest(&module)?;
    let info = module_host
        .load_file_with_host_callbacks(
            &module,
            serde_json::json!({ "storage_callback": binding.unique_name() }),
            &binding,
        )
        .await?;

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
        "verified {} as TinyBus module `{}`",
        module.display(),
        info.name
    );
    broker_task.abort();
    Ok(())
}

fn write_module_manifest(module: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let directory = module.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "module path has no parent directory",
        )
    })?;
    let name = module
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "module filename is not valid UTF-8",
            )
        })?;
    let hash = tinybus::module::sha256_file(module)?;
    let manifest = directory.join("modules.toml");
    std::fs::write(manifest, format!("\"{name}\" = \"{hash}\"\n"))?;
    Ok(())
}

fn module_argument() -> Result<PathBuf, io::Error> {
    std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: cargo run --example verify_module -- <module-path>",
            )
        })
}
