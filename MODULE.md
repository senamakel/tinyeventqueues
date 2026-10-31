# tinyeventqueues TinyBus Module

This package contains the native `tinyeventqueues` module for TinyBus module
ABI v1. Install only the archive matching the host operating system and
architecture.

The module claims `ai.tinyhumans.tinyeventqueues.EventQueues`, serves it at
`/ai/tinyhumans/tinyeventqueues/EventQueues`, and provides `Ingest`,
`Configure`, `Stats`, `ListDead`, `Redrive` and `Drain`. It durably queues
inbound events and filters them before they reach an agent; hosts serve
`ai.tinyhumans.tinyeventqueues.Host` (`ClassifySalience`, `Deliver`) for the
module's callbacks (an optional dependency). A pipeline pass takes ingress,
flushes debounce buffers, fires due cadence ticks and dispatches due turns to
the host's `Deliver`. The module runs passes on its own timer, waking at the
next debounce flush or cadence tick and right after an `Ingest`; `Drain` runs
one on demand. The interface names, object paths, member names and payload
types are published as the `tinyeventqueues-bus` crate, so a host names them
from a library rather than by string literal.

Storage is the host's: the module connects through the invocation-scoped
`StorageAddress` admitted by its host and runs over the one scope the host
bound it to, documents for its `teq.*` state and the queue port for the queues
`queues.json` declares. `tinyeventqueues-bus` (feature `storage`) publishes
`collection_specs()` for the module's document state and the shipped
`queues.json`. The private module configuration carries the host-issued
`storage_address` (`host` and invocation `object_root`) or a mutually exclusive
`storage_callback` (an attested unique host peer). An explicit route never falls
back to another route. With neither field, the module uses the host's ordinary
caller-scoped storage endpoint; setup fails if that endpoint is unavailable.
There is no in-memory fallback.

The module takes a private configuration blob:
`{"storage_address": {"host": ":1.42", "object_root": "/ai/tinyhumans/tinystorage/Invocation/abc123"}, "queues": <queues.json>, "pipeline": <pipeline.json>, "timer":
{"enabled": true, "min_interval_ms": 100, "max_interval_ms": 5000}}`. Absent
fields run the shipped files and the default timer. A host serving the queue
port should declare the same manifest it passes in `queues`.

A `pipeline.sources[]` entry may carry `"coalesce": {"debounce_ms": …,
"max_wait_ms": …, "max_events": …}`, the source's own debounce: it replaces
the pipeline's `coalesce` timings for that source, and `max_events` caps how
many events one turn of it carries. Releases before v0.1.3 reject the field.

The archive contains one `.so`, `.dylib`, or `.dll` plus `modules.toml`. Keep
those files together when copying them into a TinyBus module directory. The
allowlist binds the native library filename to its SHA-256 digest so TinyBus can
reject a missing, renamed, or modified artifact before initialization.

The GitHub release also publishes `checksum.toml` as a separate asset. TinyBus
checks that manifest before downloading and extracting the selected platform
archive. Install directly from a tagged release with:

```sh
tinybus modules load-github \
  https://github.com/tinyhumansai/tinyeventqueues/releases/tag/v0.1.0 \
  tinyeventqueues-0.1.0-ubuntu-24.04-x86_64.tar.gz \
  <archive-sha256>
```

TinyBus modules are trusted in-process code. Install release artifacts only
from a trusted source and restart the host after replacing a loaded module.
