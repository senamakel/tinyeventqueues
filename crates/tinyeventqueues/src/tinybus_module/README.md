# TinyBus Adapter

This module is the boundary between the pipeline and TinyBus module ABI v1.
`EventQueuesService` serves `Pipeline` as the typed members `Ingest`,
`Configure`, `Stats`, `ListDead`, `Redrive` and `Drain`, and `setup` registers
the object and claims the well-known interface name. Neither the name, the
object path, nor the payload types are spelled here: they come from
`tinyeventqueues-bus`, so a rename is a compile error in every consumer instead
of an `UnknownMethod` at runtime.

`exports` runs `tinybus_module::module_export_optional_static!` with a
`setup_factory`: the factory runs once per module instance and every setup of
that instance (the first and each reinitialization) shares the pass timer's
slot, so a reinitialization replaces the timer instead of leaving two running.
Without the `static-link` feature it emits the descriptor, embedded manifest,
and initialization symbols consumed by the dynamic loader; with it,
`linked_module()` for a host that links the module into its own executable.
The manifest method list must stay aligned with the interface macro's dispatch
table and with `tinyeventqueues_bus::names::METHODS`; the unit tests check
both.

- `settings.rs`: the private configuration blob (`storage_callback`, `queues`,
  `pipeline`, `timer`), strict, with the shipped files and default timer when absent.
- `storage.rs`: the host's storage over the `tinystoragedrivers` storage bus,
  the one scope the host bound the module to: documents for the `teq.*` state
  (declared by `tinyeventqueues_bus::storage::collection_specs()`) and the
  queue port for the manifest's queues. The host-provided `storage_callback`
  setting names the unique callback binding admitted for this module instance;
  missing or unusable host storage fails setup rather than starting with an
  ephemeral engine.
- `host.rs`: `BusHost`, the pipeline's `Host`, calling the host's optional
  `ai.tinyhumans.tinyeventqueues.Host` interface (`ClassifySalience`,
  `Deliver`) at `names::host::OBJECT_PATH`. While no host serves it,
  classification keeps the scored class and deliveries back off and retry.
- `ticker.rs`: the pass timer. After each pass it sleeps until
  `Pipeline::next_wake` (ready work, the earliest debounce flush or cadence
  release), clamped to the configured bounds; `Ingest` and a successful
  `Redrive` wake it early. `Drain` still runs a pass on demand; passes are
  serialized inside `Pipeline::drain`, so the timer and `Drain` never
  dispatch beyond a lane's concurrency together.
