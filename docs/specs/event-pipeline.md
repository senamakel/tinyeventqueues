# Inbound Event Pipeline

**Status:** Implemented, in the order of [the plan](../plans/event-pipeline.md). **Owner:** tinyeventqueues.

## Problem

An agent with access to chat channels (web chat, Telegram, Discord, Slack),
connector triggers (X replies through Composio), webhooks and notifications
receives far more events than it should act on. Handing each one straight to
the agent wastes turns on noise, lets one chatty source starve the rest, loses
events on a crash between the platform acknowledgement and the work, and
replays events a platform redelivers after a restart.

A person filters the same stream without thinking: they answer direct
questions now, skim group chatter, batch the newsletter, ignore the bot that
keeps repeating itself, and read the backlog as a summary.

## Goals and non-goals

Goals:

- durably queue every inbound event before the platform is acknowledged;
- recognize redeliveries across restarts;
- filter deterministically first, and ask a model only for borderline cases,
  within a budget;
- deliver work to the host one turn per conversation at a time, in order,
  with interactive work never stuck behind a background flood;
- bound how much a scheduled source costs per tick, without dropping the rest;
- make every drop, deferral and dead letter observable, with a reason.

Non-goals: talking to platforms (hosts normalize events into the envelope and
acknowledge platforms), running the agent, exactly-once delivery (delivery is
at least once and turns carry stable ids), and choosing a storage backend
(the pipeline runs over the `tinystoragedrivers` ports).

## Pipeline

```text
ingress → normalize → gate → coalesce → salience → attention lanes
        → cadence / ingestion budget → dispatch (host Deliver callback)
```

### 1. Ingress

`Ingest` validates each envelope (non-empty `event_id`, `source`,
`conversation_key`, sender id; keys at most 512 bytes; invalid → `dropped`
with `invalid_envelope`), then enqueues the raw envelope on the ingress queue
with the dedupe key `source:event_id`. It returns only after that durable
write, so the caller acknowledges the platform after it. The response says per
event whether it was `accepted`, a `duplicate`, or `dropped`, and whether the
platform may be acknowledged now (`ack_platform`).

Redeliveries:

- when the queue backend dedupes (`QueueFeatures::dedupe`), the ingress queue's
  manifest `dedupe_window_ms` (30 days shipped) is the completion tombstone:
  the key is rejected while the message is held and for the window after it is
  processed, and the record lives in storage, so it survives a restart;
- when it does not (a broker), the pipeline keeps its own tombstones in the
  document store for `ingress.tombstone_ttl_ms`, written after the enqueue, and
  also rejects events whose terminal outcome is still recorded. A crash between
  the enqueue and the tombstone can let one redelivery through; nothing is ever
  lost.

### 2. Gate (deterministic, no model)

In order, first verdict wins:

1. self-echo (`sender.is_self`) → `dropped` / `self_echo`;
2. sender deny list → `dropped` / `sender_denied`; an allow list that does not
   name the sender → `dropped` / `sender_not_allowed`;
3. direct-message and group policy → `dropped` / `direct_messages_disabled` or
   `groups_disabled`;
4. bot-loop guard: more than `max_events` events within `window_ms` between
   the same bot pair (default 20 per 60 s) → `dropped` / `bot_loop`;
5. token buckets per sender, per conversation and per source, persisted in the
   document store; an event over a bucket is not dropped but spilled to the
   digest lane with `rate_limited`;
6. group chatter not addressed to the agent → `context`: appended to the
   conversation's ambient buffer (bounded), surfaced with its next real turn,
   never silently dropped.

### 3. Coalesce

A durable debounce buffer keyed by `(conversation_key, thread_key, sender)`.
A buffer flushes `debounce_ms` after its last event or `max_wait_ms` after its
first, whichever is earlier. Media and control commands flush immediately.
Events of different threads are never merged; order within a key is preserved.
An `edit` amends the pending event it targets, a `delete` retracts it (both
events end `dropped` / `retracted_by_sender`).

### 4. Salience

Rule features — addressed to the agent, direct message, known contact,
keywords, source weight, recency — give a score from 0 to 100. Habituation: a
near-duplicate of recent content from the same source (`SimHash` over a sliding
window) has its score decayed. Only scores inside the configured borderline
band go to the host's `ClassifySalience` (noise / fyi / actionable / urgent),
within a per-window budget; outside the band or over budget, the rule score
stands.

### 5. Attention lanes

`interactive` (direct messages, mentions, web chat), `actionable`,
`background` and `digest`, each a manifest queue. A turn's queue group is its
conversation, so a conversation has at most one turn in flight; each lane has
a global concurrency limit. Interactive work is dispatched first. A full lane
spills to `digest` (`lane_overflow`), never drops; the digest lane delivers
one turn that folds many events, which the host summarizes.

### 6. Cadence and ingestion budgets

Each source or lane has a cadence: `instant`, `every(<duration>)` or
`cron("<expr>")`. Scheduled cadences accumulate and release one batch per
tick. Each cadence has an ingestion budget (turns per tick, or per second for
`instant`). Over budget, items stay queued with `not_before = next tick`
(`deferred` / `over_budget`): a conversation's pending items fold into one
entry, and backlog beyond the budget folds into the digest. An `instant`
source over budget degrades to a short debounce window and returns to instant
once under budget. The scheduler persists each cadence's next due tick in the
document store, so a restart neither skips nor double-fires a tick. Salience
can promote an item from a scheduled lane to instant.

### 7. Dispatch

The host's `Deliver` callback receives a `Turn`: a stable `turn_id`, the lane,
the conversation and thread, its events oldest first, ambient context, and the
events whose platform acknowledgement waits on it. Per-source ack policy:
`after_ingest` (acknowledged when `Ingest` returns), `after_dispatch` (when
`Deliver` accepts), `after_turn` (when the host's `Deliver` completes the
agent turn). `retry` responses back off through the queue's nack; a turn that
uses up its manifest `max_attempts`, or that the host rejects, is parked in the
dead-letter queue with its reason. `ListDead` lists dead work and `Redrive`
puts it back.

## Configuration

Two editable, strict JSON files at the repository root; unknown fields are
errors that name the field.

- `queues.json`: a `tinystoragedrivers` `QueueManifest` declaring every queue
  (ingress, the four lanes, dead letters) with `max_attempts`,
  `dedupe_window_ms` and `visibility_ms`. The pipeline applies it at
  construction. No queue name or spec is written in Rust.
- `pipeline.json` ([schema](../../schemas/pipeline.schema.json)): the
  pipeline's tunables, referring to manifest queues by name. Every reference
  must be declared, have a `visibility_ms`, and be used by one role only.

`Configure` replaces the running configuration after the same checks.

## Bus surface

Module interface `ai.tinyhumans.tinyeventqueues.EventQueues`: `Ingest`,
`Configure`, `Stats`, `ListDead`, `Redrive`, `Drain`. Host interface
`ai.tinyhumans.tinyeventqueues.Host`: `ClassifySalience`, `Deliver`. Payloads
are in `tinyeventqueues-bus`.

## Loaded module

- Storage: the module connects through the host-admitted storage callback and
  takes the one scope the host bound it to. Its `teq.*` collections are
  declared by `tinyeventqueues-bus::storage::collection_specs()`
  (owner `teq`, module scope), from which the host derives the grant; its
  queues are those `queues.json` declares, served by the host's queue port.
  Without the callback destination, setup fails; there is no in-memory
  fallback.
- Configuration: the host-provided private blob carries `storage_callback`
  (the unique name for the admitted storage callback), plus `queues` (a `queues.json`
  document), `pipeline` (a `pipeline.json` document) and `timer` bounds;
  absent fields run the shipped files. The manifest is applied through the
  queue port at setup.
- Passes: a module-owned timer runs a pass, then sleeps until the next wake
  (ready ingress or lane work, the earliest debounce flush, the earliest staged
  cadence release), clamped to `[min_interval_ms, max_interval_ms]`; `Ingest`
  and a successful `Redrive` wake it at once. `Drain` runs a pass on demand.
  A failed pass is logged and retried; it never stops the timer.
- Callbacks: `ai.tinyhumans.tinyeventqueues.Host` is an optional dependency.
  Without it classification keeps the scored class and deliveries back off.

## Invariants and constraints

- An event is persisted before `Ingest` returns, and before the platform is
  acknowledged.
- Every accepted event ends in exactly one terminal disposition — `delivered`,
  `context`, `digest`, `dead` or `dropped` with a reason. The outcome ledger
  records it with a create-only write, so a replay cannot end it twice; a
  redrive clears it so the event can end again.
- A redelivery is rejected for the tombstone window, across restarts.
- Events of different threads are never merged; events of one key keep their
  order; a conversation has at most one turn in flight.
- Overflow spills to the digest; nothing is dropped oldest-first.
- Time comes from one injectable clock; durable state lives only in the two
  stores, so a new pipeline over the same storage is a restart.

## Acceptance criteria

- A duplicate redelivery after a simulated restart (a new pipeline over the
  same storage) is rejected, with and without native dedupe.
- Gate: self-echo, bot loop, rate limits and unaddressed chatter produce the
  dispositions above.
- Coalesce: no cross-thread merge, per-key order, media and control flush.
- Cadence: an `every(10m)` source receiving 1,000 events produces at most the
  budget of turns per tick and carries the rest folded into the next tick; a
  restart between ticks neither skips nor double-fires; an instant source over
  budget degrades and recovers.
- Dispatch: retries back off; exhausted turns are dead-lettered and redrivable.
- The no-loss property holds over randomized mixed traffic, and a 10,000-event
  flood keeps per-conversation order, never merges threads, and keeps
  interactive latency bounded while background floods.
