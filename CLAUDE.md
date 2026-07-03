# CLAUDE.md — Real-Time Ordered Distribution Engine

## What this is

A high-throughput engine that keeps an **ordered, durable, append-only audit trail per key** and **fans the latest state out to massive numbers of concurrent subscribers** (human + agent) at low latency.

The engine is **payload-opaque**: it stores bytes in sequence per key, last-sequenced-wins on complete-state bytes. It **never** interprets payloads or computes new bytes from old ones. Simplicity is the product: provably correct, trivially recoverable, domain-agnostic. Primary product bet: state plane / coordination substrate for agent fleets, with the audit trail as the hero feature.

## Non-negotiable invariants

Any change that violates one of these is wrong, no matter how convenient. Flag conflicts instead of working around them.

1. **The log is the single source of truth.** Everything else (in-memory state, compacted topic, checkpoints, OLAP, cold storage) is a derived, rebuildable projection. If it can't be rebuilt from the log (plus placement history), don't build it that way.
2. **The core never interprets payloads.** No decoding, no merging, no computing bytes from bytes. Derived projections may use pluggable decoders; the core may not.
3. **One collection = one partition = one ordered lane.** The engine never splits a collection. Hot collection → the user shards their own keyspace.
4. **Ack only on durable commit** (`acks=all`). Fan-out must never emit a phantom (state that could be lost).
5. **A slow consumer never back-pressures the log or the sequencer.** Under load: conflate to latest, never block.
6. **In-memory state is a bounded cache, never a source of truth.** App-level eviction only; never rely on OS swap. Losing it must always be recoverable via rehydration.
7. **Ordering = broker arrival order per partition.** No cross-partition ordering, no client-causal ordering, no CAS/conditional writes (v1). Don't fake any of these.
8. **Delivery contract:** fan-out = at-least-latest state per key with gaps allowed (conflation), never guaranteed-contiguous events. Consumers needing every event replay the log. On any stream error: reconnect + resnapshot.
9. **The change is the unit of atomicity.** A change = N (key, bytes) entries in one collection = exactly one log record = one sequence number, applied as one unit everywhere. No layer may ever expose a partial change; conflation skips whole changes, never halves of one. Atomicity never spans collections (see `docs/engine-design.md` § Atomic changes).
10. **Coordination cost scales with events (deaths, rebalances), never with traffic or partition count.** etcd write load must stay proportional to coordination events.
11. **Ownership is assigned top-down by the controller, not elected per partition.** Leases are liveness mechanisms, not consensus rounds. The controller election is the only election in the system.
12. **Control plane down ≠ data plane down.** Existing assignments keep serving; only changes pause.
13. **Migration safety comes from the ack fence + epoch rule, never from timing.** Timing bounds freeze duration (liveness); the epoch-closed-at-X rule gives safety.

## Architecture (mental model)

- **Data model:** Tenant → Org → **Collection** → K/V (V = opaque bytes). The collection is the atom of placement, ordering, ownership, subscription.
- **Write path (stateless):** client → platform server → lookup `collection → partition` (cached, leased) → append to Redpanda partition. Offset = sequence number.
- **Messaging tier:** each server owns partitions via etcd lease and consumes the log directly. A **per-collection actor mailbox** (coroutine + Channel) serializes updates + registrations and conflates on overflow — this serialization is what makes snapshot/stream handoff correct.
- **State cache:** latest-per-key maps only for collections with live interest. Byte-weighted LRU with a hard cap; pin subscribed collections; evict only cold ones; rehydrate on miss from the compacted topic (view-at-V, catch up from V+1 — same path as failover).
- **Registration:** connect → snapshot at offset L from the in-memory cache (never from lagging OLAP) → stream L+1 onward.
- **Placement:** `collection → partition` is an explicit load-aware mapping in Postgres (never modulo hash — partition count grows append-only, nothing rehashes). Cached at platform servers as a **lease with epoch** (see migration).
- **Ownership:** `partition → server` leases in dedicated etcd (lease + watch + ModRevision fencing). Assignment controller (leader-elected replicas, one active) writes assignments; off the data path.
- **Derived:** log consumers project into ClickHouse (analytics/UI replay) and object storage (full history). Authoritative replay reads the log; convenient replay reads OLAP.

## Tech stack (locked)

| Concern | Choice |
|---|---|
| Language/runtime | Kotlin + Ktor + coroutines (actor mailbox = coroutine/Channel) |
| Log of record | Redpanda (Kafka API; offset = seq) |
| Placement + history | Postgres |
| Coordination | Dedicated etcd (leases, watch, ModRevision fencing, controller election) |
| OLAP sink | ClickHouse |
| Cold archive | S3-compatible object store (MinIO now; interface, not vendor) |
| Client transport | gRPC (pods) / WebSocket (browsers); one binary framing over both |
| Serialization | Fixed binary envelope (versioned ABI) + opaque payload |
| Orchestration | Commodity k8s (k3s/Talos); vanilla manifests/Helm |

**Portability rules:** zero cloud-specific resources in the app layer. Every stateful dependency is a standard protocol (Kafka API, S3 API, Postgres wire, ClickHouse wire) with a self-hosted default and a managed upgrade path — flipping to managed is a connection-string change, never an app change. Terraform: small swappable per-provider infra module + large provider-agnostic app module. Everything must stay rebuildable-from-log so relocation is a scripted restore/replay runbook.

## Envelope

Fixed binary envelope, versioned ABI. Must carry at minimum: tenant/collection/key, **epoch**, **write-id** (for retry dedup across partitions), and headers for the agent convention (session/agent IDs, event type, parent-span). Payload is opaque bytes. Treat envelope changes as ABI changes: versioned, additive, never reinterpreted.

Implemented so far: `EnvelopeV1` — a provisional minimal framing (version byte, entry count, then per-entry key, flags byte, value; flag bit 0 = tombstone). Tenant, epoch, write-id, and headers land with the full envelope design. **ABI freeze begins at the first real deployment**: until then v1 may be amended in place; after that, any layout change bumps the version byte and old layouts are never reinterpreted.

## Collection migration (the correctness-critical protocol)

Moving collection C from `P_old` to `P_new` without breaking per-key order. **Fence at the ack, never at a consumer** (the old owner is a consumer; it cannot reject a produce).

Prerequisites:
1. **Leased placement:** platform servers hold `(C → P, epoch e)` as a lease (TTL ~1–2s). Ack a write only while holding a valid lease at epoch e: check epoch → append with epoch stamped → re-check lease → ack (or return retryable-unacked). **Consumer rule:** epoch e is closed at offset X; every consumer skips epoch-e records for C past X. Such records were never acked, so skipping loses nothing.
2. **Never seed the log.** `state(C) = fold(P_old[..X]) ⊕ fold(P_new[Y..])` via Postgres placement history. Write a derived **checkpoint** `state-at-X` keyed `(C, epoch)` per migration; rehydrate = checkpoint + tail; missing checkpoint → segment fold (slow, correct).
3. **Pre-warmed follower:** new owner rehydrates + tails `P_old` live before any freeze.

Phases: **PREPARE** (warm, no freeze) → **FENCE** (bump epoch; barrier = all live platform servers ack, lease TTL as ceiling) → **OBSERVE X** (read end-of-partition after the barrier; X is observed, never declared) → **SEAL** (persist segment record in Postgres — the commit point; checkpoint async) → **OPEN** (ACTIVE at P_new, epoch e+1; retries carry write-id) → **HANDOFF** (redirect subscribers with `version = (e, X)`; versions order as (epoch, offset) tuples; new owner coalesces snapshots).

Write unavailability = barrier + X-read + one Postgres write. Independent of collection size.

State machine lives in etcd/Postgres and every step is idempotent (controller crash → new leader resumes). Full failure analysis in `docs/engine-design.md`.

**Documented fallback (don't build):** route writes through the partition owner (true sequencer) — simpler fencing + CAS nearly free, at the cost of write-path statelessness. Likely graduates to the v2 write path if the agent market demands CAS.

## Open issues — do not silently "solve" these

Tracked in `docs/engine-design.md` § Design review. When touching adjacent code, respect that these are open; propose designs, don't improvise:

- **Compacted topic design** (load-bearing, underspecified): a second derived topic; each record must embed the original log offset; producer/lag/failure mode TBD.
- **Write-id dedup**: where dedup state lives, retention. Kafka idempotent producers don't cover retries that switch partitions.
- **Pinned-set quotas**: per-tenant subscription/pinned-byte quotas + defined degrade mode (the pinned LRU set is currently unbounded → OOM).
- **Thundering herd on owner death**: reconnect jitter, snapshot coalescing (one rehydration serves N registrants), maybe warm standbys.
- **Head-of-line blocking**: per-tenant/collection produce quotas.
- **Deletes/tombstones**: change-level semantics decided (tombstone entries in the envelope remove keys from the latest-state view; see docs § Atomic changes). Still open: retention/compaction interplay and true erasure — product-critical (GDPR); intended direction: crypto-shredding via per-collection/tenant keys.
- **Missing sections**: authn/authz + tenant isolation, payload size limits, envelope versioning policy, DR/geo runbook, observability for correctness invariants.

## Observability requirements

Correctness invariants are first-class metrics with alerts, not logs:
- Epoch-e ack attempts after a FENCE barrier (must be zero; nonzero = lease bug).
- Exactly one ACTIVE epoch per collection; bounded MIGRATING age (stalled FENCE = live outage).
- Skipped post-X records: rare and logged, never silently common.
- Checkpoint lag behind sealed segments; compacted-topic lag; cache-cap pressure / pinned-bytes per tenant.

## Engineering conventions

- Kotlin coroutines: one mailbox coroutine per active collection; all mutations to a collection's state go through its mailbox — no shared mutable state across collections.
- Conflation is explicit and bounded: mailbox overflow → conflate to latest per key, count it (metric), never block the log consumer.
- Every derived store gets a `rebuild-from-log` path tested in CI. If a projection can't be rebuilt, the PR is wrong.
- Failure-path tests are not optional for anything touching leases, epochs, or migration: test the barrier, the suppressed-ack retry, controller crash at each phase.
- No cloud-specific APIs in app code. No OS-swap reliance. No unbounded queues anywhere.

## Reference docs

- `docs/engine-design.md` — full design, review checklists, migration redesign rationale, strategic/positioning analysis, target use cases.
