# Pivot

[![Test](https://github.com/benitogf/pivot/actions/workflows/tests.yml/badge.svg)](https://github.com/benitogf/pivot/actions/workflows/tests.yml)

Pivot synchronizes the state of multiple [ooo](https://github.com/benitogf/ooo) servers
into one logical, eventually-consistent cluster. It is a small Go library, not a
separate process: you call `pivot.Setup(server, config)` on an ooo server and it
gains the ability to mirror a configured set of keys with the rest of the cluster
over HTTP.

It is built for the case where **every node must keep accepting writes even when
the cluster leader is unreachable**, and the cluster reconciles when connectivity
returns — using version vectors for causal ordering, and converging on the leader
("last-sync-wins") to resolve true concurrency, without a consensus round-trip on
the write path.

---

## Motivation and target use case

Pivot exists for distributed, physical-site systems where edge stations run a live
workflow and stopping that workflow is worse than temporarily serving stale or
locally-divergent state. A station should keep operating if the site network drops,
if the central server is restarting, or if another station is unhealthy.

The intended shape is narrow:

- A central server carries site-wide state and coordinates synchronization.
- Each station keeps the state it needs to work offline: local configuration,
  station-scoped records, operator/user records, and shared site settings.
- Stations may update their own station-scoped state while disconnected, but they
  do not edit other stations' station-scoped state.
- Shared/global settings converge from the central server once connectivity returns.

That ownership model is why pivot does not try to be a general CRDT system. The
common path is not "many peers concurrently edit the same document and every intent
must merge"; it is "each station owns its local slice, needs a cached copy of shared
state, and must never stop accepting local progress." When the ownership rule is
broken and true concurrency happens, pivot chooses a deterministic winner instead
of preserving both edits for application-level merge.

---

## CAP positioning

> The CAP theorem says you can have only two of: **C**onsistency (serializability),
> **A**vailability (100% for reads and updates), and **P**artition tolerance.
> ([Brewer, "CAP Twelve Years Later"](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/65b514eda12d025585183a641b5a9e096a3c4be5.pdf))

Pivot is **AP**. A node accepts writes locally with no coordination; if the leader
is down or the network is partitioned, the write succeeds anyway and propagates
later. The cost is the C: two nodes can hold different values for the same key for
a window, and a pair of truly-concurrent writes resolves to one winner (the other
is dropped, not merged). If you need linearizable reads or "no acknowledged write
is ever lost," you want a CP system (raft/etcd/rqlite), not pivot.

---

## Model: leader and nodes

A pivot cluster has exactly one **leader** and zero or more **nodes**. Role is
decided entirely by `Config.ClusterURL`:

- `ClusterURL == ""` → this server is the **leader**. Its version-vector identity
  is the constant `"leader"`.
- `ClusterURL == "<leader address>"` → this server is a **node**. Its identity is
  its own address.

Both roles run the same code; they differ only in which direction sync flows and
who fans out to whom.

A cluster syncs a fixed set of **keys** (`Config.Keys`) — single keys (`"settings"`)
or globs (`"users/*"`, `"items/*/*/*"`). Each key can live in `server.Storage` or
in a separate attached storage (e.g. an auth store). Nodes are discovered through
`Config.NodesKey`, a glob whose entries carry node addresses.

---

## How synchronization works (data flow)

This is the part the rest of the README exists to explain. There are three flows.

### 1. A write on a node → the leader (push)

```
node app ─ Set ─▶ node storage ─ watch event ─▶ applyNodePush
                                                     │
                          POST /_pivot/pivot/<key>   │  (carries X-Pivot-VV,
                          ───────────────────────────▶   X-Pivot-Originator)
                                                     leader Set handler:
                                                       1. VV idempotency guard
                                                       2. write to storage
                                                       3. bump + merge VV
                                                       4. fan out to OTHER nodes
```

A local write to a synced key fires ooo's storage watch event. Pivot's callback
(`applyNodePush`) POSTs the object to the leader at `POST /_pivot/pivot/<key>`,
tagged with the node's current **version vector** (`X-Pivot-VV`) and an
**originator** id (`X-Pivot-Originator`).

The leader's `Set` handler ([handlers.go](handlers.go)):

1. **Idempotency guard.** It compares the incoming VV with its own. If the leader
   already dominates (the inbound write is `VVEqual` or `VVGreater`-dominated), it
   returns `200` and writes nothing — a retried or stale delivery can't clobber a
   newer local value. It proceeds on `VVLess`/`VVConcurrent` (the inbound carries
   new information).
2. **Write**, then **bump** its own counter and **merge** the originator's VV into
   its own, so `/activity` reflects the cluster's causal frontier.
3. **Fan out** the change to every *other* node (the originator is skipped via
   `X-Pivot-Originator`, so a write never echoes back to its source).

### 2. A write on the leader → the nodes (trigger fan-out)

```
leader app ─ Set ─▶ leader storage ─▶ bump VV ─▶ for each node in NodesKey:
                                                   GET <node>/_pivot/synchronize/pivot
                                                   ──────────────────────────────────▶ node pulls
                                                                                         from leader,
                                                                                         applies VV-gated
```

When the leader writes, it bumps its `"leader"` counter and then **dials each node**
to nudge a pull (`GET <node>/_pivot/synchronize/pivot`, a pull-only sync). The node
responds by pulling the changed keys from the leader and applying them through the
same VV gate. Unhealthy nodes are skipped (see health below) so one dead node can't
stall the fan-out.

> **This is the one place the leader initiates a connection *to* a node.** It is
> why nodes must currently be reachable (and, for encryption, certificated) — see
> *Pitfalls*.

### 3. Catch-up (sync-on-read, on-start, re-trigger)

A node that missed a fan-out — it was offline, or just joined — converges through
three non-periodic paths, not a polling loop:

- **Sync-on-read.** A read of a synced key on a node fires a `BeforeRead` hook that
  attempts to pull that key from the leader first (`TryPullKey`). If the leader is
  reachable and no sync is already in progress, this reduces stale reads; it is not
  a linearizable-read guarantee.
- **On start.** With `AutoSyncOnStart` enabled, a node does a full bidirectional
  sync with the leader when it boots.
- **Re-trigger.** The leader's fan-out re-nudges nodes on every leader write.

VV idempotency makes any of these a no-op when the node already holds the leader's
version: nothing is written and no event fires.

### Deletes and tombstones

A delete writes a **tombstone** (`pivot/<key>`) *before* removing the item. The
tombstone records "this key was deleted at time T" so a later sync round can't
resurrect the item by re-fetching it from a node that hasn't yet observed the
delete. The worst case is an orphan tombstone, which sync resolves; the reverse
order would risk silent resurrection.

---

## Version vectors: what and why

A naive sync uses the wall-clock `Updated` timestamp to decide who wins. That breaks
the moment a clock is wrong. Consider a node whose clock jumps 6 hours forward,
writes a value, then corrects itself and writes again: the *second*, correct write
now carries a *smaller* timestamp than the first and a timestamp-only sync would
treat it as stale and keep the garbage future value forever.

Pivot avoids this with a **version vector** (VV) per key — a map `{identity → counter}`
([version_vector.go](version_vector.go)):

- Each server **increments its own counter** on every local write to that key.
- VVs **merge** on receive, so each side accumulates what it has seen from everyone.
- Comparing two VVs yields `Equal`, `Less`, `Greater`, or `Concurrent` — a *causal*
  ordering that is independent of any wall clock. `{leader:2}` strictly dominates
  `{leader:1}` no matter what timestamps the writes carried.

The leader exposes the current VV for a key at `GET /_pivot/activity/<key>`.

### Conflict resolution = VV order first, leader-convergence on a tie

- If one VV **dominates** the other, the dominant write wins — full stop. Logical
  causality beats the clock. (This is what saves the clock-drift scenario above.)
- If the two VVs are **concurrent** (each saw something the other didn't — a genuine
  simultaneous edit on both sides), pivot resolves **last-sync-wins**: the **leader's**
  current value wins the reconciliation round. The node's concurrent-only value is
  dropped *that round*, then re-pushed on the node's next sync after a further local
  write advances its VV (`logConflict(..., "last-sync-wins")`, [sync.go](sync.go)).
  The leader is the convergence point.

So pivot is "version-vector-ordered, leader-convergent on true concurrency." It does
**not** merge concurrent writes (that's CRDT territory) — it deterministically picks
the leader's value and drops the other.

Wall-clock timestamps are **not** the primary resolver. `Updated` only decides in two
narrow paths: a legacy fallback when a peer or key has no VV yet (older peer, or the
cold-start window before the first bump), and the per-element tiebreak when pushing a
*glob* list to the leader. Causally-ordered writes never depend on the clock — which
is the whole point of the version vector.

---

## Usage

```go
// Create an ooo server
server := &ooo.Server{}
server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})

// Optionally, a separate storage for some keys (e.g. auth)
authStorage := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})

config := pivot.Config{
    Keys: []pivot.Key{
        {Path: "users/*", Database: authStorage}, // external storage
        {Path: "settings"},                        // nil Database ⇒ server.Storage
        {Path: "items/*/*/*"},
    },
    NodesKey:            "things/*", // node discovery; entries carry "ip" (+ "port")
    ClusterURL:          clusterURL, // "" ⇒ leader; "<leader addr>" ⇒ node
    HealthCheckInterval: 5 * time.Second,
}

// Setup wires routes, the storage-event sync callback, and BeforeRead.
pivot.Setup(server, config)

// External (non-server.Storage) keys must be attached so their writes sync.
pivot.GetInstance(server).Attach(authStorage)

server.Start("localhost:8080")
```

### Config options

- **Keys** — paths to synchronize. Each key uses its own `Database` or `nil` for
  `server.Storage`.
- **NodesKey** — glob whose entries carry node addresses (`"ip"`, optional `"port"`).
  Added to the sync set automatically. Entries with no port (`Port: 0`) are treated
  as data, not nodes.
- **ClusterURL** — `""` for the leader, the leader's address for a node.
- **HealthCheckInterval** — cadence of the *health* probes (the leader probing nodes,
  and a node probing the leader's reachability). It is **not** a data-pull cadence —
  catch-up happens on read / on start / on re-trigger, not on a timer.
- **Client** — optional custom `*http.Client` (defaults to a sync-tuned client:
  short dial timeout for fast dead-node detection, connection pooling).

### Attaching external storage

Keys not backed by `server.Storage` must be attached so their local writes fire the
sync callback:

```go
instance := pivot.GetInstance(server)
instance.Attach(authStorage)

// Attach accepts storage.Options (e.g. an AfterWrite hook for tests/instrumentation)
instance.Attach(authStorage, storage.Options{AfterWrite: myCallback})
```

### Node discovery

```go
// On the leader, register a node by writing its address into NodesKey
ooo.Push(server, "things/*", Thing{IP: "127.0.0.1", Port: 8080})
```

The leader reads `NodesKey`, builds each node's address from `ip`+`port`, and fans
out writes to them.

### HTTP routes

All pivot routes are prefixed with `/_pivot`:

| Route | Method | Purpose |
|-------|--------|---------|
| `/_pivot/synchronize/pivot` | GET | Pull-only sync — the leader hits this on a node to trigger a pull |
| `/_pivot/synchronize/node` | GET | Bidirectional sync |
| `/_pivot/pivot/<key>[/{index}]` | POST | Receive a set (node→leader push, leader→node apply) |
| `/_pivot/pivot/<key>[/{index}]/{time}` | DELETE | Receive a delete (with tombstone time) |
| `/_pivot/pivot/<key>` (single) · `/_pivot/pivot/<base>/{path}` (glob) | GET | Read synced data |
| `/_pivot/activity/<key>` | GET | Version vector / activity for a key |
| `/_pivot/health/nodes` | GET | Node health (leader only; nodes return `[]`) |
| `/_pivot/version` | GET | Protocol version + unauthenticated reachability probe |

### Node health

The leader tracks node health so a dead node doesn't impose timeout penalties on
every fan-out. `GET /_pivot/health/nodes`:

```json
[
  {"address": "192.168.1.10:8080", "healthy": true,  "lastCheck": "2026-01-05T16:43:00+08:00"},
  {"address": "192.168.1.11:8080", "healthy": false, "lastCheck": "2026-01-05T16:42:30+08:00"}
]
```

Unhealthy nodes are skipped during fan-out and re-probed in the background; a node
that comes back is marked healthy again.

---

## Strengths

- **Writes never block on the leader.** Local-first by construction — the AP guarantee.
- **Clock-drift safe.** Version vectors order writes causally; a wrong wall clock
  can't revive stale data (it only ever breaks a genuine-concurrency tie).
- **No lost duplicate-delivery bugs.** The VV idempotency guard makes retries,
  overlapping push+pull, and redundant background pulls no-ops — exactly one apply
  per logical mutation per side.
- **Drop-in for ooo.** No second process, no schema, no separate client database —
  it's `pivot.Setup` on a server you already have.
- **Per-key storage routing.** Different synced keys can live in different stores.

## Pitfalls and caveats

- **The leader dials nodes.** Fan-out and health probes are leader→node connections,
  so nodes must be reachable and (for TLS) certificated. This is the binding
  constraint in locked-down deployments; the design alternative — nodes subscribing
  outbound instead of being dialed — is explored in scratch notes but not yet
  implemented.
- **Concurrency resolves leader-convergent, not merged.** On true concurrency the
  leader's value wins the round and the node's concurrent-only value is dropped (then
  re-pushed after its next local write). If your domain needs *merge* semantics
  (collaborative text, counters, sets), pivot is the wrong tool — use a CRDT.
- **Eventual, not immediate, consistency.** Reads on different nodes can disagree
  during a propagation window or partition.
- **The leader is the tiebreaker.** Concurrency is resolved by the leader winning the
  reconciliation, so a write made against a node during a partition can lose to a
  concurrent leader write once they reconnect. Pivot does not keep a conflict record
  or merge both values; if that data must be preserved, model it as
  owned/non-concurrent state or use a system with explicit conflict handling.
- **Leader is a fan-out hub, not a quorum.** There's no consensus or automatic
  leader election; the leader is a configured role.

---

## Comparison with similar tools

The comparison target is the combined ooo/pivot requirement, not just "sync data":
embedded keyed state, direct HTTP reads/writes, WebSocket subscriptions that browser
clients can consume, offline station writes, and causal ordering that does not trust
wall-clock timestamps.

Yes, this is reinventing part of a wheel. That has a real maintenance cost: pivot
owns its protocol, conflict policy, health checks, retry behavior, route surface, and
edge cases around deletes and clock drift. The question is whether an existing wheel
meets the same constraints with less total system complexity.

| Solution | Offline writes | Ordering / conflicts | Browser-readable realtime surface | Infrastructure and fit |
|----------|----------------|----------------------|-----------------------------------|------------------------|
| **ooo + pivot** | Yes. A node accepts writes while the leader is unreachable. | Version vectors order causally-related writes without trusting clocks. True concurrency is lossy: the leader wins and pivot does not preserve a conflict record. | Yes. ooo exposes REST reads/writes and WebSocket subscriptions to keys/lists, with JSON Patch updates. ([ooo](https://github.com/benitogf/ooo)) | Embedded Go library over ooo storage. Best fit when the data is small/medium operational state, most mutable keys have one station owner, and adding a separate database/cache/sync service would become the hot path. |
| **Cloud Firestore** | Yes on web/mobile SDKs. Cached data can be read, written, listened to, and synced later. | Not causal conflict preservation. Firestore documents same-document offline conflicts as last-write-wins; transactions fail offline. | Yes. Browser SDKs expose realtime listeners with `onSnapshot`. | Strong off-the-shelf replacement if a managed cloud document database, SDK-local cache, Firebase security model, and LWW conflicts are acceptable. ([offline](https://firebase.google.com/docs/firestore/manage-data/enable-offline), [listeners](https://firebase.google.com/docs/firestore/query-data/listen)) |
| **CouchDB / PouchDB** | Yes. PouchDB can write locally and replicate with CouchDB later. | MVCC revision trees preserve conflicts for application resolution; this avoids clock-based overwrite but requires conflict handling. | Partly. PouchDB has browser-local reads and changes/replication feeds, but it is a database replication model rather than ooo-style key WebSocket streams. | Real replacement candidate if a separate document database and explicit conflict resolution are acceptable. ([CouchDB consistency](https://docs.couchdb.org/en/stable/intro/consistency.html)) |
| **PowerSync** | Yes. Local SQLite writes are queued and uploaded later. | Causal+ checkpoints and FIFO upload queues avoid clock-based client ordering; backend code still owns validation and conflict handling. | Partly. Clients read reactive local SQLite and receive sync checkpoints. It is not a generic REST/WS state server; every browser/device must run the SDK/local store. | Serious replacement candidate if the architecture can become central DB + PowerSync service + local SQLite clients + synchronous backend write handling. ([consistency](https://docs.powersync.com/architecture/consistency), [writes](https://docs.powersync.com/handling-writes/writing-client-changes)) |
| **Electric + PGlite / TanStack DB** | Not as a complete off-the-shelf answer today. The PGlite sync plugin says local writes and conflict resolution are not supported yet; TanStack DB can add optimistic write handling above that. | Server-to-client ordering follows Postgres/change-stream order. Offline write ordering/conflicts depend on the write layer you build or adopt. | Partly. Electric is designed for realtime partial replication to clients over HTTP, not an ooo-style REST/WS state server. | Good read-side realtime sync for Postgres-backed apps; incomplete for the hard offline-write requirement unless you add a write queue/conflict layer. ([PGlite sync](https://pglite.dev/docs/sync), [Electric Sync](https://electric-sql.com/sync)) |
| **Replicache** | Yes. Mutations run locally and sync later. | Client mutation IDs/order and server replay avoid clock-based client mutation ordering; server mutators decide the canonical result. | Partly. Browser UIs subscribe to local Replicache state; server "pokes" for realtime pulls require endpoints you implement. | Good library, but not an off-the-shelf backend: you build push, pull, poke, auth, and persistence integration. It replaces the state model with client KV + mutators. ([docs](https://doc.replicache.dev/)) |
| **Zero** | No for the hard case. It queues briefly while reconnecting, but rejects writes once disconnected. | Server-authoritative sync; not aimed at long offline mutation queues. | Yes for online browser sync. | Reject for this use case unless offline writes are no longer required. ([offline writes](https://zero.rocicorp.dev/docs/connection)) |
| **Supabase Realtime / Postgres changes** | No by itself. Postgres is online-first; offline writes require a separate local store and upload queue. | Postgres gives transaction/commit ordering online; offline conflict semantics are yours to build. | Yes. Supabase Realtime exposes Postgres changes over WebSocket. | Useful if SQL is primary and offline writes are out of scope, or when paired with PowerSync/Electric/another local-first layer. Alone, it adds exactly the extra local cache/sync layer pivot avoids. ([protocol](https://supabase.com/docs/guides/realtime/protocol)) |
| **rqlite / dqlite** | No, not in the AP sense. Writes go through a Raft leader/quorum; a partitioned station cannot keep committing independent local writes. | Yes, via Raft log order, stronger than pivot. | No built-in browser object/list subscription surface comparable to ooo; you would add an API and push layer. | Correct choice for strongly consistent SQL; wrong if "local progress must not stop" is non-negotiable. ([rqlite FAQ](https://rqlite.io/docs/faq/), [dqlite replication](https://canonical.com/dqlite/docs/explanation/replication)) |
| **Automerge / Yjs** | Yes. CRDT edits can be made offline and merged later. | Yes. CRDT updates are causally mergeable and preserve concurrent intent. | Not by itself. You need a provider/server/backend such as y-websocket, Hocuspocus, Liveblocks, Automerge Repo, etc. | Best fit when the same logical value is edited concurrently and both intents must survive. Overkill for station-owned settings; not off-the-shelf unless paired with a managed/provider layer. ([Automerge](https://automerge.org/), [Yjs](https://docs.yjs.dev/)) |
| **Litestream / backup replicas** | No. They protect one primary database; they do not make disconnected stations active writers. | WAL/frame order for restore, not multi-writer conflict ordering. | No. | Use for disaster recovery, not active-active station operation. ([how it works](https://litestream.io/how-it-works/)) |

**When pivot is the right call:** you need embedded operational state with direct
REST/WebSocket access, each station must keep writing through a partition, mutable
keys are mostly station-owned, and leader-convergent conflict loss is acceptable.
**When it isn't:** use Firestore/CouchDB/PowerSync if adopting their data model and
infrastructure is acceptable, use Raft-backed SQL if consistency matters more than
offline writes, or use CRDTs if concurrent edits to the same value must merge.
