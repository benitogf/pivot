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
  pulls that key from the leader first (`TryPullKey`), so a read never returns
  stale data the leader already moved past.
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
  concurrent leader write once they reconnect. (It is not lost silently — the node
  re-pushes after its next write — but it does not win that round.)
- **Leader is a fan-out hub, not a quorum.** There's no consensus or automatic
  leader election; the leader is a configured role.

---

## Comparison with similar tools

Pivot occupies a narrow niche: **AP, leader/follower, version-vector-ordered,
leader-convergent synchronization of ooo document/KV state, as an embedded Go library
over HTTP.** Most adjacent tools sit somewhere else on the consistency / merge /
transport axes.

| Tool | Consistency | Conflict handling | Shape | vs pivot |
|------|-------------|-------------------|-------|----------|
| **pivot** | AP (eventual) | VV-ordered; leader wins true concurrency (last-sync-wins) | Embedded Go lib, HTTP, leader/follower | — |
| **rqlite / dqlite** | CP (Raft) | Linearizable; no conflicts by design | Distributed SQLite (server / C lib) | Choose these for correctness-critical SQL with failover; they trade availability under partition for consistency, which pivot deliberately keeps. ([rqlite FAQ](https://rqlite.io/docs/faq/), [dqlite](https://github.com/canonical/dqlite)) |
| **Litestream** | Single-writer + async backup | n/a (no multi-writer) | SQLite WAL → object storage | Disaster-recovery/replica, not multi-node live sync; a lost primary restores from backup rather than failing over. ([Litestream alternatives](https://litestream.io/alternatives/)) |
| **CouchDB / PouchDB** | AP (multi-master, eventual) | MVCC revision trees; **app resolves** conflicts | Standalone DB + JS client, HTTP replication | Closest in spirit (HTTP, AP, multi-master). pivot resolves conflicts *automatically* (VV order, leader wins concurrency) instead of surfacing revision conflicts for the app to merge, and embeds in ooo rather than being a separate database. ([CouchDB consistency](https://docs.couchdb.org/en/stable/intro/consistency.html)) |
| **ElectricSQL / PowerSync** | Local-first sync engines | LWW by default (Electric), custom server-side (PowerSync) | Postgres ⇆ client SQLite | Full-stack local-first frameworks with a separate client DB and a Postgres backend; pivot is ooo-to-ooo, server-side, with no separate client store. ([Electric vs PowerSync](https://powersync.com/blog/electricsql-vs-powersync)) |
| **Automerge / Yjs (CRDTs)** | Strong eventual (SEC) | Mathematically auto-merging | Embedded CRDT libraries | CRDTs *merge* concurrent edits (no data loss) at the cost of complexity, data-type constraints, and metadata overhead; pivot picks a winner instead of merging — simpler, but lossy on true concurrency. ([CRDT field guide](https://www.iankduncan.com/engineering/2025-11-27-crdt-dictionary/)) |
| **etcd / ZooKeeper** | CP (consensus) | Linearizable | Coordination service | Built for config/locks/leader-election with strong consistency, not high-write-availability data sync. |

**When pivot is the right call:** you already run ooo, you want each node to keep
writing through a partition, and leader-convergent resolution (causal VV ordering so
clock skew can't corrupt state; the leader wins genuine concurrency) is acceptable
conflict handling. **When it isn't:** you need linearizable reads (→ rqlite/etcd), or
you need concurrent edits *merged* rather than one-wins (→ CRDTs).

> On the LWW-vs-CRDT question, the local-first community's rough consensus is that
> last-write-wins is sufficient for the large majority of shared-state apps, with
> CRDTs reserved for genuinely collaborative merge cases. ([ElectricSQL vs PowerSync vs Zero](https://trybuildpilot.com/648-electric-sql-vs-powersync-vs-zero-2026), [conflict-resolution tradeoffs](https://medium.com/@priyasrivastava18official/system-design-pattern-from-chaos-to-consistency-the-art-of-conflict-resolution-in-distributed-9d631028bdb4))
