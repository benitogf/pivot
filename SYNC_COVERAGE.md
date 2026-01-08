# Pivot Sync Operations Test Coverage

## All Possible Sync Operations

| Operation | Direction | Key Type | Description |
|-----------|-----------|----------|-------------|
| **Push** | Node → Pivot | Glob (`things/*`) | Create new item on node, sync to pivot |
| **Push** | Pivot → Node | Glob (`things/*`) | Create new item on pivot, sync to node |
| **Set** | Node → Pivot | Glob (`things/id`) | Update existing item on node, sync to pivot |
| **Set** | Pivot → Node | Glob (`things/id`) | Update existing item on pivot, sync to node |
| **Delete** | Node → Pivot | Glob (`things/id`) | Delete item on node, sync to pivot |
| **Delete** | Pivot → Node | Glob (`things/id`) | Delete item on pivot, sync to node |
| **Set** | Node → Pivot | Single (`settings`) | Set single key on node, sync to pivot |
| **Set** | Pivot → Node | Single (`settings`) | Set single key on pivot, sync to node |
| **Delete** | Node → Pivot | Single (`settings`) | Delete single key on node, sync to pivot |
| **Delete** | Pivot → Node | Single (`settings`) | Delete single key on pivot, sync to node |
| **Sync-on-Read** | Pivot → Node | Both | Node reads trigger pull from pivot (ensures node has latest data) |
| **Pivot IP Change** | - | Both | Data wipe when node changes pivot |

## Current Test Coverage

| Test File | Test Name | Operations Covered |
|-----------|-----------|-------------------|
| `pivot_test.go` | `testBasicPivotSync` | Push pivot→node, Set pivot→node, Set node→pivot, Delete pivot→node, Push node→pivot, Delete node→pivot, Set settings node→pivot, Set settings pivot→node, Delete settings node→pivot, Delete settings pivot→node |
| `edge_test.go` | `TestNodeToPivotSync` | Push node→pivot (glob) |
| `edge_test.go` | `TestSingleKeySync` | Set node→pivot (single) |
| `edge_test.go` | `TestEmptyStorageSync` | Empty storage read |
| `edge_test.go` | `TestSyncOnRead` | Sync-on-read: node reads pull latest from pivot |
| `crab_test.go` | `TestHermitCrab` | Pivot IP change wipe |

## Coverage Status

### Fully Covered
- All CRUD operations for glob keys in both directions
- All CRUD operations for single keys in both directions
- Websocket event propagation
- Pivot IP change data wipe
- Sync-on-read (node pulls from pivot on read)

### Not Covered (Edge Cases)
- **Concurrent multi-node writes** - Multiple nodes writing to pivot simultaneously
- **Conflict resolution** - Node and pivot modify same key simultaneously
- **External storage sync** - The `users/*` key uses `authStorage` but isn't tested for sync
- **Node offline/reconnect** - Node goes offline, pivot changes data, node comes back
- **Large batch sync** - Many items synced at once

## Sync Flow Summary

### Node → Pivot (Write)
1. Node writes to local storage
2. `OnStorageEvent` callback fires
3. `syncer.QueueOrSendSet/Delete` sends to pivot via HTTP
4. Pivot stores data and triggers `OnStorageEvent`
5. Pivot notifies other nodes via `TriggerNodeSync`

### Pivot → Node (Write)
1. Pivot writes to local storage
2. `OnStorageEvent` callback fires
3. Pivot calls `TriggerNodeSync` for all healthy nodes
4. Node receives sync trigger, calls `syncer.Pull()`
5. Node fetches data from pivot and stores locally

### Sync-on-Read (Node only)
1. Node receives read request
2. `BeforeRead` callback fires
3. Node calls `syncer.TryPull()` to fetch latest from pivot
4. Node returns data to client

Note: Pivot servers do not trigger sync on reads - they only notify nodes on writes.
