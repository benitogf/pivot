# Pivot

[![Test](https://github.com/benitogf/pivot/actions/workflows/tests.yml/badge.svg)](https://github.com/benitogf/pivot/actions/workflows/tests.yml)

A way to synchronize multiple instances of a ooo server.

https://storage.googleapis.com/pub-tools-public-publication-data/pdf/65b514eda12d025585183a641b5a9e096a3c4be5.pdf

> The CAP theorem [Bre12] says that you can only have two of the three desirable properties of:
> - C: Consistency, which we can think of as serializability for this discussion;
> - A: 100% availability, for both reads and updates;
> - P: tolerance to network partitions.
>
> This leads to three kinds of systems: CA, CP and AP, based on what letter you leave out. Note that you are not entitled to 2 of 3, and many systems have zero or one of the properties.

This distribution system prioritizes availability and partition tolerance (AP). 
Nodes accept writes even when the cluster leader is unreachable, with data synchronizing 
when connectivity is restored using last-write-wins conflict resolution.

## Usage

```go
// Create server
server := &ooo.Server{}
server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})

// Create external storage (e.g., for auth)
authStorage := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})

// Configure cluster synchronization
config := pivot.Config{
    Keys: []pivot.Key{
        {Path: "users/*", Database: authStorage}, // External storage
        {Path: "settings"},                        // nil Database = server.Storage
    },
    NodesKey: "things/*", // Node discovery path - entries must have "ip" field
    ClusterURL: clusterURL, // Empty string = cluster leader, non-empty = cluster leader's address (node mode)
}

// Setup - modifies server (routes, OnStorageEvent, BeforeRead)
pivot.Setup(server, config)

// Attach external storage for sync (handles Start + BeforeRead + WatchWithCallback)
pivot.GetInstance(server).Attach(authStorage)

// Start server
server.Start("localhost:8080")
```

### Config Options

- **Keys**: List of paths to synchronize. Each key can specify a custom `Database` or use `nil` to default to `server.Storage`.
- **NodesKey**: Path where node entries are stored. Entries must have an `"ip"` field containing the node's address. This key is automatically added to the sync list.
- **ClusterURL**: Empty string for the cluster leader server, or the cluster leader's address for node servers.

### External Storage (Attach)

For external storages (not `server.Storage`), use `GetInstance` and `Attach`:

```go
// GetInstance retrieves the cluster Instance after Setup
instance := pivot.GetInstance(server)

// Attach handles: Start with BeforeRead + WatchWithCallback for sync
instance.Attach(authStorage)

// With additional storage options (e.g., for testing)
instance.Attach(authStorage, storage.Options{AfterWrite: myCallback})
```

The `Instance` type exposes `BeforeRead` and `SyncCallback` for manual setup if needed.

### Node Discovery

Nodes register themselves by creating entries at `NodesKey` with their IP and port:

```go
// On cluster leader, create a "thing" that represents a node
// The IP and Port fields are used to construct the node address
ooo.Push(server, "things/*", Thing{IP: "127.0.0.1", Port: 8080})
```

When the cluster leader writes data, it reads all entries from `NodesKey`, extracts the `"ip"` and `"port"` fields, constructs the address (e.g., `127.0.0.1:8080`), and triggers sync on each node. Entries without a port (Port: 0) are treated as data, not node servers.

### Route Structure

All pivot routes are prefixed with `/_pivot`:

| Route | Method | Purpose |
|-------|--------|---------|
| `/_pivot/pivot` | GET | Trigger sync on node |
| `/_pivot/health/nodes` | GET | Node health status |
| `/_pivot/activity/{key}` | GET | Activity info for a key |
| `/_pivot/pivot/{key}` | GET | Get synced data |
| `/_pivot/pivot/{key}/{index}` | POST | Set synced data |
| `/_pivot/pivot/{key}/{index}/{time}` | DELETE | Delete synced data |

### Node Health Monitoring

Cluster leaders automatically track node health to avoid timeout penalties when syncing to unreachable nodes. The health status is available via a GET endpoint:

```
GET /_pivot/health/nodes
```

**Response:**
```json
[
  {"address": "192.168.1.10:8080", "healthy": true, "lastCheck": "2026-01-05T16:43:00+08:00"},
  {"address": "192.168.1.11:8080", "healthy": false, "lastCheck": "2026-01-05T16:42:30+08:00"}
]
```

- **address**: Node's IP address
- **healthy**: `true` if last sync succeeded, `false` if it failed
- **lastCheck**: RFC3339 timestamp of last health check

Unhealthy nodes are automatically skipped during sync and re-checked every 30 seconds in the background. When a node comes back online, it will be marked healthy again.

For node servers, this endpoint returns an empty array `[]`.

### HTTP Client Configuration

By default, pivot uses an HTTP client optimized for synchronization:
- **500ms dial timeout**: Quick detection of unreachable nodes
- **30s overall timeout**: Handles large data transfers
- **Connection pooling**: Efficient reuse of connections

You can provide a custom client via `Config.Client`:

```go
config := pivot.Config{
    Keys:    []pivot.Key{{Path: "settings"}},
    ClusterURL: clusterURL,
    Client:  &http.Client{Timeout: 10 * time.Second}, // Custom client
}
```

