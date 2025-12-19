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
Nodes accept writes even when the pivot is unreachable, with data synchronizing 
when connectivity is restored using last-write-wins conflict resolution.

## Usage

```go
// Create server
server := &ooo.Server{}
server.Pivot = pivotAddress // Empty string for pivot server, pivot's address for nodes

// Configure pivot synchronization
config := pivot.Config{
    Keys: []pivot.Key{
        {Path: "users/*", Database: authStorage}, // External storage
        {Path: "settings"},                        // nil Database = server.Storage
    },
    NodesKey: "things/*", // Node discovery path - entries must have "ip" field
}

// Setup pivot - configures routes, sets server.DbOpt, returns callbacks
result := pivot.Setup(server, config)

server.BeforeRead = result.BeforeRead
// Start external storages with BeforeRead callback for sync-on-read
authStorage.Start(ooo.StorageOpt{BeforeRead: result.BeforeRead})

// Watch external storage for write/delete sync
go func() {
    for event := range authStorage.Watch() {
        result.SyncCallback(event)
    }
}()

// Start server
server.Start("localhost:8080")
```

### Config Options

- **Keys**: List of paths to synchronize. Each key can specify a custom `Database` or use `nil` to default to `server.Storage`.
- **NodesKey**: Path where node entries are stored. Entries must have an `"ip"` field containing the node's address. This key is automatically added to the sync list.

### Node Discovery

Nodes register themselves by creating entries at `NodesKey` with their IP address:

```go
// On pivot server, create a "thing" that represents a node
oio.Push(server, "things/*", Thing{IP: nodeServer.Address})
```

When the pivot server writes data, it reads all entries from `NodesKey`, extracts the `"ip"` field, and triggers sync on each node.

