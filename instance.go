package pivot

import (
	"sync"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/storage"
)

// instances stores pivot Instance per server for GetInstance lookup
var instances = make(map[*ooo.Server]*Instance)
var instancesMu sync.RWMutex

// Instance contains pivot callbacks for use with external storages.
// Use GetInstance(server) to retrieve after Setup.
type Instance struct {
	BeforeRead   func(string)        // Callback for sync-on-read
	SyncCallback StorageSyncCallback // Callback for storage events (write/delete sync)
	PivotIP      string              // Empty for pivot server, pivot address for nodes
	SyncedKeys   []string            // Keys being synchronized
	NodeHealth   *NodeHealth         // Node health tracker (only for pivot servers)
}

// GetInstance returns the pivot Instance for a server configured with Setup.
// Returns nil if Setup was not called for this server.
func GetInstance(server *ooo.Server) *Instance {
	instancesMu.RLock()
	defer instancesMu.RUnlock()
	return instances[server]
}

// // GetInstanceInfo returns a function that provides pivot status for the ooo UI.
// // Pass the returned function to ui.Handler.GetPivotInfo to enable pivot status in the UI.
// // Returns nil if pivot is not configured for this server.
// func GetInstanceInfo(server *ooo.Server) func() interface{} {
// 	return func() interface{} {
// 		instance := GetInstance(server)
// 		if instance == nil {
// 			return nil
// 		}

// 		// Determine role
// 		role := "node"
// 		if instance.PivotIP == "" {
// 			role = "pivot"
// 		}

// 		// Build response matching ui.PivotInfo structure
// 		result := map[string]interface{}{
// 			"role":       role,
// 			"pivotIP":    instance.PivotIP,
// 			"syncedKeys": instance.SyncedKeys,
// 		}

// 		// Add node health for pivot servers
// 		if instance.NodeHealth != nil {
// 			result["nodes"] = instance.NodeHealth.GetStatus()
// 		} else {
// 			result["nodes"] = []NodeStatus{}
// 		}

// 		return result
// 	}
// }

// Attach configures an external storage for pivot synchronization.
// It starts the storage with BeforeRead callback and sets up event watching.
// This is a convenience method that replaces the manual setup:
//
//	db.Start(storage.Options{BeforeRead: instance.BeforeRead})
//	storage.WatchWithCallback(db, instance.SyncCallback)
//
// Optional storageOpts can be provided to pass additional storage options (e.g., AfterWrite for testing).
func (i *Instance) Attach(db storage.Database, storageOpts ...storage.Options) error {
	opts := storage.Options{BeforeRead: i.BeforeRead}
	if len(storageOpts) > 0 {
		// Merge user options, preserving BeforeRead
		userOpts := storageOpts[0]
		opts.NoBroadcastKeys = userOpts.NoBroadcastKeys
		opts.AfterWrite = userOpts.AfterWrite
		opts.Workers = userOpts.Workers
		// Always use our BeforeRead for sync-on-read
	}
	err := db.Start(opts)
	if err != nil {
		return err
	}
	storage.WatchWithCallback(db, i.SyncCallback)
	return nil
}

// storeInstance stores the pivot instance for GetInstance lookup
func storeInstance(server *ooo.Server, instance *Instance) {
	instancesMu.Lock()
	instances[server] = instance
	instancesMu.Unlock()
}
