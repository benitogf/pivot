package pivot

import (
	"sync"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/ooo/ui"
)

// instances stores pivot Instance per server for GetInstance lookup
var instances = make(map[*ooo.Server]*Instance)
var instancesMu sync.RWMutex

// Instance contains pivot callbacks for use with external storages.
// Use GetInstance(server) to retrieve after Setup.
type Instance struct {
	BeforeRead     func(string)        // Callback for sync-on-read
	SyncCallback   StorageSyncCallback // Callback for storage events (write/delete sync)
	PivotIP        string              // Empty for pivot server, pivot address for nodes
	SyncedKeys     []string            // Keys being synchronized
	NodeHealth     *NodeHealth         // Node health tracker (only for pivot servers)
	GetNodes       func() []string     // Function to get registered nodes (only for pivot servers)
	PivotHealthy   bool                // Connection status to pivot (only for node servers)
	PivotLastCheck string              // Last check time for pivot connection (only for node servers)
	syncer         *syncer             // Internal syncer for node servers (for testing hooks)
}

// SetAfterPull sets a callback that fires after each Pull operation completes.
// This is useful for test synchronization to wait for async TriggerNodeSync operations.
func (i *Instance) SetAfterPull(callback func()) {
	if i.syncer != nil {
		i.syncer.AfterPull = callback
	}
}

// GetInstance returns the pivot Instance for a server configured with Setup.
// Returns nil if Setup was not called for this server.
func GetInstance(server *ooo.Server) *Instance {
	instancesMu.RLock()
	defer instancesMu.RUnlock()
	return instances[server]
}

// GetPivotInfo returns a function that provides pivot status for the ooo UI.
// Pass the returned function to ui.Handler.GetPivotInfo to enable pivot status in the UI.
// Returns nil if pivot is not configured for this server.
func GetPivotInfo(server *ooo.Server) func() *ui.PivotInfo {
	return func() *ui.PivotInfo {
		instance := GetInstance(server)
		if instance == nil {
			return nil
		}

		// Determine role
		role := "node"
		if instance.PivotIP == "" {
			role = "pivot"
		}

		// Build node status list - only for pivot servers
		var nodes []ui.PivotNodeStatus

		if role == "pivot" {
			// First, get nodes from GetNodes function (reads from storage)
			if instance.GetNodes != nil {
				registeredNodes := instance.GetNodes()
				healthStatus := make(map[string]NodeStatus)
				if instance.NodeHealth != nil {
					for _, status := range instance.NodeHealth.GetStatus() {
						healthStatus[status.Address] = status
					}
				}
				for _, addr := range registeredNodes {
					status := ui.PivotNodeStatus{
						Address:   addr,
						Healthy:   false, // Unknown until checked
						LastCheck: "Never",
					}
					if hs, ok := healthStatus[addr]; ok {
						status.Healthy = hs.Healthy
						status.LastCheck = hs.LastCheck
					}
					nodes = append(nodes, status)
				}
			}

			// Also include nodes from health tracker that might not be in storage yet
			if instance.NodeHealth != nil {
				healthStatuses := instance.NodeHealth.GetStatus()
				existingAddrs := make(map[string]bool)
				for _, n := range nodes {
					existingAddrs[n.Address] = true
				}
				for _, status := range healthStatuses {
					if !existingAddrs[status.Address] {
						nodes = append(nodes, ui.PivotNodeStatus{
							Address:   status.Address,
							Healthy:   status.Healthy,
							LastCheck: status.LastCheck,
						})
					}
				}
			}
		}

		if nodes == nil {
			nodes = []ui.PivotNodeStatus{}
		}

		return &ui.PivotInfo{
			Role:           role,
			PivotIP:        instance.PivotIP,
			SyncedKeys:     instance.SyncedKeys,
			Nodes:          nodes,
			PivotHealthy:   instance.PivotHealthy,
			PivotLastCheck: instance.PivotLastCheck,
		}
	}
}

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
