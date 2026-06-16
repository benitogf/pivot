package pivot

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/ooo/ui"
)

// instances stores pivot Instance per server for GetInstance lookup
var instances = make(map[*ooo.Server]*Instance)
var instancesMu sync.RWMutex

// PivotHealthStatus tracks health status for a single pivot connection
type PivotHealthStatus struct {
	Healthy    bool
	LastCheck  string
	Protocol   string // Protocol version of the pivot ("2.0", "unknown")
	Compatible bool   // true if pivot protocol matches local version
}

// Instance contains pivot callbacks for use with external storages.
// Use GetInstance(server) to retrieve after Setup.
type Instance struct {
	BeforeRead     func(string)                  // Callback for sync-on-read
	SyncCallback   StorageSyncCallback           // Callback for storage events (write/delete sync)
	ClusterURL     string                        // Config.ClusterURL - empty for pure pivot server
	SyncedKeys     []string                      // Keys being synchronized
	NodeHealth     *NodeHealth                   // Node health tracker (only for pivot servers)
	GetNodes       func() []string               // Function to get registered nodes (only for pivot servers)
	PivotHealth    map[string]*PivotHealthStatus // Health status per pivot URL (for node servers)
	ExtraNodeURLs  []string                      // Additional node URLs (can be modified after Setup)
	VVManager      *VVManager                    // Version vector manager (for both pivot and node servers)
	configKeys     []Key                         // Configured keys from Setup; needed by the synchronous AfterWrite VV bump to find which base path scope to increment
	handlerTracker *HandlerWriteTracker          // Same tracker SyncCallback consumes from; AfterWrite peeks it (non-consuming) to skip its bump when a handler will bump explicitly
	attachedDBs    sync.Map                      // set of storage.Database -> struct{}; AfterWrite-driven sync bump is wired for these, so makeStorageSync's async bump must skip them to avoid double-counting
	syncerPool     *syncerPool                   // Internal syncer pool for node servers (for testing hooks)
	nodesCache     *nodesCache                   // Cache for NodesKey address list, invalidated by storage events
	triggers       *triggerCoalescer             // Per-node trigger coalescer (only set on pivot servers)
	healthMu       sync.RWMutex                  // Protects PivotHealth map
	extraNodeURLMu sync.RWMutex                  // Protects ExtraNodeURLs
	shutdown       int32                         // Atomic flag to prevent access during shutdown
	ctx            context.Context               // Instance lifetime; cancelled by Shutdown to unblock in-flight leader HTTP calls
	cancel         context.CancelFunc            // Cancels ctx; set in Setup, invoked by Shutdown
}

// AddExtraNodeURL adds a node URL to receive sync notifications (for cluster leader servers).
func (i *Instance) AddExtraNodeURL(url string) {
	i.extraNodeURLMu.Lock()
	i.ExtraNodeURLs = append(i.ExtraNodeURLs, url)
	i.extraNodeURLMu.Unlock()
}

// GetExtraNodeURLs returns a copy of the extra node URLs.
func (i *Instance) GetExtraNodeURLs() []string {
	i.extraNodeURLMu.RLock()
	defer i.extraNodeURLMu.RUnlock()
	result := make([]string, len(i.ExtraNodeURLs))
	copy(result, i.ExtraNodeURLs)
	return result
}

// Shutdown marks the instance as shutting down to prevent access during close,
// and cancels the instance context so any in-flight leader HTTP calls return
// promptly instead of waiting out the client timeout.
func (i *Instance) Shutdown() {
	atomic.StoreInt32(&i.shutdown, 1)
	if i.cancel != nil {
		i.cancel()
	}
}

// IsShutdown returns true if the instance is shutting down.
func (i *Instance) IsShutdown() bool {
	return atomic.LoadInt32(&i.shutdown) != 0
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

		// Determine role - mixed if server has both pivot and node keys
		hasPivotKeys := false
		hasNodeKeys := false
		if instance.syncerPool != nil && len(instance.syncerPool.syncers) > 0 {
			hasNodeKeys = true
		}
		// Check if any keys are in pivot mode (Local=true or no ClusterURL)
		for _, keyPath := range instance.SyncedKeys {
			if instance.syncerPool == nil || instance.syncerPool.keyMap[keyPath] == "" {
				hasPivotKeys = true
				break
			}
		}
		var role string
		if hasPivotKeys && hasNodeKeys {
			role = "mixed"
		} else if hasPivotKeys {
			role = "pivot"
		} else {
			role = "node"
		}

		// Build node status list - only for pivot servers
		var nodes []ui.PivotNodeStatus

		if role == "pivot" || role == "mixed" {
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
						Protocol:  "unknown", // Default until version check
					}
					if hs, ok := healthStatus[addr]; ok {
						status.Healthy = hs.Healthy
						status.LastCheck = hs.LastCheck
						status.Protocol = hs.Protocol
						status.Compatible = hs.Compatible
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
							Address:    status.Address,
							Healthy:    status.Healthy,
							LastCheck:  status.LastCheck,
							Protocol:   status.Protocol,
							Compatible: status.Compatible,
						})
					}
				}
			}
		}

		if nodes == nil {
			nodes = []ui.PivotNodeStatus{}
		}

		// Get overall pivot health (all pivots healthy = healthy)
		pivotHealthy := true
		pivotLastCheck := ""
		pivotProtocol := ""
		pivotCompatible := true
		instance.healthMu.RLock()
		for _, status := range instance.PivotHealth {
			if !status.Healthy {
				pivotHealthy = false
			}
			if !status.Compatible {
				pivotCompatible = false
			}
			if status.LastCheck > pivotLastCheck {
				pivotLastCheck = status.LastCheck
			}
			// Use the protocol from the first pivot (typically only one for node servers)
			if pivotProtocol == "" && status.Protocol != "" {
				pivotProtocol = status.Protocol
			}
		}
		instance.healthMu.RUnlock()

		if pivotProtocol == "" {
			pivotProtocol = "unknown"
		}

		return &ui.PivotInfo{
			Role:            role,
			PivotIP:         instance.ClusterURL,
			SyncedKeys:      instance.SyncedKeys,
			Nodes:           nodes,
			PivotHealthy:    pivotHealthy,
			PivotLastCheck:  pivotLastCheck,
			PivotProtocol:   pivotProtocol,
			PivotCompatible: pivotCompatible,
		}
	}
}

// IsAttached reports whether the given storage was registered with
// Attach. makeStorageSync uses it to decide whether the async-callback
// path should perform a VV bump: for attached storages, AfterWrite has
// already bumped synchronously and the async bump would double-count.
func (i *Instance) IsAttached(db storage.Database) bool {
	if db == nil {
		return false
	}
	_, ok := i.attachedDBs.Load(db)
	return ok
}

// bumpVVForLocalWrite is the synchronous VV bump invoked from ooo's
// AfterWriteOp. It runs inside storage.SetWithMeta / Set / Del, before
// the call returns, so by the time the caller continues the local VV
// reflects this write — no async-callback lag, no window where a pull
// tick can fire and clobber the not-yet-counted write.
//
// op ("set" or "del") comes from ooo's AfterWriteOp hook and selects which
// pull bump-skip mark to consume. This MUST be operation-specific: the pull
// path sets bumpSkipSet for pulled sets and bumpSkipDelete for pulled deletes,
// each meant to suppress the bump of that one pulled write. A pulled delete's
// mark is set just before its storage write but outside the per-key write
// lock, so a concurrent LOCAL set on the same key can run its AfterWriteOp in
// the window between. If that local set consumed the delete's mark (the old
// op-unaware `consumeBumpSkipSet() || consumeBumpSkipDelete()`), the local
// set's bump was skipped, its VV never advanced, and the peer rejected the
// pushed write as VVEqual — a permanent node↔pivot divergence. Consuming only
// the mark matching this write's own operation closes that cross-op steal.
//
// Four cases skip the bump:
//
//   - eventKey is a pivot-internal key (delete tombstones, VV storage,
//     health, etc.) — these are pivot's own bookkeeping, not application
//     writes.
//
//   - no configured Key matches the eventKey — the write went to some
//     storage path pivot isn't synchronising; bumping a VV scope for it
//     would produce a never-read entry.
//
//   - the write is pull-driven (the syncer's pullTracker has a peek-hit
//     for the key) — the pulled record carries the leader's VV, which
//     is merged into local via vvManager.set later; bumping our own
//     counter on top would double-count.
//
//   - a handler has marked the key (HandlerWriteTracker.Has) — handlers
//     bump explicitly after SetWithMeta returns, so AfterWrite would
//     duplicate. The watch goroutine still Consumes the mark to gate
//     the rest of SyncCallback; we only peek here.
//
// Direct user writes (db.Set / db.SetWithMeta on an attached storage)
// hit none of these cases and bump here, synchronously.
func (i *Instance) bumpVVForLocalWrite(eventKey string, op string) {
	if i.VVManager == nil {
		return
	}
	if strings.HasPrefix(eventKey, StoragePrefix) {
		return
	}
	if i.handlerTracker != nil && i.handlerTracker.ConsumeBumpSkip(eventKey) {
		return
	}
	var matched Key
	found := false
	for _, k := range i.configKeys {
		if key.Match(k.Path, eventKey) {
			matched = k
			found = true
			break
		}
	}
	if !found {
		return
	}
	// Pull-driven writes carry the leader's VV; merging happens via
	// vvManager.set elsewhere. Skip the counter bump so our own counter
	// only advances for writes we originate. Consuming consume (not a
	// peek) — see the pullTracker doc on why each consumer drains its
	// own mark.
	if i.syncerPool != nil {
		effectiveURL := matched.EffectiveClusterURL(i.ClusterURL)
		if effectiveURL != "" {
			if s := i.syncerPool.syncers[effectiveURL]; s != nil {
				// Consume ONLY the mark for this write's own operation — never
				// cross-op — so a pulled delete's mark can't suppress a local
				// set's bump (and vice versa).
				var pullDriven bool
				if op == "del" {
					pullDriven = s.tracker.consumeBumpSkipDelete(eventKey)
				} else {
					pullDriven = s.tracker.consumeBumpSkipSet(eventKey)
				}
				if pullDriven {
					return
				}
			}
		}
	}
	i.VVManager.increment(baseKeyFromPath(matched.Path))
}

// Attach configures an external storage for pivot synchronization.
// It starts the storage with BeforeRead callback and sets up event watching.
// This is a convenience method that replaces the manual setup:
//
//	db.Start(storage.Options{BeforeRead: instance.BeforeRead})
//	storage.WatchWithCallback(instance.ctx, db, instance.SyncCallback)
//
// Optional storageOpts can be provided to pass additional storage options (e.g., AfterWrite for testing).
//
// Attach wraps the caller's AfterWrite (if any) with a synchronous VV
// bump (see bumpVVForLocalWrite). The bump runs first, then the
// caller's AfterWrite, so tests waiting on AfterWrite still observe the
// post-bump VV. A caller-supplied BeforeRead is likewise composed with
// pivot's sync-on-read callback (pivot first, caller second) rather than
// dropped.
func (i *Instance) Attach(db storage.Database, storageOpts ...storage.Options) error {
	var opts storage.Options
	var userAfterWrite func(string)
	var userBeforeRead func(string)
	if len(storageOpts) > 0 {
		userOpts := storageOpts[0]
		opts.NoBroadcastKeys = userOpts.NoBroadcastKeys
		userAfterWrite = userOpts.AfterWrite
		userBeforeRead = userOpts.BeforeRead
		opts.Workers = userOpts.Workers
	}
	// Compose pivot's sync-on-read BeforeRead with the caller's (if any),
	// rather than overwriting it. Pivot runs first so its pull lands
	// before the caller's hook observes the key — same ordering as the
	// AfterWrite wrapping below (pivot's bump first, caller's hook second).
	// Attach's documented use case is external storages that want to read,
	// so silently dropping a caller-supplied BeforeRead would defeat it.
	beforeRead := func(eventKey string) {
		if i.BeforeRead != nil {
			i.BeforeRead(eventKey)
		}
		if userBeforeRead != nil {
			userBeforeRead(eventKey)
		}
	}
	opts.BeforeRead = beforeRead
	// VV bump via the op-aware hook so it consumes only the bump-skip mark
	// matching the write's operation (see bumpVVForLocalWrite). ooo fires
	// AfterWriteOp before AfterWrite, so the bump still lands before a
	// caller-supplied AfterWrite observes the post-bump VV.
	opts.AfterWriteOp = func(eventKey string, op string) {
		i.bumpVVForLocalWrite(eventKey, op)
	}
	opts.AfterWrite = userAfterWrite

	// Record this storage as having sync-bump wired so the async
	// SyncCallback path can skip its own bump for events from this DB.
	i.attachedDBs.Store(db, struct{}{})

	if db.Active() {
		// Storage already started - use SetBeforeRead to update callback safely
		// This works for both memory-only and embedded storage.
		// NOTE: there is no SetAfterWrite on storage.Database; if the
		// storage is already started we cannot install our wrapped
		// AfterWrite. Callers that rely on the sync-bump fix must call
		// Attach BEFORE starting the storage. For tests that bypass
		// this (already-started memory storage), the async bump in
		// makeStorageSync still applies because attachedDBs is keyed
		// on whether AfterWrite was installable — we optimistically
		// Store before checking Active(), then Delete immediately in
		// this already-started branch because wrapped AfterWrite was
		// not installable.
		db.SetBeforeRead(beforeRead)
		// Roll back the attachedDBs record because AfterWrite was NOT
		// actually installed (storage was already started).
		i.attachedDBs.Delete(db)
	} else {
		// Storage not started - start it with BeforeRead configured
		err := db.Start(opts)
		if err != nil {
			// Start failed; AfterWrite never wired up either.
			i.attachedDBs.Delete(db)
			return err
		}
	}
	storage.WatchWithCallback(i.ctx, db, i.SyncCallback)
	return nil
}

// storeInstance stores the pivot instance for GetInstance lookup
func storeInstance(server *ooo.Server, instance *Instance) {
	instancesMu.Lock()
	instances[server] = instance
	instancesMu.Unlock()
}

// removeInstance drops the entry from the registry. Called on shutdown so a
// process that recreates servers (tests, supervised in-process restarts)
// doesn't leak one map entry per server for the rest of the process's life.
func removeInstance(server *ooo.Server) {
	instancesMu.Lock()
	delete(instances, server)
	instancesMu.Unlock()
}
