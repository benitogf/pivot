package pivot

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/storage"
)

// get from pivot and write to local
func syncLocalEntries(client *http.Client, pivot string, _key Key, lastEntry int64) error {
	return syncLocalEntriesWithTracking(client, pivot, _key, lastEntry, nil, nil)
}

// syncLocalEntriesWithTracking syncs from pivot and tracks synced keys via callbacks.
// onDelete is called for each key deleted locally (item exists locally but not on pivot)
// onSet is called for each key set locally (item exists on pivot but not locally, or pivot is newer)
func syncLocalEntriesWithTracking(client *http.Client, pivot string, _key Key, lastEntry int64, onDelete func(key string), onSet func(key string)) error {
	if key.LastIndex(_key.Path) == "*" {
		baseKey := strings.Replace(_key.Path, "/*", "", 1)
		objsPivot, err := getEntriesFromPivot(client, pivot, _key.Path)
		if err != nil {
			return err
		}

		objsLocal, err := _key.Database.GetList(_key.Path)
		if err != nil {
			objsLocal = []meta.Object{}
		}

		objsToDelete := GetEntriesNegativeDiff(objsLocal, objsPivot)
		for _, index := range objsToDelete {
			fullKey := baseKey + "/" + index
			_key.Database.Del(fullKey)
			if onDelete != nil {
				onDelete(fullKey)
			}
		}

		objsToSend := GetEntriesPositiveDiff(objsLocal, objsPivot)
		for _, obj := range objsToSend {
			fullKey := baseKey + "/" + obj.Index
			_key.Database.SetWithMeta(fullKey, obj.Data, obj.Created, obj.Updated)
			if onSet != nil {
				onSet(fullKey)
			}
		}
		_key.Database.Set(StoragePrefix+baseKey, json.RawMessage(strconv.FormatInt(lastEntry, 10)))
		return nil
	}

	obj, err := getEntryFromPivot(client, pivot, _key.Path)
	if err != nil {
		return err
	}
	_key.Database.SetWithMeta(_key.Path, obj.Data, obj.Created, obj.Updated)
	if onSet != nil {
		onSet(_key.Path)
	}
	_key.Database.Set(StoragePrefix+_key.Path, json.RawMessage(strconv.FormatInt(lastEntry, 10)))

	return nil
}

// syncPivotEntriesWithDeleteCheck syncs local entries to pivot with an optional delete check.
// If isRecentDelete is provided, items that were recently deleted by a pull-only sync
// will not be re-added to pivot.
// This function also sends delete commands to pivot for items that were deleted locally.
func syncPivotEntriesWithDeleteCheck(client *http.Client, pivot string, _key Key, pivotActivity int64, isRecentDelete func(key string) bool) error {
	if key.LastIndex(_key.Path) == "*" {
		baseKey := strings.Replace(_key.Path, "/*", "", 1)
		objsLocal, err := _key.Database.GetList(_key.Path)
		if err != nil {
			objsLocal = []meta.Object{}
		}

		objsPivot, err := getEntriesFromPivot(client, pivot, _key.Path)
		if err != nil {
			return err
		}

		// Re-fetch pivot activity to get the latest value (including any recent deletes)
		// This is important because the activity might have changed since synchronizeItem was called
		latestActivity, err := checkPivotActivity(client, pivot, baseKey)
		if err == nil && latestActivity.LastEntry > pivotActivity {
			pivotActivity = latestActivity.LastEntry
		}

		// Build map of local entries for O(1) lookup
		localEntries := make(map[string]meta.Object, len(objsLocal))
		for _, obj := range objsLocal {
			localEntries[obj.Index] = obj
		}

		// Build map of pivot entries for O(1) lookup
		pivotEntries := make(map[string]meta.Object, len(objsPivot))
		for _, obj := range objsPivot {
			pivotEntries[obj.Index] = obj
		}

		// Send local items to pivot (new or updated)
		for _, objLocal := range objsLocal {
			fullKey := baseKey + "/" + objLocal.Index

			// Skip items that were recently synced by a pull-only sync
			// These items came from pivot, so we don't need to send them back
			if isRecentDelete != nil && isRecentDelete(fullKey) {
				continue
			}

			if objPivot, exists := pivotEntries[objLocal.Index]; exists {
				// Item exists on both sides - send if local is newer
				if objLocal.Updated > objPivot.Updated {
					sendToPivot(client, fullKey, pivot, objLocal)
				}
			} else {
				// Item only exists locally - check if it's new or was deleted on pivot
				// An item is considered "new" if:
				// 1. Pivot has no activity (pivotActivity == 0), OR
				// 2. The item was created AFTER pivot's last activity
				// This ensures we don't re-add items that were synced to pivot and then deleted
				if pivotActivity == 0 || objLocal.Created > pivotActivity {
					sendToPivot(client, fullKey, pivot, objLocal)
				}
			}
		}

		// Send delete commands to pivot for items that exist on pivot but not locally
		// This handles the case where an item was deleted on the node
		// Note: This is a fallback mechanism. The primary delete sync happens in StorageSync
		// which sends delete commands directly when a delete event occurs.
		for _, objPivot := range objsPivot {
			if _, exists := localEntries[objPivot.Index]; !exists {
				fullKey := baseKey + "/" + objPivot.Index
				// Skip items that were recently deleted by a pull-only sync
				// These items were deleted from pivot, so we don't need to send delete again
				if isRecentDelete != nil && isRecentDelete(fullKey) {
					continue
				}
				// Item exists on pivot but not locally - it was deleted locally
				// Send delete command to pivot
				sendDelete(client, fullKey, pivot, objPivot.Updated)
			}
		}

		return nil
	}

	obj, err := _key.Database.Get(_key.Path)
	if err != nil {
		return err
	}
	sendToPivot(client, obj.Index, pivot, obj)

	return nil
}

func synchronizeItem(client *http.Client, pivot string, key Key) error {
	return synchronizeItemWithDeleteCheck(client, pivot, key, nil)
}

func synchronizeItemWithDeleteCheck(client *http.Client, pivot string, key Key, isRecentDelete func(key string) bool) error {
	update := false
	_key := strings.Replace(key.Path, "/*", "", 1)
	//check
	activityPivot, err := checkPivotActivity(client, pivot, _key)
	if err != nil {
		return errors.New("failed to check activity for " + _key + " on pivot")
	}
	activityLocal, err := checkActivity(key)
	if err != nil {
		return errors.New("failed to check activity for " + _key + " on local")
	}

	// sync local to pivot (includes sending deletes for items deleted locally)
	if activityLocal.LastEntry > activityPivot.LastEntry {
		// Pass pivot's activity and delete check so syncPivotEntries can determine if items are new or deleted
		err := syncPivotEntriesWithDeleteCheck(client, pivot, key, activityPivot.LastEntry, isRecentDelete)
		if err != nil {
			return err
		}
		update = true
	}

	// sync pivot to local
	if activityLocal.LastEntry < activityPivot.LastEntry {
		err := syncLocalEntries(client, pivot, key, activityPivot.LastEntry)
		if err != nil {
			return err
		}
		update = true
	}

	if update {
		return nil
	}

	return errors.New("nothing to synchronize for " + key.Path)
}

// synchronizeKeys performs the actual synchronization without mutex handling
func synchronizeKeys(client *http.Client, pivot string, keys []Key) error {
	return synchronizeKeysWithDeleteCheck(client, pivot, keys, nil)
}

// synchronizeKeysWithDeleteCheck performs synchronization with an optional delete check.
// If isRecentDelete is provided, items that were recently deleted by a pull-only sync
// will not be re-added to pivot.
func synchronizeKeysWithDeleteCheck(client *http.Client, pivot string, keys []Key, isRecentDelete func(key string) bool) error {
	update := false
	for _, key := range keys {
		errItem := synchronizeItemWithDeleteCheck(client, pivot, key, isRecentDelete)
		if errItem == nil {
			update = true
		}
	}
	if update {
		return nil
	}
	return errors.New("nothing to synchronize")
}

// pullFromPivot syncs FROM pivot only, without sending local data back.
// Used by /synchronize/pivot endpoint when pivot triggers a sync on a node.
// This prevents re-adding items that pivot just deleted.
func pullFromPivot(client *http.Client, pivot string, keys []Key) error {
	return pullFromPivotWithTracking(client, pivot, keys, nil, nil)
}

// pullFromPivotWithTracking syncs FROM pivot and tracks synced keys.
// onDelete is called for each key deleted locally, onSet is called for each key set locally.
func pullFromPivotWithTracking(client *http.Client, pivot string, keys []Key, onDelete func(key string), onSet func(key string)) error {
	update := false
	for _, _key := range keys {
		baseKey := strings.Replace(_key.Path, "/*", "", 1)
		activityPivot, err := checkPivotActivity(client, pivot, baseKey)
		if err != nil {
			continue
		}
		activityLocal, err := checkActivity(_key)
		if err != nil {
			continue
		}
		// Only sync if pivot has newer data
		if activityPivot.LastEntry > activityLocal.LastEntry {
			if err := syncLocalEntriesWithTracking(client, pivot, _key, activityPivot.LastEntry, onDelete, onSet); err == nil {
				update = true
			}
		}
	}
	if update {
		return nil
	}
	return errors.New("nothing to synchronize")
}

// pullTracker tracks keys that were modified during a pull operation.
// Uses generation counters for deterministic behavior without timing assumptions.
type pullTracker struct {
	mu         sync.RWMutex
	generation uint64            // Current generation counter
	lastPull   uint64            // Generation of last pull operation
	deleted    map[string]uint64 // key -> generation when deleted
	set        map[string]uint64 // key -> generation when set
}

func newPullTracker() *pullTracker {
	return &pullTracker{
		deleted: make(map[string]uint64),
		set:     make(map[string]uint64),
	}
}

// startPull begins a new pull operation and returns its generation
func (p *pullTracker) startPull() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.generation++
	p.lastPull = p.generation
	// Clear old entries (keep only last 2 generations)
	for k, g := range p.deleted {
		if g < p.lastPull-1 {
			delete(p.deleted, k)
		}
	}
	for k, g := range p.set {
		if g < p.lastPull-1 {
			delete(p.set, k)
		}
	}
	return p.generation
}

// trackDelete records a key deleted during pull
func (p *pullTracker) trackDelete(key string, gen uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleted[key] = gen
}

// trackSet records a key set during pull
func (p *pullTracker) trackSet(key string, gen uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.set[key] = gen
}

// pulledDelete returns true if key was deleted in the last pull
func (p *pullTracker) pulledDelete(key string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.lastPull == 0 {
		return false
	}
	gen, exists := p.deleted[key]
	return exists && gen == p.lastPull
}

// pulledSet returns true if key was set in the last pull
func (p *pullTracker) pulledSet(key string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.lastPull == 0 {
		return false
	}
	gen, exists := p.set[key]
	return exists && gen == p.lastPull
}

// syncer coordinates synchronization operations for a server.
// It ensures only one sync runs at a time and tracks pulled keys.
type syncer struct {
	mu      sync.Mutex
	tracker *pullTracker
	client  *http.Client
	pivot   string
	keys    []Key
}

func newSyncer(client *http.Client, pivot string, keys []Key) *syncer {
	return &syncer{
		tracker: newPullTracker(),
		client:  client,
		pivot:   pivot,
		keys:    keys,
	}
}

// Pull syncs FROM pivot only (used when pivot notifies node of changes)
func (s *syncer) Pull() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	gen := s.tracker.startPull()
	return pullFromPivotWithTracking(s.client, s.pivot, s.keys,
		func(key string) { s.tracker.trackDelete(key, gen) },
		func(key string) { s.tracker.trackSet(key, gen) })
}

// Sync performs bidirectional synchronization
func (s *syncer) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return synchronizeKeysWithDeleteCheck(s.client, s.pivot, s.keys, s.tracker.pulledDelete)
}

// TrySync attempts sync but skips if already in progress
func (s *syncer) TrySync() error {
	if !s.mu.TryLock() {
		return nil
	}
	defer s.mu.Unlock()
	return synchronizeKeysWithDeleteCheck(s.client, s.pivot, s.keys, s.tracker.pulledDelete)
}

// PulledDelete returns true if key was deleted in the last pull
func (s *syncer) PulledDelete(key string) bool {
	return s.tracker.pulledDelete(key)
}

// PulledSet returns true if key was set in the last pull
func (s *syncer) PulledSet(key string) bool {
	return s.tracker.pulledSet(key)
}

// StorageSyncCallback is the callback type for storage sync events
type StorageSyncCallback func(event storage.Event)

// makeStorageSync creates a callback that triggers synchronization on storage events.
// For pivot servers (pivotIP == ""), it notifies all nodes on any storage change.
// For node servers, it synchronizes with the pivot on any storage change.
func makeStorageSync(client *http.Client, pivotIP string, keys []Key, _getNodes getNodes, s *syncer, nodeHealth *NodeHealth) StorageSyncCallback {
	return func(event storage.Event) {
		// For node servers, skip events caused by a pull operation
		if pivotIP != "" && s != nil {
			if event.Operation == "set" && s.PulledSet(event.Key) {
				return
			}
			if event.Operation == "del" && s.PulledDelete(event.Key) {
				return
			}
		}

		// Find matching key and its database for this event
		var matchedKey string
		var matchedDB storage.Database
		for _, k := range keys {
			if key.Match(k.Path, event.Key) {
				matchedKey = strings.Replace(k.Path, "/*", "", 1)
				matchedDB = k.Database
				break
			}
		}

		if matchedKey == "" || matchedDB == nil {
			return
		}

		// Track delete timestamps for proper sync
		if event.Operation == "del" {
			matchedDB.Set(StoragePrefix+matchedKey, json.RawMessage(ooo.Time()))
		}

		if pivotIP == "" {
			// This is the pivot server - notify all nodes in parallel
			// Wait for all notifications to complete before returning
			nodes := _getNodes()
			// Filter to healthy nodes if health tracking is enabled
			if nodeHealth != nil {
				nodes = nodeHealth.GetHealthyNodes(nodes)
			}
			if len(nodes) > 0 {
				var wg sync.WaitGroup
				for _, node := range nodes {
					wg.Add(1)
					go func(n string) {
						defer wg.Done()
						ok := TriggerNodeSyncWithHealth(client, n)
						if nodeHealth != nil {
							if ok {
								nodeHealth.MarkHealthy(n)
							} else {
								nodeHealth.MarkUnhealthy(n)
							}
						}
					}(node)
				}
				wg.Wait()
			}
		} else {
			// This is a node - handle sync with pivot
			if event.Operation == "del" {
				// For delete events, send delete command directly to pivot
				// This is more reliable than relying on bidirectional sync to detect the delete
				sendDelete(client, event.Key, pivotIP, time.Now().UnixNano())
			} else {
				// For set events, synchronize with pivot synchronously
				// Synchronous sync ensures the operation completes before the next one starts,
				// preventing race conditions where stale data overwrites newer data
				if s != nil {
					s.TrySync()
				}
			}
		}
	}
}
