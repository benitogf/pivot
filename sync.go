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

// get from local and send to pivot
// Sends new items and updates, but NOT items that were deleted on pivot.
// An item is considered "deleted on pivot" if it doesn't exist on pivot AND
// the local item was created BEFORE the pivot's last activity (which includes delete timestamps).
func syncPivotEntries(client *http.Client, pivot string, _key Key, pivotActivity int64) error {
	return syncPivotEntriesWithDeleteCheck(client, pivot, _key, pivotActivity, nil)
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

// syncFuncs holds the synchronize functions for a server
type syncFuncs struct {
	// pullOnly syncs FROM pivot only - used by /synchronize/pivot handler
	pullOnly func() error
	// bidirectional syncs both ways - used by /synchronize/node handler
	bidirectional func() error
	// trySync skips if locked - used by BeforeRead
	trySync func() error
	// isRecentPullDelete checks if a key was recently deleted by a pull-only sync
	isRecentPullDelete func(key string) bool
	// isRecentPullSet checks if a key was recently set by a pull-only sync
	isRecentPullSet func(key string) bool
	// isRecentPull checks if a pull-only sync was recently completed
	isRecentPull func() bool
}

// makeSyncFuncs creates synchronize functions with a shared per-server mutex.
// Note: Per-key mutexes could allow parallel sync of different keys but adds complexity.
// The current per-server mutex is sufficient for most use cases.
func makeSyncFuncs(client *http.Client, pivot string, keys []Key) syncFuncs {
	var mu sync.Mutex
	// Track keys that were recently deleted by pull-only sync
	recentPullDeletes := make(map[string]int64)
	var deletesMu sync.RWMutex

	// Track keys that were recently set by pull-only sync
	recentPullSets := make(map[string]int64)
	var setsMu sync.RWMutex

	// Track when the last pull-only sync completed
	var lastPullTime int64
	var pullTimeMu sync.RWMutex

	// Helper to check if a key was recently deleted by pull-only sync
	isRecentDelete := func(key string) bool {
		deletesMu.RLock()
		defer deletesMu.RUnlock()
		t, exists := recentPullDeletes[key]
		if !exists {
			return false
		}
		// Consider delete recent if within 5 seconds
		return time.Now().UnixNano()-t < 5*1e9
	}

	// Helper to check if a key was recently set by pull-only sync
	isRecentSet := func(key string) bool {
		setsMu.RLock()
		defer setsMu.RUnlock()
		t, exists := recentPullSets[key]
		if !exists {
			return false
		}
		// Consider set recent if within 5 seconds
		return time.Now().UnixNano()-t < 5*1e9
	}

	// Helper to check if a pull-only sync was recently completed
	// This is used to skip StorageSync callbacks for events that were caused by the pull
	isRecentPull := func() bool {
		pullTimeMu.RLock()
		defer pullTimeMu.RUnlock()
		// Consider pull recent if within 100ms (enough time for async events to be processed)
		return time.Now().UnixNano()-lastPullTime < 100*1e6
	}

	return syncFuncs{
		pullOnly: func() error {
			mu.Lock()
			defer mu.Unlock()
			// Clear old entries before pull (entries older than 5 seconds)
			now := time.Now().UnixNano()
			deletesMu.Lock()
			for k, t := range recentPullDeletes {
				if now-t > 5*1e9 {
					delete(recentPullDeletes, k)
				}
			}
			deletesMu.Unlock()
			setsMu.Lock()
			for k, t := range recentPullSets {
				if now-t > 5*1e9 {
					delete(recentPullSets, k)
				}
			}
			setsMu.Unlock()
			err := pullFromPivotWithTracking(client, pivot, keys,
				func(key string) {
					deletesMu.Lock()
					recentPullDeletes[key] = time.Now().UnixNano()
					deletesMu.Unlock()
				},
				func(key string) {
					setsMu.Lock()
					recentPullSets[key] = time.Now().UnixNano()
					setsMu.Unlock()
				})
			// Record when pull completed
			pullTimeMu.Lock()
			lastPullTime = time.Now().UnixNano()
			pullTimeMu.Unlock()
			return err
		},
		bidirectional: func() error {
			mu.Lock()
			defer mu.Unlock()
			// Use synchronizeKeysWithDeleteCheck to skip recently deleted items
			return synchronizeKeysWithDeleteCheck(client, pivot, keys, isRecentDelete)
		},
		trySync: func() error {
			if !mu.TryLock() {
				return nil
			}
			defer mu.Unlock()
			// Use synchronizeKeysWithDeleteCheck to skip recently deleted items
			return synchronizeKeysWithDeleteCheck(client, pivot, keys, isRecentDelete)
		},
		isRecentPullDelete: isRecentDelete,
		isRecentPullSet:    isRecentSet,
		isRecentPull:       isRecentPull,
	}
}

// StorageSyncCallback is the callback type for storage sync events
type StorageSyncCallback func(event storage.Event)

// StorageSync creates a StorageSyncCallback that triggers synchronization on storage events.
// This replaces the need for SyncWriteFilter and SyncDeleteFilter.
// For pivot servers (pivotIP == ""), it notifies all nodes on any storage change.
// For node servers, it synchronizes with the pivot synchronously on any storage change.
// If nodeHealth is provided, unhealthy nodes are skipped to avoid timeout penalties.
// isRecentPullDelete checks if a key was recently deleted by a pull-only sync.
// isRecentPull checks if a pull-only sync was recently completed.
func StorageSync(client *http.Client, pivotIP string, keys []Key, _getNodes getNodes, synchronize func() error, nodeHealth *NodeHealth, isRecentPullDelete func(key string) bool, isRecentPullSet func(key string) bool, isRecentPull func() bool) StorageSyncCallback {
	return func(event storage.Event) {
		// For node servers, skip sync for events that were caused by a pull-only sync
		// - Skip SET events from pull to prevent triggering unnecessary bidirectional sync
		// - Skip DEL events from pull to prevent triggering sync that would re-add the item
		if pivotIP != "" {
			if event.Operation == "set" && isRecentPullSet != nil && isRecentPullSet(event.Key) {
				return
			}
			if event.Operation == "del" && isRecentPullDelete != nil && isRecentPullDelete(event.Key) {
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
				synchronize()
			}
		}
	}
}
