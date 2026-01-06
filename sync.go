package pivot

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/storage"
)

// get from pivot and write to local
func syncLocalEntries(client *http.Client, pivot string, _key Key, lastEntry int64) error {
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
			_key.Database.Del(baseKey + "/" + index)
		}

		objsToSend := GetEntriesPositiveDiff(objsLocal, objsPivot)
		for _, obj := range objsToSend {
			_key.Database.SetWithMeta(baseKey+"/"+obj.Index, obj.Data, obj.Created, obj.Updated)
		}
		_key.Database.Set(StoragePrefix+baseKey, json.RawMessage(strconv.FormatInt(lastEntry, 10)))
		return nil
	}

	obj, err := getEntryFromPivot(client, pivot, _key.Path)
	if err != nil {
		return err
	}
	_key.Database.SetWithMeta(_key.Path, obj.Data, obj.Created, obj.Updated)
	_key.Database.Set(StoragePrefix+_key.Path, json.RawMessage(strconv.FormatInt(lastEntry, 10)))

	return nil
}

// get from local and send to pivot (updates only, no new entries or deletes)
func syncPivotEntries(client *http.Client, pivot string, _key Key, lastEntry int64) error {
	if key.LastIndex(_key.Path) == "*" {
		baseKey := strings.Replace(_key.Path, "/*", "", 1)
		objsLocal, err := _key.Database.GetList(_key.Path)
		if err != nil {
			return err
		}

		objsPivot, err := getEntriesFromPivot(client, pivot, _key.Path)
		if err != nil {
			return err
		}

		objsToDelete := GetEntriesNegativeDiff(objsPivot, objsLocal)
		for _, index := range objsToDelete {
			sendDelete(client, baseKey+"/"+index, pivot, lastEntry)
		}

		objsToSend := GetEntriesPositiveDiff(objsPivot, objsLocal)
		for _, obj := range objsToSend {
			sendToPivot(client, baseKey+"/"+obj.Index, pivot, obj)
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

	// sync
	if activityLocal.LastEntry > activityPivot.LastEntry {
		err := syncPivotEntries(client, pivot, key, activityLocal.LastEntry)
		if err != nil {
			return err
		}
		update = true
	}

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
	update := false
	for _, key := range keys {
		errItem := synchronizeItem(client, pivot, key)
		if errItem == nil {
			update = true
		}
	}
	if update {
		return nil
	}
	return errors.New("nothing to synchronize")
}

// syncFuncs holds the synchronize functions for a server
type syncFuncs struct {
	// sync waits for the lock - used by /synchronize handler
	sync func() error
	// trySync skips if locked - used by BeforeRead and StorageSync
	trySync func() error
}

// makeSyncFuncs creates synchronize functions with a shared per-server mutex.
// Note: Per-key mutexes could allow parallel sync of different keys but adds complexity.
// The current per-server mutex is sufficient for most use cases.
func makeSyncFuncs(client *http.Client, pivot string, keys []Key) syncFuncs {
	var mu sync.Mutex
	return syncFuncs{
		sync: func() error {
			mu.Lock()
			defer mu.Unlock()
			return synchronizeKeys(client, pivot, keys)
		},
		trySync: func() error {
			if !mu.TryLock() {
				return nil
			}
			defer mu.Unlock()
			return synchronizeKeys(client, pivot, keys)
		},
	}
}

// StorageSyncCallback is the callback type for storage sync events
type StorageSyncCallback func(event storage.Event)

// StorageSync creates a StorageSyncCallback that triggers synchronization on storage events.
// This replaces the need for SyncWriteFilter and SyncDeleteFilter.
// For pivot servers (pivotIP == ""), it notifies all nodes on any storage change.
// For node servers, it synchronizes with the pivot synchronously on any storage change.
// If nodeHealth is provided, unhealthy nodes are skipped to avoid timeout penalties.
func StorageSync(client *http.Client, pivotIP string, keys []Key, _getNodes getNodes, synchronize func() error, nodeHealth *NodeHealth) StorageSyncCallback {
	return func(event storage.Event) {
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
			// This is a node - synchronize with pivot synchronously
			// Synchronous sync ensures the operation completes before the next one starts,
			// preventing race conditions where stale data overwrites newer data
			synchronize()
		}
	}
}
