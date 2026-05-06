package pivot

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/storage"
	"github.com/gorilla/mux"
)

// SynchronizePivotHandler handles /synchronize/pivot - pull-only sync from pivot.
// Called by pivot when it has changes (including deletes) that nodes should pull.
// If ?key=path is provided, only that specific key will be synced.
func SynchronizePivotHandler(pivot string, pool *syncerPool) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if pool == nil || len(pool.syncers) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "no syncers configured")
			return
		}

		keyPath := r.URL.Query().Get("key")
		if keyPath != "" {
			pool.PullKey(keyPath)
		} else {
			pool.PullAll()
		}
		w.WriteHeader(http.StatusOK)
	}
}

// SynchronizeNodeHandler handles /synchronize/node - bidirectional sync.
// Called by node when it has local changes to push to pivot.
func SynchronizeNodeHandler(pivot string, pool *syncerPool) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if pool == nil || len(pool.syncers) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "no syncers configured")
			return
		}

		pool.SyncAll()
		w.WriteHeader(http.StatusOK)
	}
}

func GetList(db storage.Database, path string) func(w http.ResponseWriter, r *http.Request) {
	baseKey := baseKeyFromPath(path)
	return func(w http.ResponseWriter, r *http.Request) {
		objs, err := db.GetList(path)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}
		// Piggyback the activity timestamp so a node syncing up doesn't need a
		// separate /activity round-trip. Older clients ignore the header; new
		// clients use it to skip checkLeaderActivity. The value matches what
		// /activity returns: max(latestUpdated, deleteTombstone).
		activity := checkLastDelete(db, lastActivity(objs), baseKey)
		w.Header().Set(ActivityHeader, strconv.FormatInt(activity, 10))
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(objs)
	}
}

// GetSingle returns a single entry for non-glob keys
func GetSingle(db storage.Database, path string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		obj, err := db.Get(path)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(obj)
	}
}

// Set set data on the pivot instance
// originatorTracker is used to track which node originated the change (for pivot servers)
// vvManager is the version vector manager for pivot servers (nil for nodes)
func Set(db storage.Database, path string, originatorTracker *OriginatorTracker, vvManager *VVManager) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		decoded, err := meta.DecodeFromReader(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		index := mux.Vars(r)["index"]
		itemKey := path + "/" + index
		if index == "" {
			itemKey = path
		}
		// Track originator before storage write so callback can exclude it from TriggerNodeSync
		if originatorTracker != nil {
			originatorTracker.Set(itemKey, r.Header.Get(OriginatorHeader))
		}
		_, err = db.SetWithMeta(itemKey, decoded.Data, decoded.Created, decoded.Updated)
		if err != nil {
			// Drop the originator entry so it doesn't leak across requests.
			if originatorTracker != nil {
				originatorTracker.Get(itemKey)
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// VV bump happens after a successful storage write so the on-disk VV
		// can never advance past data that wasn't written. The bump is the
		// sole source of truth for VV — the storage event callback used to
		// also bump here, which produced a 2× counter on every HTTP write.
		if vvManager != nil {
			vvManager.increment(itemKey)
		}
		w.WriteHeader(http.StatusOK)
	}
}

// Delete delete data on the pivot instance
// originatorTracker is used to track which node originated the change (for pivot servers)
// vvManager is the version vector manager for pivot servers (nil for nodes)
func Delete(db storage.Database, path string, originatorTracker *OriginatorTracker, vvManager *VVManager) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		index := mux.Vars(r)["index"]
		time := mux.Vars(r)["time"]
		var itemKey string
		if index == "" {
			// Single key delete (e.g., "settings")
			itemKey = path
		} else {
			// Glob pattern delete (e.g., "things/123")
			itemKey = path + "/" + index
		}
		// Track originator before storage write so callback can exclude it from TriggerNodeSync
		if originatorTracker != nil {
			originatorTracker.Set(itemKey, r.Header.Get(OriginatorHeader))
		}
		// Tombstone first, then Del. The reverse order leaves a window where
		// the item is physically gone but no tombstone records the delete; a
		// crash, context-cancel, or storage error in that window lets the next
		// sync round re-fetch the item from a node that hasn't observed the
		// delete and silently resurrect it. Writing the tombstone first means
		// the worst case is an orphan tombstone, which sync resolves correctly.
		if _, err := db.Set(StoragePrefix+path, json.RawMessage(time)); err != nil {
			if originatorTracker != nil {
				originatorTracker.Get(itemKey)
			}
			log.Printf("[pivot] Delete tombstone write failed for %q: %v", path, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := db.Del(itemKey); err != nil {
			if originatorTracker != nil {
				originatorTracker.Get(itemKey)
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// VV bump after both storage writes succeeded — the on-disk VV can
		// never advance past data that wasn't written. Single source of truth;
		// the storage callback no longer increments here.
		if vvManager != nil {
			vvManager.increment(itemKey)
		}
		w.WriteHeader(http.StatusOK)
	}
}

// Activity route to get activity info from the pivot instance
// vvManager is the version vector manager for pivot servers (nil for nodes)
func Activity(_key Key, vvManager *VVManager) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if _key.Database == nil || !_key.Database.Active() {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		activity, _ := checkActivity(_key)
		// Include version vector if available (pivot servers)
		if vvManager != nil {
			activity.VV = vvManager.Get(_key.Path)
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(activity)
	}
}
