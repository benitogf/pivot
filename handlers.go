package pivot

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/storage"
	"github.com/gorilla/mux"
)

// SynchronizePivotHandler handles /synchronize/pivot - pull-only sync from pivot.
// Called by pivot when it has changes (including deletes) that nodes should pull.
func SynchronizePivotHandler(pivot string, s *syncer) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if pivot == "" || s == nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "this method should not be called on the pivot server")
			return
		}

		s.Pull()
		w.WriteHeader(http.StatusOK)
	}
}

// SynchronizeNodeHandler handles /synchronize/node - bidirectional sync.
// Called by node when it has local changes to push to pivot.
func SynchronizeNodeHandler(pivot string, s *syncer) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if pivot == "" || s == nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "this method should not be called on the pivot server")
			return
		}

		s.Sync()
		w.WriteHeader(http.StatusOK)
	}
}

func GetList(db storage.Database, path string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		objs, err := db.GetList(path)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
			return
		}

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
func Set(db storage.Database, path string, originatorTracker *OriginatorTracker) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		decoded, err := meta.DecodeFromReader(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		index := mux.Vars(r)["index"]
		itemKey := path + "/" + decoded.Index
		if index == "" {
			itemKey = path
		}
		// Track originator before storage write so callback can exclude it from TriggerNodeSync
		if originatorTracker != nil {
			originatorTracker.Set(itemKey, r.Header.Get(OriginatorHeader))
		}
		_, err = db.SetWithMeta(itemKey, decoded.Data, decoded.Created, decoded.Updated)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// Delete delete data on the pivot instance
// originatorTracker is used to track which node originated the change (for pivot servers)
func Delete(db storage.Database, path string, originatorTracker *OriginatorTracker) func(w http.ResponseWriter, r *http.Request) {
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
		err := db.Del(itemKey)
		db.Set(StoragePrefix+path, json.RawMessage(time))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

// Activity route to get activity info from the pivot instance
func Activity(_key Key) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if _key.Database == nil || !_key.Database.Active() {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		activity, _ := checkActivity(_key)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(activity)
	}
}
