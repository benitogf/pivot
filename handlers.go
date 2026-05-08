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

// PostWriteFunc is called by Set/Delete handlers after a successful local
// write to propagate the change. On pivot servers it triggers the matched
// peer nodes (skipping the originating peer); on node servers it pushes
// the change to the pivot leader. Running this synchronously after the
// VV bump (rather than from the async storage event callback) means a
// peer woken by the trigger sees the bumped VV, not a stale one.
type PostWriteFunc func(itemKey, op, originatorPeer string)

// Set set data on the pivot instance
// handlerTracker records "a handler will own the post-write work for this
// key" so the async storage callback skips its bump+fanout for this event;
// the handler does both, in order, after the storage write succeeds.
// vvManager is the version vector manager for pivot servers (nil for nodes).
// postWrite, if non-nil, runs synchronously after a successful VV bump to
// fan out the change to peers. May be nil in tests that don't need fanout.
func Set(db storage.Database, path string, handlerTracker *HandlerWriteTracker, vvManager *VVManager, postWrite PostWriteFunc) func(w http.ResponseWriter, r *http.Request) {
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
		// Idempotency guard via the originator's VV. A retried Trigger or
		// a delayed coalescer drainer can deliver a write whose VV is
		// dominated by what pivot already holds — without this skip, the
		// stale write would clobber a newer locally-pivoted one. VV is
		// the right signal because the codebase deliberately allows
		// older-timestamped writes with higher counters
		// (TestClockDriftScenario), so timestamp comparison can't be
		// used. Skip on VVGreater (local strictly dominates) and VVEqual
		// (exact retry of an already-applied write); proceed on VVLess
		// and VVConcurrent (inbound has new info worth integrating).
		// Missing/empty header = older peer, fall through (backward compat).
		if vvManager != nil {
			if peerVV, ok := decodeVVHeader(r.Header.Get(VVHeader)); ok {
				localVV := vvManager.Get(path)
				cmp := localVV.Compare(peerVV)
				if cmp == VVGreater || cmp == VVEqual {
					w.WriteHeader(http.StatusOK)
					return
				}
			}
		}
		// Mark before the storage write so the dedup signal is in place
		// by the time the watch goroutine processes the event. The
		// originator header is captured locally for the post-write fanout
		// to skip echoing back to the peer that drove this request.
		originatorPeer := r.Header.Get(OriginatorHeader)
		if handlerTracker != nil {
			handlerTracker.Mark(itemKey)
		}
		_, err = db.SetWithMeta(itemKey, decoded.Data, decoded.Created, decoded.Updated)
		if err != nil {
			// Contract: a non-nil error from the storage layer means no
			// event was fired (verified for Layered: every error path
			// returns before sendEvent; wrapper Databases short-circuit
			// before Layered runs at all). The Mark would never be
			// Consumed without this Unmark, so without it pending counts
			// would accumulate on every storage failure.
			if handlerTracker != nil {
				handlerTracker.Unmark(itemKey)
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// VV bump after a successful storage write. Bump at PATH scope —
		// the registered base path is what /activity exposes, what peers
		// cache as lastSyncedVV, and what every other VV consumer reads.
		// Bumping at item scope (the storage event's full key) would land
		// in a separate, never-read VV entry; /activity would still
		// expose an empty VV for glob paths and the whole VV machinery
		// would silently degrade to LastEntry-only logic. The bump must
		// complete BEFORE we trigger peers — otherwise a peer woken by
		// the trigger could read /activity and see the pre-bump VV.
		if vvManager != nil {
			vvManager.increment(path)
			// Merge-on-receive: integrate the originator's peer counters
			// into local VV. Without this, each node's VV is just
			// {"my-id": counter} and cross-node Compare always returns
			// VVConcurrent; with it, /activity reflects the cluster's
			// actual causal frontier and the idempotency / divergence
			// detection paths can make meaningful decisions.
			if peerVV, ok := decodeVVHeader(r.Header.Get(VVHeader)); ok {
				vvManager.set(path, peerVV)
			}
		}
		if postWrite != nil {
			postWrite(itemKey, "set", originatorPeer)
		}
		w.WriteHeader(http.StatusOK)
	}
}

// Delete delete data on the pivot instance
// handlerTracker / vvManager / postWrite mirror Set's parameters.
//
// Only the itemKey storage event needs Mark/Consume dedup. The tombstone
// Set fires an event for StoragePrefix+path (e.g. "pivot/things") which
// doesn't match any configured Key.Path glob (e.g. "things/*"), so
// makeStorageSync returns early at !found before reaching Consume —
// Marking it would just accumulate forever.
func Delete(db storage.Database, path string, handlerTracker *HandlerWriteTracker, vvManager *VVManager, postWrite PostWriteFunc) func(w http.ResponseWriter, r *http.Request) {
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
		// Idempotency guard via the originator's VV — same shape as the
		// Set handler. A retried or stale Delete whose VV is dominated
		// must not remove a newer local write.
		if vvManager != nil {
			if peerVV, ok := decodeVVHeader(r.Header.Get(VVHeader)); ok {
				localVV := vvManager.Get(path)
				cmp := localVV.Compare(peerVV)
				if cmp == VVGreater || cmp == VVEqual {
					w.WriteHeader(http.StatusOK)
					return
				}
			}
		}
		originatorPeer := r.Header.Get(OriginatorHeader)
		if handlerTracker != nil {
			handlerTracker.Mark(itemKey)
		}
		// Tombstone first, then Del. The reverse order leaves a window where
		// the item is physically gone but no tombstone records the delete; a
		// crash, context-cancel, or storage error in that window lets the next
		// sync round re-fetch the item from a node that hasn't observed the
		// delete and silently resurrect it. Writing the tombstone first means
		// the worst case is an orphan tombstone, which sync resolves correctly.
		if _, err := db.Set(StoragePrefix+path, json.RawMessage(time)); err != nil {
			if handlerTracker != nil {
				handlerTracker.Unmark(itemKey)
			}
			log.Printf("[pivot] Delete tombstone write failed for %q: %v", path, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if err := db.Del(itemKey); err != nil {
			if handlerTracker != nil {
				handlerTracker.Unmark(itemKey)
			}
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// VV bump at PATH scope (see the Set handler for the full rationale).
		// Must precede the post-write trigger so a peer woken by the trigger
		// reads a fresh VV. Merge-on-receive integrates the originator's
		// peer counters into local VV.
		if vvManager != nil {
			vvManager.increment(path)
			if peerVV, ok := decodeVVHeader(r.Header.Get(VVHeader)); ok {
				vvManager.set(path, peerVV)
			}
		}
		if postWrite != nil {
			postWrite(itemKey, "del", originatorPeer)
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
