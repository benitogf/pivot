package pivot

// Regression test for the invariant the redundant checkLeaderActivity call was
// protecting before it was replaced with the X-Pivot-Activity response header:
// when the leader has a delete tombstone newer than a local-only object's
// Created timestamp, syncToLeader must NOT push that object back to the leader
// (which would resurrect a just-deleted item, causing divergence).
//
// Two scenarios are exercised:
//   - new leader: emits X-Pivot-Activity, optimized path
//   - old leader: omits the header, fallback to /activity endpoint
// Both must reach the same conclusion.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
)

func TestSyncToLeaderHonoursTombstone(t *testing.T) {
	cases := []struct {
		name        string
		emitHeader  bool
		serveActivity bool
	}{
		{name: "new_leader_with_header", emitHeader: true, serveActivity: false},
		{name: "old_leader_fallback", emitHeader: false, serveActivity: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			monotonic.Init()

			// Local node has a stale copy of "items/X" with Created strictly
			// older than the tombstone timestamp the leader will report.
			localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
			if err := localDB.Start(storage.Options{}); err != nil {
				t.Fatal(err)
			}
			storage.WatchWithCallback(localDB, func(storage.Event) {})
			defer localDB.Close()

			oldTime := time.Now().UTC().Add(-2 * time.Hour).UnixNano()
			obj := meta.Object{Created: oldTime, Updated: oldTime, Index: "X", Path: "items/X", Data: []byte(`{"v":1}`)}
			body, _ := json.Marshal(obj)
			if _, err := localDB.SetWithMeta("items/X", body, oldTime, oldTime); err != nil {
				t.Fatal(err)
			}

			// Tombstone timestamp on the leader is newer than the local object.
			tombstoneTS := time.Now().UTC().UnixNano()

			// Track any push attempts to the leader. Any POST/DELETE here would
			// be a resurrection bug.
			var pushAttempts int32

			mux := http.NewServeMux()
			// GetList: returns empty list, optionally with the activity header.
			mux.HandleFunc("/_pivot/pivot/items/", func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					atomic.AddInt32(&pushAttempts, 1)
					w.WriteHeader(http.StatusOK)
					return
				}
				if tc.emitHeader {
					w.Header().Set(ActivityHeader, strconv.FormatInt(tombstoneTS, 10))
				}
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode([]meta.Object{})
			})
			// Activity endpoint: returns the tombstone timestamp.
			mux.HandleFunc("/_pivot/activity/items", func(w http.ResponseWriter, r *http.Request) {
				if !tc.serveActivity {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				_ = json.NewEncoder(w).Encode(ActivityEntry{LastEntry: tombstoneTS})
			})

			srv := httptest.NewServer(mux)
			defer srv.Close()

			// Strip scheme: ClientOpts.Leader expects host:port.
			leader := srv.URL[len("http://"):]
			clientOpts := ClientOpts{Client: srv.Client(), Leader: leader}

			err := syncToLeader(clientOpts, SyncOptions{
				Key:       Key{Path: "items/*", Database: localDB},
				LastEntry: 0, // force the function to discover the leader's activity
			})
			if err != nil {
				t.Fatalf("syncToLeader: %v", err)
			}

			if got := atomic.LoadInt32(&pushAttempts); got != 0 {
				t.Fatalf("local stale object was resurrected: %d push attempts to leader (expected 0)", got)
			}
		})
	}
}
