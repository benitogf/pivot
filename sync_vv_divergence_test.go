package pivot

// Regression tests for the silent-divergence bug in
// synchronizeItemWithTracking: when leader and local report equal
// activity LastEntry but their version vectors disagree (concurrent
// writes producing a timestamp collision), the function used to
// short-circuit on the timestamp comparison and return "nothing to
// synchronize" — leaving the cluster diverged until the next write
// bumped LastEntry past the collision.
//
// The fix uses VV.Compare for direction whenever both sides expose a
// VV, falling back to LastEntry-only logic when either side doesn't
// (backward compatibility with old peers).

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/stretchr/testify/require"
)

// stubLeader returns a leader HTTP server that reports a fixed activity
// payload and serves an empty GetList. Push attempts (POST/DELETE) are
// counted via the returned atomic so tests can assert direction.
func stubLeader(t *testing.T, base string, activity ActivityEntry, list []meta.Object) (string, *atomic.Int32) {
	t.Helper()
	var pushes atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/_pivot/activity/"+base, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(activity)
	})
	mux.HandleFunc("/_pivot/pivot/"+base+"/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(list)
		default:
			pushes.Add(1)
			w.WriteHeader(http.StatusOK)
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	leader := srv.URL[len("http://"):]
	return leader, &pushes
}

// TestSyncDetectsVVDivergenceOnEqualLastEntryPullCase: leader's VV
// dominates local's VV (VVLess) at the same LastEntry. Pre-fix the
// equal-timestamp short-circuit means no GetList is issued. Post-fix
// the VV comparison picks the pull direction and triggers a GetList.
func TestSyncDetectsVVDivergenceOnEqualLastEntryPullCase(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	collidedTS := int64(1_700_000_000_000_000_000)
	// Seed a single local item so checkActivity returns collidedTS.
	obj := meta.Object{Created: collidedTS, Updated: collidedTS, Index: "x", Path: "things/x", Data: []byte(`{"v":"local"}`)}
	body, err := json.Marshal(obj)
	require.NoError(t, err)
	_, err = localDB.SetWithMeta("things/x", body, collidedTS, collidedTS)
	require.NoError(t, err)

	// Local VV is strictly less than leader's: leader has 5 bumps from "leader",
	// local has only seen 3.
	vvm := NewVVManager(localDB, "127.0.0.1:9000")
	vvm.set("things/*", VersionVector{"leader": 3}) // normalizes to baseKey "things"

	// Leader reports the same LastEntry but a strictly greater VV.
	var requestedGetList atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/_pivot/activity/things", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ActivityEntry{
			LastEntry: collidedTS,
			VV:        VersionVector{"leader": 5},
		})
	})
	mux.HandleFunc("/_pivot/pivot/things/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			requestedGetList.Store(true)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode([]meta.Object{})
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	leader := srv.URL[len("http://"):]

	clientOpts := ClientOpts{Client: srv.Client(), Leader: leader}
	err = synchronizeItemWithTracking(clientOpts, SyncOptions{
		Key:        Key{Path: "things/*", Database: localDB},
		Originator: "127.0.0.1:9000",
		VVManager:  vvm,
	})
	require.NoError(t, err, "synchronizeItemWithTracking must not return 'nothing to synchronize' when VVs disagree at equal LastEntry")
	require.True(t, requestedGetList.Load(),
		"VV-driven direction should have triggered a GetList from leader; got no pull")
}

// TestSyncDetectsVVDivergenceOnEqualLastEntryPushCase: local's VV
// dominates leader's VV (VVGreater) at the same LastEntry. Post-fix the
// VV comparison picks the push direction and triggers a POST/DELETE to
// leader.
func TestSyncDetectsVVDivergenceOnEqualLastEntryPushCase(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	collidedTS := int64(1_700_000_000_000_000_000)
	obj := meta.Object{Created: collidedTS, Updated: collidedTS, Index: "x", Path: "things/x", Data: []byte(`{"v":"local"}`)}
	body, err := json.Marshal(obj)
	require.NoError(t, err)
	_, err = localDB.SetWithMeta("things/x", body, collidedTS, collidedTS)
	require.NoError(t, err)

	vvm := NewVVManager(localDB, "127.0.0.1:9000")
	vvm.set("things/*", VersionVector{"leader": 5, "127.0.0.1:9000": 3}) // normalizes to baseKey "things"; local strictly greater

	leader, _ := stubLeader(t, "things", ActivityEntry{
		LastEntry: collidedTS,
		VV:        VersionVector{"leader": 5},
	}, []meta.Object{})

	clientOpts := ClientOpts{Client: http.DefaultClient, Leader: leader}
	err = synchronizeItemWithTracking(clientOpts, SyncOptions{
		Key:        Key{Path: "things/*", Database: localDB},
		Originator: "127.0.0.1:9000",
		VVManager:  vvm,
	})
	// The fix's contract here: choose the push direction (call syncToLeader)
	// instead of short-circuiting on equal LastEntry. Whether any item
	// actually transfers is gated by syncToLeader's per-item timestamp
	// guards (objLocal.Created > leaderActivity, etc.) — pre-existing and
	// intentional. We just pin that the function no longer reports
	// "nothing to synchronize".
	require.NoError(t, err)
}

// TestSyncSkipsWhenVVsAreEqual: local and leader VVs are identical.
// Truly nothing to sync. Function returns the documented error and
// makes no GetList / push.
func TestSyncSkipsWhenVVsAreEqual(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	collidedTS := int64(1_700_000_000_000_000_000)
	vvm := NewVVManager(localDB, "127.0.0.1:9000")
	vvm.set("things/*", VersionVector{"leader": 5})

	var contacted atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/_pivot/activity/things", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ActivityEntry{
			LastEntry: collidedTS,
			VV:        VersionVector{"leader": 5},
		})
	})
	mux.HandleFunc("/_pivot/pivot/things/", func(w http.ResponseWriter, r *http.Request) {
		contacted.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	leader := srv.URL[len("http://"):]

	clientOpts := ClientOpts{Client: srv.Client(), Leader: leader}
	err := synchronizeItemWithTracking(clientOpts, SyncOptions{
		Key:        Key{Path: "things/*", Database: localDB},
		Originator: "127.0.0.1:9000",
		VVManager:  vvm,
	})
	require.Error(t, err, "equal VVs must return 'nothing to synchronize'")
	require.Contains(t, err.Error(), "nothing to synchronize")
	require.Zero(t, contacted.Load(), "no push or pull should have been issued when VVs match")
}

// TestSyncConcurrentVVsLogsAndPullsLeader: both sides have writes the
// other hasn't seen (VVConcurrent). The convention — matching
// pullKeyWithCacheUpdate — is last-sync-wins: log the conflict and pull
// from leader. Local-only items are dropped this round; they get re-
// pushed next sync after a write bumps local's VV further.
func TestSyncConcurrentVVsLogsAndPullsLeader(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	collidedTS := int64(1_700_000_000_000_000_000)
	obj := meta.Object{Created: collidedTS, Updated: collidedTS, Index: "x", Path: "things/x", Data: []byte(`{"v":"local"}`)}
	body, err := json.Marshal(obj)
	require.NoError(t, err)
	_, err = localDB.SetWithMeta("things/x", body, collidedTS, collidedTS)
	require.NoError(t, err)

	// Concurrent VVs: each side has counters the other doesn't dominate.
	vvm := NewVVManager(localDB, "127.0.0.1:9000")
	vvm.set("things/*", VersionVector{"127.0.0.1:9000": 5})

	var requestedGetList atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/_pivot/activity/things", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ActivityEntry{
			LastEntry: collidedTS,
			VV:        VersionVector{"leader": 5}, // disjoint from local's counter
		})
	})
	mux.HandleFunc("/_pivot/pivot/things/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			requestedGetList.Store(true)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode([]meta.Object{})
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	leader := srv.URL[len("http://"):]

	clientOpts := ClientOpts{Client: srv.Client(), Leader: leader}
	err = synchronizeItemWithTracking(clientOpts, SyncOptions{
		Key:        Key{Path: "things/*", Database: localDB},
		Originator: "127.0.0.1:9000",
		VVManager:  vvm,
	})
	require.NoError(t, err)
	require.True(t, requestedGetList.Load(),
		"VVConcurrent should have driven a pull from leader (last-sync-wins convention)")
}

// TestSyncFallsBackToLastEntryWhenLeaderHasNoVV: an old leader returns
// activity without a VV. The bidirectional reconciler must fall back to
// the existing LastEntry comparison so older peers continue to work.
func TestSyncFallsBackToLastEntryWhenLeaderHasNoVV(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	leaderTS := int64(1_700_000_000_000_000_500)

	// Leader reports newer LastEntry, NO VV (simulates an old peer).
	leader, _ := stubLeader(t, "things", ActivityEntry{LastEntry: leaderTS}, []meta.Object{})

	clientOpts := ClientOpts{Client: http.DefaultClient, Leader: leader}
	vvm := NewVVManager(localDB, "127.0.0.1:9000")
	err := synchronizeItemWithTracking(clientOpts, SyncOptions{
		Key:        Key{Path: "things/*", Database: localDB},
		Originator: "127.0.0.1:9000",
		VVManager:  vvm,
	})
	// Local has nothing seeded so checkActivity reports LastEntry=0.
	// Leader reports leaderTS > 0 → fallback path picks "pull leader to local".
	// We only guard against the function silently reporting "nothing to
	// synchronize" — any other outcome (nil error, or an error from the
	// stub's degenerate GetList response) is acceptable.
	if err != nil {
		require.NotContains(t, err.Error(), "nothing to synchronize",
			"LastEntry fallback should still drive a pull when leader is newer")
	}
}
