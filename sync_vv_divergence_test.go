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

	leader, pushes := stubLeader(t, "things", ActivityEntry{
		LastEntry: collidedTS,
		VV:        VersionVector{"leader": 5},
	}, []meta.Object{})

	clientOpts := ClientOpts{Client: http.DefaultClient, Leader: leader}
	err = synchronizeItemWithTracking(clientOpts, SyncOptions{
		Key:        Key{Path: "things/*", Database: localDB},
		Originator: "127.0.0.1:9000",
		VVManager:  vvm,
	})
	require.NoError(t, err)

	// Distinguish push from pull. With this scenario:
	//   - Push (correct, VVGreater): syncToLeader runs against an empty leader
	//     list. The local item's Created == leaderActivity, so syncToLeader's
	//     per-item guard prevents the actual POST — pushes stays at 0. But
	//     the local item is preserved.
	//   - Pull (wrong direction): syncLocalEntriesWithTracking would see x in
	//     local and not in leader, and DELETE x locally (negative-diff path).
	//   - Pre-fix "nothing to synchronize": err != nil, already caught above.
	// So the surviving local item is what pins push-direction.
	_, getErr := localDB.Get("things/x")
	require.NoError(t, getErr,
		"VVGreater should pick push direction; the local-only item must NOT be deleted (which is what wrongly choosing pull would do)")
	// pushes will be 0 here for the equal-timestamp edge case — syncToLeader's
	// per-item Created>leaderActivity guard suppresses the POST. Pre-existing
	// and orthogonal to this fix; documented in the issue's "practical impact
	// bounded" note.
	_ = pushes
}

// TestSyncPushDirectionPushesWhenItemTimestampsAllow exercises the same
// VVGreater branch with a local item whose Updated strictly exceeds
// leader's last activity, so syncToLeader's per-item guard lets the
// POST through. Without this scenario, the push-case regression test
// can't distinguish "push direction chosen but per-item guard fired"
// from "the function silently no-op'd".
func TestSyncPushDirectionPushesWhenItemTimestampsAllow(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	leaderTS := int64(1_700_000_000_000_000_000)
	localTS := leaderTS + 1_000_000 // strictly newer item

	obj := meta.Object{Created: localTS, Updated: localTS, Index: "x", Path: "things/x", Data: []byte(`{"v":"local-newer"}`)}
	body, err := json.Marshal(obj)
	require.NoError(t, err)
	_, err = localDB.SetWithMeta("things/x", body, localTS, localTS)
	require.NoError(t, err)

	vvm := NewVVManager(localDB, "127.0.0.1:9000")
	vvm.set("things/*", VersionVector{"leader": 5, "127.0.0.1:9000": 3})

	leader, pushes := stubLeader(t, "things", ActivityEntry{
		LastEntry: leaderTS,
		VV:        VersionVector{"leader": 5},
	}, []meta.Object{})

	clientOpts := ClientOpts{Client: http.DefaultClient, Leader: leader}
	err = synchronizeItemWithTracking(clientOpts, SyncOptions{
		Key:        Key{Path: "things/*", Database: localDB},
		Originator: "127.0.0.1:9000",
		VVManager:  vvm,
	})
	require.NoError(t, err)
	require.Greater(t, pushes.Load(), int32(0),
		"VVGreater with item timestamps that pass the per-item guard must POST to leader")
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
// activity without a VV. The reconciler must fall back to the existing
// LastEntry comparison and pull the leader's item. Pinning that the
// fallback path actually drives a pull (not just a no-op) — without an
// item assertion the test would pass even if the fallback regressed
// to "nothing to synchronize".
func TestSyncFallsBackToLastEntryWhenLeaderHasNoVV(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	leaderTS := int64(1_700_000_000_000_000_500)
	leaderItem := meta.Object{Created: leaderTS, Updated: leaderTS, Index: "x", Path: "things/x", Data: []byte(`{"v":"from-leader"}`)}

	// Leader reports newer LastEntry, NO VV (simulates an old peer).
	leader, _ := stubLeader(t, "things", ActivityEntry{LastEntry: leaderTS}, []meta.Object{leaderItem})

	clientOpts := ClientOpts{Client: http.DefaultClient, Leader: leader}
	vvm := NewVVManager(localDB, "127.0.0.1:9000")
	err := synchronizeItemWithTracking(clientOpts, SyncOptions{
		Key:        Key{Path: "things/*", Database: localDB},
		Originator: "127.0.0.1:9000",
		VVManager:  vvm,
	})
	require.NoError(t, err, "fallback must drive a pull, not return 'nothing to synchronize'")

	got, getErr := localDB.Get("things/x")
	require.NoError(t, getErr, "leader's item must have been pulled locally")
	require.JSONEq(t, `{"v":"from-leader"}`, string(got.Data),
		"local copy must match leader's payload")
}

// TestSyncFallsBackToLastEntryWhenLocalHasNoVV: this server has just
// started up and hasn't bumped its VV yet. Leader reports a non-empty
// VV. The asymmetric case must still drive a pull via the LastEntry
// fallback, not silently no-op. Worth pinning because a future change
// could accidentally gate the VV path on either side's presence and
// regress this code path.
func TestSyncFallsBackToLastEntryWhenLocalHasNoVV(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	leaderTS := int64(1_700_000_000_000_000_500)
	leaderItem := meta.Object{Created: leaderTS, Updated: leaderTS, Index: "x", Path: "things/x", Data: []byte(`{"v":"from-leader"}`)}

	// Leader has VV; local does not (no vvm.set calls).
	leader, _ := stubLeader(t, "things", ActivityEntry{
		LastEntry: leaderTS,
		VV:        VersionVector{"leader": 5},
	}, []meta.Object{leaderItem})

	clientOpts := ClientOpts{Client: http.DefaultClient, Leader: leader}
	vvm := NewVVManager(localDB, "127.0.0.1:9000")
	err := synchronizeItemWithTracking(clientOpts, SyncOptions{
		Key:        Key{Path: "things/*", Database: localDB},
		Originator: "127.0.0.1:9000",
		VVManager:  vvm,
	})
	require.NoError(t, err, "asymmetric VV (only leader) must fall back to LastEntry-driven pull")

	_, getErr := localDB.Get("things/x")
	require.NoError(t, getErr, "leader's item must have been pulled locally via LastEntry fallback")
}
