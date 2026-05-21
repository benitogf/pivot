package pivot

// Tests for the VV-aware inner dedup gate in syncSetFromLeader.
//
// Before the fix, the inner gate compared local.Updated >= obj.Updated to
// decide whether to skip a redundant write. That is unsafe under clock
// skew: a peer that wrote with its wall clock set forward stores a
// future-Updated value that beats every honest present-time write until
// real time catches up. With VV, the gate decides on causal order instead
// of raw timestamps, so a clock-skewed peer's writes still resolve
// correctly.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/stretchr/testify/require"
)

// TestInnerGate_VVEqualSkipsWriteEvenWhenLeaderUpdatedIsFuture is the
// bug-defeating assertion. Local already has the same causal state as the
// leader (VVEqual), but the leader serves an object whose Updated is in
// the future — because some past peer wrote it while its wall clock was
// skewed forward.
//
// Pre-fix: the inner gate at sync.go:138 sees local.Updated < obj.Updated
// and proceeds with the write, clobbering local with the future-stamped
// record. This is exactly the /device/open revert symptom.
//
// Post-fix: the gate consults the VVs (VVEqual), recognises local already
// has this revision, and skips the write. The future-Updated value on the
// leader is irrelevant to the decision.
func TestInnerGate_VVEqualSkipsWriteEvenWhenLeaderUpdatedIsFuture(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	const path = "things/x"
	now := time.Now().UTC().UnixNano()
	futureUpdated := now + int64(24*time.Hour) // 24h ahead — far beyond any clamp tolerance.

	// Local has the "current" record stamped at present.
	localBody := json.RawMessage(`{"v":"current"}`)
	_, err := localDB.SetWithMeta(path, localBody, now, now)
	require.NoError(t, err)

	// Local and leader both report the same VV (VVEqual) — i.e. local
	// already has every write the leader has ever applied to this key.
	sameVV := VersionVector{"leader": 7}
	vvm := NewVVManager(localDB, "127.0.0.1:9000")
	vvm.set("things/x", sameVV)

	// Leader stub: activity reports the equal VV; the per-item GET serves
	// a record with Updated in the future. If the inner gate fell back to
	// the timestamp comparison, this would clobber local.
	mux := http.NewServeMux()
	mux.HandleFunc("/_pivot/activity/"+path, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ActivityEntry{LastEntry: futureUpdated, VV: sameVV})
	})
	mux.HandleFunc("/_pivot/pivot/"+path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(meta.Object{
				Path:    path,
				Index:   "x",
				Created: now,
				Updated: futureUpdated, // poison value
				Data:    json.RawMessage(`{"v":"stale-future"}`),
			})
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	leader := srv.URL[len("http://"):]

	// Drive the same pull path production uses for a single-item key.
	clientOpts := ClientOpts{Client: srv.Client(), Leader: leader}
	err = synchronizeItemWithTracking(clientOpts, SyncOptions{
		Key:        Key{Path: path, Database: localDB},
		Originator: "127.0.0.1:9000",
		VVManager:  vvm,
	})
	// VVEqual returns "nothing to synchronize" — the orchestrator never
	// even reaches the inner gate. That's the right outcome.
	require.Error(t, err, "VVEqual must short-circuit before any write")
	require.Equal(t, "nothing to synchronize for "+path, err.Error())

	// Confirm local was NOT clobbered: data and Updated unchanged.
	got, err := localDB.Get(path)
	require.NoError(t, err)
	require.JSONEq(t, string(localBody), string(got.Data),
		"local data must not have been overwritten by the future-stamped leader record")
	require.Equal(t, now, got.Updated,
		"local Updated must still be the original present-time value, not the leader's future poison")
}

// TestInnerGate_VVGreaterSkipsWriteEvenWhenLeaderUpdatedIsFuture: local's
// VV strictly dominates leader's (local has every write the leader has,
// plus extras). Pre-fix the inner gate would still proceed with a write
// if the leader's served object happened to have Updated > local's. The
// VV-aware gate skips because local is ahead.
//
// Drives the inner gate directly via syncLocalEntriesWithTracking with
// LeaderVV pre-populated, bypassing the orchestrator's outer VVGreater
// branch (which goes to syncToLeader) so we exercise the inner gate path.
func TestInnerGate_VVGreaterSkipsWriteEvenWhenLeaderUpdatedIsFuture(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	const path = "things/x"
	now := time.Now().UTC().UnixNano()
	futureUpdated := now + int64(24*time.Hour)

	localBody := json.RawMessage(`{"v":"local-newer"}`)
	_, err := localDB.SetWithMeta(path, localBody, now, now)
	require.NoError(t, err)

	leaderVV := VersionVector{"leader": 5}
	localVV := VersionVector{"leader": 5, "127.0.0.1:9000": 2} // strictly greater
	vvm := NewVVManager(localDB, "127.0.0.1:9000")
	vvm.set("things/x", localVV)

	// Leader stub returns a record with Updated in the future. With the
	// pre-fix timestamp gate, local.Updated (now) < obj.Updated (future)
	// would proceed with the write, regressing local to leader's older
	// causal state.
	mux := http.NewServeMux()
	mux.HandleFunc("/_pivot/pivot/"+path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(meta.Object{
				Path:    path,
				Index:   "x",
				Created: now,
				Updated: futureUpdated,
				Data:    json.RawMessage(`{"v":"leader-older"}`),
			})
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	leader := srv.URL[len("http://"):]

	clientOpts := ClientOpts{Client: srv.Client(), Leader: leader}
	// Call syncLocalEntriesWithTracking directly with LeaderVV populated to
	// drive the inner gate's VV-aware decision.
	err = syncLocalEntriesWithTracking(clientOpts, SyncOptions{
		Key:        Key{Path: path, Database: localDB},
		Originator: "127.0.0.1:9000",
		VVManager:  vvm,
		LeaderVV:   leaderVV,
	})
	require.NoError(t, err)

	got, err := localDB.Get(path)
	require.NoError(t, err)
	require.JSONEq(t, string(localBody), string(got.Data),
		"local must not regress to leader's older record under VVGreater")
	require.Equal(t, now, got.Updated,
		"local Updated must not be overwritten by leader's future-stamped value")
}

// TestInnerGate_VVLessAcceptsLeaderWriteEvenWhenLocalUpdatedIsHigher is
// the inverse correctness check. Local's VV is strictly behind leader's
// (VVLess) so we genuinely need to pull. But local's Updated happens to
// be HIGHER than leader's — the legacy timestamp gate would refuse the
// write and leave local diverged from the cluster.
//
// The VV-aware gate correctly proceeds because VVLess says leader has
// causal state local lacks.
func TestInnerGate_VVLessAcceptsLeaderWriteEvenWhenLocalUpdatedIsHigher(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	const path = "things/x"
	now := time.Now().UTC().UnixNano()
	// Local Updated is "in the past" relative to wall clock — but higher
	// than what the leader will report — to force the legacy timestamp
	// gate to reject the leader's write.
	localUpdated := now
	leaderUpdated := now - int64(1*time.Hour)

	_, err := localDB.SetWithMeta(path, json.RawMessage(`{"v":"local-stale"}`), localUpdated, localUpdated)
	require.NoError(t, err)

	leaderVV := VersionVector{"leader": 10} // strictly greater than local
	localVV := VersionVector{"leader": 3}
	vvm := NewVVManager(localDB, "127.0.0.1:9000")
	vvm.set("things/x", localVV)

	leaderBody := json.RawMessage(`{"v":"leader-canonical"}`)
	mux := http.NewServeMux()
	mux.HandleFunc("/_pivot/pivot/"+path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(meta.Object{
				Path:    path,
				Index:   "x",
				Created: now - int64(2*time.Hour),
				Updated: leaderUpdated,
				Data:    leaderBody,
			})
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	leader := srv.URL[len("http://"):]

	clientOpts := ClientOpts{Client: srv.Client(), Leader: leader}
	err = syncLocalEntriesWithTracking(clientOpts, SyncOptions{
		Key:        Key{Path: path, Database: localDB},
		Originator: "127.0.0.1:9000",
		VVManager:  vvm,
		LeaderVV:   leaderVV,
	})
	require.NoError(t, err)

	got, err := localDB.Get(path)
	require.NoError(t, err)
	require.JSONEq(t, string(leaderBody), string(got.Data),
		"VVLess must accept the leader's write even when local Updated > leader Updated")
}
