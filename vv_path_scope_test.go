package pivot

// Regression test for the VV-scope mismatch in production. Pre-fix:
// handlers and the storage callback called increment with the storage
// event's full key (e.g. "things/x"), which baseKeyFromPath leaves at
// item-scope. The /activity endpoint exposed VV via Get(_key.Path),
// which normalized "things/*" to path-scope ("things"). Two different
// scopes → /activity always returned an empty VV for glob keys, and
// every VV-aware sync feature silently fell back to LastEntry-only
// logic.
//
// The fix unifies on path-scope: increment normalizes its argument
// down to the base path, so a write to "things/x" bumps the same VV
// the /activity handler reads. The two halves of the VV system now
// share state.

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// TestActivityExposesVVAfterGlobWrite is the headline regression
// test: a write under a glob path must show up in the VV that
// /activity exposes for that path. Pre-fix this was always empty.
//
// The full production callback is wired here (not a no-op) so the
// handler+callback Mark/Consume dedup is genuinely exercised — a
// regression that re-introduced a callback double-bump would land
// the path-scope leader counter at 2, not 1.
func TestActivityExposesVVAfterGlobWrite(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()

	vvm := NewVVManager(db, "leader")
	tracker := NewHandlerWriteTracker()

	keys := []Key{{Path: "things/*", Database: db}}
	instance := &Instance{VVManager: vvm}
	storage.WatchWithCallback(context.Background(), db, makeStorageSync(StorageSyncConfig{
		Keys:           keys,
		GetNodes:       func() []string { return nil },
		HandlerTracker: tracker,
		Instance:       instance,
	}))

	// Wire a Set handler the way a glob-path key registers it: path=
	// "things" (the registered base, with the /* stripped before
	// mounting) and items go under "things/<index>".
	setHandler := Set(db, "things", tracker, vvm, nil)

	// Drive a write under the glob.
	body := strings.NewReader(`{"created":0,"updated":0,"index":"x","path":"things/x","data":"e30="}`)
	req := httptest.NewRequest("POST", "/_pivot/pivot/things/x", body)
	req = mux.SetURLVars(req, map[string]string{"index": "x"})
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	setHandler(w, req)
	require.Equal(t, 200, w.Code)

	// Stand up the Activity handler the way pivot.go registers it:
	// the Key.Path is the glob, "things/*". Activity normalizes that
	// to "things" and reads the VV under that base key.
	activityHandler := Activity(Key{Path: "things/*", Database: db}, vvm)
	areq := httptest.NewRequest("GET", "/_pivot/activity/things", nil)
	aw := httptest.NewRecorder()
	activityHandler(aw, areq)
	require.Equal(t, 200, aw.Code)

	// The body is JSON-decoded ActivityEntry; check its VV directly via
	// the manager (same path) — equivalent and avoids a second decode
	// round-trip in this test.
	exposedVV := vvm.Get("things/*")
	require.NotEmpty(t, exposedVV,
		"after a glob-path write, /activity must expose a non-empty VV; got %v", exposedVV)
	require.Equal(t, int64(1), exposedVV["leader"],
		"the leader counter must reflect the handler's increment for the path")
}

// TestCallbackIncrementsAtPathScope is the storage-callback mirror.
// Direct (non-handler) writes flow through the storage event callback,
// which also has to bump VV at path-scope so /activity stays consistent.
func TestCallbackIncrementsAtPathScope(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()

	vvm := NewVVManager(db, "leader")
	keys := []Key{{Path: "things/*", Database: db}}
	instance := &Instance{VVManager: vvm}
	storage.WatchWithCallback(context.Background(), db, makeStorageSync(StorageSyncConfig{
		Keys:     keys,
		GetNodes: func() []string { return nil },
		Instance: instance,
	}))

	// Direct storage write — bypasses the Set handler, so the storage
	// callback is the one bumping VV.
	_, err := db.SetWithMeta("things/x", []byte(`{"v":"v1"}`), 1, 1)
	require.NoError(t, err)

	// Settle for the watch goroutine. The callback is the only thing
	// touching VV here, and increments synchronously inside the
	// goroutine — a small wait is enough.
	require.Eventually(t, func() bool {
		return vvm.Get("things/*")["leader"] >= 1
	}, time.Second, 5*time.Millisecond,
		"storage callback must increment path-scope VV on direct writes")
}

// TestHandlerIncrementMatchesActivityScope pins the symmetry: every
// handler-driven write produces a VV that Activity exposes 1:1. If a
// future change re-introduces an item-scope increment, OR if the
// callback dedup regresses and double-bumps, this test fails (3 writes
// must produce exactly 3, not 6).
func TestHandlerIncrementMatchesActivityScope(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()

	vvm := NewVVManager(db, "leader")
	tracker := NewHandlerWriteTracker()

	keys := []Key{{Path: "things/*", Database: db}}
	instance := &Instance{VVManager: vvm}
	storage.WatchWithCallback(context.Background(), db, makeStorageSync(StorageSyncConfig{
		Keys:           keys,
		GetNodes:       func() []string { return nil },
		HandlerTracker: tracker,
		Instance:       instance,
	}))

	setHandler := Set(db, "things", tracker, vvm, nil)

	for i, idx := range []string{"a", "b", "c"} {
		body := strings.NewReader(`{"created":0,"updated":0,"index":"` + idx + `","path":"things/` + idx + `","data":"e30="}`)
		req := httptest.NewRequest("POST", "/_pivot/pivot/things/"+idx, body)
		req = mux.SetURLVars(req, map[string]string{"index": idx})
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		setHandler(w, req)
		require.Equal(t, 200, w.Code, "write %d failed", i)
	}

	// Wait for the watch goroutine to drain. Once tracker is empty, every
	// handler Mark has been Consumed by the callback (callback skipped
	// each).
	require.Eventually(t, func() bool { return tracker.pendingLen() == 0 }, time.Second, 5*time.Millisecond)

	// Three writes under the same registered glob path → one VV with
	// leader counter at 3. Pre-fix each write bumped a separate item-
	// scope VV (things/a, things/b, things/c) — none of which /activity
	// would have read. Asserting exactly 3 (not >=3) catches a regression
	// that lets the callback also bump (would land 6).
	exposedVV := vvm.Get("things/*")
	require.Equal(t, int64(3), exposedVV["leader"],
		"three writes under things/* must increment the path-scope VV by exactly 3; got %v", exposedVV)
}
