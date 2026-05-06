package pivot

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// failingTombstoneStorage forces Set on the pivot tombstone prefix to fail.
// VV writes (StoragePrefix+"vv/") are also under StoragePrefix, but the
// Delete handler's tombstone target is "pivot/<basekey>" exactly, so the
// match below is precise and won't perturb other writes the test makes.
type failingTombstoneStorage struct {
	storage.Database
	tombstoneKey string
	err          error
}

func (f *failingTombstoneStorage) Set(key string, data json.RawMessage) (string, error) {
	if key == f.tombstoneKey {
		return key, f.err
	}
	return f.Database.Set(key, data)
}

// TestDeleteTombstoneAtomicity pins the invariant that a tombstone-write
// failure must not leave the item physically deleted with no record of the
// delete. The original ordering (db.Del → db.Set tombstone) was vulnerable:
// if the tombstone Set failed (or the process was cancelled between the two
// writes), the item was gone but no tombstone existed, so a subsequent sync
// round would re-fetch from a node that hadn't observed the delete and the
// item would silently resurrect.
//
// The fix writes the tombstone first; on Set error we bail before the Del.
// "Item gone + no tombstone" is unreachable via this handler. An orphan
// tombstone (Del fails after tombstone written) is recoverable by sync —
// the item gets deleted everywhere via the recorded ts — so it is not the
// state we guard against here.
func TestDeleteTombstoneAtomicity(t *testing.T) {
	monotonic.Init()
	real := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, real.Start(storage.Options{}))
	defer real.Close()
	storage.WatchWithCallback(real, func(storage.Event) {})

	itemKey := "things/abc"
	tombstoneKey := StoragePrefix + "things"

	// Seed the item so a successful Del would actually remove something.
	nowUnix := time.Now().UTC().UnixNano()
	obj := meta.Object{Created: nowUnix, Updated: nowUnix, Index: "abc", Path: itemKey, Data: []byte(`{"v":1}`)}
	body, err := json.Marshal(obj)
	require.NoError(t, err)
	_, err = real.SetWithMeta(itemKey, body, nowUnix, nowUnix)
	require.NoError(t, err)

	// Confirm seed.
	_, err = real.Get(itemKey)
	require.NoError(t, err, "seed precondition: item must be present before Delete handler runs")

	failing := &failingTombstoneStorage{
		Database:     real,
		tombstoneKey: tombstoneKey,
		err:          errors.New("simulated tombstone write rejection"),
	}

	handler := Delete(failing, "things", nil, nil, nil)

	deleteTS := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	req := httptest.NewRequest("DELETE", "/_pivot/pivot/things/abc/"+deleteTS, nil)
	req = mux.SetURLVars(req, map[string]string{"index": "abc", "time": deleteTS})
	w := httptest.NewRecorder()

	handler(w, req)

	_, getErr := real.Get(itemKey)
	itemPresent := getErr == nil

	tombObj, tombErr := real.Get(tombstoneKey)
	tombstonePresent := tombErr == nil && len(tombObj.Data) > 0 && string(tombObj.Data) != "null"

	if !itemPresent && !tombstonePresent {
		t.Fatalf("delete left item gone with no tombstone — sync round will resurrect it; status=%d itemErr=%v tombErr=%v",
			w.Code, getErr, tombErr)
	}
	// Bonus: when the tombstone write fails we should also surface that to the
	// client rather than reporting success.
	require.NotEqual(t, 200, w.Code, "tombstone write failed but handler returned 200; client cannot retry an error it never saw")
}

// TestSetVVIncrementsExactlyOnce pins the invariant that a single HTTP Set
// bumps the leader counter exactly once. Before the fix, the handler bumped
// once before the storage write and the storage event callback bumped again
// after, so each Set landed +2. Counters were therefore 2× what every other
// peer thought they should be. Sync ordering still worked (strictly
// monotonic) but the inflated values muddled divergence detection and any
// future test that asserts an exact VV value.
//
// The fix moves the handler increment to *after* the storage write succeeds
// and removes the callback's increment entirely; the handler is the sole
// source of VV bumps for handler-driven writes.
func TestSetVVIncrementsExactlyOnce(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()

	tracker := NewHandlerWriteTracker()
	vvm := NewVVManager(db, "leader")
	keys := []Key{{Path: "things/*", Database: db}}
	instance := &Instance{VVManager: vvm}
	storage.WatchWithCallback(db, makeStorageSync(StorageSyncConfig{
		Keys:           keys,
		GetNodes:       func() []string { return nil },
		HandlerTracker: tracker,
		Instance:       instance,
	}))

	handler := Set(db, "things", tracker, vvm, nil)

	doSet := func(idx string) {
		body := strings.NewReader(`{"created":0,"updated":0,"index":"` + idx + `","path":"things/` + idx + `","data":"e30="}`)
		req := httptest.NewRequest("POST", "/_pivot/pivot/things/"+idx, body)
		req = mux.SetURLVars(req, map[string]string{"index": idx})
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler(w, req)
		require.Equal(t, 200, w.Code)
	}

	doSet("abc")
	// Settle: callback runs in the watch goroutine, so we need to let any
	// stray increment race past the handler's own Get before reading.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(1), vvm.Get("things/abc")["leader"], "first Set must bump leader counter to exactly 1")

	doSet("abc")
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(2), vvm.Get("things/abc")["leader"], "second Set must bump leader counter to exactly 2")
}

// TestSetVVIncrementsExactlyOnceNodeRole is the node-side mirror of
// TestSetVVIncrementsExactlyOnce. Before the fix, node servers had no
// originator tracker, so the dedup signal was missing and HTTP Sets to a
// node still double-bumped via handler + callback. The fix creates an
// originator tracker on every server (not just pivots) so the dedup works
// for node-role keys too.
func TestSetVVIncrementsExactlyOnceNodeRole(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()

	tracker := NewHandlerWriteTracker()
	vvm := NewVVManager(db, "127.0.0.1:9999") // node-style nodeID

	keys := []Key{{Path: "things/*", Database: db}}
	instance := &Instance{VVManager: vvm}
	storage.WatchWithCallback(db, makeStorageSync(StorageSyncConfig{
		Keys:             keys,
		ConfigClusterURL: "127.0.0.1:8000", // non-empty -> node mode
		GetNodes:         func() []string { return nil },
		HandlerTracker:   tracker,
		Instance:         instance,
	}))

	handler := Set(db, "things", tracker, vvm, nil)
	body := strings.NewReader(`{"created":0,"updated":0,"index":"abc","path":"things/abc","data":"e30="}`)
	req := httptest.NewRequest("POST", "/_pivot/pivot/things/abc", body)
	req = mux.SetURLVars(req, map[string]string{"index": "abc"})
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, 200, w.Code)
	time.Sleep(100 * time.Millisecond)

	got := vvm.Get("things/abc")
	require.Equal(t, int64(1), got["127.0.0.1:9999"],
		"node-role HTTP Set must bump local counter exactly once; got %d", got["127.0.0.1:9999"])
}

// TestSetVVIncrementsExactlyOnceUnderBurst pins the invariant that even
// when N rapid handler-driven Sets to the same key fire before the watch
// goroutine can drain any of them, the VV bumps exactly N times — not
// N + (number-of-events-the-callback-double-bumps-for).
//
// Pre-fix the tracker stored a single per-key entry (last-writer-wins),
// so K back-to-back handler Sets would collapse into one entry; the
// callback would Take it on the first event and then find the tracker
// empty for the remaining K-1 events and bump again. The fix uses a
// per-key counter so each handler's Mark is matched by exactly one
// Consume.
func TestSetVVIncrementsExactlyOnceUnderBurst(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()

	tracker := NewHandlerWriteTracker()
	vvm := NewVVManager(db, "leader")
	keys := []Key{{Path: "things/*", Database: db}}
	instance := &Instance{VVManager: vvm}
	storage.WatchWithCallback(db, makeStorageSync(StorageSyncConfig{
		Keys:           keys,
		GetNodes:       func() []string { return nil },
		HandlerTracker: tracker,
		Instance:       instance,
	}))

	handler := Set(db, "things", tracker, vvm, nil)

	const burst = 10
	for range burst {
		body := strings.NewReader(`{"created":0,"updated":0,"index":"abc","path":"things/abc","data":"e30="}`)
		req := httptest.NewRequest("POST", "/_pivot/pivot/things/abc", body)
		req = mux.SetURLVars(req, map[string]string{"index": "abc"})
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler(w, req)
		require.Equal(t, 200, w.Code)
	}

	// Generous settle: every event must drain through the watch goroutine
	// before we assert. If the callback double-bumped on any event, we'll
	// see a counter > burst here.
	time.Sleep(300 * time.Millisecond)

	got := vvm.Get("things/abc")["leader"]
	require.Equal(t, int64(burst), got,
		"%d back-to-back HTTP Sets must bump leader counter exactly %d times; got %d means the per-key dedup collapsed under burst",
		burst, burst, got)
}

// TestSetPostWriteSeesBumpedVV pins the in-handler ordering: the post-write
// hook runs after vvManager.increment, on the same goroutine. That's what
// makes the trigger fanout fire AFTER the bump, so a peer woken by the
// trigger reads a fresh VV.
//
// Note: this only pins the local sequence inside the handler. The actual
// cross-process race (peer GET /activity racing our increment) is closed
// by that local sequence — verifying it end-to-end would require two
// httptest servers and intercepting the trigger HTTP call, which the
// existing integration tests in cluster_test.go cover at a higher level.
func TestSetPostWriteSeesBumpedVV(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()
	storage.WatchWithCallback(db, func(storage.Event) {})

	tracker := NewHandlerWriteTracker()
	vvm := NewVVManager(db, "leader")

	var observedAt int64
	postWrite := func(itemKey, op, originatorPeer string) {
		observedAt = vvm.Get(itemKey)["leader"]
	}

	handler := Set(db, "things", tracker, vvm, postWrite)

	body := strings.NewReader(`{"created":0,"updated":0,"index":"abc","path":"things/abc","data":"e30="}`)
	req := httptest.NewRequest("POST", "/_pivot/pivot/things/abc", body)
	req = mux.SetURLVars(req, map[string]string{"index": "abc"})
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, 200, w.Code)

	require.Equal(t, int64(1), observedAt,
		"post-write callback must see the bumped VV; got %d means the trigger fired before the bump", observedAt)
}

// TestSetVVDoesNotBumpOnStorageFailure pins the rollback invariant: if the
// storage write fails, the VV must NOT be incremented. Otherwise peers
// comparing VVs would see pivot ahead of its own data store and pull stale
// values thinking they were current.
func TestSetVVDoesNotBumpOnStorageFailure(t *testing.T) {
	monotonic.Init()
	real := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, real.Start(storage.Options{}))
	defer real.Close()
	storage.WatchWithCallback(real, func(storage.Event) {})

	failing := &failingItemStorage{
		Database:    real,
		failItemKey: "things/abc",
		err:         errors.New("simulated storage rejection"),
	}

	vvm := NewVVManager(failing, "leader")
	tracker := NewHandlerWriteTracker()

	handler := Set(failing, "things", tracker, vvm, nil)

	body := strings.NewReader(`{"created":0,"updated":0,"index":"abc","path":"things/abc","data":"e30="}`)
	req := httptest.NewRequest("POST", "/_pivot/pivot/things/abc", body)
	req = mux.SetURLVars(req, map[string]string{"index": "abc"})
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler(w, req)

	require.Equal(t, 500, w.Code, "storage failure must surface as 500")
	vv := vvm.Get("things/abc")
	require.Equal(t, int64(0), vv["leader"],
		"VV must not bump when the storage write failed; got %d means VV is now ahead of storage", vv["leader"])
}

// failingItemStorage forces SetWithMeta on a specific item to fail while
// leaving every other Set/SetWithMeta path intact.
type failingItemStorage struct {
	storage.Database
	failItemKey string
	err         error
}

func (f *failingItemStorage) SetWithMeta(key string, data json.RawMessage, created, updated int64) (string, error) {
	if key == f.failItemKey {
		return key, f.err
	}
	return f.Database.SetWithMeta(key, data, created, updated)
}

// TestDeleteVVDoesNotBumpOnStorageFailure mirrors the Set rollback invariant
// for the Delete handler: a tombstone-write failure must not leave the VV
// incremented for a delete that never happened.
func TestDeleteVVDoesNotBumpOnStorageFailure(t *testing.T) {
	monotonic.Init()
	real := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, real.Start(storage.Options{}))
	defer real.Close()
	storage.WatchWithCallback(real, func(storage.Event) {})

	itemKey := "things/abc"
	tombstoneKey := StoragePrefix + "things"

	nowUnix := time.Now().UTC().UnixNano()
	obj := meta.Object{Created: nowUnix, Updated: nowUnix, Index: "abc", Path: itemKey, Data: []byte(`{"v":1}`)}
	body, err := json.Marshal(obj)
	require.NoError(t, err)
	_, err = real.SetWithMeta(itemKey, body, nowUnix, nowUnix)
	require.NoError(t, err)

	failing := &failingTombstoneStorage{
		Database:     real,
		tombstoneKey: tombstoneKey,
		err:          errors.New("simulated tombstone rejection"),
	}
	vvm := NewVVManager(failing, "leader")
	tracker := NewHandlerWriteTracker()

	handler := Delete(failing, "things", tracker, vvm, nil)
	deleteTS := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	req := httptest.NewRequest("DELETE", "/_pivot/pivot/things/abc/"+deleteTS, nil)
	req = mux.SetURLVars(req, map[string]string{"index": "abc", "time": deleteTS})
	w := httptest.NewRecorder()
	handler(w, req)

	require.Equal(t, 500, w.Code)
	vv := vvm.Get(itemKey)
	require.Equal(t, int64(0), vv["leader"],
		"VV must not bump when the Delete tombstone write failed; got %d means VV is ahead of storage", vv["leader"])
}

// TestDeleteHappyPathStillCommitsBoth guards against an over-eager fix that
// returns early in the success case. With healthy storage the handler must
// still remove the item and write the tombstone.
func TestDeleteHappyPathStillCommitsBoth(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()
	storage.WatchWithCallback(db, func(storage.Event) {})

	itemKey := "things/abc"
	tombstoneKey := StoragePrefix + "things"

	nowUnix := time.Now().UTC().UnixNano()
	obj := meta.Object{Created: nowUnix, Updated: nowUnix, Index: "abc", Path: itemKey, Data: []byte(`{"v":1}`)}
	body, err := json.Marshal(obj)
	require.NoError(t, err)
	_, err = db.SetWithMeta(itemKey, body, nowUnix, nowUnix)
	require.NoError(t, err)

	handler := Delete(db, "things", nil, nil, nil)
	deleteTS := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	req := httptest.NewRequest("DELETE", "/_pivot/pivot/things/abc/"+deleteTS, nil)
	req = mux.SetURLVars(req, map[string]string{"index": "abc", "time": deleteTS})
	w := httptest.NewRecorder()

	handler(w, req)

	require.Equal(t, 200, w.Code)
	_, getErr := db.Get(itemKey)
	require.Error(t, getErr, "item must be removed on the happy path")

	tomb, err := db.Get(tombstoneKey)
	require.NoError(t, err, "tombstone must be present on the happy path")
	require.Equal(t, deleteTS, strings.TrimSpace(string(tomb.Data)), "tombstone payload must be the delete timestamp")
}
