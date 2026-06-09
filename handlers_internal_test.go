package pivot

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// watchProcessed wraps a storage sync callback so wg.Done() fires after the
// watch goroutine finishes processing each event whose key matches glob. It
// lets these handler+callback tests wait deterministically for the async
// callback to drain the writes they care about — the watch goroutine is what
// could double-bump the VV, so observing its completion is the right signal —
// without sleeps or polling internal tracker counters. Bumps that persist the
// VV write to StoragePrefix keys don't match the data glob, so they don't
// inflate the count.
func watchProcessed(db storage.Database, cb StorageSyncCallback, glob string, wg *sync.WaitGroup) {
	storage.WatchWithCallback(db, func(e storage.Event) {
		cb(e)
		if key.Match(glob, e.Key) {
			wg.Done()
		}
	})
}

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
	var processed sync.WaitGroup
	watchProcessed(db, makeStorageSync(StorageSyncConfig{
		Keys:           keys,
		GetNodes:       func() []string { return nil },
		HandlerTracker: tracker,
		Instance:       instance,
	}), "things/*", &processed)

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

	// Wait for the watch goroutine to process each write before reading the
	// counter: it is the path that could double-bump, so its completion (not a
	// sleep) is the deterministic signal that the count has settled.
	processed.Add(1)
	doSet("abc")
	processed.Wait()
	require.Equal(t, int64(1), vvm.Get("things")["leader"], "first Set must bump leader counter to exactly 1")

	processed.Add(1)
	doSet("abc")
	processed.Wait()
	require.Equal(t, int64(2), vvm.Get("things")["leader"], "second Set must bump leader counter to exactly 2")
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
	var processed sync.WaitGroup
	watchProcessed(db, makeStorageSync(StorageSyncConfig{
		Keys:             keys,
		ConfigClusterURL: "127.0.0.1:8000", // non-empty -> node mode
		GetNodes:         func() []string { return nil },
		HandlerTracker:   tracker,
		Instance:         instance,
	}), "things/*", &processed)

	handler := Set(db, "things", tracker, vvm, nil)
	body := strings.NewReader(`{"created":0,"updated":0,"index":"abc","path":"things/abc","data":"e30="}`)
	req := httptest.NewRequest("POST", "/_pivot/pivot/things/abc", body)
	req = mux.SetURLVars(req, map[string]string{"index": "abc"})
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	processed.Add(1)
	handler(w, req)
	require.Equal(t, 200, w.Code)
	processed.Wait() // watch goroutine processed the write (no second bump)

	got := vvm.Get("things")
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
	var processed sync.WaitGroup
	watchProcessed(db, makeStorageSync(StorageSyncConfig{
		Keys:           keys,
		GetNodes:       func() []string { return nil },
		HandlerTracker: tracker,
		Instance:       instance,
	}), "things/*", &processed)

	handler := Set(db, "things", tracker, vvm, nil)

	const burst = 10
	processed.Add(burst)
	for range burst {
		body := strings.NewReader(`{"created":0,"updated":0,"index":"abc","path":"things/abc","data":"e30="}`)
		req := httptest.NewRequest("POST", "/_pivot/pivot/things/abc", body)
		req = mux.SetURLVars(req, map[string]string{"index": "abc"})
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handler(w, req)
		require.Equal(t, 200, w.Code)
	}

	// Wait for the watch goroutine to process all burst events (each Consumes
	// one Mark). Its completion is the deterministic signal that every event
	// drained — no polling of internal tracker state.
	processed.Wait()

	got := vvm.Get("things")["leader"]
	require.Equal(t, int64(burst), got,
		"%d back-to-back HTTP Sets must bump leader counter exactly %d times; got %d means the per-key dedup collapsed under burst",
		burst, burst, got)
}

// TestDeleteDoesNotLeakHandlerMarks pins the invariant that a handler-
// driven Delete leaves the tracker empty after both storage events drain.
// Pre-fix the handler Marked the tombstone key (StoragePrefix+path) too,
// but the tombstone event doesn't match any configured Key.Path glob so
// the storage callback returned at !found before reaching Consume — the
// tombstone Mark accumulated forever.
func TestDeleteDoesNotLeakHandlerMarks(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()

	tracker := NewHandlerWriteTracker()
	vvm := NewVVManager(db, "leader")
	keys := []Key{{Path: "things/*", Database: db}}
	instance := &Instance{VVManager: vvm}
	var processed sync.WaitGroup
	watchProcessed(db, makeStorageSync(StorageSyncConfig{
		Keys:           keys,
		GetNodes:       func() []string { return nil },
		HandlerTracker: tracker,
		Instance:       instance,
	}), "things/*", &processed)

	// Seed a few items so each Delete actually has something to remove.
	// Seeding goes through db.SetWithMeta directly (no Mark), so each event
	// flows through the callback's empty-tracker branch and bumps VV once at
	// path scope. Wait for all seed events to be processed (deterministic).
	indices := []string{"a", "b", "c"}
	processed.Add(len(indices))
	for _, idx := range indices {
		nowUnix := time.Now().UTC().UnixNano()
		obj := meta.Object{Created: nowUnix, Updated: nowUnix, Index: idx, Path: "things/" + idx, Data: []byte(`{"v":1}`)}
		body, err := json.Marshal(obj)
		require.NoError(t, err)
		_, err = db.SetWithMeta("things/"+idx, body, nowUnix, nowUnix)
		require.NoError(t, err)
	}
	processed.Wait()
	require.Equal(t, int64(len(indices)), vvm.Get("things")["leader"], "each seed must bump path-scope VV exactly once")

	handler := Delete(db, "things", tracker, vvm, nil)
	processed.Add(len(indices))
	for _, idx := range indices {
		ts := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
		req := httptest.NewRequest("DELETE", "/_pivot/pivot/things/"+idx+"/"+ts, nil)
		req = mux.SetURLVars(req, map[string]string{"index": idx, "time": ts})
		w := httptest.NewRecorder()
		handler(w, req)
		require.Equal(t, 200, w.Code)
	}
	processed.Wait()

	// No leaked handler marks. Post-fix the Delete handler Marks only the item
	// key (Consumed by the item's del event the watch goroutine just processed),
	// never the tombstone key. Asserted via the public Consume contract, not an
	// internal length: a leaked tombstone Mark would make Consume return true,
	// and any unconsumed item Mark likewise.
	require.False(t, tracker.Consume(StoragePrefix+"things"), "Delete must not leak a tombstone-key handler mark")
	for _, idx := range indices {
		require.False(t, tracker.Consume("things/"+idx), "Delete item Mark must have been consumed by the watch goroutine")
	}
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
		// VV is bumped at path scope ("things"), not itemKey scope —
		// see the increment in the Set handler.
		observedAt = vvm.Get("things")["leader"]
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
	vv := vvm.Get("things")
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
	vv := vvm.Get("things")
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

// TestHandlerWriteTracker_DualCounterConsumeSemantics pins that Mark
// sets BOTH the fanout-skip (pending) and bump-skip (bumpPending)
// counters, and each is consumed by exactly one consumer without
// depriving the other. The earlier Has() peek was non-consuming, so a
// stale mark could silently swallow a later direct write's VV bump when
// the watch goroutine never ran to drain it.
func TestHandlerWriteTracker_DualCounterConsumeSemantics(t *testing.T) {
	tr := NewHandlerWriteTracker()
	const k = "things/abc"

	// Mark sets both the bump-skip and fanout-skip counters. Assert the contract
	// through the public consume methods (not an internal length): each counter
	// is consumable exactly once, and consuming one does not deprive the other.
	tr.Mark(k)

	// The bump-skip consumer drains its own counter; consuming, not peeking, so a
	// second consume returns false — the property that stops a stale mark from
	// swallowing a later direct write's bump.
	require.True(t, tr.ConsumeBumpSkip(k), "first ConsumeBumpSkip sees the mark")
	require.False(t, tr.ConsumeBumpSkip(k), "second ConsumeBumpSkip must return false (consumed, not peeked)")

	// The fanout-skip mark is still present — the two counters are independent,
	// so consuming bump-skip above did not drain it.
	require.True(t, tr.Consume(k), "Consume still sees the fanout-skip mark after the bump-skip consume")
	require.False(t, tr.Consume(k), "second Consume must return false")
}

// TestHandlerWriteTracker_UnmarkClearsBothCounters pins that the error
// path (handler bails after Mark but before the storage write fires an
// event) drains BOTH counters — otherwise a leaked bump-skip mark would
// swallow a later direct write's VV bump for the same key.
func TestHandlerWriteTracker_UnmarkClearsBothCounters(t *testing.T) {
	tr := NewHandlerWriteTracker()
	const k = "things/abc"

	tr.Mark(k)
	tr.Unmark(k)

	// After Unmark, neither consumer sees a mark — both counters were cleared,
	// so a subsequent direct write to the same key runs its full path. Asserted
	// through the public consume contract rather than an internal length.
	require.False(t, tr.ConsumeBumpSkip(k), "no bump-skip mark after Unmark")
	require.False(t, tr.Consume(k), "no fanout-skip mark after Unmark")
}
