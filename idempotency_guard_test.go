package pivot

// Regression tests for the VV-driven idempotency guard on the Set and
// Delete handlers (closes #44). Without it, a retried Trigger or a
// delayed coalescer drainer can deliver an inbound write whose VV is
// dominated by what pivot already holds — the older write would
// otherwise clobber a newer locally-pivoted one.
//
// Prerequisites (now in place via #46 + #47): VV is scoped consistently
// at path scope, and pivot merges peer counters from the X-Pivot-VV
// header into local VV. With those two foundations, localVV.Compare(
// inboundVV) produces meaningful Greater/Less/Equal/Concurrent results.
//
// Guard policy:
//   - VVGreater (local strictly dominates): skip — inbound is stale.
//   - VVEqual (identical): skip — exact retry of an already-applied write.
//   - VVLess: proceed — inbound has strictly newer info.
//   - VVConcurrent: proceed — both sides have unique writes; merge will
//     integrate inbound's peer counters and let downstream sync resolve.
//   - Missing/empty header (older peer): proceed — backward compat.

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// TestSetGuardSkipsStaleRetry pins the canonical clobber-prevention
// case: pivot has bumped its leader counter past the inbound's view
// (operator wrote locally after the originator's last sync), the
// inbound's VV is strictly dominated, the handler must skip the write
// AND keep the newer local data.
func TestSetGuardSkipsStaleRetry(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()
	storage.WatchWithCallback(db, func(storage.Event) {})

	// Seed local with the newer "operator" value. Local VV reflects a
	// prior synced write from node A (A:2) and the operator's local
	// bump (leader:5). Inbound's view is {leader:4, A:2} — A had
	// observed pivot at leader:4 before the operator's bump.
	_, err := db.SetWithMeta("things/x", []byte(`{"v":"newer"}`), 100, 100)
	require.NoError(t, err)

	vvm := NewVVManager(db, "leader")
	vvm.set("things/*", VersionVector{"leader": 5, "10.0.0.1:9000": 2})
	handler := Set(db, "things", NewHandlerWriteTracker(), vvm, nil)

	staleObj := meta.Object{Created: 50, Updated: 50, Index: "x", Path: "things/x", Data: []byte(`{"v":"stale"}`)}
	staleBody, err := json.Marshal(staleObj)
	require.NoError(t, err)

	req := httptest.NewRequest("POST", "/_pivot/pivot/things/x", bytes.NewReader(staleBody))
	req = mux.SetURLVars(req, map[string]string{"index": "x"})
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(VVHeader, `{"10.0.0.1:9000":2,"leader":4}`) // strictly dominated
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, 200, w.Code, "stale retry must be idempotent (200)")

	got, err := db.Get("things/x")
	require.NoError(t, err)
	require.JSONEq(t, `{"v":"newer"}`, string(got.Data),
		"VVGreater must NOT clobber the newer local value")
}

// TestSetGuardSkipsExactRetry pins the no-double-bump invariant: an
// identical-VV duplicate of an already-applied write must not bump the
// local leader counter a second time.
func TestSetGuardSkipsExactRetry(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()
	storage.WatchWithCallback(db, func(storage.Event) {})

	_, err := db.SetWithMeta("things/x", []byte(`{"v":"v1"}`), 100, 100)
	require.NoError(t, err)

	vvm := NewVVManager(db, "leader")
	vvm.set("things/*", VersionVector{"10.0.0.1:9000": 1})
	handler := Set(db, "things", NewHandlerWriteTracker(), vvm, nil)

	obj := meta.Object{Created: 100, Updated: 100, Index: "x", Path: "things/x", Data: []byte(`{"v":"v1"}`)}
	body, err := json.Marshal(obj)
	require.NoError(t, err)

	req := httptest.NewRequest("POST", "/_pivot/pivot/things/x", bytes.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"index": "x"})
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(VVHeader, `{"10.0.0.1:9000":1}`)
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, 200, w.Code)

	require.Equal(t, int64(0), vvm.Get("things")["leader"],
		"VVEqual must skip — leader counter must not advance from a duplicate")
	require.Equal(t, int64(1), vvm.Get("things")["10.0.0.1:9000"],
		"peer counter from the seed must be preserved (no merge on skip)")
}

// TestSetGuardAcceptsHigherVV: inbound strictly dominates local
// (VVLess from local's perspective). Proceeds normally.
func TestSetGuardAcceptsHigherVV(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()
	storage.WatchWithCallback(db, func(storage.Event) {})

	_, err := db.SetWithMeta("things/x", []byte(`{"v":"older"}`), 100, 100)
	require.NoError(t, err)

	vvm := NewVVManager(db, "leader")
	vvm.set("things/*", VersionVector{"10.0.0.1:9000": 1})
	handler := Set(db, "things", NewHandlerWriteTracker(), vvm, nil)

	newerObj := meta.Object{Created: 200, Updated: 200, Index: "x", Path: "things/x", Data: []byte(`{"v":"newer"}`)}
	body, err := json.Marshal(newerObj)
	require.NoError(t, err)

	req := httptest.NewRequest("POST", "/_pivot/pivot/things/x", bytes.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"index": "x"})
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(VVHeader, `{"10.0.0.1:9000":2}`) // strictly dominates local
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, 200, w.Code)

	got, err := db.Get("things/x")
	require.NoError(t, err)
	require.JSONEq(t, `{"v":"newer"}`, string(got.Data),
		"VVLess must let the inbound write replace local")
}

// TestSetGuardProceedsWithoutHeader pins backward compat: an older
// peer that doesn't emit the header must still be accepted.
func TestSetGuardProceedsWithoutHeader(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()
	storage.WatchWithCallback(db, func(storage.Event) {})

	vvm := NewVVManager(db, "leader")
	// Seed local VV that *would* dominate if compared.
	vvm.set("things/*", VersionVector{"leader": 5})
	handler := Set(db, "things", NewHandlerWriteTracker(), vvm, nil)

	obj := meta.Object{Created: 100, Updated: 100, Index: "x", Path: "things/x", Data: []byte(`{"v":"from-old-peer"}`)}
	body, err := json.Marshal(obj)
	require.NoError(t, err)

	req := httptest.NewRequest("POST", "/_pivot/pivot/things/x", bytes.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"index": "x"})
	req.Header.Set("Content-Type", "application/json")
	// No VVHeader.
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, 200, w.Code)

	got, err := db.Get("things/x")
	require.NoError(t, err)
	require.JSONEq(t, `{"v":"from-old-peer"}`, string(got.Data),
		"missing header must fall back to existing accept-all behavior")
}

// TestSetGuardPreservesClockDriftScenario is the canary: the canonical
// clock-drift scenario — node bumped its counter to N+1 after a clock
// correction, sending a write with an OLDER timestamp but a HIGHER VV
// counter — must still be accepted. Without VV-driven dispatch, the
// timestamp-only check would reject it (the original failure mode).
func TestSetGuardPreservesClockDriftScenario(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()
	storage.WatchWithCallback(db, func(storage.Event) {})

	// Local has the future-timestamped write at A:1. Pivot's local VV
	// reflects {A:1}.
	_, err := db.SetWithMeta("things/x", []byte(`{"phase":"future"}`), 999, 999)
	require.NoError(t, err)

	vvm := NewVVManager(db, "leader")
	vvm.set("things/*", VersionVector{"10.0.0.1:9000": 1})
	handler := Set(db, "things", NewHandlerWriteTracker(), vvm, nil)

	// Node sends with corrected (older) timestamp but bumped VV {A:2}.
	correctedObj := meta.Object{Created: 100, Updated: 100, Index: "x", Path: "things/x", Data: []byte(`{"phase":"corrected"}`)}
	body, err := json.Marshal(correctedObj)
	require.NoError(t, err)

	req := httptest.NewRequest("POST", "/_pivot/pivot/things/x", bytes.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"index": "x"})
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(VVHeader, `{"10.0.0.1:9000":2}`)
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, 200, w.Code)

	got, err := db.Get("things/x")
	require.NoError(t, err)
	require.JSONEq(t, `{"phase":"corrected"}`, string(got.Data),
		"VVLess (higher counter, older timestamp) must be accepted — this is the clock-drift recovery path")
}

// TestDeleteGuardSkipsStaleTombstone mirrors the Set test for Delete.
func TestDeleteGuardSkipsStaleTombstone(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()
	storage.WatchWithCallback(db, func(storage.Event) {})

	_, err := db.SetWithMeta("things/x", []byte(`{"v":"newer"}`), 100, 100)
	require.NoError(t, err)

	vvm := NewVVManager(db, "leader")
	vvm.set("things/*", VersionVector{"leader": 5, "10.0.0.1:9000": 2})
	handler := Delete(db, "things", NewHandlerWriteTracker(), vvm, nil)

	deleteTS := strconv.FormatInt(50, 10)
	req := httptest.NewRequest("DELETE", "/_pivot/pivot/things/x/"+deleteTS, nil)
	req = mux.SetURLVars(req, map[string]string{"index": "x", "time": deleteTS})
	req.Header.Set(VVHeader, `{"10.0.0.1:9000":2,"leader":4}`) // strictly dominated
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, 200, w.Code)

	got, err := db.Get("things/x")
	require.NoError(t, err, "VVGreater Delete must NOT remove the newer local item")
	require.JSONEq(t, `{"v":"newer"}`, string(got.Data))
}
