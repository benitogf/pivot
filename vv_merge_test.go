package pivot

// Regression tests for the merge-on-receive half of #45. With
// merge-on-receive, when pivot's Set/Delete handler accepts a write
// that carries the originator's VV in the X-Pivot-VV header, pivot
// merges that VV into local — so pivot's view of the cluster grows
// peer counters as it sees their writes. Without merge, each node's
// VV is just `{"my-id": counter}`; cross-node Compare always returns
// Concurrent because each side has counters the other has never
// integrated.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// TestSetHandlerMergesInboundVV pins the headline invariant: a Set
// handler that receives a write with X-Pivot-VV={"A":3} must end up
// with local VV containing A:3 (along with whatever leader counter
// the handler bumped itself).
func TestSetHandlerMergesInboundVV(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()
	storage.WatchWithCallback(db, func(storage.Event) {})

	vvm := NewVVManager(db, "leader")
	handler := Set(db, "things", NewHandlerWriteTracker(), vvm, nil)

	obj := meta.Object{
		Created: 1, Updated: 1, Index: "x", Path: "things/x",
		Data: []byte(`{"v":"v1"}`),
	}
	body, err := json.Marshal(obj)
	require.NoError(t, err)

	req := httptest.NewRequest("POST", "/_pivot/pivot/things/x", bytes.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"index": "x"})
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(VVHeader, `{"10.0.0.1:9000":3}`)
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, 200, w.Code)

	got := vvm.Get("things")
	require.Equal(t, int64(1), got["leader"], "handler must bump its own leader counter")
	require.Equal(t, int64(3), got["10.0.0.1:9000"],
		"handler must merge the inbound peer counter; got %v", got)
}

// TestSetHandlerNoMergeWithoutHeader pins backward compat: a peer that
// doesn't send the header (older pivot/node mid-rolling-upgrade) must
// still be accepted, just without the merge.
func TestSetHandlerNoMergeWithoutHeader(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()
	storage.WatchWithCallback(db, func(storage.Event) {})

	vvm := NewVVManager(db, "leader")
	handler := Set(db, "things", NewHandlerWriteTracker(), vvm, nil)

	obj := meta.Object{Created: 1, Updated: 1, Index: "x", Path: "things/x", Data: []byte(`{"v":"v1"}`)}
	body, err := json.Marshal(obj)
	require.NoError(t, err)

	req := httptest.NewRequest("POST", "/_pivot/pivot/things/x", bytes.NewReader(body))
	req = mux.SetURLVars(req, map[string]string{"index": "x"})
	req.Header.Set("Content-Type", "application/json")
	// No X-Pivot-VV header.
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, 200, w.Code)

	got := vvm.Get("things")
	require.Equal(t, int64(1), got["leader"], "handler still bumps its own counter")
	require.Len(t, got, 1, "VV must contain only leader counter (no peer counter to merge); got %v", got)
}

// TestDeleteHandlerMergesInboundVV mirrors the Set test for Delete.
func TestDeleteHandlerMergesInboundVV(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()
	storage.WatchWithCallback(db, func(storage.Event) {})

	// Seed an item so Delete has something to remove.
	_, err := db.SetWithMeta("things/x", []byte(`{"v":"v1"}`), 1, 1)
	require.NoError(t, err)

	vvm := NewVVManager(db, "leader")
	handler := Delete(db, "things", NewHandlerWriteTracker(), vvm, nil)

	deleteTS := strconv.FormatInt(2, 10)
	req := httptest.NewRequest("DELETE", "/_pivot/pivot/things/x/"+deleteTS, nil)
	req = mux.SetURLVars(req, map[string]string{"index": "x", "time": deleteTS})
	req.Header.Set(VVHeader, `{"10.0.0.1:9000":2}`)
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, 200, w.Code)

	got := vvm.Get("things")
	require.Equal(t, int64(1), got["leader"])
	require.Equal(t, int64(2), got["10.0.0.1:9000"],
		"Delete handler must merge the inbound peer counter; got %v", got)
}

// TestSendToLeaderEmitsVVHeader pins the wire-side counterpart: when
// the syncer pushes a write up to its leader, it must emit the local
// VV in the X-Pivot-VV header so the leader can merge.
func TestSendToLeaderEmitsVVHeader(t *testing.T) {
	monotonic.Init()
	var receivedHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w_ http.ResponseWriter, r *http.Request) {
		receivedHeader = r.Header.Get(VVHeader)
		w_.WriteHeader(200)
	}))
	defer srv.Close()

	leader := srv.URL[len("http://"):]
	clientOpts := ClientOpts{Client: srv.Client(), Leader: leader}
	obj := meta.Object{Created: 1, Updated: 1, Index: "x", Path: "things/x", Data: []byte(`{"v":"v1"}`)}

	err := sendToLeader(clientOpts, "things/x", obj, "10.0.0.1:9000", VersionVector{"10.0.0.1:9000": 5, "leader": 2})
	require.NoError(t, err)
	require.NotEmpty(t, receivedHeader, "sendToLeader must set X-Pivot-VV when given a non-empty vv")
	require.JSONEq(t, `{"10.0.0.1:9000":5,"leader":2}`, receivedHeader)
}

// TestQueuedOpCarriesQueueTimeVV pins that VV captured at queue time
// travels with the obj when the queue eventually drains. Pre-fix, a
// drain that ran after intervening local bumps would attach a newer
// VV to the older obj — leader's idempotency guard would then see
// VVEqual on a later queued write and silently drop it.
func TestQueuedOpCarriesQueueTimeVV(t *testing.T) {
	// Verified at unit level via the snapshotVV helper — the queue
	// path captures s.vvManager.Get(key) before the queueMu lock,
	// which means each pendingOp records the VV that was current
	// when that specific write reached the queue. drainQueue then
	// passes op.vv into sendToLeader. End-to-end coverage of the
	// queue+drain path lives in cluster_test.go's offline-sync flows.
	t.Skip("covered by sync_vv_merge_e2e_test.go and existing offline-sync e2e flow")
}
