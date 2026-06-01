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
	"sync"
	"testing"
	"time"

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

	_, err := sendToLeader(clientOpts, "things/x", obj, "10.0.0.1:9000", VersionVector{"10.0.0.1:9000": 5, "leader": 2})
	require.NoError(t, err)
	require.NotEmpty(t, receivedHeader, "sendToLeader must set X-Pivot-VV when given a non-empty vv")
	require.JSONEq(t, `{"10.0.0.1:9000":5,"leader":2}`, receivedHeader)
}

// TestQueuedOpCarriesQueueTimeVV pins that VV captured at queue time
// travels with the obj when the queue eventually drains. Without
// queue-time capture (i.e. if drainQueue read s.vvManager fresh per
// op), a drain that ran after intervening local bumps would attach a
// uniform "current" VV to every queued op — receiver's merge would
// then see the same VV on the second op as on the first, and any
// future idempotency consumer downstream would mistakenly skip the
// later write.
//
// Setup: queue two ops with different VV snapshots, drain, verify
// the leader received them in order with the per-op VV.
func TestQueuedOpCarriesQueueTimeVV(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()

	// Capture the VV header from every inbound POST.
	type recv struct {
		key string
		vv  string
	}
	var (
		mu       struct{ sync.Mutex }
		received []recv
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		received = append(received, recv{key: r.URL.Path, vv: r.Header.Get(VVHeader)})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Build a syncer with an empty nodeAddr so writes are queued, not sent
	// immediately. We'll drain via SetNodeAddr at the end.
	vvm := NewVVManager(db, "10.0.0.1:9000")
	pool := newSyncerPool(srv.Client(), []Key{{Path: "things/*", Database: db}},
		srv.URL[len("http://"):], false, vvm)

	// Op #1: snapshot at VV {nodeA:1}.
	vvm.set("things/*", VersionVector{"10.0.0.1:9000": 1})
	pool.syncers[srv.URL[len("http://"):]].QueueOrSendSet("things/a",
		meta.Object{Created: 1, Updated: 1, Index: "a", Path: "things/a", Data: []byte(`{}`)})

	// Op #2: VV bumps to {nodeA:2} between the two writes.
	vvm.set("things/*", VersionVector{"10.0.0.1:9000": 2})
	pool.syncers[srv.URL[len("http://"):]].QueueOrSendSet("things/b",
		meta.Object{Created: 2, Updated: 2, Index: "b", Path: "things/b", Data: []byte(`{}`)})

	// Drain by setting the node addr — the queue runs through sendToLeader
	// with op.vv as the header value.
	pool.SetNodeAddr("10.0.0.1:9000")

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == 2
	}, time.Second, 5*time.Millisecond, "queue never drained both ops")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, received, 2)
	require.Contains(t, received[0].key, "things/a")
	require.JSONEq(t, `{"10.0.0.1:9000":1}`, received[0].vv,
		"first op must carry the queue-time VV {nodeA:1}, not the later snapshot")
	require.Contains(t, received[1].key, "things/b")
	require.JSONEq(t, `{"10.0.0.1:9000":2}`, received[1].vv,
		"second op must carry the queue-time VV {nodeA:2}")
}
