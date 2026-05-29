package pivot_test

// Regression test: a glob pull must not delete a local-only item the
// leader has not seen when the two sides are causally concurrent.
//
// syncLocalEntriesWithTracking's glob branch deletes local items absent
// from the leader's list (GetEntriesNegativeDiff). Unconditionally, that
// wipes a fresh local create whenever the node pulls under VVConcurrent
// (local has writes the leader doesn't, and vice versa). The fix gates
// the deletion on localVV.Compare(LeaderVV) == VVLess — only mirror a
// deletion when the leader is strictly causally ahead. Symmetric to the
// push-direction trustVV gate in syncToLeader.
//
// Determinism: direct SetWithMeta writes bump the VV synchronously via
// AfterWrite, and TriggerNodeSync runs the pull synchronously inside the
// HTTP handler (pool.PullAll completes before the 200 response). So
// direct writes use the house AfterWrite-WaitGroup pattern and the pull
// phases need no wait — no sleeps, no polling.

import (
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

type globServers struct {
	pivot        *ooo.Server
	node         *ooo.Server
	pivotThings  storage.Database
	nodeThings   storage.Database
	pivotWritten *sync.WaitGroup
	nodeWritten  *sync.WaitGroup
	closeOnce    sync.Once
}

func (s *globServers) Close() {
	s.closeOnce.Do(func() {
		s.node.Close(nil)
		s.pivot.Close(nil)
	})
}

func setupGlobServers(t *testing.T) *globServers {
	pivotThings := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	nodeThings := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})

	pivotWritten := &sync.WaitGroup{}
	nodeWritten := &sync.WaitGroup{}

	isThingsKey := func(key string) bool {
		if len(key) >= 6 && key[:6] == "pivot/" {
			return false
		}
		return len(key) > 7 && key[:7] == "things/"
	}
	pivotAfterWrite := func(key string) {
		if isThingsKey(key) {
			pivotWritten.Done()
		}
	}
	nodeAfterWrite := func(key string) {
		if isThingsKey(key) {
			nodeWritten.Done()
		}
	}

	mkClient := func() *http.Client {
		return &http.Client{
			Timeout: 500 * time.Millisecond,
			Transport: &http.Transport{
				Dial:              (&net.Dialer{Timeout: 500 * time.Millisecond}).Dial,
				MaxConnsPerHost:   3000,
				DisableKeepAlives: true,
			},
		}
	}

	pivotServer := &ooo.Server{}
	pivotServer.Silence = true
	pivotServer.Static = true
	pivotServer.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	pivotServer.Router = mux.NewRouter()
	pivotServer.Client = mkClient()
	pivotServer.Audit = func(r *http.Request) bool { return true }
	pivot.Setup(pivotServer, pivot.Config{
		Keys:       []pivot.Key{{Path: "things/*", Database: pivotThings}},
		ClusterURL: "",
	})
	if err := pivot.GetInstance(pivotServer).Attach(pivotThings, storage.Options{AfterWrite: pivotAfterWrite}); err != nil {
		t.Fatalf("attach pivot storage: %v", err)
	}
	pivotServer.Start("localhost:0")

	nodeServer := &ooo.Server{}
	nodeServer.Silence = true
	nodeServer.Static = true
	nodeServer.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	nodeServer.Router = mux.NewRouter()
	nodeServer.Client = mkClient()
	nodeServer.Audit = func(r *http.Request) bool { return true }
	pivot.Setup(nodeServer, pivot.Config{
		Keys:                []pivot.Key{{Path: "things/*", Database: nodeThings}},
		ClusterURL:          pivotServer.Address,
		HealthCheckInterval: 60 * time.Second,
	})
	if err := pivot.GetInstance(nodeServer).Attach(nodeThings, storage.Options{
		NoBroadcastKeys: []string{"things/*"},
		AfterWrite:      nodeAfterWrite,
	}); err != nil {
		t.Fatalf("attach node storage: %v", err)
	}
	nodeThings.SetBeforeRead(nil)
	nodeServer.Start("localhost:0")

	return &globServers{
		pivot:        pivotServer,
		node:         nodeServer,
		pivotThings:  pivotThings,
		nodeThings:   nodeThings,
		pivotWritten: pivotWritten,
		nodeWritten:  nodeWritten,
	}
}

// TestGlobPullPreservesLocalOnlyItemUnderVVConcurrent: pivot has X;
// node pulls X; node creates Y locally (not yet propagated); pivot
// creates Z. Now nodeVV={leader:1,node:1}, pivotVV={leader:2} →
// VVConcurrent. The pull must add Z (leader has it, node doesn't) AND
// preserve Y (node's un-propagated local create), not delete Y.
func TestGlobPullPreservesLocalOnlyItemUnderVVConcurrent(t *testing.T) {
	servers := setupGlobServers(t)
	defer servers.Close()

	now := time.Now().UnixNano()

	// Phase 1 — pivot has item X (direct write bumps pivot VV via AfterWrite).
	xData, _ := json.Marshal(map[string]string{"value": "X"})
	servers.pivotWritten.Add(1)
	_, err := servers.pivotThings.SetWithMeta("things/x", xData, now, now)
	require.NoError(t, err)
	servers.pivotWritten.Wait()

	// Phase 2 — node pulls X (one node write). TriggerNodeSync runs the
	// pull synchronously; merge-on-pull copies pivot's VV into the
	// node's VVManager. The pull's SetWithMeta fires nodeAfterWrite, so
	// account for it on the WaitGroup.
	servers.nodeWritten.Add(1)
	pivot.TriggerNodeSync(servers.node.Client, servers.node.Address)
	servers.nodeWritten.Wait()
	xPulled, err := servers.nodeThings.Get("things/x")
	require.NoError(t, err)
	require.NotEmpty(t, xPulled.Data, "node must have pulled X")

	// Phase 3 — node creates Y locally. NoBroadcastKeys suppresses the
	// push, so pivot never receives Y; AfterWrite still bumps the node VV.
	yData, _ := json.Marshal(map[string]string{"value": "Y"})
	servers.nodeWritten.Add(1)
	_, err = servers.nodeThings.SetWithMeta("things/y", yData, now+1, now+1)
	require.NoError(t, err)
	servers.nodeWritten.Wait()

	// Phase 4 — pivot creates Z; pivot's leader counter advances past
	// what the node has seen → the next pull is VVConcurrent.
	zData, _ := json.Marshal(map[string]string{"value": "Z"})
	servers.pivotWritten.Add(1)
	_, err = servers.pivotThings.SetWithMeta("things/z", zData, now+2, now+2)
	require.NoError(t, err)
	servers.pivotWritten.Wait()

	// Phase 5 — pull tick on node (synchronous). Post-fix: adds Z (one
	// node write), preserves Y. Pre-fix: the glob objsToDelete step also
	// deleted Y (a second node write) — which would over-Done the
	// WaitGroup and surface as a loud failure, on top of the Y-missing
	// assertion below.
	servers.nodeWritten.Add(1)
	pivot.TriggerNodeSync(servers.node.Client, servers.node.Address)
	servers.nodeWritten.Wait()

	final, err := servers.nodeThings.GetList("things/*")
	require.NoError(t, err)
	byIndex := map[string]meta.Object{}
	for _, o := range final {
		byIndex[o.Index] = o
	}

	_, hasZ := byIndex["z"]
	require.True(t, hasZ, "sanity: pivot's new item Z must reach the node via the pull")

	yObj, hasY := byIndex["y"]
	require.True(t, hasY,
		"glob pull deleted the node's local-only create Y. Under VVConcurrent the "+
			"glob objsToDelete step must not remove local items the leader hasn't seen; "+
			"deletion is gated on localVV.Compare(LeaderVV) == VVLess.")
	var yDecoded map[string]string
	require.NoError(t, json.Unmarshal(yObj.Data, &yDecoded))
	require.Equal(t, "Y", yDecoded["value"], "Y must round-trip with its data intact")
}
