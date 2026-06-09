package pivot_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

func TestVersionVectorComparison(t *testing.T) {
	// Test VV comparison logic
	vv1 := pivot.VersionVector{"leader": 1, "nodeA": 2}
	vv2 := pivot.VersionVector{"leader": 1, "nodeA": 2}
	vv3 := pivot.VersionVector{"leader": 2, "nodeA": 2}
	vv4 := pivot.VersionVector{"leader": 1, "nodeA": 3}
	vv5 := pivot.VersionVector{"leader": 2, "nodeA": 1}

	// Equal
	require.Equal(t, pivot.VVEqual, vv1.Compare(vv2))

	// vv1 < vv3 (leader incremented)
	require.Equal(t, pivot.VVLess, vv1.Compare(vv3))

	// vv1 < vv4 (nodeA incremented)
	require.Equal(t, pivot.VVLess, vv1.Compare(vv4))

	// vv3 > vv1
	require.Equal(t, pivot.VVGreater, vv3.Compare(vv1))

	// vv4 and vv5 are concurrent (vv4 has higher nodeA, vv5 has higher leader)
	require.Equal(t, pivot.VVConcurrent, vv4.Compare(vv5))
	require.Equal(t, pivot.VVConcurrent, vv5.Compare(vv4))
}

func TestVersionVectorMerge(t *testing.T) {
	vv1 := pivot.VersionVector{"leader": 2, "nodeA": 1}
	vv2 := pivot.VersionVector{"leader": 1, "nodeA": 3, "nodeB": 1}

	merged := vv1.Merge(vv2)

	require.Equal(t, int64(2), merged["leader"])
	require.Equal(t, int64(3), merged["nodeA"])
	require.Equal(t, int64(1), merged["nodeB"])
}

func TestConflictDetection(t *testing.T) {
	// Test that concurrent VVs are properly detected
	local := pivot.VersionVector{"leader": 1, "nodeA": 2}
	remote := pivot.VersionVector{"leader": 2, "nodeA": 1}

	result := local.Compare(remote)
	require.Equal(t, pivot.VVConcurrent, result, "Should detect concurrent vectors")

	// After merge, both should be accounted for
	merged := local.Merge(remote)
	require.Equal(t, int64(2), merged["leader"])
	require.Equal(t, int64(2), merged["nodeA"])
}

// OfflineTestServers holds pivot and node servers for offline sync testing
// Uses SEPARATE storage for policies with AfterWrite callbacks (like cluster_test.go)
// This avoids server.Start() overwriting the AfterWrite callback on server.Storage
type OfflineTestServers struct {
	Pivot         *ooo.Server
	Node          *ooo.Server
	PivotPolicies storage.Database
	NodePolicies  storage.Database
	PivotWg       *sync.WaitGroup
	NodeWg        *sync.WaitGroup
}

// offlineOpt configures setupOfflineServers before the servers start.
type offlineOpt func(*offlineConfig)

type offlineConfig struct {
	// pivotVVWrites, if set, has Done() called for every committed write to the
	// pivot's "pivot/vv/policies" key — the leader's version-vector persistence.
	// It rides the standard server.AfterWrite storage callback (wired before
	// Start), so a test can wait on the leader's post-write VV bump by observing
	// the storage event the bump produces, rather than polling VVManager.
	pivotVVWrites *sync.WaitGroup
}

// withPivotVVWrites wires wg to the leader's VV-persistence storage writes.
func withPivotVVWrites(wg *sync.WaitGroup) offlineOpt {
	return func(c *offlineConfig) { c.pivotVVWrites = wg }
}

func setupOfflineServers(t *testing.T, opts ...offlineOpt) *OfflineTestServers {
	var cfg offlineConfig
	for _, o := range opts {
		o(&cfg)
	}

	pivotWg := &sync.WaitGroup{}
	nodeWg := &sync.WaitGroup{}

	// AfterWrite callback for pivot policies storage
	pivotAfterWrite := func(key string) {
		if strings.HasPrefix(key, "pivot/") {
			return
		}
		t.Logf("[pivot] storage write: %s", key)
		pivotWg.Done()
	}

	// AfterWrite callback for node policies storage
	nodeAfterWrite := func(key string) {
		if strings.HasPrefix(key, "pivot/") {
			return
		}
		t.Logf("[node] storage write: %s", key)
		nodeWg.Done()
	}

	// Create SEPARATE storage for policies (not affected by server.Start)
	pivotPoliciesStorage := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	nodePoliciesStorage := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})

	// Create pivot server (leader)
	pivotServer := &ooo.Server{}
	pivotServer.Silence = true
	pivotServer.Static = true
	pivotServer.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	pivotServer.Router = mux.NewRouter()
	pivotServer.Client = &http.Client{
		Timeout: 500 * time.Millisecond,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 500 * time.Millisecond,
			}).Dial,
			MaxConnsPerHost:   3000,
			DisableKeepAlives: true,
		},
	}
	pivotServer.Audit = func(r *http.Request) bool { return true }

	// Configure pivot with policies using SEPARATE storage
	pivotConfig := pivot.Config{
		Keys: []pivot.Key{
			{Path: "policies", Database: pivotPoliciesStorage},
		},
		ClusterURL: "",
	}
	pivot.Setup(pivotServer, pivotConfig)

	// Attach policies storage with AfterWrite callback (won't be overwritten by server.Start)
	err := pivot.GetInstance(pivotServer).Attach(pivotPoliciesStorage, storage.Options{AfterWrite: pivotAfterWrite})
	if err != nil {
		t.Fatalf("Failed to attach pivot policies storage: %v", err)
	}

	// Add HTTP route for policies (like cluster_test.go)
	pivotServer.Router.HandleFunc("/policies", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			obj, err := pivotPoliciesStorage.Get("policies")
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(obj.Data)
		case http.MethodPost:
			var data map[string]string
			if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			dataBytes, _ := json.Marshal(data)
			if _, err := pivotPoliciesStorage.Set("policies", dataBytes); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		}
	}).Methods(http.MethodGet, http.MethodPost)

	// Observe the leader's VV persistence (pivot/vv/policies) through the standard
	// server.AfterWrite storage callback. pivot uses server.AfterWriteOp for its
	// own bump, leaving server.AfterWrite free for this. Set before Start so the
	// storage layer captures it; nil unless a test opted in via withPivotVVWrites.
	if cfg.pivotVVWrites != nil {
		pivotServer.AfterWrite = func(key string) {
			if key == pivot.VVKeyPrefix+"policies" {
				cfg.pivotVVWrites.Done()
			}
		}
	}

	pivotServer.Start("localhost:0")

	// Create node server (follower)
	nodeServer := &ooo.Server{}
	nodeServer.Silence = true
	nodeServer.Static = true
	nodeServer.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	nodeServer.Router = mux.NewRouter()
	nodeServer.Client = &http.Client{
		Timeout: 500 * time.Millisecond,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 500 * time.Millisecond,
			}).Dial,
			MaxConnsPerHost:   3000,
			DisableKeepAlives: true,
		},
	}
	nodeServer.Audit = func(r *http.Request) bool { return true }

	// Configure node with policies using SEPARATE storage
	nodeConfig := pivot.Config{
		Keys: []pivot.Key{
			{Path: "policies", Database: nodePoliciesStorage},
		},
		ClusterURL:          pivotServer.Address,
		HealthCheckInterval: 500 * time.Millisecond,
	}
	pivot.Setup(nodeServer, nodeConfig)

	// Attach policies storage with AfterWrite callback
	err = pivot.GetInstance(nodeServer).Attach(nodePoliciesStorage, storage.Options{AfterWrite: nodeAfterWrite})
	if err != nil {
		t.Fatalf("Failed to attach node policies storage: %v", err)
	}

	// Add HTTP route for policies
	nodeServer.Router.HandleFunc("/policies", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			obj, err := nodePoliciesStorage.Get("policies")
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(obj.Data)
		case http.MethodPost:
			var data map[string]string
			if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			dataBytes, _ := json.Marshal(data)
			if _, err := nodePoliciesStorage.Set("policies", dataBytes); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		}
	}).Methods(http.MethodGet, http.MethodPost)

	nodeServer.Start("localhost:0")

	return &OfflineTestServers{
		Pivot:         pivotServer,
		Node:          nodeServer,
		PivotPolicies: pivotPoliciesStorage,
		NodePolicies:  nodePoliciesStorage,
		PivotWg:       pivotWg,
		NodeWg:        nodeWg,
	}
}

func (s *OfflineTestServers) Close() {
	s.Node.Close(nil)
	s.Pivot.Close(nil)
}

func TestOfflineNodeWriteAndSync(t *testing.T) {
	servers := setupOfflineServers(t)
	defer servers.Close()

	// Verify separate policies storage is active
	t.Logf("Pivot policies storage active: %v", servers.PivotPolicies.Active())
	t.Logf("Node policies storage active: %v", servers.NodePolicies.Active())

	// This test asserts the observable data-sync effect (a node write reaches the
	// pivot and a pivot write reaches the node), synchronised by the storage-write
	// WaitGroups — not pivot's internal VV counters. The VV machinery is exercised
	// through its effect on the available API (GET /_pivot/activity) in
	// TestVersionVectorActivityEndpoint.

	// Phase 1: a node write syncs to the pivot. NodeWg fires on the node-local
	// write, PivotWg on the pivot-applied write; once both drain the synced data
	// is observable on the pivot.
	servers.NodeWg.Add(1)
	servers.PivotWg.Add(1)

	payload := []byte(`{"value": "from-node"}`)
	resp, err := servers.Node.Client.Post("http://"+servers.Node.Address+"/policies", "application/json", bytes.NewBuffer(payload))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	servers.NodeWg.Wait()
	servers.PivotWg.Wait()

	// Effect: the pivot received the node's data.
	pivotObj, err := servers.PivotPolicies.Get("policies")
	require.NoError(t, err)
	var pivotData map[string]string
	json.Unmarshal(pivotObj.Data, &pivotData)
	require.Equal(t, "from-node", pivotData["value"], "Pivot should have received data from node")

	t.Log("Phase 1 passed: node write syncs to pivot")

	// Phase 2: Pivot writes via HTTP, then manually trigger node sync
	// (Pivot doesn't auto-sync to node since node isn't registered in NodesKey)
	// Expect: 1 pivot write (local) + 1 node write (from manual sync trigger)
	servers.PivotWg.Add(1)

	payload = []byte(`{"value": "from-pivot"}`)
	resp, err = servers.Pivot.Client.Post("http://"+servers.Pivot.Address+"/policies", "application/json", bytes.NewBuffer(payload))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for pivot write
	servers.PivotWg.Wait()

	// Manually trigger node to pull from pivot
	servers.NodeWg.Add(1)
	pivot.TriggerNodeSync(servers.Node.Client, servers.Node.Address)

	// Wait for node write to complete
	servers.NodeWg.Wait()

	// Verify node received the update
	nodeObj2, err := servers.NodePolicies.Get("policies")
	require.NoError(t, err)
	var nodeData map[string]string
	json.Unmarshal(nodeObj2.Data, &nodeData)
	require.Equal(t, "from-pivot", nodeData["value"], "Node should have received update from pivot")

	t.Log("Phase 2 passed: pivot write syncs to node")
}

func TestVersionVectorActivityEndpoint(t *testing.T) {
	// pivotVVWrites observes the leader's VV persistence through the standard
	// server.AfterWrite storage callback (see setupOfflineServers). A node push
	// drives the pivot Set handler, which increments then merges its VV — two
	// writes to pivot/vv/policies. Waiting on those makes the bump durable before
	// we read it back through the external /activity endpoint, with no polling of
	// internal VV state.
	var pivotVVWrites sync.WaitGroup
	servers := setupOfflineServers(t, withPivotVVWrites(&pivotVVWrites))
	defer servers.Close()

	servers.NodeWg.Add(1)
	servers.PivotWg.Add(1)
	pivotVVWrites.Add(2) // pivot Set handler: increment-save + merge-save

	payload := []byte(`{"value": "test"}`)
	resp, err := servers.Node.Client.Post("http://"+servers.Node.Address+"/policies", "application/json", bytes.NewBuffer(payload))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	servers.NodeWg.Wait()
	servers.PivotWg.Wait()
	pivotVVWrites.Wait()

	// Assert through the external API: /activity exposes the leader's VV.
	resp, err = servers.Pivot.Client.Get("http://" + servers.Pivot.Address + "/_pivot/activity/policies")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var activity pivot.ActivityEntry
	err = json.NewDecoder(resp.Body).Decode(&activity)
	require.NoError(t, err)

	// Pivot should have VV with leader counter incremented
	require.NotEmpty(t, activity.VV, "Activity should include version vector")
	require.Greater(t, activity.VV["leader"], int64(0), "Leader counter should be > 0")

	t.Logf("Activity VV: %v", activity.VV)
	t.Log("Activity endpoint VV test passed")
}
