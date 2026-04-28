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

func setupOfflineServers(t *testing.T) *OfflineTestServers {
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

	// Get instances to access VVManager
	pivotInstance := pivot.GetInstance(servers.Pivot)
	nodeInstance := pivot.GetInstance(servers.Node)
	require.NotNil(t, pivotInstance, "Pivot instance should exist")
	require.NotNil(t, nodeInstance, "Node instance should exist")
	require.NotNil(t, pivotInstance.VVManager, "Pivot should have VVManager")
	require.NotNil(t, nodeInstance.VVManager, "Node should have VVManager")

	// Phase 1: Node writes data via HTTP, syncs to pivot
	// Expect: 1 node write (local) + 1 pivot write (from sync)
	servers.NodeWg.Add(1)
	servers.PivotWg.Add(1)

	// Use HTTP POST to trigger proper AfterWrite callback
	payload := []byte(`{"value": "from-node"}`)
	resp, err := servers.Node.Client.Post("http://"+servers.Node.Address+"/policies", "application/json", bytes.NewBuffer(payload))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for both writes to complete
	servers.NodeWg.Wait()
	servers.PivotWg.Wait()

	nodeVV := nodeInstance.VVManager.Get("policies")
	require.NotEmpty(t, nodeVV, "Node should have VV for policies")
	t.Logf("Node VV after write: %v", nodeVV)

	// Verify pivot received the data
	pivotObj, err := servers.PivotPolicies.Get("policies")
	require.NoError(t, err)
	var pivotData map[string]string
	json.Unmarshal(pivotObj.Data, &pivotData)
	require.Equal(t, "from-node", pivotData["value"], "Pivot should have received data from node")

	// Verify pivot incremented its VV (via Set handler)
	pivotVV := pivotInstance.VVManager.Get("policies")
	require.NotEmpty(t, pivotVV, "Pivot should have VV for policies")
	require.Greater(t, pivotVV["leader"], int64(0), "Pivot leader counter should be > 0")
	t.Logf("Pivot VV after receiving: %v", pivotVV)

	t.Log("Phase 1 passed: Node write syncs to pivot with VV tracking")

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

	// Check pivot VV incremented again
	pivotVV2 := pivotInstance.VVManager.Get("policies")
	require.Greater(t, pivotVV2["leader"], pivotVV["leader"], "Pivot VV should have incremented")
	t.Logf("Pivot VV after second write: %v", pivotVV2)

	t.Log("Phase 2 passed: Pivot write syncs to node")

	t.Log("Offline sync test completed successfully")
}

func TestVersionVectorActivityEndpoint(t *testing.T) {
	servers := setupOfflineServers(t)
	defer servers.Close()

	// Write data via HTTP to trigger VV increment
	// Expect: 1 node write (local) + 1 pivot write (from sync)
	servers.NodeWg.Add(1)
	servers.PivotWg.Add(1)

	payload := []byte(`{"value": "test"}`)
	resp, err := servers.Node.Client.Post("http://"+servers.Node.Address+"/policies", "application/json", bytes.NewBuffer(payload))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for both writes to complete
	servers.NodeWg.Wait()
	servers.PivotWg.Wait()

	// Verify pivot has VV
	pivotInstance := pivot.GetInstance(servers.Pivot)
	pivotVV := pivotInstance.VVManager.Get("policies")
	require.Greater(t, pivotVV["leader"], int64(0), "Pivot should have VV")

	// Check activity endpoint on pivot includes VV
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

// TestClockDriftScenario tests the exact scenario that motivated Version Vectors:
// 1. Node clock drifts 6 hours forward
// 2. Update happens on synced key (with future timestamp)
// 3. Clock goes back to actual time
// 4. New updates happen with normal timestamp
// 5. Without VV: old "future" data would overwrite new data during sync
// 6. With VV: logical counters ensure new data wins regardless of timestamps
func TestClockDriftScenario(t *testing.T) {
	servers := setupOfflineServers(t)
	defer servers.Close()

	pivotInstance := pivot.GetInstance(servers.Pivot)
	nodeInstance := pivot.GetInstance(servers.Node)
	require.NotNil(t, pivotInstance.VVManager)
	require.NotNil(t, nodeInstance.VVManager)

	sixHours := int64(6 * 60 * 60 * 1000000000) // 6 hours in nanoseconds
	now := time.Now().UnixNano()

	// === Phase 1: Node clock drifts 6 hours FORWARD ===
	t.Log("Phase 1: Simulating node clock 6 hours in the future")

	// Write data with future timestamp (simulating drifted clock)
	futureData := map[string]string{"value": "written-with-future-clock", "phase": "1"}
	futureBytes, _ := json.Marshal(futureData)

	// Directly set in separate policies storage
	// This triggers: nodeAfterWrite + automatic sync to pivot (pivotAfterWrite)
	servers.NodeWg.Add(1)
	servers.PivotWg.Add(1)
	_, err := servers.NodePolicies.Set("policies", futureBytes)
	require.NoError(t, err)
	servers.NodeWg.Wait()
	servers.PivotWg.Wait()

	// Get the object and manipulate its timestamp to be 6 hours in the future
	futureObj, err := servers.NodePolicies.Get("policies")
	require.NoError(t, err)
	futureTimestamp := now + sixHours
	futureObj.Created = futureTimestamp
	futureObj.Updated = futureTimestamp
	t.Logf("Future timestamp: %d (6 hours ahead of now: %d)", futureObj.Updated, now)

	// Trigger sync to pivot with future-timestamped data
	// Expect: 1 pivot write (from sync)
	servers.PivotWg.Add(1)

	nodeInstance.SyncCallback(storage.Event{
		Key:       "policies",
		Operation: "set",
		Object:    &futureObj,
	})

	// Wait for pivot write to complete
	servers.PivotWg.Wait()

	// Verify pivot received phase 1 data
	phase1Obj, err := servers.PivotPolicies.Get("policies")
	require.NoError(t, err)
	var phase1Data map[string]string
	json.Unmarshal(phase1Obj.Data, &phase1Data)
	require.Equal(t, "1", phase1Data["phase"], "Pivot should receive future-timestamped data")

	// Record VV state after phase 1
	pivotVV1 := pivotInstance.VVManager.Get("policies")
	t.Logf("Pivot VV after phase 1: %v", pivotVV1)

	// === Phase 2: Clock goes back to ACTUAL time ===
	t.Log("Phase 2: Clock returns to actual time, new update happens")

	// Now write NEW data with normal (current) timestamp
	// This simulates the clock being corrected back to real time
	currentData := map[string]string{"value": "written-with-correct-clock", "phase": "2"}
	currentBytes, _ := json.Marshal(currentData)

	// Direct Set triggers: nodeAfterWrite + automatic sync to pivot (pivotAfterWrite)
	servers.NodeWg.Add(1)
	servers.PivotWg.Add(1)
	_, err = servers.NodePolicies.Set("policies", currentBytes)
	require.NoError(t, err)
	servers.NodeWg.Wait()
	servers.PivotWg.Wait()

	// Get the object - it has normal timestamp (which is "in the past" compared to phase 1)
	currentObj, err := servers.NodePolicies.Get("policies")
	require.NoError(t, err)
	t.Logf("Current timestamp: %d (normal time, appears 'older' than future: %d)", currentObj.Updated, futureTimestamp)

	// Verify the timestamp is indeed "older" than the future one
	require.Less(t, currentObj.Updated, futureTimestamp,
		"Current timestamp should be less than future timestamp")

	// Trigger sync with current-timestamped data
	// Expect: 1 pivot write (from sync)
	servers.PivotWg.Add(1)

	nodeInstance.SyncCallback(storage.Event{
		Key:       "policies",
		Operation: "set",
		Object:    &currentObj,
	})

	// === Phase 3: Verify VV prevents the "future" data from winning ===
	t.Log("Phase 3: Verifying Version Vector prevents future-timestamp overwrite")

	// Wait for pivot write to complete
	servers.PivotWg.Wait()

	// Verify the correct data is on pivot
	pivotObj, err := servers.PivotPolicies.Get("policies")
	require.NoError(t, err)
	var pivotData map[string]string
	json.Unmarshal(pivotObj.Data, &pivotData)

	require.Equal(t, "2", pivotData["phase"], "Phase 2 data should be on pivot")
	require.Equal(t, "written-with-correct-clock", pivotData["value"],
		"Current data should win over future-timestamped data")

	// Verify VV incremented correctly
	pivotVV2 := pivotInstance.VVManager.Get("policies")
	t.Logf("Pivot VV after phase 2: %v", pivotVV2)
	require.Greater(t, pivotVV2["leader"], pivotVV1["leader"],
		"VV counter should have incremented, proving logical ordering over timestamps")

	// === Phase 4: Simulate reverse sync (pivot -> node) to ensure no regression ===
	t.Log("Phase 4: Verify pivot -> node sync also respects VV")

	// Write on pivot via HTTP, then manually trigger node sync
	// (Pivot doesn't auto-sync to node since node isn't registered)
	servers.PivotWg.Add(1)

	payload := []byte(`{"value": "pivot-update-after-drift", "phase": "3"}`)
	resp, err := servers.Pivot.Client.Post("http://"+servers.Pivot.Address+"/policies", "application/json", bytes.NewBuffer(payload))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wait for pivot write
	servers.PivotWg.Wait()

	// Manually trigger node to pull from pivot
	servers.NodeWg.Add(1)
	pivot.TriggerNodeSync(servers.Node.Client, servers.Node.Address)
	servers.NodeWg.Wait()

	nodeObj, err := servers.NodePolicies.Get("policies")
	require.NoError(t, err)
	var nodeData map[string]string
	json.Unmarshal(nodeObj.Data, &nodeData)
	require.Equal(t, "pivot-update-after-drift", nodeData["value"])

	t.Log("SUCCESS: Version Vector correctly handled clock drift scenario")
	t.Log("- Future-timestamped data did NOT overwrite newer logical updates")
	t.Log("- VV counters provide reliable ordering independent of wall clock")
}
