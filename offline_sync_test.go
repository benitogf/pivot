package pivot_test

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/monotonic"
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

func TestVersionVectorIncrement(t *testing.T) {
	vv := pivot.VersionVector{"leader": 1}
	vv2 := vv.Increment("nodeA")

	// Original unchanged
	require.Equal(t, int64(0), vv["nodeA"])
	// New has increment
	require.Equal(t, int64(1), vv2["nodeA"])
	require.Equal(t, int64(1), vv2["leader"])
}

func TestVVManagerBasic(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	db.Start(storage.Options{})
	defer db.Close()

	manager := pivot.NewVVManager(db, "leader")

	// Get non-existent key returns empty VV
	vv := manager.Get("testkey")
	require.Empty(t, vv)

	// Increment creates entry
	vv = manager.Increment("testkey")
	require.Equal(t, int64(1), vv["leader"])

	// Increment again
	vv = manager.Increment("testkey")
	require.Equal(t, int64(2), vv["leader"])

	// Set from remote
	remoteVV := pivot.VersionVector{"leader": 2, "nodeA": 5}
	manager.Set("testkey", remoteVV)

	vv = manager.Get("testkey")
	require.Equal(t, int64(2), vv["leader"])
	require.Equal(t, int64(5), vv["nodeA"])
}

func TestVVManagerPersistence(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	db.Start(storage.Options{})
	defer db.Close()

	// Create manager and set some data
	manager1 := pivot.NewVVManager(db, "leader")
	manager1.Increment("testkey")
	manager1.Increment("testkey")

	// Create new manager with same storage - should load persisted data
	manager2 := pivot.NewVVManager(db, "leader")
	vv := manager2.Get("testkey")
	require.Equal(t, int64(2), vv["leader"])
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
type OfflineTestServers struct {
	Pivot   *ooo.Server
	Node    *ooo.Server
	PivotWg *sync.WaitGroup
	NodeWg  *sync.WaitGroup
}

func setupOfflineServers(t *testing.T) *OfflineTestServers {
	pivotWg := &sync.WaitGroup{}
	nodeWg := &sync.WaitGroup{}

	pivotAfterWrite := func(key string) {
		if strings.HasPrefix(key, "pivot/") {
			return
		}
		t.Logf("[pivot] storage write: %s", key)
		pivotWg.Done()
	}

	nodeAfterWrite := func(key string) {
		if strings.HasPrefix(key, "pivot/") {
			return
		}
		t.Logf("[node] storage write: %s", key)
		nodeWg.Done()
	}

	// Create pivot server (leader)
	pivotServer := &ooo.Server{}
	pivotServer.Silence = true
	pivotServer.Static = true
	pivotServer.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	pivotServer.Router = mux.NewRouter()
	pivotServer.Client = &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 5 * time.Second,
			}).Dial,
			MaxConnsPerHost:   3000,
			DisableKeepAlives: true,
		},
	}
	pivotServer.Audit = func(r *http.Request) bool { return true }

	pivotConfig := pivot.Config{
		Keys: []pivot.Key{
			{Path: "policies"},
		},
		ClusterURL: "", // Empty = this is the leader
	}
	pivot.Setup(pivotServer, pivotConfig)
	pivotServer.Storage.Start(storage.Options{AfterWrite: pivotAfterWrite})
	pivotServer.OpenFilter("policies")
	pivotServer.Start("localhost:0")

	// Create node server (follower)
	nodeServer := &ooo.Server{}
	nodeServer.Silence = true
	nodeServer.Static = true
	nodeServer.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	nodeServer.Router = mux.NewRouter()
	nodeServer.Client = &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 5 * time.Second,
			}).Dial,
			MaxConnsPerHost:   3000,
			DisableKeepAlives: true,
		},
	}
	nodeServer.Audit = func(r *http.Request) bool { return true }

	nodeConfig := pivot.Config{
		Keys: []pivot.Key{
			{Path: "policies"},
		},
		ClusterURL: pivotServer.Address, // Connect to pivot
	}
	pivot.Setup(nodeServer, nodeConfig)
	nodeServer.Storage.Start(storage.Options{AfterWrite: nodeAfterWrite})
	nodeServer.OpenFilter("policies")
	nodeServer.Start("localhost:0")

	return &OfflineTestServers{
		Pivot:   pivotServer,
		Node:    nodeServer,
		PivotWg: pivotWg,
		NodeWg:  nodeWg,
	}
}

func (s *OfflineTestServers) Close() {
	s.Node.Close(nil)
	s.Pivot.Close(nil)
}

func TestOfflineNodeWriteAndSync(t *testing.T) {
	servers := setupOfflineServers(t)
	defer servers.Close()

	// Get instances to access VVManager
	pivotInstance := pivot.GetInstance(servers.Pivot)
	nodeInstance := pivot.GetInstance(servers.Node)
	require.NotNil(t, pivotInstance, "Pivot instance should exist")
	require.NotNil(t, nodeInstance, "Node instance should exist")
	require.NotNil(t, pivotInstance.VVManager, "Pivot should have VVManager")
	require.NotNil(t, nodeInstance.VVManager, "Node should have VVManager")

	// Phase 1: Node writes data locally
	testData := map[string]string{"value": "from-node"}
	dataBytes, _ := json.Marshal(testData)

	_, err := servers.Node.Storage.Set("policies", dataBytes)
	require.NoError(t, err)

	// Fetch object and manually trigger sync (direct Storage.Set bypasses OnStorageEvent)
	nodeObj, err := servers.Node.Storage.Get("policies")
	require.NoError(t, err)
	nodeInstance.SyncCallback(storage.Event{
		Key:       "policies",
		Operation: "set",
		Object:    &nodeObj,
	})

	// Poll for sync completion - node VV should be set
	require.Eventually(t, func() bool {
		nodeVV := nodeInstance.VVManager.Get("policies")
		return len(nodeVV) > 0
	}, 2*time.Second, 10*time.Millisecond, "Node should have VV for policies")

	nodeVV := nodeInstance.VVManager.Get("policies")
	t.Logf("Node VV after write: %v", nodeVV)

	// Poll for pivot to receive data
	require.Eventually(t, func() bool {
		pivotObj, err := servers.Pivot.Storage.Get("policies")
		if err != nil {
			return false
		}
		var pivotData map[string]string
		json.Unmarshal(pivotObj.Data, &pivotData)
		return pivotData["value"] == "from-node"
	}, 2*time.Second, 10*time.Millisecond, "Pivot should have received data from node")

	// Verify pivot incremented its VV (via Set handler)
	pivotVV := pivotInstance.VVManager.Get("policies")
	require.NotEmpty(t, pivotVV, "Pivot should have VV for policies")
	require.Greater(t, pivotVV["leader"], int64(0), "Pivot leader counter should be > 0")
	t.Logf("Pivot VV after receiving: %v", pivotVV)

	t.Log("Phase 1 passed: Node write syncs to pivot with VV tracking")

	// Phase 2: Pivot writes, node should receive
	pivotUpdate := map[string]string{"value": "from-pivot"}
	pivotBytes, _ := json.Marshal(pivotUpdate)

	_, err = servers.Pivot.Storage.Set("policies", pivotBytes)
	require.NoError(t, err)

	// Fetch object and manually trigger sync (direct Storage.Set bypasses OnStorageEvent)
	pivotObj, err := servers.Pivot.Storage.Get("policies")
	require.NoError(t, err)
	pivotInstance.SyncCallback(storage.Event{
		Key:       "policies",
		Operation: "set",
		Object:    &pivotObj,
	})

	// Poll for node to receive update
	require.Eventually(t, func() bool {
		nodeObj2, err := servers.Node.Storage.Get("policies")
		if err != nil {
			return false
		}
		var nodeData map[string]string
		json.Unmarshal(nodeObj2.Data, &nodeData)
		return nodeData["value"] == "from-pivot"
	}, 2*time.Second, 10*time.Millisecond, "Node should have received update from pivot")

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

	// Write data to trigger VV increment
	testData := map[string]string{"value": "test"}
	dataBytes, _ := json.Marshal(testData)

	_, err := servers.Node.Storage.Set("policies", dataBytes)
	require.NoError(t, err)

	// Fetch the object for sync event
	obj, err := servers.Node.Storage.Get("policies")
	require.NoError(t, err)

	// Manually trigger sync (direct Storage.Set bypasses OnStorageEvent)
	nodeInstance := pivot.GetInstance(servers.Node)
	nodeInstance.SyncCallback(storage.Event{
		Key:       "policies",
		Operation: "set",
		Object:    &obj,
	})

	// Poll for sync completion
	pivotInstance := pivot.GetInstance(servers.Pivot)
	require.Eventually(t, func() bool {
		pivotVV := pivotInstance.VVManager.Get("policies")
		return pivotVV["leader"] > 0
	}, 2*time.Second, 10*time.Millisecond, "Pivot should have VV")

	// Check activity endpoint on pivot includes VV
	resp, err := servers.Pivot.Client.Get("http://" + servers.Pivot.Address + "/_pivot/activity/policies")
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

	// Directly set in storage
	_, err := servers.Node.Storage.Set("policies", futureBytes)
	require.NoError(t, err)

	// Get the object and manipulate its timestamp to be 6 hours in the future
	futureObj, err := servers.Node.Storage.Get("policies")
	require.NoError(t, err)
	futureTimestamp := now + sixHours
	futureObj.Created = futureTimestamp
	futureObj.Updated = futureTimestamp
	t.Logf("Future timestamp: %d (6 hours ahead of now: %d)", futureObj.Updated, now)

	// Trigger sync to pivot with future-timestamped data
	nodeInstance.SyncCallback(storage.Event{
		Key:       "policies",
		Operation: "set",
		Object:    &futureObj,
	})

	// Wait for pivot to receive the future-timestamped data
	require.Eventually(t, func() bool {
		obj, err := servers.Pivot.Storage.Get("policies")
		if err != nil {
			return false
		}
		var data map[string]string
		json.Unmarshal(obj.Data, &data)
		return data["phase"] == "1"
	}, 2*time.Second, 10*time.Millisecond, "Pivot should receive future-timestamped data")

	// Record VV state after phase 1
	pivotVV1 := pivotInstance.VVManager.Get("policies")
	t.Logf("Pivot VV after phase 1: %v", pivotVV1)

	// === Phase 2: Clock goes back to ACTUAL time ===
	t.Log("Phase 2: Clock returns to actual time, new update happens")

	// Now write NEW data with normal (current) timestamp
	// This simulates the clock being corrected back to real time
	currentData := map[string]string{"value": "written-with-correct-clock", "phase": "2"}
	currentBytes, _ := json.Marshal(currentData)

	_, err = servers.Node.Storage.Set("policies", currentBytes)
	require.NoError(t, err)

	// Get the object - it has normal timestamp (which is "in the past" compared to phase 1)
	currentObj, err := servers.Node.Storage.Get("policies")
	require.NoError(t, err)
	t.Logf("Current timestamp: %d (normal time, appears 'older' than future: %d)", currentObj.Updated, futureTimestamp)

	// Verify the timestamp is indeed "older" than the future one
	require.Less(t, currentObj.Updated, futureTimestamp,
		"Current timestamp should be less than future timestamp")

	// Trigger sync with current-timestamped data
	nodeInstance.SyncCallback(storage.Event{
		Key:       "policies",
		Operation: "set",
		Object:    &currentObj,
	})

	// === Phase 3: Verify VV prevents the "future" data from winning ===
	t.Log("Phase 3: Verifying Version Vector prevents future-timestamp overwrite")

	// Wait for pivot to receive the update
	require.Eventually(t, func() bool {
		obj, err := servers.Pivot.Storage.Get("policies")
		if err != nil {
			return false
		}
		var data map[string]string
		json.Unmarshal(obj.Data, &data)
		return data["phase"] == "2"
	}, 2*time.Second, 10*time.Millisecond, "Pivot should have phase 2 data (VV wins over timestamp)")

	// Verify the correct data is on pivot
	pivotObj, err := servers.Pivot.Storage.Get("policies")
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

	// Write on pivot
	pivotUpdate := map[string]string{"value": "pivot-update-after-drift", "phase": "3"}
	pivotBytes, _ := json.Marshal(pivotUpdate)
	_, err = servers.Pivot.Storage.Set("policies", pivotBytes)
	require.NoError(t, err)

	pivotObj2, err := servers.Pivot.Storage.Get("policies")
	require.NoError(t, err)
	pivotInstance.SyncCallback(storage.Event{
		Key:       "policies",
		Operation: "set",
		Object:    &pivotObj2,
	})

	// Wait for node to receive
	require.Eventually(t, func() bool {
		obj, err := servers.Node.Storage.Get("policies")
		if err != nil {
			return false
		}
		var data map[string]string
		json.Unmarshal(obj.Data, &data)
		return data["phase"] == "3"
	}, 2*time.Second, 10*time.Millisecond, "Node should receive pivot update")

	nodeObj, err := servers.Node.Storage.Get("policies")
	require.NoError(t, err)
	var nodeData map[string]string
	json.Unmarshal(nodeObj.Data, &nodeData)
	require.Equal(t, "pivot-update-after-drift", nodeData["value"])

	t.Log("SUCCESS: Version Vector correctly handled clock drift scenario")
	t.Log("- Future-timestamped data did NOT overwrite newer logical updates")
	t.Log("- VV counters provide reliable ordering independent of wall clock")
}
