package pivot_test

// E2E coverage for the clock-drift bug class — wall-clock skew creating
// future-Updated records that without VV would dominate honest writes.
//
// Two scenarios live here:
//
//   - TestClockDriftScenario: same-peer clock jump. Node's wall clock
//     drifts 6h forward, writes once, clock corrects, writes again.
//     Asserts the corrected-time write wins despite carrying a
//     numerically-smaller Updated.
//
//   - TestPullClobbersLocalPresentTimeWriteWhenLeaderHoldsFutureUpdated:
//     cross-peer skewed clock. A peer plants a future-Updated record on
//     the leader; the node has it locally; the node then writes at
//     present time and a pull tick fires. Asserts the local write
//     survives.
//
// Each test owns its own harness because the scenarios need different
// constraints (the cross-peer test must control the push/pull race
// window, the same-peer test does not). Both use AfterWrite-driven
// sync.WaitGroups for deterministic synchronisation — no sleeps, no
// require.Eventually.

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------
// TestClockDriftScenario: same-peer clock jump.
// ---------------------------------------------------------------------

// TestClockDriftScenario covers the scenario that motivated Version
// Vectors:
//
//  1. Node clock drifts 6 hours forward.
//  2. Update happens on synced key (with future timestamp).
//  3. Clock goes back to actual time.
//  4. New updates happen with normal timestamp.
//  5. Without VV: old "future" data would overwrite new data during sync.
//  6. With VV: logical counters ensure new data wins regardless of
//     timestamps.
//
// Phase-1 and phase-2 use SetWithMeta with an explicit Updated value to
// simulate the drifted / corrected clock. An earlier version mutated a
// returned meta.Object and re-triggered SyncCallback synthetically; that
// pattern depended on the async-bump implementation detail and stopped
// exercising anything once VV bumps moved into AfterWrite. SetWithMeta
// persists the future-stamped record to storage and lets the real push
// pipeline carry it to pivot — the scenario the test name describes.
func TestClockDriftScenario(t *testing.T) {
	servers := setupOfflineServers(t)
	defer servers.Close()

	pivotInstance := pivot.GetInstance(servers.Pivot)
	nodeInstance := pivot.GetInstance(servers.Node)
	require.NotNil(t, pivotInstance.VVManager)
	require.NotNil(t, nodeInstance.VVManager)

	sixHours := int64(6 * 60 * 60 * 1000000000) // 6 hours in nanoseconds
	now := time.Now().UnixNano()
	futureTimestamp := now + sixHours

	// === Phase 1: Node clock drifts 6 hours FORWARD ===
	t.Log("Phase 1: Simulating node clock 6 hours in the future")

	futureData := map[string]string{"value": "written-with-future-clock", "phase": "1"}
	futureBytes, _ := json.Marshal(futureData)

	// SetWithMeta(futureTimestamp, futureTimestamp) writes the future-
	// stamped record directly into node storage — the same effect a
	// drifted wall clock would have. The storage event then triggers
	// applyNodePush which POSTs to pivot, carrying the future Updated
	// in the wire-format body.
	servers.NodeWg.Add(1)
	servers.PivotWg.Add(1)
	_, err := servers.NodePolicies.SetWithMeta("policies", futureBytes, futureTimestamp, futureTimestamp)
	require.NoError(t, err)
	servers.NodeWg.Wait()
	servers.PivotWg.Wait()

	// Phase 1 must reach pivot with the future Updated intact.
	phase1Obj, err := servers.PivotPolicies.Get("policies")
	require.NoError(t, err)
	var phase1Data map[string]string
	json.Unmarshal(phase1Obj.Data, &phase1Data)
	require.Equal(t, "1", phase1Data["phase"], "Pivot should receive future-timestamped data")
	require.Equal(t, futureTimestamp, phase1Obj.Updated,
		"Pivot's stored Updated should reflect the node-pushed future timestamp")

	// Record VV state after phase 1.
	pivotVV1 := pivotInstance.VVManager.Get("policies")
	t.Logf("Pivot VV after phase 1: %v", pivotVV1)
	require.Greater(t, pivotVV1["leader"], int64(0),
		"phase-1 push should have bumped pivot's leader counter")

	// === Phase 2: Clock goes back to ACTUAL time ===
	t.Log("Phase 2: Clock returns to actual time, new update happens")

	currentData := map[string]string{"value": "written-with-correct-clock", "phase": "2"}
	currentBytes, _ := json.Marshal(currentData)
	currentTimestamp := time.Now().UnixNano()
	require.Less(t, currentTimestamp, futureTimestamp,
		"phase-2 (current-time) Updated must be 'older' than phase-1 (future) Updated")

	// SetWithMeta with a present-time Updated mirrors the post-correction
	// real-clock write. The VV bump that fires from AfterWrite makes
	// node's VV strictly greater than what pivot already has, so the
	// idempotency guard on pivot accepts the push despite the smaller
	// Updated.
	servers.NodeWg.Add(1)
	servers.PivotWg.Add(1)
	_, err = servers.NodePolicies.SetWithMeta("policies", currentBytes, currentTimestamp, currentTimestamp)
	require.NoError(t, err)
	servers.NodeWg.Wait()
	servers.PivotWg.Wait()

	// === Phase 3: Verify VV prevents the "future" data from winning ===
	t.Log("Phase 3: Verifying Version Vector prevents future-timestamp overwrite")

	pivotObj, err := servers.PivotPolicies.Get("policies")
	require.NoError(t, err)
	var pivotData map[string]string
	json.Unmarshal(pivotObj.Data, &pivotData)

	require.Equal(t, "2", pivotData["phase"], "Phase 2 data should be on pivot")
	require.Equal(t, "written-with-correct-clock", pivotData["value"],
		"Current data should win over future-timestamped data")
	require.Equal(t, currentTimestamp, pivotObj.Updated,
		"pivot should have phase-2's present-time Updated, proving the wall clock did not gate this")

	pivotVV2 := pivotInstance.VVManager.Get("policies")
	t.Logf("Pivot VV after phase 2: %v", pivotVV2)
	require.Greater(t, pivotVV2["leader"], pivotVV1["leader"],
		"VV counter should have incremented, proving logical ordering over timestamps")

	// === Phase 4: Simulate reverse sync (pivot -> node) to ensure no regression ===
	t.Log("Phase 4: Verify pivot -> node sync also respects VV")

	// Write on pivot via HTTP, then manually trigger node sync
	// (Pivot doesn't auto-sync to node since node isn't registered).
	servers.PivotWg.Add(1)

	payload := []byte(`{"value": "pivot-update-after-drift", "phase": "3"}`)
	resp, err := servers.Pivot.Client.Post("http://"+servers.Pivot.Address+"/policies", "application/json", bytes.NewBuffer(payload))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	servers.PivotWg.Wait()

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

// ---------------------------------------------------------------------
// TestPullClobbersLocalPresentTimeWriteWhenLeaderHoldsFutureUpdated:
// cross-peer skewed clock, present-time local write must survive pull.
// ---------------------------------------------------------------------

// clobberServers is the harness for the cross-peer clock-drift test.
// Two test-only deviations from setupOfflineServers:
//
//   - NoBroadcastKeys=["policies"] on the node side so direct
//     SetWithMeta calls do not fire the storage watch event. Without
//     this the watch goroutine's applyNodePush would propagate every
//     local write to pivot before the pull tick fires, eliminating the
//     race we are trying to capture.
//
//   - BeforeRead cleared on the node's policies storage so the
//     assertion-phase Gets do not trigger background pulls that mutate
//     the state being inspected.
//
// pivotWritten / nodeWritten are AfterWrite-driven sync.WaitGroups
// scoped to the configured key. Each storage write to "policies" fires
// Done() exactly once; tests Add() the expected count before each
// triggering action and Wait() before asserting. No sleeps, no
// require.Eventually — every wait point is paired with a precise
// expected-events count.
type clobberServers struct {
	pivot         *ooo.Server
	node          *ooo.Server
	pivotPolicies storage.Database
	nodePolicies  storage.Database
	pivotWritten  *sync.WaitGroup
	nodeWritten   *sync.WaitGroup
	closeOnce     sync.Once
}

func (s *clobberServers) Close() {
	s.closeOnce.Do(func() {
		s.node.Close(nil)
		s.pivot.Close(nil)
	})
}

func setupClobberServers(t *testing.T) *clobberServers {
	pivotPoliciesStorage := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	nodePoliciesStorage := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})

	pivotWritten := &sync.WaitGroup{}
	nodeWritten := &sync.WaitGroup{}

	pivotAfterWrite := func(key string) {
		if key == "policies" {
			pivotWritten.Done()
		}
	}
	nodeAfterWrite := func(key string) {
		if key == "policies" {
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
		Keys:       []pivot.Key{{Path: "policies", Database: pivotPoliciesStorage}},
		ClusterURL: "",
	})
	if err := pivot.GetInstance(pivotServer).Attach(pivotPoliciesStorage, storage.Options{
		AfterWrite: pivotAfterWrite,
	}); err != nil {
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
		Keys:                []pivot.Key{{Path: "policies", Database: nodePoliciesStorage}},
		ClusterURL:          pivotServer.Address,
		HealthCheckInterval: 500 * time.Millisecond,
	})
	if err := pivot.GetInstance(nodeServer).Attach(nodePoliciesStorage, storage.Options{
		NoBroadcastKeys: []string{"policies"},
		AfterWrite:      nodeAfterWrite,
	}); err != nil {
		t.Fatalf("attach node storage: %v", err)
	}
	nodePoliciesStorage.SetBeforeRead(nil)
	nodeServer.Start("localhost:0")

	return &clobberServers{
		pivot:         pivotServer,
		node:          nodeServer,
		pivotPolicies: pivotPoliciesStorage,
		nodePolicies:  nodePoliciesStorage,
		pivotWritten:  pivotWritten,
		nodeWritten:   nodeWritten,
	}
}

// TestPullClobbersLocalPresentTimeWriteWhenLeaderHoldsFutureUpdated
// reproduces the production "device.open revert" complaint.
//
// Scenario:
//   - A peer with a forward-skewed clock pushed a record into the cluster.
//     Pivot holds it with Updated far in the future. (Simulated by
//     direct SetWithMeta on pivot's storage.)
//   - The node has already received that record via a normal sync tick.
//   - A user-driven local write lands on the node with present-time
//     Updated. (Direct SetWithMeta on the node with NoBroadcastKeys
//     suppressing the async push that would otherwise short-circuit
//     the race.)
//   - A pull tick fires on the node — the same path TriggerNodeSync
//     and the syncer's periodic Pull use.
//
// Expected: the local present-time write survives.
//
// Pre-fix actual: the pull silently overwrites the node's present-time
// write with pivot's future-Updated record.
func TestPullClobbersLocalPresentTimeWriteWhenLeaderHoldsFutureUpdated(t *testing.T) {
	servers := setupClobberServers(t)
	defer servers.Close()

	const dayNs = int64(24 * 60 * 60 * 1_000_000_000)
	now := time.Now().UnixNano()
	futureTimestamp := now + dayNs

	// Phase 1 — plant pivot's future-Updated record. One pivot write
	// from this SetWithMeta; AfterWrite fires once for "policies".
	futureBytes, _ := json.Marshal(map[string]string{"value": "future-from-skewed-peer"})
	servers.pivotWritten.Add(1)
	_, err := servers.pivotPolicies.SetWithMeta("policies", futureBytes, futureTimestamp, futureTimestamp)
	require.NoError(t, err)
	servers.pivotWritten.Wait()

	pivotStored, err := servers.pivotPolicies.Get("policies")
	require.NoError(t, err)
	require.Equal(t, futureTimestamp, pivotStored.Updated,
		"sanity: pivot must hold the future-Updated record before phase 2")

	// Phase 2 — node pulls pivot's record. TriggerNodeSync's HTTP
	// response is written after pool.PullAll() returns; the pull is
	// synchronous inside that handler, and the SetWithMeta that lands
	// pivot's record on node fires nodeAfterWrite once.
	servers.nodeWritten.Add(1)
	pivot.TriggerNodeSync(servers.node.Client, servers.node.Address)
	servers.nodeWritten.Wait()

	nodePulled, err := servers.nodePolicies.Get("policies")
	require.NoError(t, err)
	var pulledData map[string]string
	require.NoError(t, json.Unmarshal(nodePulled.Data, &pulledData))
	require.Equal(t, "future-from-skewed-peer", pulledData["value"],
		"sanity: node must have pulled the future-stamped record from pivot")

	// Phase 3 — local present-time write on node. One node write;
	// NoBroadcastKeys suppresses the storage watch event, so no
	// applyNodePush races us by propagating this to pivot before
	// phase 4 fires the pull tick.
	presentTimestamp := time.Now().UnixNano()
	require.Less(t, presentTimestamp, futureTimestamp,
		"present-time must be 'older' than the planted future timestamp")

	presentBytes, _ := json.Marshal(map[string]string{"value": "user-edit-at-present-time"})
	servers.nodeWritten.Add(1)
	_, err = servers.nodePolicies.SetWithMeta("policies", presentBytes, presentTimestamp, presentTimestamp)
	require.NoError(t, err)
	servers.nodeWritten.Wait()

	nodeAfterLocal, err := servers.nodePolicies.Get("policies")
	require.NoError(t, err)
	require.Equal(t, presentTimestamp, nodeAfterLocal.Updated,
		"sanity: node's local write must be present-time before the pull")

	// Phase 4 — pull tick fires on the node. With the fix applied,
	// pullKeyWithCacheUpdate sees localVV strictly dominate pivotVV
	// (the sync VV bump and merge-on-pull made local's frontier
	// strictly newer) and returns "nothing to synchronize". No write.
	// We do NOT wait on nodeWritten because the count is zero — Wait()
	// on a zero counter returns immediately.
	//
	// Pre-fix this same call would write once to node (the clobber);
	// the absence of that write is what the test verifies, and the
	// final data/Updated assertions below catch it directly.
	//
	// TriggerNodeSync's HTTP response is written after pool.PullAll()
	// returns, so by the time TriggerNodeSync returns any write the
	// pull would have done has already committed via SetWithMeta. We
	// can read storage directly with no settle window.
	pivot.TriggerNodeSync(servers.node.Client, servers.node.Address)

	final, err := servers.nodePolicies.Get("policies")
	require.NoError(t, err)
	var finalData map[string]string
	require.NoError(t, json.Unmarshal(final.Data, &finalData))

	require.Equal(t, "user-edit-at-present-time", finalData["value"],
		"the node's present-time local write must not be reverted by a leader-held future-Updated record")
	require.Equal(t, presentTimestamp, final.Updated,
		"the node's stored Updated must remain at the present-time value")
}
