package pivot_test

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/pivot"
	"github.com/stretchr/testify/require"
)

func TestNodeHealth_IsHealthy_UnknownNode(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)
	// Unknown nodes should be assumed healthy
	require.True(t, nh.IsHealthy("unknown-node:8080"))
}

func TestNodeHealth_MarkHealthy(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)
	node := "192.168.1.1:8080"

	// Mark as unhealthy first
	nh.MarkUnhealthy(node)
	require.False(t, nh.IsHealthy(node))

	// Mark as healthy
	nh.MarkHealthy(node)
	require.True(t, nh.IsHealthy(node))
}

func TestNodeHealth_MarkUnhealthy(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)
	node := "192.168.1.1:8080"

	// Mark as healthy first
	nh.MarkHealthy(node)
	require.True(t, nh.IsHealthy(node))

	// Mark as unhealthy
	nh.MarkUnhealthy(node)
	require.False(t, nh.IsHealthy(node))
}

func TestNodeHealth_GetHealthyNodes(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)

	nodes := []string{"node1:8080", "node2:8080", "node3:8080", "node4:8080"}

	// Mark some as unhealthy
	nh.MarkUnhealthy("node2:8080")
	nh.MarkUnhealthy("node4:8080")
	nh.MarkHealthy("node1:8080")
	nh.MarkHealthy("node3:8080")

	healthy := nh.GetHealthyNodes(nodes)

	require.Len(t, healthy, 2)
	require.Contains(t, healthy, "node1:8080")
	require.Contains(t, healthy, "node3:8080")
}

func TestNodeHealth_GetHealthyNodes_UnknownNodesIncluded(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)

	nodes := []string{"known:8080", "unknown:8080"}

	// Only mark one as unhealthy
	nh.MarkUnhealthy("known:8080")

	healthy := nh.GetHealthyNodes(nodes)

	// Unknown nodes should be included (assumed healthy)
	require.Len(t, healthy, 1)
	require.Contains(t, healthy, "unknown:8080")
}

func TestNodeHealth_GetHealthyNodes_AllUnknown(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)

	nodes := []string{"node1:8080", "node2:8080", "node3:8080"}

	// All unknown - all should be returned
	healthy := nh.GetHealthyNodes(nodes)

	require.Len(t, healthy, 3)
}

func TestNodeHealth_GetHealthyNodes_EmptyList(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)

	healthy := nh.GetHealthyNodes([]string{})

	require.Len(t, healthy, 0)
}

func TestNodeHealth_GetStatus(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)

	nh.MarkHealthy("node1:8080")
	nh.MarkUnhealthy("node2:8080")

	status := nh.GetStatus()

	require.Len(t, status, 2)

	// Find each node in status
	var node1Status, node2Status *pivot.NodeStatus
	for i := range status {
		if status[i].Address == "node1:8080" {
			node1Status = &status[i]
		}
		if status[i].Address == "node2:8080" {
			node2Status = &status[i]
		}
	}

	require.NotNil(t, node1Status)
	require.True(t, node1Status.Healthy)
	require.NotEmpty(t, node1Status.LastCheck)

	require.NotNil(t, node2Status)
	require.False(t, node2Status.Healthy)
	require.NotEmpty(t, node2Status.LastCheck)
}

func TestNodeHealth_GetStatus_Empty(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)

	status := nh.GetStatus()

	require.Len(t, status, 0)
}

func TestNodeHealthHandler_NilNodeHealth(t *testing.T) {
	handler := pivot.NodeHealthHandler(nil)

	req := httptest.NewRequest("GET", "/health/nodes", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	require.Equal(t, "[]", w.Body.String())
}

func TestNodeHealthHandler_WithNodes(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)
	nh.MarkHealthy("node1:8080")
	nh.MarkUnhealthy("node2:8080")

	handler := pivot.NodeHealthHandler(nh)

	req := httptest.NewRequest("GET", "/health/nodes", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	require.Contains(t, w.Body.String(), "node1:8080")
	require.Contains(t, w.Body.String(), "node2:8080")
}

func TestNodeHealth_DefaultCheckInterval(t *testing.T) {
	// Default backoff intervals are used (1s initial, 5s after 10min, 10s after 30min)
	nh := pivot.NewNodeHealth(http.DefaultClient)
	require.NotNil(t, nh)
}

func TestNodeHealth_StartBackgroundCheck_StopsWhenInactive(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)

	var active int32 = 1
	isActive := func() bool { return atomic.LoadInt32(&active) == 1 }
	getNodes := func() []string { return []string{} }

	nh.StartBackgroundCheck(getNodes, isActive)

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)

	// Signal inactive
	atomic.StoreInt32(&active, 0)

	// Wait for goroutine to notice and stop
	time.Sleep(100 * time.Millisecond)

	// Goroutine should have stopped - we can verify by trying to start again
	// If it stopped, starting again should work
	atomic.StoreInt32(&active, 1)
	nh.StartBackgroundCheck(getNodes, isActive)

	// Clean up
	atomic.StoreInt32(&active, 0)
	time.Sleep(100 * time.Millisecond)
}

func TestNodeHealth_StartBackgroundCheck_DoubleStart(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)

	var active int32 = 1
	isActive := func() bool { return atomic.LoadInt32(&active) == 1 }
	getNodes := func() []string { return []string{} }

	// Start twice - should not panic or create multiple goroutines
	nh.StartBackgroundCheck(getNodes, isActive)
	nh.StartBackgroundCheck(getNodes, isActive)

	// Clean up
	atomic.StoreInt32(&active, 0)
	time.Sleep(150 * time.Millisecond)
}

func TestNodeHealth_RecheckUnhealthyNodes_WithReachableServer(t *testing.T) {
	// Create a test server that responds to health checks
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Extract host:port from test server URL (remove http://)
	serverAddr := ts.URL[7:] // Remove "http://"

	nh := pivot.NewNodeHealth(ts.Client())

	// Mark node as unhealthy
	nh.MarkUnhealthy(serverAddr)
	require.False(t, nh.IsHealthy(serverAddr))

	var active int32 = 1
	isActive := func() bool { return atomic.LoadInt32(&active) == 1 }
	getNodes := func() []string { return []string{serverAddr} }

	nh.StartBackgroundCheck(getNodes, isActive)

	// Wait for recheck to happen (base tick is 1s, need to wait for tick + recheck)
	time.Sleep(1500 * time.Millisecond)

	// Node should now be healthy since test server is reachable
	require.True(t, nh.IsHealthy(serverAddr))

	// Clean up
	atomic.StoreInt32(&active, 0)
	time.Sleep(100 * time.Millisecond)
}

func TestNodeHealth_RecheckUnhealthyNodes_WithUnreachableServer(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)

	// Use an address that won't be reachable
	unreachableAddr := "192.168.99.99:9999"

	// Mark node as unhealthy
	nh.MarkUnhealthy(unreachableAddr)
	require.False(t, nh.IsHealthy(unreachableAddr))

	var active int32 = 1
	isActive := func() bool { return atomic.LoadInt32(&active) == 1 }
	getNodes := func() []string { return []string{unreachableAddr} }

	nh.StartBackgroundCheck(getNodes, isActive)

	// Wait for recheck to happen (base tick is 1s)
	time.Sleep(1500 * time.Millisecond)

	// Node should still be unhealthy since server is unreachable
	require.False(t, nh.IsHealthy(unreachableAddr))

	// Clean up
	atomic.StoreInt32(&active, 0)
	time.Sleep(100 * time.Millisecond)
}

func TestNodeHealth_BackoffResetOnHealthy(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)
	node := "192.168.1.1:8080"

	// Mark as unhealthy - should start tracking unhealthySince
	nh.MarkUnhealthy(node)
	require.False(t, nh.IsHealthy(node))

	// Mark as healthy - should reset backoff
	nh.MarkHealthy(node)
	require.True(t, nh.IsHealthy(node))

	// Mark as unhealthy again - should start fresh backoff
	nh.MarkUnhealthy(node)
	require.False(t, nh.IsHealthy(node))
}

func TestNodeHealth_Stop_ImmediateShutdown(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)

	var active int32 = 1
	isActive := func() bool { return atomic.LoadInt32(&active) == 1 }
	getNodes := func() []string { return []string{"192.168.1.1:8080"} }

	nh.StartBackgroundCheck(getNodes, isActive)

	// Stop should return immediately without waiting for ticker
	start := time.Now()
	nh.Stop()
	elapsed := time.Since(start)

	// Should complete in well under 1 second (the ticker interval)
	require.Less(t, elapsed, 100*time.Millisecond, "Stop() should return immediately")
}

func TestNodeHealth_Stop_SafeToCallMultipleTimes(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)

	var active int32 = 1
	isActive := func() bool { return atomic.LoadInt32(&active) == 1 }
	getNodes := func() []string { return []string{} }

	nh.StartBackgroundCheck(getNodes, isActive)

	// Multiple Stop calls should be safe
	nh.Stop()
	nh.Stop()
	nh.Stop()
}

func TestNodeHealth_Stop_SafeWithoutStart(t *testing.T) {
	nh := pivot.NewNodeHealth(http.DefaultClient)

	// Stop without Start should be safe (no-op)
	nh.Stop()
}

// =============================================================================
// End-to-End Node Health Tests
// =============================================================================

func TestE2E_NodeHealth_PivotTracksNodeHealth(t *testing.T) {
	// Create pivot server
	pivotServer := FakeServer(t, "", nil)
	defer pivotServer.Close(os.Interrupt)

	// Create node server
	nodeServer := FakeServer(t, pivotServer.Address, nil)
	defer nodeServer.Close(os.Interrupt)

	// Register the node on the pivot
	nodeIP, nodePort, _ := net.SplitHostPort(nodeServer.Address)
	nodePortInt, _ := strconv.Atoi(nodePort)
	ooo.Push(pivotServer, "things/*", Thing{IP: nodeIP, Port: nodePortInt, On: true})

	// Get pivot instance to access NodeHealth
	instance := pivot.GetInstance(pivotServer)
	require.NotNil(t, instance)
	require.NotNil(t, instance.NodeHealth)

	// Trigger a sync by writing to pivot - this should mark node as healthy
	ooo.Set(pivotServer, "settings", Settings{DayEpoch: 1})

	// Wait for sync to complete
	time.Sleep(100 * time.Millisecond)

	// Node should be marked healthy after successful sync
	require.True(t, instance.NodeHealth.IsHealthy(nodeServer.Address))

	// Verify node health status endpoint returns data
	resp, err := pivotServer.Client.Get("http://" + pivotServer.Address + "/_pivot/health/nodes")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var status []pivot.NodeStatus
	err = json.NewDecoder(resp.Body).Decode(&status)
	require.NoError(t, err)
	require.Len(t, status, 1)
	require.Equal(t, nodeServer.Address, status[0].Address)
	require.True(t, status[0].Healthy)
}

func TestE2E_NodeHealth_UnreachableNodeMarkedUnhealthy(t *testing.T) {
	// Create pivot server
	pivotServer := FakeServer(t, "", nil)
	defer pivotServer.Close(os.Interrupt)

	// Register a fake unreachable node
	ooo.Push(pivotServer, "things/*", Thing{IP: "192.168.99.99", Port: 9999, On: true})

	// Get pivot instance
	instance := pivot.GetInstance(pivotServer)
	require.NotNil(t, instance)
	require.NotNil(t, instance.NodeHealth)

	// Trigger a sync - this should fail and mark node as unhealthy
	// The sync uses a 5 second timeout, so we need to wait longer
	ooo.Set(pivotServer, "settings", Settings{DayEpoch: 1})

	// Wait for sync attempt to complete (timeout is 5s, but connection refused is fast)
	// Poll for the node to appear in health status
	var status []pivot.NodeStatus
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		status = instance.NodeHealth.GetStatus()
		if len(status) > 0 {
			break
		}
	}

	// Node should be marked unhealthy after failed sync
	require.Len(t, status, 1, "expected node to be tracked in health status")
	require.Equal(t, "192.168.99.99:9999", status[0].Address)
	require.False(t, status[0].Healthy)
	require.False(t, instance.NodeHealth.IsHealthy("192.168.99.99:9999"))
}

func TestE2E_NodeHealth_MixedHealthyUnhealthyNodes(t *testing.T) {
	// Create pivot server
	pivotServer := FakeServer(t, "", nil)
	defer pivotServer.Close(os.Interrupt)

	// Create one real node
	nodeServer := FakeServer(t, pivotServer.Address, nil)
	defer nodeServer.Close(os.Interrupt)

	// Register both real and fake nodes
	nodeIP, nodePort, _ := net.SplitHostPort(nodeServer.Address)
	nodePortInt, _ := strconv.Atoi(nodePort)
	ooo.Push(pivotServer, "things/*", Thing{IP: nodeIP, Port: nodePortInt, On: true})
	ooo.Push(pivotServer, "things/*", Thing{IP: "192.168.99.99", Port: 9999, On: true})

	// Get pivot instance
	instance := pivot.GetInstance(pivotServer)
	require.NotNil(t, instance.NodeHealth)

	// Trigger sync
	ooo.Set(pivotServer, "settings", Settings{DayEpoch: 1})

	// Poll for both nodes to appear in health status
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		status := instance.NodeHealth.GetStatus()
		if len(status) >= 2 {
			break
		}
	}

	// Real node should be healthy, fake node should be unhealthy
	require.True(t, instance.NodeHealth.IsHealthy(nodeServer.Address))
	require.False(t, instance.NodeHealth.IsHealthy("192.168.99.99:9999"))

	// GetHealthyNodes should filter correctly
	allNodes := []string{nodeServer.Address, "192.168.99.99:9999"}
	healthyNodes := instance.NodeHealth.GetHealthyNodes(allNodes)
	require.Len(t, healthyNodes, 1)
	require.Contains(t, healthyNodes, nodeServer.Address)
}

func TestE2E_NodeHealth_NodeRecovery(t *testing.T) {
	// Create pivot server
	pivotServer := FakeServer(t, "", nil)
	defer pivotServer.Close(os.Interrupt)

	// Get pivot instance
	instance := pivot.GetInstance(pivotServer)
	require.NotNil(t, instance.NodeHealth)

	// Register a fake unreachable node first
	ooo.Push(pivotServer, "things/*", Thing{IP: "192.168.99.99", Port: 9999, On: true})

	// Trigger sync - node becomes unhealthy
	ooo.Set(pivotServer, "settings", Settings{DayEpoch: 1})

	// Poll for node to appear in health status
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		status := instance.NodeHealth.GetStatus()
		if len(status) > 0 {
			break
		}
	}
	require.False(t, instance.NodeHealth.IsHealthy("192.168.99.99:9999"))

	// Now create a real node at a different address
	nodeServer := FakeServer(t, pivotServer.Address, nil)
	defer nodeServer.Close(os.Interrupt)

	// Register the real node
	nodeIP, nodePort, _ := net.SplitHostPort(nodeServer.Address)
	nodePortInt, _ := strconv.Atoi(nodePort)
	ooo.Push(pivotServer, "things/*", Thing{IP: nodeIP, Port: nodePortInt, On: true})

	// Trigger another sync
	ooo.Set(pivotServer, "settings", Settings{DayEpoch: 2})

	// Poll for new node to appear in health status
	for i := 0; i < 50; i++ {
		time.Sleep(100 * time.Millisecond)
		status := instance.NodeHealth.GetStatus()
		if len(status) >= 2 {
			break
		}
	}

	// New node should be healthy
	require.True(t, instance.NodeHealth.IsHealthy(nodeServer.Address))
	// Old fake node still unhealthy
	require.False(t, instance.NodeHealth.IsHealthy("192.168.99.99:9999"))
}

func TestE2E_NodeHealth_NodeServerNoHealthTracking(t *testing.T) {
	// Create pivot server
	pivotServer := FakeServer(t, "", nil)
	defer pivotServer.Close(os.Interrupt)

	// Create node server
	nodeServer := FakeServer(t, pivotServer.Address, nil)
	defer nodeServer.Close(os.Interrupt)

	// Node servers should NOT have NodeHealth (only pivot servers do)
	nodeInstance := pivot.GetInstance(nodeServer)
	require.NotNil(t, nodeInstance)
	require.Nil(t, nodeInstance.NodeHealth)

	// Pivot server SHOULD have NodeHealth
	pivotInstance := pivot.GetInstance(pivotServer)
	require.NotNil(t, pivotInstance)
	require.NotNil(t, pivotInstance.NodeHealth)
}

func TestE2E_NodeHealth_HealthEndpointOnNode(t *testing.T) {
	// Create pivot server
	pivotServer := FakeServer(t, "", nil)
	defer pivotServer.Close(os.Interrupt)

	// Create node server
	nodeServer := FakeServer(t, pivotServer.Address, nil)
	defer nodeServer.Close(os.Interrupt)

	// Node server health endpoint should return empty array (no health tracking)
	resp, err := nodeServer.Client.Get("http://" + nodeServer.Address + "/_pivot/health/nodes")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var status []pivot.NodeStatus
	err = json.NewDecoder(resp.Body).Decode(&status)
	require.NoError(t, err)
	require.Len(t, status, 0)
}

func TestE2E_NodeHealth_MultipleNodesAllHealthy(t *testing.T) {
	// Create pivot server
	pivotServer := FakeServer(t, "", nil)
	defer pivotServer.Close(os.Interrupt)

	// Create multiple node servers
	node1 := FakeServer(t, pivotServer.Address, nil)
	defer node1.Close(os.Interrupt)
	node2 := FakeServer(t, pivotServer.Address, nil)
	defer node2.Close(os.Interrupt)
	node3 := FakeServer(t, pivotServer.Address, nil)
	defer node3.Close(os.Interrupt)

	// Register all nodes
	node1IP, node1Port, _ := net.SplitHostPort(node1.Address)
	node1PortInt, _ := strconv.Atoi(node1Port)
	node2IP, node2Port, _ := net.SplitHostPort(node2.Address)
	node2PortInt, _ := strconv.Atoi(node2Port)
	node3IP, node3Port, _ := net.SplitHostPort(node3.Address)
	node3PortInt, _ := strconv.Atoi(node3Port)

	ooo.Push(pivotServer, "things/*", Thing{IP: node1IP, Port: node1PortInt, On: true})
	ooo.Push(pivotServer, "things/*", Thing{IP: node2IP, Port: node2PortInt, On: true})
	ooo.Push(pivotServer, "things/*", Thing{IP: node3IP, Port: node3PortInt, On: true})

	// Get pivot instance
	instance := pivot.GetInstance(pivotServer)
	require.NotNil(t, instance.NodeHealth)

	// Trigger sync
	ooo.Set(pivotServer, "settings", Settings{DayEpoch: 1})
	time.Sleep(200 * time.Millisecond)

	// All nodes should be healthy
	require.True(t, instance.NodeHealth.IsHealthy(node1.Address))
	require.True(t, instance.NodeHealth.IsHealthy(node2.Address))
	require.True(t, instance.NodeHealth.IsHealthy(node3.Address))

	// Status endpoint should show all 3 healthy
	status := instance.NodeHealth.GetStatus()
	require.Len(t, status, 3)
	for _, s := range status {
		require.True(t, s.Healthy)
	}
}
