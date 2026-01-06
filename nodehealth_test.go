package pivot_test

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

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
