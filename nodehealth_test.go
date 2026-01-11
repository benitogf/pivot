package pivot_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/benitogf/pivot"
	"github.com/stretchr/testify/require"
)

func TestNodeHealth_IsHealthy_UnknownNode(t *testing.T) {
	nh := pivot.NewNodeHealth(nil)
	// Unknown nodes should NOT be healthy (WebSocket-based - must be connected)
	require.False(t, nh.IsHealthy("unknown-node:8080"))
}

func TestNodeHealth_MarkHealthy(t *testing.T) {
	nh := pivot.NewNodeHealth(nil)
	node := "192.168.1.1:8080"

	// Mark as unhealthy first
	nh.MarkUnhealthy(node)
	require.False(t, nh.IsHealthy(node))

	// Mark as healthy
	nh.MarkHealthy(node)
	require.True(t, nh.IsHealthy(node))
}

func TestNodeHealth_MarkUnhealthy(t *testing.T) {
	nh := pivot.NewNodeHealth(nil)
	node := "192.168.1.1:8080"

	// Mark as healthy first
	nh.MarkHealthy(node)
	require.True(t, nh.IsHealthy(node))

	// Mark as unhealthy
	nh.MarkUnhealthy(node)
	require.False(t, nh.IsHealthy(node))
}

func TestNodeHealth_GetHealthyNodes(t *testing.T) {
	nh := pivot.NewNodeHealth(nil)

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

func TestNodeHealth_GetHealthyNodes_UnknownNodesExcluded(t *testing.T) {
	nh := pivot.NewNodeHealth(nil)

	nodes := []string{"known:8080", "unknown:8080"}

	// Mark one as healthy, one as unhealthy
	nh.MarkHealthy("known:8080")
	nh.MarkUnhealthy("unknown:8080")

	healthy := nh.GetHealthyNodes(nodes)

	// Only connected nodes should be included
	require.Len(t, healthy, 1)
	require.Contains(t, healthy, "known:8080")
}

func TestNodeHealth_GetHealthyNodes_AllUnknown(t *testing.T) {
	nh := pivot.NewNodeHealth(nil)

	nodes := []string{"node1:8080", "node2:8080", "node3:8080"}

	// All unknown - none should be returned (WebSocket-based requires connection)
	healthy := nh.GetHealthyNodes(nodes)

	require.Len(t, healthy, 0)
}

func TestNodeHealth_GetHealthyNodes_EmptyList(t *testing.T) {
	nh := pivot.NewNodeHealth(nil)

	healthy := nh.GetHealthyNodes([]string{})

	require.Len(t, healthy, 0)
}

func TestNodeHealth_GetStatus(t *testing.T) {
	nh := pivot.NewNodeHealth(nil)

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
	nh := pivot.NewNodeHealth(nil)

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
	nh := pivot.NewNodeHealth(nil)
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
	nh := pivot.NewNodeHealth(nil)
	require.NotNil(t, nh)
}

func TestNodeHealth_OnConnectOnDisconnect(t *testing.T) {
	nh := pivot.NewNodeHealth(nil)
	node := "192.168.1.1:8080"
	// Key uses underscore instead of colon (URL-safe format)
	key := pivot.HealthPath + "/192.168.1.1_8080"

	// Initially not connected
	require.False(t, nh.IsHealthy(node))

	// Connect via OnConnect (key has underscore, extracts to colon)
	nh.OnConnect(key)
	require.True(t, nh.IsHealthy(node))

	// Disconnect via OnDisconnect
	nh.OnDisconnect(key)
	require.False(t, nh.IsHealthy(node))
}

func TestNodeHealth_Stop_SafeWithoutStart(t *testing.T) {
	nh := pivot.NewNodeHealth(nil)

	// Stop without Start should be safe (no-op)
	nh.Stop()
}

// =============================================================================
// End-to-End Node Health Tests
// Note: Health is now tracked via WebSocket connections, not sync operations.
// The node connects to pivot's health path via WebSocket subscription.
// =============================================================================

func TestE2E_NodeHealth_PivotHasHealthTracker(t *testing.T) {
	// Create pivot server
	pivotServer := FakeServer(t, "", nil)
	defer pivotServer.Close(os.Interrupt)

	// Get pivot instance to access NodeHealth
	instance := pivot.GetInstance(pivotServer)
	require.NotNil(t, instance)
	require.NotNil(t, instance.NodeHealth, "Pivot server should have NodeHealth tracker")
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
	require.Nil(t, nodeInstance.NodeHealth, "Node server should NOT have NodeHealth tracker")

	// Pivot server SHOULD have NodeHealth
	pivotInstance := pivot.GetInstance(pivotServer)
	require.NotNil(t, pivotInstance)
	require.NotNil(t, pivotInstance.NodeHealth, "Pivot server should have NodeHealth tracker")
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
