package pivot

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// HealthPath is the WebSocket subscription path for node health tracking.
// Nodes subscribe to this path to indicate they are online.
const HealthPath = StoragePrefix + "health"

// NodeHealth tracks the availability of nodes via periodic health checks.
type NodeHealth struct {
	mu             sync.RWMutex
	connected      map[string]bool      // node address -> is connected
	lastConnect    map[string]time.Time // node address -> last connection time
	lastDisconnect map[string]time.Time // node address -> last disconnection time
	stopChan       chan struct{}
	wg             sync.WaitGroup // tracks background goroutine
	client         *http.Client
	onHealthChange func() // callback when health status changes
}

// NewNodeHealth creates a new NodeHealth tracker.
func NewNodeHealth(client *http.Client) *NodeHealth {
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	return &NodeHealth{
		connected:      make(map[string]bool),
		lastConnect:    make(map[string]time.Time),
		lastDisconnect: make(map[string]time.Time),
		stopChan:       make(chan struct{}),
		client:         client,
	}
}

// IsHealthy returns true if the node is connected via WebSocket.
func (nh *NodeHealth) IsHealthy(node string) bool {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	return nh.connected[node]
}

// OnConnect marks a node as healthy when it connects via WebSocket.
// The key format is expected to be HealthPath + "/" + nodeAddress.
func (nh *NodeHealth) OnConnect(key string) {
	node := extractNodeFromKey(key)
	if node == "" {
		return
	}
	nh.mu.Lock()
	defer nh.mu.Unlock()
	nh.connected[node] = true
	nh.lastConnect[node] = time.Now()
}

// OnDisconnect marks a node as unhealthy when it disconnects.
func (nh *NodeHealth) OnDisconnect(key string) {
	node := extractNodeFromKey(key)
	if node == "" {
		return
	}
	nh.mu.Lock()
	defer nh.mu.Unlock()
	nh.connected[node] = false
	nh.lastDisconnect[node] = time.Now()
}

// extractNodeFromKey extracts the node address from a subscription key.
// Key format: pivot/health/{safeNodeAddress} where safeNodeAddress has _ instead of :
func extractNodeFromKey(key string) string {
	if !strings.HasPrefix(key, HealthPath+"/") {
		return ""
	}
	safeAddr := strings.TrimPrefix(key, HealthPath+"/")
	// Convert underscore back to colon to get the actual address
	return strings.ReplaceAll(safeAddr, "_", ":")
}

// GetHealthyNodes filters a list of nodes to only include healthy (connected) ones.
func (nh *NodeHealth) GetHealthyNodes(nodes []string) []string {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	result := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if nh.connected[node] {
			result = append(result, node)
		}
	}
	return result
}

// NodeStatus represents the health status of a single node
type NodeStatus struct {
	Address   string `json:"address"`
	Healthy   bool   `json:"healthy"`
	LastCheck string `json:"lastCheck"` // RFC3339 formatted timestamp
}

// GetStatus returns the health status of all known nodes
func (nh *NodeHealth) GetStatus() []NodeStatus {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	result := make([]NodeStatus, 0, len(nh.connected))
	for addr, connected := range nh.connected {
		status := NodeStatus{
			Address: addr,
			Healthy: connected,
		}
		if connected {
			if t, ok := nh.lastConnect[addr]; ok {
				status.LastCheck = t.Format(time.RFC3339)
			}
		} else {
			if t, ok := nh.lastDisconnect[addr]; ok {
				status.LastCheck = t.Format(time.RFC3339)
			}
		}
		result = append(result, status)
	}
	return result
}

// NodeHealthHandler returns an HTTP handler that serves node health status as JSON
func NodeHealthHandler(nh *NodeHealth) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if nh == nil {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, "[]")
			return
		}
		status := nh.GetStatus()
		json.NewEncoder(w).Encode(status)
	}
}

// Stop stops the background health checker and waits for it to finish
func (nh *NodeHealth) Stop() {
	if nh.stopChan != nil {
		close(nh.stopChan)
	}
	nh.wg.Wait()
}

// StartBackgroundCheck starts a goroutine that periodically checks node health
func (nh *NodeHealth) StartBackgroundCheck(getNodes func() []string, interval time.Duration) {
	nh.wg.Add(1)
	go func() {
		defer nh.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Check if already stopped before initial check
		select {
		case <-nh.stopChan:
			return
		default:
		}

		// Do an initial check immediately
		nh.checkAllNodes(getNodes())

		for {
			select {
			case <-nh.stopChan:
				return
			case <-ticker.C:
				nh.checkAllNodes(getNodes())
			}
		}
	}()
}

// checkAllNodes pings all nodes and updates their health status
func (nh *NodeHealth) checkAllNodes(nodes []string) {
	for _, node := range nodes {
		go func(n string) {
			healthy := nh.pingNode(n)
			if healthy {
				nh.MarkHealthy(n)
			} else {
				nh.MarkUnhealthy(n)
			}
		}(node)
	}
}

// pingNode checks if a node is reachable
func (nh *NodeHealth) pingNode(node string) bool {
	url := "http://" + node + "/"
	resp, err := nh.client.Get(url)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// SetOnHealthChange sets a callback that's called when health status changes
func (nh *NodeHealth) SetOnHealthChange(callback func()) {
	nh.mu.Lock()
	defer nh.mu.Unlock()
	nh.onHealthChange = callback
}

// MarkHealthy marks a node as healthy (called when sync succeeds)
func (nh *NodeHealth) MarkHealthy(node string) {
	nh.mu.Lock()
	wasHealthy := nh.connected[node]
	nh.connected[node] = true
	nh.lastConnect[node] = time.Now()
	callback := nh.onHealthChange
	nh.mu.Unlock()

	// Notify if status changed
	if !wasHealthy && callback != nil {
		callback()
	}
}

// MarkUnhealthy marks a node as unhealthy (called when sync fails)
func (nh *NodeHealth) MarkUnhealthy(node string) {
	nh.mu.Lock()
	wasHealthy := nh.connected[node]
	nh.connected[node] = false
	nh.lastDisconnect[node] = time.Now()
	callback := nh.onHealthChange
	nh.mu.Unlock()

	// Notify if status changed
	if wasHealthy && callback != nil {
		callback()
	}
}
