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
	protocol       map[string]string    // node address -> protocol version ("2.0", "unknown")
	stopChan       chan struct{}
	wg             sync.WaitGroup // tracks background goroutine
	callbackWg     sync.WaitGroup // tracks in-flight callbacks
	client         *http.Client
	onHealthChange func() // callback when health status changes
	stopped        bool   // true after Stop() is called, prevents callbacks
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
		protocol:       make(map[string]string),
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
	Address    string `json:"address"`
	Healthy    bool   `json:"healthy"`
	LastCheck  string `json:"lastCheck"`  // RFC3339 formatted timestamp
	Protocol   string `json:"protocol"`   // "2.0", "unknown"
	Compatible bool   `json:"compatible"` // true if protocol matches current version
}

// GetStatus returns the health status of all known nodes
func (nh *NodeHealth) GetStatus() []NodeStatus {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	result := make([]NodeStatus, 0, len(nh.connected))
	for addr, connected := range nh.connected {
		proto := nh.protocol[addr]
		if proto == "" {
			proto = "unknown"
		}
		status := NodeStatus{
			Address:    addr,
			Healthy:    connected,
			Protocol:   proto,
			Compatible: proto == ProtocolVersion,
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
	nh.mu.Lock()
	nh.stopped = true
	nh.mu.Unlock()
	if nh.stopChan != nil {
		close(nh.stopChan)
	}
	nh.wg.Wait()
	nh.callbackWg.Wait() // wait for in-flight callbacks to complete
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
				// Check protocol version
				proto := nh.checkNodeVersion(n)
				nh.setProtocol(n, proto)
			} else {
				nh.MarkUnhealthy(n)
			}
		}(node)
	}
}

// checkNodeVersion queries the node's version endpoint to determine protocol version
func (nh *NodeHealth) checkNodeVersion(node string) string {
	url := "http://" + node + RoutePrefix + "/version"
	resp, err := nh.client.Get(url)
	if err != nil {
		return "unknown"
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "unknown"
	}

	var info VersionInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return "unknown"
	}

	if info.Protocol == "" {
		return "unknown"
	}

	return info.Protocol
}

// setProtocol updates the protocol version for a node
func (nh *NodeHealth) setProtocol(node, protocol string) {
	nh.mu.Lock()
	oldProto := nh.protocol[node]
	nh.protocol[node] = protocol
	callback := nh.onHealthChange
	nh.mu.Unlock()

	// Notify if protocol changed
	if oldProto != protocol && callback != nil {
		callback()
	}
}

// GetProtocol returns the detected protocol version for a node
func (nh *NodeHealth) GetProtocol(node string) string {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	proto := nh.protocol[node]
	if proto == "" {
		return "unknown"
	}
	return proto
}

// IsCompatible returns true if the node is running a compatible protocol version.
// If the protocol is not yet known, it checks on-demand before deciding.
func (nh *NodeHealth) IsCompatible(node string) bool {
	nh.mu.RLock()
	proto := nh.protocol[node]
	nh.mu.RUnlock()

	// If protocol not yet detected, check on-demand
	if proto == "" {
		proto = nh.checkNodeVersion(node)
		nh.setProtocol(node, proto)
	}

	return proto == ProtocolVersion
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
	if nh.stopped {
		nh.mu.Unlock()
		return
	}
	wasHealthy := nh.connected[node]
	nh.connected[node] = true
	nh.lastConnect[node] = time.Now()
	callback := nh.onHealthChange
	if !wasHealthy && callback != nil {
		nh.callbackWg.Add(1)
	}
	nh.mu.Unlock()

	// Notify if status changed
	if !wasHealthy && callback != nil {
		callback()
		nh.callbackWg.Done()
	}
}

// MarkUnhealthy marks a node as unhealthy (called when sync fails)
func (nh *NodeHealth) MarkUnhealthy(node string) {
	nh.mu.Lock()
	if nh.stopped {
		nh.mu.Unlock()
		return
	}
	wasHealthy := nh.connected[node]
	nh.connected[node] = false
	nh.lastDisconnect[node] = time.Now()
	callback := nh.onHealthChange
	if wasHealthy && callback != nil {
		nh.callbackWg.Add(1)
	}
	nh.mu.Unlock()

	// Notify if status changed
	if wasHealthy && callback != nil {
		callback()
		nh.callbackWg.Done()
	}
}
