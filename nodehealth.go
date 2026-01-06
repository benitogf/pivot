package pivot

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/benitogf/network"
)

// Backoff thresholds for health check intervals (per-node)
const (
	backoffInitialInterval = 1 * time.Second  // Check every 1s initially
	backoffMediumInterval  = 5 * time.Second  // Check every 5s after 10min unhealthy
	backoffMaxInterval     = 10 * time.Second // Check every 10s after 30min unhealthy
	backoffMediumThreshold = 10 * time.Minute // Time unhealthy before medium backoff
	backoffMaxThreshold    = 30 * time.Minute // Time unhealthy before max backoff
)

// NodeHealth tracks the availability of nodes to avoid timeout penalties
// for long-term offline nodes during synchronization.
type NodeHealth struct {
	mu               sync.RWMutex
	healthy          map[string]bool      // node address -> is healthy
	lastCheck        map[string]time.Time // node address -> last check time
	unhealthySince   map[string]time.Time // node address -> when it first became unhealthy
	client           *http.Client
	running          bool
	baseTickInterval time.Duration // base tick interval for the background goroutine
}

// NewNodeHealth creates a new NodeHealth tracker.
// The health checker uses per-node backoff: 1s initially, 5s after 10min unhealthy, 10s after 30min.
func NewNodeHealth(client *http.Client) *NodeHealth {
	return &NodeHealth{
		healthy:          make(map[string]bool),
		lastCheck:        make(map[string]time.Time),
		unhealthySince:   make(map[string]time.Time),
		client:           client,
		baseTickInterval: backoffInitialInterval,
	}
}

// IsHealthy returns true if the node is known to be healthy or hasn't been checked yet.
// Unknown nodes are assumed healthy to allow first contact.
func (nh *NodeHealth) IsHealthy(node string) bool {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	healthy, exists := nh.healthy[node]
	if !exists {
		return true // Unknown nodes assumed healthy
	}
	return healthy
}

// MarkHealthy marks a node as healthy after successful communication.
// Resets the unhealthySince timestamp.
func (nh *NodeHealth) MarkHealthy(node string) {
	nh.mu.Lock()
	defer nh.mu.Unlock()
	nh.healthy[node] = true
	nh.lastCheck[node] = time.Now()
	delete(nh.unhealthySince, node) // Reset backoff when healthy
}

// MarkUnhealthy marks a node as unhealthy after failed communication.
// Tracks when the node first became unhealthy for backoff calculation.
func (nh *NodeHealth) MarkUnhealthy(node string) {
	nh.mu.Lock()
	defer nh.mu.Unlock()
	wasHealthy := nh.healthy[node]
	nh.healthy[node] = false
	nh.lastCheck[node] = time.Now()
	// Only set unhealthySince if transitioning from healthy to unhealthy
	if wasHealthy || nh.unhealthySince[node].IsZero() {
		if _, exists := nh.unhealthySince[node]; !exists {
			nh.unhealthySince[node] = time.Now()
		}
	}
}

// getCheckInterval returns the appropriate check interval for a node based on how long it's been unhealthy.
func (nh *NodeHealth) getCheckInterval(node string) time.Duration {
	unhealthyTime, exists := nh.unhealthySince[node]
	if !exists {
		return backoffInitialInterval
	}
	duration := time.Since(unhealthyTime)
	if duration >= backoffMaxThreshold {
		return backoffMaxInterval
	}
	if duration >= backoffMediumThreshold {
		return backoffMediumInterval
	}
	return backoffInitialInterval
}

// GetHealthyNodes filters a list of nodes to only include healthy ones.
func (nh *NodeHealth) GetHealthyNodes(nodes []string) []string {
	nh.mu.RLock()
	defer nh.mu.RUnlock()
	result := make([]string, 0, len(nodes))
	for _, node := range nodes {
		healthy, exists := nh.healthy[node]
		if !exists || healthy {
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
	result := make([]NodeStatus, 0, len(nh.healthy))
	for addr, healthy := range nh.healthy {
		status := NodeStatus{
			Address: addr,
			Healthy: healthy,
		}
		if lastCheck, ok := nh.lastCheck[addr]; ok {
			status.LastCheck = lastCheck.Format(time.RFC3339)
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

// StartBackgroundCheck starts a background goroutine that periodically
// re-checks unhealthy nodes to detect when they come back online.
// Uses per-node backoff: 1s initially, 5s after 10min unhealthy, 10s after 30min.
// The goroutine will automatically stop when isActive returns false.
func (nh *NodeHealth) StartBackgroundCheck(getNodes func() []string, isActive func() bool) {
	nh.mu.Lock()
	if nh.running {
		nh.mu.Unlock()
		return
	}
	nh.running = true
	nh.mu.Unlock()

	go func() {
		// Tick at the fastest interval (1s) - per-node backoff is checked in recheckUnhealthyNodes
		ticker := time.NewTicker(nh.baseTickInterval)
		defer ticker.Stop()
		for {
			if !isActive() {
				nh.mu.Lock()
				nh.running = false
				nh.mu.Unlock()
				return
			}
			<-ticker.C
			if isActive() {
				nh.recheckUnhealthyNodes(getNodes())
			}
		}
	}()
}

// recheckUnhealthyNodes checks unhealthy nodes based on their individual backoff intervals.
func (nh *NodeHealth) recheckUnhealthyNodes(allNodes []string) {
	nh.mu.RLock()
	nodesToCheck := make([]string, 0)
	now := time.Now()
	for _, node := range allNodes {
		healthy, exists := nh.healthy[node]
		if exists && !healthy {
			lastCheck := nh.lastCheck[node]
			checkInterval := nh.getCheckInterval(node)
			if now.Sub(lastCheck) >= checkInterval {
				nodesToCheck = append(nodesToCheck, node)
			}
		}
	}
	nh.mu.RUnlock()

	// Check nodes in parallel
	var wg sync.WaitGroup
	for _, node := range nodesToCheck {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			if nh.pingNode(n) {
				nh.MarkHealthy(n)
			} else {
				nh.MarkUnhealthy(n)
			}
		}(node)
	}
	wg.Wait()
}

// pingNode attempts a quick health check on a node using network.IsHostReachable.
func (nh *NodeHealth) pingNode(node string) bool {
	return network.IsHostReachable(nh.client, node+RoutePrefix+"/pivot")
}
