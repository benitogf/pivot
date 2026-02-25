package pivot

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/benitogf/ooo/storage"
)

// VVKeyPrefix is the storage key prefix for version vectors
const VVKeyPrefix = StoragePrefix + "vv/"

// LeaderID is the identifier used for the pivot/leader in version vectors
const LeaderID = "leader"

// VersionVector represents a vector clock for tracking causality across nodes.
// Maps node ID to that node's logical counter.
type VersionVector map[string]int64

// Clone creates a deep copy of the version vector
func (vv VersionVector) Clone() VersionVector {
	if vv == nil {
		return nil
	}
	clone := make(VersionVector, len(vv))
	for k, v := range vv {
		clone[k] = v
	}
	return clone
}

// CompareResult represents the result of comparing two version vectors
type CompareResult int

const (
	// VVEqual means vectors are identical
	VVEqual CompareResult = iota
	// VVLess means first vector is strictly less (happened-before)
	VVLess
	// VVGreater means first vector is strictly greater
	VVGreater
	// VVConcurrent means vectors are concurrent (conflict)
	VVConcurrent
)

// Compare compares two version vectors and returns the ordering relationship.
// Returns:
//   - VVEqual: vectors are identical
//   - VVLess: vv < other (vv happened-before other)
//   - VVGreater: vv > other (other happened-before vv)
//   - VVConcurrent: neither dominates (concurrent/conflict)
func (vv VersionVector) Compare(other VersionVector) CompareResult {
	if vv == nil && other == nil {
		return VVEqual
	}
	if vv == nil {
		return VVLess
	}
	if other == nil {
		return VVGreater
	}

	hasLess := false
	hasGreater := false

	// Check all keys in vv
	for k, v := range vv {
		otherV := other[k] // defaults to 0 if not present
		if v < otherV {
			hasLess = true
		} else if v > otherV {
			hasGreater = true
		}
	}

	// Check keys in other that might not be in vv
	for k, otherV := range other {
		if _, exists := vv[k]; !exists {
			// vv[k] is effectively 0
			if otherV > 0 {
				hasLess = true
			}
		}
	}

	if hasLess && hasGreater {
		return VVConcurrent
	}
	if hasLess {
		return VVLess
	}
	if hasGreater {
		return VVGreater
	}
	return VVEqual
}

// Merge returns a new version vector that is the element-wise maximum of both.
// Used after conflict resolution to ensure both sides converge.
func (vv VersionVector) Merge(other VersionVector) VersionVector {
	result := vv.Clone()
	if result == nil {
		result = make(VersionVector)
	}
	for k, v := range other {
		if v > result[k] {
			result[k] = v
		}
	}
	return result
}

// Increment increments the counter for a given node ID and returns the new vector.
// Does not modify the original.
func (vv VersionVector) Increment(nodeID string) VersionVector {
	result := vv.Clone()
	if result == nil {
		result = make(VersionVector)
	}
	result[nodeID]++
	return result
}

// VVManager manages version vectors for synced keys with storage persistence.
type VVManager struct {
	mu       sync.Mutex
	vectors  map[string]VersionVector // keyPath -> version vector
	storage  storage.Database
	nodeID   string // ID of this node ("leader" for pivot, node path for nodes)
	shutdown int32  // atomic flag to prevent writes during shutdown
}

// NewVVManager creates a new version vector manager.
// nodeID should be "leader" for pivot servers, or the node's path in NodesKey for nodes.
func NewVVManager(db storage.Database, nodeID string) *VVManager {
	return &VVManager{
		vectors: make(map[string]VersionVector),
		storage: db,
		nodeID:  nodeID,
	}
}

// normalizeKeyPath converts a key path to its base form for VV tracking.
// "things/*" becomes "things", "things/123" stays "things/123"
func normalizeKeyPath(keyPath string) string {
	return strings.Replace(keyPath, "/*", "", 1)
}

// Get returns the current version vector for a key.
// Returns empty vector if none exists.
func (m *VVManager) Get(keyPath string) VersionVector {
	baseKey := normalizeKeyPath(keyPath)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.vectors[baseKey]; !exists {
		m.loadFromStorage(baseKey)
	}

	return m.vectors[baseKey].Clone()
}

// Increment increments this node's counter in the version vector for a key.
// Returns the new version vector.
func (m *VVManager) Increment(keyPath string) VersionVector {
	baseKey := normalizeKeyPath(keyPath)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.vectors[baseKey]; !exists {
		m.loadFromStorage(baseKey)
	}

	if m.vectors[baseKey] == nil {
		m.vectors[baseKey] = make(VersionVector)
	}
	m.vectors[baseKey][m.nodeID]++

	m.saveToStorage(baseKey)

	return m.vectors[baseKey].Clone()
}

// Set sets the version vector for a key (used when receiving from remote).
// The vector is merged with existing to ensure monotonicity.
func (m *VVManager) Set(keyPath string, vv VersionVector) {
	baseKey := normalizeKeyPath(keyPath)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.vectors[baseKey]; !exists {
		m.loadFromStorage(baseKey)
	}

	// Merge to ensure we never go backward
	m.vectors[baseKey] = m.vectors[baseKey].Merge(vv)
	m.saveToStorage(baseKey)
}

// MergeAndIncrement merges a remote vector, then increments this node's counter.
// Used when accepting a conflicting update via last-sync-wins.
func (m *VVManager) MergeAndIncrement(keyPath string, remoteVV VersionVector) VersionVector {
	baseKey := normalizeKeyPath(keyPath)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.vectors[baseKey]; !exists {
		m.loadFromStorage(baseKey)
	}

	// Merge remote vector
	m.vectors[baseKey] = m.vectors[baseKey].Merge(remoteVV)
	// Increment our counter
	m.vectors[baseKey][m.nodeID]++

	m.saveToStorage(baseKey)

	return m.vectors[baseKey].Clone()
}

// loadFromStorage loads a version vector from storage into the cache.
// Caller must hold m.mu.
func (m *VVManager) loadFromStorage(baseKey string) {
	if m.storage == nil || !m.storage.Active() {
		m.vectors[baseKey] = make(VersionVector)
		return
	}

	obj, err := m.storage.Get(VVKeyPrefix + baseKey)
	if err != nil {
		m.vectors[baseKey] = make(VersionVector)
		return
	}

	var vv VersionVector
	if err := json.Unmarshal(obj.Data, &vv); err != nil {
		m.vectors[baseKey] = make(VersionVector)
		return
	}

	m.vectors[baseKey] = vv
}

// saveToStorage persists a version vector to storage.
// Caller must hold m.mu.
func (m *VVManager) saveToStorage(baseKey string) {
	// Check shutdown flag to avoid racing with storage.Close()
	if atomic.LoadInt32(&m.shutdown) != 0 {
		return
	}
	if m.storage == nil || !m.storage.Active() {
		return
	}

	data, err := json.Marshal(m.vectors[baseKey])
	if err != nil {
		return
	}

	m.storage.Set(VVKeyPrefix+baseKey, data)
}

// Shutdown marks the manager as shutting down to prevent storage writes.
// Should be called before closing the storage.
// Acquires the mutex to ensure any in-progress saveToStorage completes first.
func (m *VVManager) Shutdown() {
	m.mu.Lock()
	atomic.StoreInt32(&m.shutdown, 1)
	m.mu.Unlock()
}

// Reset clears a key's version vector.
func (m *VVManager) Reset(keyPath string) {
	baseKey := normalizeKeyPath(keyPath)

	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.vectors, baseKey)
	if m.storage != nil && m.storage.Active() {
		m.storage.Del(VVKeyPrefix + baseKey)
	}
}

// SetNodeID sets the node ID for this manager.
// Used by node servers to set their address once known.
func (m *VVManager) SetNodeID(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeID = nodeID
}

// GetNodeID returns the current node ID.
func (m *VVManager) GetNodeID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nodeID
}

// LogConflict logs a conflict detection event.
func LogConflict(keyPath string, localVV, remoteVV VersionVector, resolution string) {
	log.Printf("[pivot] CONFLICT detected for key %q: local=%v remote=%v resolution=%s",
		keyPath, localVV, remoteVV, resolution)
}
