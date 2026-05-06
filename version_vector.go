package pivot

import (
	"encoding/json"
	"log"
	"maps"
	"slices"
	"strconv"
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
	maps.Copy(clone, vv)
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

// encodeVV produces the JSON-compatible byte representation of a VersionVector
// without going through encoding/json's reflection path. Keys are sorted
// alphabetically and quoted via strconv.AppendQuote.
//
// Scope of byte-equivalence: the output is byte-identical to json.Marshal of a
// map[string]int64 for the IDs we actually use as VV keys — LeaderID
// ("leader") and host:port strings produced by parseNodeAddr. Both producers
// are constrained to ASCII characters that need no escaping or use only the
// shared Go/JSON escape forms (\\, \", \n, \t, etc.). For arbitrary keys
// containing HTML-unsafe characters (<, >, &) or control characters other
// than \b\f\n\r\t, this encoder diverges from json.Marshal — keep the
// constraint at the producers, do not feed encodeVV unconstrained input.
//
// Decoding still uses json.Unmarshal in loadFromStorage, so on-disk format
// stays in sync with what json.Marshal would have written.
func encodeVV(vv VersionVector) []byte {
	if vv == nil {
		return []byte("null")
	}
	if len(vv) == 0 {
		return []byte("{}")
	}
	keys := make([]string, 0, len(vv))
	for k := range vv {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	// Pre-size: '{' + '}' + per-entry: 2 quotes + key + ':' + max int64 digits + ','.
	// Estimate generously to avoid grow-on-append.
	buf := make([]byte, 0, 2+len(keys)*32)
	buf = append(buf, '{')
	for i, k := range keys {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = strconv.AppendQuote(buf, k)
		buf = append(buf, ':')
		buf = strconv.AppendInt(buf, vv[k], 10)
	}
	buf = append(buf, '}')
	return buf
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
	return baseKeyFromPath(keyPath)
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

// increment bumps this node's counter in the version vector for a key and
// persists the result. Fire-and-forget: callers don't observe the new vector
// directly — peers read it via the activity handler (which calls Get and gets
// a Clone). Internal-only so we can keep the hot-path allocation-free without
// expanding the public API surface; tests in this package call it directly.
func (m *VVManager) increment(keyPath string) {
	baseKey := normalizeKeyPath(keyPath)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Node servers create their VVManager with nodeID="" during Setup and only
	// call SetNodeID once server.Address is known (inside OnStart). The TCP
	// listener is up before OnStart fires, so storage events can reach this
	// path with an empty nodeID. Persisting "" would gain a counter no peer
	// ever increments and live in storage forever. Skip and log loudly so the
	// regression surfaces if a caller starts incrementing pre-SetNodeID.
	if m.nodeID == "" {
		log.Printf("[pivot] VVManager.increment skipped for %q: nodeID not set yet", keyPath)
		return
	}

	if _, exists := m.vectors[baseKey]; !exists {
		m.loadFromStorage(baseKey)
	}
	if m.vectors[baseKey] == nil {
		m.vectors[baseKey] = make(VersionVector)
	}
	m.vectors[baseKey][m.nodeID]++

	m.saveToStorage(baseKey)
}

// set merges a remote version vector into the local one to ensure
// monotonicity. Internal-only; tests in this package call it directly.
func (m *VVManager) set(keyPath string, vv VersionVector) {
	baseKey := normalizeKeyPath(keyPath)

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.vectors[baseKey]; !exists {
		m.loadFromStorage(baseKey)
	}

	m.vectors[baseKey] = m.vectors[baseKey].Merge(vv)
	m.saveToStorage(baseKey)
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

	// Manual encoder skips encoding/json's reflection path. On-disk bytes are
	// identical to json.Marshal output (keys sorted), so existing data parses
	// unchanged via the json.Unmarshal in loadFromStorage.
	m.storage.Set(VVKeyPrefix+baseKey, encodeVV(m.vectors[baseKey]))
}

// Shutdown marks the manager as shutting down to prevent storage writes.
// Should be called before closing the storage.
// Acquires the mutex to ensure any in-progress saveToStorage completes first.
func (m *VVManager) Shutdown() {
	m.mu.Lock()
	atomic.StoreInt32(&m.shutdown, 1)
	m.mu.Unlock()
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

// logConflict logs a conflict detection event.
func logConflict(keyPath string, localVV, remoteVV VersionVector, resolution string) {
	log.Printf("[pivot] CONFLICT detected for key %q: local=%v remote=%v resolution=%s",
		keyPath, localVV, remoteVV, resolution)
}
