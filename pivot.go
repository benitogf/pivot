package pivot

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/storage"
)

// Node is the expected structure for entries in the NodesKey path.
// User data stored at NodesKey must include an "ip" field.
type Node struct {
	IP string `json:"ip"`
}

const (
	// RoutePrefix is the HTTP route prefix for all pivot endpoints
	RoutePrefix = "/_pivot"
	// StoragePrefix is the key prefix for pivot metadata in storage
	StoragePrefix = "pivot/"
)

// Config holds the configuration for pivot synchronization.
type Config struct {
	Keys     []Key        // Keys to sync (not including NodesKey)
	NodesKey string       // Path for nodes - automatically synced via server.Storage, entries must have "ip" field
	PivotIP  string       // Address of the pivot server. Empty string means this server IS the pivot.
	Client   *http.Client // Optional HTTP client for sync requests. If nil, DefaultClient() is used.
}

// DefaultClient returns an HTTP client optimized for pivot synchronization.
// - Short dial timeout (500ms) to quickly detect unreachable nodes
// - Longer response timeout (30s) to handle large data transfers
// - Connection pooling for efficiency
func DefaultClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   500 * time.Millisecond,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second,
		},
	}
}

// getNodes function type used internally for node discovery
type getNodes func() []string

// GetNodes is the exported type for node discovery functions (backward compatibility)
type GetNodes func() []string

// buildKeys constructs the full keys list from config.
// Keys with nil Database are filled with server.Storage.
// NodesKey is appended if not already present.
func buildKeys(server *ooo.Server, config Config) []Key {
	keys := make([]Key, len(config.Keys))
	copy(keys, config.Keys)

	// Fill nil Database with server.Storage
	for i := range keys {
		if keys[i].Database == nil {
			keys[i].Database = server.Storage
		}
	}

	// Append NodesKey if not already present
	if config.NodesKey != "" {
		found := false
		for _, k := range keys {
			if k.Path == config.NodesKey {
				found = true
				break
			}
		}
		if !found {
			keys = append(keys, Key{Path: config.NodesKey, Database: server.Storage})
		}
	}

	return keys
}

// makeGetNodes creates a function that returns node IPs from the NodesKey path.
func makeGetNodes(server *ooo.Server, nodesKey string) getNodes {
	return func() []string {
		var result []string
		if nodesKey == "" || server.Storage == nil {
			return result
		}
		objs, err := server.Storage.GetList(nodesKey)
		if err != nil {
			return result
		}
		for _, obj := range objs {
			var node Node
			if err := json.Unmarshal(obj.Data, &node); err != nil {
				continue
			}
			if node.IP != "" {
				result = append(result, node.IP)
			}
		}
		return result
	}
}

// pivotIPKey is the storage key for persisting the pivot IP
const pivotIPKey = StoragePrefix + "pivotip"

// checkPivotIPChange checks if the pivot IP changed since last run for this storage.
// If changed, wipes all data for the configured keys to prevent contamination.
// The pivot IP is persisted in storage so it survives process restarts.
// Returns true if data was wiped.
func checkPivotIPChange(server *ooo.Server, config Config, pivotIP string) bool {
	// Skip if storage is not active (will be checked again when storage starts)
	if server.Storage == nil || !server.Storage.Active() {
		return false
	}

	// Read stored pivot IP from storage
	obj, err := server.Storage.Get(pivotIPKey)
	storedIP := ""
	if err == nil {
		storedIP = string(obj.Data)
	}

	if storedIP != "" && storedIP != pivotIP {
		log.Printf("WARNING: Pivot IP changed from %q to %q - wiping all synced data", storedIP, pivotIP)
		// Wipe all data for configured keys
		for _, k := range config.Keys {
			db := k.Database
			if db == nil {
				db = server.Storage
			}
			wipeStorage(db, k.Path)
		}
		// Wipe nodes key data
		if config.NodesKey != "" {
			wipeStorage(server.Storage, config.NodesKey)
		}
		// Update stored pivot IP after wipe
		server.Storage.Set(pivotIPKey, []byte(pivotIP))
		return true
	}

	// First run or same pivot IP - just store/update
	server.Storage.Set(pivotIPKey, []byte(pivotIP))
	return false
}

// wipeStorage deletes all entries matching the given path pattern
// and also deletes the associated activity metadata
func wipeStorage(db storage.Database, path string) {
	baseKey := strings.Replace(path, "/*", "", 1)
	// Delete activity metadata
	db.Del(StoragePrefix + baseKey)

	// For wildcard paths, get all entries and delete individually
	if key.LastIndex(path) == "*" {
		objs, err := db.GetList(path)
		if err != nil {
			return
		}
		for _, obj := range objs {
			db.Del(baseKey + "/" + obj.Index)
		}
	} else {
		db.Del(path)
	}
}

// Setup configures pivot synchronization on the server.
// It modifies the server by setting routes, OnStorageEvent, and BeforeRead.
// Returns the server to make side-effects explicit.
// Use GetInstance(server) to access BeforeRead/SyncCallback for external storages.
func Setup(server *ooo.Server, config Config) *ooo.Server {
	pivotIP := config.PivotIP

	// Use config.Client if provided, otherwise use DefaultClient()
	client := config.Client
	if client == nil {
		client = DefaultClient()
	}

	// Check if pivot IP changed since last run - wipe data if so
	checkPivotIPChange(server, config, pivotIP)

	keys := buildKeys(server, config)
	getNodes := makeGetNodes(server, config.NodesKey)

	// Create syncer for node servers (nil for pivot servers)
	var s *syncer
	if pivotIP != "" {
		s = newSyncer(client, pivotIP, keys)
	}

	// Create node health tracker for pivot servers to skip unhealthy nodes
	var nodeHealth *NodeHealth
	if pivotIP == "" {
		nodeHealth = NewNodeHealth(client)
		nodeHealth.StartBackgroundCheck(getNodes, server.Active)
	}

	syncCallback := makeStorageSync(client, pivotIP, keys, getNodes, s, nodeHealth)

	// Set up OnStorageEvent for write/delete synchronization on server.Storage
	server.OnStorageEvent = storage.EventCallback(syncCallback)

	// Set up HTTP routes for pivot protocol
	// /synchronize/pivot - pull-only sync, used when pivot triggers sync on node (e.g., after delete)
	// /synchronize/node - bidirectional sync, used when node has local changes to push
	server.Router.HandleFunc(RoutePrefix+"/synchronize/pivot", SynchronizePivotHandler(pivotIP, s)).Methods("GET")
	server.Router.HandleFunc(RoutePrefix+"/synchronize/node", SynchronizeNodeHandler(pivotIP, s)).Methods("GET")

	// Node health endpoint (only meaningful on pivot servers)
	server.Router.HandleFunc(RoutePrefix+"/health/nodes", NodeHealthHandler(nodeHealth)).Methods("GET")
	for _, k := range keys {
		baseKey := strings.Replace(k.Path, "/*", "", 1)
		server.Router.HandleFunc(RoutePrefix+"/activity/"+baseKey, Activity(k)).Methods("GET")
		if baseKey != k.Path {
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+baseKey+"/{index:[a-zA-Z\\*\\d\\/]+}", Set(k.Database, baseKey)).Methods("POST")
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+baseKey+"/{index:[a-zA-Z\\*\\d\\/]+}/{time:[a-zA-Z\\*\\d\\/]+}", Delete(k.Database, baseKey)).Methods("DELETE")
		} else {
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+baseKey, Set(k.Database, baseKey)).Methods("POST")
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+baseKey+"/{time:[a-zA-Z\\*\\d\\/]+}", Delete(k.Database, baseKey)).Methods("DELETE")
		}
		// Expose GET routes for all synced keys
		if baseKey != k.Path {
			// List pattern like "users/*" - register as "/users/*" to handle the wildcard
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+baseKey+"/{path:.*}", GetList(k.Database, k.Path)).Methods("GET")
		} else {
			// Single key like "settings" - use GetSingle
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+k.Path, GetSingle(k.Database, k.Path)).Methods("GET")
		}
	}

	// Create BeforeRead callback for sync-on-read
	// Uses trySync to avoid blocking if sync is already in progress
	var syncing int32
	beforeRead := func(readKey string) {
		if pivotIP == "" {
			return
		}
		if !atomic.CompareAndSwapInt32(&syncing, 0, 1) {
			return
		}
		defer atomic.StoreInt32(&syncing, 0)
		for _, k := range keys {
			if key.Match(k.Path, readKey) {
				if s != nil {
					s.TrySync()
				}
				return
			}
		}
	}

	// Assign BeforeRead to server
	server.BeforeRead = beforeRead

	// Store instance for GetInstance lookup
	// Include pivot info for UI integration
	var keyPaths []string
	for _, k := range keys {
		keyPaths = append(keyPaths, k.Path)
	}
	instance := &Instance{
		BeforeRead:   beforeRead,
		SyncCallback: syncCallback,
		PivotIP:      pivotIP,
		SyncedKeys:   keyPaths,
		NodeHealth:   nodeHealth,
	}
	storeInstance(server, instance)

	return server
}

// SyncDeleteFilter returns a delete filter that syncs deletes to pivot and notifies nodes.
// This is for backward compatibility with code that uses the old pivot API.
func SyncDeleteFilter(client *http.Client, pivotIP string, db storage.Database, keyPath string, _getNodes GetNodes) func(index string) error {
	return func(index string) error {
		// Delete locally first
		err := db.Del(keyPath + "/" + index)
		if err != nil {
			return err
		}

		// If this is a node, send delete to pivot
		if pivotIP != "" {
			sendDelete(client, keyPath+"/"+index, pivotIP, time.Now().UnixNano())
		} else {
			// If this is pivot, notify all nodes
			nodes := _getNodes()
			if len(nodes) > 0 {
				var wg sync.WaitGroup
				for _, node := range nodes {
					wg.Add(1)
					go func(n string) {
						defer wg.Done()
						TriggerNodeSyncWithHealth(client, n)
					}(node)
				}
				wg.Wait()
			}
		}
		return nil
	}
}
