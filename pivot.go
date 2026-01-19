package pivot

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benitogf/coat"
	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/ooo/stream"
	"github.com/gorilla/mux"
)

// Node is the expected structure for entries in the NodesKey path.
// User data stored at NodesKey must include "ip" and "port" fields.
// Accepts both lowercase and uppercase field names for flexibility.
type Node struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
	// Alternative field names (uppercase)
	IPAlt   string `json:"IP"`
	PortAlt int    `json:"Port"`
}

// GetIP returns the IP from either field
func (n Node) GetIP() string {
	if n.IP != "" {
		return n.IP
	}
	return n.IPAlt
}

// GetPort returns the port from either field
func (n Node) GetPort() int {
	if n.Port > 0 {
		return n.Port
	}
	return n.PortAlt
}

const (
	// RoutePrefix is the HTTP route prefix for all pivot endpoints
	RoutePrefix = "/_pivot"
	// StoragePrefix is the key prefix for pivot metadata in storage
	StoragePrefix = "pivot/"
)

// Config holds the configuration for cluster synchronization.
type Config struct {
	Keys            []Key        // Keys to sync (not including NodesKey)
	NodesKey        string       // Path for nodes - automatically synced via server.Storage, entries must have "ip" field
	ExtraNodeURLs   []string     // Additional node URLs to sync with (not stored in NodesKey, e.g. auth servers)
	ClusterURL      string       // Address of the cluster leader. Empty string means this server IS the leader.
	Client          *http.Client // Optional HTTP client for sync requests. If nil, DefaultClient() is used.
	AutoSyncOnStart bool         // If true, perform full bidirectional sync with cluster leader when node starts. Default false.
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

// makeGetNodes creates a function that returns node addresses from the NodesKey path
// plus any extra node URLs from the Instance (can be added dynamically after Setup).
// Only returns entries with a non-zero port - these are actual node servers.
func makeGetNodes(server *ooo.Server, nodesKey string, instance *Instance) getNodes {
	return func() []string {
		// Start with extra node URLs (e.g., auth servers not in nodesKey)
		extraURLs := instance.GetExtraNodeURLs()
		result := make([]string, 0, len(extraURLs))
		result = append(result, extraURLs...)

		if nodesKey == "" {
			return result
		}
		if server.Storage == nil {
			return result
		}
		if !server.Storage.Active() {
			return result
		}
		objs, err := server.Storage.GetList(nodesKey)
		if err != nil {
			return result
		}
		for _, obj := range objs {
			// Try to parse as a map first to be more flexible with field names
			var rawData map[string]interface{}
			if err := json.Unmarshal(obj.Data, &rawData); err != nil {
				continue
			}

			// Look for ip/IP field
			var ip string
			if v, ok := rawData["ip"].(string); ok {
				ip = v
			} else if v, ok := rawData["IP"].(string); ok {
				ip = v
			}

			// Look for port/Port field - handle int, float64, and string
			var port int
			if v, ok := rawData["port"].(float64); ok {
				port = int(v)
			} else if v, ok := rawData["Port"].(float64); ok {
				port = int(v)
			} else if v, ok := rawData["port"].(int); ok {
				port = v
			} else if v, ok := rawData["Port"].(int); ok {
				port = v
			} else if v, ok := rawData["port"].(string); ok {
				port, _ = strconv.Atoi(v)
			} else if v, ok := rawData["Port"].(string); ok {
				port, _ = strconv.Atoi(v)
			}

			// Only include entries with both ip and port
			if ip != "" && port > 0 {
				result = append(result, fmt.Sprintf("%s:%d", ip, port))
			}
		}
		return result
	}
}

// pivotURLKeyPrefix is the storage key prefix for persisting per-key pivot URLs
const pivotURLKeyPrefix = StoragePrefix + "keyurl/"

// checkClusterURLChange checks if any key's effective pivot URL changed since last run.
// If changed, wipes data for that specific key to prevent contamination.
// Per-key pivot URLs are persisted in storage so they survive process restarts.
// Returns true if any data was wiped.
func checkClusterURLChange(server *ooo.Server, config Config, configClusterURL string) bool {
	// Skip if storage is not active (will be checked again when storage starts)
	if server.Storage == nil || !server.Storage.Active() {
		return false
	}

	wiped := false

	// Check each key's effective ClusterURL
	for _, k := range config.Keys {
		effectiveURL := k.EffectiveClusterURL(configClusterURL)
		// Use base key (without wildcard) for storage key to avoid issues with * in key names
		baseKey := strings.Replace(k.Path, "/*", "", 1)
		storageKey := pivotURLKeyPrefix + baseKey

		// Read stored pivot URL for this key
		obj, err := server.Storage.Get(storageKey)
		storedURL := ""
		if err == nil {
			storedURL = string(obj.Data)
		}

		if storedURL != "" && storedURL != effectiveURL {
			log.Printf("WARNING: Pivot URL for key %q changed from %q to %q - wiping data", k.Path, storedURL, effectiveURL)
			db := k.Database
			if db == nil {
				db = server.Storage
			}
			wipeStorage(db, k.Path)
			wiped = true
		}

		// Store/update the effective pivot URL for this key
		server.Storage.Set(storageKey, []byte(effectiveURL))
	}

	// Also check NodesKey (always uses configClusterURL)
	if config.NodesKey != "" {
		// Use base key (without wildcard) for storage key
		baseKey := strings.Replace(config.NodesKey, "/*", "", 1)
		storageKey := pivotURLKeyPrefix + baseKey
		obj, err := server.Storage.Get(storageKey)
		storedURL := ""
		if err == nil {
			storedURL = string(obj.Data)
		}

		if storedURL != "" && storedURL != configClusterURL {
			log.Printf("WARNING: Pivot URL for NodesKey %q changed from %q to %q - wiping data", config.NodesKey, storedURL, configClusterURL)
			wipeStorage(server.Storage, config.NodesKey)
			wiped = true
		}

		server.Storage.Set(storageKey, []byte(configClusterURL))
	}

	return wiped
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
	// Initialize router if not set
	if server.Router == nil {
		server.Router = mux.NewRouter()
	}

	// Initialize storage if not set
	if server.Storage == nil {
		server.Storage = storage.New(storage.LayeredConfig{
			Memory: storage.NewMemoryLayer(),
		})
	}

	pivotURL := config.ClusterURL

	// Validate key configurations
	for _, k := range config.Keys {
		// Panic if both Local and ClusterURL are set - conflicting configuration
		if k.Local && k.ClusterURL != "" {
			panic("pivot: Key " + k.Path + " has both Local=true and ClusterURL set - these are mutually exclusive")
		}
		// Panic if Config.ClusterURL is empty but Key.ClusterURL is set without Local
		// This would mean the server is a pivot but trying to sync some keys from another pivot
		// which requires the server to also be a node (have Config.ClusterURL set)
		if pivotURL == "" && k.ClusterURL != "" {
			panic("pivot: Key " + k.Path + " has ClusterURL set but Config.ClusterURL is empty - set Config.ClusterURL or use Local=true for local keys")
		}
	}

	// Use config.Client if provided, otherwise use DefaultClient()
	client := config.Client
	if client == nil {
		client = DefaultClient()
	}

	// Check if pivot IP changed since last run - wipe data if so
	checkClusterURLChange(server, config, pivotURL)

	keys := buildKeys(server, config)

	// Create instance early so makeGetNodes can reference it for dynamic ExtraNodeURLs
	var keyPaths []string
	for _, k := range keys {
		keyPaths = append(keyPaths, k.Path)
	}
	instance := &Instance{
		ClusterURL:    pivotURL,
		SyncedKeys:    keyPaths,
		ExtraNodeURLs: config.ExtraNodeURLs,
	}

	getNodes := makeGetNodes(server, config.NodesKey, instance)

	// Create syncer pool for keys that need outbound sync
	// Keys with Local=true or where server IS pivot won't have syncers
	pool := newSyncerPool(client, keys, pivotURL)

	// Create node health tracker
	var nodeHealth *NodeHealth
	if pivotURL == "" {
		nodeHealth = NewNodeHealth(client)
		// Broadcast health changes
		nodeHealth.SetOnHealthChange(func() {
			info := GetPivotInfo(server)()
			data, _ := json.Marshal(info)
			now := time.Now().UTC().UnixNano()
			obj := meta.Object{
				Created: now,
				Updated: now,
				Index:   "pivot-status",
				Data:    data,
			}
			server.Stream.Broadcast("pivot/status", stream.BroadcastOpt{
				Key:       "pivot/status",
				Operation: "set",
				Object:    &obj,
				FilterObject: func(key string, o meta.Object) (meta.Object, error) {
					return o, nil
				},
			})
		})
		// Stop NodeHealth before stream is closed
		server.RegisterPreClose(func() {
			nodeHealth.Stop()
		})
		// Start background health check in OnStart (after storage is initialized)
		existingOnStartPivot := server.OnStart
		server.OnStart = func() {
			if existingOnStartPivot != nil {
				existingOnStartPivot()
			}
			nodeHealth.StartBackgroundCheck(getNodes, 3*time.Second)
		}
	}

	// Create read filter for pivot/status WebSocket subscription (works for both pivot and node)
	server.ReadObjectFilter("pivot/status", func(key string, obj meta.Object) (meta.Object, error) {
		info := GetPivotInfo(server)()
		data, _ := json.Marshal(info)
		now := time.Now().UTC().UnixNano()
		return meta.Object{
			Created: now,
			Updated: now,
			Index:   "pivot-status",
			Data:    data,
		}, nil
	})

	// Create originator tracker for pivot servers to skip TriggerNodeSync back to originating node
	var originatorTracker *OriginatorTracker
	if pivotURL == "" {
		originatorTracker = NewOriginatorTracker()
	}

	syncCallback := makeStorageSync(client, pivotURL, keys, getNodes, pool, nodeHealth, originatorTracker, instance)

	// Set up OnStorageEvent for write/delete synchronization on server.Storage
	server.OnStorageEvent = storage.EventCallback(syncCallback)

	// Set up HTTP routes for pivot protocol
	// /synchronize/pivot - pull-only sync, used when pivot triggers sync on node (e.g., after delete)
	// /synchronize/node - bidirectional sync, used when node has local changes to push
	server.Router.HandleFunc(RoutePrefix+"/synchronize/pivot", SynchronizePivotHandler(pivotURL, pool)).Methods("GET")
	server.Router.HandleFunc(RoutePrefix+"/synchronize/node", SynchronizeNodeHandler(pivotURL, pool)).Methods("GET")

	// Node health endpoint (only meaningful on pivot servers)
	server.Router.HandleFunc(RoutePrefix+"/health/nodes", NodeHealthHandler(nodeHealth)).Methods("GET")
	// Version endpoint for protocol detection
	server.Router.HandleFunc(RoutePrefix+"/version", VersionHandler()).Methods("GET")
	for _, k := range keys {
		baseKey := strings.Replace(k.Path, "/*", "", 1)
		server.Router.HandleFunc(RoutePrefix+"/activity/"+baseKey, Activity(k)).Methods("GET")
		if baseKey != k.Path {
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+baseKey+"/{index:[a-zA-Z\\*\\d\\/]+}", Set(k.Database, baseKey, originatorTracker)).Methods("POST")
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+baseKey+"/{index:[a-zA-Z\\*\\d\\/]+}/{time:[a-zA-Z\\*\\d\\/]+}", Delete(k.Database, baseKey, originatorTracker)).Methods("DELETE")
		} else {
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+baseKey, Set(k.Database, baseKey, originatorTracker)).Methods("POST")
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+baseKey+"/{time:[a-zA-Z\\*\\d\\/]+}", Delete(k.Database, baseKey, originatorTracker)).Methods("DELETE")
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
	// Uses TryPullKey() on the appropriate syncer based on key's effective ClusterURL
	var syncing int32
	beforeRead := func(readKey string) {
		if !atomic.CompareAndSwapInt32(&syncing, 0, 1) {
			return
		}
		defer atomic.StoreInt32(&syncing, 0)
		for _, k := range keys {
			if key.Match(k.Path, readKey) {
				// Find the syncer for this key's effective ClusterURL
				effectiveURL := k.EffectiveClusterURL(pivotURL)
				if effectiveURL != "" && pool != nil {
					if s := pool.syncers[effectiveURL]; s != nil {
						s.TryPullKey(k.Path)
					}
				}
				return
			}
		}
	}

	// Assign BeforeRead to server
	server.BeforeRead = beforeRead

	// Complete instance setup and store for GetInstance lookup
	instance.BeforeRead = beforeRead
	instance.SyncCallback = syncCallback
	instance.NodeHealth = nodeHealth
	instance.GetNodes = getNodes
	instance.syncerPool = pool
	storeInstance(server, instance)

	// For node servers, start background pivot health check and initial sync
	if pivotURL != "" {
		stopHealthCheck := make(chan struct{})
		var healthCheckWg sync.WaitGroup

		// Stop health check before stream is closed
		server.RegisterPreClose(func() {
			close(stopHealthCheck)
			healthCheckWg.Wait() // Wait for goroutine to exit before stream is modified
		})

		// Start health check and set nodeAddr when server starts
		// (must wait for server.Start() to initialize Stream)
		existingOnStart := server.OnStart
		server.OnStart = func() {
			if existingOnStart != nil {
				existingOnStart()
			}
			// Set node address for originator tracking
			if pool != nil {
				pool.SetNodeAddr(server.Address)
			}
			// Start health check goroutine (after Stream is initialized)
			healthCheckWg.Add(1)
			go func() {
				defer healthCheckWg.Done()
				startPivotHealthCheck(client, pivotURL, instance, server, stopHealthCheck)
			}()
			// Perform initial sync with pivot on startup (if enabled)
			if config.AutoSyncOnStart && pool != nil && len(pool.syncers) > 0 {
				server.Console.Log("pivot: performing initial sync with pivot on startup")
				if err := pool.SyncAll(); err != nil {
					server.Console.Err("pivot: initial sync failed, starting background retry", err)
					go retryInitialSyncPool(pool, server.Console, stopHealthCheck)
				} else {
					server.Console.Log("pivot: initial sync completed successfully")
				}
			}
		}
	}

	// Set GetPivotInfo on server for UI integration
	server.GetPivotInfo = GetPivotInfo(server)

	return server
}

// retryInitialSyncPool retries the initial sync for all syncers with exponential backoff until successful or stopped.
// Backoff starts at 1s and doubles each attempt up to 60s max.
func retryInitialSyncPool(pool *syncerPool, console *coat.Console, stop <-chan struct{}) {
	const (
		initialBackoff = 1 * time.Second
		maxBackoff     = 60 * time.Second
		multiplier     = 2.0
	)

	backoff := initialBackoff
	attempt := 0

	for {
		attempt++
		select {
		case <-stop:
			console.Log("pivot: stopping initial sync retry (server closing)")
			return
		default:
		}

		// Wait for backoff duration, but check stop channel
		timer := time.NewTimer(backoff)
		select {
		case <-stop:
			timer.Stop()
			console.Log("pivot: stopping initial sync retry (server closing)")
			return
		case <-timer.C:
		}

		console.Log(fmt.Sprintf("pivot: retrying initial sync (attempt %d, backoff %v)", attempt, backoff))
		if err := pool.SyncAll(); err != nil {
			console.Err(fmt.Sprintf("pivot: initial sync retry %d failed", attempt), err)
			// Increase backoff with exponential growth, capped at max
			backoff = time.Duration(float64(backoff) * multiplier)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		} else {
			console.Log("pivot: initial sync completed successfully after retry")
			return
		}
	}
}

// startPivotHealthCheck periodically pings the pivot server and updates the instance health status
func startPivotHealthCheck(client *http.Client, pivotIP string, instance *Instance, server *ooo.Server, stop <-chan struct{}) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// isStopped checks if the stop channel is closed
	isStopped := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	checkHealth := func() {
		// Check if stopped before doing any work
		if isStopped() {
			return
		}
		url := "http://" + pivotIP + "/"
		resp, err := client.Get(url)
		now := time.Now().Format(time.RFC3339)

		instance.healthMu.Lock()
		if instance.PivotHealth == nil {
			instance.PivotHealth = make(map[string]*PivotHealthStatus)
		}
		status := instance.PivotHealth[pivotIP]
		if status == nil {
			status = &PivotHealthStatus{}
			instance.PivotHealth[pivotIP] = status
		}
		wasHealthy := status.Healthy
		if err != nil {
			status.Healthy = false
			status.LastCheck = now
		} else {
			resp.Body.Close()
			status.Healthy = resp.StatusCode == http.StatusOK
			status.LastCheck = now
		}
		instance.healthMu.Unlock()

		// Broadcast if status changed - check stop again before accessing server.Stream
		if wasHealthy != status.Healthy && !isStopped() {
			info := GetPivotInfo(server)()
			data, _ := json.Marshal(info)
			now := time.Now().UTC().UnixNano()
			obj := meta.Object{
				Created: now,
				Updated: now,
				Index:   "pivot-status",
				Data:    data,
			}
			server.Stream.Broadcast("pivot/status", stream.BroadcastOpt{
				Key:       "pivot/status",
				Operation: "set",
				Object:    &obj,
				FilterObject: func(key string, o meta.Object) (meta.Object, error) {
					return o, nil
				},
			})
		}
	}

	// Initial check
	checkHealth()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			checkHealth()
		}
	}
}

// SyncDeleteFilter returns a delete filter that syncs deletes to pivot and notifies nodes.
// This is for backward compatibility with code that uses the old pivot API.
// nodeAddr is the address of this node (empty for pivot servers).
// nodeHealth is optional - if provided, incompatible nodes will be skipped.
func SyncDeleteFilter(client *http.Client, pivotURL string, db storage.Database, keyPath string, _getNodes GetNodes, nodeAddr string, nodeHealth *NodeHealth) func(index string) error {
	return func(index string) error {
		// Delete locally first
		err := db.Del(keyPath + "/" + index)
		if err != nil {
			return err
		}

		// If this is a node, send delete to pivot
		if pivotURL != "" {
			sendDelete(client, keyPath+"/"+index, pivotURL, time.Now().UnixNano(), nodeAddr)
		} else {
			// If this is pivot, notify all nodes
			nodes := _getNodes()
			if len(nodes) > 0 {
				var wg sync.WaitGroup
				for _, node := range nodes {
					// Skip incompatible nodes
					if nodeHealth != nil && !nodeHealth.IsCompatible(node) {
						continue
					}
					wg.Add(1)
					go func(n string) {
						defer wg.Done()
						TriggerNodeSyncWithHealth(client, n, "")
					}(node)
				}
				wg.Wait()
			}
		}
		return nil
	}
}
