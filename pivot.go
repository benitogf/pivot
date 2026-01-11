package pivot

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
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

// Config holds the configuration for pivot synchronization.
type Config struct {
	Keys            []Key        // Keys to sync (not including NodesKey)
	NodesKey        string       // Path for nodes - automatically synced via server.Storage, entries must have "ip" field
	PivotIP         string       // Address of the pivot server. Empty string means this server IS the pivot.
	Client          *http.Client // Optional HTTP client for sync requests. If nil, DefaultClient() is used.
	AutoSyncOnStart bool         // If true, perform full bidirectional sync with pivot when node server starts. Default false.
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

// makeGetNodes creates a function that returns node addresses from the NodesKey path.
// Only returns entries with a non-zero port - these are actual node servers.
func makeGetNodes(server *ooo.Server, nodesKey string) getNodes {
	return func() []string {
		var result []string
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

			// Look for port/Port field - handle both int and float64 (JSON numbers)
			var port int
			if v, ok := rawData["port"].(float64); ok {
				port = int(v)
			} else if v, ok := rawData["Port"].(float64); ok {
				port = int(v)
			} else if v, ok := rawData["port"].(int); ok {
				port = v
			} else if v, ok := rawData["Port"].(int); ok {
				port = v
			}

			// Only include entries with both ip and port
			if ip != "" && port > 0 {
				result = append(result, fmt.Sprintf("%s:%d", ip, port))
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

	// Create node health tracker for pivot servers
	// Health is tracked via periodic pings and sync success/failure
	var nodeHealth *NodeHealth
	if pivotIP == "" {
		nodeHealth = NewNodeHealth(client)
		// Set callback to broadcast to pivot/status subscribers when health changes
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
		// Start background health check every 3 seconds
		nodeHealth.StartBackgroundCheck(getNodes, 3*time.Second)
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
	if pivotIP == "" {
		originatorTracker = NewOriginatorTracker()
	}

	syncCallback := makeStorageSync(client, pivotIP, keys, getNodes, s, nodeHealth, originatorTracker)

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
	// Uses Pull() to sync FROM pivot only, avoiding conflicts with per-key sends
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
					s.TryPull()
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
		GetNodes:     getNodes,
		syncer:       s,
	}
	storeInstance(server, instance)

	// For node servers, start background pivot health check and initial sync
	if pivotIP != "" {
		stopHealthCheck := make(chan struct{})
		var healthCheckWg sync.WaitGroup

		// Stop health check when server closes and wait for it to finish
		// before Server.Close() modifies server.Stream
		existingOnClose := server.OnClose
		server.OnClose = func() {
			close(stopHealthCheck)
			healthCheckWg.Wait() // Wait for goroutine to exit before stream is modified
			if existingOnClose != nil {
				existingOnClose()
			}
		}

		// Start health check and set nodeAddr when server starts
		// (must wait for server.Start() to initialize Stream)
		existingOnStart := server.OnStart
		server.OnStart = func() {
			if existingOnStart != nil {
				existingOnStart()
			}
			// Set node address for originator tracking
			if s != nil {
				s.SetNodeAddr(server.Address)
			}
			// Start health check goroutine (after Stream is initialized)
			healthCheckWg.Add(1)
			go func() {
				defer healthCheckWg.Done()
				startPivotHealthCheck(client, pivotIP, instance, server, stopHealthCheck)
			}()
			// Perform initial sync with pivot on startup (if enabled)
			if config.AutoSyncOnStart && s != nil {
				server.Console.Log("pivot: performing initial sync with pivot on startup")
				if err := s.Sync(); err != nil {
					server.Console.Err("pivot: initial sync failed, starting background retry", err)
					go retryInitialSync(s, server.Console, stopHealthCheck)
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

// retryInitialSync retries the initial sync with exponential backoff until successful or stopped.
// Backoff starts at 1s and doubles each attempt up to 60s max.
func retryInitialSync(s *syncer, console *coat.Console, stop <-chan struct{}) {
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
		if err := s.Sync(); err != nil {
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
		wasHealthy := instance.PivotHealthy
		if err != nil {
			instance.PivotHealthy = false
			instance.PivotLastCheck = now
		} else {
			resp.Body.Close()
			instance.PivotHealthy = resp.StatusCode == http.StatusOK
			instance.PivotLastCheck = now
		}
		// Broadcast if status changed - check stop again before accessing server.Stream
		if wasHealthy != instance.PivotHealthy && !isStopped() {
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
func SyncDeleteFilter(client *http.Client, pivotIP string, db storage.Database, keyPath string, _getNodes GetNodes, nodeAddr string) func(index string) error {
	return func(index string) error {
		// Delete locally first
		err := db.Del(keyPath + "/" + index)
		if err != nil {
			return err
		}

		// If this is a node, send delete to pivot
		if pivotIP != "" {
			sendDelete(client, keyPath+"/"+index, pivotIP, time.Now().UnixNano(), nodeAddr)
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
