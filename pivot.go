package pivot

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
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

// Entries written to NodesKey must include "ip" and "port" fields (lower or
// upper case accepted) — this is the wire contract for parseNodeAddr.

const (
	// RoutePrefix is the HTTP route prefix for all pivot endpoints
	RoutePrefix = "/_pivot"
	// StoragePrefix is the key prefix for pivot metadata in storage
	StoragePrefix = "pivot/"
)

// Config holds the configuration for cluster synchronization.
type Config struct {
	Keys                []Key         // Keys to sync (not including NodesKey)
	NodesKey            string        // Path for nodes - automatically synced via server.Storage, entries must have "ip" field
	ExtraNodeURLs       []string      // Additional node URLs to sync with (not stored in NodesKey, e.g. auth servers)
	ClusterURL          string        // Address of the cluster leader. Empty string means this server IS the leader.
	Client              *http.Client  // Optional HTTP client for sync requests. If nil, an internal default is used.
	AutoSyncOnStart     bool          // If true, perform full bidirectional sync with cluster leader when node starts. Default false.
	SSL                 bool          // If true, use HTTPS instead of HTTP for all requests. Default false.
	HealthCheckInterval time.Duration // Interval for health checks. Default 3s.
	SyncRetryInterval   time.Duration // Initial backoff for sync retries. Default 1s.
}

// Scheme returns "https" if SSL is enabled, "http" otherwise.
func (c Config) Scheme() string {
	if c.SSL {
		return "https"
	}
	return "http"
}

// defaultClient returns an HTTP client tuned for pivot synchronization:
// short dial timeout to detect unreachable nodes quickly, longer response
// timeout for bulk data, and connection pooling.
func defaultClient() *http.Client {
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

// parseNodeAddr extracts "ip:port" from a node entry's JSON data.
// Returns "" if either field is missing or invalid. Accepts lower/upper case
// keys and int/float64/string port encodings.
func parseNodeAddr(data []byte) string {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return ""
	}
	var ip string
	if v, ok := raw["ip"].(string); ok {
		ip = v
	} else if v, ok := raw["IP"].(string); ok {
		ip = v
	}
	var port int
	for _, k := range [2]string{"port", "Port"} {
		// encoding/json decodes JSON numbers into float64 for map[string]any.
		// String fallback covers values written as quoted numbers.
		switch v := raw[k].(type) {
		case float64:
			port = int(v)
		case string:
			port, _ = strconv.Atoi(v)
		}
		if port > 0 {
			break
		}
	}
	if ip == "" || port <= 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", ip, port)
}

// nodesCache caches the resolved "ip:port" list from NodesKey so that
// GetNodes does not rescan and re-unmarshal every node entry on each call.
// Invalidation is driven by storage events via update(): settings-only
// changes to an entry (where ip/port don't change) are intentionally ignored,
// since they are common but irrelevant to the node-address list.
type nodesCache struct {
	server     *ooo.Server
	nodesKey   string
	isShutdown func() bool

	mu      sync.RWMutex
	loaded  bool
	entries map[string]string // obj.Index -> "ip:port"
	slice   []string          // immutable after rebuild; callers must not mutate
}

func newNodesCache(server *ooo.Server, nodesKey string, isShutdown func() bool) *nodesCache {
	return &nodesCache{
		server:     server,
		nodesKey:   nodesKey,
		isShutdown: isShutdown,
	}
}

// get returns the current list of node addresses. The returned slice is
// shared — callers must not mutate it.
func (c *nodesCache) get() []string {
	if c == nil || c.isShutdown() {
		return nil
	}
	c.mu.RLock()
	if c.loaded {
		s := c.slice
		c.mu.RUnlock()
		return s
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded {
		c.loadLocked()
	}
	return c.slice
}

// loadLocked does a full rebuild from storage. Caller must hold c.mu.
func (c *nodesCache) loadLocked() {
	c.entries = make(map[string]string)
	c.loaded = true
	if c.nodesKey == "" || c.server.Storage == nil || !c.server.Storage.Active() {
		c.slice = nil
		return
	}
	objs, err := c.server.Storage.GetList(c.nodesKey)
	if err != nil {
		c.slice = nil
		return
	}
	for _, obj := range objs {
		if addr := parseNodeAddr(obj.Data); addr != "" {
			c.entries[obj.Index] = addr
		}
	}
	c.rebuildSliceLocked()
}

// update processes a NodesKey storage event. Fast path when ip/port didn't
// change is ~1 json.Unmarshal + 1 string compare, no slice rebuild.
func (c *nodesCache) update(event storage.Event) {
	if c == nil || event.Object == nil || event.Object.Index == "" {
		return
	}
	idx := event.Object.Index

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded {
		// No cache yet — the next get() will do a full load.
		return
	}

	switch event.Operation {
	case "del":
		if _, had := c.entries[idx]; !had {
			return
		}
		delete(c.entries, idx)
		c.rebuildSliceLocked()
	case "set":
		newAddr := parseNodeAddr(event.Object.Data)
		prev, had := c.entries[idx]
		if had && newAddr == prev {
			// Settings-only change — ip:port unchanged, nothing to do.
			return
		}
		if newAddr == "" {
			if !had {
				return
			}
			delete(c.entries, idx)
		} else {
			c.entries[idx] = newAddr
		}
		c.rebuildSliceLocked()
	}
}

// rebuildSliceLocked produces a new sorted slice from the current entries.
// Copy-on-write: callers still holding the old slice remain correct.
func (c *nodesCache) rebuildSliceLocked() {
	s := make([]string, 0, len(c.entries))
	for _, addr := range c.entries {
		s = append(s, addr)
	}
	sort.Strings(s)
	c.slice = s
}

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

// makeGetNodes returns a function that resolves the full node list fresh from
// storage on every call. This preserves synchronous read-after-write semantics
// for external consumers (e.g., the UI, GetPivotInfo) who expect a node that
// was just registered to appear immediately.
func makeGetNodes(server *ooo.Server, nodesKey string, instance *Instance) getNodes {
	return func() []string {
		if instance.IsShutdown() {
			return nil
		}
		extras := instance.GetExtraNodeURLs()
		if nodesKey == "" || server.Storage == nil || !server.Storage.Active() {
			if len(extras) == 0 {
				return nil
			}
			return extras
		}
		objs, err := server.Storage.GetList(nodesKey)
		if err != nil {
			if len(extras) == 0 {
				return nil
			}
			return extras
		}
		result := make([]string, 0, len(extras)+len(objs))
		result = append(result, extras...)
		for _, obj := range objs {
			if addr := parseNodeAddr(obj.Data); addr != "" {
				result = append(result, addr)
			}
		}
		return result
	}
}

// makeGetNodesCached returns a function backed by instance.nodesCache.
// Used on the event-driven hot path (makeStorageSync), where update()
// has already reconciled the cache with the current event before
// GetNodes is called — so cache and storage are consistent at read time.
// Settings-only changes to node entries are a no-op inside update(),
// which is the whole point of caching here.
func makeGetNodesCached(instance *Instance) getNodes {
	return func() []string {
		if instance.IsShutdown() {
			return nil
		}
		extras := instance.GetExtraNodeURLs()
		cached := instance.nodesCache.get()
		if len(extras) == 0 {
			return cached
		}
		if len(cached) == 0 {
			return extras
		}
		result := make([]string, 0, len(extras)+len(cached))
		result = append(result, extras...)
		result = append(result, cached...)
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
		baseKey := baseKeyFromPath(k.Path)
		storageKey := pivotURLKeyPrefix + baseKey

		// Read stored pivot URL for this key. Persisted as a JSON-quoted
		// string so the storage layer's json.RawMessage round-trip succeeds.
		storedURL := readURLFingerprint(server.Storage, storageKey)

		if storedURL != "" && storedURL != effectiveURL {
			log.Printf("WARNING: Pivot URL for key %q changed from %q to %q - wiping data", k.Path, storedURL, effectiveURL)
			db := k.Database
			if db == nil {
				db = server.Storage
			}
			wipeStorage(db, k.Path)
			wiped = true
		}

		// Store/update the effective pivot URL for this key. The value goes
		// through the storage layer's json.RawMessage round-trip, so it must
		// be valid JSON — store as a quoted JSON string, not as raw bytes.
		encoded, err := json.Marshal(effectiveURL)
		if err != nil {
			log.Printf("[pivot] failed to encode URL fingerprint for %q: %v", k.Path, err)
			continue
		}
		if _, err := server.Storage.Set(storageKey, encoded); err != nil {
			log.Printf("[pivot] failed to persist URL fingerprint for %q: %v", k.Path, err)
		}
	}

	// Also check NodesKey (always uses configClusterURL)
	if config.NodesKey != "" {
		// Use base key (without wildcard) for storage key
		baseKey := baseKeyFromPath(config.NodesKey)
		storageKey := pivotURLKeyPrefix + baseKey
		storedURL := readURLFingerprint(server.Storage, storageKey)

		if storedURL != "" && storedURL != configClusterURL {
			log.Printf("WARNING: Pivot URL for NodesKey %q changed from %q to %q - wiping data", config.NodesKey, storedURL, configClusterURL)
			wipeStorage(server.Storage, config.NodesKey)
			wiped = true
		}

		encoded, err := json.Marshal(configClusterURL)
		if err != nil {
			log.Printf("[pivot] failed to encode URL fingerprint for NodesKey %q: %v", config.NodesKey, err)
		} else if _, err := server.Storage.Set(storageKey, encoded); err != nil {
			log.Printf("[pivot] failed to persist URL fingerprint for NodesKey %q: %v", config.NodesKey, err)
		}
	}

	return wiped
}

// readURLFingerprint reads a persisted pivot URL fingerprint. The on-disk
// format is a JSON-quoted string (so it survives the storage layer's
// json.RawMessage round-trip); a missing or unparseable entry yields the
// empty string, which the caller treats as "no prior fingerprint".
func readURLFingerprint(db storage.Database, storageKey string) string {
	obj, err := db.Get(storageKey)
	if err != nil {
		return ""
	}
	var url string
	if err := json.Unmarshal(obj.Data, &url); err != nil {
		// Pre-existing data written with the broken raw-bytes format will
		// fail to decode; treat as no fingerprint, the next write replaces
		// it with the correct format.
		return ""
	}
	return url
}

// wipeStorage deletes all entries matching the given path pattern
// and also deletes the associated activity metadata
func wipeStorage(db storage.Database, path string) {
	baseKey := baseKeyFromPath(path)
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

	// Use config.Client if provided, otherwise the internal default.
	client := config.Client
	if client == nil {
		client = defaultClient()
	}

	// Run the wipe-on-cluster-URL-change check immediately when storage is
	// already active (e.g. user pre-started embedded storage before calling
	// Setup). Doing it here lets the wipe's storage events run before the
	// user has had a chance to wrap server.OnStorageEvent post-Setup, so
	// downstream test/observer code only sees events for *intentional*
	// writes. The OnStart wrapper below covers the deferred case where
	// storage activates inside server.Start.
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
	instance.nodesCache = newNodesCache(server, config.NodesKey, instance.IsShutdown)

	getNodes := makeGetNodes(server, config.NodesKey, instance)
	getNodesCached := makeGetNodesCached(instance)

	// Create syncer pool for keys that need outbound sync
	// Keys with Local=true or where server IS pivot won't have syncers
	pool := newSyncerPool(client, keys, pivotURL, config.SSL)

	// Create node health tracker
	// NodeHealth is needed if server is pivot for ANY key (pure pivot or mixed role)
	hasPivotKeys := false
	for _, k := range keys {
		if k.EffectiveClusterURL(pivotURL) == "" {
			hasPivotKeys = true
			break
		}
	}
	var nodeHealth *NodeHealth
	if hasPivotKeys {
		nodeHealth = NewNodeHealthWithSSL(client, config.SSL)
		// Broadcast health changes
		nodeHealth.SetOnHealthChange(func() {
			// Check shutdown flag to avoid broadcasting after stream is closed
			if instance.IsShutdown() {
				return
			}
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
		healthCheckInterval := config.HealthCheckInterval
		if healthCheckInterval == 0 {
			healthCheckInterval = 3 * time.Second
		}
		existingOnStartPivot := server.OnStart
		server.OnStart = func() {
			if existingOnStartPivot != nil {
				existingOnStartPivot()
			}
			nodeHealth.StartBackgroundCheck(getNodes, healthCheckInterval)
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

	// Originator tracker is created on every server. On pivots it serves the
	// trigger-fanout role (skip echoing back to the peer that just wrote);
	// on both pivots and nodes it serves as the "handler is driving this VV
	// bump" dedup signal so the storage event callback doesn't double-bump.
	// Create version vector manager for all servers (pivot uses LeaderID,
	// nodes use their address).
	originatorTracker := NewOriginatorTracker()
	var vvManager *VVManager
	if pivotURL == "" {
		vvManager = NewVVManager(server.Storage, LeaderID)
	} else {
		// Node servers: VVManager will be initialized with node address once server starts
		// For now create with empty ID, will be set via SetNodeID later
		vvManager = NewVVManager(server.Storage, "")
	}
	instance.VVManager = vvManager

	// Per-node trigger coalescer — replaces the goroutine-per-event-per-node
	// fan-out from the broadcast loop. Only pivot servers broadcast, so we
	// only spin one up when this server has pivot-role keys.
	if hasPivotKeys {
		instance.triggers = newTriggerCoalescer(client, pool, nodeHealth)
	}

	// Shutdown instance and VVManager before storage closes to prevent race conditions.
	// removeInstance drops the registry entry so re-creating the server doesn't leak.
	server.RegisterPreClose(func() {
		instance.Shutdown()
		if vvManager != nil {
			vvManager.Shutdown()
		}
		if instance.triggers != nil {
			instance.triggers.Shutdown()
		}
		removeInstance(server)
	})

	syncCallback := makeStorageSync(StorageSyncConfig{
		Client:            client,
		ConfigClusterURL:  pivotURL,
		Keys:              keys,
		NodesKey:          config.NodesKey,
		GetNodes:          getNodesCached,
		Pool:              pool,
		NodeHealth:        nodeHealth,
		OriginatorTracker: originatorTracker,
		Instance:          instance,
	})

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
		baseKey := baseKeyFromPath(k.Path)
		server.Router.HandleFunc(RoutePrefix+"/activity/"+baseKey, Activity(k, vvManager)).Methods("GET")
		if baseKey != k.Path {
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+baseKey+"/{index:[a-zA-Z\\*\\d\\/]+}", Set(k.Database, baseKey, originatorTracker, vvManager)).Methods("POST")
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+baseKey+"/{index:[a-zA-Z\\*\\d\\/]+}/{time:[a-zA-Z\\*\\d\\/]+}", Delete(k.Database, baseKey, originatorTracker, vvManager)).Methods("DELETE")
		} else {
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+baseKey, Set(k.Database, baseKey, originatorTracker, vvManager)).Methods("POST")
			server.Router.HandleFunc(RoutePrefix+"/pivot/"+baseKey+"/{time:[a-zA-Z\\*\\d\\/]+}", Delete(k.Database, baseKey, originatorTracker, vvManager)).Methods("DELETE")
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
			// Set node address for originator tracking and VV manager
			if pool != nil {
				pool.SetNodeAddr(server.Address)
			}
			if vvManager != nil && vvManager.GetNodeID() == "" {
				vvManager.SetNodeID(server.Address)
			}
			// Start health check goroutine (after Stream is initialized)
			healthCheckWg.Go(func() {
				startPivotHealthCheck(HealthCheckConfig{
					PivotURL: pivotURL,
					Instance: instance,
					Server:   server,
					Stop:     stopHealthCheck,
					SSL:      config.SSL,
					Interval: config.HealthCheckInterval,
				})
			})
			// Perform initial sync with pivot on startup (if enabled)
			if config.AutoSyncOnStart && pool != nil && len(pool.syncers) > 0 {
				server.Console.Log("pivot: performing initial sync with pivot on startup")
				if err := pool.SyncAll(); err != nil {
					server.Console.Err("pivot: initial sync failed, starting background retry", err)
					go retryInitialSyncPool(pool, server.Console, stopHealthCheck, config.SyncRetryInterval)
				} else {
					server.Console.Log("pivot: initial sync completed successfully")
				}
			}
		}
	}

	// Run the wipe-on-cluster-URL-change check from OnStart so it executes
	// once storage is guaranteed active (memory-backed storages activate
	// inside server.Start, not before). Wraps the outermost OnStart so the
	// wipe runs before SetNodeAddr / SetNodeID / AutoSyncOnStart from the
	// node-startup wrapper above. Idempotent across restarts: a second
	// invocation reads the URL it just persisted and is a no-op.
	clusterURLCheckExisting := server.OnStart
	server.OnStart = func() {
		checkClusterURLChange(server, config, pivotURL)
		if clusterURLCheckExisting != nil {
			clusterURLCheckExisting()
		}
	}

	// Set GetPivotInfo on server for UI integration
	server.GetPivotInfo = GetPivotInfo(server)

	return server
}

// retryInitialSyncPool retries the initial sync for all syncers with exponential backoff until successful or stopped.
// Backoff starts at initialBackoff and doubles each attempt up to 60s max.
func retryInitialSyncPool(pool *syncerPool, console *coat.Console, stop <-chan struct{}, initialBackoff time.Duration) {
	const (
		maxBackoff = 60 * time.Second
		multiplier = 2.0
	)

	if initialBackoff == 0 {
		initialBackoff = 1 * time.Second
	}
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
			backoff = min(time.Duration(float64(backoff)*multiplier), maxBackoff)
		} else {
			console.Log("pivot: initial sync completed successfully after retry")
			return
		}
	}
}

// HealthCheckConfig holds configuration for pivot health check.
type HealthCheckConfig struct {
	PivotURL string
	Instance *Instance
	Server   *ooo.Server
	Stop     <-chan struct{}
	SSL      bool
	Interval time.Duration // Health check interval (default: 3s)
}

// startPivotHealthCheck periodically pings the pivot server and updates the instance health status
func startPivotHealthCheck(cfg HealthCheckConfig) {
	// Strip scheme prefix if present for URL construction
	pivotHost := strings.TrimPrefix(strings.TrimPrefix(cfg.PivotURL, "https://"), "http://")
	scheme := "http"
	if cfg.SSL {
		scheme = "https"
	}
	interval := cfg.Interval
	if interval == 0 {
		interval = 3 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Create cancellable context tied to stop channel
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-cfg.Stop
		cancel()
	}()

	// Health check client with short timeout (doesn't block shutdown)
	// Use interval as timeout to avoid blocking longer than one check cycle
	healthTimeout := min(interval, 2*time.Second)
	healthClient := &http.Client{Timeout: healthTimeout}

	checkHealth := func() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		url := scheme + "://" + pivotHost + "/"
		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		resp, err := healthClient.Do(req)
		now := time.Now().Format(time.RFC3339)

		// Check pivot version
		protocol := "unknown"
		compatible := false
		versionURL := scheme + "://" + pivotHost + RoutePrefix + "/version"
		versionReq, _ := http.NewRequestWithContext(ctx, "GET", versionURL, nil)
		versionResp, versionErr := healthClient.Do(versionReq)
		if versionErr == nil {
			defer versionResp.Body.Close()
			if versionResp.StatusCode == http.StatusOK {
				var info VersionInfo
				if json.NewDecoder(versionResp.Body).Decode(&info) == nil && info.Protocol != "" {
					protocol = info.Protocol
					compatible = info.Protocol == ProtocolVersion
				}
			}
		}

		cfg.Instance.healthMu.Lock()
		if cfg.Instance.PivotHealth == nil {
			cfg.Instance.PivotHealth = make(map[string]*PivotHealthStatus)
		}
		status := cfg.Instance.PivotHealth[cfg.PivotURL]
		if status == nil {
			status = &PivotHealthStatus{}
			cfg.Instance.PivotHealth[cfg.PivotURL] = status
		}
		wasHealthy := status.Healthy
		wasCompatible := status.Compatible
		if err != nil {
			status.Healthy = false
			status.LastCheck = now
		} else {
			resp.Body.Close()
			status.Healthy = resp.StatusCode == http.StatusOK
			status.LastCheck = now
		}
		status.Protocol = protocol
		status.Compatible = compatible
		cfg.Instance.healthMu.Unlock()

		// Broadcast if status changed
		select {
		case <-ctx.Done():
			return
		default:
		}
		if wasHealthy != status.Healthy || wasCompatible != status.Compatible {
			info := GetPivotInfo(cfg.Server)()
			data, _ := json.Marshal(info)
			now := time.Now().UTC().UnixNano()
			obj := meta.Object{
				Created: now,
				Updated: now,
				Index:   "pivot-status",
				Data:    data,
			}
			cfg.Server.Stream.Broadcast("pivot/status", stream.BroadcastOpt{
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
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkHealth()
		}
	}
}
