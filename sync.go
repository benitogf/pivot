package pivot

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/storage"
)

// SyncOptions holds configuration for sync operations.
type SyncOptions struct {
	Key            Key    // Key configuration to sync
	Originator     string // Node address for originator tracking
	LastEntry      int64  // Last known leader activity timestamp
	OnDelete       func(key string)
	OnSet          func(key string)
	SkipSet        func(key string) bool
	IsRecentDelete func(key string) bool
}

// baseKeyFromPath strips all trailing glob segments (/*) from a path.
// "items/*/*/*" → "items", "things/*" → "things", "settings" → "settings"
func baseKeyFromPath(path string) string {
	for strings.HasSuffix(path, "/*") {
		path = strings.TrimSuffix(path, "/*")
	}
	return path
}

// syncLocalEntriesWithTracking syncs from leader and tracks synced keys via callbacks.
// onDelete is called for each key deleted locally (item exists locally but not on leader)
// onSet is called for each key set locally (item exists on leader but not locally, or leader is newer)
// skipSet is called before setting - if it returns true, the set is skipped (for locally deleted keys)
func syncLocalEntriesWithTracking(clientOpts ClientOpts, opts SyncOptions) error {
	_key := opts.Key
	lastEntry := opts.LastEntry
	onDelete := opts.OnDelete
	onSet := opts.OnSet
	skipSet := opts.SkipSet
	if _key.Database == nil || !_key.Database.Active() {
		return ErrStorageNotActive
	}
	if key.LastIndex(_key.Path) == "*" {
		baseKey := baseKeyFromPath(_key.Path)
		objsLeader, err := getEntriesFromLeader(clientOpts, _key.Path)
		if err != nil {
			return err
		}

		objsLocal, err := _key.Database.GetList(_key.Path)
		if err != nil {
			objsLocal = []meta.Object{}
		}

		// Build index→path map from local objects for multi-glob key reconstruction
		indexToPath := make(map[string]string, len(objsLocal))
		for _, obj := range objsLocal {
			indexToPath[obj.Index] = obj.Path
		}

		objsToDelete := GetEntriesNegativeDiff(objsLocal, objsLeader)
		for _, index := range objsToDelete {
			fullKey := indexToPath[index]
			// Track BEFORE delete so storage callback can skip this event
			if onDelete != nil {
				onDelete(fullKey)
			}
			_key.Database.Del(fullKey)
		}

		objsToSend := GetEntriesPositiveDiff(objsLocal, objsLeader)
		for _, obj := range objsToSend {
			fullKey := obj.Path
			// Skip if this key was locally deleted and not yet synced
			if skipSet != nil && skipSet(fullKey) {
				continue
			}
			// Track BEFORE set so storage callback can skip this event
			if onSet != nil {
				onSet(fullKey)
			}
			_key.Database.SetWithMeta(fullKey, obj.Data, obj.Created, obj.Updated)
		}
		// Only update activity timestamp if items were actually synced
		if len(objsToDelete) > 0 || len(objsToSend) > 0 {
			_key.Database.Set(StoragePrefix+baseKey, json.RawMessage(strconv.FormatInt(lastEntry, 10)))
		}
		return nil
	}

	obj, err := getEntryFromLeader(clientOpts, _key.Path)
	if err != nil {
		// Key doesn't exist on pivot - check if it exists locally and delete it
		_, localErr := _key.Database.Get(_key.Path)
		if localErr == nil {
			// Key exists locally but not on pivot - delete it
			if onDelete != nil {
				onDelete(_key.Path)
			}
			_key.Database.Del(_key.Path)
		}
		// Clear delete timestamp so activity becomes 0 (matching pivot's 0)
		_key.Database.Del(StoragePrefix + _key.Path)
		return nil
	}
	// Skip if this key was locally deleted and not yet synced
	if skipSet != nil && skipSet(_key.Path) {
		return nil
	}
	// Re-check: if a local delete happened while we were fetching from pivot, skip the set
	// This prevents a racing delete from being overwritten by stale pivot data
	localDeleteTs, delErr := _key.Database.Get(StoragePrefix + _key.Path)
	if delErr == nil && len(localDeleteTs.Data) > 0 {
		// Local delete timestamp exists - check if it's newer than pivot data
		deleteTime, _ := strconv.ParseInt(string(localDeleteTs.Data), 10, 64)
		if deleteTime > obj.Created {
			// Local delete is newer than pivot data - skip the set
			return nil
		}
	}
	// Check if local data exists and is up-to-date (skip write if timestamps match)
	localObj, localErr := _key.Database.Get(_key.Path)
	if localErr == nil && localObj.Updated >= obj.Updated {
		// Local data is same or newer - no need to write
		return nil
	}
	// Track BEFORE set so storage callback can skip this event
	if onSet != nil {
		onSet(_key.Path)
	}
	_key.Database.SetWithMeta(_key.Path, obj.Data, obj.Created, obj.Updated)
	// Clear delete timestamp so activity is based on object timestamp
	_key.Database.Del(StoragePrefix + _key.Path)

	return nil
}

// syncToLeader syncs local entries to leader with an optional delete check.
// If isRecentDelete is provided, items that were recently deleted by a pull-only sync
// will not be re-added to leader.
// This function also sends delete commands to leader for items that were deleted locally.
func syncToLeader(clientOpts ClientOpts, opts SyncOptions) error {
	_key := opts.Key
	leaderActivity := opts.LastEntry
	isRecentDelete := opts.IsRecentDelete
	originator := opts.Originator

	if _key.Database == nil || !_key.Database.Active() {
		return ErrStorageNotActive
	}
	if key.LastIndex(_key.Path) == "*" {
		baseKey := baseKeyFromPath(_key.Path)
		objsLocal, err := _key.Database.GetList(_key.Path)
		if err != nil {
			objsLocal = []meta.Object{}
		}

		objsLeader, headerActivity, hasActivity, err := getEntriesAndActivityFromLeader(clientOpts, _key.Path)
		if err != nil {
			return err
		}

		// Pick up the latest leader activity (including any recent delete tombstones)
		// so we don't resurrect items the leader just deleted. New leaders piggyback
		// it as a response header on GetList; older leaders need a separate call.
		if hasActivity {
			if headerActivity > leaderActivity {
				leaderActivity = headerActivity
			}
		} else {
			latestActivity, actErr := checkLeaderActivity(clientOpts, baseKey)
			if actErr == nil && latestActivity.LastEntry > leaderActivity {
				leaderActivity = latestActivity.LastEntry
			}
		}

		// Build map of local entries for O(1) lookup
		localEntries := make(map[string]meta.Object, len(objsLocal))
		for _, obj := range objsLocal {
			localEntries[obj.Index] = obj
		}

		// Build map of leader entries for O(1) lookup
		leaderEntries := make(map[string]meta.Object, len(objsLeader))
		for _, obj := range objsLeader {
			leaderEntries[obj.Index] = obj
		}

		// Send local items to leader (new or updated)
		for _, objLocal := range objsLocal {
			fullKey := objLocal.Path

			// Skip items that were recently synced by a pull-only sync
			if isRecentDelete != nil && isRecentDelete(fullKey) {
				continue
			}

			if objLeader, exists := leaderEntries[objLocal.Index]; exists {
				// Item exists on both sides - send if local is newer
				if objLocal.Updated > objLeader.Updated {
					sendToLeader(clientOpts, fullKey, objLocal, originator)
				}
			} else {
				// Item only exists locally - check if it's new or was deleted on leader
				if leaderActivity == 0 || objLocal.Created > leaderActivity {
					sendToLeader(clientOpts, fullKey, objLocal, originator)
				}
			}
		}

		// Send delete commands for items that exist on leader but not locally
		for _, objLeader := range objsLeader {
			if _, exists := localEntries[objLeader.Index]; !exists {
				fullKey := objLeader.Path
				if isRecentDelete != nil && isRecentDelete(fullKey) {
					continue
				}
				sendDeleteToLeader(clientOpts, fullKey, objLeader.Updated, originator)
			}
		}

		return nil
	}

	obj, err := _key.Database.Get(_key.Path)
	if err != nil {
		// Key doesn't exist locally - check if it exists on leader and delete it
		leaderObj, leaderErr := getEntryFromLeader(clientOpts, _key.Path)
		if leaderErr == nil {
			sendDeleteToLeader(clientOpts, _key.Path, leaderObj.Updated, originator)
		}
		return nil
	}
	sendToLeader(clientOpts, obj.Index, obj, originator)

	return nil
}

func synchronizeItemWithTracking(clientOpts ClientOpts, opts SyncOptions) error {
	update := false
	_key := baseKeyFromPath(opts.Key.Path)

	activityLeader, err := checkLeaderActivity(clientOpts, _key)
	if err != nil {
		return errors.New("failed to check activity for " + _key + " on leader")
	}
	activityLocal, err := checkActivity(opts.Key)
	if err != nil {
		return errors.New("failed to check activity for " + _key + " on local")
	}

	// sync local to leader (includes sending deletes for items deleted locally)
	if activityLocal.LastEntry > activityLeader.LastEntry {
		opts.LastEntry = activityLeader.LastEntry
		if err := syncToLeader(clientOpts, opts); err != nil {
			return err
		}
		update = true
	}

	// sync leader to local
	if activityLocal.LastEntry < activityLeader.LastEntry {
		opts.LastEntry = activityLeader.LastEntry
		if err := syncLocalEntriesWithTracking(clientOpts, opts); err != nil {
			return err
		}
		update = true
	}

	if update {
		return nil
	}

	return errors.New("nothing to synchronize for " + opts.Key.Path)
}

// synchronizeKeysWithTracking performs synchronization with tracking callbacks.
// onDelete/onSet are called for each key deleted/set locally during sync.
// originator is passed to leader so it can skip TriggerNodeSync back to the originating node.
func synchronizeKeysWithTracking(clientOpts ClientOpts, opts SyncOptions, keys []Key) error {
	update := false
	for _, k := range keys {
		keyOpts := opts
		keyOpts.Key = k
		if err := synchronizeItemWithTracking(clientOpts, keyOpts); err == nil {
			update = true
		}
	}
	if update {
		return nil
	}
	return errors.New("nothing to synchronize")
}

// pullTracker tracks keys that are being modified during sync operations.
// Keys are tracked before storage operations and consumed (removed) when checked.
// This ensures each storage event is only skipped once.
type pullTracker struct {
	mu      sync.Mutex
	deleted map[string]bool // keys being deleted during current sync
	set     map[string]bool // keys being set during current sync
}

func newPullTracker() *pullTracker {
	return &pullTracker{
		deleted: make(map[string]bool),
		set:     make(map[string]bool),
	}
}

// trackDelete records a key being deleted during sync
func (p *pullTracker) trackDelete(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deleted[key] = true
}

// trackSet records a key being set during sync
func (p *pullTracker) trackSet(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.set[key] = true
}

// pulledDelete returns true if key was deleted during sync, and consumes the flag
func (p *pullTracker) pulledDelete(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.deleted[key] {
		delete(p.deleted, key)
		return true
	}
	return false
}

// pulledSet returns true if key was set during sync, and consumes the flag
func (p *pullTracker) pulledSet(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.set[key] {
		delete(p.set, key)
		return true
	}
	return false
}

// pendingOp represents a queued per-key operation
type pendingOp struct {
	opType string      // "set" or "del"
	key    string      // full key path
	obj    meta.Object // for set operations
	ts     int64       // timestamp for delete operations
}

// syncer coordinates synchronization operations for a server.
// It ensures only one sync runs at a time and tracks pulled keys.
// Uses a per-key queue to ensure operations aren't lost during contention.
type syncer struct {
	mu       sync.Mutex
	tracker  *pullTracker
	client   *http.Client
	pivot    string
	keys     []Key
	nodeAddr string // This node's address (for originator tracking)
	ssl      bool   // Use HTTPS instead of HTTP
	queueMu  sync.Mutex
	queue    []pendingOp // Per-key operations queued during Pull

	// Version vector cache: tracks last synced pivot VV per key.
	// This is the primary sync indicator, independent of system clocks.
	// VV comparison is used when pivot returns a non-nil VV.
	vvMu         sync.RWMutex
	lastSyncedVV map[string]VersionVector // baseKey -> last known pivot VV after sync

	// Activity cache: tracks last synced pivot activity per key to skip HTTP requests
	// when local activity hasn't changed since last sync.
	// Only used after receiving at least one TriggerNodeSync (connected flag is true).
	// Fallback when pivot doesn't support sequences (backward compatibility).
	activityMu         sync.RWMutex
	lastSyncedActivity map[string]int64 // baseKey -> last known pivot LastEntry after sync
	connected          map[string]bool  // baseKey -> true if we've received TriggerNodeSync for this key
}

func newSyncer(client *http.Client, pivot string, keys []Key, ssl bool) *syncer {
	return &syncer{
		tracker:            newPullTracker(),
		client:             client,
		pivot:              pivot,
		keys:               keys,
		ssl:                ssl,
		queue:              make([]pendingOp, 0),
		lastSyncedVV:       make(map[string]VersionVector),
		lastSyncedActivity: make(map[string]int64),
		connected:          make(map[string]bool),
	}
}

// ClientOpts returns the client options for this syncer.
func (s *syncer) ClientOpts() ClientOpts {
	return ClientOpts{Client: s.client, Leader: s.pivot, SSL: s.ssl}
}

// syncerPool manages multiple syncers, one per unique pivot URL.
// Keys are grouped by their effective ClusterURL and each group gets its own syncer.
type syncerPool struct {
	syncers  map[string]*syncer // pivotURL -> syncer
	keyMap   map[string]string  // key path -> pivotURL (for routing)
	client   *http.Client
	nodeAddr string
	ssl      bool // Use HTTPS instead of HTTP
}

// newSyncerPool creates a syncer pool from keys grouped by their effective ClusterURL.
// configClusterURL is used as fallback for keys without explicit ClusterURL.
// ssl enables HTTPS for URL construction.
func newSyncerPool(client *http.Client, keys []Key, configClusterURL string, ssl bool) *syncerPool {
	pool := &syncerPool{
		syncers: make(map[string]*syncer),
		keyMap:  make(map[string]string),
		client:  client,
		ssl:     ssl,
	}

	// Group keys by effective ClusterURL
	keysByPivot := make(map[string][]Key)
	for _, k := range keys {
		effectiveURL := k.EffectiveClusterURL(configClusterURL)
		if effectiveURL == "" {
			// This key is in pivot mode (Local=true or both empty) - no syncer needed
			continue
		}
		keysByPivot[effectiveURL] = append(keysByPivot[effectiveURL], k)
		pool.keyMap[k.Path] = effectiveURL
	}

	// Create a syncer for each unique pivot URL
	for pivotURL, pivotKeys := range keysByPivot {
		pool.syncers[pivotURL] = newSyncer(client, pivotURL, pivotKeys, ssl)
	}

	return pool
}

// SetNodeAddr sets the node address on all syncers
func (p *syncerPool) SetNodeAddr(addr string) {
	p.nodeAddr = addr
	for _, s := range p.syncers {
		s.SetNodeAddr(addr)
	}
}

// PullAll triggers Pull on all syncers
func (p *syncerPool) PullAll() {
	for _, s := range p.syncers {
		s.Pull()
	}
}

// PullKey triggers PullKey on all syncers for a specific key path (blocking)
func (p *syncerPool) PullKey(keyPath string) {
	for _, s := range p.syncers {
		s.PullKey(keyPath)
	}
}

// SyncAll triggers Sync on all syncers
func (p *syncerPool) SyncAll() error {
	var lastErr error
	for _, s := range p.syncers {
		if err := s.Sync(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// SetNodeAddr sets the node's address (called after server starts)
func (s *syncer) SetNodeAddr(addr string) {
	s.nodeAddr = addr
}

// Pull syncs FROM pivot only (used when pivot notifies node of changes)
// This is called by TriggerNodeSync when pivot has new data.
func (s *syncer) Pull() error {
	// Mark all keys as connected since pivot is sending us notifications
	s.activityMu.Lock()
	for _, k := range s.keys {
		baseKey := baseKeyFromPath(k.Path)
		s.connected[baseKey] = true
		// Invalidate cache since pivot has new data
		delete(s.lastSyncedActivity, baseKey)
	}
	s.activityMu.Unlock()

	s.mu.Lock()
	err := s.pullKeyWithCacheUpdate(s.keys)
	s.mu.Unlock()

	// Process any queued per-key operations
	s.processQueue()

	return err
}

// TryPullKey attempts to sync a specific key FROM pivot, skipping if already in progress.
// Uses activity caching to skip HTTP requests when local data hasn't changed since last sync.
// Caching only activates after receiving at least one TriggerNodeSync for the key.
func (s *syncer) TryPullKey(keyPath string) error {
	// Find the matching key configuration first (before lock)
	var matchingKey *Key
	for i := range s.keys {
		if s.keys[i].Path == keyPath {
			matchingKey = &s.keys[i]
			break
		}
	}
	if matchingKey == nil {
		return nil
	}

	// Check if we can skip based on activity cache (no lock needed for read)
	// Only use cache if we're "connected" (have received TriggerNodeSync for this key)
	baseKey := baseKeyFromPath(keyPath)
	s.activityMu.RLock()
	isConnected := s.connected[baseKey]
	lastSynced, hasCached := s.lastSyncedActivity[baseKey]
	s.activityMu.RUnlock()

	if isConnected && hasCached {
		activityLocal, err := checkActivity(*matchingKey)
		if err == nil && activityLocal.LastEntry == lastSynced {
			// Local activity equals what we last synced, nothing has changed
			// No need to make HTTP request to check pivot activity
			return nil
		}
	}

	if !s.mu.TryLock() {
		return nil
	}

	// Perform sync with cache update
	err := s.pullKeyWithCacheUpdate([]Key{*matchingKey})
	s.mu.Unlock()

	// Process any queued per-key operations
	s.processQueue()

	return err
}

// pullKeyWithCacheUpdate syncs from pivot and updates the VV/activity cache.
// Uses version vector comparison when pivot supports it (VV is non-nil).
// Falls back to timestamp-based comparison for backward compatibility.
// Caller must hold s.mu lock.
func (s *syncer) pullKeyWithCacheUpdate(keys []Key) error {
	update := false
	for _, _key := range keys {
		baseKey := baseKeyFromPath(_key.Path)
		activityPivot, err := checkPivotActivity(s.ClientOpts(), baseKey)
		if err != nil {
			continue
		}

		// Determine if sync is needed using version vector or timestamp
		needSync := false
		s.vvMu.RLock()
		localVV, hasLocalVV := s.lastSyncedVV[baseKey]
		s.vvMu.RUnlock()

		if len(activityPivot.VV) > 0 && hasLocalVV {
			// Version vector comparison (preferred, clock-independent)
			// Compare pivot's VV with our last synced VV
			switch localVV.Compare(activityPivot.VV) {
			case VVLess:
				// Local is behind pivot - need to sync
				needSync = true
			case VVConcurrent:
				// Conflict detected - log and sync (last-sync-wins)
				logConflict(baseKey, localVV, activityPivot.VV, "last-sync-wins: accepting pivot data")
				needSync = true
			}
			// VVEqual or VVGreater means no sync needed
		} else {
			// Fallback: timestamp-based comparison
			// Used for backward compatibility and first sync (before we have local VV)
			activityLocal, err := checkActivity(_key)
			if err != nil {
				continue
			}
			needSync = activityPivot.LastEntry > activityLocal.LastEntry
		}

		// Update caches with pivot's current values
		if len(activityPivot.VV) > 0 {
			s.vvMu.Lock()
			s.lastSyncedVV[baseKey] = activityPivot.VV.Clone()
			s.vvMu.Unlock()
		}
		s.activityMu.Lock()
		s.lastSyncedActivity[baseKey] = activityPivot.LastEntry
		s.activityMu.Unlock()

		// Sync if leader has newer data
		if needSync {
			opts := SyncOptions{
				Key:       _key,
				LastEntry: activityPivot.LastEntry,
				OnDelete:  s.tracker.trackDelete,
				OnSet:     s.tracker.trackSet,
			}
			if err := syncLocalEntriesWithTracking(s.ClientOpts(), opts); err == nil {
				update = true
			}
		}
	}
	if update {
		return nil
	}
	return errors.New("nothing to synchronize")
}

// PullKey syncs a specific key FROM pivot (blocking - waits for lock)
// This is called by TriggerNodeSync when pivot has new data.
func (s *syncer) PullKey(keyPath string) error {
	// Mark key as connected and invalidate caches since pivot is telling us it has new data
	baseKey := baseKeyFromPath(keyPath)
	s.vvMu.Lock()
	delete(s.lastSyncedVV, baseKey)
	s.vvMu.Unlock()
	s.activityMu.Lock()
	s.connected[baseKey] = true
	delete(s.lastSyncedActivity, baseKey)
	s.activityMu.Unlock()

	s.mu.Lock()
	// Find the matching key configuration
	var matchingKeys []Key
	for _, k := range s.keys {
		if k.Path == keyPath {
			matchingKeys = append(matchingKeys, k)
			break
		}
	}
	if len(matchingKeys) == 0 {
		s.mu.Unlock()
		return nil
	}
	err := s.pullKeyWithCacheUpdate(matchingKeys)
	s.mu.Unlock()

	// Process any queued per-key operations
	s.processQueue()

	return err
}

// processQueue sends all queued per-key operations to leader
func (s *syncer) processQueue() {
	s.queueMu.Lock()
	if len(s.queue) == 0 {
		s.queueMu.Unlock()
		return
	}
	// Take ownership of the queue
	pending := s.queue
	s.queue = make([]pendingOp, 0)
	s.queueMu.Unlock()

	// Process each operation
	for _, op := range pending {
		switch op.opType {
		case "set":
			sendToLeader(s.ClientOpts(), op.key, op.obj, s.nodeAddr)
		case "del":
			sendDeleteToLeader(s.ClientOpts(), op.key, op.ts, s.nodeAddr)
		}
	}
}

// processQueueLocked sends all queued per-key operations to leader (caller must hold s.mu)
func (s *syncer) processQueueLocked() {
	s.queueMu.Lock()
	if len(s.queue) == 0 {
		s.queueMu.Unlock()
		return
	}
	pending := s.queue
	s.queue = make([]pendingOp, 0)
	s.queueMu.Unlock()

	for _, op := range pending {
		switch op.opType {
		case "set":
			sendToLeader(s.ClientOpts(), op.key, op.obj, s.nodeAddr)
		case "del":
			sendDeleteToLeader(s.ClientOpts(), op.key, op.ts, s.nodeAddr)
		}
	}
}

// QueueOrSendSet sends a set operation to leader, or queues it if Pull is in progress
func (s *syncer) QueueOrSendSet(key string, obj meta.Object) {
	if s.mu.TryLock() {
		sendToLeader(s.ClientOpts(), key, obj, s.nodeAddr)
		s.mu.Unlock()
	} else {
		// Pull in progress - queue for later and wait for Pull to complete then process
		s.queueMu.Lock()
		s.queue = append(s.queue, pendingOp{opType: "set", key: key, obj: obj})
		s.queueMu.Unlock()
		// Wait for Pull to release the lock, then process queue under lock
		s.mu.Lock()
		s.processQueueLocked()
		s.mu.Unlock()
	}
}

// QueueOrSendDelete sends a delete operation to leader, or queues it if Pull is in progress
func (s *syncer) QueueOrSendDelete(key string, ts int64) {
	if s.mu.TryLock() {
		sendDeleteToLeader(s.ClientOpts(), key, ts, s.nodeAddr)
		s.mu.Unlock()
	} else {
		// Pull in progress - queue for later and wait for Pull to complete then process
		s.queueMu.Lock()
		s.queue = append(s.queue, pendingOp{opType: "del", key: key, ts: ts})
		s.queueMu.Unlock()
		// Wait for Pull to release the lock, then process queue under lock
		s.mu.Lock()
		s.processQueueLocked()
		s.mu.Unlock()
	}
}

// Sync performs bidirectional synchronization with tracking
func (s *syncer) Sync() error {
	s.mu.Lock()
	opts := SyncOptions{
		Originator:     s.nodeAddr,
		IsRecentDelete: s.tracker.pulledDelete,
		OnDelete:       s.tracker.trackDelete,
		OnSet:          s.tracker.trackSet,
	}
	err := synchronizeKeysWithTracking(s.ClientOpts(), opts, s.keys)
	s.mu.Unlock()
	return err
}

// PulledDelete returns true if key was deleted in the last pull
func (s *syncer) PulledDelete(key string) bool {
	return s.tracker.pulledDelete(key)
}

// PulledSet returns true if key was set in the last pull
func (s *syncer) PulledSet(key string) bool {
	return s.tracker.pulledSet(key)
}

// StorageSyncCallback is the callback type for storage sync events
type StorageSyncCallback func(event storage.Event)

// OriginatorTracker tracks which node originated a storage change.
// This allows pivot to skip TriggerNodeSync back to the originating node.
type OriginatorTracker struct {
	mu          sync.Mutex
	originators map[string]string // key -> originator address
}

// NewOriginatorTracker creates a new originator tracker
func NewOriginatorTracker() *OriginatorTracker {
	return &OriginatorTracker{
		originators: make(map[string]string),
	}
}

// Set records the originator for a key (call before storage write)
func (t *OriginatorTracker) Set(key, originator string) {
	if originator == "" {
		return
	}
	t.mu.Lock()
	t.originators[key] = originator
	t.mu.Unlock()
}

// Get returns and clears the originator for a key (call in storage callback)
func (t *OriginatorTracker) Get(key string) string {
	t.mu.Lock()
	originator := t.originators[key]
	delete(t.originators, key)
	t.mu.Unlock()
	return originator
}

// StorageSyncConfig holds configuration for storage sync callback creation.
type StorageSyncConfig struct {
	Client            *http.Client
	ConfigClusterURL  string
	Keys              []Key
	NodesKey          string
	GetNodes          getNodes
	Pool              *syncerPool
	NodeHealth        *NodeHealth
	OriginatorTracker *OriginatorTracker
	Instance          *Instance
}

// makeStorageSync creates a callback that triggers synchronization on storage events.
// For keys where server IS pivot - broadcasts to nodes.
// For keys where server IS node - syncs to the appropriate pivot via syncerPool.
func makeStorageSync(cfg StorageSyncConfig) StorageSyncCallback {
	return func(event storage.Event) {
		// Find matching key and its database for this event
		var matchedKeyConfig Key
		var matchedKey string
		var matchedDB storage.Database
		var found bool
		for _, k := range cfg.Keys {
			if key.Match(k.Path, event.Key) {
				matchedKeyConfig = k
				matchedKey = baseKeyFromPath(k.Path)
				matchedDB = k.Database
				found = true
				break
			}
		}

		if !found || matchedDB == nil {
			return
		}

		// Keep the nodes-address cache in sync with NodesKey writes. Settings-only
		// changes (ip/port unchanged) are a no-op inside update().
		if cfg.NodesKey != "" && matchedKeyConfig.Path == cfg.NodesKey && cfg.Instance != nil {
			cfg.Instance.nodesCache.update(event)
		}

		// Determine if this server is pivot or node for this specific key
		effectiveClusterURL := matchedKeyConfig.EffectiveClusterURL(cfg.ConfigClusterURL)
		isPivotForKey := effectiveClusterURL == ""

		// For node keys, skip events caused by a pull operation
		if !isPivotForKey && cfg.Pool != nil {
			s := cfg.Pool.syncers[effectiveClusterURL]
			if s != nil {
				if event.Operation == "set" && s.PulledSet(event.Key) {
					return
				}
				if event.Operation == "del" && s.PulledDelete(event.Key) {
					return
				}
			}
		}

		// Track delete timestamps for proper sync
		switch event.Operation {
		case "del":
			matchedDB.Set(StoragePrefix+matchedKey, json.RawMessage(ooo.Time()))
		case "set":
			matchedDB.Del(StoragePrefix + matchedKey)
		}

		if isPivotForKey {
			// This server IS pivot for this key - increment VV and notify all nodes asynchronously
			if cfg.Instance != nil && cfg.Instance.VVManager != nil {
				cfg.Instance.VVManager.increment(event.Key)
			}
			// Get and clear the originator for this key (set by handler before storage write)
			var originator string
			if cfg.OriginatorTracker != nil {
				originator = cfg.OriginatorTracker.Get(event.Key)
			}
			nodes := cfg.GetNodes()
			for _, node := range nodes {
				// Skip the originating node to prevent echo-back race condition
				if node == originator {
					continue
				}
				// Skip incompatible nodes - don't sync to nodes with different protocol version
				if cfg.NodeHealth != nil && !cfg.NodeHealth.IsCompatible(node) {
					continue
				}
				go func(n string, keyPath string) {
					ssl := false
					if cfg.Pool != nil {
						ssl = cfg.Pool.ssl
					}
					ok := TriggerNodeSyncWithHealth(ClientOpts{Client: cfg.Client, SSL: ssl}, n, keyPath)
					if cfg.NodeHealth != nil {
						if ok {
							cfg.NodeHealth.MarkHealthy(n)
						} else {
							cfg.NodeHealth.MarkUnhealthy(n)
						}
					}
				}(node, matchedKeyConfig.Path)
			}
		} else {
			// This server is node for this key - increment local VV and sync to pivot
			if cfg.Instance != nil && cfg.Instance.VVManager != nil {
				cfg.Instance.VVManager.increment(event.Key)
			}
			if cfg.Pool != nil {
				s := cfg.Pool.syncers[effectiveClusterURL]
				if s != nil {
					if event.Operation == "del" {
						s.QueueOrSendDelete(event.Key, time.Now().UnixNano())
					} else {
						obj, err := matchedDB.Get(event.Key)
						if err == nil {
							s.QueueOrSendSet(event.Key, obj)
						}
					}
				}
			}
		}
	}
}
