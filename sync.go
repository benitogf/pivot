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

// syncLocalEntriesWithTracking syncs from pivot and tracks synced keys via callbacks.
// onDelete is called for each key deleted locally (item exists locally but not on pivot)
// onSet is called for each key set locally (item exists on pivot but not locally, or pivot is newer)
// skipSet is called before setting - if it returns true, the set is skipped (for locally deleted keys)
func syncLocalEntriesWithTracking(client *http.Client, pivot string, _key Key, lastEntry int64, onDelete func(key string), onSet func(key string), skipSet func(key string) bool) error {
	if key.LastIndex(_key.Path) == "*" {
		baseKey := strings.Replace(_key.Path, "/*", "", 1)
		objsPivot, err := getEntriesFromPivot(client, pivot, _key.Path)
		if err != nil {
			return err
		}

		objsLocal, err := _key.Database.GetList(_key.Path)
		if err != nil {
			objsLocal = []meta.Object{}
		}

		objsToDelete := GetEntriesNegativeDiff(objsLocal, objsPivot)
		for _, index := range objsToDelete {
			fullKey := baseKey + "/" + index
			// Track BEFORE delete so storage callback can skip this event
			if onDelete != nil {
				onDelete(fullKey)
			}
			_key.Database.Del(fullKey)
		}

		objsToSend := GetEntriesPositiveDiff(objsLocal, objsPivot)
		for _, obj := range objsToSend {
			fullKey := baseKey + "/" + obj.Index
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

	obj, err := getEntryFromPivot(client, pivot, _key.Path)
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
	// Track BEFORE set so storage callback can skip this event
	if onSet != nil {
		onSet(_key.Path)
	}
	_key.Database.SetWithMeta(_key.Path, obj.Data, obj.Created, obj.Updated)
	// Clear delete timestamp so activity is based on object timestamp
	_key.Database.Del(StoragePrefix + _key.Path)

	return nil
}

// syncPivotEntriesWithDeleteCheck syncs local entries to pivot with an optional delete check.
// If isRecentDelete is provided, items that were recently deleted by a pull-only sync
// will not be re-added to pivot.
// This function also sends delete commands to pivot for items that were deleted locally.
// originator is passed to pivot so it can skip TriggerNodeSync back to the originating node.
func syncPivotEntriesWithDeleteCheck(client *http.Client, pivot string, _key Key, pivotActivity int64, isRecentDelete func(key string) bool, originator string) error {
	if key.LastIndex(_key.Path) == "*" {
		baseKey := strings.Replace(_key.Path, "/*", "", 1)
		objsLocal, err := _key.Database.GetList(_key.Path)
		if err != nil {
			objsLocal = []meta.Object{}
		}

		objsPivot, err := getEntriesFromPivot(client, pivot, _key.Path)
		if err != nil {
			return err
		}

		// Re-fetch pivot activity to get the latest value (including any recent deletes)
		// This is important because the activity might have changed since synchronizeItem was called
		latestActivity, err := checkPivotActivity(client, pivot, baseKey)
		if err == nil && latestActivity.LastEntry > pivotActivity {
			pivotActivity = latestActivity.LastEntry
		}

		// Build map of local entries for O(1) lookup
		localEntries := make(map[string]meta.Object, len(objsLocal))
		for _, obj := range objsLocal {
			localEntries[obj.Index] = obj
		}

		// Build map of pivot entries for O(1) lookup
		pivotEntries := make(map[string]meta.Object, len(objsPivot))
		for _, obj := range objsPivot {
			pivotEntries[obj.Index] = obj
		}

		// Send local items to pivot (new or updated)
		for _, objLocal := range objsLocal {
			fullKey := baseKey + "/" + objLocal.Index

			// Skip items that were recently synced by a pull-only sync
			// These items came from pivot, so we don't need to send them back
			if isRecentDelete != nil && isRecentDelete(fullKey) {
				continue
			}

			if objPivot, exists := pivotEntries[objLocal.Index]; exists {
				// Item exists on both sides - send if local is newer
				if objLocal.Updated > objPivot.Updated {
					sendToPivot(client, fullKey, pivot, objLocal, originator)
				}
			} else {
				// Item only exists locally - check if it's new or was deleted on pivot
				// An item is considered "new" if:
				// 1. Pivot has no activity (pivotActivity == 0), OR
				// 2. The item was created AFTER pivot's last activity
				// This ensures we don't re-add items that were synced to pivot and then deleted
				if pivotActivity == 0 || objLocal.Created > pivotActivity {
					sendToPivot(client, fullKey, pivot, objLocal, originator)
				}
			}
		}

		// Send delete commands to pivot for items that exist on pivot but not locally
		// This handles the case where an item was deleted on the node
		// Note: This is a fallback mechanism. The primary delete sync happens in StorageSync
		// which sends delete commands directly when a delete event occurs.
		for _, objPivot := range objsPivot {
			if _, exists := localEntries[objPivot.Index]; !exists {
				fullKey := baseKey + "/" + objPivot.Index
				// Skip items that were recently deleted by a pull-only sync
				// These items were deleted from pivot, so we don't need to send delete again
				if isRecentDelete != nil && isRecentDelete(fullKey) {
					continue
				}
				// Item exists on pivot but not locally - it was deleted locally
				// Send delete command to pivot
				sendDelete(client, fullKey, pivot, objPivot.Updated, originator)
			}
		}

		return nil
	}

	obj, err := _key.Database.Get(_key.Path)
	if err != nil {
		// Key doesn't exist locally - check if it exists on pivot and delete it
		pivotObj, pivotErr := getEntryFromPivot(client, pivot, _key.Path)
		if pivotErr == nil {
			// Key exists on pivot but not locally - send delete
			sendDelete(client, _key.Path, pivot, pivotObj.Updated, originator)
		}
		return nil
	}
	sendToPivot(client, obj.Index, pivot, obj, originator)

	return nil
}

func synchronizeItemWithTracking(client *http.Client, pivot string, key Key, isRecentDelete func(key string) bool, onDelete func(key string), onSet func(key string), originator string) error {
	update := false
	_key := strings.Replace(key.Path, "/*", "", 1)
	//check
	activityPivot, err := checkPivotActivity(client, pivot, _key)
	if err != nil {
		return errors.New("failed to check activity for " + _key + " on pivot")
	}
	activityLocal, err := checkActivity(key)
	if err != nil {
		return errors.New("failed to check activity for " + _key + " on local")
	}

	// sync local to pivot (includes sending deletes for items deleted locally)
	if activityLocal.LastEntry > activityPivot.LastEntry {
		// Pass pivot's activity and delete check so syncPivotEntries can determine if items are new or deleted
		err := syncPivotEntriesWithDeleteCheck(client, pivot, key, activityPivot.LastEntry, isRecentDelete, originator)
		if err != nil {
			return err
		}
		update = true
	}

	// sync pivot to local
	if activityLocal.LastEntry < activityPivot.LastEntry {
		err := syncLocalEntriesWithTracking(client, pivot, key, activityPivot.LastEntry, onDelete, onSet, nil)
		if err != nil {
			return err
		}
		update = true
	}

	if update {
		return nil
	}

	return errors.New("nothing to synchronize for " + key.Path)
}

// synchronizeKeys performs the actual synchronization without mutex handling
// synchronizeKeysWithTracking performs synchronization with tracking callbacks.
// onDelete/onSet are called for each key deleted/set locally during sync.
// originator is passed to pivot so it can skip TriggerNodeSync back to the originating node.
func synchronizeKeysWithTracking(client *http.Client, pivot string, keys []Key, isRecentDelete func(key string) bool, onDelete func(key string), onSet func(key string), originator string) error {
	update := false
	for _, key := range keys {
		errItem := synchronizeItemWithTracking(client, pivot, key, isRecentDelete, onDelete, onSet, originator)
		if errItem == nil {
			update = true
		}
	}
	if update {
		return nil
	}
	return errors.New("nothing to synchronize")
}

// pullFromPivotWithTracking syncs FROM pivot and tracks synced keys.
// onDelete is called for each key deleted locally, onSet is called for each key set locally.
// skipSet is called before setting a key - if it returns true, the set is skipped (for locally deleted keys).
func pullFromPivotWithTracking(client *http.Client, pivot string, keys []Key, onDelete func(key string), onSet func(key string), skipSet func(key string) bool) error {
	update := false
	for _, _key := range keys {
		baseKey := strings.Replace(_key.Path, "/*", "", 1)
		activityPivot, err := checkPivotActivity(client, pivot, baseKey)
		if err != nil {
			continue
		}
		activityLocal, err := checkActivity(_key)
		if err != nil {
			continue
		}
		// Only sync if pivot has newer data
		if activityPivot.LastEntry > activityLocal.LastEntry {
			if err := syncLocalEntriesWithTracking(client, pivot, _key, activityPivot.LastEntry, onDelete, onSet, skipSet); err == nil {
				update = true
			}
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
	mu        sync.Mutex
	tracker   *pullTracker
	client    *http.Client
	pivot     string
	keys      []Key
	nodeAddr  string // This node's address (for originator tracking)
	queueMu   sync.Mutex
	queue     []pendingOp // Per-key operations queued during Pull
	AfterPull func()      // Optional callback after Pull completes (for testing)
}

func newSyncer(client *http.Client, pivot string, keys []Key) *syncer {
	return &syncer{
		tracker: newPullTracker(),
		client:  client,
		pivot:   pivot,
		keys:    keys,
		queue:   make([]pendingOp, 0),
	}
}

// syncerPool manages multiple syncers, one per unique pivot URL.
// Keys are grouped by their effective ClusterURL and each group gets its own syncer.
type syncerPool struct {
	syncers  map[string]*syncer // pivotURL -> syncer
	keyMap   map[string]string  // key path -> pivotURL (for routing)
	client   *http.Client
	nodeAddr string
}

// newSyncerPool creates a syncer pool from keys grouped by their effective ClusterURL.
// configClusterURL is used as fallback for keys without explicit ClusterURL.
func newSyncerPool(client *http.Client, keys []Key, configClusterURL string) *syncerPool {
	pool := &syncerPool{
		syncers: make(map[string]*syncer),
		keyMap:  make(map[string]string),
		client:  client,
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
		pool.syncers[pivotURL] = newSyncer(client, pivotURL, pivotKeys)
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

// GetSyncer returns the syncer for a given key path, or nil if key is in pivot mode
func (p *syncerPool) GetSyncer(keyPath string) *syncer {
	pivotURL, ok := p.keyMap[keyPath]
	if !ok {
		return nil
	}
	return p.syncers[pivotURL]
}

// GetSyncerForKey returns the syncer for a key that matches the given index
func (p *syncerPool) GetSyncerForKey(index string, keys []Key, configClusterURL string) *syncer {
	for _, k := range keys {
		if key.Match(k.Path, index) {
			effectiveURL := k.EffectiveClusterURL(configClusterURL)
			if effectiveURL == "" {
				return nil // pivot mode
			}
			return p.syncers[effectiveURL]
		}
	}
	return nil
}

// AllSyncers returns all syncers in the pool
func (p *syncerPool) AllSyncers() []*syncer {
	result := make([]*syncer, 0, len(p.syncers))
	for _, s := range p.syncers {
		result = append(result, s)
	}
	return result
}

// ClusterURLs returns all unique pivot URLs in the pool
func (p *syncerPool) ClusterURLs() []string {
	result := make([]string, 0, len(p.syncers))
	for url := range p.syncers {
		result = append(result, url)
	}
	return result
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
func (s *syncer) Pull() error {
	s.mu.Lock()
	err := pullFromPivotWithTracking(s.client, s.pivot, s.keys,
		s.tracker.trackDelete,
		s.tracker.trackSet,
		nil)
	s.mu.Unlock()

	// Process any queued per-key operations
	s.processQueue()

	// Call AfterPull callback if set (for test synchronization)
	if s.AfterPull != nil {
		s.AfterPull()
	}
	return err
}

// TryPull attempts to sync FROM pivot, skipping if already in progress
func (s *syncer) TryPull() error {
	if !s.mu.TryLock() {
		return nil
	}
	err := pullFromPivotWithTracking(s.client, s.pivot, s.keys,
		s.tracker.trackDelete,
		s.tracker.trackSet,
		nil)
	s.mu.Unlock()

	// Process any queued per-key operations
	s.processQueue()

	// Call AfterPull callback if set (for test synchronization)
	if s.AfterPull != nil {
		s.AfterPull()
	}
	return err
}

// TryPullKey attempts to sync a specific key FROM pivot, skipping if already in progress
func (s *syncer) TryPullKey(keyPath string) error {
	if !s.mu.TryLock() {
		return nil
	}
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
	err := pullFromPivotWithTracking(s.client, s.pivot, matchingKeys,
		s.tracker.trackDelete,
		s.tracker.trackSet,
		nil)
	s.mu.Unlock()

	// Process any queued per-key operations
	s.processQueue()

	// Call AfterPull callback if set (for test synchronization)
	if s.AfterPull != nil {
		s.AfterPull()
	}
	return err
}

// PullKey syncs a specific key FROM pivot (blocking - waits for lock)
func (s *syncer) PullKey(keyPath string) error {
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
	err := pullFromPivotWithTracking(s.client, s.pivot, matchingKeys,
		s.tracker.trackDelete,
		s.tracker.trackSet,
		nil)
	s.mu.Unlock()

	// Process any queued per-key operations
	s.processQueue()

	// Call AfterPull callback if set (for test synchronization)
	if s.AfterPull != nil {
		s.AfterPull()
	}
	return err
}

// processQueue sends all queued per-key operations to pivot
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
			sendToPivot(s.client, op.key, s.pivot, op.obj, s.nodeAddr)
		case "del":
			sendDelete(s.client, op.key, s.pivot, op.ts, s.nodeAddr)
		}
	}
}

// processQueueLocked sends all queued per-key operations to pivot (caller must hold s.mu)
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
			sendToPivot(s.client, op.key, s.pivot, op.obj, s.nodeAddr)
		case "del":
			sendDelete(s.client, op.key, s.pivot, op.ts, s.nodeAddr)
		}
	}
}

// QueueOrSendSet sends a set operation to pivot, or queues it if Pull is in progress
func (s *syncer) QueueOrSendSet(key string, obj meta.Object) {
	if s.mu.TryLock() {
		sendToPivot(s.client, key, s.pivot, obj, s.nodeAddr)
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

// QueueOrSendDelete sends a delete operation to pivot, or queues it if Pull is in progress
func (s *syncer) QueueOrSendDelete(key string, ts int64) {
	if s.mu.TryLock() {
		sendDelete(s.client, key, s.pivot, ts, s.nodeAddr)
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
	err := synchronizeKeysWithTracking(s.client, s.pivot, s.keys, s.tracker.pulledDelete,
		s.tracker.trackDelete,
		s.tracker.trackSet,
		s.nodeAddr)
	s.mu.Unlock()
	return err
}

// TrySync attempts sync but skips if already in progress
// Uses tracking to prevent storage events from triggering redundant syncs
func (s *syncer) TrySync() error {
	if !s.mu.TryLock() {
		return nil
	}
	err := synchronizeKeysWithTracking(s.client, s.pivot, s.keys, s.tracker.pulledDelete,
		s.tracker.trackDelete,
		s.tracker.trackSet,
		s.nodeAddr)
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

// makeStorageSync creates a callback that triggers synchronization on storage events.
// For keys where server IS pivot - broadcasts to nodes.
// For keys where server IS node - syncs to the appropriate pivot via syncerPool.
// originatorTracker is used by pivot to skip TriggerNodeSync back to the originating node.
// instance is used to access pivot Instance for sync tracking (can be nil).
func makeStorageSync(client *http.Client, configClusterURL string, keys []Key, _getNodes getNodes, pool *syncerPool, nodeHealth *NodeHealth, originatorTracker *OriginatorTracker, instance *Instance) StorageSyncCallback {
	return func(event storage.Event) {
		// Find matching key and its database for this event
		var matchedKeyConfig Key
		var matchedKey string
		var matchedDB storage.Database
		var found bool
		for _, k := range keys {
			if key.Match(k.Path, event.Key) {
				matchedKeyConfig = k
				matchedKey = strings.Replace(k.Path, "/*", "", 1)
				matchedDB = k.Database
				found = true
				break
			}
		}

		if !found || matchedDB == nil {
			return
		}

		// Determine if this server is pivot or node for this specific key
		effectiveClusterURL := matchedKeyConfig.EffectiveClusterURL(configClusterURL)
		isPivotForKey := effectiveClusterURL == ""

		// For node keys, skip events caused by a pull operation
		if !isPivotForKey && pool != nil {
			s := pool.syncers[effectiveClusterURL]
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
			// This server IS pivot for this key - notify all nodes asynchronously
			// Get and clear the originator for this key (set by handler before storage write)
			var originator string
			if originatorTracker != nil {
				originator = originatorTracker.Get(event.Key)
			}
			nodes := _getNodes()
			for _, node := range nodes {
				// Skip the originating node to prevent echo-back race condition
				if node == originator {
					continue
				}
				// Skip incompatible nodes - don't sync to nodes with different protocol version
				if nodeHealth != nil && !nodeHealth.IsCompatible(node) {
					continue
				}
				go func(n string, keyPath string) {
					ok := TriggerNodeSyncWithHealth(client, n, keyPath)
					if nodeHealth != nil {
						if ok {
							nodeHealth.MarkHealthy(n)
						} else {
							nodeHealth.MarkUnhealthy(n)
						}
					}
				}(node, matchedKeyConfig.Path)
			}
		} else {
			// This server is node for this key - sync to the appropriate pivot
			if pool != nil {
				s := pool.syncers[effectiveClusterURL]
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
