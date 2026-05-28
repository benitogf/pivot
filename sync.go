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
	// VVManager exposes the local version vector to the bidirectional
	// reconciler. When non-nil and both sides report a VV, direction is
	// chosen by VV.Compare instead of LastEntry — the latter silently
	// reports "nothing to synchronize" on timestamp collisions even when
	// content actually diverges. Optional for backward compatibility:
	// nil falls back to LastEntry-only logic.
	VVManager *VVManager
	// LeaderVV is the leader's VersionVector for opts.Key as reported by
	// the leader's /activity endpoint, propagated from the caller so the
	// inner per-item dedup in syncLocalEntriesWithTracking can ask
	// "does my local VV already cover this base key?" instead of falling
	// back to the wall-clock timestamp gate. Empty when the leader does
	// not expose a VV (older peer); the inner gate then uses the
	// timestamp fallback as the legacy code did.
	LeaderVV VersionVector
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
	// Dedup gate: skip the write if local already covers this revision.
	//
	// When both sides expose a VersionVector, compare on causal order:
	// VVGreater or VVEqual means local already has at least everything
	// the leader served, so the write would be a no-op or a regression.
	// VVLess and VVConcurrent fall through to the write — VVLess because
	// the leader genuinely has new state; VVConcurrent because the
	// outer orchestrator already chose "pull leader" under the last-
	// sync-wins policy and the inner gate must not silently override
	// that.
	//
	// Without a VV (older peer, or pre-VV-bump startup window), fall
	// back to the legacy timestamp comparison. Note: this fallback is
	// unsafe under clock skew — a leader-served future-Updated record
	// will beat any honest present-time local write. The caller
	// (pullKeyWithCacheUpdate, synchronizeItemWithTracking) is
	// responsible for not invoking this path when timestamps cannot be
	// trusted; both now derive direction from VV exclusively when both
	// sides have one.
	localObj, localErr := _key.Database.Get(_key.Path)
	if localErr == nil {
		if opts.VVManager != nil && len(opts.LeaderVV) > 0 {
			localVV := opts.VVManager.Get(baseKeyFromPath(_key.Path))
			if len(localVV) > 0 {
				switch localVV.Compare(opts.LeaderVV) {
				case VVGreater, VVEqual:
					return nil
				}
			} else if localObj.Updated >= obj.Updated {
				// Local has no VV yet (cold start); use the
				// timestamp fallback for this single decision.
				return nil
			}
		} else if localObj.Updated >= obj.Updated {
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
	// localVV is the originator's per-key VV at send time; the leader
	// merges it into its own VV (merge-on-receive). Nil when the syncer
	// has no VVManager — the leader treats that as the legacy behavior
	// and skips the merge.
	var localVV VersionVector
	if opts.VVManager != nil {
		localVV = opts.VVManager.Get(_key.Path)
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
					if rVV, err := sendToLeader(clientOpts, fullKey, objLocal, originator, localVV); err == nil {
						mergeLeaderVV(opts.VVManager, baseKey, rVV)
					}
				}
			} else {
				// Item only exists locally - check if it's new or was deleted on leader
				if leaderActivity == 0 || objLocal.Created > leaderActivity {
					if rVV, err := sendToLeader(clientOpts, fullKey, objLocal, originator, localVV); err == nil {
						mergeLeaderVV(opts.VVManager, baseKey, rVV)
					}
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
				if rVV, err := sendDeleteToLeader(clientOpts, fullKey, objLeader.Updated, originator, localVV); err == nil {
					mergeLeaderVV(opts.VVManager, baseKey, rVV)
				}
			}
		}

		return nil
	}

	obj, err := _key.Database.Get(_key.Path)
	if err != nil {
		// Key doesn't exist locally - check if it exists on leader and delete it
		leaderObj, leaderErr := getEntryFromLeader(clientOpts, _key.Path)
		if leaderErr == nil {
			if rVV, err := sendDeleteToLeader(clientOpts, _key.Path, leaderObj.Updated, originator, localVV); err == nil {
				mergeLeaderVV(opts.VVManager, baseKeyFromPath(_key.Path), rVV)
			}
		}
		return nil
	}
	if rVV, err := sendToLeader(clientOpts, obj.Index, obj, originator, localVV); err == nil {
		mergeLeaderVV(opts.VVManager, baseKeyFromPath(_key.Path), rVV)
	}

	return nil
}

// mergeLeaderVV is the symmetric merge for the push path: pivot
// echoes its post-write VV back via the VVHeader on each push
// response; the sender merges it so its VVManager reflects pivot's
// frontier. Without this the sender's VV stays narrow ({self:N}) and
// the next pull tick compares against pivot's wider VV
// ({leader:M, self:N-1, ...}), seeing VVConcurrent on every push and
// triggering reconciliation pulls that delete the just-pushed items
// when pivot hasn't received them yet.
//
// No-op for older peers that don't echo VV (leaderVV is empty) and
// for senders without a VVManager configured.
func mergeLeaderVV(vvManager *VVManager, baseKey string, leaderVV VersionVector) {
	if vvManager == nil || len(leaderVV) == 0 {
		return
	}
	vvManager.set(baseKey, leaderVV)
}

func synchronizeItemWithTracking(clientOpts ClientOpts, opts SyncOptions) error {
	_key := baseKeyFromPath(opts.Key.Path)

	activityLeader, err := checkLeaderActivity(clientOpts, _key)
	if err != nil {
		return errors.New("failed to check activity for " + _key + " on leader")
	}
	activityLocal, err := checkActivity(opts.Key)
	if err != nil {
		return errors.New("failed to check activity for " + _key + " on local")
	}

	// checkActivity is a local read that doesn't carry the VV. Pull it
	// from the manager directly so the comparison below has both sides.
	if opts.VVManager != nil {
		activityLocal.VV = opts.VVManager.Get(_key)
	}

	// Direction logic. When BOTH sides report a non-empty VV, use VV
	// comparison — that's clock-independent and detects content
	// divergence even when timestamps collide. Concurrent writes
	// producing equal LastEntry but disjoint VV updates used to fall
	// through the timestamp comparison and silently report "nothing to
	// synchronize", leaving the cluster diverged until the next write
	// bumped the timestamp past the collision.
	//
	// Fall back to LastEntry comparison when either side has no VV
	// (older peers, or the very first sync before any VV bump).
	//
	// Always propagate the leader's VV (when present) so the inner
	// per-item gate in syncLocalEntriesWithTracking can do VV-aware
	// dedup independently of whether the outer direction was chosen
	// via VV or via LastEntry. Without this a useVV=false outer
	// decision would leave the inner gate falling back to the
	// wall-clock timestamp comparison, which is unsafe under clock
	// skew (a peer's future-Updated record would beat any honest
	// present-time local write).
	opts.LeaderVV = activityLeader.VV

	useVV := len(activityLeader.VV) > 0 && len(activityLocal.VV) > 0
	if useVV {
		switch activityLocal.VV.Compare(activityLeader.VV) {
		case VVEqual:
			return errors.New("nothing to synchronize for " + opts.Key.Path)
		case VVGreater:
			opts.LastEntry = activityLeader.LastEntry
			return syncToLeader(clientOpts, opts)
		case VVLess:
			opts.LastEntry = activityLeader.LastEntry
			return syncLocalEntriesWithTracking(clientOpts, opts)
		case VVConcurrent:
			// Both sides have writes the other hasn't seen. Match the
			// last-sync-wins convention from pullKeyWithCacheUpdate so
			// the cluster converges on a single canonical state — local-
			// only items are dropped this round but get re-pushed on the
			// next sync after a write bumps local's VV further.
			logConflict(_key, activityLocal.VV, activityLeader.VV, "last-sync-wins: pulling leader")
			opts.LastEntry = activityLeader.LastEntry
			return syncLocalEntriesWithTracking(clientOpts, opts)
		}
	}

	// LastEntry fallback (backward compatibility with peers that don't expose VV).
	update := false
	if activityLocal.LastEntry > activityLeader.LastEntry {
		opts.LastEntry = activityLeader.LastEntry
		if err := syncToLeader(clientOpts, opts); err != nil {
			return err
		}
		update = true
	}

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

// hasPulledSet is a non-consuming variant of pulledSet, used by the
// synchronous VV-bump path in AfterWrite. The watch goroutine still
// consumes the flag via pulledSet to gate fanout; AfterWrite only needs
// to know "is this storage write coming from a pull operation?" so it
// can skip our own counter bump (the pulled record carries the leader's
// VV, not ours).
func (p *pullTracker) hasPulledSet(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.set[key]
}

// hasPulledDelete mirrors hasPulledSet for delete operations.
func (p *pullTracker) hasPulledDelete(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.deleted[key]
}

// pendingOp represents a queued per-key operation
type pendingOp struct {
	opType string        // "set" or "del"
	key    string        // full key path
	obj    meta.Object   // for set operations
	ts     int64         // timestamp for delete operations
	vv     VersionVector // originator's VV at queue time — sent verbatim
	// to leader so the receiver merges the VV that matched obj, not
	// whatever the syncer's manager holds at drain time (which has
	// advanced past this write).
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
	nodeAddr  string     // This node's address (for originator tracking)
	ssl       bool       // Use HTTPS instead of HTTP
	vvManager *VVManager // Local VV manager — passed into SyncOptions so the bidirectional reconciler can compare VVs.
	queueMu   sync.Mutex
	queue     []pendingOp // Per-key operations queued during Pull

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

func newSyncer(client *http.Client, pivot string, keys []Key, ssl bool, vvManager *VVManager) *syncer {
	return &syncer{
		tracker:            newPullTracker(),
		client:             client,
		pivot:              pivot,
		keys:               keys,
		ssl:                ssl,
		vvManager:          vvManager,
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
func newSyncerPool(client *http.Client, keys []Key, configClusterURL string, ssl bool, vvManager *VVManager) *syncerPool {
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
		pool.syncers[pivotURL] = newSyncer(client, pivotURL, pivotKeys, ssl, vvManager)
	}

	return pool
}

// SetNodeAddr sets the node address on all syncers and drains anything that
// was queued while the address was unset (the pre-OnStart startup window).
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

// SetNodeAddr sets the node's address (called after server starts) and drains
// any operations that were queued while nodeAddr was empty (the pre-OnStart
// startup window). Drained ops are sent with the correct originator so the
// leader can identify and skip this node when fanning the change out.
func (s *syncer) SetNodeAddr(addr string) {
	s.queueMu.Lock()
	s.nodeAddr = addr
	s.queueMu.Unlock()
	s.drainQueue()
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

// pullKeyWithCacheUpdate syncs from pivot when pivot has writes the
// local node hasn't seen. Direction is decided by VersionVector when
// both sides expose one; LastEntry is used only when at least one side
// lacks a VV (older peers, very first sync before a VV bump). This is
// the periodic / TriggerNodeSync-driven pull path.
//
// Three properties matter for correctness:
//
//  1. The local-VV input must be the syncer's authoritative view, not
//     just whatever pivot reported last time. We read it from
//     s.vvManager (the same vvManager that AfterWrite bumps
//     synchronously on a local write). The previous code compared
//     pivot's current VV to a CACHE of pivot's previous VV, which
//     answered "did pivot change?" — not "is local behind pivot?".
//
//  2. LastEntry is NOT a direction overrider. Treating
//     activityPivot.LastEntry > activityLocal.LastEntry as "pivot has
//     newer data" re-introduces the wall-clock dependency VV was
//     supposed to remove: a peer with a forward-skewed clock can plant
//     a future-LastEntry record that beats every honest present-time
//     write here. LastEntry stays only as a fallback for the no-VV
//     case.
//
//  3. After a successful pull, pivot's VV is merged into local so the
//     receiver actually advances its causal frontier — a pull that
//     only updates storage without merging VV leaves vvManager.Get
//     returning the pre-pull state forever and every subsequent local
//     write looks VVConcurrent with pivot. handlers.Set already merges
//     on the push receive side; this is the symmetric merge for the
//     pull receive side.
//
// Caller must hold s.mu lock.
func (s *syncer) pullKeyWithCacheUpdate(keys []Key) error {
	update := false
	for _, _key := range keys {
		baseKey := baseKeyFromPath(_key.Path)
		activityPivot, err := checkPivotActivity(s.ClientOpts(), baseKey)
		if err != nil {
			continue
		}

		// Local VV from the authoritative source: vvManager. (1) above.
		var localVV VersionVector
		if s.vvManager != nil {
			localVV = s.vvManager.Get(baseKey)
		}

		// Local activity (LastEntry) is read for the no-VV fallback only.
		activityLocal, actErr := checkActivity(_key)

		needSync := false
		usedVV := false
		if len(activityPivot.VV) > 0 && len(localVV) > 0 {
			usedVV = true
			switch localVV.Compare(activityPivot.VV) {
			case VVLess:
				needSync = true
			case VVConcurrent:
				logConflict(baseKey, localVV, activityPivot.VV, "last-sync-wins: accepting pivot data")
				needSync = true
			}
		}
		if !usedVV && actErr == nil && activityPivot.LastEntry > activityLocal.LastEntry {
			// No-VV fallback only. (2) above keeps this gated to the
			// case where VV-based comparison was not possible.
			needSync = true
		}

		// Refresh the lastSyncedVV / lastSyncedActivity caches regardless
		// of the sync decision. Other code (e.g. checkPivotActivity
		// short-circuits) reads these.
		if len(activityPivot.VV) > 0 {
			s.vvMu.Lock()
			s.lastSyncedVV[baseKey] = activityPivot.VV.Clone()
			s.vvMu.Unlock()
		}
		s.activityMu.Lock()
		s.lastSyncedActivity[baseKey] = activityPivot.LastEntry
		s.activityMu.Unlock()

		if needSync {
			opts := SyncOptions{
				Key:       _key,
				LastEntry: activityPivot.LastEntry,
				OnDelete:  s.tracker.trackDelete,
				OnSet:     s.tracker.trackSet,
				VVManager: s.vvManager,
				LeaderVV:  activityPivot.VV,
			}
			if err := syncLocalEntriesWithTracking(s.ClientOpts(), opts); err == nil {
				update = true
				// (3) merge pivot's VV into local so the receiver's
				// vvManager reflects what it just absorbed.
				if s.vvManager != nil && len(activityPivot.VV) > 0 {
					s.vvManager.set(baseKey, activityPivot.VV)
				}
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

// drainQueue snapshots the pending queue and the current nodeAddr under
// queueMu, then sends each op outside the lock. Returns immediately if
// nodeAddr is unset (we're still in the pre-OnStart startup window) so the
// queued ops wait for SetNodeAddr to drain them with the correct originator.
func (s *syncer) drainQueue() {
	s.queueMu.Lock()
	if s.nodeAddr == "" || len(s.queue) == 0 {
		s.queueMu.Unlock()
		return
	}
	addr := s.nodeAddr
	pending := s.queue
	s.queue = make([]pendingOp, 0)
	s.queueMu.Unlock()

	for _, op := range pending {
		switch op.opType {
		case "set":
			if rVV, err := sendToLeader(s.ClientOpts(), op.key, op.obj, addr, op.vv); err == nil {
				s.mergeLeaderVVForItemKey(op.key, rVV)
			}
		case "del":
			if rVV, err := sendDeleteToLeader(s.ClientOpts(), op.key, op.ts, addr, op.vv); err == nil {
				s.mergeLeaderVVForItemKey(op.key, rVV)
			}
		}
	}
}

// mergeLeaderVVForItemKey is the syncer-side helper that finds the
// registered Key.Path matching itemKey, derives its base scope, and
// merges leaderVV into the local VVManager at that scope. Used by
// every push call site to keep local's VVManager in step with pivot's
// post-write VV (see mergeLeaderVV for the rationale).
func (s *syncer) mergeLeaderVVForItemKey(itemKey string, leaderVV VersionVector) {
	if s.vvManager == nil || len(leaderVV) == 0 {
		return
	}
	for _, k := range s.keys {
		if key.Match(k.Path, itemKey) {
			s.vvManager.set(baseKeyFromPath(k.Path), leaderVV)
			return
		}
	}
}

// processQueue sends all queued per-key operations to leader
func (s *syncer) processQueue() {
	s.drainQueue()
}

// processQueueLocked sends all queued per-key operations to leader (caller must hold s.mu)
func (s *syncer) processQueueLocked() {
	s.drainQueue()
}

// snapshotVV returns the originator's current VV at the registered
// path scope for the storage key, or nil if no VVManager is configured.
// Captured eagerly at queue/send time so the value travels with the
// obj to the leader. Resolves the matching registered Key.Path so the
// snapshot reads from the same scope writes/`/activity` use; without
// that resolution, snapshotVV("things/x") would look at item scope
// (nothing in production writes there).
func (s *syncer) snapshotVV(itemKey string) VersionVector {
	if s.vvManager == nil {
		return nil
	}
	for _, k := range s.keys {
		if key.Match(k.Path, itemKey) {
			return s.vvManager.Get(k.Path)
		}
	}
	// No registered match — fall back to whatever the item-scope key
	// returns; will be empty in normal operation.
	return s.vvManager.Get(itemKey)
}

// QueueOrSendSet sends a set operation to leader, or queues it if Pull is in
// progress or the syncer's nodeAddr hasn't been set yet (pre-OnStart startup
// window). The queue is drained by the next Pull completion or by
// SetNodeAddr, whichever comes first.
func (s *syncer) QueueOrSendSet(key string, obj meta.Object) {
	vv := s.snapshotVV(key)
	s.queueMu.Lock()
	if s.nodeAddr == "" {
		s.queue = append(s.queue, pendingOp{opType: "set", key: key, obj: obj, vv: vv})
		s.queueMu.Unlock()
		return
	}
	addr := s.nodeAddr
	s.queueMu.Unlock()

	if s.mu.TryLock() {
		if rVV, err := sendToLeader(s.ClientOpts(), key, obj, addr, vv); err == nil {
			s.mergeLeaderVVForItemKey(key, rVV)
		}
		s.mu.Unlock()
	} else {
		// Pull in progress - queue for later and wait for Pull to complete then process
		s.queueMu.Lock()
		s.queue = append(s.queue, pendingOp{opType: "set", key: key, obj: obj, vv: vv})
		s.queueMu.Unlock()
		// Wait for Pull to release the lock, then process queue under lock
		s.mu.Lock()
		s.processQueueLocked()
		s.mu.Unlock()
	}
}

// QueueOrSendDelete sends a delete operation to leader, or queues it if Pull
// is in progress or the syncer's nodeAddr hasn't been set yet (pre-OnStart
// startup window).
func (s *syncer) QueueOrSendDelete(key string, ts int64) {
	vv := s.snapshotVV(key)
	s.queueMu.Lock()
	if s.nodeAddr == "" {
		s.queue = append(s.queue, pendingOp{opType: "del", key: key, ts: ts, vv: vv})
		s.queueMu.Unlock()
		return
	}
	addr := s.nodeAddr
	s.queueMu.Unlock()

	if s.mu.TryLock() {
		if rVV, err := sendDeleteToLeader(s.ClientOpts(), key, ts, addr, vv); err == nil {
			s.mergeLeaderVVForItemKey(key, rVV)
		}
		s.mu.Unlock()
	} else {
		// Pull in progress - queue for later and wait for Pull to complete then process
		s.queueMu.Lock()
		s.queue = append(s.queue, pendingOp{opType: "del", key: key, ts: ts, vv: vv})
		s.queueMu.Unlock()
		// Wait for Pull to release the lock, then process queue under lock
		s.mu.Lock()
		s.processQueueLocked()
		s.mu.Unlock()
	}
}

// Sync performs bidirectional synchronization with tracking. Returns nil
// without contacting the leader if nodeAddr is unset (pre-OnStart startup
// window) — sending with empty Originator would prevent the leader from
// skipping this node when fanning the change out, producing a self-trigger
// echo. Reachable callers (AutoSyncOnStart, /synchronize/node) re-trigger
// after OnStart, so skipping here is safe.
func (s *syncer) Sync() error {
	s.queueMu.Lock()
	addr := s.nodeAddr
	s.queueMu.Unlock()
	if addr == "" {
		return nil
	}
	s.mu.Lock()
	opts := SyncOptions{
		Originator:     addr,
		IsRecentDelete: s.tracker.pulledDelete,
		OnDelete:       s.tracker.trackDelete,
		OnSet:          s.tracker.trackSet,
		VVManager:      s.vvManager,
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

// HandlerWriteTracker counts in-flight handler-driven writes per key.
// The Set/Delete handlers Mark before the storage write and the storage
// event callback Consumes when the event lands; while the count for a
// key is > 0, the callback knows a handler will own the post-write work
// (VV bump + fanout/push) and skips its own. A counter — not a single
// last-writer-wins entry — is required because two rapid handler writes
// to the same key would otherwise have one tracker entry but produce
// two storage events; the second event would find an empty tracker and
// the callback would double-do the work.
//
// Direct (non-handler) storage writes never call Mark, so Consume returns
// false and the callback runs its full path — that's how the callback
// remains the source of truth for direct writes.
type HandlerWriteTracker struct {
	mu      sync.Mutex
	pending map[string]int
}

// NewHandlerWriteTracker creates an empty tracker.
func NewHandlerWriteTracker() *HandlerWriteTracker {
	return &HandlerWriteTracker{pending: make(map[string]int)}
}

// Mark records that a handler is about to drive a write for the given key.
// Must be called before the storage write so the dedup signal is in place
// by the time the watch goroutine processes the event.
func (t *HandlerWriteTracker) Mark(key string) {
	t.mu.Lock()
	t.pending[key]++
	t.mu.Unlock()
}

// Unmark drops a previously-recorded mark — call from the handler's error
// path when the storage write didn't fire an event.
func (t *HandlerWriteTracker) Unmark(key string) {
	t.mu.Lock()
	if t.pending[key] > 0 {
		t.pending[key]--
		if t.pending[key] == 0 {
			delete(t.pending, key)
		}
	}
	t.mu.Unlock()
}

// Consume returns true and decrements the pending count if a mark is
// present for the key (i.e. this event is handler-driven). Returns false
// otherwise (i.e. this event is from a direct storage write).
func (t *HandlerWriteTracker) Consume(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pending[key] == 0 {
		return false
	}
	t.pending[key]--
	if t.pending[key] == 0 {
		delete(t.pending, key)
	}
	return true
}

// Has reports whether a mark is currently outstanding for the key,
// without consuming it. Used by AfterWrite's synchronous VV bump to
// skip its bump when a handler will explicitly bump after SetWithMeta
// returns. The watch goroutine still Consumes the mark to gate the
// rest of the SyncCallback processing.
func (t *HandlerWriteTracker) Has(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.pending[key] > 0
}

// applyPivotFanout triggers a sync on every healthy peer node except the
// originating one. Shared between the Set/Delete handlers (called
// synchronously after the local VV bump) and the storage event callback
// (called only for direct, non-handler writes).
func applyPivotFanout(instance *Instance, getNodes getNodes, nodeHealth *NodeHealth, keyPath, originatorPeer string) {
	if instance == nil || instance.triggers == nil || getNodes == nil {
		return
	}
	for _, node := range getNodes() {
		// Self-marker is collapsed by peerOriginator before this is called,
		// so an originatorPeer of "" means "no peer to skip".
		if node == originatorPeer && originatorPeer != "" {
			continue
		}
		if nodeHealth != nil && !nodeHealth.IsCompatible(node) {
			continue
		}
		instance.triggers.Trigger(node, keyPath)
	}
}

// applyNodePush queues a Set or Delete to push to the pivot leader for the
// node-role key. Shared between handler post-write and the storage event
// callback's direct-write path.
func applyNodePush(pool *syncerPool, db storage.Database, effectiveURL, op, itemKey string) {
	if pool == nil {
		return
	}
	s := pool.syncers[effectiveURL]
	if s == nil {
		return
	}
	if op == "del" {
		s.QueueOrSendDelete(itemKey, time.Now().UnixNano())
		return
	}
	obj, err := db.Get(itemKey)
	if err == nil {
		s.QueueOrSendSet(itemKey, obj)
	}
}

// StorageSyncConfig holds configuration for storage sync callback creation.
type StorageSyncConfig struct {
	Client           *http.Client
	ConfigClusterURL string
	Keys             []Key
	NodesKey         string
	GetNodes         getNodes
	Pool             *syncerPool
	NodeHealth       *NodeHealth
	HandlerTracker   *HandlerWriteTracker
	Instance         *Instance
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

		// Get and clear the originator for this key. A non-empty entry
		// Handler-driven writes Mark before db.Set, so a Consume hit means
		// "a handler will own both the VV bump and the post-write fanout/push";
		// skip BOTH here so the handler can sequence them: bump first, then
		// trigger peers. That ordering guarantee matters because a peer woken
		// by the trigger immediately reads /activity to compare VVs, and a
		// pre-bump VV would make it skip the pull. A miss means a direct
		// (non-handler) storage write — the callback is then the only place
		// that can bump and propagate.
		//
		// Caveat: a direct write that lands BETWEEN a handler's Mark and the
		// watch goroutine processing the handler's event would Consume the
		// handler's mark; the direct write would then get no bump/fanout
		// here, and the handler's later event would find no mark and bump
		// again. Closed-network deployment assumes a single writer per key
		// path, so this race is unreachable in practice. If that assumption
		// ever weakens, swap the per-key counter for an originator-tagged
		// event id matched 1:1 to a handler write.
		if cfg.HandlerTracker != nil && cfg.HandlerTracker.Consume(event.Key) {
			return
		}
		// VV bump: synchronous AfterWrite on attached storages handles
		// the bump before the write returns (see Instance.Attach /
		// Instance.bumpVVForLocalWrite), so the async bump here would
		// double-count for those events. Storages that were not
		// installed through Attach — most commonly server.Storage,
		// which ooo.Server.Start brings up without our AfterWrite
		// wrapper — still need the bump from this callback because
		// there is no synchronous bump path for them.
		if cfg.Instance != nil && cfg.Instance.VVManager != nil &&
			!cfg.Instance.IsAttached(matchedDB) {
			// Increment at PATH scope — matchedKey is already
			// baseKeyFromPath(matchedKeyConfig.Path), the same scope
			// /activity exposes. Bumping at event.Key (item scope)
			// would land in a separate, never-read VV entry.
			cfg.Instance.VVManager.increment(matchedKey)
		}
		if isPivotForKey {
			applyPivotFanout(cfg.Instance, cfg.GetNodes, cfg.NodeHealth, matchedKeyConfig.Path, "")
		} else {
			applyNodePush(cfg.Pool, matchedDB, effectiveClusterURL, event.Operation, event.Key)
		}
	}
}
