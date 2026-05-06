package pivot

// VVManager unit tests live in the internal package so they can drive the
// unexported increment method directly. There's only one increment in the
// codebase — production callers (sync.go, handlers.go) and these tests use
// the same path.

import (
	"testing"

	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/stretchr/testify/require"
)

// BenchmarkVVManagerIncrement measures the per-event cost of the storage-event
// hot path's only mandatory operation. The number that matters most is ns/op
// (~1µs target on modern hardware). Allocations should be near zero — every
// extra alloc is paid for every storage event in a busy cluster.
func BenchmarkVVManagerIncrement(b *testing.B) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	if err := db.Start(storage.Options{}); err != nil {
		b.Fatal(err)
	}
	storage.WatchWithCallback(db, func(storage.Event) {})
	b.Cleanup(func() { db.Close() })
	m := NewVVManager(db, "leader")

	keys := []string{"items", "things", "settings", "events", "metrics"}
	// Prime the per-key entries so we measure steady-state cost, not first-load.
	for _, k := range keys {
		m.increment(k)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		m.increment(keys[i%len(keys)])
	}
}

func TestVVManagerBasic(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	db.Start(storage.Options{})
	defer db.Close()

	manager := NewVVManager(db, "leader")

	// Get non-existent key returns empty VV
	require.Empty(t, manager.Get("testkey"))

	// First increment lands a counter of 1.
	manager.increment("testkey")
	require.Equal(t, int64(1), manager.Get("testkey")["leader"])

	// Second increment bumps to 2.
	manager.increment("testkey")
	require.Equal(t, int64(2), manager.Get("testkey")["leader"])

	// set merges a remote vector — local "leader" stays at 2, "nodeA" arrives.
	manager.set("testkey", VersionVector{"leader": 2, "nodeA": 5})

	vv := manager.Get("testkey")
	require.Equal(t, int64(2), vv["leader"])
	require.Equal(t, int64(5), vv["nodeA"])
}

// TestVVManagerIncrementWithEmptyNodeIDDoesNotPollute pins the invariant that
// increment must refuse to write when nodeID is unset. Node servers construct
// their VVManager during Setup with nodeID="" and only call SetNodeID once
// server.Address is known (inside OnStart). The TCP listener is up before
// OnStart fires, so any storage event reaching makeStorageSync in that window
// would call increment with an empty nodeID; without this guard the persisted
// VV gains a "" entry that no peer ever increments and lives forever in
// storage. Test both the in-memory cache via Get and the on-disk bytes via
// fresh-manager reload to catch either side regressing.
func TestVVManagerIncrementWithEmptyNodeIDDoesNotPollute(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()

	m := NewVVManager(db, "")
	m.increment("things")
	m.increment("things")

	vv := m.Get("things")
	_, hasEmpty := vv[""]
	require.False(t, hasEmpty, "increment with empty nodeID polluted in-memory VV with '' key: %v", vv)

	// Reload from storage to confirm nothing was persisted under "".
	reloaded := NewVVManager(db, "")
	persisted := reloaded.Get("things")
	_, hasEmpty = persisted[""]
	require.False(t, hasEmpty, "increment with empty nodeID polluted on-disk VV with '' key: %v", persisted)
}

// TestVVManagerIncrementResumesAfterSetNodeID verifies the fix doesn't cause
// permanent loss: once SetNodeID is called with a real address, subsequent
// increments must work normally.
func TestVVManagerIncrementResumesAfterSetNodeID(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()

	m := NewVVManager(db, "")
	m.increment("things") // dropped — nodeID empty
	m.SetNodeID("127.0.0.1:9000")
	m.increment("things")
	m.increment("things")

	vv := m.Get("things")
	require.Equal(t, int64(2), vv["127.0.0.1:9000"], "increments after SetNodeID must land on the real node ID")
	_, hasEmpty := vv[""]
	require.False(t, hasEmpty, "VV must not carry phantom '' counter after SetNodeID")
}

func TestVVManagerPersistence(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	db.Start(storage.Options{})
	defer db.Close()

	// Create manager and bump counter twice.
	manager1 := NewVVManager(db, "leader")
	manager1.increment("testkey")
	manager1.increment("testkey")

	// Fresh manager against the same storage must see the persisted value.
	manager2 := NewVVManager(db, "leader")
	require.Equal(t, int64(2), manager2.Get("testkey")["leader"])
}
