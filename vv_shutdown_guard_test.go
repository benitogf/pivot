package pivot

// Characterization test for the VVManager shutdown guard: once Shutdown() has
// run, saveToStorage must not write — otherwise a write could race storage
// Close(). This pins the observable behavior so the guard's mechanism can be
// simplified (mutex-only, no redundant atomic) without changing what callers
// see.

import (
	"testing"

	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/stretchr/testify/require"
)

func TestVVManagerDoesNotPersistAfterShutdown(t *testing.T) {
	monotonic.Init()
	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()

	m := NewVVManager(db, "node1")
	m.increment("things/*") // persists things -> {node1:1}

	before, err := db.Get(VVKeyPrefix + "things")
	require.NoError(t, err)

	m.Shutdown()
	m.increment("things/*") // in-memory bumps, but the storage write must be skipped

	after, err := db.Get(VVKeyPrefix + "things")
	require.NoError(t, err)
	require.Equal(t, string(before.Data), string(after.Data),
		"VVManager persisted a write after Shutdown")
}
