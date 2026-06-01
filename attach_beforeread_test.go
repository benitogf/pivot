package pivot

import (
	"testing"

	"github.com/benitogf/ooo/storage"
	"github.com/stretchr/testify/require"
)

// TestAttachChainsUserBeforeRead pins that Attach composes the caller's
// BeforeRead with pivot's sync-on-read callback instead of dropping it.
// Attach's documented use case is external storages that want to read; a
// consumer that needs its own BeforeRead (lazy-load, audit, metrics) must
// still see it invoked, alongside pivot's. Both Attach paths are covered:
// the not-started Start(opts) branch and the already-started
// SetBeforeRead branch.
func TestAttachChainsUserBeforeRead(t *testing.T) {
	t.Run("not-started storage (Start path)", func(t *testing.T) {
		var pivotCalled, userCalled bool
		inst := &Instance{BeforeRead: func(string) { pivotCalled = true }}
		db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
		require.NoError(t, inst.Attach(db, storage.Options{
			BeforeRead: func(string) { userCalled = true },
		}))
		defer db.Close()

		_, _ = db.Get("things/1") // read triggers BeforeRead

		require.True(t, pivotCalled, "pivot sync-on-read BeforeRead must run")
		require.True(t, userCalled, "caller's BeforeRead must run, not be dropped")
	})

	t.Run("already-started storage (SetBeforeRead path)", func(t *testing.T) {
		var pivotCalled, userCalled bool
		inst := &Instance{BeforeRead: func(string) { pivotCalled = true }}
		db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
		require.NoError(t, db.Start(storage.Options{}))
		defer db.Close()

		require.NoError(t, inst.Attach(db, storage.Options{
			BeforeRead: func(string) { userCalled = true },
		}))

		_, _ = db.Get("things/1") // read triggers BeforeRead

		require.True(t, pivotCalled, "pivot sync-on-read BeforeRead must run")
		require.True(t, userCalled, "caller's BeforeRead must run, not be dropped")
	})
}
