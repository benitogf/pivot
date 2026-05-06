package pivot_test

// Regression test for the bug where checkClusterURLChange ran at Setup time
// and silently no-op'd when storage wasn't yet active. With memory-backed
// embedded storage (storage activates inside server.Start, not before), the
// wipe-on-cluster-URL-change protection was bypassed entirely:
//   - Round 1 Setup: storage inactive → check no-op → URL fingerprint never
//     persisted to storage.
//   - Round 1 server.Start: storage activates, but nothing re-runs the check.
//   - Round 1 Close: persisted state has user data, no URL fingerprint.
//   - Round 2 Setup with a different ClusterURL: storage inactive → no-op.
//   - Round 2 server.Start: storage activates, but the (still missing) check
//     never runs. Stale data from leaderA stays mixed with whatever leaderB
//     pushes.
//
// After the fix, checkClusterURLChange runs from inside the OnStart wrapper
// once storage is guaranteed active. It also encodes the persisted URL as a
// JSON string (the storage layer treats values as json.RawMessage; raw
// host:port bytes aren't valid JSON, so the prior format silently failed to
// round-trip on reload regardless of when the function ran).

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/benitogf/ko"
	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// runRound boots a fresh server with the given embedded storage path and
// cluster URL, optionally writes a payload, then closes the server.
func runRound(t *testing.T, dbPath, clusterURL string, write func(storage.Database)) {
	t.Helper()
	embedded := &ko.EmbeddedStorage{Path: dbPath}
	dataStorage := storage.New(storage.LayeredConfig{
		Memory:   storage.NewMemoryLayer(),
		Embedded: embedded,
	})
	server := &ooo.Server{
		Silence: true,
		Static:  true,
		Storage: dataStorage,
		Router:  mux.NewRouter(),
	}
	server = pivot.Setup(server, pivot.Config{
		Keys: []pivot.Key{
			{Path: "items/*"},
		},
		NodesKey:            "devices/*",
		ClusterURL:          clusterURL,
		AutoSyncOnStart:     false, // no leader to sync with; we just want the URL-change protection
		HealthCheckInterval: time.Hour,
		SyncRetryInterval:   time.Hour,
	})

	// The storage must NOT be pre-started — that's the whole point of the bug.
	require.False(t, dataStorage.Active(), "test precondition: storage must be inactive at this point")

	server.Start("127.0.0.1:0")

	if write != nil {
		write(dataStorage)
	}

	server.Close(os.Interrupt)
}

func TestClusterURLChangeWipesAfterRestartWithChangedURL(t *testing.T) {
	monotonic.Init()
	dbPath := fmt.Sprintf("test/cluster_url_change_%d", time.Now().UnixNano())
	defer os.RemoveAll(dbPath)

	// Round 1: come up against leaderA, write some user data, shut down.
	runRound(t, dbPath, "127.0.0.1:19111", func(db storage.Database) {
		_, err := db.Set("items/x", []byte(`{"v":1}`))
		require.NoError(t, err, "round 1: write user data while storage active")
	})

	// Sanity: the user data persisted across the close. If even items/x is
	// missing, something deeper is wrong with embedded persistence and the
	// rest of the test would fail for the wrong reason.
	{
		embedded := &ko.EmbeddedStorage{Path: dbPath}
		probe := storage.New(storage.LayeredConfig{
			Memory:   storage.NewMemoryLayer(),
			Embedded: embedded,
		})
		require.NoError(t, probe.Start(storage.Options{}))
		obj, err := probe.Get("items/x")
		require.NoError(t, err, "between rounds: items/x must persist across close — embedded storage issue if not")
		require.NotEmpty(t, obj.Data)
		probe.Close()
	}

	// Round 2: come up against leaderB (different URL). The cluster-URL change
	// detection must wipe items/* before this round's normal operation begins.
	runRound(t, dbPath, "127.0.0.1:19222", func(db storage.Database) {
		_, err := db.Get("items/x")
		require.Error(t, err, "round 2: items/x must be wiped because cluster URL changed (was leaderA, now leaderB)")
	})
}

// TestClusterURLChangeNoWipeOnSameURL asserts the inverse: the wipe must NOT
// fire when the cluster URL is unchanged across restarts. Otherwise we'd
// destroy data on every routine restart.
func TestClusterURLChangeNoWipeOnSameURL(t *testing.T) {
	monotonic.Init()
	dbPath := fmt.Sprintf("test/cluster_url_unchanged_%d", time.Now().UnixNano())
	defer os.RemoveAll(dbPath)

	const sameURL = "127.0.0.1:19333"

	runRound(t, dbPath, sameURL, func(db storage.Database) {
		_, err := db.Set("items/y", []byte(`{"v":2}`))
		require.NoError(t, err)
	})

	runRound(t, dbPath, sameURL, func(db storage.Database) {
		obj, err := db.Get("items/y")
		require.NoError(t, err, "items/y must persist across restart with unchanged cluster URL")
		require.NotEmpty(t, obj.Data)
	})
}
