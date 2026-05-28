package pivot

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/stretchr/testify/require"
)

func TestSingleKeyPullIgnoresSkewedDeleteTimestampWhenVVLess(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	// Simulate a skewed future tombstone timestamp for this key.
	_, err := localDB.Set(StoragePrefix+"things/x", []byte("999999999999999999"))
	require.NoError(t, err)

	vvm := NewVVManager(localDB, "node")
	vvm.set("things/x", VersionVector{"node": 1})

	mux := http.NewServeMux()
	mux.HandleFunc("/_pivot/activity/things/x", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ActivityEntry{
			LastEntry: 9_999,
			VV:        VersionVector{"node": 2}, // leader dominates local
		})
	})
	mux.HandleFunc("/_pivot/pivot/things/x", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"index":   "x",
			"path":    "things/x",
			"created": int64(100),
			"updated": int64(100),
			"data":    json.RawMessage(`{"v":"from-leader"}`),
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	leader := srv.URL[len("http://"):]

	err = synchronizeItemWithTracking(ClientOpts{Client: srv.Client(), Leader: leader}, SyncOptions{
		Key:        Key{Path: "things/x", Database: localDB},
		Originator: "node",
		VVManager:  vvm,
	})
	require.NoError(t, err)

	got, err := localDB.Get("things/x")
	require.NoError(t, err)
	require.JSONEq(t, `{"v":"from-leader"}`, string(got.Data),
		"VVLess pull should not be blocked by skewed local delete timestamp")

	_, err = localDB.Get(StoragePrefix + "things/x")
	require.Error(t, err, "successful set should clear single-key delete tombstone")
}
