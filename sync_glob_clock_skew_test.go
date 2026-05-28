package pivot

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/stretchr/testify/require"
)

func TestGlobPullUsesVVAuthorityWhenTimestampsDisagree(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	// Local has a future timestamp with stale payload.
	_, err := localDB.SetWithMeta("things/x", []byte(`{"v":"stale-local"}`), 9_999, 9_999)
	require.NoError(t, err)

	vvm := NewVVManager(localDB, "node")
	vvm.set("things/*", VersionVector{"leader": 2, "node": 1}) // local is VVLess

	var pulled atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("/_pivot/activity/things", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ActivityEntry{
			LastEntry: 9_999,
			VV:        VersionVector{"leader": 3, "node": 1},
		})
	})
	mux.HandleFunc("/_pivot/pivot/things/", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		pulled.Store(true)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{
				"index":   "x",
				"path":    "things/x",
				"created": int64(100),
				"updated": int64(100),
				"data":    json.RawMessage(`{"v":"leader-newer-causally"}`),
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	leader := srv.URL[len("http://"):]

	err = synchronizeItemWithTracking(ClientOpts{Client: srv.Client(), Leader: leader}, SyncOptions{
		Key:        Key{Path: "things/*", Database: localDB},
		Originator: "node",
		VVManager:  vvm,
	})
	require.NoError(t, err)
	require.True(t, pulled.Load(), "VVLess should have pulled from leader")

	got, err := localDB.Get("things/x")
	require.NoError(t, err)
	require.JSONEq(t, `{"v":"leader-newer-causally"}`, string(got.Data),
		"glob pull must not reject leader data just because leader Updated is numerically smaller")
}

func TestGlobPushUsesVVAuthorityWhenTimestampsDisagree(t *testing.T) {
	monotonic.Init()
	localDB := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, localDB.Start(storage.Options{}))
	defer localDB.Close()
	storage.WatchWithCallback(localDB, func(storage.Event) {})

	// Local has causally newer data but smaller wall-clock timestamp.
	_, err := localDB.SetWithMeta("things/x", []byte(`{"v":"local-causally-newer"}`), 100, 100)
	require.NoError(t, err)

	vvm := NewVVManager(localDB, "node")
	vvm.set("things/*", VersionVector{"leader": 4, "node": 3}) // local is VVGreater

	var pushes atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/_pivot/activity/things", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ActivityEntry{
			LastEntry: 9_999,
			VV:        VersionVector{"leader": 4, "node": 2},
		})
	})
	mux.HandleFunc("/_pivot/pivot/things/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{
					"index":   "x",
					"path":    "things/x",
					"created": int64(9_999),
					"updated": int64(9_999),
					"data":    json.RawMessage(`{"v":"leader-stale-by-vv"}`),
				},
			})
		case http.MethodPost:
			pushes.Add(1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	leader := srv.URL[len("http://"):]

	err = synchronizeItemWithTracking(ClientOpts{Client: srv.Client(), Leader: leader}, SyncOptions{
		Key:        Key{Path: "things/*", Database: localDB},
		Originator: "node",
		VVManager:  vvm,
	})
	require.NoError(t, err)
	require.Greater(t, pushes.Load(), int32(0),
		"VVGreater push must not be blocked by leader holding a future timestamp")
}
