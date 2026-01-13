package pivot_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/goccy/go-json"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

func TestGetClusterLeaderInfoWithNodes(t *testing.T) {
	// Create a server with storage
	server := &ooo.Server{
		Router:  mux.NewRouter(),
		Silence: true,
		Storage: storage.New(storage.LayeredConfig{
			Memory: storage.NewMemoryLayer(),
		}),
	}

	// Setup pivot with nodes key
	config := pivot.Config{
		Keys:       []pivot.Key{{Path: "test/*"}},
		NodesKey:   "nodes/*",
		ClusterURL: "", // Empty = pivot server
	}

	pivot.Setup(server, config)
	server.Start("localhost:0")
	defer server.Close(nil)

	// Add a node entry to storage
	nodeData := []byte(`{"ip": "192.168.1.100", "port": 8080}`)
	_, err := server.Storage.Set("nodes/server1", nodeData)
	require.NoError(t, err)

	// Get pivot info
	require.NotNil(t, server.GetPivotInfo, "GetPivotInfo should be set")
	info := server.GetPivotInfo()
	require.NotNil(t, info, "PivotInfo should not be nil")
	require.Equal(t, "pivot", info.Role)
	require.Len(t, info.Nodes, 1, "Should have 1 node")
	require.Equal(t, "192.168.1.100:8080", info.Nodes[0].Address)
}

func TestGetClusterLeaderInfoWithMultipleNodes(t *testing.T) {
	server := &ooo.Server{
		Router:  mux.NewRouter(),
		Silence: true,
		Storage: storage.New(storage.LayeredConfig{
			Memory: storage.NewMemoryLayer(),
		}),
	}

	config := pivot.Config{
		Keys:       []pivot.Key{{Path: "test/*"}},
		NodesKey:   "nodes/*",
		ClusterURL: "",
	}

	pivot.Setup(server, config)
	server.Start("localhost:0")
	defer server.Close(nil)

	// Add multiple node entries - some with port, some without
	server.Storage.Set("nodes/server1", []byte(`{"ip": "192.168.1.100", "port": 8080}`))
	server.Storage.Set("nodes/server2", []byte(`{"ip": "192.168.1.101", "port": 8081}`))
	server.Storage.Set("nodes/client1", []byte(`{"name": "client", "status": "online"}`))
	server.Storage.Set("nodes/device1", []byte(`{"ip": "192.168.1.200"}`))

	info := server.GetPivotInfo()
	require.NotNil(t, info)
	require.Equal(t, "pivot", info.Role)
	require.Len(t, info.Nodes, 2, "Should have 2 nodes (only entries with ip AND port)")
}

func TestGetClusterLeaderInfoViaHTTPAPI(t *testing.T) {
	// This test simulates how data would be written through the HTTP API
	server := &ooo.Server{
		Router:  mux.NewRouter(),
		Silence: true,
		Storage: storage.New(storage.LayeredConfig{
			Memory: storage.NewMemoryLayer(),
		}),
	}

	// Allow writes to nodes/*
	server.WriteFilter("nodes/*", func(key string, data json.RawMessage) (json.RawMessage, error) {
		return data, nil
	})

	config := pivot.Config{
		Keys:       []pivot.Key{{Path: "test/*"}},
		NodesKey:   "nodes/*",
		ClusterURL: "",
	}

	pivot.Setup(server, config)
	server.Start("localhost:0")
	defer server.Close(nil)

	// Write node data via HTTP POST (simulating user action)
	nodeJSON := `{"ip": "192.168.1.100", "port": 8080}`
	req, _ := http.NewRequest("POST", "/nodes/server1", strings.NewReader(nodeJSON))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	server.Router.ServeHTTP(w, req)
	require.Equal(t, 200, w.Code, "POST should succeed")

	// Verify data was stored correctly
	objs, err := server.Storage.GetList("nodes/*")
	require.NoError(t, err)
	require.Len(t, objs, 1)
	t.Logf("Stored data: %s", string(objs[0].Data))

	// Get pivot info
	info := server.GetPivotInfo()
	require.NotNil(t, info)
	require.Equal(t, "pivot", info.Role)
	require.Len(t, info.Nodes, 1, "Should have 1 node")
	require.Equal(t, "192.168.1.100:8080", info.Nodes[0].Address)
}
