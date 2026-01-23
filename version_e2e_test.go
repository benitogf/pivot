package pivot_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/client"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

type VersionItem struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func VersionTestServer(t *testing.T, clusterURL string) (*ooo.Server, *sync.WaitGroup) {
	wg := &sync.WaitGroup{}

	afterWrite := func(key string) {
		if strings.HasPrefix(key, "pivot/") {
			return
		}
		wg.Done()
	}

	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Client = &http.Client{Timeout: 5 * time.Second}
	server.Audit = func(r *http.Request) bool { return true }

	config := pivot.Config{
		Keys:       []pivot.Key{{Path: "items/*"}},
		NodesKey:   "nodes/*",
		ClusterURL: clusterURL,
	}

	pivot.Setup(server, config)
	server.Storage.Start(storage.Options{AfterWrite: afterWrite})
	server.OpenFilter("items/*")
	server.OpenFilter("nodes/*")
	server.Start("localhost:0")

	return server, wg
}

func TestE2E_VersionSync_CompatibleServers(t *testing.T) {
	// This test verifies that compatible servers can detect each other's version
	// Full bidirectional sync is tested in cluster_test.go (testClusterSync)

	pivotServer, _ := VersionTestServer(t, "")
	defer pivotServer.Close(os.Interrupt)

	nodeServer, _ := VersionTestServer(t, "http://"+pivotServer.Address)
	defer nodeServer.Close(os.Interrupt)

	nodeHost, nodePortStr, _ := net.SplitHostPort(nodeServer.Address)
	nodePort, _ := strconv.Atoi(nodePortStr)

	// Register node on pivot
	pivotServer.Storage.Set("nodes/testnode", []byte(`{"ip":"`+nodeHost+`","port":`+strconv.Itoa(nodePort)+`}`))

	// Get pivot instance to access NodeHealth directly
	instance := pivot.GetInstance(pivotServer)
	require.NotNil(t, instance)
	require.NotNil(t, instance.NodeHealth)

	// Verify pivot detects node as compatible via on-demand check
	isCompatible := instance.NodeHealth.IsCompatible(nodeServer.Address)
	require.True(t, isCompatible, "node should be compatible")
	require.Equal(t, pivot.ProtocolVersion, instance.NodeHealth.GetProtocol(nodeServer.Address))

	// Verify node's role
	nodeInfo := pivot.GetPivotInfo(nodeServer)()
	require.NotNil(t, nodeInfo)
	require.Equal(t, "node", nodeInfo.Role)
	require.Equal(t, "http://"+pivotServer.Address, nodeInfo.PivotIP)
}

func TestE2E_VersionSync_PivotBlocksIncompatibleNode(t *testing.T) {
	var syncAttempts int32

	mockNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pivot.RoutePrefix+"/version" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(pivot.VersionInfo{Protocol: "1.0"})
			return
		}
		if r.URL.Path == pivot.RoutePrefix+"/synchronize/pivot" {
			atomic.AddInt32(&syncAttempts, 1)
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockNode.Close()

	var wsWg sync.WaitGroup
	var pivotItems []client.Meta[VersionItem]
	var mu sync.Mutex

	pivotServer, _ := VersionTestServer(t, "")
	defer pivotServer.Close(os.Interrupt)

	ctx := t.Context()

	wsWg.Add(1)
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: pivotServer.Address},
		Silence: true,
	}, "items/*", client.SubscribeListEvents[VersionItem]{
		OnMessage: func(data []client.Meta[VersionItem]) {
			mu.Lock()
			pivotItems = data
			mu.Unlock()
			wsWg.Done()
		},
	})

	wsWg.Wait()

	mockHost, mockPortStr, _ := net.SplitHostPort(mockNode.Listener.Addr().String())
	mockPort, _ := strconv.Atoi(mockPortStr)

	// Register mock node
	pivotServer.Storage.Set("nodes/mocknode", []byte(`{"ip":"`+mockHost+`","port":`+strconv.Itoa(mockPort)+`}`))

	// Get pivot instance to trigger version detection
	instance := pivot.GetInstance(pivotServer)
	require.NotNil(t, instance)
	mockAddr := mockNode.Listener.Addr().String()
	isCompatible := instance.NodeHealth.IsCompatible(mockAddr)
	require.False(t, isCompatible, "mock node should be incompatible")
	require.Equal(t, "1.0", instance.NodeHealth.GetProtocol(mockAddr), "mock node should report version 1.0")

	atomic.StoreInt32(&syncAttempts, 0)

	// Push another item - only pivot WS event expected (no sync to incompatible node)
	wsWg.Add(1)
	item := VersionItem{Name: "test", Value: 42}
	data, _ := json.Marshal(item)
	resp, err := http.Post("http://"+pivotServer.Address+"/items/*", "application/json", bytes.NewReader(data))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	wsWg.Wait()

	require.Equal(t, int32(0), atomic.LoadInt32(&syncAttempts),
		"pivot should NOT sync to incompatible node")

	mu.Lock()
	require.GreaterOrEqual(t, len(pivotItems), 1, "pivot should have items")
	mu.Unlock()
}

func TestE2E_VersionSync_NodeBlocksIncompatiblePivot(t *testing.T) {
	var syncAttempts int32

	mockPivot := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pivot.RoutePrefix+"/version" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(pivot.VersionInfo{Protocol: "1.0"})
			return
		}
		if r.URL.Path == pivot.RoutePrefix+"/pivot/items" {
			atomic.AddInt32(&syncAttempts, 1)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
			return
		}
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockPivot.Close()

	var wsWg sync.WaitGroup
	var nodeItems []client.Meta[VersionItem]
	var mu sync.Mutex

	nodeServer, _ := VersionTestServer(t, "http://"+mockPivot.Listener.Addr().String())
	defer nodeServer.Close(os.Interrupt)

	ctx := t.Context()

	wsWg.Add(1)
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: nodeServer.Address},
		Silence: true,
	}, "items/*", client.SubscribeListEvents[VersionItem]{
		OnMessage: func(data []client.Meta[VersionItem]) {
			mu.Lock()
			nodeItems = data
			mu.Unlock()
			wsWg.Done()
		},
	})

	wsWg.Wait()

	atomic.StoreInt32(&syncAttempts, 0)

	// Push item to node - only node WS event expected (no sync to incompatible pivot)
	wsWg.Add(1)
	item := VersionItem{Name: "nodeItem", Value: 123}
	data, _ := json.Marshal(item)
	resp, err := http.Post("http://"+nodeServer.Address+"/items/*", "application/json", bytes.NewReader(data))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	wsWg.Wait()

	require.Equal(t, int32(0), atomic.LoadInt32(&syncAttempts),
		"node should NOT sync to incompatible pivot")

	mu.Lock()
	require.Equal(t, 1, len(nodeItems), "node should have 1 item locally")
	require.Equal(t, "nodeItem", nodeItems[0].Data.Name)
	mu.Unlock()
}

func TestE2E_VersionSync_OldNodeWithoutVersionEndpoint(t *testing.T) {
	var syncAttempts int32

	oldNode := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == pivot.RoutePrefix+"/synchronize/pivot" {
			atomic.AddInt32(&syncAttempts, 1)
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path == "/" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer oldNode.Close()

	var wsWg sync.WaitGroup
	var pivotItems []client.Meta[VersionItem]
	var mu sync.Mutex

	pivotServer, _ := VersionTestServer(t, "")
	defer pivotServer.Close(os.Interrupt)

	ctx := t.Context()

	wsWg.Add(1)
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: pivotServer.Address},
		Silence: true,
	}, "items/*", client.SubscribeListEvents[VersionItem]{
		OnMessage: func(data []client.Meta[VersionItem]) {
			mu.Lock()
			pivotItems = data
			mu.Unlock()
			wsWg.Done()
		},
	})

	wsWg.Wait()

	oldHost, oldPortStr, _ := net.SplitHostPort(oldNode.Listener.Addr().String())
	oldPort, _ := strconv.Atoi(oldPortStr)

	// Register old node
	pivotServer.Storage.Set("nodes/oldnode", []byte(`{"ip":"`+oldHost+`","port":`+strconv.Itoa(oldPort)+`}`))

	// Trigger initial item to force version detection
	wsWg.Add(1)
	pivotServer.Storage.Set("items/trigger", []byte(`{"name":"trigger","value":0}`))
	wsWg.Wait()

	info := pivot.GetPivotInfo(pivotServer)()
	found := false
	for _, node := range info.Nodes {
		if node.Address == oldNode.Listener.Addr().String() {
			found = true
			require.Equal(t, "unknown", node.Protocol)
			require.False(t, node.Compatible)
		}
	}
	require.True(t, found)

	atomic.StoreInt32(&syncAttempts, 0)

	// Push item - only pivot WS event expected (no sync to old node)
	wsWg.Add(1)
	item := VersionItem{Name: "test", Value: 1}
	data, _ := json.Marshal(item)
	resp, err := http.Post("http://"+pivotServer.Address+"/items/*", "application/json", bytes.NewReader(data))
	require.NoError(t, err)
	resp.Body.Close()

	wsWg.Wait()

	require.Equal(t, int32(0), atomic.LoadInt32(&syncAttempts),
		"pivot should NOT sync to old node without version endpoint")

	mu.Lock()
	require.GreaterOrEqual(t, len(pivotItems), 1)
	mu.Unlock()
}
