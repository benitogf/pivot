package pivot_test

import (
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	ooio "github.com/benitogf/ooo/io"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// TestNodeToLeaderSync verifies that node writes sync to leader correctly.
func TestNodeToLeaderSync(t *testing.T) {
	pivotServer, wgPivot := createEdgeTestServer("", nil)
	defer pivotServer.Close(os.Interrupt)

	nodeServer, wgNode := createEdgeTestServer(pivotServer.Address, nil)
	defer nodeServer.Close(os.Interrupt)

	// Use node server address so pivot can find the node for sync notifications
	nodeIP, nodePort, _ := net.SplitHostPort(nodeServer.Address)
	nodePortInt, _ := strconv.Atoi(nodePort)

	// Push and wait for sync (1 event on node + 1 event on pivot)
	wgNode.Add(1)
	wgPivot.Add(1)
	thingID, err := ooo.Push(nodeServer, "things/*", Thing{IP: nodeIP, Port: nodePortInt, On: true})
	require.NoError(t, err)
	wgNode.Wait()
	wgPivot.Wait()

	pivotThing, err := ooo.Get[Thing](pivotServer, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, true, pivotThing.Data.On)
	require.Equal(t, nodeIP, pivotThing.Data.IP)
	require.Equal(t, nodePortInt, pivotThing.Data.Port)

	// Second push and wait for sync
	wgNode.Add(1)
	wgPivot.Add(1)
	_, err = ooo.Push(nodeServer, "things/*", Thing{IP: "192.168.1.1", Port: 0, On: false})
	require.NoError(t, err)
	wgNode.Wait()
	wgPivot.Wait()

	pivotThings, err := ooo.GetList[Thing](pivotServer, "things/*")
	require.NoError(t, err)
	require.Equal(t, 2, len(pivotThings))

	t.Log("Node to pivot sync test passed!")
}

// TestSingleKeySync verifies sync works for non-glob keys.
func TestSingleKeySync(t *testing.T) {
	pivotServer, wgPivot := createEdgeTestServer("", nil)
	defer pivotServer.Close(os.Interrupt)

	nodeServer, wgNode := createEdgeTestServer(pivotServer.Address, nil)
	defer nodeServer.Close(os.Interrupt)

	wgNode.Add(1)
	wgPivot.Add(1)
	err := ooo.Set(nodeServer, "settings", Settings{DayEpoch: 100})
	require.NoError(t, err)
	wgNode.Wait()
	wgPivot.Wait()

	pivotSettings, err := ooo.Get[Settings](pivotServer, "settings")
	require.NoError(t, err)
	require.Equal(t, 100, pivotSettings.Data.DayEpoch)

	t.Log("Single key sync test passed!")
}

// TestEmptyStorageSync verifies sync works with empty storage.
func TestEmptyStorageSync(t *testing.T) {
	pivotServer := createEdgeTestServerNoSync("")
	defer pivotServer.Close(os.Interrupt)

	nodeServer := createEdgeTestServerNoSync(pivotServer.Address)
	defer nodeServer.Close(os.Interrupt)

	pivotThings, err := ooo.GetList[Thing](pivotServer, "things/*")
	require.NoError(t, err)
	require.Equal(t, 0, len(pivotThings))

	nodeThings, err := ooo.GetList[Thing](nodeServer, "things/*")
	require.NoError(t, err)
	require.Equal(t, 0, len(nodeThings))

	t.Log("Empty storage sync test passed!")
}

// TestSyncOnRead verifies that node reads trigger a pull from pivot.
// This tests the scenario where pivot has data that node doesn't know about yet.
// We write directly to pivot's storage (bypassing OnStorageEvent) to simulate
// a node that missed a notification.
func TestSyncOnRead(t *testing.T) {
	pivotServer := createEdgeTestServerNoSync("")
	defer pivotServer.Close(os.Interrupt)

	nodeServer := createEdgeTestServerNoSync(pivotServer.Address)
	defer nodeServer.Close(os.Interrupt)

	pivotCfg := ooio.RemoteConfig{Client: &http.Client{Timeout: 5 * time.Second}, Host: pivotServer.Address}

	// Write to pivot via HTTP (bypasses node notification since node isn't registered yet)
	// This simulates pivot having data that node doesn't know about
	err := ooio.RemoteSet(pivotCfg, "settings", Settings{DayEpoch: 42})
	require.NoError(t, err)

	// Node should NOT have the data yet (no sync was triggered)
	// But when we READ from node, BeforeRead triggers TryPull from pivot
	nodeSettings, err := ooo.Get[Settings](nodeServer, "settings")
	require.NoError(t, err)
	require.Equal(t, 42, nodeSettings.Data.DayEpoch, "node should have pulled settings from pivot on read")

	// Test with glob key - write to pivot via HTTP
	resp, err := ooio.RemotePushWithResponse(pivotCfg, "things/*", Thing{IP: "10.0.0.1", Port: 8080, On: true})
	require.NoError(t, err)
	thingID := resp.Index

	// Read from node - should pull from pivot
	nodeThing, err := ooo.Get[Thing](nodeServer, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, true, nodeThing.Data.On, "node should have pulled thing from pivot on read")
	require.Equal(t, "10.0.0.1", nodeThing.Data.IP)

	// Update on pivot via HTTP
	err = ooio.RemoteSet(pivotCfg, "things/"+thingID, Thing{IP: "10.0.0.1", Port: 8080, On: false})
	require.NoError(t, err)

	// Read from node - should get updated value via sync-on-read
	nodeThing, err = ooo.Get[Thing](nodeServer, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, false, nodeThing.Data.On, "node should have pulled updated thing from pivot on read")

	t.Log("Sync-on-read test passed!")
}

// createEdgeTestServer creates a test server and returns it with its WaitGroup
func createEdgeTestServer(pivotIP string, nodeStorage storage.Database) (*ooo.Server, *sync.WaitGroup) {
	var wg sync.WaitGroup
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	if nodeStorage != nil {
		server.Storage = nodeStorage
	} else {
		server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	}
	server.Router = mux.NewRouter()
	server.Client = &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			Dial:              (&net.Dialer{Timeout: 5 * time.Second}).Dial,
			MaxConnsPerHost:   3000,
			DisableKeepAlives: true,
		},
	}
	server.Audit = func(r *http.Request) bool { return true }

	config := pivot.Config{
		Keys:       []pivot.Key{{Path: "settings"}},
		NodesKey:   "things/*",
		ClusterURL: pivotIP,
	}

	pivot.Setup(server, config)

	// Wrap OnStorageEvent to signal WaitGroup
	originalCallback := server.OnStorageEvent
	server.OnStorageEvent = func(event storage.Event) {
		if originalCallback != nil {
			originalCallback(event)
		}
		if strings.HasPrefix(event.Key, "things/") || event.Key == "settings" {
			wg.Done()
		}
	}

	server.OpenFilter("things/*")
	server.OpenFilter("settings")
	server.Start("localhost:0")
	return server, &wg
}

// createEdgeTestServerNoSync creates a test server without WaitGroup tracking
func createEdgeTestServerNoSync(pivotIP string) *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Client = &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			Dial:              (&net.Dialer{Timeout: 5 * time.Second}).Dial,
			MaxConnsPerHost:   3000,
			DisableKeepAlives: true,
		},
	}
	server.Audit = func(r *http.Request) bool { return true }

	config := pivot.Config{
		Keys:       []pivot.Key{{Path: "settings"}},
		NodesKey:   "things/*",
		ClusterURL: pivotIP,
	}

	pivot.Setup(server, config)

	server.OpenFilter("things/*")
	server.OpenFilter("settings")
	server.Start("localhost:0")
	return server
}
