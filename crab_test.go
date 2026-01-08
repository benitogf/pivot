package pivot_test

import (
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/storage"
	pivot "github.com/benitogf/pivot"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// TestHermitCrab tests the pivot IP change behavior - like a hermit crab changing shells.
// When a node server changes its pivot, all previously synced data should be wiped
// to prevent contamination from the old pivot.
func TestHermitCrab(t *testing.T) {
	// Start two pivot servers (Shell A and Shell B)
	pivotA, wgA := startPivotServer("")
	defer pivotA.Close(os.Interrupt)
	t.Logf("Pivot A (Shell A) started at %s", pivotA.Address)

	pivotB, wgB := startPivotServer("")
	defer pivotB.Close(os.Interrupt)
	t.Logf("Pivot B (Shell B) started at %s", pivotB.Address)

	// Create persistent storage for the node (simulates disk storage that persists across restarts)
	nodeStorage := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	err := nodeStorage.Start(storage.Options{})
	require.NoError(t, err)

	// Start node server connected to Pivot A
	node, wgNode := startNodeServer(pivotA.Address, nodeStorage)
	t.Logf("Node (Hermit Crab) started at %s, connected to Shell A", node.Address)

	// Write data to node - expect 2 sync events (node write + pivot receive)
	wgNode.Add(1)
	wgA.Add(1)
	thingID, err := ooo.Push(node, "things/*", Thing{IP: "192.168.1.1", Port: 0, On: true})
	require.NoError(t, err)
	require.NotEmpty(t, thingID)
	t.Logf("Wrote thing %s to node", thingID)
	wgNode.Wait()
	wgA.Wait()

	// Verify sync to pivot A
	thingOnPivotA, err := ooo.Get[Thing](pivotA, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, true, thingOnPivotA.Data.On)
	t.Log("Verified data synced to Pivot A")

	// Stop node server (storage is closed by server.Close)
	node.Close(os.Interrupt)
	t.Log("Node stopped")

	// Restart storage (simulates process restart with persistent storage)
	err = nodeStorage.Start(storage.Options{})
	require.NoError(t, err)

	// Start node again but connected to Pivot B (changing shells!)
	// Pivot IP change wipes data synchronously during Setup (before server starts)
	// so no events are fired to OnStorageEvent - we don't wait for wipe events
	node, wgNode = startNodeServer(pivotB.Address, nodeStorage)
	t.Logf("Node restarted at %s, now connected to Shell B", node.Address)

	// Read from node - data should be wiped because pivot changed
	things, err := ooo.GetList[Thing](node, "things/*")
	require.NoError(t, err)
	require.Equal(t, 0, len(things), "Data should be wiped after changing pivot")
	t.Log("Verified node data was wiped after changing to Shell B")

	// Write new data to node - expect 2 sync events (node write + pivot receive)
	wgNode.Add(1)
	wgB.Add(1)
	thingID2, err := ooo.Push(node, "things/*", Thing{IP: "192.168.2.2", Port: 0, On: false})
	require.NoError(t, err)
	require.NotEmpty(t, thingID2)
	t.Logf("Wrote thing %s to node (for Shell B)", thingID2)
	wgNode.Wait()
	wgB.Wait()

	// Verify sync to pivot B
	thingOnPivotB, err := ooo.Get[Thing](pivotB, "things/"+thingID2)
	require.NoError(t, err)
	require.Equal(t, false, thingOnPivotB.Data.On)
	t.Log("Verified data synced to Pivot B")

	// Verify Pivot A still has original data
	thingStillOnA, err := ooo.Get[Thing](pivotA, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, true, thingStillOnA.Data.On)
	t.Log("Verified Pivot A still has original data")

	// Verify Pivot A does NOT have Pivot B's data
	_, err = ooo.Get[Thing](pivotA, "things/"+thingID2)
	require.Error(t, err, "Pivot A should not have Pivot B's data")
	t.Log("Verified Pivot A does not have Pivot B's data")

	// Stop node and restart connected to Pivot A again (back to original shell)
	node.Close(os.Interrupt)
	t.Log("Node stopped")

	// Restart storage (simulates process restart with persistent storage)
	err = nodeStorage.Start(storage.Options{})
	require.NoError(t, err)

	// Start node again connected to Pivot A (back to original shell)
	// Pivot IP change wipes data synchronously during Setup (before server starts)
	node, wgNode = startNodeServer(pivotA.Address, nodeStorage)
	defer node.Close(os.Interrupt)
	t.Logf("Node restarted at %s, back to Shell A", node.Address)

	// Read from node - sync-on-read will pull 1 thing from Pivot A (1 event on node)
	wgNode.Add(1)
	nodeThings, err := ooo.GetList[Thing](node, "things/*")
	require.NoError(t, err)
	wgNode.Wait()
	require.Equal(t, 1, len(nodeThings), "Node should have Pivot A's data after sync")
	require.Equal(t, true, nodeThings[0].Data.On)
	t.Log("Verified node has Pivot A's data after returning to Shell A")

	// Verify node does NOT have Pivot B's data
	_, err = ooo.Get[Thing](node, "things/"+thingID2)
	require.Error(t, err, "Node should not have Pivot B's data")
	t.Log("Verified node does not have Pivot B's data")

	t.Log("Hermit crab successfully changed shells!")
}

// startPivotServer creates a pivot server (empty pivotIP = this is the pivot)
// Returns the server and WaitGroup for tracking storage events
func startPivotServer(pivotIP string) (*ooo.Server, *sync.WaitGroup) {
	var wg sync.WaitGroup
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Client = &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 5 * time.Second,
			}).Dial,
			MaxConnsPerHost:   3000,
			DisableKeepAlives: true,
		},
	}
	server.Audit = func(r *http.Request) bool {
		return true
	}

	config := pivot.Config{
		Keys: []pivot.Key{
			{Path: "settings"},
		},
		NodesKey: "things/*",
		PivotIP:  pivotIP,
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

// startNodeServer creates a node server connected to a pivot
// The storage is passed in to simulate persistent storage across restarts
// Returns the server and WaitGroup for tracking storage events
func startNodeServer(pivotIP string, nodeStorage storage.Database) (*ooo.Server, *sync.WaitGroup) {
	var wg sync.WaitGroup
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = nodeStorage
	server.Router = mux.NewRouter()
	server.Client = &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 5 * time.Second,
			}).Dial,
			MaxConnsPerHost:   3000,
			DisableKeepAlives: true,
		},
	}
	server.Audit = func(r *http.Request) bool {
		return true
	}

	config := pivot.Config{
		Keys: []pivot.Key{
			{Path: "settings"},
		},
		NodesKey: "things/*",
		PivotIP:  pivotIP,
	}

	pivot.Setup(server, config)

	// Wrap OnStorageEvent to signal WaitGroup (pivot.Setup sets its own callback)
	pivotCallback := server.OnStorageEvent
	server.OnStorageEvent = func(event storage.Event) {
		if pivotCallback != nil {
			pivotCallback(event)
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
