package pivot_test

import (
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	oio "github.com/benitogf/ooo/io"
	pivot "github.com/benitogf/pivot"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// TestHermitCrab tests the pivot IP change behavior - like a hermit crab changing shells.
// When a node server changes its pivot, all previously synced data should be wiped
// to prevent contamination from the old pivot.
func TestHermitCrab(t *testing.T) {
	// Start two pivot servers (Shell A and Shell B)
	pivotA := startPivotServer(t, "")
	defer pivotA.Close(os.Interrupt)
	t.Logf("Pivot A (Shell A) started at %s", pivotA.Address)

	pivotB := startPivotServer(t, "")
	defer pivotB.Close(os.Interrupt)
	t.Logf("Pivot B (Shell B) started at %s", pivotB.Address)

	// Create persistent storage for the node (simulates disk storage that persists across restarts)
	nodeStorage := &ooo.MemoryStorage{}
	err := nodeStorage.Start(ooo.StorageOpt{})
	require.NoError(t, err)

	// Start node server connected to Pivot A
	node := startNodeServer(t, pivotA.Address, nodeStorage)
	t.Logf("Node (Hermit Crab) started at %s, connected to Shell A", node.Address)

	// Write data to node - should sync to Pivot A
	thingID, err := oio.Push(node, "things/*", Thing{IP: "192.168.1.1", On: true})
	require.NoError(t, err)
	require.NotEmpty(t, thingID)
	t.Logf("Wrote thing %s to node", thingID)

	// Trigger sync to pivot A by reading from pivot
	time.Sleep(50 * time.Millisecond)
	thingOnPivotA, err := oio.Get[Thing](pivotA, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, true, thingOnPivotA.Data.On)
	t.Log("Verified data synced to Pivot A")

	// Stop node server (but storage persists)
	node.Close(os.Interrupt)
	t.Log("Node stopped")

	// Start node again but connected to Pivot B (changing shells!)
	node = startNodeServer(t, pivotB.Address, nodeStorage)
	t.Logf("Node restarted at %s, now connected to Shell B", node.Address)

	// Read from node - data should be wiped because pivot changed
	things, err := oio.GetList[Thing](node, "things/*")
	require.NoError(t, err)
	require.Equal(t, 0, len(things), "Data should be wiped after changing pivot")
	t.Log("Verified node data was wiped after changing to Shell B")

	// Write new data to node - should sync to Pivot B
	thingID2, err := oio.Push(node, "things/*", Thing{IP: "192.168.2.2", On: false})
	require.NoError(t, err)
	require.NotEmpty(t, thingID2)
	t.Logf("Wrote thing %s to node (for Shell B)", thingID2)

	// Trigger sync to pivot B
	time.Sleep(50 * time.Millisecond)
	thingOnPivotB, err := oio.Get[Thing](pivotB, "things/"+thingID2)
	require.NoError(t, err)
	require.Equal(t, false, thingOnPivotB.Data.On)
	t.Log("Verified data synced to Pivot B")

	// Verify Pivot A still has original data
	thingStillOnA, err := oio.Get[Thing](pivotA, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, true, thingStillOnA.Data.On)
	t.Log("Verified Pivot A still has original data")

	// Verify Pivot A does NOT have Pivot B's data
	_, err = oio.Get[Thing](pivotA, "things/"+thingID2)
	require.Error(t, err, "Pivot A should not have Pivot B's data")
	t.Log("Verified Pivot A does not have Pivot B's data")

	// Stop node and restart connected to Pivot A again (back to original shell)
	node.Close(os.Interrupt)
	t.Log("Node stopped")

	node = startNodeServer(t, pivotA.Address, nodeStorage)
	defer node.Close(os.Interrupt)
	t.Logf("Node restarted at %s, back to Shell A", node.Address)

	// Read from node - should have Pivot A's data after sync
	// First trigger a sync by reading
	nodeThings, err := oio.GetList[Thing](node, "things/*")
	require.NoError(t, err)
	require.Equal(t, 1, len(nodeThings), "Node should have Pivot A's data after sync")
	require.Equal(t, true, nodeThings[0].Data.On)
	t.Log("Verified node has Pivot A's data after returning to Shell A")

	// Verify node does NOT have Pivot B's data
	_, err = oio.Get[Thing](node, "things/"+thingID2)
	require.Error(t, err, "Node should not have Pivot B's data")
	t.Log("Verified node does not have Pivot B's data")

	t.Log("Hermit crab successfully changed shells!")
}

// startPivotServer creates a pivot server (empty pivotIP = this is the pivot)
func startPivotServer(t *testing.T, pivotIP string) *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	storage := &ooo.MemoryStorage{}
	server.Storage = storage
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

	// Start storage before Setup
	err := storage.Start(ooo.StorageOpt{})
	require.NoError(t, err)

	config := pivot.Config{
		Keys: []pivot.Key{
			{Path: "settings"},
		},
		NodesKey: "things/*",
		PivotIP:  pivotIP,
	}

	result := pivot.Setup(server, config)
	server.BeforeRead = result.BeforeRead

	server.WriteFilter("things/*", ooo.NoopFilter)
	server.WriteFilter("settings", ooo.NoopFilter)
	server.ReadFilter("things/*", ooo.NoopFilter)
	server.ReadFilter("settings", ooo.NoopFilter)

	server.Start("localhost:0")
	return server
}

// startNodeServer creates a node server connected to a pivot
// The storage is passed in to simulate persistent storage across restarts
func startNodeServer(t *testing.T, pivotIP string, storage *ooo.MemoryStorage) *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage
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

	result := pivot.Setup(server, config)
	server.BeforeRead = result.BeforeRead

	server.WriteFilter("things/*", ooo.NoopFilter)
	server.WriteFilter("settings", ooo.NoopFilter)
	server.ReadFilter("things/*", ooo.NoopFilter)
	server.ReadFilter("settings", ooo.NoopFilter)

	server.Start("localhost:0")
	return server
}
