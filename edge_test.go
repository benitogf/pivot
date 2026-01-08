package pivot_test

import (
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// TestNodeToPivotSync verifies that node writes sync to pivot correctly.
func TestNodeToPivotSync(t *testing.T) {
	syncEvents := newSyncCounter()

	pivotServer := createEdgeTestServer("", syncEvents)
	defer pivotServer.Close(os.Interrupt)

	nodeServer := createEdgeTestServer(pivotServer.Address, syncEvents)
	defer nodeServer.Close(os.Interrupt)

	// Use node server address so pivot can find the node for sync notifications
	nodeIP, nodePort, _ := net.SplitHostPort(nodeServer.Address)
	nodePortInt, _ := strconv.Atoi(nodePort)

	syncEvents.expect(1)
	thingID, err := ooo.Push(nodeServer, "things/*", Thing{IP: nodeIP, Port: nodePortInt, On: true})
	require.NoError(t, err)
	syncEvents.wait()

	pivotThing, err := ooo.Get[Thing](pivotServer, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, true, pivotThing.Data.On)
	require.Equal(t, nodeIP, pivotThing.Data.IP)
	require.Equal(t, nodePortInt, pivotThing.Data.Port)

	syncEvents.expect(2) // Node storage event + pivot storage event after sync
	_, err = ooo.Push(nodeServer, "things/*", Thing{IP: "192.168.1.1", Port: 0, On: false})
	require.NoError(t, err)
	syncEvents.wait()

	pivotThings, err := ooo.GetList[Thing](pivotServer, "things/*")
	require.NoError(t, err)
	require.Equal(t, 2, len(pivotThings))

	t.Log("Node to pivot sync test passed!")
}

// TestSingleKeySync verifies sync works for non-glob keys.
func TestSingleKeySync(t *testing.T) {
	syncEvents := newSyncCounter()

	pivotServer := createEdgeTestServer("", syncEvents)
	defer pivotServer.Close(os.Interrupt)

	nodeServer := createEdgeTestServer(pivotServer.Address, syncEvents)
	defer nodeServer.Close(os.Interrupt)

	syncEvents.expect(1)
	err := ooo.Set(nodeServer, "settings", Settings{DayEpoch: 100})
	require.NoError(t, err)
	syncEvents.wait()

	pivotSettings, err := ooo.Get[Settings](pivotServer, "settings")
	require.NoError(t, err)
	require.Equal(t, 100, pivotSettings.Data.DayEpoch)

	t.Log("Single key sync test passed!")
}

// TestEmptyStorageSync verifies sync works with empty storage.
func TestEmptyStorageSync(t *testing.T) {
	syncEvents := newSyncCounter()

	pivotServer := createEdgeTestServer("", syncEvents)
	defer pivotServer.Close(os.Interrupt)

	nodeServer := createEdgeTestServer(pivotServer.Address, syncEvents)
	defer nodeServer.Close(os.Interrupt)

	pivotThings, err := ooo.GetList[Thing](pivotServer, "things/*")
	require.NoError(t, err)
	require.Equal(t, 0, len(pivotThings))

	nodeThings, err := ooo.GetList[Thing](nodeServer, "things/*")
	require.NoError(t, err)
	require.Equal(t, 0, len(nodeThings))

	t.Log("Empty storage sync test passed!")
}

func createEdgeTestServer(pivotIP string, syncEvents *syncCounter) *ooo.Server {
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
		Keys:     []pivot.Key{{Path: "settings"}},
		NodesKey: "things/*",
		PivotIP:  pivotIP,
	}

	pivot.Setup(server, config)

	if syncEvents != nil {
		originalCallback := server.OnStorageEvent
		server.OnStorageEvent = func(event storage.Event) {
			if originalCallback != nil {
				originalCallback(event)
			}
			syncEvents.signal()
		}
	}

	server.OpenFilter("things/*")
	server.OpenFilter("settings")
	server.Start("localhost:0")
	return server
}
