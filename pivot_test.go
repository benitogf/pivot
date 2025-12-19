package pivot_test

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/benitogf/auth"
	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/client"
	oio "github.com/benitogf/ooo/io"
	"github.com/benitogf/pivot"
	"github.com/goccy/go-json"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

type Thing struct {
	IP string `json:"ip"`
	On bool   `json:"on"`
}

type Settings struct {
	DayEpoch int `json:"startOfDay"`
}

func RegisterUser(t *testing.T, server *ooo.Server, account string) string {
	var c auth.Credentials
	payload := []byte(fmt.Sprintf(`{
        "name": "%s",
        "account":"%s",
        "password": "000",
        "email": "%s@test.cc",
        "phone": "555"
    }`, account, account, account))
	req, err := http.NewRequest("POST", "/register", bytes.NewBuffer(payload))
	require.NoError(t, err)
	w := httptest.NewRecorder()
	server.Router.ServeHTTP(w, req)
	response := w.Result()
	require.Equal(t, http.StatusOK, response.StatusCode)
	dec := json.NewDecoder(response.Body)
	err = dec.Decode(&c)
	require.NoError(t, err)
	require.NotEmpty(t, c.Token)
	return c.Token
}

func Authorize(t *testing.T, server *ooo.Server, account string) string {
	var c auth.Credentials
	payload := []byte(fmt.Sprintf(`{
        "account":"%s",
        "password": "000"
    }`, account))
	req, err := http.NewRequest("POST", "/authorize", bytes.NewBuffer(payload))
	require.NoError(t, err)
	w := httptest.NewRecorder()
	server.Router.ServeHTTP(w, req)
	response := w.Result()
	require.Equal(t, http.StatusOK, response.StatusCode)
	dec := json.NewDecoder(response.Body)
	err = dec.Decode(&c)
	require.NoError(t, err)
	require.NotEmpty(t, c.Token)
	return c.Token
}

func makeGetNodes(server *ooo.Server) func() []string {
	return func() []string {
		var result []string
		things, err := oio.GetList[Thing](server, "things/*")
		if err != nil {
			return result
		}
		for _, thing := range things {
			result = append(result, thing.Data.IP)
		}
		return result
	}
}

// FakeServer creates a server using storage-level synchronization via pivot.Setup.
// This approach uses BeforeRead for sync-on-read and storage.Watch() for write sync.
func FakeServer(t *testing.T, pivotIP string) *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Pivot = pivotIP
	server.Storage = &ooo.MemoryStorage{}
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

	// Create auth store (not started yet)
	authStorage := &ooo.MemoryStorage{}
	_auth := auth.New(
		auth.NewJwtStore("key", time.Minute*10),
		authStorage,
	)

	// Define sync keys with their databases
	keys := []pivot.Key{
		{Path: "users/*", Database: authStorage},
		{Path: "things/*", Database: server.Storage},
		{Path: "settings", Database: server.Storage},
	}

	// Setup pivot - sets server.DbOpt, returns BeforeRead callback for external storages
	beforeRead := pivot.Setup(server, keys, makeGetNodes(server))

	// Start external storages with the BeforeRead callback
	err := authStorage.Start(ooo.StorageOpt{BeforeRead: beforeRead})
	require.NoError(t, err)

	// Watch external storage for write sync
	syncCallback := pivot.StorageSync(server.Client, pivotIP, keys, makeGetNodes(server))
	go func() {
		for event := range authStorage.Watch() {
			syncCallback(event)
		}
	}()

	// WriteFilter and ReadFilter needed to allow reads/writes
	server.WriteFilter("things/*", ooo.NoopFilter)
	server.WriteFilter("settings", ooo.NoopFilter)
	server.ReadFilter("things/*", ooo.NoopFilter)
	server.ReadFilter("settings", ooo.NoopFilter)

	_auth.Routes(server)

	server.Start("localhost:0")
	return server
}

// Global state variables for test
var (
	wg            sync.WaitGroup
	pivotThings   []client.Meta[Thing]
	nodeThings    []client.Meta[Thing]
	pivotSettings []client.Meta[Settings]
	nodeSettings  []client.Meta[Settings]
)

func TestBasicPivotSync(t *testing.T) {
	pivotServer := FakeServer(t, "")
	defer pivotServer.Close(os.Interrupt)

	nodeServer := FakeServer(t, pivotServer.Address)
	defer nodeServer.Close(os.Interrupt)

	// Register first user and get auth token (used for initial setup)
	token := RegisterUser(t, pivotServer, "root")
	require.NotEqual(t, "", token)
	authHeader := http.Header{}
	authHeader.Set("Authorization", "Bearer "+token)

	// Authorize on node server - the user should now be synced
	token = Authorize(t, nodeServer, "root")
	require.NotEqual(t, "", token)
	authHeader = http.Header{}
	authHeader.Set("Authorization", "Bearer "+token)

	ctx := t.Context()
	defer ctx.Done()
	// Expect 4 initial snapshots (1 per subscription)
	wg.Add(4)

	// Subscribe to things on pivot server with auth
	go client.Subscribe(ctx, "ws", pivotServer.Address, "things/*", authHeader, func(data []client.Meta[Thing]) {
		pivotThings = data
		wg.Done()
	})

	// Subscribe to things on node server with auth
	go client.Subscribe(ctx, "ws", nodeServer.Address, "things/*", authHeader, func(data []client.Meta[Thing]) {
		nodeThings = data
		wg.Done()
	})

	// Subscribe to settings on node server with auth
	go client.Subscribe(ctx, "ws", nodeServer.Address, "settings", authHeader, func(data []client.Meta[Settings]) {
		nodeSettings = data
		wg.Done()
	})

	// Subscribe to settings on pivot server with auth
	go client.Subscribe(ctx, "ws", pivotServer.Address, "settings", authHeader, func(data []client.Meta[Settings]) {
		pivotSettings = data
		wg.Done()
	})

	// Wait for initial snapshots
	wg.Wait()

	// Initial state - things list should be empty
	require.Equal(t, 0, len(pivotThings))
	require.Equal(t, 0, len(nodeThings))

	// Create a thing on pivot server with node's address - expect 1 message on pivotThings
	// This registers the node so that future storage events will trigger sync to it
	wg.Add(1)
	thingID, err := oio.Push(pivotServer, "things/*", Thing{IP: nodeServer.Address, On: false})
	require.NoError(t, err)
	require.NotEmpty(t, thingID)
	wg.Wait()

	require.Equal(t, 1, len(pivotThings))
	require.Equal(t, false, pivotThings[0].Data.On)

	// Read from node triggers sync - expect 1 message on nodeThings
	wg.Add(1)
	_, err = oio.Get[Thing](nodeServer, "things/"+thingID)
	require.NoError(t, err)
	wg.Wait()

	require.Equal(t, 1, len(nodeThings))
	require.Equal(t, false, nodeThings[0].Data.On)

	// Now register a second user on pivot - this will trigger sync to node via StorageSync
	// because getNodes() now returns the node's address from things
	token2 := RegisterUser(t, pivotServer, "testuser")
	require.NotEqual(t, "", token2)

	// Authorize on node server - the user should now be synced
	token = Authorize(t, nodeServer, "testuser")
	require.NotEqual(t, "", token)
	authHeader = http.Header{}
	authHeader.Set("Authorization", "Bearer "+token)

	// Modify thing on pivot - expect 2 messages (pivotThings + nodeThings sync)
	wg.Add(2)
	pivotThing, err := oio.Get[Thing](pivotServer, "things/"+thingID)
	require.NoError(t, err)
	pivotThing.Data.On = true
	err = oio.Set(pivotServer, "things/"+thingID, pivotThing.Data)
	require.NoError(t, err)
	wg.Wait()

	// Verify via HTTP
	updatedThing, err := oio.Get[Thing](pivotServer, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, true, updatedThing.Data.On)

	// Verify data consistency via HTTP
	nodeThing, err := oio.Get[Thing](nodeServer, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, updatedThing.Data, nodeThing.Data)

	// Update settings on node - expect 2 messages (nodeSettings + pivotSettings sync)
	wg.Add(2)
	err = oio.Set(nodeServer, "settings", Settings{DayEpoch: 1})
	require.NoError(t, err)
	wg.Wait()

	require.Equal(t, 1, nodeSettings[0].Data.DayEpoch)

	// Update settings on pivot - expect 1 message on pivotSettings
	wg.Add(1)
	err = oio.Set(pivotServer, "settings", Settings{DayEpoch: 9})
	require.NoError(t, err)
	wg.Wait()

	require.Equal(t, 9, pivotSettings[0].Data.DayEpoch)
}
