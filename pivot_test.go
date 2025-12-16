package pivot_test

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/goccy/go-json"

	"github.com/benitogf/auth"
	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/client"
	"github.com/benitogf/ooo/io"
	"github.com/benitogf/pivot"
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

// Register creates a new user and returns the auth token
func Register(t *testing.T, server *ooo.Server) string {
	var c auth.Credentials
	payload := []byte(`{
        "name": "root",
        "account":"root",
        "password": "000",
        "email": "root@root.cc",
        "phone": "555"
    }`)
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

func makeGetNodes(server *ooo.Server) func() []string {
	return func() []string {
		var result []string
		things, err := io.GetList[Thing](server, "things/*")
		if err != nil {
			return result
		}
		for _, thing := range things {
			result = append(result, thing.Data.IP)
		}
		return result
	}
}

func FakeServer(t *testing.T, pivotIP string) *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Pivot = pivotIP
	server.Storage = &ooo.MemoryStorage{}

	// Setup auth store
	authStore := &ooo.MemoryStorage{}
	err := authStore.Start(ooo.StorageOpt{})
	require.NoError(t, err)
	go ooo.WatchStorageNoop(authStore)
	tokenAuth := auth.New(
		auth.NewJwtStore("key", time.Minute*10),
		authStore,
	)

	// Server routing - allow all requests for now (auth routes still registered for token generation)
	server.Audit = func(r *http.Request) bool {
		return true
	}

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
	server.Router = mux.NewRouter()
	getNodes := makeGetNodes(server)
	keys := []string{"things/*", "settings"}
	server.WriteFilter("things/*", ooo.NoopFilter)
	server.AfterWrite("things/*", pivot.SyncWriteFilter(server.Client, pivotIP, getNodes))
	server.ReadFilter("things/*", pivot.SyncReadFilter(server.Client, server.Storage, pivotIP, keys))
	server.DeleteFilter("things/*", pivot.SyncDeleteFilter(server.Client, pivotIP, server.Storage, "things", getNodes))
	server.WriteFilter("settings", ooo.NoopFilter)
	server.AfterWrite("settings", pivot.SyncWriteFilter(server.Client, pivotIP, getNodes))
	server.ReadFilter("settings", pivot.SyncReadFilter(server.Client, server.Storage, pivotIP, keys))
	pivot.Router(server.Router, server.Storage, server.Client, server.Pivot, keys)
	// Register auth routes
	tokenAuth.Routes(server)
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

	// Register and get auth token
	token := Register(t, pivotServer)
	require.NotEqual(t, "", token)
	authHeader := http.Header{}
	authHeader.Set("Authorization", "Bearer "+token)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Expect 4 initial snapshots (1 per subscription)
	wg.Add(4)

	// Subscribe to things on pivot server with auth
	go subscribeWithAuth[Thing](ctx, pivotServer.Address, "/things/*", authHeader, func(data []client.Meta[Thing]) {
		pivotThings = data
		wg.Done()
	})

	// Subscribe to things on node server with auth
	go subscribeWithAuth[Thing](ctx, nodeServer.Address, "/things/*", authHeader, func(data []client.Meta[Thing]) {
		nodeThings = data
		wg.Done()
	})

	// Subscribe to settings on node server with auth
	go subscribeWithAuth[Settings](ctx, nodeServer.Address, "/settings", authHeader, func(data []client.Meta[Settings]) {
		nodeSettings = data
		wg.Done()
	})

	// Subscribe to settings on pivot server with auth
	go subscribeWithAuth[Settings](ctx, pivotServer.Address, "/settings", authHeader, func(data []client.Meta[Settings]) {
		pivotSettings = data
		wg.Done()
	})

	// Wait for initial snapshots
	wg.Wait()

	// Initial state - things list should be empty
	require.Equal(t, 0, len(pivotThings))
	require.Equal(t, 0, len(nodeThings))

	// Create a thing on pivot server - expect 1 message on pivotThings
	wg.Add(1)
	thingID, err := Push(pivotServer, "/things/*", Thing{IP: nodeServer.Address, On: false}, authHeader)
	require.NoError(t, err)
	wg.Wait()

	require.Equal(t, 1, len(pivotThings))
	require.Equal(t, false, pivotThings[0].Data.On)

	// Read from node triggers sync - expect 1 message on nodeThings
	wg.Add(1)
	_, err = Get[Thing](nodeServer, "/things/"+thingID, authHeader)
	require.NoError(t, err)
	wg.Wait()

	require.Equal(t, 1, len(nodeThings))
	require.Equal(t, false, nodeThings[0].Data.On)

	// Modify thing on pivot - expect 2 messages (pivotThings + nodeThings sync)
	wg.Add(2)
	pivotThing, err := Get[Thing](pivotServer, "/things/"+thingID, authHeader)
	require.NoError(t, err)
	pivotThing.Data.On = true
	err = Set(pivotServer, "/things/"+thingID, pivotThing.Data, authHeader)
	require.NoError(t, err)
	wg.Wait()

	// Verify via HTTP
	updatedThing, err := Get[Thing](pivotServer, "/things/"+thingID, authHeader)
	require.NoError(t, err)
	require.Equal(t, true, updatedThing.Data.On)

	// Verify data consistency via HTTP
	nodeThing, err := Get[Thing](nodeServer, "/things/"+thingID, authHeader)
	require.NoError(t, err)
	require.Equal(t, updatedThing.Data, nodeThing.Data)

	// Update settings on node - expect 2 messages (nodeSettings + pivotSettings sync)
	wg.Add(2)
	err = Set(nodeServer, "/settings", Settings{DayEpoch: 1}, authHeader)
	require.NoError(t, err)
	wg.Wait()

	require.Equal(t, 1, nodeSettings[0].Data.DayEpoch)

	// Update settings on pivot - expect 1 message on pivotSettings
	wg.Add(1)
	err = Set(pivotServer, "/settings", Settings{DayEpoch: 9}, authHeader)
	require.NoError(t, err)
	wg.Wait()

	require.Equal(t, 9, pivotSettings[0].Data.DayEpoch)
}
