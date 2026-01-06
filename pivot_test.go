package pivot_test

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benitogf/auth"
	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/client"
	"github.com/benitogf/ooo/storage"
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
	// Use real HTTP request to ensure sync callbacks work properly
	resp, err := server.Client.Post("http://"+server.Address+"/register", "application/json", bytes.NewBuffer(payload))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	dec := json.NewDecoder(resp.Body)
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
	// Use real HTTP request to ensure sync-on-read works properly
	resp, err := server.Client.Post("http://"+server.Address+"/authorize", "application/json", bytes.NewBuffer(payload))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&c)
	require.NoError(t, err)
	require.NotEmpty(t, c.Token)
	return c.Token
}

// FakeServer creates a server using storage-level synchronization via pivot.Setup.
// Optional afterAuthWrite is called after each auth storage write (for test synchronization).
func FakeServer(t *testing.T, pivotIP string, afterAuthWrite func(key string)) *ooo.Server {
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

	// Create auth store
	authStorage := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	_auth := auth.New(
		auth.NewJwtStore("key", time.Minute*10),
		authStorage,
	)

	// Configure pivot synchronization
	config := pivot.Config{
		Keys: []pivot.Key{
			{Path: "users/*", Database: authStorage},
			{Path: "settings"},
		},
		NodesKey: "things/*",
		PivotIP:  pivotIP,
	}

	// Setup pivot - modifies server (routes, OnStorageEvent, BeforeRead)
	pivot.Setup(server, config)

	// Use Attach for simplified external storage setup
	err := pivot.GetInstance(server).Attach(authStorage, storage.Options{AfterWrite: afterAuthWrite})
	require.NoError(t, err)

	server.OpenFilter("things/*")
	server.OpenFilter("settings")
	server.OpenFilter("users/*")

	_auth.Routes(server)

	server.Start("localhost:0")
	return server
}

func TestBasicPivotSync(t *testing.T) {
	// WaitGroup for websocket events
	var wsWg sync.WaitGroup
	// WaitGroup for auth storage writes
	var authWg sync.WaitGroup

	// State variables for subscriptions (protected by mutex for concurrent writes)
	var pivotThings []client.Meta[Thing]
	var nodeThings []client.Meta[Thing]
	var pivotSettings []client.Meta[Settings]
	var nodeSettings []client.Meta[Settings]
	var mu sync.Mutex

	// Callback for auth storage writes - signals WaitGroup when expected
	// Use atomic counter to safely handle writes from sync
	var pendingWrites int32
	afterAuthWrite := func(key string) {
		if atomic.AddInt32(&pendingWrites, -1) >= 0 {
			authWg.Done()
		}
	}

	pivotServer := FakeServer(t, "", afterAuthWrite)
	defer pivotServer.Close(os.Interrupt)

	nodeServer := FakeServer(t, pivotServer.Address, afterAuthWrite)
	defer nodeServer.Close(os.Interrupt)

	// Register first user on pivot - expect 1 auth write
	atomic.StoreInt32(&pendingWrites, 1)
	authWg.Add(1)
	token := RegisterUser(t, pivotServer, "root")
	require.NotEqual(t, "", token)
	authWg.Wait()
	authHeader := http.Header{}
	authHeader.Set("Authorization", "Bearer "+token)

	// Authorize on node (triggers sync-on-read for user data) - expect 1 auth write from sync
	atomic.StoreInt32(&pendingWrites, 1)
	authWg.Add(1)
	token = Authorize(t, nodeServer, "root")
	require.NotEqual(t, "", token)
	authWg.Wait()
	authHeader = http.Header{}
	authHeader.Set("Authorization", "Bearer "+token)

	ctx := t.Context()

	// Expect 4 initial snapshots from subscriptions
	wsWg.Add(4)

	// Subscribe to things on pivot
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: pivotServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "things/*", client.SubscribeListEvents[Thing]{
		OnMessage: func(data []client.Meta[Thing]) {
			mu.Lock()
			pivotThings = data
			mu.Unlock()
			wsWg.Done()
		},
	})

	// Subscribe to things on node
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: nodeServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "things/*", client.SubscribeListEvents[Thing]{
		OnMessage: func(data []client.Meta[Thing]) {
			mu.Lock()
			nodeThings = data
			mu.Unlock()
			wsWg.Done()
		},
	})

	// Subscribe to settings on node
	go client.Subscribe(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: nodeServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "settings", client.SubscribeEvents[Settings]{
		OnMessage: func(data client.Meta[Settings]) {
			mu.Lock()
			nodeSettings = []client.Meta[Settings]{data}
			mu.Unlock()
			wsWg.Done()
		},
	})

	// Subscribe to settings on pivot
	go client.Subscribe(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: pivotServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "settings", client.SubscribeEvents[Settings]{
		OnMessage: func(data client.Meta[Settings]) {
			mu.Lock()
			pivotSettings = []client.Meta[Settings]{data}
			mu.Unlock()
			wsWg.Done()
		},
	})

	// Wait for initial snapshots
	wsWg.Wait()

	// Verify initial state
	mu.Lock()
	require.Equal(t, 0, len(pivotThings))
	require.Equal(t, 0, len(nodeThings))
	mu.Unlock()

	// Push thing to pivot - expect 1 ws event on pivotThings
	wsWg.Add(1)
	thingID, err := ooo.Push(pivotServer, "things/*", Thing{IP: nodeServer.Address, On: false})
	require.NoError(t, err)
	require.NotEmpty(t, thingID)
	wsWg.Wait()

	mu.Lock()
	require.Equal(t, 1, len(pivotThings))
	require.Equal(t, false, pivotThings[0].Data.On)
	mu.Unlock()

	// Read from node triggers sync - expect 1 ws event on nodeThings
	wsWg.Add(1)
	_, err = ooo.Get[Thing](nodeServer, "things/"+thingID)
	require.NoError(t, err)
	wsWg.Wait()

	mu.Lock()
	require.Equal(t, 1, len(nodeThings))
	require.Equal(t, false, nodeThings[0].Data.On)
	mu.Unlock()

	// Register second user on pivot - expect 2 auth writes:
	// 1. Pivot writes user to its auth storage
	// 2. SyncCallback triggers node sync, node writes user to its auth storage
	atomic.StoreInt32(&pendingWrites, 2)
	authWg.Add(2)
	token2 := RegisterUser(t, pivotServer, "testuser")
	require.NotEqual(t, "", token2)
	authWg.Wait()

	// Authorize testuser on node - user already synced, no additional auth write expected
	token = Authorize(t, nodeServer, "testuser")
	require.NotEqual(t, "", token)
	authHeader = http.Header{}
	authHeader.Set("Authorization", "Bearer "+token)

	// Modify thing on pivot - expect 2 ws events (pivotThings + nodeThings)
	wsWg.Add(2)
	pivotThing, err := ooo.Get[Thing](pivotServer, "things/"+thingID)
	require.NoError(t, err)
	pivotThing.Data.On = true
	err = ooo.Set(pivotServer, "things/"+thingID, pivotThing.Data)
	require.NoError(t, err)
	wsWg.Wait()

	// Verify via storage
	updatedThing, err := ooo.Get[Thing](pivotServer, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, true, updatedThing.Data.On)

	nodeThing, err := ooo.Get[Thing](nodeServer, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, updatedThing.Data, nodeThing.Data)

	// Set settings on node - expect 2 ws events (nodeSettings + pivotSettings)
	wsWg.Add(2)
	err = ooo.Set(nodeServer, "settings", Settings{DayEpoch: 1})
	require.NoError(t, err)
	wsWg.Wait()

	mu.Lock()
	require.Equal(t, 1, nodeSettings[0].Data.DayEpoch)
	require.Equal(t, 1, pivotSettings[0].Data.DayEpoch)
	mu.Unlock()

	// Set settings on pivot - expect 2 ws events (pivotSettings + nodeSettings)
	wsWg.Add(2)
	err = ooo.Set(pivotServer, "settings", Settings{DayEpoch: 9})
	require.NoError(t, err)
	wsWg.Wait()

	// Verify final state via storage
	pivotSettingsObj, err := pivotServer.Storage.Get("settings")
	require.NoError(t, err)
	var pivotSettingsData Settings
	err = json.Unmarshal(pivotSettingsObj.Data, &pivotSettingsData)
	require.NoError(t, err)
	require.Equal(t, 9, pivotSettingsData.DayEpoch)

	nodeSettingsObj, err := nodeServer.Storage.Get("settings")
	require.NoError(t, err)
	var nodeSettingsData Settings
	err = json.Unmarshal(nodeSettingsObj.Data, &nodeSettingsData)
	require.NoError(t, err)
	require.Equal(t, 9, nodeSettingsData.DayEpoch)
}
