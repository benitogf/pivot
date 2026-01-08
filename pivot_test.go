package pivot_test

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benitogf/auth"
	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/client"
	ooio "github.com/benitogf/ooo/io"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/goccy/go-json"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

type Thing struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
	On   bool   `json:"on"`
}

// parseAddress splits "host:port" into IP and Port components
func parseAddress(addr string) (string, int) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, 0
	}
	port, _ := strconv.Atoi(portStr)
	return host, port
}

// newThing creates a Thing from an address string (e.g., "127.0.0.1:8080")
func newThing(addr string, on bool) Thing {
	ip, port := parseAddress(addr)
	return Thing{IP: ip, Port: port, On: on}
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
	// NodesKey is automatically added to Keys by buildKeys if not present
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

func TestBasicPivotSyncLocal(t *testing.T) {
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
	thingID, err := ooo.Push(pivotServer, "things/*", newThing(nodeServer.Address, false))
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

	// Verify final state via ooo.Get
	pivotSettingsObj, err := ooo.Get[Settings](pivotServer, "settings")
	require.NoError(t, err)
	require.Equal(t, 9, pivotSettingsObj.Data.DayEpoch)

	nodeSettingsObj, err := ooo.Get[Settings](nodeServer, "settings")
	require.NoError(t, err)
	require.Equal(t, 9, nodeSettingsObj.Data.DayEpoch)

	// Test delete sync from pivot to node
	// Push a second thing to pivot
	wsWg.Add(2) // pivotThings + nodeThings
	thingID2, err := ooo.Push(pivotServer, "things/*", Thing{IP: "10.0.0.1", Port: 0, On: true})
	require.NoError(t, err)
	require.NotEmpty(t, thingID2)
	wsWg.Wait()

	mu.Lock()
	require.Equal(t, 2, len(pivotThings), "pivot should have 2 things")
	require.Equal(t, 2, len(nodeThings), "node should have 2 things")
	mu.Unlock()

	// Delete from pivot - expect 2 ws events (pivotThings + nodeThings after sync)
	// With /synchronize/pivot endpoint, node pulls from pivot and sees the delete
	// The delete is tracked to prevent StorageSync from re-adding the item
	wsWg.Add(2)
	err = ooo.Delete(pivotServer, "things/"+thingID2)
	require.NoError(t, err)
	wsWg.Wait()

	// Verify pivot - item should be deleted
	_, err = ooo.Get[Thing](pivotServer, "things/"+thingID2)
	require.Error(t, err, "thingID2 should be deleted from pivot")

	// Verify node - delete should have propagated via pull-only sync
	_, err = ooo.Get[Thing](nodeServer, "things/"+thingID2)
	require.Error(t, err, "thingID2 should be deleted from node after sync")

	// Verify subscription state
	mu.Lock()
	require.Equal(t, 1, len(pivotThings), "pivot should have 1 thing after pivot-to-node delete")
	require.Equal(t, 1, len(nodeThings), "node should have 1 thing after pivot-to-node delete")
	require.Equal(t, thingID, pivotThings[0].Index)
	require.Equal(t, thingID, nodeThings[0].Index)
	mu.Unlock()

	// Test delete sync from node to pivot
	// Push a third thing to node
	wsWg.Add(2) // pivotThings + nodeThings
	thingID3, err := ooo.Push(nodeServer, "things/*", Thing{IP: "172.16.0.1", Port: 0, On: false})
	require.NoError(t, err)
	require.NotEmpty(t, thingID3)
	wsWg.Wait()

	mu.Lock()
	require.Equal(t, 2, len(pivotThings), "pivot should have 2 things after node push")
	require.Equal(t, 2, len(nodeThings), "node should have 2 things after node push")
	mu.Unlock()

	// Delete from node - expect 2 ws events (nodeThings + pivotThings after sync)
	// Node's StorageSync triggers bidirectional sync which sends delete to pivot
	wsWg.Add(2)
	err = ooo.Delete(nodeServer, "things/"+thingID3)
	require.NoError(t, err)
	wsWg.Wait()

	// Verify node - item should be deleted
	_, err = ooo.Get[Thing](nodeServer, "things/"+thingID3)
	require.Error(t, err, "thingID3 should be deleted from node")

	// Verify pivot - delete should have propagated via bidirectional sync
	_, err = ooo.Get[Thing](pivotServer, "things/"+thingID3)
	require.Error(t, err, "thingID3 should be deleted from pivot after sync")

	// Verify subscription state
	mu.Lock()
	require.Equal(t, 1, len(pivotThings), "pivot should have 1 thing after node-to-pivot delete")
	require.Equal(t, 1, len(nodeThings), "node should have 1 thing after node-to-pivot delete")
	require.Equal(t, thingID, pivotThings[0].Index)
	require.Equal(t, thingID, nodeThings[0].Index)
	mu.Unlock()

	// Test update sync from node to pivot (modify existing thing on node)
	// Keep the node server address so pivot can still find the node for sync
	wsWg.Add(2) // nodeThings + pivotThings
	nodeThing, err = ooo.Get[Thing](nodeServer, "things/"+thingID)
	require.NoError(t, err)
	nodeThing.Data.On = false // Only change the On field, keep IP as node server address
	err = ooo.Set(nodeServer, "things/"+thingID, nodeThing.Data)
	require.NoError(t, err)
	wsWg.Wait()

	// Verify update synced to pivot
	nodeIP, nodePort := parseAddress(nodeServer.Address)
	pivotThing, err = ooo.Get[Thing](pivotServer, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, nodeIP, pivotThing.Data.IP)
	require.Equal(t, nodePort, pivotThing.Data.Port)
	require.Equal(t, false, pivotThing.Data.On)

	mu.Lock()
	require.Equal(t, nodeIP, nodeThings[0].Data.IP)
	require.Equal(t, nodeIP, pivotThings[0].Data.IP)
	mu.Unlock()

	// Test delete settings from node to pivot (single key delete)
	// Expect 2 ws events: nodeSettings (empty) + pivotSettings (empty after sync)
	wsWg.Add(2)
	err = ooo.Delete(nodeServer, "settings")
	require.NoError(t, err)
	wsWg.Wait()

	// Verify settings deleted on both sides
	_, err = ooo.Get[Settings](nodeServer, "settings")
	require.Error(t, err, "settings should be deleted from node")
	_, err = ooo.Get[Settings](pivotServer, "settings")
	require.Error(t, err, "settings should be deleted from pivot after sync")

	// Test set settings on pivot after delete - expect 2 ws events
	wsWg.Add(2)
	err = ooo.Set(pivotServer, "settings", Settings{DayEpoch: 42})
	require.NoError(t, err)
	wsWg.Wait()

	// Verify settings synced to node
	nodeSettingsObj, err = ooo.Get[Settings](nodeServer, "settings")
	require.NoError(t, err)
	require.Equal(t, 42, nodeSettingsObj.Data.DayEpoch)

	// Test delete settings from pivot to node (single key delete)
	// Expect 2 ws events: pivotSettings (empty) + nodeSettings (empty after sync)
	wsWg.Add(2)
	err = ooo.Delete(pivotServer, "settings")
	require.NoError(t, err)
	wsWg.Wait()

	// Verify settings deleted on both sides
	_, err = ooo.Get[Settings](pivotServer, "settings")
	require.Error(t, err, "settings should be deleted from pivot")
	_, err = ooo.Get[Settings](nodeServer, "settings")
	require.Error(t, err, "settings should be deleted from node after sync")
}

func TestBasicPivotSyncRemote(t *testing.T) {
	var wsWg sync.WaitGroup
	var authWg sync.WaitGroup
	var pivotThings, nodeThings []client.Meta[Thing]
	var pivotSettings, nodeSettings []client.Meta[Settings]
	var mu sync.Mutex
	_ = pivotThings
	_ = nodeThings
	_ = pivotSettings
	_ = nodeSettings
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

	pivotCfg := ooio.RemoteConfig{Client: &http.Client{Timeout: 5 * time.Second}, Host: pivotServer.Address}
	nodeCfg := ooio.RemoteConfig{Client: &http.Client{Timeout: 5 * time.Second}, Host: nodeServer.Address}

	atomic.StoreInt32(&pendingWrites, 1)
	authWg.Add(1)
	token := RegisterUser(t, pivotServer, "root")
	require.NotEqual(t, "", token)
	authWg.Wait()

	atomic.StoreInt32(&pendingWrites, 1)
	authWg.Add(1)
	token = Authorize(t, nodeServer, "root")
	require.NotEqual(t, "", token)
	authWg.Wait()

	authHeader := http.Header{}
	authHeader.Set("Authorization", "Bearer "+token)
	pivotCfg.Header = authHeader
	nodeCfg.Header = authHeader

	ctx := t.Context()
	wsWg.Add(4)

	go client.SubscribeList(client.SubscribeConfig{Ctx: ctx, Server: client.Server{Protocol: "ws", Host: pivotServer.Address}, Header: authHeader, Silence: true}, "things/*", client.SubscribeListEvents[Thing]{OnMessage: func(data []client.Meta[Thing]) { mu.Lock(); pivotThings = data; mu.Unlock(); wsWg.Done() }})
	go client.SubscribeList(client.SubscribeConfig{Ctx: ctx, Server: client.Server{Protocol: "ws", Host: nodeServer.Address}, Header: authHeader, Silence: true}, "things/*", client.SubscribeListEvents[Thing]{OnMessage: func(data []client.Meta[Thing]) { mu.Lock(); nodeThings = data; mu.Unlock(); wsWg.Done() }})
	go client.Subscribe(client.SubscribeConfig{Ctx: ctx, Server: client.Server{Protocol: "ws", Host: nodeServer.Address}, Header: authHeader, Silence: true}, "settings", client.SubscribeEvents[Settings]{OnMessage: func(data client.Meta[Settings]) {
		mu.Lock()
		nodeSettings = []client.Meta[Settings]{data}
		mu.Unlock()
		wsWg.Done()
	}})
	go client.Subscribe(client.SubscribeConfig{Ctx: ctx, Server: client.Server{Protocol: "ws", Host: pivotServer.Address}, Header: authHeader, Silence: true}, "settings", client.SubscribeEvents[Settings]{OnMessage: func(data client.Meta[Settings]) {
		mu.Lock()
		pivotSettings = []client.Meta[Settings]{data}
		mu.Unlock()
		wsWg.Done()
	}})

	wsWg.Wait()

	nodeIP, nodePort := parseAddress(nodeServer.Address)

	// Push thing to pivot via remote - expect 2 ws events (pivot write + node sync via TriggerNodeSync)
	wsWg.Add(2)
	thingIDResp, err := ooio.RemotePushWithResponse(pivotCfg, "things/*", Thing{IP: nodeIP, Port: nodePort, On: false})
	require.NoError(t, err)
	thingID := thingIDResp.Index
	require.NotEmpty(t, thingID, "RemotePushWithResponse should return a non-empty index")
	wsWg.Wait()

	mu.Lock()
	require.Equal(t, 1, len(pivotThings), "pivot should have 1 thing after push")
	require.Equal(t, 1, len(nodeThings), "node should have 1 thing after sync")
	mu.Unlock()

	// Read from node via remote - should have the thing after sync
	nodeThing, err := ooio.RemoteGet[Thing](nodeCfg, "things/"+thingID)
	require.NoError(t, err)
	require.Equal(t, false, nodeThing.Data.On)

	// Modify thing on pivot
	wsWg.Add(2)
	err = ooio.RemoteSet(pivotCfg, "things/"+thingID, Thing{IP: nodeIP, Port: nodePort, On: true})
	require.NoError(t, err)
	wsWg.Wait()

	// Set settings on node
	wsWg.Add(2)
	err = ooio.RemoteSet(nodeCfg, "settings", Settings{DayEpoch: 1})
	require.NoError(t, err)
	wsWg.Wait()

	// Set settings on pivot
	wsWg.Add(2)
	err = ooio.RemoteSet(pivotCfg, "settings", Settings{DayEpoch: 9})
	require.NoError(t, err)
	wsWg.Wait()

	pivotSettingsObj, err := ooio.RemoteGet[Settings](pivotCfg, "settings")
	require.NoError(t, err)
	require.Equal(t, 9, pivotSettingsObj.Data.DayEpoch)

	// Delete thing from pivot
	wsWg.Add(2)
	thingID2Resp, _ := ooio.RemotePushWithResponse(pivotCfg, "things/*", Thing{IP: "10.0.0.1", Port: 0, On: true})
	thingID2 := thingID2Resp.Index
	wsWg.Wait()

	wsWg.Add(2)
	err = ooio.RemoteDelete(pivotCfg, "things/"+thingID2)
	require.NoError(t, err)
	wsWg.Wait()

	_, err = ooio.RemoteGet[Thing](pivotCfg, "things/"+thingID2)
	require.Error(t, err)

	// Delete settings
	wsWg.Add(2)
	err = ooio.RemoteDelete(nodeCfg, "settings")
	require.NoError(t, err)
	wsWg.Wait()

	_, err = ooio.RemoteGet[Settings](nodeCfg, "settings")
	require.Error(t, err)
}
