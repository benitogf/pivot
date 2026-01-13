package pivot_test

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
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

type Settings struct {
	DayEpoch int `json:"startOfDay"`
}

type Policies struct {
	MaxRetries int      `json:"maxRetries"`
	Allowed    []string `json:"allowed"`
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
// Returns the server and a WaitGroup controlled by storage AfterWrite callbacks.
func FakeServer(t *testing.T, clusterURL string) (*ooo.Server, *sync.WaitGroup) {
	wg := &sync.WaitGroup{}

	afterWrite := func(key string) {
		if strings.HasPrefix(key, "pivot/") {
			return
		}
		t.Logf("[server] storage write: %s", key)
		wg.Done()
	}
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
			{Path: "policies", Database: authStorage},
			{Path: "settings"},
		},
		NodesKey:   "things/*",
		ClusterURL: clusterURL,
	}

	// Setup pivot - modifies server (routes, OnStorageEvent, BeforeRead)
	pivot.Setup(server, config)

	// Use Attach for simplified external storage setup with AfterWrite callback
	err := pivot.GetInstance(server).Attach(authStorage, storage.Options{AfterWrite: afterWrite})
	require.NoError(t, err)

	// Start main storage with AfterWrite callback
	server.Storage.Start(storage.Options{AfterWrite: afterWrite})

	server.OpenFilter("things/*")
	server.OpenFilter("settings")

	_auth.Routes(server)

	// Custom endpoints for policies (stored in authStorage, not server.Storage)
	server.Router.HandleFunc("/policies", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			obj, err := authStorage.Get("policies")
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(obj.Data)
		case http.MethodPost:
			var policies Policies
			if err := json.NewDecoder(r.Body).Decode(&policies); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			data, err := json.Marshal(policies)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if _, err := authStorage.Set("policies", data); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			if err := authStorage.Del("policies"); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}).Methods(http.MethodGet, http.MethodPost, http.MethodDelete)

	server.Start("localhost:0")
	return server, wg
}

// syncTestOps provides operations that can be local or remote based on the useRemote flag
type syncTestOps struct {
	useRemote   bool
	pivotServer *ooo.Server
	nodeServer  *ooo.Server
	pivotWg     *sync.WaitGroup
	nodeWg      *sync.WaitGroup
	pivotCfg    ooio.RemoteConfig
	nodeCfg     ooio.RemoteConfig
}

func (ops *syncTestOps) pushThing(t *testing.T, toPivot bool, thing Thing) string {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if toPivot {
			cfg = ops.pivotCfg
		}
		resp, err := ooio.RemotePushWithResponse(cfg, "things/*", thing)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Index)
		return resp.Index
	}
	server := ops.nodeServer
	if toPivot {
		server = ops.pivotServer
	}
	id, err := ooo.Push(server, "things/*", thing)
	require.NoError(t, err)
	require.NotEmpty(t, id)
	return id
}

func (ops *syncTestOps) getThing(t *testing.T, fromPivot bool, id string) Thing {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if fromPivot {
			cfg = ops.pivotCfg
		}
		result, err := ooio.RemoteGet[Thing](cfg, "things/"+id)
		require.NoError(t, err)
		return result.Data
	}
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	result, err := ooo.Get[Thing](server, "things/"+id)
	require.NoError(t, err)
	return result.Data
}

func (ops *syncTestOps) getThingExpectError(t *testing.T, fromPivot bool, id string, msg string) {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if fromPivot {
			cfg = ops.pivotCfg
		}
		_, err := ooio.RemoteGet[Thing](cfg, "things/"+id)
		require.Error(t, err, msg)
		return
	}
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	_, err := ooo.Get[Thing](server, "things/"+id)
	require.Error(t, err, msg)
}

func (ops *syncTestOps) setThing(t *testing.T, toPivot bool, id string, thing Thing) {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if toPivot {
			cfg = ops.pivotCfg
		}
		err := ooio.RemoteSet(cfg, "things/"+id, thing)
		require.NoError(t, err)
		return
	}
	server := ops.nodeServer
	if toPivot {
		server = ops.pivotServer
	}
	err := ooo.Set(server, "things/"+id, thing)
	require.NoError(t, err)
}

func (ops *syncTestOps) deleteThing(t *testing.T, fromPivot bool, id string) {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if fromPivot {
			cfg = ops.pivotCfg
		}
		err := ooio.RemoteDelete(cfg, "things/"+id)
		require.NoError(t, err)
		return
	}
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	err := ooo.Delete(server, "things/"+id)
	require.NoError(t, err)
}

func (ops *syncTestOps) setSettings(t *testing.T, toPivot bool, settings Settings) {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if toPivot {
			cfg = ops.pivotCfg
		}
		err := ooio.RemoteSet(cfg, "settings", settings)
		require.NoError(t, err)
		return
	}
	server := ops.nodeServer
	if toPivot {
		server = ops.pivotServer
	}
	err := ooo.Set(server, "settings", settings)
	require.NoError(t, err)
}

func (ops *syncTestOps) getSettings(t *testing.T, fromPivot bool) Settings {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if fromPivot {
			cfg = ops.pivotCfg
		}
		result, err := ooio.RemoteGet[Settings](cfg, "settings")
		require.NoError(t, err)
		return result.Data
	}
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	result, err := ooo.Get[Settings](server, "settings")
	require.NoError(t, err)
	return result.Data
}

func (ops *syncTestOps) getSettingsExpectError(t *testing.T, fromPivot bool, msg string) {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if fromPivot {
			cfg = ops.pivotCfg
		}
		_, err := ooio.RemoteGet[Settings](cfg, "settings")
		require.Error(t, err, msg)
		return
	}
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	_, err := ooo.Get[Settings](server, "settings")
	require.Error(t, err, msg)
}

func (ops *syncTestOps) deleteSettings(t *testing.T, fromPivot bool) {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if fromPivot {
			cfg = ops.pivotCfg
		}
		err := ooio.RemoteDelete(cfg, "settings")
		require.NoError(t, err)
		return
	}
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	err := ooo.Delete(server, "settings")
	require.NoError(t, err)
}

func (ops *syncTestOps) setPolicies(t *testing.T, toPivot bool, policies Policies) {
	server := ops.nodeServer
	if toPivot {
		server = ops.pivotServer
	}
	data, err := json.Marshal(policies)
	require.NoError(t, err)
	resp, err := server.Client.Post("http://"+server.Address+"/policies", "application/json", bytes.NewBuffer(data))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func (ops *syncTestOps) getPolicies(t *testing.T, fromPivot bool) Policies {
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	resp, err := server.Client.Get("http://" + server.Address + "/policies")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var policies Policies
	err = json.NewDecoder(resp.Body).Decode(&policies)
	require.NoError(t, err)
	return policies
}

func (ops *syncTestOps) getPoliciesExpectError(t *testing.T, fromPivot bool, msg string) {
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	resp, err := server.Client.Get("http://" + server.Address + "/policies")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotEqual(t, http.StatusOK, resp.StatusCode, msg)
}

func (ops *syncTestOps) deletePolicies(t *testing.T, fromPivot bool) {
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	req, err := http.NewRequest(http.MethodDelete, "http://"+server.Address+"/policies", nil)
	require.NoError(t, err)
	resp, err := server.Client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func testClusterSync(t *testing.T, useRemote bool) {
	var wsWg sync.WaitGroup
	var pivotThings, nodeThings []client.Meta[Thing]
	var pivotSettings, nodeSettings []client.Meta[Settings]
	var mu sync.Mutex

	pivotServer, pivotWg := FakeServer(t, "")
	defer pivotServer.Close(os.Interrupt)
	nodeServer, nodeWg := FakeServer(t, pivotServer.Address)
	defer nodeServer.Close(os.Interrupt)

	ops := &syncTestOps{
		useRemote:   useRemote,
		pivotServer: pivotServer,
		nodeServer:  nodeServer,
		pivotWg:     pivotWg,
		nodeWg:      nodeWg,
	}
	if useRemote {
		ops.pivotCfg = ooio.RemoteConfig{Client: &http.Client{Timeout: 5 * time.Second}, Host: pivotServer.Address}
		ops.nodeCfg = ooio.RemoteConfig{Client: &http.Client{Timeout: 5 * time.Second}, Host: nodeServer.Address}
	}

	// Register and authorize users
	pivotWg.Add(1)
	token := RegisterUser(t, pivotServer, "root")
	require.NotEqual(t, "", token)
	pivotWg.Wait()

	nodeWg.Add(1)
	token = Authorize(t, nodeServer, "root")
	require.NotEqual(t, "", token)
	nodeWg.Wait()

	authHeader := http.Header{}
	authHeader.Set("Authorization", "Bearer "+token)
	if useRemote {
		ops.pivotCfg.Header = authHeader
		ops.nodeCfg.Header = authHeader
	}

	ctx := t.Context()
	wsWg.Add(4)

	// Subscribe to things and settings on both servers
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

	// Verify initial state
	mu.Lock()
	require.Equal(t, 0, len(pivotThings))
	require.Equal(t, 0, len(nodeThings))
	mu.Unlock()

	// Get node address for Thing creation
	nodeIP, nodePort, _ := net.SplitHostPort(nodeServer.Address)
	nodePortInt, _ := strconv.Atoi(nodePort)

	// Push thing to pivot - expect 2 ws events (pivot + node via TriggerNodeSync)
	wsWg.Add(2)
	thingID := ops.pushThing(t, true, Thing{IP: nodeIP, Port: nodePortInt, On: false})
	wsWg.Wait()

	mu.Lock()
	require.Equal(t, 1, len(pivotThings), "pivot should have 1 thing")
	require.Equal(t, false, pivotThings[0].Data.On)
	require.Equal(t, 1, len(nodeThings), "node should have 1 thing")
	require.Equal(t, false, nodeThings[0].Data.On)
	mu.Unlock()

	// Read from node - should have the thing after sync
	nodeThing := ops.getThing(t, false, thingID)
	require.Equal(t, false, nodeThing.On)

	// Modify thing on pivot - expect 2 ws events
	wsWg.Add(2)
	ops.setThing(t, true, thingID, Thing{IP: nodeIP, Port: nodePortInt, On: true})
	wsWg.Wait()

	// Verify update
	updatedThing := ops.getThing(t, true, thingID)
	require.Equal(t, true, updatedThing.On)
	nodeThing = ops.getThing(t, false, thingID)
	require.Equal(t, true, nodeThing.On)

	// Set settings on node - expect 2 ws events
	wsWg.Add(2)
	ops.setSettings(t, false, Settings{DayEpoch: 1})
	wsWg.Wait()

	mu.Lock()
	require.Equal(t, 1, nodeSettings[0].Data.DayEpoch)
	require.Equal(t, 1, pivotSettings[0].Data.DayEpoch)
	mu.Unlock()

	// Set settings on pivot - expect 2 ws events
	wsWg.Add(2)
	ops.setSettings(t, true, Settings{DayEpoch: 9})
	wsWg.Wait()

	// Verify settings
	pivotSettingsObj := ops.getSettings(t, true)
	require.Equal(t, 9, pivotSettingsObj.DayEpoch)
	nodeSettingsObj := ops.getSettings(t, false)
	require.Equal(t, 9, nodeSettingsObj.DayEpoch)

	// Push a second thing to pivot
	wsWg.Add(2)
	thingID2 := ops.pushThing(t, true, Thing{IP: "10.0.0.1", Port: 0, On: true})
	wsWg.Wait()

	mu.Lock()
	require.Equal(t, 2, len(pivotThings), "pivot should have 2 things")
	require.Equal(t, 2, len(nodeThings), "node should have 2 things")
	mu.Unlock()

	// Delete from pivot - expect 2 ws events
	wsWg.Add(2)
	ops.deleteThing(t, true, thingID2)
	wsWg.Wait()

	// Verify deletion
	ops.getThingExpectError(t, true, thingID2, "thingID2 should be deleted from pivot")
	ops.getThingExpectError(t, false, thingID2, "thingID2 should be deleted from node after sync")

	mu.Lock()
	require.Equal(t, 1, len(pivotThings), "pivot should have 1 thing after delete")
	require.Equal(t, 1, len(nodeThings), "node should have 1 thing after delete")
	require.Equal(t, thingID, pivotThings[0].Index)
	require.Equal(t, thingID, nodeThings[0].Index)
	mu.Unlock()

	// Push a third thing to node
	wsWg.Add(2)
	thingID3 := ops.pushThing(t, false, Thing{IP: "172.16.0.1", Port: 0, On: false})
	wsWg.Wait()

	mu.Lock()
	require.Equal(t, 2, len(pivotThings), "pivot should have 2 things after node push")
	require.Equal(t, 2, len(nodeThings), "node should have 2 things after node push")
	mu.Unlock()

	// Delete from node - expect 2 ws events
	wsWg.Add(2)
	ops.deleteThing(t, false, thingID3)
	wsWg.Wait()

	// Verify deletion
	ops.getThingExpectError(t, false, thingID3, "thingID3 should be deleted from node")
	ops.getThingExpectError(t, true, thingID3, "thingID3 should be deleted from pivot after sync")

	mu.Lock()
	require.Equal(t, 1, len(pivotThings), "pivot should have 1 thing after node delete")
	require.Equal(t, 1, len(nodeThings), "node should have 1 thing after node delete")
	mu.Unlock()

	// Update thing on node - expect 2 ws events
	wsWg.Add(2)
	ops.setThing(t, false, thingID, Thing{IP: nodeIP, Port: nodePortInt, On: false})
	wsWg.Wait()

	// Verify update synced to pivot
	pivotThing := ops.getThing(t, true, thingID)
	require.Equal(t, nodeIP, pivotThing.IP)
	require.Equal(t, nodePortInt, pivotThing.Port)
	require.Equal(t, false, pivotThing.On)

	// Delete settings from node - expect 2 ws events
	wsWg.Add(2)
	ops.deleteSettings(t, false)
	wsWg.Wait()

	// Verify settings deleted
	ops.getSettingsExpectError(t, false, "settings should be deleted from node")
	ops.getSettingsExpectError(t, true, "settings should be deleted from pivot after sync")

	// Set settings on pivot after delete - expect 2 ws events
	wsWg.Add(2)
	ops.setSettings(t, true, Settings{DayEpoch: 42})
	wsWg.Wait()

	// Verify settings synced
	nodeSettingsObj = ops.getSettings(t, false)
	require.Equal(t, 42, nodeSettingsObj.DayEpoch)

	// Delete settings from pivot - expect 2 ws events
	wsWg.Add(2)
	ops.deleteSettings(t, true)
	wsWg.Wait()

	// Verify settings deleted
	ops.getSettingsExpectError(t, true, "settings should be deleted from pivot")
	ops.getSettingsExpectError(t, false, "settings should be deleted from node after sync")

	// === Policies sync tests (stored in authStorage, not server.Storage) ===

	// Set policies on pivot - expect sync to node via authStorage AfterWrite callback
	pivotWg.Add(1)
	nodeWg.Add(1)
	ops.setPolicies(t, true, Policies{MaxRetries: 3, Allowed: []string{"read", "write"}})
	pivotWg.Wait()
	nodeWg.Wait()

	// Verify policies synced to node
	pivotPolicies := ops.getPolicies(t, true)
	require.Equal(t, 3, pivotPolicies.MaxRetries)
	require.Equal(t, []string{"read", "write"}, pivotPolicies.Allowed)
	nodePolicies := ops.getPolicies(t, false)
	require.Equal(t, 3, nodePolicies.MaxRetries)
	require.Equal(t, []string{"read", "write"}, nodePolicies.Allowed)

	// Update policies on node - expect sync to pivot
	nodeWg.Add(1)
	pivotWg.Add(1)
	ops.setPolicies(t, false, Policies{MaxRetries: 5, Allowed: []string{"admin"}})
	nodeWg.Wait()
	pivotWg.Wait()

	// Verify policies synced to pivot
	pivotPolicies = ops.getPolicies(t, true)
	require.Equal(t, 5, pivotPolicies.MaxRetries)
	require.Equal(t, []string{"admin"}, pivotPolicies.Allowed)
	nodePolicies = ops.getPolicies(t, false)
	require.Equal(t, 5, nodePolicies.MaxRetries)
	require.Equal(t, []string{"admin"}, nodePolicies.Allowed)

	// Delete policies from pivot - expect sync to node
	pivotWg.Add(1)
	nodeWg.Add(1)
	ops.deletePolicies(t, true)
	pivotWg.Wait()
	nodeWg.Wait()

	// Verify policies deleted from both
	ops.getPoliciesExpectError(t, true, "policies should be deleted from pivot")
	ops.getPoliciesExpectError(t, false, "policies should be deleted from node after sync")

	// Set policies on node after delete - expect sync to pivot
	nodeWg.Add(1)
	pivotWg.Add(1)
	ops.setPolicies(t, false, Policies{MaxRetries: 10, Allowed: []string{"guest"}})
	nodeWg.Wait()
	pivotWg.Wait()

	// Verify policies synced to pivot
	pivotPolicies = ops.getPolicies(t, true)
	require.Equal(t, 10, pivotPolicies.MaxRetries)
	require.Equal(t, []string{"guest"}, pivotPolicies.Allowed)

	// Delete policies from node - expect sync to pivot
	nodeWg.Add(1)
	pivotWg.Add(1)
	ops.deletePolicies(t, false)
	nodeWg.Wait()
	pivotWg.Wait()

	// Verify policies deleted from both
	ops.getPoliciesExpectError(t, false, "policies should be deleted from node")
	ops.getPoliciesExpectError(t, true, "policies should be deleted from pivot after sync")
}

func TestClusterSyncLocal(t *testing.T) {
	testClusterSync(t, false)
}

func TestClusterSyncRemote(t *testing.T) {
	testClusterSync(t, true)
}

func TestOnStartSync(t *testing.T) {
	// Test that node server syncs with pivot on startup via OnStart callback
	pivotServer := &ooo.Server{}
	pivotServer.Silence = true
	pivotServer.Static = true
	pivotServer.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	pivotServer.OpenFilter("things/*")
	pivotServer.OpenFilter("settings")

	pivotConfig := pivot.Config{
		Keys:       []pivot.Key{{Path: "things/*"}, {Path: "settings"}},
		NodesKey:   "things/*",
		ClusterURL: "", // This is the pivot
	}
	pivot.Setup(pivotServer, pivotConfig)
	pivotServer.Start("localhost:0")
	defer pivotServer.Close(os.Interrupt)

	// Add data to pivot before node starts
	err := ooo.Set(pivotServer, "settings", Settings{DayEpoch: 123})
	require.NoError(t, err)
	_, err = ooo.Push(pivotServer, "things/*", Thing{IP: "10.0.0.1", Port: 8080, On: true})
	require.NoError(t, err)

	// Create node server - should sync on start
	nodeServer := &ooo.Server{}
	nodeServer.Silence = true
	nodeServer.Static = true
	nodeServer.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	nodeServer.OpenFilter("things/*")
	nodeServer.OpenFilter("settings")

	nodeConfig := pivot.Config{
		Keys:       []pivot.Key{{Path: "things/*"}, {Path: "settings"}},
		NodesKey:   "things/*",
		ClusterURL: pivotServer.Address, // This is a node
	}
	pivot.Setup(nodeServer, nodeConfig)
	nodeServer.Start("localhost:0")
	defer nodeServer.Close(os.Interrupt)

	// Verify node has synced data from pivot (via OnStart)
	nodeSettings, err := ooo.Get[Settings](nodeServer, "settings")
	require.NoError(t, err)
	require.Equal(t, 123, nodeSettings.Data.DayEpoch)

	nodeThings, err := ooo.GetList[Thing](nodeServer, "things/*")
	require.NoError(t, err)
	require.Equal(t, 1, len(nodeThings))
	require.Equal(t, "10.0.0.1", nodeThings[0].Data.IP)
	require.Equal(t, 8080, nodeThings[0].Data.Port)
	require.Equal(t, true, nodeThings[0].Data.On)
}

func TestOnStartComposition(t *testing.T) {
	// Test that pivot Setup composes OnStart callbacks correctly
	var order []string

	pivotServer := &ooo.Server{}
	pivotServer.Silence = true
	pivotServer.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	pivotServer.OpenFilter("things/*")

	pivotConfig := pivot.Config{
		Keys:       []pivot.Key{{Path: "things/*"}},
		NodesKey:   "things/*",
		ClusterURL: "",
	}
	pivot.Setup(pivotServer, pivotConfig)
	pivotServer.Start("localhost:0")
	defer pivotServer.Close(os.Interrupt)

	// Create node with existing OnStart
	nodeServer := &ooo.Server{}
	nodeServer.Silence = true
	nodeServer.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	nodeServer.OpenFilter("things/*")
	nodeServer.OnStart = func() {
		order = append(order, "user-callback")
	}

	nodeConfig := pivot.Config{
		Keys:       []pivot.Key{{Path: "things/*"}},
		NodesKey:   "things/*",
		ClusterURL: pivotServer.Address,
	}
	pivot.Setup(nodeServer, nodeConfig)
	nodeServer.Start("localhost:0")
	defer nodeServer.Close(os.Interrupt)

	// Verify both callbacks were called in order (user first, then pivot sync)
	require.Equal(t, []string{"user-callback"}, order)
}
