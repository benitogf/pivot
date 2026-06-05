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
	"github.com/benitogf/go-json"
	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/client"
	ooio "github.com/benitogf/ooo/io"
	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
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

type Item struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func RegisterUser(t *testing.T, server *ooo.Server, account string) string {
	var c auth.Credentials
	payload := fmt.Appendf(nil, `{
        "name": "%s",
        "account":"%s",
        "password": "000",
        "email": "%s@test.cc",
        "phone": "555"
    }`, account, account, account)
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
	payload := fmt.Appendf(nil, `{
        "account":"%s",
        "password": "000"
    }`, account)
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
func FakeServer(t *testing.T, clusterURL string) *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Client = &http.Client{
		Timeout: 500 * time.Millisecond,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 500 * time.Millisecond,
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
			{Path: "items/*/*/*"},
		},
		NodesKey:            "things/*",
		ClusterURL:          clusterURL,
		HealthCheckInterval: 500 * time.Millisecond,
	}

	// Setup pivot - modifies server (routes, OnStorageEvent, BeforeRead)
	pivot.Setup(server, config)

	// Attach the external auth storage for pivot synchronization. No AfterWrite
	// is needed here: tests synchronize on observed state convergence (see
	// requireConverged), not on storage-callback counts.
	err := pivot.GetInstance(server).Attach(authStorage)
	require.NoError(t, err)

	server.OpenFilter("things/*")
	server.OpenFilter("settings")
	server.OpenFilter("items/*/*/*")

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
	return server
}

// requireConverged polls cond until it holds or the deadline elapses, failing
// the test with msg if it never converges. This replaces the old exact-count
// WaitGroup synchronization: the number of async events (websocket deliveries,
// storage AfterWrite callbacks) per logical operation is NOT deterministic in a
// cross-cluster sync (coalescing, push-vs-pull, duplicate deliveries, and
// delete-of-absent no-ops that emit no event), so counting them is inherently
// flaky and a missed count hangs to the package deadline. Waiting for the
// observable end-state instead is robust and fails fast with a clear message.
func requireConverged(t *testing.T, msg string, cond func() bool, diag ...func() string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			extra := ""
			if len(diag) > 0 && diag[0] != nil {
				extra = " | " + diag[0]()
			}
			t.Fatalf("convergence timeout: %s%s", msg, extra)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// settingsPresent reports whether "settings" currently reads successfully on the
// given side (non-failing — safe to call from a polling loop).
func (ops *syncTestOps) settingsPresent(fromPivot bool) bool {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if fromPivot {
			cfg = ops.pivotCfg
		}
		_, err := ooio.RemoteGet[Settings](cfg, "settings")
		return err == nil
	}
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	_, err := ooo.Get[Settings](server, "settings")
	return err == nil
}

// tryGetItem reads an item by full key on the given side without failing the
// test (returns ok=false on any error). Diagnostic only.
func (ops *syncTestOps) tryGetItem(fromPivot bool, fullKey string) (Item, bool) {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if fromPivot {
			cfg = ops.pivotCfg
		}
		r, err := ooio.RemoteGet[Item](cfg, fullKey)
		if err != nil {
			return Item{}, false
		}
		return r.Data, true
	}
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	r, err := ooo.Get[Item](server, fullKey)
	if err != nil {
		return Item{}, false
	}
	return r.Data, true
}

// tryGetThing reads things/<id> on the given side without failing the test
// (returns ok=false on any error). Used for diagnostics that distinguish a sync
// failure (storage missing the value) from a websocket-delivery gap (storage
// has it but the subscription slice is stale).
func (ops *syncTestOps) tryGetThing(fromPivot bool, id string) (Thing, bool) {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if fromPivot {
			cfg = ops.pivotCfg
		}
		r, err := ooio.RemoteGet[Thing](cfg, "things/"+id)
		if err != nil {
			return Thing{}, false
		}
		return r.Data, true
	}
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	r, err := ooo.Get[Thing](server, "things/"+id)
	if err != nil {
		return Thing{}, false
	}
	return r.Data, true
}

// policiesValue returns the parsed policies and whether GET /policies returned
// 200 on the given side (non-failing — safe to call from a polling loop).
func (ops *syncTestOps) policiesValue(fromPivot bool) (Policies, bool) {
	var p Policies
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	resp, err := server.Client.Get("http://" + server.Address + "/policies")
	if err != nil {
		return p, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return p, false
	}
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		return p, false
	}
	return p, true
}

// syncTestOps provides operations that can be local or remote based on the useRemote flag
type syncTestOps struct {
	useRemote   bool
	pivotServer *ooo.Server
	nodeServer  *ooo.Server
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

func (ops *syncTestOps) pushItem(t *testing.T, toPivot bool, path string, item Item) string {
	basePath := strings.TrimSuffix(path, "/*")
	if ops.useRemote {
		cfg := ops.nodeCfg
		if toPivot {
			cfg = ops.pivotCfg
		}
		resp, err := ooio.RemotePushWithResponse(cfg, path, item)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Index)
		return basePath + "/" + resp.Index
	}
	server := ops.nodeServer
	if toPivot {
		server = ops.pivotServer
	}
	id, err := ooo.Push(server, path, item)
	require.NoError(t, err)
	require.NotEmpty(t, id)
	return basePath + "/" + id
}

func (ops *syncTestOps) setItem(t *testing.T, toPivot bool, key string, item Item) {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if toPivot {
			cfg = ops.pivotCfg
		}
		err := ooio.RemoteSet(cfg, key, item)
		require.NoError(t, err)
		return
	}
	server := ops.nodeServer
	if toPivot {
		server = ops.pivotServer
	}
	err := ooo.Set(server, key, item)
	require.NoError(t, err)
}

func (ops *syncTestOps) deleteItem(t *testing.T, fromPivot bool, key string) {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if fromPivot {
			cfg = ops.pivotCfg
		}
		err := ooio.RemoteDelete(cfg, key)
		require.NoError(t, err)
		return
	}
	server := ops.nodeServer
	if fromPivot {
		server = ops.pivotServer
	}
	err := ooo.Delete(server, key)
	require.NoError(t, err)
}

func testClusterSync(t *testing.T, useRemote bool) {
	var pivotThings, nodeThings []client.Meta[Thing]
	var pivotSettings, nodeSettings []client.Meta[Settings]
	var pivotItems, nodeItems []client.Meta[Item]
	// Subscriptions for the special-character key sync cases. The
	// multi-glob pattern items/cat-1/sub.v2/* lets us drive items whose
	// path segments contain hyphens and dots while the leaf carries an
	// underscore — exercises all three new key characters at once.
	var pivotSpecialItems, nodeSpecialItems []client.Meta[Item]
	var mu sync.Mutex

	pivotServer := FakeServer(t, "")
	defer pivotServer.Close(os.Interrupt)
	nodeServer := FakeServer(t, pivotServer.Address)
	defer nodeServer.Close(os.Interrupt)

	ops := &syncTestOps{
		useRemote:   useRemote,
		pivotServer: pivotServer,
		nodeServer:  nodeServer,
	}
	if useRemote {
		ops.pivotCfg = ooio.RemoteConfig{Client: &http.Client{Timeout: 500 * time.Millisecond}, Host: pivotServer.Address}
		ops.nodeCfg = ooio.RemoteConfig{Client: &http.Client{Timeout: 500 * time.Millisecond}, Host: nodeServer.Address}
	}

	// Register a user on the pivot and authorize on the node. Both are
	// synchronous HTTP calls (Authorize triggers pivot's sync-on-read to pull
	// users/root onto the node), so no extra wait is needed before proceeding.
	token := RegisterUser(t, pivotServer, "root")
	require.NotEqual(t, "", token)
	token = Authorize(t, nodeServer, "root")
	require.NotEqual(t, "", token)

	authHeader := http.Header{}
	authHeader.Set("Authorization", "Bearer "+token)
	if useRemote {
		ops.pivotCfg.Header = authHeader
		ops.nodeCfg.Header = authHeader
	}

	ctx := t.Context()

	// Subscribe to things, settings, and items on both servers. Each OnMessage
	// records the latest delivered state under mu; the test then waits for that
	// state to CONVERGE (requireConverged) rather than counting per-operation
	// deliveries — the number of ws events per logical op is not deterministic
	// across the cluster (coalescing, push-vs-pull, delete-of-absent no-ops), so
	// counting them is what made the old test flaky.
	//
	// An establishment barrier IS still required before the first write: a
	// subscription delivers an empty initial snapshot at connect, and a write
	// that broadcasts before the subscription is fully live is missed and never
	// re-delivered, leaving its slice permanently stale. wsReady waits for each
	// sub's initial snapshot — a deterministic count of 8, independent of any
	// test write — so every sub is live before writes begin. Each established()
	// closure fires its Done exactly once (on the sub's first delivery).
	var wsReady sync.WaitGroup
	wsReady.Add(8)
	established := func() func() {
		var once sync.Once
		return func() { once.Do(wsReady.Done) }
	}
	estPivotThings, estNodeThings := established(), established()
	estNodeSettings, estPivotSettings := established(), established()
	estPivotItems, estNodeItems := established(), established()
	estPivotSpecial, estNodeSpecial := established(), established()

	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: pivotServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "things/*", client.SubscribeListEvents[Thing]{OnMessage: func(data []client.Meta[Thing]) {
		mu.Lock()
		pivotThings = data
		mu.Unlock()
		estPivotThings()
	}})
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:    ctx,
		Server: client.Server{Protocol: "ws", Host: nodeServer.Address},
		Header: authHeader, Silence: true}, "things/*",
		client.SubscribeListEvents[Thing]{OnMessage: func(data []client.Meta[Thing]) {
			mu.Lock()
			nodeThings = data
			mu.Unlock()
			estNodeThings()
		}})
	go client.Subscribe(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: nodeServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "settings", client.SubscribeEvents[Settings]{OnMessage: func(data client.Meta[Settings]) {
		mu.Lock()
		nodeSettings = []client.Meta[Settings]{data}
		mu.Unlock()
		estNodeSettings()
	}})
	go client.Subscribe(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: pivotServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "settings", client.SubscribeEvents[Settings]{OnMessage: func(data client.Meta[Settings]) {
		mu.Lock()
		pivotSettings = []client.Meta[Settings]{data}
		mu.Unlock()
		estPivotSettings()
	}})
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: pivotServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "items/cat/sub/*", client.SubscribeListEvents[Item]{OnMessage: func(data []client.Meta[Item]) {
		mu.Lock()
		pivotItems = data
		mu.Unlock()
		estPivotItems()
	}})
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: nodeServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "items/cat/sub/*", client.SubscribeListEvents[Item]{OnMessage: func(data []client.Meta[Item]) {
		mu.Lock()
		nodeItems = data
		mu.Unlock()
		estNodeItems()
	}})
	// Multi-glob subscription with special characters in the path
	// segments — covers `-` in cat-1, `.` in sub.v2, and (via the leaf
	// keys we write below) `_` in file_name.
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: pivotServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "items/cat-1/sub.v2/*", client.SubscribeListEvents[Item]{OnMessage: func(data []client.Meta[Item]) {
		mu.Lock()
		pivotSpecialItems = data
		mu.Unlock()
		estPivotSpecial()
	}})
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: nodeServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "items/cat-1/sub.v2/*", client.SubscribeListEvents[Item]{OnMessage: func(data []client.Meta[Item]) {
		mu.Lock()
		nodeSpecialItems = data
		mu.Unlock()
		estNodeSpecial()
	}})

	// Wait for every subscription's initial snapshot so all subs are live
	// before the first write (closes the connect→broadcast race above).
	wsEstablished := make(chan struct{})
	go func() {
		wsReady.Wait()
		close(wsEstablished)
	}()
	select {
	case <-wsEstablished:
	case <-time.After(20 * time.Second):
		t.Fatal("subscriptions did not deliver initial snapshots within 20s")
	}

	// diagThings reports, on a convergence timeout, the websocket-slice view vs.
	// the authoritative storage view — so a sync failure (storage wrong) is
	// distinguishable from a websocket-delivery gap (storage right, slice stale).
	diagThings := func(id string) string {
		mu.Lock()
		var pIdx, nIdx []string
		for _, m := range pivotThings {
			pIdx = append(pIdx, m.Index)
		}
		for _, m := range nodeThings {
			nIdx = append(nIdx, m.Index)
		}
		mu.Unlock()
		pGet, nGet := "absent", "absent"
		if _, ok := ops.tryGetThing(true, id); ok {
			pGet = "present"
		}
		if _, ok := ops.tryGetThing(false, id); ok {
			nGet = "present"
		}
		return fmt.Sprintf("things slice pivot=%v node=%v; storage[%s] pivot=%s node=%s", pIdx, nIdx, id, pGet, nGet)
	}

	// Get node address for Thing creation
	nodeIP, nodePort, _ := net.SplitHostPort(nodeServer.Address)
	nodePortInt, _ := strconv.Atoi(nodePort)

	// Push thing to pivot - converges on both servers' things/* subscriptions.
	thingID := ops.pushThing(t, true, Thing{IP: nodeIP, Port: nodePortInt, On: false})
	requireConverged(t, "push thing to pivot should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotThings) == 1 && len(nodeThings) == 1
	}, func() string { return diagThings(thingID) })

	mu.Lock()
	require.Equal(t, 1, len(pivotThings), "pivot should have 1 thing")
	require.Equal(t, false, pivotThings[0].Data.On)
	require.Equal(t, 1, len(nodeThings), "node should have 1 thing")
	require.Equal(t, false, nodeThings[0].Data.On)
	mu.Unlock()

	// Read from node - should have the thing after sync
	nodeThing := ops.getThing(t, false, thingID)
	require.Equal(t, false, nodeThing.On)

	// thingOn finds a thing by id in a subscription snapshot, returning whether
	// it is present and its On flag. Used to wait for an update (which doesn't
	// change the list length) to converge through the websocket sub.
	thingOn := func(list []client.Meta[Thing], id string) (found, on bool) {
		for _, m := range list {
			if m.Index == id {
				return true, m.Data.On
			}
		}
		return false, false
	}

	// Modify thing on pivot - both subs should converge to On=true.
	ops.setThing(t, true, thingID, Thing{IP: nodeIP, Port: nodePortInt, On: true})
	requireConverged(t, "thing On=true update should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		_, pon := thingOn(pivotThings, thingID)
		_, non := thingOn(nodeThings, thingID)
		return pon && non
	})

	// Verify update
	updatedThing := ops.getThing(t, true, thingID)
	require.Equal(t, true, updatedThing.On)
	nodeThing = ops.getThing(t, false, thingID)
	require.Equal(t, true, nodeThing.On)

	// Set settings on node - converges to DayEpoch=1 on both subs.
	ops.setSettings(t, false, Settings{DayEpoch: 1})
	requireConverged(t, "settings=1 should sync to node+pivot", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(nodeSettings) > 0 && nodeSettings[0].Data.DayEpoch == 1 &&
			len(pivotSettings) > 0 && pivotSettings[0].Data.DayEpoch == 1
	})

	// Set settings on pivot - converges to DayEpoch=9 on both subs.
	ops.setSettings(t, true, Settings{DayEpoch: 9})
	requireConverged(t, "settings=9 should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotSettings) > 0 && pivotSettings[0].Data.DayEpoch == 9 &&
			len(nodeSettings) > 0 && nodeSettings[0].Data.DayEpoch == 9
	})

	// Verify settings
	pivotSettingsObj := ops.getSettings(t, true)
	require.Equal(t, 9, pivotSettingsObj.DayEpoch)
	nodeSettingsObj := ops.getSettings(t, false)
	require.Equal(t, 9, nodeSettingsObj.DayEpoch)

	// Push a second thing to pivot - converges to 2 things on both.
	thingID2 := ops.pushThing(t, true, Thing{IP: "10.0.0.1", Port: 0, On: true})
	requireConverged(t, "second thing should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotThings) == 2 && len(nodeThings) == 2
	})

	// Delete from pivot - converges back to 1 thing (thingID) on both.
	ops.deleteThing(t, true, thingID2)
	requireConverged(t, "thingID2 delete should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotThings) == 1 && pivotThings[0].Index == thingID &&
			len(nodeThings) == 1 && nodeThings[0].Index == thingID
	})

	// Verify deletion
	ops.getThingExpectError(t, true, thingID2, "thingID2 should be deleted from pivot")
	ops.getThingExpectError(t, false, thingID2, "thingID2 should be deleted from node after sync")

	mu.Lock()
	require.Equal(t, 1, len(pivotThings), "pivot should have 1 thing after delete")
	require.Equal(t, 1, len(nodeThings), "node should have 1 thing after delete")
	require.Equal(t, thingID, pivotThings[0].Index)
	require.Equal(t, thingID, nodeThings[0].Index)
	mu.Unlock()

	// Push a third thing to node - converges to 2 things on both.
	thingID3 := ops.pushThing(t, false, Thing{IP: "172.16.0.1", Port: 0, On: false})
	requireConverged(t, "third thing (node push) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotThings) == 2 && len(nodeThings) == 2
	})

	// Delete from node - converges back to 1 thing (thingID) on both.
	ops.deleteThing(t, false, thingID3)
	requireConverged(t, "thingID3 delete (node) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotThings) == 1 && pivotThings[0].Index == thingID &&
			len(nodeThings) == 1 && nodeThings[0].Index == thingID
	}, func() string { return diagThings(thingID3) })

	// Verify deletion
	ops.getThingExpectError(t, false, thingID3, "thingID3 should be deleted from node")
	ops.getThingExpectError(t, true, thingID3, "thingID3 should be deleted from pivot after sync")

	// Update thing on node - converges to On=false on both subs.
	ops.setThing(t, false, thingID, Thing{IP: nodeIP, Port: nodePortInt, On: false})
	requireConverged(t, "thing On=false update (node) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		pf, pon := thingOn(pivotThings, thingID)
		nf, non := thingOn(nodeThings, thingID)
		return pf && !pon && nf && !non
	})

	// Verify update synced to pivot
	pivotThing := ops.getThing(t, true, thingID)
	require.Equal(t, nodeIP, pivotThing.IP)
	require.Equal(t, nodePortInt, pivotThing.Port)
	require.Equal(t, false, pivotThing.On)

	// Delete settings from node - converges to absent on both sides.
	ops.deleteSettings(t, false)
	requireConverged(t, "settings delete (node) should sync to pivot+node", func() bool {
		return !ops.settingsPresent(false) && !ops.settingsPresent(true)
	})

	// Verify settings deleted
	ops.getSettingsExpectError(t, false, "settings should be deleted from node")
	ops.getSettingsExpectError(t, true, "settings should be deleted from pivot after sync")

	// Set settings on pivot after delete - converges to DayEpoch=42 on both.
	ops.setSettings(t, true, Settings{DayEpoch: 42})
	requireConverged(t, "settings=42 should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotSettings) > 0 && pivotSettings[0].Data.DayEpoch == 42 &&
			len(nodeSettings) > 0 && nodeSettings[0].Data.DayEpoch == 42
	})

	// Verify settings synced
	nodeSettingsObj = ops.getSettings(t, false)
	require.Equal(t, 42, nodeSettingsObj.DayEpoch)

	// Delete settings from pivot - converges to absent on both sides.
	ops.deleteSettings(t, true)
	requireConverged(t, "settings delete (pivot) should sync to pivot+node", func() bool {
		return !ops.settingsPresent(true) && !ops.settingsPresent(false)
	})

	// Verify settings deleted
	ops.getSettingsExpectError(t, true, "settings should be deleted from pivot")
	ops.getSettingsExpectError(t, false, "settings should be deleted from node after sync")

	// === Policies sync tests (stored in authStorage, not server.Storage) ===

	// policiesMaxRetries reports whether GET /policies on the given side returns
	// 200 with the expected MaxRetries (each step below uses a distinct value,
	// so this uniquely identifies that the synced version has landed).
	policiesMaxRetries := func(fromPivot bool, want int) bool {
		p, ok := ops.policiesValue(fromPivot)
		return ok && p.MaxRetries == want
	}
	policiesAbsent := func(fromPivot bool) bool {
		_, ok := ops.policiesValue(fromPivot)
		return !ok
	}

	// Set policies on pivot - converges on both sides.
	ops.setPolicies(t, true, Policies{MaxRetries: 3, Allowed: []string{"read", "write"}})
	requireConverged(t, "policies(set,pivot) should sync to pivot+node", func() bool {
		return policiesMaxRetries(true, 3) && policiesMaxRetries(false, 3)
	})

	// Verify policies synced to node
	pivotPolicies := ops.getPolicies(t, true)
	require.Equal(t, 3, pivotPolicies.MaxRetries)
	require.Equal(t, []string{"read", "write"}, pivotPolicies.Allowed)
	nodePolicies := ops.getPolicies(t, false)
	require.Equal(t, 3, nodePolicies.MaxRetries)
	require.Equal(t, []string{"read", "write"}, nodePolicies.Allowed)

	// Update policies on node - converges on both sides.
	ops.setPolicies(t, false, Policies{MaxRetries: 5, Allowed: []string{"admin"}})
	requireConverged(t, "policies(update,node) should sync to pivot+node", func() bool {
		return policiesMaxRetries(true, 5) && policiesMaxRetries(false, 5)
	})

	// Verify policies synced to pivot
	pivotPolicies = ops.getPolicies(t, true)
	require.Equal(t, 5, pivotPolicies.MaxRetries)
	require.Equal(t, []string{"admin"}, pivotPolicies.Allowed)
	nodePolicies = ops.getPolicies(t, false)
	require.Equal(t, 5, nodePolicies.MaxRetries)
	require.Equal(t, []string{"admin"}, nodePolicies.Allowed)

	// Delete policies from pivot - converges to absent on both sides.
	ops.deletePolicies(t, true)
	requireConverged(t, "policies(delete,pivot) should sync to pivot+node", func() bool {
		return policiesAbsent(true) && policiesAbsent(false)
	})

	// Verify policies deleted from both
	ops.getPoliciesExpectError(t, true, "policies should be deleted from pivot")
	ops.getPoliciesExpectError(t, false, "policies should be deleted from node after sync")

	// Set policies on node after delete - converges on both sides.
	ops.setPolicies(t, false, Policies{MaxRetries: 10, Allowed: []string{"guest"}})
	requireConverged(t, "policies(set-after-delete,node) should sync to pivot+node", func() bool {
		return policiesMaxRetries(true, 10) && policiesMaxRetries(false, 10)
	}, func() string {
		p, pok := ops.policiesValue(true)
		n, nok := ops.policiesValue(false)
		return fmt.Sprintf("pivot{ok=%v mr=%d} node{ok=%v mr=%d}", pok, p.MaxRetries, nok, n.MaxRetries)
	})

	// Verify policies synced to pivot
	pivotPolicies = ops.getPolicies(t, true)
	require.Equal(t, 10, pivotPolicies.MaxRetries)
	require.Equal(t, []string{"guest"}, pivotPolicies.Allowed)

	// Delete policies from node - converges to absent on both sides.
	ops.deletePolicies(t, false)
	requireConverged(t, "policies(delete,node) should sync to pivot+node", func() bool {
		return policiesAbsent(false) && policiesAbsent(true)
	})

	// Verify policies deleted from both
	ops.getPoliciesExpectError(t, false, "policies should be deleted from node")
	ops.getPoliciesExpectError(t, true, "policies should be deleted from pivot after sync")

	// === Multi-glob sync tests (items/*/*/*) ===

	// itemNamed reports whether an items subscription snapshot contains an item
	// with the given leaf index and name — used to wait for an item update (no
	// length change) to converge through the websocket sub.
	itemNamed := func(list []client.Meta[Item], leaf, name string) bool {
		for _, m := range list {
			if m.Index == leaf && m.Data.Name == name {
				return true
			}
		}
		return false
	}

	// Push item to pivot - converges to 1 item ("p1") on both.
	itemID := ops.pushItem(t, true, "items/cat/sub/*", Item{Name: "p1", Value: 1})
	requireConverged(t, "item p1 (pivot push) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotItems) == 1 && pivotItems[0].Data.Name == "p1" &&
			len(nodeItems) == 1 && nodeItems[0].Data.Name == "p1"
	})

	// Push item from node - converges to 2 items on both.
	itemID2 := ops.pushItem(t, false, "items/cat/sub/*", Item{Name: "p2", Value: 2})
	requireConverged(t, "item p2 (node push) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotItems) == 2 && len(nodeItems) == 2
	})

	// Update item on pivot - converges to the new name on both.
	ops.setItem(t, true, itemID, Item{Name: "p1-updated", Value: 10})
	requireConverged(t, "item p1 update (pivot) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return itemNamed(pivotItems, key.LastIndex(itemID), "p1-updated") &&
			itemNamed(nodeItems, key.LastIndex(itemID), "p1-updated")
	}, func() string {
		mu.Lock()
		pn, nn := itemNamed(pivotItems, key.LastIndex(itemID), "p1-updated"), itemNamed(nodeItems, key.LastIndex(itemID), "p1-updated")
		mu.Unlock()
		pv, _ := ops.tryGetItem(true, itemID)
		nv, _ := ops.tryGetItem(false, itemID)
		return fmt.Sprintf("slice pivot=%v node=%v; storage pivot.name=%q node.name=%q", pn, nn, pv.Name, nv.Name)
	})

	mu.Lock()
	require.Equal(t, 2, len(pivotItems), "pivot should still have 2 items after update")
	require.Equal(t, 2, len(nodeItems), "node should still have 2 items after update")
	for _, item := range pivotItems {
		if item.Index == key.LastIndex(itemID) {
			require.Equal(t, "p1-updated", item.Data.Name)
			require.Equal(t, 10, item.Data.Value)
		}
	}
	for _, item := range nodeItems {
		if item.Index == key.LastIndex(itemID) {
			require.Equal(t, "p1-updated", item.Data.Name)
			require.Equal(t, 10, item.Data.Value)
		}
	}
	mu.Unlock()

	// Update item from node - converges to the new name on both.
	ops.setItem(t, false, itemID2, Item{Name: "p2-updated", Value: 20})
	requireConverged(t, "item p2 update (node) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return itemNamed(pivotItems, key.LastIndex(itemID2), "p2-updated") &&
			itemNamed(nodeItems, key.LastIndex(itemID2), "p2-updated")
	})

	mu.Lock()
	require.Equal(t, 2, len(pivotItems), "pivot should still have 2 items after node update")
	require.Equal(t, 2, len(nodeItems), "node should still have 2 items after node update")
	for _, item := range pivotItems {
		if item.Index == key.LastIndex(itemID2) {
			require.Equal(t, "p2-updated", item.Data.Name)
			require.Equal(t, 20, item.Data.Value)
		}
	}
	for _, item := range nodeItems {
		if item.Index == key.LastIndex(itemID2) {
			require.Equal(t, "p2-updated", item.Data.Name)
			require.Equal(t, 20, item.Data.Value)
		}
	}
	mu.Unlock()

	// Delete item from pivot - converges to 1 item ("p2-updated") on both.
	ops.deleteItem(t, true, itemID)
	requireConverged(t, "item p1 delete (pivot) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotItems) == 1 && pivotItems[0].Data.Name == "p2-updated" &&
			len(nodeItems) == 1 && nodeItems[0].Data.Name == "p2-updated"
	})

	// Delete item from node - converges to 0 items on both.
	ops.deleteItem(t, false, itemID2)
	requireConverged(t, "item p2 delete (node) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotItems) == 0 && len(nodeItems) == 0
	})

	// === Special-character key sync tests ===
	// The ooo dependency (PR #73) widens key.IsValid to admit hyphens,
	// dots, and underscores in middle positions. Pivot's index regex
	// for the /_pivot/<base>/{index}/{time} routes is widened to match.
	// Verify the new characters propagate end-to-end through pivot's
	// HTTP set/delete routes AND through both the pattern-level
	// subscription (things/*, which still receives writes to
	// things/<special-char-id>) and a sub-glob subscription that has
	// special characters in its own path segments
	// (items/cat-1/sub.v2/*).

	// findThing scans a things/* subscription's state for a specific id.
	findThing := func(list []client.Meta[Thing], id string) bool {
		for _, m := range list {
			if m.Index == id {
				return true
			}
		}
		return false
	}
	// Each id covers one of the three new characters in turn.
	specialIDs := []string{"hyphen-id", "underscore_id", "dot.id"}

	// Cycle each special-id thing through the pivot direction: pivot
	// writes, syncs to node via the trigger fanout (the pivot Set/Delete
	// HTTP handler is the URL surface that PR #73 widened — but the
	// pivot's local ooo.Set→callback fanout also matters because the
	// triggered node pulls via /pivot/things which serves entries whose
	// indexes now legally contain hyphens, dots, and underscores).
	for _, id := range specialIDs {
		ops.setThing(t, true, id, Thing{IP: "10.1.1.1", Port: 0, On: true})
		requireConverged(t, fmt.Sprintf("things/%s (pivot set) should sync to pivot+node", id), func() bool {
			mu.Lock()
			defer mu.Unlock()
			return findThing(pivotThings, id) && findThing(nodeThings, id)
		})

		ops.deleteThing(t, true, id)
		requireConverged(t, fmt.Sprintf("things/%s delete should sync to pivot+node", id), func() bool {
			mu.Lock()
			defer mu.Unlock()
			return !findThing(pivotThings, id) && !findThing(nodeThings, id)
		})
	}

	// Multi-glob with special characters in the path AND the leaf key.
	// The subscription pattern itself contains `-` (cat-1) and `.` (sub.v2);
	// the leaf key (file_name) carries `_`. All three new characters are
	// exercised at once, giving node→pivot URL coverage of the widened
	// /pivot/<base>/{index} regex.
	specialItemKey := "items/cat-1/sub.v2/file_name"

	// Pivot-side set: covers /pivot fanout → node pull on a special path.
	ops.setItem(t, true, specialItemKey, Item{Name: "special-1", Value: 100})
	requireConverged(t, "special item (pivot set) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotSpecialItems) == 1 && pivotSpecialItems[0].Data.Name == "special-1" &&
			len(nodeSpecialItems) == 1 && nodeSpecialItems[0].Data.Name == "special-1"
	})

	// Node-side update: exercises pivot's widened {index} regex via the
	// node→leader POST URL /pivot/items/cat-1/sub.v2/file_name. The pivot
	// Set handler accepts the path, writes locally, and the node sees the
	// confirmation via its WS sub.
	ops.setItem(t, false, specialItemKey, Item{Name: "special-2", Value: 200})
	requireConverged(t, "special item (node update) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotSpecialItems) == 1 && pivotSpecialItems[0].Data.Name == "special-2" &&
			len(nodeSpecialItems) == 1 && nodeSpecialItems[0].Data.Name == "special-2"
	})

	// Node-side delete: exercises the widened {index} regex on the DELETE
	// route too (/pivot/<base>/{index}/{time}).
	ops.deleteItem(t, false, specialItemKey)
	requireConverged(t, "special item (node delete) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotSpecialItems) == 0 && len(nodeSpecialItems) == 0
	})
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
		Keys:                []pivot.Key{{Path: "things/*"}, {Path: "settings"}},
		NodesKey:            "things/*",
		ClusterURL:          pivotServer.Address, // This is a node
		HealthCheckInterval: 500 * time.Millisecond,
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
		Keys:                []pivot.Key{{Path: "things/*"}},
		NodesKey:            "things/*",
		ClusterURL:          pivotServer.Address,
		HealthCheckInterval: 500 * time.Millisecond,
	}
	pivot.Setup(nodeServer, nodeConfig)
	nodeServer.Start("localhost:0")
	defer nodeServer.Close(os.Interrupt)

	// Verify both callbacks were called in order (user first, then pivot sync)
	require.Equal(t, []string{"user-callback"}, order)
}
