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
//
// onPolicyWrite (if non-nil) fires once per committed write to the "policies"
// key on this server's auth storage. Policies are served through custom HTTP
// routes, not a websocket-broadcast path, so unlike things/settings/items they
// deliver nothing to a subscription — the storage write is the only
// deterministic completion signal a test can wait on. Bookkeeping writes
// (the StoragePrefix tombstone) are excluded so the count stays exactly one
// per logical policies mutation per side.
func FakeServer(t *testing.T, clusterURL string, onPolicyWrite func()) *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	// Lossless watch: never drop a storage→broadcast event under consumer
	// stall. The default drop-after-timeout is a production write-hang
	// resilience feature, but in this deterministic test a dropped broadcast
	// would silently desync a subscriber and surface as a flaky hung Wait. With
	// it off, every committed write deterministically reaches every live sub —
	// exactly what the per-operation WaitGroups below assume. OnDroppedEvent is
	// wired as a guard: it must never fire.
	server.LosslessWatch = true
	server.OnDroppedEvent = func(ev storage.Event) {
		t.Errorf("watch event dropped (key=%q op=%q) — lossless watch should prevent this", ev.Key, ev.Operation)
	}
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

	// Attach the external auth storage for pivot synchronization. The AfterWrite
	// signals committed "policies" writes so a test can wait on them
	// deterministically — policies have no websocket subscription to count
	// deliveries on (custom HTTP routes), so the storage write is the signal.
	err := pivot.GetInstance(server).Attach(authStorage, storage.Options{
		AfterWrite: func(key string) {
			if key == "policies" && onPolicyWrite != nil {
				onPolicyWrite()
			}
		},
	})
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

	// policyWrites counts committed "policies" writes across both servers.
	// Policies have no websocket subscription (custom HTTP routes), so the
	// storage write — wired via FakeServer's onPolicyWrite — is the
	// deterministic completion signal. Each policies mutation writes once per
	// side, so the policies steps below arm policyWrites.Add(2).
	var policyWrites sync.WaitGroup
	onPolicyWrite := func() { policyWrites.Done() }

	pivotServer := FakeServer(t, "", onPolicyWrite)
	defer pivotServer.Close(os.Interrupt)
	nodeServer := FakeServer(t, pivotServer.Address, onPolicyWrite)
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
	// records the latest delivered state and signals a WaitGroup — both under mu.
	//
	// Two deterministic barriers replace the old convergence polling:
	//
	//   - wsReady (count 8): a subscription's FIRST delivery is its empty
	//     initial snapshot at connect. A write that broadcasts before a
	//     subscription is live is missed and never re-delivered, so every sub
	//     must be live before the first write. The count is exactly 8 — one per
	//     subscription, independent of any test write.
	//
	//   - deliv: every SUBSEQUENT delivery (one caused by a test operation).
	//     With the version-vector fix in this branch, each logical mutation
	//     produces exactly one delivery per subscribed side — the duplicate
	//     push-vs-pull deliveries that once made counts non-deterministic are
	//     gone. So each operation below arms deliv.Add(2) (the pivot sub + the
	//     node sub), triggers, and Waits. Counts are exact; a wrong count
	//     surfaces as a hung Wait — the signal to fix the count, per
	//     /testing-go-backend-async (no sleeps, no polling, no time.After).
	//
	// delivered() returns a per-subscription closure (invoked under mu) that
	// routes the first delivery to wsReady and every later one to deliv.
	var wsReady sync.WaitGroup
	wsReady.Add(8)
	var deliv sync.WaitGroup
	delivered := func() func() {
		established := false
		return func() {
			if established {
				deliv.Done()
				return
			}
			established = true
			wsReady.Done()
		}
	}
	onPivotThings, onNodeThings := delivered(), delivered()
	onNodeSettings, onPivotSettings := delivered(), delivered()
	onPivotItems, onNodeItems := delivered(), delivered()
	onPivotSpecial, onNodeSpecial := delivered(), delivered()

	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: pivotServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "things/*", client.SubscribeListEvents[Thing]{OnMessage: func(data []client.Meta[Thing]) {
		mu.Lock()
		pivotThings = data
		onPivotThings()
		mu.Unlock()
	}})
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:    ctx,
		Server: client.Server{Protocol: "ws", Host: nodeServer.Address},
		Header: authHeader, Silence: true}, "things/*",
		client.SubscribeListEvents[Thing]{OnMessage: func(data []client.Meta[Thing]) {
			mu.Lock()
			nodeThings = data
			onNodeThings()
			mu.Unlock()
		}})
	go client.Subscribe(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: nodeServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "settings", client.SubscribeEvents[Settings]{OnMessage: func(data client.Meta[Settings]) {
		mu.Lock()
		nodeSettings = []client.Meta[Settings]{data}
		onNodeSettings()
		mu.Unlock()
	}})
	go client.Subscribe(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: pivotServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "settings", client.SubscribeEvents[Settings]{OnMessage: func(data client.Meta[Settings]) {
		mu.Lock()
		pivotSettings = []client.Meta[Settings]{data}
		onPivotSettings()
		mu.Unlock()
	}})
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: pivotServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "items/cat/sub/*", client.SubscribeListEvents[Item]{OnMessage: func(data []client.Meta[Item]) {
		mu.Lock()
		pivotItems = data
		onPivotItems()
		mu.Unlock()
	}})
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: nodeServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "items/cat/sub/*", client.SubscribeListEvents[Item]{OnMessage: func(data []client.Meta[Item]) {
		mu.Lock()
		nodeItems = data
		onNodeItems()
		mu.Unlock()
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
		onPivotSpecial()
		mu.Unlock()
	}})
	go client.SubscribeList(client.SubscribeConfig{
		Ctx:     ctx,
		Server:  client.Server{Protocol: "ws", Host: nodeServer.Address},
		Header:  authHeader,
		Silence: true,
	}, "items/cat-1/sub.v2/*", client.SubscribeListEvents[Item]{OnMessage: func(data []client.Meta[Item]) {
		mu.Lock()
		nodeSpecialItems = data
		onNodeSpecial()
		mu.Unlock()
	}})

	// Wait for every subscription's initial snapshot so all subs are live
	// before the first write (closes the connect→broadcast race above). A
	// plain WaitGroup of a known count — no timeout wrapper.
	wsReady.Wait()

	// converged asserts the post-operation state holds. It runs AFTER the
	// operation's deliv.Wait()/policyWrites.Wait() returns, so the deliveries it
	// checks have already arrived — it verifies the delivered CONTENT (value,
	// length), not a race. cond locks mu itself where it reads shared slices.
	converged := func(msg string, cond func() bool) {
		t.Helper()
		require.True(t, cond(), msg)
	}

	// Get node address for Thing creation
	nodeIP, nodePort, _ := net.SplitHostPort(nodeServer.Address)
	nodePortInt, _ := strconv.Atoi(nodePort)

	// Push thing to pivot - one delivery to each things/* sub (pivot + node).
	deliv.Add(2)
	thingID := ops.pushThing(t, true, Thing{IP: nodeIP, Port: nodePortInt, On: false})
	deliv.Wait()
	converged("push thing to pivot should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotThings) == 1 && len(nodeThings) == 1
	})

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

	// Modify thing on pivot - one delivery to each sub, both reach On=true.
	deliv.Add(2)
	ops.setThing(t, true, thingID, Thing{IP: nodeIP, Port: nodePortInt, On: true})
	deliv.Wait()
	converged("thing On=true update should sync to pivot+node", func() bool {
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

	// Set settings on node - one delivery to each settings sub (DayEpoch=1).
	deliv.Add(2)
	ops.setSettings(t, false, Settings{DayEpoch: 1})
	deliv.Wait()
	converged("settings=1 should sync to node+pivot", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(nodeSettings) > 0 && nodeSettings[0].Data.DayEpoch == 1 &&
			len(pivotSettings) > 0 && pivotSettings[0].Data.DayEpoch == 1
	})

	// Set settings on pivot - one delivery to each settings sub (DayEpoch=9).
	deliv.Add(2)
	ops.setSettings(t, true, Settings{DayEpoch: 9})
	deliv.Wait()
	converged("settings=9 should sync to pivot+node", func() bool {
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

	// Push a second thing to pivot - one delivery to each sub (now 2 things).
	deliv.Add(2)
	thingID2 := ops.pushThing(t, true, Thing{IP: "10.0.0.1", Port: 0, On: true})
	deliv.Wait()
	converged("second thing should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotThings) == 2 && len(nodeThings) == 2
	})

	// Delete from pivot - one delivery to each sub (back to 1 thing, thingID).
	deliv.Add(2)
	ops.deleteThing(t, true, thingID2)
	deliv.Wait()
	converged("thingID2 delete should sync to pivot+node", func() bool {
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

	// Push a third thing to node - one delivery to each sub (now 2 things).
	deliv.Add(2)
	thingID3 := ops.pushThing(t, false, Thing{IP: "172.16.0.1", Port: 0, On: false})
	deliv.Wait()
	converged("third thing (node push) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotThings) == 2 && len(nodeThings) == 2
	})

	// Delete from node - one delivery to each sub (back to 1 thing, thingID).
	deliv.Add(2)
	ops.deleteThing(t, false, thingID3)
	deliv.Wait()
	converged("thingID3 delete (node) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotThings) == 1 && pivotThings[0].Index == thingID &&
			len(nodeThings) == 1 && nodeThings[0].Index == thingID
	})

	// Verify deletion
	ops.getThingExpectError(t, false, thingID3, "thingID3 should be deleted from node")
	ops.getThingExpectError(t, true, thingID3, "thingID3 should be deleted from pivot after sync")

	// Update thing on node - one delivery to each sub, both reach On=false.
	deliv.Add(2)
	ops.setThing(t, false, thingID, Thing{IP: nodeIP, Port: nodePortInt, On: false})
	deliv.Wait()
	converged("thing On=false update (node) should sync to pivot+node", func() bool {
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

	// Delete settings from node - one delivery to each sub (now absent).
	deliv.Add(2)
	ops.deleteSettings(t, false)
	deliv.Wait()
	converged("settings delete (node) should sync to pivot+node", func() bool {
		return !ops.settingsPresent(false) && !ops.settingsPresent(true)
	})

	// Verify settings deleted
	ops.getSettingsExpectError(t, false, "settings should be deleted from node")
	ops.getSettingsExpectError(t, true, "settings should be deleted from pivot after sync")

	// Set settings on pivot after delete - one delivery to each sub (DayEpoch=42).
	deliv.Add(2)
	ops.setSettings(t, true, Settings{DayEpoch: 42})
	deliv.Wait()
	converged("settings=42 should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotSettings) > 0 && pivotSettings[0].Data.DayEpoch == 42 &&
			len(nodeSettings) > 0 && nodeSettings[0].Data.DayEpoch == 42
	})

	// Verify settings synced
	nodeSettingsObj = ops.getSettings(t, false)
	require.Equal(t, 42, nodeSettingsObj.DayEpoch)

	// Delete settings from pivot - one delivery to each sub (now absent).
	deliv.Add(2)
	ops.deleteSettings(t, true)
	deliv.Wait()
	converged("settings delete (pivot) should sync to pivot+node", func() bool {
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

	// Set policies on pivot - one authStorage write per side (no ws sub).
	policyWrites.Add(2)
	ops.setPolicies(t, true, Policies{MaxRetries: 3, Allowed: []string{"read", "write"}})
	policyWrites.Wait()
	converged("policies(set,pivot) should sync to pivot+node", func() bool {
		return policiesMaxRetries(true, 3) && policiesMaxRetries(false, 3)
	})

	// Verify policies synced to node
	pivotPolicies := ops.getPolicies(t, true)
	require.Equal(t, 3, pivotPolicies.MaxRetries)
	require.Equal(t, []string{"read", "write"}, pivotPolicies.Allowed)
	nodePolicies := ops.getPolicies(t, false)
	require.Equal(t, 3, nodePolicies.MaxRetries)
	require.Equal(t, []string{"read", "write"}, nodePolicies.Allowed)

	// Update policies on node - one authStorage write per side.
	policyWrites.Add(2)
	ops.setPolicies(t, false, Policies{MaxRetries: 5, Allowed: []string{"admin"}})
	policyWrites.Wait()
	converged("policies(update,node) should sync to pivot+node", func() bool {
		return policiesMaxRetries(true, 5) && policiesMaxRetries(false, 5)
	})

	// Verify policies synced to pivot
	pivotPolicies = ops.getPolicies(t, true)
	require.Equal(t, 5, pivotPolicies.MaxRetries)
	require.Equal(t, []string{"admin"}, pivotPolicies.Allowed)
	nodePolicies = ops.getPolicies(t, false)
	require.Equal(t, 5, nodePolicies.MaxRetries)
	require.Equal(t, []string{"admin"}, nodePolicies.Allowed)

	// Delete policies from pivot - one authStorage write per side (now absent).
	policyWrites.Add(2)
	ops.deletePolicies(t, true)
	policyWrites.Wait()
	converged("policies(delete,pivot) should sync to pivot+node", func() bool {
		return policiesAbsent(true) && policiesAbsent(false)
	})

	// Verify policies deleted from both
	ops.getPoliciesExpectError(t, true, "policies should be deleted from pivot")
	ops.getPoliciesExpectError(t, false, "policies should be deleted from node after sync")

	// Set policies on node after delete - one authStorage write per side.
	policyWrites.Add(2)
	ops.setPolicies(t, false, Policies{MaxRetries: 10, Allowed: []string{"guest"}})
	policyWrites.Wait()
	converged("policies(set-after-delete,node) should sync to pivot+node", func() bool {
		return policiesMaxRetries(true, 10) && policiesMaxRetries(false, 10)
	})

	// Verify policies synced to pivot
	pivotPolicies = ops.getPolicies(t, true)
	require.Equal(t, 10, pivotPolicies.MaxRetries)
	require.Equal(t, []string{"guest"}, pivotPolicies.Allowed)

	// Delete policies from node - one authStorage write per side (now absent).
	policyWrites.Add(2)
	ops.deletePolicies(t, false)
	policyWrites.Wait()
	converged("policies(delete,node) should sync to pivot+node", func() bool {
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

	// Push item to pivot - one delivery to each items sub (now 1 item "p1").
	deliv.Add(2)
	itemID := ops.pushItem(t, true, "items/cat/sub/*", Item{Name: "p1", Value: 1})
	deliv.Wait()
	converged("item p1 (pivot push) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotItems) == 1 && pivotItems[0].Data.Name == "p1" &&
			len(nodeItems) == 1 && nodeItems[0].Data.Name == "p1"
	})

	// Push item from node - one delivery to each items sub (now 2 items).
	deliv.Add(2)
	itemID2 := ops.pushItem(t, false, "items/cat/sub/*", Item{Name: "p2", Value: 2})
	deliv.Wait()
	converged("item p2 (node push) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotItems) == 2 && len(nodeItems) == 2
	})

	// Update item on pivot - one delivery to each items sub (new name).
	deliv.Add(2)
	ops.setItem(t, true, itemID, Item{Name: "p1-updated", Value: 10})
	deliv.Wait()
	converged("item p1 update (pivot) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return itemNamed(pivotItems, key.LastIndex(itemID), "p1-updated") &&
			itemNamed(nodeItems, key.LastIndex(itemID), "p1-updated")
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

	// Update item from node - one delivery to each items sub (new name).
	deliv.Add(2)
	ops.setItem(t, false, itemID2, Item{Name: "p2-updated", Value: 20})
	deliv.Wait()
	converged("item p2 update (node) should sync to pivot+node", func() bool {
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

	// Delete item from pivot - one delivery to each sub (now 1 item "p2-updated").
	deliv.Add(2)
	ops.deleteItem(t, true, itemID)
	deliv.Wait()
	converged("item p1 delete (pivot) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotItems) == 1 && pivotItems[0].Data.Name == "p2-updated" &&
			len(nodeItems) == 1 && nodeItems[0].Data.Name == "p2-updated"
	})

	// Delete item from node - one delivery to each items sub (now 0 items).
	deliv.Add(2)
	ops.deleteItem(t, false, itemID2)
	deliv.Wait()
	converged("item p2 delete (node) should sync to pivot+node", func() bool {
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
		// things/<special-id> writes land on the things/* subs (pivot + node).
		deliv.Add(2)
		ops.setThing(t, true, id, Thing{IP: "10.1.1.1", Port: 0, On: true})
		deliv.Wait()
		converged(fmt.Sprintf("things/%s (pivot set) should sync to pivot+node", id), func() bool {
			mu.Lock()
			defer mu.Unlock()
			return findThing(pivotThings, id) && findThing(nodeThings, id)
		})

		deliv.Add(2)
		ops.deleteThing(t, true, id)
		deliv.Wait()
		converged(fmt.Sprintf("things/%s delete should sync to pivot+node", id), func() bool {
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
	deliv.Add(2)
	ops.setItem(t, true, specialItemKey, Item{Name: "special-1", Value: 100})
	deliv.Wait()
	converged("special item (pivot set) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotSpecialItems) == 1 && pivotSpecialItems[0].Data.Name == "special-1" &&
			len(nodeSpecialItems) == 1 && nodeSpecialItems[0].Data.Name == "special-1"
	})

	// Node-side update: exercises pivot's widened {index} regex via the
	// node→leader POST URL /pivot/items/cat-1/sub.v2/file_name. The pivot
	// Set handler accepts the path, writes locally, and the node sees the
	// confirmation via its WS sub.
	deliv.Add(2)
	ops.setItem(t, false, specialItemKey, Item{Name: "special-2", Value: 200})
	deliv.Wait()
	converged("special item (node update) should sync to pivot+node", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(pivotSpecialItems) == 1 && pivotSpecialItems[0].Data.Name == "special-2" &&
			len(nodeSpecialItems) == 1 && nodeSpecialItems[0].Data.Name == "special-2"
	})

	// Node-side delete: exercises the widened {index} regex on the DELETE
	// route too (/pivot/<base>/{index}/{time}).
	deliv.Add(2)
	ops.deleteItem(t, false, specialItemKey)
	deliv.Wait()
	converged("special item (node delete) should sync to pivot+node", func() bool {
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
