package pivot_test

import (
	"bytes"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	ooio "github.com/benitogf/ooo/io"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/goccy/go-json"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// ClusterUser represents a user for auth sync testing
type ClusterUser struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// ClusterPolicy represents a policy for auth sync testing
type ClusterPolicy struct {
	MaxRetries int      `json:"maxRetries"`
	Allowed    []string `json:"allowed"`
}

// ClusterDevice represents a device/node entry
type ClusterDevice struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
	Name string `json:"name"`
}

func clusterTestClient() *http.Client {
	return &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 5 * time.Second,
			}).Dial,
			MaxConnsPerHost:   3000,
			DisableKeepAlives: true,
		},
	}
}

// multiClusterTestOps provides operations that can be local or remote based on the useRemote flag
type multiClusterTestOps struct {
	useRemote      bool
	authServer     *ooo.Server
	devicePivot    *ooo.Server
	nodeDevice     *ooo.Server
	authCfg        ooio.RemoteConfig
	devicePivotCfg ooio.RemoteConfig
	nodeCfg        ooio.RemoteConfig
}

// === Device operations (via OpenFilter - direct storage access) ===

func (ops *multiClusterTestOps) pushDevice(t *testing.T, toDevicePivot bool, device ClusterDevice) string {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if toDevicePivot {
			cfg = ops.devicePivotCfg
		}
		resp, err := ooio.RemotePushWithResponse(cfg, "devices/*", device)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Index)
		return resp.Index
	}
	server := ops.nodeDevice
	if toDevicePivot {
		server = ops.devicePivot
	}
	id, err := ooo.Push(server, "devices/*", device)
	require.NoError(t, err)
	require.NotEmpty(t, id)
	return id
}

func (ops *multiClusterTestOps) getDevice(t *testing.T, from string, id string) ClusterDevice {
	server := ops.selectServer(from)
	if ops.useRemote {
		cfg := ops.selectConfig(from)
		result, err := ooio.RemoteGet[ClusterDevice](cfg, "devices/"+id)
		require.NoError(t, err)
		return result.Data
	}
	result, err := ooo.Get[ClusterDevice](server, "devices/"+id)
	require.NoError(t, err)
	return result.Data
}

func (ops *multiClusterTestOps) getDeviceSafe(from string, id string) (ClusterDevice, error) {
	server := ops.selectServer(from)
	if ops.useRemote {
		cfg := ops.selectConfig(from)
		result, err := ooio.RemoteGet[ClusterDevice](cfg, "devices/"+id)
		if err != nil {
			return ClusterDevice{}, err
		}
		return result.Data, nil
	}
	result, err := ooo.Get[ClusterDevice](server, "devices/"+id)
	if err != nil {
		return ClusterDevice{}, err
	}
	return result.Data, nil
}

func (ops *multiClusterTestOps) setDevice(t *testing.T, toDevicePivot bool, id string, device ClusterDevice) {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if toDevicePivot {
			cfg = ops.devicePivotCfg
		}
		err := ooio.RemoteSet(cfg, "devices/"+id, device)
		require.NoError(t, err)
		return
	}
	server := ops.nodeDevice
	if toDevicePivot {
		server = ops.devicePivot
	}
	err := ooo.Set(server, "devices/"+id, device)
	require.NoError(t, err)
}

func (ops *multiClusterTestOps) deleteDevice(t *testing.T, fromDevicePivot bool, id string) {
	if ops.useRemote {
		cfg := ops.nodeCfg
		if fromDevicePivot {
			cfg = ops.devicePivotCfg
		}
		err := ooio.RemoteDelete(cfg, "devices/"+id)
		require.NoError(t, err)
		return
	}
	server := ops.nodeDevice
	if fromDevicePivot {
		server = ops.devicePivot
	}
	err := ooo.Delete(server, "devices/"+id)
	require.NoError(t, err)
}

func (ops *multiClusterTestOps) getDeviceExpectError(t *testing.T, from string, id string, msg string) {
	server := ops.selectServer(from)
	if ops.useRemote {
		cfg := ops.selectConfig(from)
		_, err := ooio.RemoteGet[ClusterDevice](cfg, "devices/"+id)
		require.Error(t, err, msg)
		return
	}
	_, err := ooo.Get[ClusterDevice](server, "devices/"+id)
	require.Error(t, err, msg)
}

// === User operations (via custom endpoints - NOT OpenFilter) ===

func (ops *multiClusterTestOps) setUser(t *testing.T, toAuth bool, id string, user ClusterUser) {
	server := ops.nodeDevice
	if toAuth {
		server = ops.authServer
	}
	data, err := json.Marshal(user)
	require.NoError(t, err)
	resp, err := server.Client.Post("http://"+server.Address+"/users/"+id, "application/json", bytes.NewBuffer(data))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func (ops *multiClusterTestOps) getUser(t *testing.T, from string, id string) ClusterUser {
	server := ops.selectServer(from)
	resp, err := server.Client.Get("http://" + server.Address + "/users/" + id)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var user ClusterUser
	err = json.NewDecoder(resp.Body).Decode(&user)
	require.NoError(t, err)
	return user
}

func (ops *multiClusterTestOps) getUserExpectError(t *testing.T, from string, id string, msg string) {
	server := ops.selectServer(from)
	resp, err := server.Client.Get("http://" + server.Address + "/users/" + id)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotEqual(t, http.StatusOK, resp.StatusCode, msg)
}

func (ops *multiClusterTestOps) deleteUser(t *testing.T, fromAuth bool, id string) {
	server := ops.nodeDevice
	if fromAuth {
		server = ops.authServer
	}
	req, err := http.NewRequest(http.MethodDelete, "http://"+server.Address+"/users/"+id, nil)
	require.NoError(t, err)
	resp, err := server.Client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// === Policy operations (via custom endpoints - NOT OpenFilter) ===

func (ops *multiClusterTestOps) setPolicy(t *testing.T, toAuth bool, policy ClusterPolicy) {
	server := ops.nodeDevice
	if toAuth {
		server = ops.authServer
	}
	data, err := json.Marshal(policy)
	require.NoError(t, err)
	resp, err := server.Client.Post("http://"+server.Address+"/policies", "application/json", bytes.NewBuffer(data))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func (ops *multiClusterTestOps) getPolicy(t *testing.T, from string) ClusterPolicy {
	server := ops.selectServer(from)
	resp, err := server.Client.Get("http://" + server.Address + "/policies")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var policy ClusterPolicy
	err = json.NewDecoder(resp.Body).Decode(&policy)
	require.NoError(t, err)
	return policy
}

func (ops *multiClusterTestOps) getPolicyExpectError(t *testing.T, from string, msg string) {
	server := ops.selectServer(from)
	resp, err := server.Client.Get("http://" + server.Address + "/policies")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotEqual(t, http.StatusOK, resp.StatusCode, msg)
}

func (ops *multiClusterTestOps) deletePolicy(t *testing.T, fromAuth bool) {
	server := ops.nodeDevice
	if fromAuth {
		server = ops.authServer
	}
	req, err := http.NewRequest(http.MethodDelete, "http://"+server.Address+"/policies", nil)
	require.NoError(t, err)
	resp, err := server.Client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func (ops *multiClusterTestOps) selectServer(from string) *ooo.Server {
	switch from {
	case "auth":
		return ops.authServer
	case "devicePivot":
		return ops.devicePivot
	case "node":
		return ops.nodeDevice
	default:
		return ops.nodeDevice
	}
}

func (ops *multiClusterTestOps) selectConfig(from string) ooio.RemoteConfig {
	switch from {
	case "auth":
		return ops.authCfg
	case "devicePivot":
		return ops.devicePivotCfg
	case "node":
		return ops.nodeCfg
	default:
		return ops.nodeCfg
	}
}

// addUserEndpoints adds custom /users/:id endpoints to a server
// users/* is NOT exposed via OpenFilter - only through these endpoints
func addUserEndpoints(server *ooo.Server, userStorage storage.Database) {
	server.Router.HandleFunc("/users/{id}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		id := vars["id"]
		switch r.Method {
		case http.MethodGet:
			obj, err := userStorage.Get("users/" + id)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(obj.Data)
		case http.MethodPost:
			var user ClusterUser
			if err := json.NewDecoder(r.Body).Decode(&user); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			data, err := json.Marshal(user)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if _, err := userStorage.Set("users/"+id, data); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			if err := userStorage.Del("users/" + id); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}).Methods(http.MethodGet, http.MethodPost, http.MethodDelete)
}

// addPolicyEndpoints adds custom /policies endpoint to a server
// policies is NOT exposed via OpenFilter - only through this endpoint
func addPolicyEndpoints(server *ooo.Server, policyStorage storage.Database) {
	server.Router.HandleFunc("/policies", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			obj, err := policyStorage.Get("policies")
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(obj.Data)
		case http.MethodPost:
			var policy ClusterPolicy
			if err := json.NewDecoder(r.Body).Decode(&policy); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			data, err := json.Marshal(policy)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if _, err := policyStorage.Set("policies", data); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			if err := policyStorage.Del("policies"); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}).Methods(http.MethodGet, http.MethodPost, http.MethodDelete)
}

// MultiClusterAuthServer creates the authentication server:
// - IS pivot for users/* and policies (Local=true) - accessed via custom endpoints
// - IS node for devices/* (syncs from deviceClusterURL) - accessed via OpenFilter
func MultiClusterAuthServer(t *testing.T, deviceClusterURL string, afterAuthWrite func(key string)) *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Client = clusterTestClient()
	server.Audit = func(r *http.Request) bool { return true }

	// Create separate storage for users and policies (external storage pattern)
	authStorage := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})

	// Configure pivot synchronization
	// Auth server IS pivot for users/* and policies (Local=true)
	// Auth server IS node for devices/* (syncs from deviceClusterURL)
	config := pivot.Config{
		Keys: []pivot.Key{
			{Path: "users/*", Database: authStorage, Local: true},
			{Path: "policies", Database: authStorage, Local: true},
			{Path: "devices/*"},
		},
		NodesKey:   "devices/*",
		ClusterURL: deviceClusterURL,
	}

	pivot.Setup(server, config)

	// Attach auth storage with AfterWrite callback for test synchronization
	err := pivot.GetInstance(server).Attach(authStorage, storage.Options{AfterWrite: afterAuthWrite})
	require.NoError(t, err)

	// Only devices/* is exposed via OpenFilter (for WebSocket subscriptions)
	server.OpenFilter("devices/*")

	// Add custom endpoints for users and policies (NOT exposed via OpenFilter)
	addUserEndpoints(server, authStorage)
	addPolicyEndpoints(server, authStorage)

	server.Start("localhost:0")
	return server
}

// MultiClusterDevicePivot creates the device pivot server:
// - IS pivot for devices/* - accessed via OpenFilter
// Use AddExtraNodeURL after creation to add auth servers that should receive sync notifications
func MultiClusterDevicePivot(t *testing.T) *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Client = clusterTestClient()
	server.Audit = func(r *http.Request) bool { return true }

	config := pivot.Config{
		Keys: []pivot.Key{
			{Path: "devices/*"},
		},
		NodesKey:   "devices/*",
		ClusterURL: "",
	}

	pivot.Setup(server, config)
	server.OpenFilter("devices/*")
	server.Start("localhost:0")
	return server
}

// MultiClusterNodeDevice creates a node device:
// - IS node for users/* and policies (syncs from authServerURL) - accessed via custom endpoints
// - IS node for devices/* (syncs from deviceClusterURL) - accessed via OpenFilter
func MultiClusterNodeDevice(t *testing.T, authServerURL string, deviceClusterURL string, afterAuthWrite func(key string)) *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Client = clusterTestClient()
	server.Audit = func(r *http.Request) bool { return true }

	// Create separate storage for users and policies
	authStorage := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})

	// Configure pivot synchronization
	// Node syncs users/* and policies from authServerURL
	// Node syncs devices/* from deviceClusterURL
	config := pivot.Config{
		Keys: []pivot.Key{
			{Path: "users/*", Database: authStorage, ClusterURL: authServerURL},
			{Path: "policies", Database: authStorage, ClusterURL: authServerURL},
			{Path: "devices/*"},
		},
		NodesKey:   "devices/*",
		ClusterURL: deviceClusterURL,
	}

	pivot.Setup(server, config)

	// Attach auth storage with AfterWrite callback
	err := pivot.GetInstance(server).Attach(authStorage, storage.Options{AfterWrite: afterAuthWrite})
	require.NoError(t, err)

	// Only devices/* is exposed via OpenFilter
	server.OpenFilter("devices/*")

	// Add custom endpoints for users and policies
	addUserEndpoints(server, authStorage)
	addPolicyEndpoints(server, authStorage)

	server.Start("localhost:0")
	return server
}

func testMultiClusterSync(t *testing.T, useRemote bool) {
	var authWg, nodeWg sync.WaitGroup // Storage callbacks for auth data (users/policies)

	// Auth storage callbacks - only for users/* and policies
	authAfterWrite := func(key string) {
		if strings.HasPrefix(key, "pivot/") {
			return
		}
		t.Logf("[auth] storage write: %s", key)
		authWg.Done()
	}

	nodeAfterWrite := func(key string) {
		if strings.HasPrefix(key, "pivot/") {
			return
		}
		t.Logf("[node] storage write: %s", key)
		nodeWg.Done()
	}

	// Create device pivot (IS cluster leader for devices/*)
	devicePivot := MultiClusterDevicePivot(t)
	defer devicePivot.Close(os.Interrupt)

	// Create auth server (IS cluster leader for users/policies, IS node for devices)
	authServer := MultiClusterAuthServer(t, devicePivot.Address, authAfterWrite)
	defer authServer.Close(os.Interrupt)

	// Add auth server to devicePivot's ExtraNodeURLs - auth receives sync notifications
	// but is NOT registered in devices/* (nodesKey)
	pivot.GetInstance(devicePivot).AddExtraNodeURL(authServer.Address)

	// Verify ExtraNodeURLs was added correctly
	extraURLs := pivot.GetInstance(devicePivot).GetExtraNodeURLs()
	require.Equal(t, 1, len(extraURLs), "should have 1 ExtraNodeURL")
	require.Equal(t, authServer.Address, extraURLs[0], "ExtraNodeURL should be auth server")

	// Verify getNodes returns auth's address
	nodes := pivot.GetInstance(devicePivot).GetNodes()
	require.Contains(t, nodes, authServer.Address, "getNodes should include auth from ExtraNodeURLs")

	// Create node device (IS node for users/policies from auth, IS node for devices from devicePivot)
	nodeDevice := MultiClusterNodeDevice(t, authServer.Address, devicePivot.Address, nodeAfterWrite)
	defer nodeDevice.Close(os.Interrupt)

	ops := &multiClusterTestOps{
		useRemote:   useRemote,
		authServer:  authServer,
		devicePivot: devicePivot,
		nodeDevice:  nodeDevice,
	}
	if useRemote {
		ops.authCfg = ooio.RemoteConfig{Client: &http.Client{Timeout: 5 * time.Second}, Host: authServer.Address}
		ops.devicePivotCfg = ooio.RemoteConfig{Client: &http.Client{Timeout: 5 * time.Second}, Host: devicePivot.Address}
		ops.nodeCfg = ooio.RemoteConfig{Client: &http.Client{Timeout: 5 * time.Second}, Host: nodeDevice.Address}
	}

	// Register node device in devices/* (nodesKey)
	nodeIP, nodePort, _ := net.SplitHostPort(nodeDevice.Address)
	nodePortInt, _ := strconv.Atoi(nodePort)
	t.Logf("Registering node-device to devicePivot")
	nodeDeviceID, err := ooo.Push(devicePivot, "devices/*", ClusterDevice{IP: nodeIP, Port: nodePortInt, Name: "node-device"})
	require.NoError(t, err)
	// Wait for sync by polling auth
	require.Eventually(t, func() bool {
		_, err := ops.getDeviceSafe("auth", nodeDeviceID)
		return err == nil
	}, 2*time.Second, 10*time.Millisecond, "node-device should sync to auth")
	t.Logf("node-device registered")

	// Push sensor device - now node is registered in nodesKey
	t.Logf("Pushing sensor1 to devicePivot")
	deviceID := ops.pushDevice(t, true, ClusterDevice{IP: "10.0.0.100", Port: 9000, Name: "sensor1"})
	// Wait for sync to all servers by polling
	require.Eventually(t, func() bool {
		_, err := ops.getDeviceSafe("auth", deviceID)
		return err == nil
	}, 2*time.Second, 10*time.Millisecond, "sensor1 should sync to auth")
	require.Eventually(t, func() bool {
		_, err := ops.getDeviceSafe("node", deviceID)
		return err == nil
	}, 2*time.Second, 10*time.Millisecond, "sensor1 should sync to node")
	t.Logf("sensor1 pushed")

	// Verify device on pivot
	pivotDevice := ops.getDevice(t, "devicePivot", deviceID)
	require.Equal(t, "sensor1", pivotDevice.Name)

	// Verify device synced to auth (via ExtraNodeURLs)
	authDevice := ops.getDevice(t, "auth", deviceID)
	require.Equal(t, "sensor1", authDevice.Name)

	// Verify device synced to node (via nodesKey)
	nodeDeviceData := ops.getDevice(t, "node", deviceID)
	require.Equal(t, "sensor1", nodeDeviceData.Name)

	// === Test 2: Update device on devicePivot ===
	ops.setDevice(t, true, deviceID, ClusterDevice{IP: "10.0.0.100", Port: 9000, Name: "sensor1-updated"})
	// Wait for sync by polling
	require.Eventually(t, func() bool {
		d, err := ops.getDeviceSafe("auth", deviceID)
		return err == nil && d.Name == "sensor1-updated"
	}, 2*time.Second, 10*time.Millisecond, "sensor1-updated should sync to auth")
	require.Eventually(t, func() bool {
		d, err := ops.getDeviceSafe("node", deviceID)
		return err == nil && d.Name == "sensor1-updated"
	}, 2*time.Second, 10*time.Millisecond, "sensor1-updated should sync to node")

	pivotDevice = ops.getDevice(t, "devicePivot", deviceID)
	require.Equal(t, "sensor1-updated", pivotDevice.Name)
	authDevice = ops.getDevice(t, "auth", deviceID)
	require.Equal(t, "sensor1-updated", authDevice.Name)
	nodeDeviceData = ops.getDevice(t, "node", deviceID)
	require.Equal(t, "sensor1-updated", nodeDeviceData.Name)

	// === Test 3: Set user on auth (auth IS cluster leader for users) ===
	// Auth writes user, syncs to node -> node writes user
	authWg.Add(1)
	nodeWg.Add(1)
	ops.setUser(t, true, "alice", ClusterUser{Name: "Alice", Email: "alice@test.com"})
	authWg.Wait()
	nodeWg.Wait()

	authUser := ops.getUser(t, "auth", "alice")
	require.Equal(t, "Alice", authUser.Name)
	nodeUser := ops.getUser(t, "node", "alice")
	require.Equal(t, "Alice", nodeUser.Name)

	// === Test 4: Update user on node ===
	// Node writes user, syncs to auth -> auth writes user
	nodeWg.Add(1)
	authWg.Add(1)
	ops.setUser(t, false, "alice", ClusterUser{Name: "Alice Updated", Email: "alice-new@test.com"})
	nodeWg.Wait()
	authWg.Wait()

	authUser = ops.getUser(t, "auth", "alice")
	require.Equal(t, "Alice Updated", authUser.Name)
	nodeUser = ops.getUser(t, "node", "alice")
	require.Equal(t, "Alice Updated", nodeUser.Name)

	// === Test 5: Set policy on auth ===
	// Auth writes policy, syncs to node -> node writes policy
	authWg.Add(1)
	nodeWg.Add(1)
	ops.setPolicy(t, true, ClusterPolicy{MaxRetries: 3, Allowed: []string{"read", "write"}})
	authWg.Wait()
	nodeWg.Wait()

	authPolicy := ops.getPolicy(t, "auth")
	require.Equal(t, 3, authPolicy.MaxRetries)
	nodePolicy := ops.getPolicy(t, "node")
	require.Equal(t, 3, nodePolicy.MaxRetries)

	// === Test 6: Update policy on node ===
	// Node writes policy, syncs to auth -> auth writes policy
	nodeWg.Add(1)
	authWg.Add(1)
	ops.setPolicy(t, false, ClusterPolicy{MaxRetries: 5, Allowed: []string{"admin"}})
	nodeWg.Wait()
	authWg.Wait()

	authPolicy = ops.getPolicy(t, "auth")
	require.Equal(t, 5, authPolicy.MaxRetries)
	require.Equal(t, []string{"admin"}, authPolicy.Allowed)
	nodePolicy = ops.getPolicy(t, "node")
	require.Equal(t, 5, nodePolicy.MaxRetries)

	// === Test 7: Delete device on devicePivot ===
	ops.deleteDevice(t, true, deviceID)
	// Wait for delete sync by polling
	require.Eventually(t, func() bool {
		_, err := ops.getDeviceSafe("auth", deviceID)
		return err != nil
	}, 2*time.Second, 10*time.Millisecond, "device should be deleted from auth")
	require.Eventually(t, func() bool {
		_, err := ops.getDeviceSafe("node", deviceID)
		return err != nil
	}, 2*time.Second, 10*time.Millisecond, "device should be deleted from node")
	ops.getDeviceExpectError(t, "devicePivot", deviceID, "device should be deleted from devicePivot")

	// === Test 8: Push device from node ===
	deviceID2 := ops.pushDevice(t, false, ClusterDevice{IP: "10.0.0.2", Port: 9000, Name: "sensor2"})
	// Wait for sync to pivot and auth by polling
	require.Eventually(t, func() bool {
		_, err := ops.getDeviceSafe("devicePivot", deviceID2)
		return err == nil
	}, 2*time.Second, 10*time.Millisecond, "sensor2 should sync to pivot")
	require.Eventually(t, func() bool {
		_, err := ops.getDeviceSafe("auth", deviceID2)
		return err == nil
	}, 2*time.Second, 10*time.Millisecond, "sensor2 should sync to auth")
	pivotDevice = ops.getDevice(t, "devicePivot", deviceID2)
	require.Equal(t, "sensor2", pivotDevice.Name)

	// === Test 9: Delete user on auth ===
	// Auth deletes user, syncs to node -> node deletes user
	authWg.Add(1)
	nodeWg.Add(1)
	ops.deleteUser(t, true, "alice")
	authWg.Wait()
	nodeWg.Wait()
	ops.getUserExpectError(t, "auth", "alice", "user should be deleted from auth")
	ops.getUserExpectError(t, "node", "alice", "user should be deleted from node")

	// === Test 10: Delete policy on node ===
	// Node deletes policy, syncs to auth -> auth deletes policy
	nodeWg.Add(1)
	authWg.Add(1)
	ops.deletePolicy(t, false)
	nodeWg.Wait()
	authWg.Wait()
	ops.getPolicyExpectError(t, "auth", "policy should be deleted from auth")
	ops.getPolicyExpectError(t, "node", "policy should be deleted from node")

	// === Test 11: Clean up ===
	ops.deleteDevice(t, false, deviceID2)
	// Wait for delete sync by polling
	require.Eventually(t, func() bool {
		_, err := ops.getDeviceSafe("devicePivot", deviceID2)
		return err != nil
	}, 2*time.Second, 10*time.Millisecond, "sensor2 should be deleted from pivot")
	require.Eventually(t, func() bool {
		_, err := ops.getDeviceSafe("auth", deviceID2)
		return err != nil
	}, 2*time.Second, 10*time.Millisecond, "sensor2 should be deleted from auth")
}

func TestMultiClusterSync_Local(t *testing.T) {
	testMultiClusterSync(t, false)
}

func TestMultiClusterSync_Remote(t *testing.T) {
	testMultiClusterSync(t, true)
}

// TestMultiCluster_ValidationPanics tests that invalid configurations panic
func TestMultiCluster_ValidationPanics(t *testing.T) {
	t.Run("Local and ClusterURL both set panics", func(t *testing.T) {
		server := &ooo.Server{}
		server.Silence = true
		server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
		server.Router = mux.NewRouter()

		require.Panics(t, func() {
			pivot.Setup(server, pivot.Config{
				Keys: []pivot.Key{
					{Path: "users/*", Local: true, ClusterURL: "localhost:8080"},
				},
				ClusterURL: "localhost:9090",
			})
		}, "Should panic when both Local and ClusterURL are set")
	})

	t.Run("Key ClusterURL without Config ClusterURL panics", func(t *testing.T) {
		server := &ooo.Server{}
		server.Silence = true
		server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
		server.Router = mux.NewRouter()

		require.Panics(t, func() {
			pivot.Setup(server, pivot.Config{
				Keys: []pivot.Key{
					{Path: "users/*", ClusterURL: "localhost:8080"},
				},
				ClusterURL: "",
			})
		}, "Should panic when Key.ClusterURL is set but Config.ClusterURL is empty")
	})
}

// TestMultiCluster_RoleDetection tests that GetPivotInfo correctly identifies roles
func TestMultiCluster_RoleDetection(t *testing.T) {
	t.Run("Mixed role - auth server", func(t *testing.T) {
		devicePivot := MultiClusterDevicePivot(t)
		defer devicePivot.Close(os.Interrupt)

		authServer := MultiClusterAuthServer(t, devicePivot.Address, nil)
		defer authServer.Close(os.Interrupt)

		info := pivot.GetPivotInfo(authServer)()
		require.NotNil(t, info)
		require.Equal(t, "mixed", info.Role)
	})

	t.Run("Pure pivot role - device pivot", func(t *testing.T) {
		devicePivot := MultiClusterDevicePivot(t)
		defer devicePivot.Close(os.Interrupt)

		info := pivot.GetPivotInfo(devicePivot)()
		require.NotNil(t, info)
		require.Equal(t, "pivot", info.Role)
	})

	t.Run("Pure node role - node device", func(t *testing.T) {
		devicePivot := MultiClusterDevicePivot(t)
		defer devicePivot.Close(os.Interrupt)

		authServer := MultiClusterAuthServer(t, devicePivot.Address, nil)
		defer authServer.Close(os.Interrupt)

		nodeDevice := MultiClusterNodeDevice(t, authServer.Address, devicePivot.Address, nil)
		defer nodeDevice.Close(os.Interrupt)

		info := pivot.GetPivotInfo(nodeDevice)()
		require.NotNil(t, info)
		require.Equal(t, "node", info.Role)
	})
}
