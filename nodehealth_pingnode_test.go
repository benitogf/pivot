package pivot

import (
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/storage"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// TestPingNodeProbesUnauthedEndpoint pins that pingNode can reach a hardened
// node — one whose ooo Audit denies requests — by probing the pivot-internal
// /_pivot/version route, which is registered without the Audit wrapper.
//
// Before the fix, pingNode issued GET "/" against the ooo root. The root is
// served by the UI handler, which returns 401 whenever a non-trivial Audit
// denies the request. Every health check against a hardened node therefore
// failed, and the node was permanently marked unhealthy. The probe now targets
// /_pivot/version, which the Audit gate does not cover. (GET, not HEAD, so the
// probe stays compatible with older nodes whose route registered GET only.)
func TestPingNodeProbesUnauthedEndpoint(t *testing.T) {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Client = &http.Client{Timeout: 500 * time.Millisecond}
	// Hardened node: Audit denies everything, so the ooo root ("/") returns
	// 401. /_pivot/version stays reachable because pivot registers it without
	// the Audit wrapper.
	server.Audit = func(r *http.Request) bool { return false }

	Setup(server, Config{Keys: []Key{{Path: "items/*"}}, NodesKey: "nodes/*"})
	server.Start("localhost:0")
	defer server.Close(os.Interrupt)

	nh := NewNodeHealth(nil)
	defer nh.Stop()

	require.True(t, nh.pingNode(server.Address),
		"pingNode must reach a hardened node via the Audit-exempt /_pivot/version route")
}
