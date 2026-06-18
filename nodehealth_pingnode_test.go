package pivot

import (
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/storage"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// TestPingNodeProbesUnauthedEndpoint pins that pingNode can reach a hardened
// node — one whose request gate denies the ooo root — by probing the
// pivot-internal /_pivot/version route, which the gate leaves reachable.
//
// Before the fix, pingNode issued GET "/" against the ooo root. The root is
// served by the UI handler, which returns 401 whenever the operator's gate
// denies the request. Every health check against a hardened node therefore
// failed, and the node was permanently marked unhealthy. The probe now targets
// /_pivot/version, which the gate exempts. (GET, not HEAD, so the probe stays
// compatible with older nodes whose route registered GET only.)
func TestPingNodeProbesUnauthedEndpoint(t *testing.T) {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Client = &http.Client{Timeout: 500 * time.Millisecond}
	// Hardened node: a gate middleware denies everything except the
	// pivot-internal routes, so the ooo root ("/") returns 401 while
	// /_pivot/version stays reachable (the probe endpoint pingNode must use).
	server.Router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, RoutePrefix+"/") {
				next.ServeHTTP(w, r)
				return
			}
			w.WriteHeader(http.StatusUnauthorized)
		})
	})

	Setup(server, Config{Keys: []Key{{Path: "items/*"}}, NodesKey: "nodes/*"})
	server.Start("localhost:0")
	defer server.Close(os.Interrupt)

	// Premise check: the ooo root IS behind the gate, so a direct GET "/" on
	// this hardened server is rejected. If this ever stops being 401, the bug
	// this test guards against no longer exists.
	rootResp, err := http.Get("http://" + server.Address + "/")
	require.NoError(t, err)
	rootResp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, rootResp.StatusCode,
		"premise: the ooo root must be gated on a hardened node")

	nh := NewNodeHealth(nil)
	defer nh.Stop()

	require.True(t, nh.pingNode(server.Address),
		"pingNode must reach a hardened node via the gate-exempt /_pivot/version route")
}
