package pivot

// Regression test for the instances-map leak: every Setup adds an entry to
// the package-level registry; Shutdown / server.Close() must remove it so
// that a process recreating servers (tests, supervised restarts) doesn't
// retain dead Instance pointers for the rest of its life.

import (
	"testing"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/gorilla/mux"
)

func TestInstanceRegistryRemovedOnShutdown(t *testing.T) {
	monotonic.Init()
	server := &ooo.Server{
		Router:  mux.NewRouter(),
		Silence: true,
		Storage: storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()}),
	}
	Setup(server, Config{
		Keys:       []Key{{Path: "items/*"}},
		ClusterURL: "",
	})
	server.Start("localhost:0")

	// After Setup the registry must contain this server.
	instancesMu.RLock()
	_, present := instances[server]
	instancesMu.RUnlock()
	if !present {
		t.Fatalf("expected instances[server] to be set after Setup")
	}

	// Close drives RegisterPreClose, which must drop the registry entry.
	server.Close(nil)

	instancesMu.RLock()
	_, stillPresent := instances[server]
	instancesMu.RUnlock()
	if stillPresent {
		t.Fatalf("instances[server] still set after Close — registry leaks one entry per server lifecycle")
	}
}
