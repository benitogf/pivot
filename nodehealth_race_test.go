package pivot_test

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// TestRace_BackgroundHealthCheck_StorageInit tests that the background health check
// does not race with storage initialization.
//
// BUG FIXED: StartBackgroundCheck was called in Setup() before server.Start(),
// causing getNodes() to race with storage.Start() writing to the active field.
//
// FIX: Moved StartBackgroundCheck into the OnStart callback so it only runs
// after storage is fully initialized.
//
// Run with: go test -race -run TestRace_BackgroundHealthCheck_StorageInit
func TestRace_BackgroundHealthCheck_StorageInit(t *testing.T) {
	for i := 0; i < 10; i++ {
		server := &ooo.Server{}
		server.Silence = true
		server.Static = true
		server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
		server.Router = mux.NewRouter()
		server.OpenFilter("things/*")

		config := pivot.Config{
			Keys:       []pivot.Key{{Path: "things/*"}},
			NodesKey:   "things/*",
			ClusterURL: "", // Pivot server - creates NodeHealth with background check
		}

		// Setup creates NodeHealth and (with fix) defers StartBackgroundCheck to OnStart
		pivot.Setup(server, config)

		// Start the server - initializes storage and calls OnStart
		// Without the fix, getNodes() in background goroutine races with storage.Start()
		server.Start("localhost:0")

		// Give background check time to run at least once
		time.Sleep(10 * time.Millisecond)

		server.Close(os.Interrupt)
	}
}

// TestRace_NodeHealth_StopWaitsForGoroutine tests that Stop() waits for the background
// goroutine to finish before returning.
//
// BUG FIXED: Stop() returned immediately after closing stopChan, allowing the
// goroutine to continue accessing resources (like server.Storage) during shutdown,
// which caused races with server.Close() clearing those resources.
//
// FIX: Added WaitGroup to track the background goroutine. Stop() now waits for
// the goroutine to exit via wg.Wait().
//
// This test uses a barrier pattern to deterministically verify Stop() blocks
// until the goroutine exits.
func TestRace_NodeHealth_StopWaitsForGoroutine(t *testing.T) {
	// Use channels as barriers to control goroutine execution
	enteredGetNodes := make(chan struct{}) // Signals goroutine entered getNodes
	releaseGetNodes := make(chan struct{}) // Signals goroutine can exit getNodes

	nh := pivot.NewNodeHealth(nil)

	// Create getNodes that signals when entered and blocks until released
	getNodes := func() []string {
		// Signal that we've entered getNodes
		close(enteredGetNodes)
		// Block until released
		<-releaseGetNodes
		return []string{}
	}

	// Start background check
	nh.StartBackgroundCheck(getNodes, time.Hour) // Long interval - only initial check runs

	// Wait for goroutine to enter getNodes (this ensures it's past the stopChan check)
	select {
	case <-enteredGetNodes:
		// Good - goroutine is now blocked in getNodes
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Goroutine did not enter getNodes in time")
	}

	// Now call Stop() in a goroutine - it should block because goroutine is in getNodes
	stopReturned := make(chan struct{})
	go func() {
		nh.Stop()
		close(stopReturned)
	}()

	// Give Stop() a moment to start waiting on WaitGroup
	time.Sleep(10 * time.Millisecond)

	// Verify Stop() hasn't returned yet (goroutine is blocked in getNodes)
	select {
	case <-stopReturned:
		t.Fatal("Stop() returned before goroutine exited - WaitGroup not working")
	default:
		// Good - Stop() is still waiting
	}

	// Release the goroutine - it should exit, then Stop() should return
	close(releaseGetNodes)

	// Wait for Stop() to return with timeout
	select {
	case <-stopReturned:
		// Good - Stop() returned after goroutine exited
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Stop() did not return after goroutine was unblocked")
	}
}

// TestRace_NodeHealth_RapidStartStop tests rapid start/stop cycles to ensure
// no races occur during the lifecycle of NodeHealth.
//
// Run with: go test -race -run TestRace_NodeHealth_RapidStartStop
func TestRace_NodeHealth_RapidStartStop(t *testing.T) {
	for i := 0; i < 20; i++ {
		nh := pivot.NewNodeHealth(nil)

		var callCount atomic.Int32
		getNodes := func() []string {
			callCount.Add(1)
			return []string{"node1:8080", "node2:8080"}
		}

		// Start and immediately stop
		nh.StartBackgroundCheck(getNodes, time.Millisecond)
		nh.Stop()

		// After Stop(), no more calls should occur
		countAfterStop := callCount.Load()
		time.Sleep(5 * time.Millisecond)
		require.Equal(t, countAfterStop, callCount.Load(),
			"getNodes called after Stop() returned")
	}
}

// TestRace_PivotServer_RapidStartStop tests rapid pivot server start/stop cycles
// to ensure no races occur during the full server lifecycle with NodeHealth.
//
// Run with: go test -race -run TestRace_PivotServer_RapidStartStop
func TestRace_PivotServer_RapidStartStop(t *testing.T) {
	for i := 0; i < 5; i++ {
		server := &ooo.Server{}
		server.Silence = true
		server.Static = true
		server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
		server.Router = mux.NewRouter()
		server.OpenFilter("things/*")

		config := pivot.Config{
			Keys:       []pivot.Key{{Path: "things/*"}},
			NodesKey:   "things/*",
			ClusterURL: "", // Pivot server
		}

		pivot.Setup(server, config)
		server.Start("localhost:0")

		// Immediate close - tests that Stop() properly waits for background goroutine
		server.Close(os.Interrupt)
	}
}

// TestRace_ConcurrentHealthAccess tests concurrent access to NodeHealth methods
// while the background check is running. This exercises the mutex protection.
//
// Run with: go test -race -run TestRace_ConcurrentHealthAccess
func TestRace_ConcurrentHealthAccess(t *testing.T) {
	nh := pivot.NewNodeHealth(nil)

	getNodes := func() []string {
		return []string{"node1:8080", "node2:8080", "node3:8080"}
	}

	nh.StartBackgroundCheck(getNodes, 2*time.Millisecond)

	var wg sync.WaitGroup
	done := make(chan struct{})

	// Concurrent readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					nh.IsHealthy("node1:8080")
					nh.GetHealthyNodes([]string{"node1:8080", "node2:8080"})
					nh.GetStatus()
				}
			}
		}()
	}

	// Concurrent writers
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			node := "node1:8080"
			if id == 1 {
				node = "node2:8080"
			} else if id == 2 {
				node = "node3:8080"
			}
			for {
				select {
				case <-done:
					return
				default:
					nh.MarkHealthy(node)
					nh.MarkUnhealthy(node)
				}
			}
		}(i)
	}

	// Let concurrent operations run
	time.Sleep(50 * time.Millisecond)

	close(done)
	wg.Wait()

	// Stop should still work properly after concurrent access
	nh.Stop()
}
