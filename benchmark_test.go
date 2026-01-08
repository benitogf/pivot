package pivot_test

import (
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/key"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/gorilla/mux"
)

// createBenchServer creates a server without pivot synchronization (baseline)
func createBenchServer() *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Client = &http.Client{
		Timeout: time.Millisecond * 100,
		Transport: &http.Transport{
			Dial:              (&net.Dialer{Timeout: 100 * time.Millisecond}).Dial,
			MaxConnsPerHost:   3000,
			DisableKeepAlives: true,
		},
	}
	server.Audit = func(r *http.Request) bool { return true }
	server.OpenFilter("things/*")
	server.OpenFilter("settings")
	server.Start("localhost:0")
	return server
}

// createBenchPivotServer creates a pivot server
// benchClient returns an HTTP client optimized for benchmarks with quick timeouts
func benchClient() *http.Client {
	return &http.Client{
		Timeout: time.Millisecond * 100,
		Transport: &http.Transport{
			DialContext:       (&net.Dialer{Timeout: 100 * time.Millisecond}).DialContext,
			MaxConnsPerHost:   3000,
			DisableKeepAlives: true,
		},
	}
}

func createBenchPivotServer() *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Audit = func(r *http.Request) bool { return true }

	config := pivot.Config{
		Keys:     []pivot.Key{{Path: "settings"}},
		NodesKey: "things/*",
		PivotIP:  "",
		Client:   benchClient(),
	}
	pivot.Setup(server, config)

	server.OpenFilter("things/*")
	server.OpenFilter("settings")
	server.Start("localhost:0")
	return server
}

// createBenchNodeServer creates a node server connected to a pivot
func createBenchNodeServer(pivotAddress string) *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Audit = func(r *http.Request) bool { return true }

	config := pivot.Config{
		Keys:     []pivot.Key{{Path: "settings"}},
		NodesKey: "things/*",
		PivotIP:  pivotAddress,
		Client:   benchClient(),
	}
	pivot.Setup(server, config)

	server.OpenFilter("things/*")
	server.OpenFilter("settings")
	server.Start("localhost:0")
	return server
}

// =============================================================================
// BASELINE BENCHMARKS (no pivot)
// =============================================================================

func BenchmarkBaseline_Write(b *testing.B) {
	server := createBenchServer()
	defer server.Close(os.Interrupt)
	for b.Loop() {
		ooo.Push(server, "things/*", Thing{IP: "192.168.1.1", Port: 0, On: true})
	}
}

func BenchmarkBaseline_Set(b *testing.B) {
	server := createBenchServer()
	defer server.Close(os.Interrupt)
	i := 0
	for b.Loop() {
		ooo.Set(server, "settings", Settings{DayEpoch: i})
		i++
	}
}

func BenchmarkBaseline_Read(b *testing.B) {
	server := createBenchServer()
	defer server.Close(os.Interrupt)
	thingID, _ := ooo.Push(server, "things/*", Thing{IP: "192.168.1.1", Port: 0, On: true})
	for b.Loop() {
		ooo.Get[Thing](server, "things/"+thingID)
	}
}

// =============================================================================
// PIVOT SERVER BENCHMARKS (no nodes registered)
// =============================================================================

func BenchmarkPivotServer_NoNodes_Set(b *testing.B) {
	server := createBenchPivotServer()
	defer server.Close(os.Interrupt)
	i := 0
	for b.Loop() {
		ooo.Set(server, "settings", Settings{DayEpoch: i})
		i++
	}
}

// =============================================================================
// PIVOT SERVER BENCHMARKS (with unreachable node registered)
// =============================================================================

func BenchmarkPivotServer_UnreachableNode_Set(b *testing.B) {
	server := createBenchPivotServer()
	defer server.Close(os.Interrupt)

	// Register a fake unreachable node
	ooo.Push(server, "things/*", Thing{IP: "192.168.99.99", Port: 9999, On: true})

	i := 0
	for b.Loop() {
		ooo.Set(server, "settings", Settings{DayEpoch: i})
		i++
	}
}

// =============================================================================
// PIVOT SERVER BENCHMARKS (with reachable node)
// =============================================================================

func BenchmarkPivotServer_ReachableNode_Set(b *testing.B) {
	pivotServer := createBenchPivotServer()
	defer pivotServer.Close(os.Interrupt)

	nodeServer := createBenchNodeServer(pivotServer.Address)
	defer nodeServer.Close(os.Interrupt)

	// Register the node on the pivot
	nodeIP, nodePort, _ := net.SplitHostPort(nodeServer.Address)
	nodePortInt, _ := strconv.Atoi(nodePort)
	ooo.Push(pivotServer, "things/*", Thing{IP: nodeIP, Port: nodePortInt, On: true})

	i := 0
	for b.Loop() {
		ooo.Set(pivotServer, "settings", Settings{DayEpoch: i})
		i++
	}
}

// =============================================================================
// NODE SERVER BENCHMARKS (syncing to pivot)
// =============================================================================

func BenchmarkNodeServer_Set(b *testing.B) {
	pivotServer := createBenchPivotServer()
	defer pivotServer.Close(os.Interrupt)

	nodeServer := createBenchNodeServer(pivotServer.Address)
	defer nodeServer.Close(os.Interrupt)

	i := 0
	for b.Loop() {
		ooo.Set(nodeServer, "settings", Settings{DayEpoch: i})
		i++
	}
}

func BenchmarkNodeServer_Write(b *testing.B) {
	pivotServer := createBenchPivotServer()
	defer pivotServer.Close(os.Interrupt)

	nodeServer := createBenchNodeServer(pivotServer.Address)
	defer nodeServer.Close(os.Interrupt)

	for b.Loop() {
		ooo.Push(nodeServer, "things/*", Thing{IP: "192.168.1.1", Port: 0, On: true})
	}
}

func BenchmarkNodeServer_Read(b *testing.B) {
	pivotServer := createBenchPivotServer()
	defer pivotServer.Close(os.Interrupt)

	nodeServer := createBenchNodeServer(pivotServer.Address)
	defer nodeServer.Close(os.Interrupt)

	// Pre-populate data on node
	thingID, _ := ooo.Push(nodeServer, "things/*", Thing{IP: "192.168.1.1", Port: 0, On: true})

	for b.Loop() {
		ooo.Get[Thing](nodeServer, "things/"+thingID)
	}
}

// =============================================================================
// END-TO-END SYNC BENCHMARKS (full round-trip with event subscription)
// =============================================================================

// eventWaiter wraps OnStorageEvent to signal when events arrive
type eventWaiter struct {
	ch chan string
}

func newEventWaiter() *eventWaiter {
	return &eventWaiter{ch: make(chan string, 100)}
}

func (w *eventWaiter) callback(event storage.Event) {
	select {
	case w.ch <- event.Key:
	default:
	}
}

func (w *eventWaiter) wait(keyPattern string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		select {
		case k := <-w.ch:
			if key.Match(keyPattern, k) {
				return true
			}
		case <-time.After(time.Until(deadline)):
			return false
		}
	}
}

// createBenchPivotServerWithWaiter creates a pivot server with event waiter
func createBenchPivotServerWithWaiter(waiter *eventWaiter) *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Audit = func(r *http.Request) bool { return true }

	config := pivot.Config{
		Keys:     []pivot.Key{{Path: "settings"}},
		NodesKey: "things/*",
		PivotIP:  "",
		Client:   benchClient(),
	}
	pivot.Setup(server, config)

	// Wrap OnStorageEvent to also signal waiter
	originalCallback := server.OnStorageEvent
	server.OnStorageEvent = func(event storage.Event) {
		if originalCallback != nil {
			originalCallback(event)
		}
		if waiter != nil {
			waiter.callback(event)
		}
	}

	server.OpenFilter("things/*")
	server.OpenFilter("settings")
	server.Start("localhost:0")
	return server
}

// createBenchNodeServerWithWaiter creates a node server with event waiter
func createBenchNodeServerWithWaiter(pivotAddress string, waiter *eventWaiter) *ooo.Server {
	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Audit = func(r *http.Request) bool { return true }

	config := pivot.Config{
		Keys:     []pivot.Key{{Path: "settings"}},
		NodesKey: "things/*",
		PivotIP:  pivotAddress,
		Client:   benchClient(),
	}
	pivot.Setup(server, config)

	// Wrap OnStorageEvent to also signal waiter
	originalCallback := server.OnStorageEvent
	server.OnStorageEvent = func(event storage.Event) {
		if originalCallback != nil {
			originalCallback(event)
		}
		if waiter != nil {
			waiter.callback(event)
		}
	}

	server.OpenFilter("things/*")
	server.OpenFilter("settings")
	server.Start("localhost:0")
	return server
}

// BenchmarkE2E_PivotToNode_Set measures: write on pivot -> wait for sync on node -> verify
func BenchmarkE2E_PivotToNode_Set(b *testing.B) {
	pivotWaiter := newEventWaiter()
	nodeWaiter := newEventWaiter()

	pivotServer := createBenchPivotServerWithWaiter(pivotWaiter)
	defer pivotServer.Close(os.Interrupt)

	nodeServer := createBenchNodeServerWithWaiter(pivotServer.Address, nodeWaiter)
	defer nodeServer.Close(os.Interrupt)

	// Register the node on the pivot
	nodeIP, nodePort, _ := net.SplitHostPort(nodeServer.Address)
	nodePortInt, _ := strconv.Atoi(nodePort)
	ooo.Push(pivotServer, "things/*", Thing{IP: nodeIP, Port: nodePortInt, On: true})

	i := 0
	for b.Loop() {
		// Write on pivot
		ooo.Set(pivotServer, "settings", Settings{DayEpoch: i})
		// Wait for event on node (sync completed)
		nodeWaiter.wait("settings", time.Second)
		i++
	}
}

// BenchmarkE2E_NodeToPivot_Set measures: write on node -> wait for sync on pivot -> verify
func BenchmarkE2E_NodeToPivot_Set(b *testing.B) {
	pivotWaiter := newEventWaiter()
	nodeWaiter := newEventWaiter()

	pivotServer := createBenchPivotServerWithWaiter(pivotWaiter)
	defer pivotServer.Close(os.Interrupt)

	nodeServer := createBenchNodeServerWithWaiter(pivotServer.Address, nodeWaiter)
	defer nodeServer.Close(os.Interrupt)
	_ = nodeWaiter

	i := 0
	for b.Loop() {
		// Write on node
		ooo.Set(nodeServer, "settings", Settings{DayEpoch: i})
		// Wait for event on pivot (sync completed)
		pivotWaiter.wait("settings", time.Second)
		i++
	}
}

// BenchmarkE2E_PivotToNode_Write measures: push on pivot -> wait for sync on node -> verify
func BenchmarkE2E_PivotToNode_Write(b *testing.B) {
	pivotWaiter := newEventWaiter()
	nodeWaiter := newEventWaiter()

	pivotServer := createBenchPivotServerWithWaiter(pivotWaiter)
	defer pivotServer.Close(os.Interrupt)

	nodeServer := createBenchNodeServerWithWaiter(pivotServer.Address, nodeWaiter)
	defer nodeServer.Close(os.Interrupt)

	// Register the node on the pivot
	nodeIP2, nodePort2, _ := net.SplitHostPort(nodeServer.Address)
	nodePortInt2, _ := strconv.Atoi(nodePort2)
	ooo.Push(pivotServer, "things/*", Thing{IP: nodeIP2, Port: nodePortInt2, On: true})
	_ = pivotWaiter

	for b.Loop() {
		// Write on pivot
		ooo.Push(pivotServer, "things/*", Thing{IP: "192.168.1.1", Port: 0, On: true})
		// Wait for event on node (sync completed)
		nodeWaiter.wait("things/*", time.Second)
	}
}

// BenchmarkE2E_NodeToPivot_Write measures: push on node -> wait for sync on pivot -> verify
func BenchmarkE2E_NodeToPivot_Write(b *testing.B) {
	pivotWaiter := newEventWaiter()
	nodeWaiter := newEventWaiter()

	pivotServer := createBenchPivotServerWithWaiter(pivotWaiter)
	defer pivotServer.Close(os.Interrupt)

	nodeServer := createBenchNodeServerWithWaiter(pivotServer.Address, nodeWaiter)
	defer nodeServer.Close(os.Interrupt)
	_ = nodeWaiter

	for b.Loop() {
		// Write on node
		ooo.Push(nodeServer, "things/*", Thing{IP: "192.168.1.1", Port: 0, On: true})
		// Wait for event on pivot (sync completed)
		pivotWaiter.wait("things/*", time.Second)
	}
}

// BenchmarkE2E_MultiNode_Set measures: write on pivot -> wait for sync on all nodes
func BenchmarkE2E_MultiNode_Set(b *testing.B) {
	pivotWaiter := newEventWaiter()
	node1Waiter := newEventWaiter()
	node2Waiter := newEventWaiter()
	node3Waiter := newEventWaiter()

	pivotServer := createBenchPivotServerWithWaiter(pivotWaiter)
	defer pivotServer.Close(os.Interrupt)
	_ = pivotWaiter

	// Create 3 node servers
	node1 := createBenchNodeServerWithWaiter(pivotServer.Address, node1Waiter)
	defer node1.Close(os.Interrupt)
	node2 := createBenchNodeServerWithWaiter(pivotServer.Address, node2Waiter)
	defer node2.Close(os.Interrupt)
	node3 := createBenchNodeServerWithWaiter(pivotServer.Address, node3Waiter)
	defer node3.Close(os.Interrupt)

	// Register all nodes on the pivot
	node1IP, node1Port, _ := net.SplitHostPort(node1.Address)
	node1PortInt, _ := strconv.Atoi(node1Port)
	node2IP, node2Port, _ := net.SplitHostPort(node2.Address)
	node2PortInt, _ := strconv.Atoi(node2Port)
	node3IP, node3Port, _ := net.SplitHostPort(node3.Address)
	node3PortInt, _ := strconv.Atoi(node3Port)
	ooo.Push(pivotServer, "things/*", Thing{IP: node1IP, Port: node1PortInt, On: true})
	ooo.Push(pivotServer, "things/*", Thing{IP: node2IP, Port: node2PortInt, On: true})
	ooo.Push(pivotServer, "things/*", Thing{IP: node3IP, Port: node3PortInt, On: true})

	i := 0
	for b.Loop() {
		// Write on pivot
		ooo.Set(pivotServer, "settings", Settings{DayEpoch: i})
		// Wait for event on all nodes (sync completed)
		node1Waiter.wait("settings", time.Second)
		node2Waiter.wait("settings", time.Second)
		node3Waiter.wait("settings", time.Second)
		i++
	}
}
