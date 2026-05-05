package pivot

// Deterministic correctness tests for the trigger coalescer.
//
// Pattern: WaitGroup with exact counts (calculated up front) + the
// coalescer's own Shutdown(), which internally wg.Wait()s every drainer
// goroutine — so when Shutdown returns, no further HTTP fires are possible.
// After Shutdown we assert hit counts (bounded, since coalescing collapses
// an unspecified number of triggers into ≥1 HTTP calls).

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTriggerCoalescerDeliversAtLeastOnce pins the core invariant: after any
// number of Trigger calls for a node, that node receives at least one HTTP
// wake-up. Coalescing collapses bursts but never drops every signal.
func TestTriggerCoalescerDeliversAtLeastOnce(t *testing.T) {
	var firstHit sync.WaitGroup
	firstHit.Add(1)
	var once sync.Once
	var hits int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		once.Do(firstHit.Done)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTriggerCoalescer(srv.Client(), nil, nil)
	addr := srv.Listener.Addr().String()

	for range 100 {
		c.Trigger(addr, "items/*")
	}

	firstHit.Wait()
	c.Shutdown()

	final := atomic.LoadInt32(&hits)
	require.GreaterOrEqual(t, final, int32(1), "node received no triggers — coalescer dropped them all")
	require.LessOrEqual(t, final, int32(100), "more triggers than caller signaled — coalescer is amplifying work")
}

// TestTriggerCoalescerEachNodeGetsAtLeastOne ensures multi-node fan-out
// reaches every registered node, not just the first one — i.e., per-node
// drainers are independent.
func TestTriggerCoalescerEachNodeGetsAtLeastOne(t *testing.T) {
	const numNodes = 4

	var allFirstHits sync.WaitGroup
	allFirstHits.Add(numNodes)

	var hits [numNodes]int32
	var onces [numNodes]sync.Once
	addrs := make([]string, numNodes)

	for i := range numNodes {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&hits[i], 1)
			onces[i].Do(allFirstHits.Done)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		addrs[i] = srv.Listener.Addr().String()
	}

	c := newTriggerCoalescer(http.DefaultClient, nil, nil)
	for i := range 50 {
		c.Trigger(addrs[i%numNodes], "items/*")
	}

	allFirstHits.Wait()
	c.Shutdown()

	for i := range numNodes {
		require.GreaterOrEqual(t, atomic.LoadInt32(&hits[i]), int32(1),
			"node %d (%s) received no triggers", i, addrs[i])
	}
}

// TestTriggerCoalescerShutdownIdempotent verifies double-Shutdown is safe.
func TestTriggerCoalescerShutdownIdempotent(t *testing.T) {
	c := newTriggerCoalescer(http.DefaultClient, nil, nil)
	c.Shutdown()
	c.Shutdown() // must not panic on already-closed stop channel
}

// TestTriggerCoalescerNoTriggersAfterShutdown verifies Trigger calls that
// arrive after Shutdown are silent no-ops (no HTTP fire, no goroutine spawn).
func TestTriggerCoalescerNoTriggersAfterShutdown(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTriggerCoalescer(srv.Client(), nil, nil)
	c.Shutdown() // shut down BEFORE any triggers

	addr := srv.Listener.Addr().String()
	for range 10 {
		c.Trigger(addr, "items/*")
	}

	// Trigger() after Shutdown is synchronous no-op — drainer goroutines have
	// already exited via wg.Wait() inside Shutdown(). Nothing async to wait
	// on; if the contract were broken, hits would be > 0.
	require.Equal(t, int32(0), atomic.LoadInt32(&hits), "trigger after shutdown should not fire HTTP")
}
