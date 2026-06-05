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

// TestTriggerCoalescerRetainReapsDepartedNodes pins the bound on perNode: once
// a node leaves the cluster (drops out of the authoritative membership set
// passed to Retain), its per-node trigger and drainer goroutine are reaped, so
// neither perNode nor the goroutine count grows without bound under churn.
func TestTriggerCoalescerRetainReapsDepartedNodes(t *testing.T) {
	const numNodes = 3

	var allFirstHits sync.WaitGroup
	allFirstHits.Add(numNodes)
	var onces [numNodes]sync.Once
	addrs := make([]string, numNodes)

	for i := range numNodes {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			onces[i].Do(allFirstHits.Done)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		addrs[i] = srv.Listener.Addr().String()
	}

	c := newTriggerCoalescer(http.DefaultClient, nil, nil)
	defer c.Shutdown()

	for i := range numNodes {
		c.Trigger(addrs[i], "items/*")
	}
	allFirstHits.Wait() // all three drainers are live and have fired once

	// Capture the two drainers about to leave so we can assert their goroutines
	// actually exit — not merely that the map shrank.
	c.mu.Lock()
	require.Len(t, c.perNode, numNodes)
	departed := []*nodeTrigger{c.perNode[addrs[1]], c.perNode[addrs[2]]}
	c.mu.Unlock()

	// Membership now reports only node 0; nodes 1 and 2 have left the cluster.
	c.Retain([]string{addrs[0]})

	// The map shrinks synchronously under Retain's lock.
	c.mu.Lock()
	n := len(c.perNode)
	_, kept := c.perNode[addrs[0]]
	c.mu.Unlock()
	require.Equal(t, 1, n, "departed nodes were not reaped from perNode")
	require.True(t, kept, "the still-present node was reaped")

	// Departed drainers exit deterministically (channel close, no poll/sleep).
	for _, nt := range departed {
		<-nt.exited
	}
}

// TestTriggerCoalescerRetainEmptyIsNoOp guards the transient-empty membership
// case: getNodes() can momentarily return nil (storage read error, shutdown),
// and reaping on that would kill every live drainer only to respawn it on the
// next write. Retain(nil) must leave perNode untouched.
func TestTriggerCoalescerRetainEmptyIsNoOp(t *testing.T) {
	var firstHit sync.WaitGroup
	firstHit.Add(1)
	var once sync.Once

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		once.Do(firstHit.Done)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := newTriggerCoalescer(http.DefaultClient, nil, nil)
	defer c.Shutdown()

	addr := srv.Listener.Addr().String()
	c.Trigger(addr, "items/*")
	firstHit.Wait()

	c.Retain(nil) // transient-empty membership — must not reap

	c.mu.Lock()
	n := len(c.perNode)
	_, kept := c.perNode[addr]
	c.mu.Unlock()
	require.Equal(t, 1, n)
	require.True(t, kept, "Retain(nil) reaped a live node on a transient-empty membership read")
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
