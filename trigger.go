package pivot

import (
	"net/http"
	"sync"
)

// triggerCoalescer dispatches "wake up and pull" signals to nodes, collapsing
// rapid bursts on the same node into a single in-flight HTTP call. Each node
// gets a dedicated drainer goroutine; multiple triggers that arrive while the
// drainer is in the middle of one HTTP call accumulate in a per-node pending
// set, and the drainer's next iteration drains them with one more HTTP call.
//
// Coalescing factor scales with load: under quiescence triggers fire 1:1, under
// heavy bursts many events collapse into a single trigger. There is no timer
// involved — coalescing emerges from "single consumer + 1-buffered notify"
// rather than from any time-based debounce, which is what keeps pivot's
// deterministic-causal-chain testing model intact (no time.Sleep needed).
//
// Safety under the consistency principle: TriggerNodeSync is a "wake up and
// pull" signal, not data delivery. The node always pulls *current* state when
// it acts on a trigger. Fewer triggers → same eventual pull → same convergent
// state.
type triggerCoalescer struct {
	mu      sync.Mutex
	perNode map[string]*nodeTrigger
	closed  bool

	client *http.Client
	pool   *syncerPool // for SSL flag; may be nil
	health *NodeHealth // may be nil

	stop chan struct{}
	wg   sync.WaitGroup
}

// nodeTrigger holds per-node coalescing state and runs one drainer goroutine.
type nodeTrigger struct {
	node   string
	parent *triggerCoalescer

	mu      sync.Mutex
	pending map[string]struct{} // key paths waiting to be sent
	notify  chan struct{}       // 1-buffered wake-up signal

	done   chan struct{} // closed by Retain to stop just this node's drainer
	exited chan struct{} // closed by drain when its goroutine returns
}

func newTriggerCoalescer(client *http.Client, pool *syncerPool, health *NodeHealth) *triggerCoalescer {
	return &triggerCoalescer{
		perNode: make(map[string]*nodeTrigger),
		client:  client,
		pool:    pool,
		health:  health,
		stop:    make(chan struct{}),
	}
}

// Trigger queues a "wake up and pull" signal for `node`. Multiple Trigger
// calls for the same node coalesce into a single in-flight HTTP request.
// The keyPath is forwarded to the node so it can pull only the affected key
// when possible; if multiple distinct keys are pending at drain time, the
// drainer falls back to a SyncAll (empty keyPath) which the node-side handler
// already supports.
func (c *triggerCoalescer) Trigger(node, keyPath string) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	nt, ok := c.perNode[node]
	if !ok {
		nt = c.newNodeTriggerLocked(node)
	}
	c.mu.Unlock()
	nt.signal(keyPath)
}

// newNodeTriggerLocked spawns a drainer for node. Caller must hold c.mu.
func (c *triggerCoalescer) newNodeTriggerLocked(node string) *nodeTrigger {
	nt := &nodeTrigger{
		node:    node,
		parent:  c,
		pending: make(map[string]struct{}),
		notify:  make(chan struct{}, 1),
		done:    make(chan struct{}),
		exited:  make(chan struct{}),
	}
	c.perNode[node] = nt
	c.wg.Add(1)
	go nt.drain()
	return nt
}

// signal records keyPath as pending and wakes the drainer.
func (nt *nodeTrigger) signal(keyPath string) {
	nt.mu.Lock()
	nt.pending[keyPath] = struct{}{}
	nt.mu.Unlock()
	// Non-blocking notify: if a wake-up is already pending, drop. The drainer
	// reads the pending map atomically when it picks up the signal, so
	// missed signals don't drop work.
	select {
	case nt.notify <- struct{}{}:
	default:
	}
}

// drain is the per-node loop that fires HTTP triggers. Stops when parent.stop
// is closed (full shutdown) or nt.done is closed (this node reaped by Retain
// because it left the cluster).
func (nt *nodeTrigger) drain() {
	defer nt.parent.wg.Done()
	defer close(nt.exited)
	for {
		select {
		case <-nt.parent.stop:
			return
		case <-nt.done:
			return
		case <-nt.notify:
			// Re-check stop/done in case shutdown or a reap raced a notify.
			select {
			case <-nt.parent.stop:
				return
			case <-nt.done:
				return
			default:
			}
			keyPath, hasWork := nt.takePending()
			if !hasWork {
				continue
			}
			nt.fire(keyPath)
		}
	}
}

// takePending atomically drains the pending set and returns the keyPath to
// pass to TriggerNodeSync. Empty string means "multiple keys pending, fall
// back to SyncAll on the receiver side."
func (nt *nodeTrigger) takePending() (keyPath string, hasWork bool) {
	nt.mu.Lock()
	defer nt.mu.Unlock()
	if len(nt.pending) == 0 {
		return "", false
	}
	if len(nt.pending) == 1 {
		for k := range nt.pending {
			keyPath = k
		}
	}
	// len > 1 → leave keyPath empty; receiver does SyncAll.
	nt.pending = make(map[string]struct{})
	return keyPath, true
}

// fire issues the HTTP wake-up call and updates node health.
func (nt *nodeTrigger) fire(keyPath string) {
	ssl := false
	if nt.parent.pool != nil {
		ssl = nt.parent.pool.ssl
	}
	ok := TriggerNodeSyncWithHealth(ClientOpts{Client: nt.parent.client, SSL: ssl}, nt.node, keyPath)
	if nt.parent.health != nil {
		if ok {
			nt.parent.health.MarkHealthy(nt.node)
		} else {
			nt.parent.health.MarkUnhealthy(nt.node)
		}
	}
}

// Retain prunes per-node triggers whose node is absent from current, stopping
// each pruned node's drainer goroutine, so perNode (and its goroutines) can't
// grow without bound as nodes leave the cluster. It is driven off the
// write-fanout path with the authoritative membership list — event-driven, no
// timer — which keeps pivot's deterministic-causal testing model intact.
//
// An empty current set is treated as "no information", not "everyone left":
// getNodes() can momentarily return nil during a storage read error or
// shutdown, and reaping on that would kill every live drainer only to respawn
// it on the next write. Retain(nil) is therefore a no-op.
func (c *triggerCoalescer) Retain(current []string) {
	if len(current) == 0 {
		return
	}
	keep := make(map[string]struct{}, len(current))
	for _, node := range current {
		keep[node] = struct{}{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	for node, nt := range c.perNode {
		if _, ok := keep[node]; ok {
			continue
		}
		close(nt.done)
		delete(c.perNode, node)
	}
}

// Shutdown stops all drainer goroutines and blocks until they exit. Idempotent.
func (c *triggerCoalescer) Shutdown() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	close(c.stop)
	c.mu.Unlock()
	c.wg.Wait()
}
