package pivot

// Internal-package test for the syncer's pre-init startup window.
//
// Race window: in ooo's StartWithError, waitListen sets server.Address and
// flips active=1 (line 347), then wg.Done() releases StartWithError, which
// continues to call OnStart() at line 682. pivot.Setup wires SetNodeAddr into
// OnStart, so between listener-up and OnStart-firing the syncer carries
// nodeAddr="". Any storage event in that window calls QueueOrSendSet/Delete,
// which without this guard sends to the leader with no X-Pivot-Originator —
// the leader can't identify this node, fans the change back to it, and
// produces a self-trigger echo (and on every retry, no skip).
//
// Pattern: drive the syncer directly with empty nodeAddr — same shape as the
// VVManager test added in #31. WaitGroup-based synchronization, no sleeps.

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/monotonic"
	"github.com/stretchr/testify/require"
)

// fakeLeader records the originator header from any inbound POST/DELETE and
// signals first-hit via a WaitGroup so tests synchronise without time.Sleep.
type fakeLeader struct {
	srv      *httptest.Server
	hits     int32
	firstHit sync.WaitGroup
	once     sync.Once

	mu             sync.Mutex
	lastOriginator string
	allOriginators []string
}

func newFakeLeader() *fakeLeader {
	l := &fakeLeader{}
	l.firstHit.Add(1)
	l.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&l.hits, 1)
		orig := r.Header.Get(OriginatorHeader)
		l.mu.Lock()
		l.lastOriginator = orig
		l.allOriginators = append(l.allOriginators, orig)
		l.mu.Unlock()
		l.once.Do(l.firstHit.Done)
		w.WriteHeader(http.StatusOK)
	}))
	return l
}

func (l *fakeLeader) addr() string { return l.srv.URL[len("http://"):] }
func (l *fakeLeader) close()       { l.srv.Close() }

func (l *fakeLeader) hitCount() int32 { return atomic.LoadInt32(&l.hits) }

func (l *fakeLeader) originators() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.allOriginators))
	copy(out, l.allOriginators)
	return out
}

// TestSyncerDefersSendUntilNodeAddrSet pins the bug: a syncer constructed with
// empty nodeAddr (mirroring pre-OnStart state) must not push to the leader
// with an empty X-Pivot-Originator. Once SetNodeAddr fires, the queued op
// must drain with the correct originator.
func TestSyncerDefersSendUntilNodeAddrSet(t *testing.T) {
	monotonic.Init()
	leader := newFakeLeader()
	defer leader.close()

	s := newSyncer(leader.srv.Client(), leader.addr(), []Key{{Path: "things/*"}}, false)

	now := monotonic.Now()
	obj := meta.Object{
		Created: now,
		Updated: now,
		Index:   "abc",
		Path:    "things/abc",
		Data:    []byte(`{"v":1}`),
	}

	// Pre-init: nodeAddr is "". A storage-event-driven send must not reach the
	// leader yet — it must be deferred until the addr is known.
	s.QueueOrSendSet("things/abc", obj)

	// Now fire SetNodeAddr; the deferred op must drain with the correct
	// originator.
	const nodeAddr = "127.0.0.1:9001"
	s.SetNodeAddr(nodeAddr)

	leader.firstHit.Wait()

	require.Equal(t, nodeAddr, leader.originators()[0],
		"leader received request with wrong X-Pivot-Originator (originators seen: %v)", leader.originators())
}

// TestSyncerDefersDeleteUntilNodeAddrSet — same invariant for delete ops.
func TestSyncerDefersDeleteUntilNodeAddrSet(t *testing.T) {
	monotonic.Init()
	leader := newFakeLeader()
	defer leader.close()

	s := newSyncer(leader.srv.Client(), leader.addr(), []Key{{Path: "things/*"}}, false)

	s.QueueOrSendDelete("things/abc", monotonic.Now())

	const nodeAddr = "127.0.0.1:9002"
	s.SetNodeAddr(nodeAddr)

	leader.firstHit.Wait()

	require.Equal(t, nodeAddr, leader.originators()[0],
		"leader received delete with wrong X-Pivot-Originator (originators seen: %v)", leader.originators())
}

// TestSyncerSendsImmediatelyAfterNodeAddrSet — once nodeAddr is set,
// QueueOrSendSet should send synchronously (not buffer indefinitely).
func TestSyncerSendsImmediatelyAfterNodeAddrSet(t *testing.T) {
	monotonic.Init()
	leader := newFakeLeader()
	defer leader.close()

	s := newSyncer(leader.srv.Client(), leader.addr(), []Key{{Path: "things/*"}}, false)
	const nodeAddr = "127.0.0.1:9003"
	s.SetNodeAddr(nodeAddr)

	now := monotonic.Now()
	obj := meta.Object{Created: now, Updated: now, Index: "abc", Path: "things/abc", Data: []byte(`{"v":1}`)}
	s.QueueOrSendSet("things/abc", obj)

	leader.firstHit.Wait()
	require.Equal(t, nodeAddr, leader.originators()[0])
	require.Equal(t, int32(1), leader.hitCount())
}
