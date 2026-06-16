package pivot

import (
	"fmt"
	"strings"
	"testing"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/meta"
	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
)

// Benchmark scenarios for Finding #2 from review.md: GetNodes caching.
// The hot path is makeStorageSync calling cfg.GetNodes() on every storage
// event. Before the cache, each call did GetList + N*json.Unmarshal. After
// the cache, steady-state reads are O(1) and settings-only changes to node
// entries are handled with a single Unmarshal instead of N.

// nodePayload builds a JSON body for a node entry. extraSettingsKB lets us
// simulate the user's scenario where node entries carry heavy device
// settings alongside ip/port.
func nodePayload(ip string, port int, extraSettingsKB int) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, `{"ip":"%s","port":%d`, ip, port)
	if extraSettingsKB > 0 {
		filler := strings.Repeat("x", 1024)
		b.WriteString(`,"settings":{`)
		for i := range extraSettingsKB {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `"k%d":"%s"`, i, filler)
		}
		b.WriteString(`}`)
	}
	b.WriteString(`}`)
	return []byte(b.String())
}

func newBenchServer() *ooo.Server {
	monotonic.Init()
	s := &ooo.Server{}
	s.Silence = true
	s.Static = true
	s.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	if err := s.Storage.Start(storage.Options{}); err != nil {
		panic(err)
	}
	return s
}

func seedNodes(t testing.TB, s *ooo.Server, n int, extraSettingsKB int) {
	t.Helper()
	for i := range n {
		key := fmt.Sprintf("nodes/node%d", i)
		data := nodePayload(fmt.Sprintf("10.0.0.%d", i+1), 8000+i, extraSettingsKB)
		if _, err := s.Storage.Set(key, data); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

// newBenchInstance wires an Instance + nodesCache exactly as Setup would for
// the getNodes hot path. We avoid full Setup() so the benchmark is focused
// on the GetNodes call and does not spin up health-check goroutines etc.
func newBenchInstance(s *ooo.Server, nodesKey string) *Instance {
	inst := &Instance{}
	inst.nodesCache = newNodesCache(s, nodesKey, "", inst.IsShutdown)
	return inst
}

// BenchmarkGetNodesFresh measures the pre-cache behavior: every call scans
// storage and unmarshals every node entry. This is what Instance.GetNodes
// (the public API) still does.
func BenchmarkGetNodesFresh(b *testing.B) {
	for _, n := range []int{10, 100} {
		for _, kb := range []int{0, 4} {
			b.Run(fmt.Sprintf("N=%d/SettingsKB=%d", n, kb), func(b *testing.B) {
				s := newBenchServer()
				defer s.Storage.Close()
				seedNodes(b, s, n, kb)
				inst := newBenchInstance(s, "nodes/*")
				get := makeGetNodes(s, "nodes/*", "", inst)

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					_ = get()
				}
			})
		}
	}
}

// BenchmarkGetNodesCached measures the event-driven hot path: after a
// one-time warmup, each call is O(1) (return the shared slice). This is
// what the broadcast loop in makeStorageSync now uses.
func BenchmarkGetNodesCached(b *testing.B) {
	for _, n := range []int{10, 100} {
		for _, kb := range []int{0, 4} {
			b.Run(fmt.Sprintf("N=%d/SettingsKB=%d", n, kb), func(b *testing.B) {
				s := newBenchServer()
				defer s.Storage.Close()
				seedNodes(b, s, n, kb)
				inst := newBenchInstance(s, "nodes/*")
				get := makeGetNodesCached(inst)
				// Prime the cache so we measure steady state, not first load.
				_ = get()

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					_ = get()
				}
			})
		}
	}
}

// BenchmarkGetNodesCachedSettingsChurn exercises the user's real-world
// scenario: node entries carry bulky settings that change often, but
// ip/port stay the same. Each iteration fires a "set" event with the same
// ip:port (settings bumped) then reads GetNodes. Under the fast path this
// should be ~1 unmarshal of the single changed object + O(1) read, not a
// full rebuild.
func BenchmarkGetNodesCachedSettingsChurn(b *testing.B) {
	for _, n := range []int{10, 100} {
		for _, kb := range []int{4} {
			b.Run(fmt.Sprintf("N=%d/SettingsKB=%d", n, kb), func(b *testing.B) {
				s := newBenchServer()
				defer s.Storage.Close()
				seedNodes(b, s, n, kb)
				inst := newBenchInstance(s, "nodes/*")
				get := makeGetNodesCached(inst)
				_ = get()

				// Pre-build churn events: same ip/port, new settings payload.
				events := make([]storage.Event, n)
				for i := range n {
					events[i] = storage.Event{
						Key:       fmt.Sprintf("nodes/node%d", i),
						Operation: "set",
						Object: &meta.Object{
							Index: fmt.Sprintf("node%d", i),
							Data:  nodePayload(fmt.Sprintf("10.0.0.%d", i+1), 8000+i, kb),
						},
					}
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					inst.nodesCache.update(events[i%n])
					_ = get()
				}
			})
		}
	}
}

// BenchmarkGetNodesCachedIPChange measures the slow path inside update():
// an ip/port actually changed, so we must rebuild the slice. Provided as
// a ceiling so the churn benchmark can be compared to it.
func BenchmarkGetNodesCachedIPChange(b *testing.B) {
	for _, n := range []int{10, 100} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			s := newBenchServer()
			defer s.Storage.Close()
			seedNodes(b, s, n, 0)
			inst := newBenchInstance(s, "nodes/*")
			get := makeGetNodesCached(inst)
			_ = get()

			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				// Alternate port each iteration to force a real change.
				port := 9000 + (i % 1000)
				ev := storage.Event{
					Key:       "nodes/node0",
					Operation: "set",
					Object: &meta.Object{
						Index: "node0",
						Data:  nodePayload("10.0.0.1", port, 0),
					},
				}
				inst.nodesCache.update(ev)
				_ = get()
			}
		})
	}
}
