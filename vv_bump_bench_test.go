package pivot_test

// Benchmarks comparing the cost of pivot's VV bump on the local write
// hot path. Run on master/PR-51 (async bump) and again on the
// sync-VV-bump branch (synchronous bump); the delta is the cost the
// user pays to remove the "VV-lag" race that lets pullKeyWithCacheUpdate
// clobber local writes.
//
//   go test -bench BenchmarkVVBump -benchmem -benchtime=2s -run ^$
//
// Three angles are measured:
//
//   - BenchmarkVVBumpSingleWriter — single-goroutine SetWithMeta in a
//     tight loop, measures per-call latency. Captures the cost the
//     caller pays inline once the bump moves to AfterWrite.
//
//   - BenchmarkVVBumpConcurrentWriters — N writers racing on the same
//     key. Captures lock contention on the VVManager mutex; the bump
//     used to live on a dedicated watch goroutine, after the change it
//     lives on the writer's goroutine, so concurrent writers now
//     contend on the same critical section.
//
//   - BenchmarkVVVisibilityAfterWrite — measures how long after Set
//     returns the VV reflects the new bump. On master this is the
//     watch-goroutine dispatch latency; on the new branch this should
//     collapse to zero because the bump completes inside Set.

import (
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/benitogf/ooo"
	"github.com/benitogf/ooo/storage"
	"github.com/benitogf/pivot"
	"github.com/gorilla/mux"
)

// benchPivotServer is a minimal pivot Setup'd server backed by an in-
// memory layered storage. No node-side syncer pool, no nodes registered
// — keeps the benchmark focused on the VV-bump portion of a write.
func benchPivotServer(b *testing.B) (*ooo.Server, storage.Database, func()) {
	b.Helper()
	policiesStorage := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})

	server := &ooo.Server{}
	server.Silence = true
	server.Static = true
	server.Storage = storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	server.Router = mux.NewRouter()
	server.Client = &http.Client{
		Timeout: 500 * time.Millisecond,
		Transport: &http.Transport{
			Dial:              (&net.Dialer{Timeout: 500 * time.Millisecond}).Dial,
			MaxConnsPerHost:   3000,
			DisableKeepAlives: true,
		},
	}

	pivot.Setup(server, pivot.Config{
		Keys:       []pivot.Key{{Path: "policies", Database: policiesStorage}},
		ClusterURL: "",
	})
	if err := pivot.GetInstance(server).Attach(policiesStorage); err != nil {
		b.Fatalf("attach: %v", err)
	}
	server.Start("localhost:0")

	return server, policiesStorage, func() { server.Close(nil) }
}

func BenchmarkVVBumpSingleWriter(b *testing.B) {
	_, db, cleanup := benchPivotServer(b)
	defer cleanup()

	payload, _ := json.Marshal(map[string]string{"v": "x"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts := time.Now().UnixNano() + int64(i)
		_, err := db.SetWithMeta("policies", payload, ts, ts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVVBumpConcurrentWriters(b *testing.B) {
	for _, n := range []int{2, 8, 32} {
		b.Run("writers="+strconv.Itoa(n), func(b *testing.B) {
			_, db, cleanup := benchPivotServer(b)
			defer cleanup()
			payload, _ := json.Marshal(map[string]string{"v": "x"})

			b.ResetTimer()
			b.SetParallelism(n)
			b.RunParallel(func(pb *testing.PB) {
				i := int64(0)
				for pb.Next() {
					i++
					ts := time.Now().UnixNano() + i
					_, err := db.SetWithMeta("policies", payload, ts, ts)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

// BenchmarkVVVisibilityAfterWrite measures the gap between SetWithMeta
// returning and the VV reflecting the new bump. On master/PR-51 the
// bump runs on the watch goroutine after Set returns, so the gap is
// non-zero and depends on goroutine scheduling. After the fix the bump
// runs synchronously inside Set, so the gap should be zero (we should
// see the post-bump VV on the very first read).
//
// Reported metric is the average number of vvManager.Get() polls
// required before the VV bumped. 1 means "visible on first read" (sync
// bump). >1 means the bump hadn't landed yet on the first read.
func BenchmarkVVVisibilityAfterWrite(b *testing.B) {
	server, db, cleanup := benchPivotServer(b)
	defer cleanup()
	instance := pivot.GetInstance(server)
	if instance == nil || instance.VVManager == nil {
		b.Fatal("no VVManager")
	}

	payload, _ := json.Marshal(map[string]string{"v": "x"})
	var totalPolls atomic.Int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Snapshot the pre-write VV value for our nodeID. After Set, we
		// want to see this number increment by 1.
		before := instance.VVManager.Get("policies")
		var beforeCounter int64
		for _, v := range before {
			if v > beforeCounter {
				beforeCounter = v
			}
		}

		ts := time.Now().UnixNano() + int64(i)
		_, err := db.SetWithMeta("policies", payload, ts, ts)
		if err != nil {
			b.Fatal(err)
		}

		polls := int64(0)
		for {
			polls++
			now := instance.VVManager.Get("policies")
			var nowCounter int64
			for _, v := range now {
				if v > nowCounter {
					nowCounter = v
				}
			}
			if nowCounter > beforeCounter {
				break
			}
			// Yield so the watch goroutine gets a chance to run.
			time.Sleep(time.Microsecond)
			if polls > 100_000 {
				b.Fatalf("VV never bumped after Set (polls=%d)", polls)
			}
		}
		totalPolls.Add(polls)
	}
	b.ReportMetric(float64(totalPolls.Load())/float64(b.N), "polls/op")
}
