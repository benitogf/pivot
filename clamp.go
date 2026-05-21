package pivot

import (
	"log"
	"sync/atomic"
	"time"
)

// MaxFutureUpdatedSkew bounds how far ahead of the current wall clock an
// incoming Updated timestamp is allowed to be before pivot clamps it.
//
// Pivot's sync layer uses Updated for last-write-wins ordering. A peer with
// a clock skewed forward — by NTP misconfiguration, container clock drift,
// a stray `date -s`, etc. — can otherwise write records with Updated values
// minutes, hours, or years in the future. Those values then dominate every
// present-time write at every other peer until wall-clock time catches up,
// so a single misconfigured node can silently freeze a key for the entire
// cluster (a `device.open=true` write succeeds locally and is immediately
// reverted by the next pull-sync, for example).
//
// Clamping at ingress prevents the poison from entering the cluster's
// storage in the first place. The bound applies at every write that
// originates outside the local Set path:
//
//   - handlers.Set (POST /_pivot/{base}/{index}): pivot's receiver for
//     peer-pushed writes.
//   - syncSetFromLeader, syncLocalEntriesWithTracking (sync.go): pivot's
//     pull-from-leader paths.
//
// The default of 5 minutes covers NTP transients and short clock jumps
// without rejecting routine traffic. Operators with tighter clock discipline
// can lower it; operators running virtualised peers known to drift can
// raise it. Set to 0 to disable the clamp entirely (legacy behaviour).
//
// Stored as int64 nanoseconds in an atomic so the value can be tuned at
// runtime by an operator endpoint without locks.
var MaxFutureUpdatedSkew atomic.Int64

func init() {
	MaxFutureUpdatedSkew.Store(int64(5 * time.Minute))
}

// nowUnixNano is the wall-clock source used by clampFutureUpdated. Tests
// substitute their own; production keeps time.Now().
var nowUnixNano = func() int64 { return time.Now().UnixNano() }

// clampFutureUpdated returns updated unchanged if it is within
// MaxFutureUpdatedSkew of the current wall clock. If updated is farther in
// the future than that, it is clamped to now+skew and a one-line warning is
// logged identifying the affected key and the drift that was rejected.
//
// Used by every pivot ingress that accepts an externally-supplied Updated
// timestamp. Local writes that go through Storage.Set are stamped from the
// monotonic clock and never hit this path.
func clampFutureUpdated(key string, updated int64) int64 {
	skew := MaxFutureUpdatedSkew.Load()
	if skew <= 0 {
		return updated
	}
	now := nowUnixNano()
	limit := now + skew
	if updated > limit {
		log.Printf("[pivot] WARN clamping future Updated for %q: was %d (%s ahead of wall clock), clamped to now+skew %d",
			key, updated, time.Duration(updated-now), limit)
		return limit
	}
	return updated
}
