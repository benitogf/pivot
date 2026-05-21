package pivot

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/benitogf/ooo/monotonic"
	"github.com/benitogf/ooo/storage"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
)

// TestClampFutureUpdated_BelowLimitPassesThrough confirms the no-op case:
// any Updated value within MaxFutureUpdatedSkew of "now" is returned
// unchanged. Routine NTP-disciplined traffic must not trip the clamp.
func TestClampFutureUpdated_BelowLimitPassesThrough(t *testing.T) {
	restore := installFixedNow(t, 1_000_000_000)
	defer restore()

	old := MaxFutureUpdatedSkew.Load()
	MaxFutureUpdatedSkew.Store(int64(5 * time.Minute))
	defer MaxFutureUpdatedSkew.Store(old)

	// 4m59s ahead of fixed "now" — still under the 5-minute skew limit.
	in := int64(1_000_000_000) + int64(4*time.Minute+59*time.Second)
	got := clampFutureUpdated("things/x", in)
	require.Equal(t, in, got, "values within MaxFutureUpdatedSkew must not be clamped")
}

// TestClampFutureUpdated_FutureBeyondLimitClamps is the bug-defeating
// assertion: a peer's far-future Updated value lands as now+skew, not as
// the original poisonous value.
func TestClampFutureUpdated_FutureBeyondLimitClamps(t *testing.T) {
	restore := installFixedNow(t, 1_000_000_000)
	defer restore()

	old := MaxFutureUpdatedSkew.Load()
	MaxFutureUpdatedSkew.Store(int64(5 * time.Minute))
	defer MaxFutureUpdatedSkew.Store(old)

	// 24 hours in the future — well beyond the 5-minute skew tolerance.
	in := int64(1_000_000_000) + int64(24*time.Hour)
	got := clampFutureUpdated("things/x", in)
	want := int64(1_000_000_000) + int64(5*time.Minute)
	require.Equal(t, want, got, "future-skewed Updated must be clamped to now+MaxFutureUpdatedSkew")
}

// TestClampFutureUpdated_ZeroSkewDisables documents the escape hatch: an
// operator who needs the legacy behaviour back can set MaxFutureUpdatedSkew
// to 0 and pivot stops clamping entirely.
func TestClampFutureUpdated_ZeroSkewDisables(t *testing.T) {
	restore := installFixedNow(t, 1_000_000_000)
	defer restore()

	old := MaxFutureUpdatedSkew.Load()
	MaxFutureUpdatedSkew.Store(0)
	defer MaxFutureUpdatedSkew.Store(old)

	in := int64(1_000_000_000) + int64(365*24*time.Hour) // a year in the future
	got := clampFutureUpdated("things/x", in)
	require.Equal(t, in, got, "skew=0 must disable the clamp")
}

// TestHandlerSet_ClampsFutureUpdatedAtIngress is the integration check.
// A peer pushes a record with Updated 30 days in the future via the real
// HTTP handler; storage must end up holding a clamped timestamp, not the
// original poisonous value. Without the fix, the SetWithMeta inside the
// handler would persist Updated = now + 30d, freezing the key for a month.
func TestHandlerSet_ClampsFutureUpdatedAtIngress(t *testing.T) {
	monotonic.Init()
	restore := installFixedNow(t, time.Now().UnixNano())
	defer restore()
	old := MaxFutureUpdatedSkew.Load()
	MaxFutureUpdatedSkew.Store(int64(5 * time.Minute))
	defer MaxFutureUpdatedSkew.Store(old)

	db := storage.New(storage.LayeredConfig{Memory: storage.NewMemoryLayer()})
	require.NoError(t, db.Start(storage.Options{}))
	defer db.Close()

	tracker := NewHandlerWriteTracker()
	vvm := NewVVManager(db, "leader")
	handler := Set(db, "things", tracker, vvm, nil)

	// Updated = 30 days ahead of the pinned "now". Body must serialise the
	// meta.Object envelope the way real pivot peers send it.
	farFuture := nowUnixNano() + int64(30*24*time.Hour)
	body := strings.NewReader(
		`{"created":0,"updated":` +
			itoa(farFuture) +
			`,"index":"abc","path":"things/abc","data":"e30="}`)
	req := httptest.NewRequest("POST", "/_pivot/pivot/things/abc", body)
	req = mux.SetURLVars(req, map[string]string{"index": "abc"})
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	handler(rec, req)
	require.Equal(t, 200, rec.Code)

	stored, err := db.Get("things/abc")
	require.NoError(t, err)
	limit := nowUnixNano() + int64(5*time.Minute)
	require.LessOrEqual(t, stored.Updated, limit,
		"stored Updated must not exceed now+MaxFutureUpdatedSkew; got %d (limit %d)",
		stored.Updated, limit)
	require.Less(t, stored.Updated, farFuture,
		"stored Updated must NOT equal the peer's poisonous value")
}

// installFixedNow swaps the package-level nowUnixNano so the clamp tests
// observe a deterministic "now". The previous value is returned to its
// original state by the returned restore function.
func installFixedNow(t *testing.T, fixed int64) func() {
	t.Helper()
	prev := nowUnixNano
	nowUnixNano = func() int64 { return fixed }
	return func() { nowUnixNano = prev }
}

// itoa is a tiny strconv.FormatInt wrapper to keep the test body readable.
func itoa(v int64) string {
	const digits = "0123456789"
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = digits[v%10]
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
