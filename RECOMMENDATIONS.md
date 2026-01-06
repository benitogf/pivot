# Pivot Package - Recommendations

Analysis of the pivot package for missing test cases, edge scenarios, and improvements.

## Overview

The `pivot` package implements a synchronization system between a central "pivot" server and multiple "node" servers. It handles bidirectional data sync, node health tracking, and pivot IP change detection.

---

## Missing Test Cases

### 1. NodeHealth Component Tests
The `NodeHealth` struct has significant functionality but no dedicated unit tests:

- [x] `IsHealthy` - Unknown nodes assumed healthy, known nodes return stored state
- [x] `MarkHealthy`/`MarkUnhealthy` - State transitions
- [x] `GetHealthyNodes` - Filtering logic
- [x] `GetStatus` - JSON serialization
- [ ] `StartBackgroundCheck`/`Stop` - Goroutine lifecycle (double-start, stop without start)
- [ ] `recheckUnhealthyNodes` - Parallel health checks
- [ ] `pingNode` - HTTP health check logic

### 2. Diff Algorithm Tests
`GetEntriesNegativeDiff` and `GetEntriesPositiveDiff` extracted to `diff.go` with tests in `diff_test.go`:

- [x] Empty source/destination lists
- [x] Identical lists (no diff)
- [x] All entries new/deleted
- [x] Updated entries with same/different timestamps
- [x] Large lists (performance)
- [x] Order preservation
- [x] Benchmarks in `diff_bench_test.go`

### 3. Activity Checking Tests
`checkActivity` and `checkLastDelete` are untested:

- [ ] Glob patterns (`/*`) vs single keys
- [ ] Missing pivot timestamp entries
- [ ] Malformed timestamp data
- [ ] Database errors

### 4. HTTP Handler Tests
Individual handlers lack isolation tests:

- [ ] `Pivot()` - Called on pivot server (should return 400)
- [ ] `Get()` - Database errors, empty results
- [ ] `Set()` - Malformed JSON, missing index
- [ ] `Delete()` - Non-existent entries
- [ ] `Activity()` - Inactive database
- [ ] `NodeHealthHandler()` - nil NodeHealth

### 5. Key and FindKeyStorage Tests
`Key` type and `FindKeyStorage` extracted to `key.go` with tests in `key_test.go`:

- [x] Matching key found (glob and exact)
- [x] No matching key (error case)
- [x] Multiple keys with first-match-wins behavior
- [x] Empty keys list
- [x] Nil database handling
- [x] Key.MatchesIndex, Key.IsGlobPattern, Key.BasePath methods
- [x] Benchmarks in `key_bench_test.go`

### 6. Concurrent Sync Tests
- [ ] Multiple simultaneous sync requests
- [ ] Sync during active read/write operations
- [ ] Race conditions in `makeSyncFuncs` mutex handling

### 7. Network Failure Scenarios
- [ ] Pivot unreachable during node write
- [ ] Node unreachable during pivot broadcast
- [ ] Partial sync failures (some entries succeed, others fail)
- [ ] HTTP response body read errors

### 8. Data Integrity Edge Cases
- [ ] Sync with corrupted/malformed JSON data
- [ ] Very large entries (memory pressure)
- [ ] Entries with special characters in keys
- [ ] Timestamp overflow/edge values

---

## Edge Scenarios Not Covered

### 1. Conflict Resolution
When `activityLocal.LastEntry == activityPivot.LastEntry`, no sync occurs. But what if:
- Both have different data with same timestamp?
- Clock skew between servers?

### 2. Delete During Sync
- Entry deleted on pivot while node is syncing
- Entry deleted on node while sending to pivot

### 3. Rapid Pivot Changes
The `checkPivotIPChange` uses in-memory `storedPivotIPs` map:
- What happens on process restart? (Map is cleared)
- Multiple Setup calls with different pivots

### 4. External Storage Edge Cases
- `Attach()` called before `Setup()`
- `Attach()` called multiple times on same database
- Database becomes inactive during sync

### 5. Route Conflicts
- Routes under `/_pivot` that conflict with existing routes

---

## Suggested Improvements

### 1. Error Handling & Observability
```go
// Current: Errors silently swallowed
// Suggested: Add error callback or metrics
type Config struct {
    // ... existing fields
    OnError func(operation string, err error) // Optional error callback
}
```

### 2. Persistent Pivot IP Tracking
```go
// Current: In-memory map cleared on restart
// Suggested: Store in database
func checkPivotIPChange(server *ooo.Server, config Config, pivotIP string) bool {
    // Read from server.Storage.Get("_pivot:ip")
    // Compare and wipe if changed
    // Store new IP
}
```

### 3. Conflict Resolution Strategy
```go
type Config struct {
    // ... existing fields
    ConflictResolver func(local, remote meta.Object) meta.Object // Optional
}
```

### 4. Graceful Shutdown
`NodeHealth.Stop()` should integrate with server lifecycle and wait for goroutine exit.

### 5. Retry Logic
No retry mechanism for failed sync operations:
```go
type Config struct {
    // ... existing fields
    MaxRetries int           // Default: 0 (no retry)
    RetryDelay time.Duration // Default: 1s
}
```

### 6. Sync Timeout
No timeout on individual sync operations - consider using context for cancellation/timeout.

### 7. Instance Cleanup
`instances` map never cleaned up when server closes - add cleanup function.

### 8. Validation in Setup
No validation of config - add error returns for invalid configurations.

### 9. Metrics/Telemetry
Add optional metrics for monitoring:
- Sync duration
- Sync success/failure counts
- Node health status changes
- Data transfer sizes

### 10. Documentation
- Add godoc examples for common use cases
- Document thread-safety guarantees
- Document expected behavior during network partitions

---

## Test Coverage Summary

| Category | Current Coverage | Gap |
|----------|-----------------|-----|
| Basic sync (pivot↔node) | ✅ Good | - |
| External storage sync | ✅ Good | - |
| Pivot IP change | ✅ Good | Persistence across restarts |
| Node health tracking | ⚠️ Integration only | Unit tests missing |
| Error scenarios | ❌ Minimal | Network failures, malformed data |
| Concurrent operations | ❌ Minimal | Race conditions |
| HTTP handlers | ❌ None | Individual handler tests |
| Diff algorithms | ❌ None | Unit tests for edge cases |

---

## Progress Tracking

- [x] Create RECOMMENDATIONS.md
- [x] Extract NodeHealth to nodehealth.go
- [x] Add unit tests for NodeHealth (nodehealth_test.go)
- [x] Use network.IsHostReachable for ping checks
- [x] Implement per-node backoff: 1s initial, 5s after 10min, 10s after 30min
- [x] Integrate NodeHealth lifecycle with server.Active()
- [x] Add diff algorithm tests (diff.go, diff_test.go, diff_bench_test.go)
- [x] Add Key/FindKeyStorage tests (key.go, key_test.go, key_bench_test.go)
- [ ] Add HTTP handler tests
- [ ] Add concurrent sync tests
- [ ] Implement persistent pivot IP tracking
- [ ] Add error callback support
- [ ] Add instance cleanup on server close
