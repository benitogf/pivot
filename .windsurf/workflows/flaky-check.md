---
description: Run tests in batches to detect flaky scenarios
---

# Flaky Test Detection

Run tests in batches of 10 iterations (target: 1000 total iterations = 100 batches) to detect non-deterministic behavior.

## Usage

When the user invokes `/flaky-check`, ask which test pattern to run (or use `-run .` for all tests).

## Steps

1. Ask the user for the test pattern (default: all tests with `-run .`)

2. Run a single test iteration to measure baseline duration:
// turbo
```bash
time go test -race -count=1 -run <PATTERN> 2>&1
```
   - Extract the duration from output (e.g., "ok ... 5.123s")
   - Calculate timeout for 10 iterations: baseline × 10 × 1.5 (minimum 60s)

3. Run batches of 10 iterations continuously:
// turbo
```bash
go test -v -failfast -race -count=10 -timeout=<CALCULATED>s -run <PATTERN> 2>&1 | tail -20
```

   After each batch, clear the test folder to prevent disk accumulation:
// turbo
```bash
rm -rf test/*
```

4. After each batch:
   - If PASS: report progress (e.g., "Batch 5 complete, 50 iterations passed") and run another batch
   - If FAIL: stop immediately and proceed to step 5

5. On failure - investigate and fix:
   - Analyze the failure output (panic, assertion, timeout, etc.)
   - Read the failing test code to understand the root cause
   - Apply a fix to the test or production code
   - If fix is applied: restart from step 2 (reset batch counter)
   - If unable to fix: explain the issue clearly and stop

6. Keep running batches until:
   - 1000 iterations (100 batches) pass successfully - verification complete
   - The user stops the process
   - A failure cannot be fixed (report reason and stop)

## Timeout Calculation

- Run 1 test first to get baseline duration
- Timeout per batch = baseline × 10 × 1.5
- Minimum timeout: 60 seconds

## Notes

- Always use -count=10 per batch
- On failure, always attempt to fix before giving up
- After fixing, restart batches from the beginning to ensure stability
- Report progress after each batch to show tests are still running
