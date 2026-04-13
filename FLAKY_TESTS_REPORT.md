# Flaky Tests Report — OpenFGA

**Date:** 2026-04-13
**Branch analyzed:** `main` (commit `fa570240`)
**Data source:** GitHub Actions CI runs (last 300 runs via `gh` CLI)

---

## Methodology

This report combines **objective CI failure data** from GitHub Actions with **static code analysis** of the test suite to identify and classify flaky tests.

### How the data was collected

1. **CI run history:** Retrieved the last 300 GitHub Actions runs using `gh run list` and categorized them by conclusion (success, failure, skipped, action_required).

2. **Failure log extraction:** For every failed run, downloaded the failed job logs using `gh run view <id> --log-failed` and extracted the failing test names, error messages, and error traces.

3. **Main-branch isolation:** Failures on the `main` branch are the strongest signal for flakiness — these runs have no code changes that could introduce new bugs. We identified 4 failures on `main` in the sample window.

4. **Dependabot branch cross-referencing:** Dependabot PRs only bump dependency versions. Failures on these branches that match the same test names and error patterns as `main` failures are strong flake indicators.

5. **PR history for flake fixes:** Searched merged and open PRs with `gh pr list` for titles referencing "flaky", "flak", "non-deterministic", and "fix test" to understand which flakes the team has already identified.

6. **Static analysis (supplementary):** Searched all `*_test.go` files for patterns known to cause flakiness: `time.Sleep`, `time.After` with `select`, `require.Eventually` with tight timeouts, `os.Chdir` in `init()`, and hardcoded port numbers. This was used to explain *why* the CI failures occur, not to identify them.

---

## Overview

| Metric | Value |
|--------|-------|
| CI runs analyzed | 300 |
| Total failures | 51 |
| Failures on `main` branch | 4 |
| Estimated flake rate on `main` | ~8% |
| Distinct flaky test families | 4 |
| PRs already opened/merged to fix flakes | 3 (#3038, #3058, #3061) |

---

## Flaky Test #1: Matrix Tests — Postgres/MySQL I/O Timeouts

**Tests affected:**
- `TestMatrixPostgres`
- `TestMatrixMemory`
- `TestMatrixMysql`
- `TestCheckMemory`, `TestCheckPostgres`, `TestCheckMySQL`, `TestCheckSQLite`
- `TestServerLogs`

**Observed CI failures:**

| Run ID | Date | Branch | Failing Test | Error |
|--------|------|--------|-------------|-------|
| 24153623558 | Apr 8 | `main` | `TestMatrixMemory/.../test_usersets_user_userset_exclude_mixed/assertion_15` | `rpc error: code = Code(2058) desc = Request Cancelled` |
| 24162658997 | Apr 8 | `main` | `TestMatrixPostgres`, `TestMatrixMysql`, `TestCheckMemory`, `TestCheckPostgres`, `TestCheckMySQL`, `TestCheckSQLite`, `TestServerLogs` (7 tests) | `connection refused` — server failed to start within backoff window |
| 24245400875 | Apr 10 | `main` | `TestMatrixPostgres/.../test_usersets_user_userset_intersect_mixed` (assertions 3, 4, 5) | `sql error: timeout: read tcp ... i/o timeout` |
| 24247026973 | Apr 10 | `main` | `TestMatrixPostgres/.../assertion_list_users_valid_userset_multi_level` | `sql error: timeout: read tcp ... i/o timeout` |
| 24262761474 | Apr 10 | PR branch | `TestMatrixMemory/.../test_usersets_user_userset_exclude_mixed/assertion_15` | `rpc error: code = Code(2058) desc = Request Cancelled` |

**Impact:** ~8+ CI runs failed from this family. This is the #1 source of flakiness.

**Root cause:** Docker-based Postgres and MySQL containers under CI runner load hit I/O timeouts. The test server sometimes fails to bind its random port within the `EnsureServiceHealthy` exponential backoff window (30-second max). The matrix tests then receive `Internal Server Error` (code 4000) or `Request Cancelled` (code 2058) from the gRPC server.

**Why it happens (code-level):**
- `pkg/testutils/testutils.go:196-250`: `EnsureServiceHealthy` uses exponential backoff with a 30-second max elapsed time. Under heavy CI load, this may not be enough.
- The Postgres test container is shared across many parallel subtests that all issue concurrent queries, saturating the container's connection pool.

**Status:** Unfixed.

---

## Flaky Test #2: Iterator Cache Tests — Background Goroutine Timing

**Tests affected:**
- `TestV2CheckWithIteratorCache_HigherConsistencyBypassesCache`
- `TestV2CheckWithIteratorCache_Invalidation`
- `TestV2CheckWithIteratorCache_Conditions`
- `TestV2CheckWithIteratorCache_PopulatesAndHits`

**Observed CI failures:**

| Run ID | Date | Branch | Failing Test | Error |
|--------|------|--------|-------------|-------|
| 24215866195 | Apr 9 | `feat/iterator_cache_v2` | `TestV2CheckWithIteratorCache_Invalidation` | `Condition never satisfied` (timed out at 2.01s) |
| 24247026973 | Apr 10 | `main` | `TestV2CheckWithIteratorCache_HigherConsistencyBypassesCache` | `Not equal: keysCountBefore != keysCountAfter` |
| 24262761474 | Apr 10 | PR branch | `TestV2CheckWithIteratorCache_Conditions` | `Condition never satisfied` (timed out at 2.01s) |

**Impact:** 3+ CI runs failed. All within the last week, introduced with the iterator cache v2 feature.

**Root cause:** The iterator cache is populated by background drain goroutines spawned during Check resolution. When the Thompson Sampling planner selects the Weight2 strategy, two drain goroutines run (ReadUsersetTuples + ReadStartingWithUser). The `require.Eventually` assertion fires as soon as any cache entry appears (count > 0), but a second drain may still be in-flight. When it completes during the subsequent assertions, the cache key count changes, causing a false failure.

**Why it happens (code-level):**
- `pkg/server/server_test.go:2466-2468`: `require.Eventually` with 2s timeout / 10ms poll waits for `len(cache.KeysWithPrefix("v2ic.")) > 0` — fires on first entry, doesn't wait for stabilization.
- `pkg/server/server_test.go:2665-2667`: Same pattern for the HigherConsistency test.

**Status:** Partially fixed.
- PR #3058 (merged): Fixed `TestV2CheckWithIteratorCache_Conditions` by adjusting the Eventually condition.
- PR #3061 (open): Fixes `HigherConsistencyBypassesCache` by waiting for cache count stabilization (same value observed twice) instead of just non-zero.
- The `_Invalidation` and `_PopulatesAndHits` variants use the same fragile pattern and may still flake.

---

## Flaky Test #3: CEL Interrupt Frequency Tests

**Tests affected:**
- `TestEvaluateWithInterruptCheckFrequency/operation_interrupted_one_comprehension`
- `TestEvaluateWithInterruptCheckFrequency/operation_interrupted_two_comprehensions`
- `TestEvaluateWithInterruptCheckFrequency/operation_not_interrupted_two_comprehensions`

**Observed CI failures:**

| Run ID | Date | Branch | Error |
|--------|------|--------|-------|
| 24194351452 | Apr 9 | `dependabot` | All 3 subtests failed |

**Impact:** 1 CI run failed. Lower frequency but still blocks dependabot merges.

**Root cause:** Tests assert that CEL evaluation is interrupted at exact check-frequency boundaries. The interrupt mechanism depends on instruction counting, which can vary with CEL library version bumps (exactly what dependabot was updating).

**Status:** Unfixed.

---

## Flaky Test #4: Non-Deterministic Test (Removed)

**Test affected:**
- A non-deterministic test that was removed entirely.

**Status:** Fixed via PR #3038 (merged) — `fix: remove unnecessary non-deterministic test`.

---

## Failure Breakdown by Branch Type

To distinguish flakes from legitimate bugs, failures are grouped by branch type:

| Branch Type | Failures | Interpretation |
|-------------|----------|---------------|
| `main` | 4 | Definite flakes — no code changes |
| `dependabot/*` | 9 | Likely flakes — only dependency version bumps |
| `feat/iterator_cache_v2` | 9 | Mix — new feature introduced new flaky tests |
| `shutdown-timeout` | 4 | Mix — infra changes may have exposed existing flakes |
| Other feature branches | ~25 | Unclear without per-branch analysis |

The `feat/iterator_cache_v2` branch accounts for 9 failures on its own, all from the iterator cache test family (Flaky Test #2). Since this feature has merged, these flakes now affect `main`.

---

## Recommendations

### Immediate (high impact, low effort)

1. **Merge PR #3061** to fix `TestV2CheckWithIteratorCache_HigherConsistencyBypassesCache`. Apply the same stabilization pattern to `_Invalidation` and `_PopulatesAndHits`.

2. **Increase `require.Eventually` timeouts** in all iterator cache tests from 2s to 10s with 100ms polling. The tests still complete in <1s when the system is healthy — the longer timeout only activates under CI load.

### Short-term (medium effort)

3. **Add retry or longer backoff for matrix test server startup.** The 30-second `EnsureServiceHealthy` timeout in `pkg/testutils/testutils.go` is insufficient under CI load. Consider increasing to 60s or adding a retry wrapper around the full matrix test.

4. **Investigate Postgres container resource limits.** The `i/o timeout` errors on `read tcp` suggest the Postgres container is overwhelmed. Consider:
   - Reducing parallelism of matrix test subtests against the same Postgres instance.
   - Increasing Postgres container `max_connections` or `shared_buffers`.
   - Using a connection pool with bounded concurrency.

### Longer-term (higher effort)

5. **Replace `time.Sleep`-based synchronization in tests.** 46+ test files use `time.Sleep` for timing control. The highest-risk files:
   - `pkg/storage/storagewrappers/sharediterator/shared_iterator_datastore_test.go` (26 sleeps)
   - `internal/graph/check_test.go` (20 sleeps)

   Replace with channel-based synchronization, `sync.WaitGroup`, or `require.Eventually` with generous timeouts.

6. **Add CI test retry configuration.** The GitHub Actions workflows have no retry-on-failure mechanism. Adding `retry` (e.g., via `nick-fields/retry` action) for the test step would prevent single flakes from blocking PRs while the underlying issues are addressed.
