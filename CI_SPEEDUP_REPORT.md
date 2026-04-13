# CI Speed-Up Report — OpenFGA

**Date:** 2026-04-13
**Data source:** GitHub Actions CI runs, workflow YAML files, Makefile, test logs

---

## Methodology

1. **Workflow analysis:** Read all GitHub Actions workflow files (`.github/workflows/pull_request.yaml`, `main.yaml`) to map the full CI pipeline structure.

2. **Job-level timing:** Extracted start/completion timestamps from multiple successful PR runs using `gh run view <id> --json jobs` and computed durations per job.

3. **Step-level timing:** Drilled into the slowest jobs to measure individual step durations (setup, build, test execution, upload).

4. **Per-package test timing:** Extracted `go test` per-package durations from CI logs (the `ok github.com/openfga/... Xs` lines) to identify the slowest test packages.

5. **Test structure analysis:** Read test source files to understand how many server instances, Docker containers, and database connections are created during a single `make test` run.

6. **Coverage overhead analysis:** Identified the `-coverpkg=./...` flag in `make test` and assessed its impact on compilation and execution time.

---

## Current CI Pipeline Structure

The Pull Request workflow runs **6 parallel jobs**, all on `ubuntu-latest`:

```
┌─────────────────────────────────────────────────────────┐
│                    PR Workflow                           │
│                                                         │
│  ┌─────────┐ ┌────────────┐ ┌───────┐ ┌─────────────┐  │
│  │  lint   │ │   tests    │ │ bench │ │ goreleaser  │  │
│  │  ~1m    │ │  ~13m      │ │ ~7m   │ │ dry-run ~13m│  │
│  └─────────┘ └────────────┘ └───────┘ └─────────────┘  │
│                                                         │
│  ┌─────────────┐ ┌──────────────┐                       │
│  │ govulncheck │ │ docker-tests │                       │
│  │    ~1m      │ │    ~3m       │                       │
│  └─────────────┘ └──────────────┘                       │
│                                                         │
│  Wall-clock time: ~13-14 minutes (when runners          │
│  available), up to ~47 minutes with runner queuing      │
└─────────────────────────────────────────────────────────┘
```

**Critical path:** The `tests` job at ~13 minutes. It ties with `goreleaser dry-run` (~13 minutes) as the longest-running job.

---

## The `tests` Job Breakdown

The `tests` job runs `make test`, which executes:

```bash
go test -race -coverpkg=./... -coverprofile=coverageunit.tmp.out -covermode=atomic -count=1 -timeout=10m ${GO_PACKAGES}
```

This is a **single `go test` invocation** that runs all ~65 packages. Go runs packages in parallel (default: GOMAXPROCS, typically 4 on CI runners), but some packages are so slow they dominate the wall clock.

### Per-Package Duration (from successful run 24258258579)

| Rank | Package | Duration | What it does |
|------|---------|----------|-------------|
| 1 | `pkg/storage/sqlite` | **507s (8.4m)** | SQLite storage integration tests |
| 2 | `tests/check` | **371s (6.2m)** | Matrix tests: Check across Memory/Postgres/MySQL |
| 3 | `pkg/storage/mysql` | **263s (4.4m)** | MySQL storage integration tests |
| 4 | `tests/listobjects` | **222s (3.7m)** | Matrix tests: ListObjects across engines |
| 5 | `pkg/storage/postgres` | **139s (2.3m)** | Postgres storage integration tests |
| 6 | `tests/listusers` | **121s (2.0m)** | Matrix tests: ListUsers across engines |
| 7 | `pkg/server` | **104s (1.7m)** | Server unit tests + iterator cache tests |
| 8 | `internal/containers/mpmc` | **75s (1.2m)** | MPMC queue concurrency tests |
| 9 | `cmd/run` | **61s (1.0m)** | TLS, cert rotation, server startup tests |
| 10 | `internal/listobjects/pipeline` | **58s (1.0m)** | Pipeline processing tests |
| — | All other packages (~55) | **~60s total** | Mostly <2s each |

**Total CPU time: ~1981s (~33 minutes)**
**Wall-clock time: ~13 minutes** (packages run in parallel, max 4 concurrent)

### Key Observations

1. **`pkg/storage/sqlite` is the single longest package at 8.4 minutes.** It dominates the critical path — no other packages can use that CPU slot while it runs.

2. **Database tests run the same test suite 3-4 times across engines.** The matrix tests (`tests/check`, `tests/listobjects`, `tests/listusers`) each spin up full OpenFGA server instances with Docker-based Postgres/MySQL containers. `tests/check` alone creates **6 server instances** (3 experimental configs × 2 for Matrix + testRunAll).

3. **`-coverpkg=./...` forces recompilation.** This flag instruments *every* package for coverage, not just the package under test. Each of the ~65 test packages must recompile all ~65 packages with coverage instrumentation. This is a significant hidden cost.

4. **`-race` adds ~2-10x overhead.** The race detector is important for correctness but adds substantial CPU and memory overhead to every test.

5. **`internal/containers/mpmc` takes 75 seconds** for queue tests that mostly exercise concurrency timing — disproportionate for a container utility package.

---

## The `goreleaser dry-run` Job Breakdown

| Step | Duration |
|------|----------|
| Set up Go | 15s |
| GoReleaser dry run | **729s (12.1m)** |
| Everything else | <5s |

GoReleaser builds binaries for **3 OS × multiple architectures** (linux, windows, darwin), plus Docker images. This is a full cross-compilation of the project — 12 minutes for a dry run that only validates the release process won't break.

---

## The `go-bench` Job Breakdown

| Step | Duration |
|------|----------|
| Set up Go | 16s |
| Run benchmark | **416s (6.9m)** |
| Benchmark comparison | <1s |

Benchmarks run with `-bench . -benchtime 1s -timeout 0 -run=XXX -cpu 1 -benchmem` across all packages.

---

## Runner Queuing

In one observed run (24258258579), the `tests` job started **34 minutes after** all other jobs because no runner was available. All 6 jobs compete for runners simultaneously. This turned a 13-minute pipeline into a 47-minute wait.

---

## Recommendations

### Tier 1: High Impact, Low Effort

#### 1. Split `tests` into parallel jobs by package group

Instead of one monolithic `make test`, split into 3-4 jobs that run in parallel:

| Job | Packages | Est. Duration |
|-----|----------|---------------|
| `tests-storage` | `pkg/storage/sqlite`, `pkg/storage/mysql`, `pkg/storage/postgres`, `pkg/storage/memory` | ~8.5m (sqlite dominates) |
| `tests-matrix` | `tests/check`, `tests/listobjects`, `tests/listusers` | ~6.2m (check dominates) |
| `tests-unit` | Everything else (~55 packages) | ~2-3m |

This would reduce the critical path from ~13m to ~8.5m by running the storage and matrix tests concurrently on separate runners.

**Implementation:** Use `go test -run` or package lists in separate workflow jobs, merge coverage at the end.

#### 2. Remove `-coverpkg=./...` or scope it

Replace:
```bash
go test -race -coverpkg=./... -coverprofile=coverageunit.tmp.out ...
```

With per-package coverage:
```bash
go test -race -coverprofile=coverageunit.tmp.out ...
```

The `-coverpkg=./...` flag forces Go to instrument and recompile all ~65 packages for each test package, dramatically increasing compile time. Per-package coverage (`-cover` without `-coverpkg`) only instruments the package under test. The coverage numbers will be slightly different but still useful.

**Estimated savings:** 2-4 minutes of compilation overhead.

#### 3. Move `goreleaser dry-run` to a conditional or nightly job

At 12+ minutes, the goreleaser dry run is tied for the longest job, but it only validates release packaging — not code correctness. Options:

- **Run only when release-related files change** (`.goreleaser.yaml`, `Dockerfile`, `cmd/openfga/`).
- **Run on `main` only**, not on every PR.
- **Run nightly** alongside the existing `release-nightly.yaml`.

**Estimated savings:** Eliminates one runner slot for most PRs, reducing queuing pressure.

### Tier 2: Medium Impact, Medium Effort

#### 4. Use larger runners for the `tests` job

GitHub Actions provides `ubuntu-latest-4-cores` (or larger). The default `ubuntu-latest` has 4 vCPUs. Go's test parallelism is bound by GOMAXPROCS.

Doubling to 8 cores would allow more packages to run concurrently, especially helping when multiple slow packages queue behind each other.

**Estimated savings:** 3-5 minutes on the tests job.

#### 5. Share Docker containers across test functions

Currently, each call to `BuildClientInterface` with a database engine creates a **new Docker container** via `RunDatastoreTestContainer`. In `tests/check` alone, 3 separate Postgres containers are created for the matrix tests.

Refactoring to share a single Postgres and MySQL container per test package (with separate databases or schemas) would eliminate container startup overhead and reduce I/O contention.

**Estimated savings:** 1-2 minutes + significant reduction in flakiness (see FLAKY_TESTS_REPORT.md).

#### 6. Cache Go build artifacts between CI runs

The workflow defines `go-cache-paths` but **never uses them** in a cache action. The `actions/setup-go` action does cache `GOMODCACHE` via `cache-dependency-path`, but the build cache (`GOCACHE`) is not explicitly cached.

Adding:
```yaml
- uses: actions/cache@v4
  with:
    path: ${{ steps.go-cache-paths.outputs.go-build }}
    key: ${{ runner.os }}-go-build-${{ hashFiles('**/*.go') }}
    restore-keys: ${{ runner.os }}-go-build-
```

Would allow test binary compilation to be partially incremental across runs.

**Estimated savings:** 1-2 minutes on the first compilation phase.

### Tier 3: Higher Effort, Longer-Term

#### 7. Split storage integration tests into a separate workflow

The storage integration tests (`pkg/storage/sqlite`, `pkg/storage/mysql`, `pkg/storage/postgres`) run the same `storage.RunAllTests()` test suite across all backends. Each takes 2-8 minutes and requires Docker.

These could run as a **separate required check** that triggers only when storage-related code changes:
- `pkg/storage/**`
- `pkg/testfixtures/storage/**`
- `go.mod` / `go.sum` (dependency changes)

For non-storage PRs, this eliminates ~15 minutes of CPU time from the test run.

#### 8. Investigate SQLite test slowness

`pkg/storage/sqlite` at **507 seconds (8.4 minutes)** is the single longest package — 2x longer than MySQL and 3.6x longer than Postgres, despite testing the same interface. Possible causes:

- SQLite write contention (noted in the commented-out `TestMatrixSqlite`).
- Sequential transaction handling inherent to SQLite.
- Suboptimal test configuration (WAL mode, busy timeout).

Profiling this package could reveal quick wins.

#### 9. Move benchmarks out of the critical path

The `go-bench` job (7 minutes) runs on every PR to compare performance. For most code changes, this is informational, not blocking. Options:

- Make it a **non-required check** so it doesn't gate merges.
- **Only run when performance-sensitive code changes** (e.g., `internal/graph/`, `pkg/storage/`, `internal/listobjects/`).

#### 10. Reduce `internal/containers/mpmc` test duration

75 seconds for a container utility package is excessive. These tests use tight `time.After` timeouts and run concurrency stress tests. Consider:

- Reducing iteration counts in CI (controlled by an environment variable).
- Moving stress tests to a separate `//go:build stress` build tag that runs nightly.

---

## Projected Impact

| Change | Current | Projected | Savings |
|--------|---------|-----------|---------|
| Split tests into parallel jobs | 13m | 8.5m | **4.5m** |
| Remove `-coverpkg=./...` | — | — | **2-4m** |
| Conditional goreleaser dry-run | 13m (parallel) | 0m (most PRs) | **1 runner slot** |
| Larger runners (8-core) | 13m | 8-10m | **3-5m** |
| Share Docker containers | — | — | **1-2m + reliability** |
| Cache Go build artifacts | — | — | **1-2m** |

**Best case (all Tier 1 + Tier 2):** Critical path drops from **~13 minutes to ~5-6 minutes**.

**Minimum viable improvement (Tier 1 only):** Critical path drops from **~13 minutes to ~8 minutes**, with reduced runner queuing.

---

## Appendix: Raw Timing Data

### Successful Run Durations (3 samples)

| Run ID | tests | goreleaser | bench | docker-tests | govulncheck | lint |
|--------|-------|------------|-------|--------------|-------------|------|
| 24258258579 | 13m42s | 12m33s | 7m21s | 2m34s | 0m56s | 1m21s |
| 24256121596 | 12m45s | 13m05s | 7m44s | 3m04s | 1m02s | 0m54s |
| 24253493292 | 13m51s | 13m16s | 7m32s | 2m46s | 0m57s | 0m38s |

### Runner Queuing Observed

| Run ID | Expected Start | Actual `tests` Start | Queue Delay |
|--------|---------------|---------------------|-------------|
| 24258258579 | 18:36 | 19:10 | **34 minutes** |
| 24256121596 | 17:44 | 17:44 | 0 |
| 24253493292 | 16:38 | 16:38 | 0 |
