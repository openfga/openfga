# Plan: CI Speed-Up — OpenFGA

**Date:** 2026-04-13
**Status:** In Progress
**Data source:** GitHub Actions CI runs, workflow YAML files, Makefile, test logs

---

## Problem Statement

The CI pipeline takes ~13-14 minutes on the critical path (up to ~47 minutes with runner queuing). A single monolithic `tests` job runs all ~65 packages sequentially, and expensive jobs like `goreleaser dry-run` and `go-bench` run unconditionally on every PR.

---

## Analysis

### Current CI Pipeline Structure

The Pull Request workflow runs **6 parallel jobs**, all on `ubuntu-latest`:

```text
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

**Critical path:** The `tests` job at ~13 minutes, tied with `goreleaser dry-run` (~13 minutes).

### The `tests` Job Breakdown

The `tests` job runs `make test`, which executes:

```bash
go test -race -coverpkg=./... -coverprofile=coverageunit.tmp.out -covermode=atomic -count=1 -timeout=10m ${GO_PACKAGES}
```

This is a **single `go test` invocation** that runs all ~65 packages.

#### Per-Package Duration (from successful run 24258258579)

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

#### Key Observations

1. **`pkg/storage/sqlite` is the single longest package at 8.4 minutes.** It dominates the critical path.
2. **Database tests run the same test suite 3-4 times across engines.** The matrix tests each spin up full OpenFGA server instances with Docker-based Postgres/MySQL containers.
3. **`-coverpkg=./...` forces recompilation.** Each of the ~65 test packages must recompile all ~65 packages with coverage instrumentation.
4. **`-race` adds ~2-10x overhead.** Important for correctness but adds substantial CPU and memory overhead.
5. **`internal/containers/mpmc` takes 75 seconds** — disproportionate for a container utility package.

---

## Plan

### Phase 1: High Impact, Low Effort (Target: -5 minutes on critical path)

#### 1.1 Split `tests` into parallel jobs by package group

- [x] **Done** — Split into `tests-unit`, `tests-storage`, `tests-matrix` in both `pull_request.yaml` and `main.yaml`
- [x] **Done** — Add aggregate `tests` job for branch protection compatibility
- [x] **Done** — Add `test-unit`, `test-storage`, `test-matrix` Makefile targets

| Job | Packages | Est. Duration |
|-----|----------|---------------|
| `tests-storage` | `pkg/storage/sqlite`, `pkg/storage/mysql`, `pkg/storage/postgres`, `pkg/storage/memory` | ~8.5m (sqlite dominates) |
| `tests-matrix` | `tests/check`, `tests/listobjects`, `tests/listusers` | ~6.2m (check dominates) |
| `tests-unit` | Everything else (~55 packages) | ~2-3m |

#### 1.2 Remove `-coverpkg=./...` from split targets

- [x] **Done** — Split targets use per-package coverage for faster compilation

**Estimated savings:** 2-4 minutes of compilation overhead.

#### 1.3 Gate `goreleaser dry-run` on path filters

- [x] **Done** — Only runs when release-related files change (`.goreleaser.yaml`, `Dockerfile`, `cmd/openfga/`)

**Estimated savings:** Eliminates one runner slot for most PRs.

### Phase 2: Medium Impact, Medium Effort

#### 2.1 Use larger runners for test jobs

- [x] **Done** — Test jobs now use `ubuntu-latest-16-cores`

**Estimated savings:** 3-5 minutes on the tests job.

#### 2.2 Cache Go build artifacts between CI runs

- [x] **Done** — Added `actions/cache` for `GOCACHE` keyed on source file hashes

**Estimated savings:** 1-2 minutes on the first compilation phase.

#### 2.3 Gate `go-bench` on performance-sensitive path filters

- [x] **Done** — Only runs when `internal/graph/`, `pkg/storage/`, `internal/listobjects/`, etc. change

#### 2.4 Share Docker containers across test functions

- [ ] **Not started** — Refactor matrix tests to share a single Postgres/MySQL container per test package

**Estimated savings:** 1-2 minutes + significant reduction in flakiness.

### Phase 3: Higher Effort, Longer-Term

#### 3.1 Split storage integration tests into a separate workflow

- [ ] **Not started** — Trigger only when storage-related code changes

#### 3.2 Investigate SQLite test slowness

- [x] **Partially done** — Increased `busy_timeout` to 5000ms and added `synchronous(NORMAL)` pragma

Further investigation needed: SQLite at 507s is 2x longer than MySQL and 3.6x longer than Postgres.

#### 3.3 Reduce `internal/containers/mpmc` test duration

- [x] **Done** — Reduced test message count from 1000 to 200; shrunk "large" test case capacity/cycles

---

## Projected Impact

| Change | Current | Projected | Savings |
|--------|---------|-----------|---------|
| Split tests into parallel jobs | 13m | 8.5m | **4.5m** |
| Remove `-coverpkg=./...` | — | — | **2-4m** |
| Conditional goreleaser dry-run | 13m (parallel) | 0m (most PRs) | **1 runner slot** |
| Larger runners (4-core) | 13m | 8-10m | **3-5m** |
| Share Docker containers | — | — | **1-2m + reliability** |
| Cache Go build artifacts | — | — | **1-2m** |

**Best case (all Phase 1 + Phase 2):** Critical path drops from **~13 minutes to ~5-6 minutes**.

**Minimum viable improvement (Phase 1 only):** Critical path drops from **~13 minutes to ~8 minutes**, with reduced runner queuing.

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
