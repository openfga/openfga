# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

OpenFGA is a high-performance ReBAC (Relationship-Based Access Control) authorization engine inspired by Google Zanzibar. It evaluates graph-based authorization queries using set operations (union, intersection, exclusion).

**Project priorities** (in order):
1. **Correctness** — all authorization results must be accurate
2. **Reliability** — the service must be available
3. **Performance** — lowest possible latency and fewest memory allocations

## Development Commands

Use `make` targets, which are the source of truth for builds, tests, and linting.

```bash
make build                        # Build binary to ./dist/openfga
make test                         # Generate mocks + run all tests with race/coverage
make test FILTER="TestCheckLogs"  # Run specific test(s)
make test-bench                   # Run benchmark tests with -benchmem
make test-docker                  # Run tests requiring Docker
make lint                         # golangci-lint v2 with auto-fix
make generate-mocks               # Generate mock stubs (also run by `make test`)
make dev-run                      # Hot-reload dev server (in-memory)
DATASTORE="postgres" make dev-run # Hot-reload with postgres (requires Docker)
```

## Architecture

```text
HTTP/gRPC Request → Middleware → Server Handler → Command → Graph Resolution → Storage → Response
```

### Directory Structure

| Directory | Purpose |
|---|---|
| `cmd/run/run.go` | Server startup, configuration, graceful shutdown |
| `pkg/server/` | gRPC/HTTP API handlers |
| `pkg/server/commands/` | Business logic layer (decoupled from transport) |
| `internal/graph/` | Core authorization check engine (`LocalChecker`, resolver chain) |
| `internal/planner/` | Thompson Sampling strategy planner for resolver selection |
| `pkg/storage/` | `OpenFGADatastore` interface + implementations (memory, postgres, mysql, sqlite) |
| `pkg/typesystem/` | Authorization model parsing/validation, `weighted_graph.go` for query path optimization |
| `internal/validation/` | Tuple/request validation against type system |
| `internal/authn/`, `internal/authz/` | Authentication (None, Preshared Key, OIDC) and API authorization |

### Resolver Chain

Resolvers are composed as a **circular linked list** (last delegates back to first). Resolvers are **conditionally added** based on configuration:

```text
[CachedCheckResolver]? → [DispatchThrottlingCheckResolver]? → [ShadowResolver | LocalChecker] ⟲
```

- `CachedCheckResolver` - Added if caching enabled
- `DispatchThrottlingCheckResolver` - Added if throttling enabled
- `ShadowResolver` - Wraps two `LocalChecker` instances (main + shadow) for A/B testing; added only if shadow mode enabled. Otherwise, a plain `LocalChecker` is used.

Construction: `internal/graph/builder.go`. Interface: `internal/graph/interface.go`.

## Code Conventions

### Import Rules (lint-enforced)

Import aliases are **enforced by golangci-lint** (`importas` with `no-unaliased: true`):
- `openfgav1` for `github.com/openfga/api/proto/openfga/v1`
- `parser` for `github.com/openfga/language/pkg/go/transformer`

Import order (enforced by `gci`): standard → external → `github.com/openfga` → local module

### Naming Conventions

- **Interfaces**: No "I" prefix (`CheckResolver` not `ICheckResolver`)
- **Constructors**: `New<Type>()`, options: `With<Property>()`, option types: `<Type>Option func(*<Type>)`
- **Errors**: Sentinel pattern `Err<Name>` (e.g., `ErrNotFound`), wrap with `fmt.Errorf("context: %w", err)`
- **Constants**: Exported PascalCase, unexported camelCase, context keys: `<name>CtxKey`

### Error Handling

- Sentinel errors in `var ()` blocks: `var ErrNotFound = errors.New("not found")`
- Always wrap with `%w` (never `%v`), check with `errors.Is`/`errors.As` (never `==`)
- Domain → API error mapping via `<Command>ErrorToServerError` converter functions

## Testing

### Test-Driven Development

Follow TDD: write failing test → observe failure → implement → run tests → refactor.

### YAML Matrix Tests

Primary test format for Check, ListObjects, and ListUsers APIs. Test files embedded via `go:embed` in `assets/assets.go`:
- `assets/tests/consolidated_1_1_tests.yaml` - Core relationship tests
- `assets/tests/abac_tests.yaml` - Condition/CEL tests

```yaml
tests:
  - name: descriptive_test_name
    stages:
      - model: |
          model
            schema 1.1
          type user
          type document
            relations
              define viewer: [user]
        tuples:
          - object: document:1
            relation: viewer
            user: user:alice
        checkAssertions:
          - tuple: { object: document:1, relation: viewer, user: user:alice }
            expectation: true
        listObjectsAssertions:
          - request: { user: user:alice, type: document, relation: viewer }
            expectation: [document:1]
        listUsersAssertions:
          - request: { object: document:1, relation: viewer, filters: [user] }
            expectation: [user:alice]
```

Runners: `tests/check/check.go`, `tests/listobjects/listobjects.go`, `tests/listusers/listusers.go` (each has `RunAllTests()`).

## Files You Must Not Edit

- **`internal/mocks/`** - Auto-generated by `mockgen`. Run `make generate-mocks` to regenerate.
- **`*.pb.go`** - Generated protobuf code.
- **Protobuf definitions** - Live in [openfga/api](https://github.com/openfga/api) repo. Changes require a PR there first.

## Common Gotchas

- **Mock generation**: `make test` runs `make generate-mocks` automatically. If using `go test` directly, run `go generate ./...` first.
- **Import aliases enforced**: `openfgav1` and `parser` aliases are required. Raw import paths fail linting.
- **Storage integration tests need Docker**: Use `make test-docker` for Postgres/MySQL tests.
- **Resolver chain is circular**: Last resolver delegates back to first. Be aware when debugging `internal/graph/builder.go`.
- **Protobuf changes require separate repo**: Update `openfga/api` first, then `go get github.com/openfga/api@<version>`.
- **golangci-lint is v2**: Config file `.golangci.yaml` uses `version: "2"` format.

## Common Development Tasks

- **New API endpoint**: Define protobuf in `openfga/api` → handler in `pkg/server/{method}.go` → command in `pkg/server/commands/{method}_command.go` → tests
- **Storage interface changes**: Update `pkg/storage/storage.go` → test in `pkg/storage/test/storage.go` → implement for all backends
- **Graph resolver**: Implement `CheckResolver` interface (`internal/graph/interface.go`) → add builder option in `internal/graph/builder.go` → tests
- **TypeSystem changes**: Update `pkg/typesystem/typesystem.go` and `weighted_graph.go` → validation tests
