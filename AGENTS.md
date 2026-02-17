# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Goals and Non-Goals

### Goals

- **ReBAC authorization engine** inspired by Google Zanzibar with graph evaluation (union, intersection, exclusion)
- **Low-latency checks** (sub-millisecond to single-digit ms) with minimal memory allocations
- **Bounded concurrency** with breadth/depth limits, dispatch throttling, multi-layer caching
- **Deployment flexibility**: Service mode (HTTP/gRPC APIs) or library mode (embeddable Go library)
- **SQL storage**: PostgreSQL (production), MySQL, SQLite (beta) with ACID properties
- **Production-grade observability**: Prometheus metrics, OpenTelemetry tracing, structured logging
- **Developer experience**: Playground UI, SDKs (Java, Node.js, Go, Python, .NET), YAML test matrices

### Non-Goals

**Storage Backends:**
- NoSQL databases (MongoDB, DynamoDB, Cassandra, Redis as primary storage)

**Identity and Authentication:**
- Identity provider functionality (user creation, password management, user profiles)
- User authentication and credential validation
- LDAP or SAML integration for user authentication

**Authorization Paradigms:**
- Pure Attribute-Based Access Control (ABAC) without relationships

**API Protocols:**
- GraphQL API

**Audit and Compliance:**
- Unlimited historical audit logs with full change tracking

**General-Purpose Features:**
- General-purpose database or key-value store
- Business logic execution or workflow orchestration
- Rate limiting or API gateway features

**Production Features:**
- Production deployment of Playground UI (localhost development only)
- Single-node in-memory storage for production (ephemeral, dev-only)

## Testing Guidelines

### Test-Driven Development Workflow

**IMPORTANT**: Always follow TDD cycle:
1. **Write failing test** - Validates expected behavior before implementing
2. **Observe failure** - Confirms test works correctly
3. **Implement minimal code** - Make test pass
4. **Run related tests/benchmarks** - Detect regressions
5. **Refactor** - Improve code while keeping tests green

**Critical**: Must observe test failure before implementing to ensure test validates behavior.

### Test Organization by Component

**Storage**: Integration tests in `pkg/storage/test/storage.go` using `RunAllTests` pattern (all backends: memory, postgres, mysql, sqlite)

**API**:
- Unit tests with mocks in `pkg/server/commands/*_test.go` (use `mockgen`)
- Functional tests in `tests/functional_test.go` (end-to-end with real server)
- Query API tests (Check, ListObjects, ListUsers) in `tests/check/`, `tests/listobjects/`, `tests/listusers/` (YAML matrix testing)

**Graph**: Unit tests in `internal/graph/*_test.go` (test recursion limits, concurrency, set operations)

**TypeSystem**: Validation tests in `pkg/typesystem/*_test.go` (valid/invalid models, CEL conditions)

**Server config**: Flag tests in `pkg/server/server_test.go`, header tests in `cmd/run/run_test.go`, Docker tests in `cmd/openfga/main_test.go`

### Coverage Target

- **Target 95% test coverage** for new code (aspirational goal)
- CI currently enforces 85% via `codecov.yml` - aim to exceed this
- All exported functions **must** have unit tests
- All exported functions **must** have benchmark tests
- Critical paths (Check, ListObjects, ListUsers) **must** have integration tests

### Test Naming

- Pattern: `Test<FunctionName>` for main test function
- Use nested `t.Run("scenario_description")` for subtests
- Subtest names: `lowercase_with_underscores` describing scenario
- Be descriptive: test names should clearly indicate what is being tested

### Table-Driven Tests

- Use `[]struct{name, input, expected}` pattern for multiple scenarios
- **Always** include `name` field for clear test identification
- Use descriptive field names (not generic `in`/`out`)
- Makes it easy to add new test cases and understand failures

### Mocking

- Use `gomock` for interface mocking (never use concrete types in tests when interface exists)
- Generate mocks with `go generate ./...` (uses `mockgen`)
- Pattern: `mockController := gomock.NewController(t)` with `defer mockController.Finish()`
- Use `EXPECT().Times(n).DoAndReturn(func)` for behavior specification
- Use `gomock.Any()` for arguments you don't care about checking

### Assertions

- Use `testify/require` for fatal assertions (test stops on failure)
- Use `testify/assert` for non-fatal assertions (test continues)
- Use `go-cmp/cmp` with `protocmp` for complex protobuf comparisons
- Choose the right assertion level: use `require` for setup, `assert` for multiple checks

### Benchmarks

**All exported functions must have benchmark tests.** This ensures performance regressions are detected early.

- Pattern: `Benchmark<FunctionName>(b *testing.B)`
- Use `b.ResetTimer()` after setup to exclude initialization from measurements
- Loop with `for i := 0; i < b.N; i++` for operation under test
- Include `b.Cleanup()` for resource cleanup
- Run with `-benchmem` flag for memory profiling
- Compare before/after with `benchcmp` tool

### Cleanup and Leak Detection

- Use `t.Cleanup(func)` for resource cleanup (preferred over `defer` in tests)
- Use `goleak.VerifyNone(t)` for goroutine leak detection in critical tests
- Close datastores and connections properly in cleanup
- Cleanup runs in reverse order, ensuring proper teardown

### Integration Tests

- Storage layer: Use exported `RunAllTests()` pattern in `pkg/storage/test/storage.go`
- Run against all backends: memory, postgres, mysql, sqlite
- Use `storagetest` package for backend-agnostic tests

### Test Utilities

- Use helpers from `internal/testutils/` for common setup
- Mark helpers with `t.Helper()` to improve error reporting line numbers
- Extract common test data setup into reusable functions

### Best Practices Summary

- Test both success and failure paths with edge cases (empty input, nil values, boundary conditions)
- Test error scenarios: authorization failures, validation errors, storage errors, timeouts
- Benchmark critical paths (Check API, graph resolution, storage queries)
- Use YAML matrix tests for query APIs (Check, ListObjects, ListUsers) to cover model variations
- Add comments to complex YAML test cases explaining the authorization model and expected behavior

## Code Style

Import aliases:
- `openfgav1` for `github.com/openfga/api/proto/openfga/v1`
- `parser` for language transformer

Import order: standard → external → github.com/openfga → local module

## Code Conventions

This section defines code quality standards for the OpenFGA codebase. AI agents should follow these conventions when writing, reviewing, or modifying code. The emphasis is on writing code that is clear, maintainable, and consistent with existing patterns throughout the project.

### Documentation Comments

Explain **why** and **how to use**, not just **what**. Provide context and usage guidance.

- **Exported types/functions**: Must document purpose, parameters, return values, error conditions, edge cases
- **Interface methods**: Document preconditions, error conditions, behavioral guarantees
- **Constants**: Explain rationale ("why this value?") not just definition
- **Private functions**: Brief comments only for non-obvious logic
- **Inline comments**: Sparingly, for gotchas or architectural decisions
- **Cross-references**: Use `[[package.Type]]` syntax

### Naming Conventions

- **Interfaces**: No "I" prefix (use `CheckResolver` not `ICheckResolver`), descriptive role/action names
- **Structs**: PascalCase compound names indicating responsibility (e.g., `CachedCheckResolver`, `LocalChecker`)
- **Functions**: Constructors `New<Type>()`, options `With<Property>()`, option types `<Type>Option func(*<Type>)`, methods use verb form
- **Variables**: Short in narrow scopes (`ctx`, `err`, `req`), descriptive for significant variables (`checkResolver`)
- **Constants**: Exported PascalCase, unexported camelCase, context keys use `<name>CtxKey` suffix
- **Errors**: Sentinel pattern `Err<Name>` (e.g., `ErrNotFound`)

### Error Handling Conventions

- **Sentinel errors**: `var Err<Name> = errors.New("message")` pattern, grouped in `var ()` blocks
- **Wrapping**: Always use `fmt.Errorf("context: %w", err)` (never `%v`), preserve error chain
- **Custom types**: Implement `Error()`, `Unwrap()`, optionally `Is()` for sentinel comparison
- **Checking**: Use `errors.Is(err, sentinel)` and `errors.As(err, &targetType)`, never `err == sentinel`
- **Mapping**: Converter functions `<Command>ErrorToServerError` for domain → API error mapping

## Maintaining This File

Update this file when making significant architectural changes:
- New packages/APIs: Document in Architecture section
- Test patterns: Add to Testing Guidelines
- Graph/ListObjects changes: Review matrix tests in `tests/check/` and `tests/listobjects/`

## Development Commands

```bash
# Build
go build -o ./dist/openfga ./cmd/openfga
go install ./cmd/openfga  # Install to $GOBIN

# Testing
go generate ./...                              # Generate mocks
go test -race ./...                           # Run all tests
go test -race -run "TestCheckLogs" ./...      # Specific test
go test -bench=. -benchmem ./...              # Benchmarks
go test -tags=docker ./cmd/openfga/...        # Docker tests

# Development server
./openfga run  # In-memory storage

# Linting
golangci-lint run --fix ./...
go mod tidy

# Migrations
openfga migrate --datastore-engine postgres --datastore-uri 'postgres://...'
```

## Architecture

OpenFGA is a high-performance authorization engine inspired by Google Zanzibar. It evaluates relationship-based access control queries.

### Core Request Flow

```text
HTTP/gRPC Request → Middleware → Server Handler → Command → Graph Resolution → Storage → Response
```

### High-Level Structure

OpenFGA follows a layered architecture:

```text
┌─────────────────────────────────────────┐
│     HTTP/gRPC API Layer (Gateway)       │
├─────────────────────────────────────────┤
│        Middleware Stack                 │
│  (Auth, Logging, Validation, Timeout)   │
├─────────────────────────────────────────┤
│     Server Handlers (pkg/server/)       │
│  Check, Write, Read, ListObjects, etc.  │
├─────────────────────────────────────────┤
│    Commands (pkg/server/commands/)      │
│      Business Logic Layer               │
├─────────────────────────────────────────┤
│  Graph Resolution (internal/graph/)     │
│    Authorization Check Engine           │
├─────────────────────────────────────────┤
│   Storage Abstraction (pkg/storage/)    │
│  Memory, Postgres, MySQL, SQLite        │
└─────────────────────────────────────────┘
```

### Key Components

**`pkg/server/`**: API handlers (gRPC/HTTP), `commands/` decoupled business logic (Check, Write, Read, etc.)

**`internal/graph/`**: Core authorization engine. `LocalChecker` with resolver chain (Cache → DispatchThrottling → Shadow → LocalChecker). Handles DirectEdge, ComputedUsersetEdge, TupleToUsersetEdge with set operations (union, intersection, exclusion).

**`pkg/storage/`**: Abstract `OpenFGADatastore` interface with implementations (memory, postgres, mysql, sqlite). Storage wrappers: request (contextual tuples), iterator cache, query cache, throttling.

**`pkg/typesystem/`**: Parses/validates authorization models (DSL), type definitions, relations, CEL conditions. `weighted_graph.go` optimizes query paths.

**`internal/validation/`**: Validates tuples/requests against type system, type restrictions, CEL conditions.

**`internal/authn/` and `internal/authz/`**: Authentication (None, Preshared Key, OIDC) and API authorization.

### Key APIs

- **Check** - Verify if user has relation to object
- **ListObjects** - List objects user can access
- **ListUsers** - List users with access to object
- **Write** - Create/delete relationship tuples
- **BatchCheck** - Batch authorization checks

### Ports

- 8080: HTTP API
- 8081: gRPC API
- 3000: Playground UI
- 2112: Prometheus metrics

### Request Flow: Check API

```text
1. HTTP/gRPC request arrives
2. Middleware: Recovery → RequestID → StoreID → Validation → Logging → Auth
3. Server.Check() handler
4. Load TypeSystem from storage (cached)
5. Build resolver chain with config options
6. CheckCommand.Execute():
   - Validate request against TypeSystem
   - Wrap storage with contextual tuples + caches
7. Resolver chain execution:
   - Check query cache → return if hit
   - Check dispatch throttling → queue if needed
   - Shadow resolver (optional A/B testing)
   - LocalChecker:
     * Get relationship edges from TypeSystem
     * For each edge: query storage, recurse, apply set operations
     * Return allowed/denied result
8. Collect metadata (query count, duration)
9. Record Prometheus metrics
10. Return CheckResponse
```

### Request Flow: BatchCheck API

**Extends Check API with:**
- Processes multiple checks concurrently (bounded parallelism via `sourcegraph/conc` pool)
- Deduplication using xxhash cache keys (correlation IDs → unique checks)
- Shared cache controller across all checks for maximum hit rate
- Per-check error handling (individual failures don't fail batch)
- Key config: `--max-checks-per-batch-check`, `--max-concurrent-checks-per-batch-check`
- Files: `pkg/server/batch_check.go`, `pkg/server/commands/batch_check_command.go`

### Request Flow: ListObjects API

**Reverse expansion** (graph traversal backwards from relation to tuples) to find objects user can access:
- **Three algorithmic strategies**: Weighted Graph (cost-based optimization, default), Pipeline (experimental message-passing with O(1) memory), Classic (recursive fallback)
- **Two-phase processing**: Reverse expansion yields candidates → Check API validates conditional results (intersections/exclusions)
- **Result status**: NoFurtherEvalStatus (direct match) vs RequiresFurtherEvalStatus (needs Check validation)
- **Streaming support**: `StreamedListObjects` for large result sets
- Key config: `--listobjects-deadline`, `--listobjects-max-results`, `--experimentals=pipeline-listobjects`
- Files: `pkg/server/list_objects.go`, `pkg/server/commands/reverseexpand/`

### Request Flow: ListUsers API

**Reverse expansion** to find users with access to an object:
- Traverses backwards from object+relation to users
- **User filtering**: Optional filters by user type and relation
- **Typed wildcards**: Special handling for public access (`type:*`)
- **Cycle detection**: Uses `visitedUsersetsMap` to prevent infinite loops
- **Standalone architecture**: Doesn't use CheckResolver chain (simpler, specialized)
- Key config: `--listusers-deadline`, `--listusers-max-results`
- Files: `pkg/server/list_users.go`, `pkg/server/commands/listusers/`

### Resolver Chain Pattern

Resolvers are composed in a chain using circular linked list:

```text
[CacheResolver] → [DispatchThrottlingResolver] → [ShadowResolver] → [LocalChecker] ⟲
```

Each resolver can:
- Handle the request directly (e.g., cache hit)
- Delegate to next resolver in chain
- Decorate the response (e.g., add metrics)

### Concurrency & Performance

- **Breadth limit**: Controls concurrent goroutines per recursion level
- **Depth limit**: Prevents stack overflow from deep model nesting
- **Datastore throttling**: Suspends queries when DB concurrency is high
- **Caching**: Multi-layer (model cache, iterator cache, query cache)
- **Planner**: Thompson Sampling to select best resolver strategy
- **Shadow resolver**: A/B test new strategies without affecting production

## Important Patterns

- **Functional Options**: Components use `New<Type>(opts ...Option)` pattern with `With<Property>()` options
- **Context Threading**: TypeSystem, request metadata (store_id, model_id), and telemetry (traces, request_id) propagated via context
- **Interface-Driven Design**: Storage, authenticators, resolvers use interfaces for testability (mocks via `mockgen`) and multiple implementations

## Key Files to Understand

- `pkg/server/server.go` - Server initialization and handler registration
- `pkg/server/check.go` - Check API handler (most important API)
- `internal/graph/check.go` - `LocalChecker` core resolution logic
- `pkg/storage/storage.go` - Storage interface definitions
- `pkg/typesystem/typesystem.go` - Authorization model parsing/validation
- `cmd/run/run.go` - Server startup, configuration, graceful shutdown
- `internal/graph/builder.go` - Resolver chain construction

## Configuration

Via CLI flags (`./openfga run --help`), environment variables (`OPENFGA_*`), or YAML config file (`.config-schema.json`)

Key flags: `--datastore-engine`, `--datastore-uri`, `--grpc-addr`, `--http-addr`, `--playground-enabled`, `--check-query-cache-enabled`, `--experimentals`

## API Structure

Main endpoints (all in `pkg/server/`):
- **Check**: Is user allowed? (core authorization check)
- **ListObjects**: What objects can user access?
- **ListUsers**: What users have access to object?
- **Write**: Create/delete relationship tuples
- **Read**: Read tuples (debugging)
- **Expand**: Expand relation recursively (debugging)
- **ReadAuthorizationModel**: Get model definition
- **WriteAuthorizationModel**: Create new model version
- **ReadChanges**: Get changelog (audit log)

## Storage Backends

- **Postgres** (recommended for production): Full feature support, pgx connection pooling, PostgreSQL 14+
- **MySQL**: Stricter length limits on tuple properties
- **SQLite** (beta): Single-file database, good for testing, limited concurrency
- **Memory**: Development only, ephemeral (lost on restart)

## Common Development Tasks

**New API endpoint**: Define protobuf in [openfga/api](https://github.com/openfga/api) repo → handler in `pkg/server/{method}.go` → command in `pkg/server/commands/{method}_command.go` → unit/functional tests → update `TestHTTPHeaders` if needed

**Storage interface changes**: Update `pkg/storage/storage.go` → add test in `pkg/storage/test/storage.go` → implement for all backends (memory, postgres, mysql, sqlite)

**Graph resolver**: Implement `CheckResolver` interface in `internal/graph/` → add builder option in `internal/graph/builder.go` → unit tests with mocks

**TypeSystem changes**: Update `pkg/typesystem/typesystem.go` and `weighted_graph.go` → validation tests → check backward compatibility

## Telemetry

- **Logs**: Structured logging via zap (debug/info/warn/error), request-scoped fields (request_id, store_id)
- **Metrics**: Prometheus (request duration, dispatch count, datastore query count/duration, cache hit/miss, throttled requests)
- **Traces**: OpenTelemetry distributed tracing, spans per operation, OTLP collector export

## Security Considerations

When modifying code:
- Validate all user inputs against TypeSystem
- Prevent injection attacks (SQL, command)
- Respect type restrictions in authorization models
- Always validate contextual tuples more strictly than stored tuples
- Use parameterized queries for all database operations
- Implement proper timeout handling to prevent DoS
- Be careful with recursion depth to prevent stack overflow

## Production Considerations

- Never use in-memory storage in production
- Configure appropriate connection pool sizes for storage
- Set reasonable resolve node limits (breadth/depth)
- Enable caching for better performance
- Configure authentication (OIDC or preshared keys)
- Monitor metrics for performance issues
- Set up distributed tracing for debugging
- Use database migrations before deploying new versions
- Consider read replicas for read-heavy workloads
