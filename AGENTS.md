# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Build & Development Commands

```bash
make build                    # Build binary to dist/openfga
make test                     # Run all tests with race detection
make test FILTER="TestName"   # Run specific test by name
make lint                     # Run golangci-lint with auto-fix
make generate-mocks           # Generate mock stubs using mockgen
make dev-run                  # Run server with hot reloading (in-memory storage)
make dev-run DATASTORE="postgres"  # Run with Postgres
make dev-run DATASTORE="mysql"     # Run with MySQL
```

## Architecture Overview

OpenFGA is a high-performance authorization engine inspired by Google Zanzibar. It evaluates relationship-based access control queries.

### Core Request Flow
```
HTTP/gRPC Request → Middleware → Server Handler → Command → Graph Resolution → Storage → Response
```

### Key Packages

**pkg/server/** - gRPC/HTTP server implementation
- `server.go` - Main server struct implementing OpenFGAServiceServer
- `commands/` - Business logic for each API (Check, Write, ListObjects, etc.)
- `commands/reverseexpand/` - Core ListObjects implementation using reverse expansion

**internal/graph/** - Authorization check resolution
- `check.go` - Recursive graph traversal for Check API
- `cached_resolver.go` - Caching layer for check results
- CheckResolver interface chains: caching → throttling → local resolution

**pkg/storage/** - Storage interfaces and adapters
- `storage.go` - Core interfaces (RelationshipTupleReader, RelationshipTupleWriter, OpenFGADatastore)
- Adapters: `memory/`, `postgres/`, `mysql/`, `sqlite/`
- `storagewrappers/` - Decorators for caching, request bounding

**pkg/typesystem/** - Authorization model parsing and validation

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

## Testing Guidelines

### Test-Driven Development Workflow

**IMPORTANT**: Always follow the test-driven development (TDD) cycle when making code changes:

1. **Write a failing test** - Before implementing any feature or fix, write a test that validates the expected behavior
2. **Observe the test failure** - Run the test to confirm it fails for the expected reason (this validates the test itself)
3. **Implement the minimal code** - Write only enough code to make the test pass
4. **Execute related tests and benchmarks** - Run all relevant tests and benchmarks to detect regressions
5. **Refactor if needed** - Improve code quality while keeping tests green

**Critical requirement**: You must observe a test failure before implementing code. This ensures:
- The test actually validates the behavior you're implementing
- You understand the problem clearly
- The test suite would catch regressions if the feature breaks later

### Benchmark Requirements

**All exported functions must have benchmark tests.** This ensures performance regressions are detected early.

```bash
# Run all benchmark tests
make test-bench

# Run specific benchmark
make test-bench FILTER="BenchmarkCheck"

# Compare benchmarks before/after changes
go test -bench=. -benchmem ./... > before.txt
# (make changes)
go test -bench=. -benchmem ./... > after.txt
benchcmp before.txt after.txt
```

When adding new exported functions, create corresponding benchmark tests in `*_test.go` files following the pattern:
```go
func BenchmarkYourFunction(b *testing.B) {
    // Setup
    for i := 0; i < b.N; i++ {
        // Call function being benchmarked
    }
}
```

### Test Organization by Component

**Storage changes**:
- Add integration tests in `pkg/storage/test/storage.go`
- Use the `RunAllTests` pattern to ensure tests run against all storage backends (memory, postgres, mysql, sqlite)
- Test edge cases: pagination, filtering, transaction boundaries, concurrent access

**API changes**:
- Command unit tests with mocks in `pkg/server/commands/*_test.go`
  - Use `mockgen` to generate mocks for dependencies (storage, typesystem, etc.)
  - Test success paths, error scenarios, validation failures, authorization checks
- Functional tests in `tests/functional_test.go`
  - Full end-to-end API tests with real server setup
  - Test HTTP/gRPC protocol behaviors
- Query API tests (Check, ListObjects, ListUsers) in `tests/check/`, `tests/listobjects/`, `tests/listusers/`
  - YAML-based test cases for matrix testing (different models + data combinations)
  - Test complex authorization model patterns (TTU, computed usersets, intersections, exclusions)
  - Use patterns like `TestMatrixMemory`, `TestMatrixPostgres` to test across backends

**Graph resolver changes**:
- Unit tests in `internal/graph/*_test.go` with mocked storage
- Test recursion limits, concurrency behavior, cycle detection
- Verify set operations (union, intersection, exclusion) work correctly

**TypeSystem changes**:
- Validation tests in `pkg/typesystem/*_test.go`
- Test valid and invalid model definitions
- Test type restrictions, CEL conditions, relation metadata queries

**Docker/CLI changes**:
- Tests in `cmd/openfga/main_test.go`
- Test flag parsing, configuration loading, startup/shutdown behavior

### Test Execution Workflow

When making code changes, follow this bash command workflow:

```bash
# 1. Generate mocks (required after interface changes)
make generate-mocks

# 2. Run the specific failing test to observe failure
make test FILTER="TestYourNewTest"

# 3. Implement the code change

# 4. Run the specific test to verify it passes
make test FILTER="TestYourNewTest"

# 5. Run all related tests to detect regressions
make test FILTER="TestComponentName"

# 6. Run benchmarks for affected components
make test-bench FILTER="BenchmarkComponentName"

# 7. Run full test suite before committing
make test
```

### Best Practices

- **Isolation**: Use mocks to isolate the unit being tested. Generate mocks with `make generate-mocks`
- **Coverage**: Test both success and failure paths. Include edge cases (empty input, nil values, boundary conditions)
- **Error scenarios**: Test authorization failures, validation errors, storage errors, timeout scenarios
- **Focused benchmarks**: Benchmark critical paths (Check API, graph resolution, storage queries)
- **Matrix tests**: For query APIs (Check, ListObjects, ListUsers), use YAML test cases to cover model variations
- **Test helpers**: Use utilities from `internal/testutils` for common setup (creating test stores, models, tuples)
- **Documentation**: Add comments to complex YAML test cases explaining the authorization model and expected behavior

## Code Style

Import aliases:
- `openfgav1` for `github.com/openfga/api/proto/openfga/v1`
- `parser` for language transformer

Import order: standard → external → github.com/openfga → local module

## Maintaining This File

Keep this file up to date when making significant changes to the codebase:

- **New packages**: Add to Key Packages section if architecturally significant
- **New APIs**: Document in Key APIs section
- **Build changes**: Update Build & Development Commands
- **Port changes**: Update Ports section
- **New test patterns**: Add to Testing Guidelines
- **Import conventions**: Update Code Style if new aliases are adopted
- **Graph/ListObjects changes**: When modifying `internal/graph/` or `server/commands/reverseexpand/`, review and stress test matrix tests in `tests/check/` and `tests/listobjects/` to ensure edge cases are covered

Review this file during major refactors to ensure accuracy.

## Development Commands

### Building & Installing
```bash
# Build the binary (output to ./dist/openfga by default)
make build

# Build to custom location
BUILD_DIR="." make build

# Install to $GOBIN (ensure $GOBIN is on $PATH)
make install

# Build directly with go
go build -o ./openfga ./cmd/openfga
```

### Testing
```bash
# Run all tests (requires mock generation first)
make test

# Run specific test with filter
make test FILTER="TestCheckLogs"

# Run Docker-specific tests (requires Docker)
make test-docker

# Run benchmark tests
make test-bench

# Generate mock stubs (required before running tests)
make generate-mocks
```

### Development Server
```bash
# Run with hot-reload (in-memory storage)
make dev-run

# Run with MySQL
DATASTORE="mysql" make dev-run

# Run with Postgres
DATASTORE="postgres" make dev-run

# Run with SQLite
DATASTORE="sqlite" make dev-run

# Run directly without hot-reload
./openfga run

# Run with specific storage backend
./openfga run --datastore-engine postgres --datastore-uri 'postgres://postgres:password@localhost:5432/postgres'
```

### Linting & Code Quality
```bash
# Run linter with auto-fix
make lint

# Download/update dependencies
make deps
```

### Database Migrations
```bash
# Run migrations
openfga migrate --datastore-engine postgres --datastore-uri 'postgres://...'
```

## Architecture

### High-Level Structure

OpenFGA follows a layered architecture:

```
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

#### `pkg/server/`
- API endpoint handlers (gRPC and HTTP via grpc-gateway)
- `commands/` - Decoupled business logic (Check, Write, Read, etc.)
- Each API follows pattern: Handler → Validate → Load TypeSystem → Execute Command → Return

#### `internal/graph/`
- **Core authorization resolution engine**
- `LocalChecker` - Implements recursive graph traversal for Check operations
- Resolver chain pattern: Cache → DispatchThrottling → Shadow → LocalChecker
- Handles three edge types:
  - **DirectEdge**: Direct tuple relationships (e.g., `doc:1#viewer@user:alice`)
  - **ComputedUsersetEdge**: Computed relations (e.g., `viewer = owner`)
  - **TupleToUsersetEdge**: TTU relations (e.g., `viewer from parent`)
- Supports set operations: union, intersection, exclusion
- Concurrency controlled by breadth/depth limits

#### `pkg/storage/`
- Abstract storage interface: `OpenFGADatastore`
- Implementations: `memory/`, `postgres/`, `mysql/`, `sqlite/`
- Storage wrappers add features:
  - `Request storage wrapper` - Injects contextual tuples
  - `Iterator cache` - Caches tuple queries
  - `Query cache` - Caches check results
  - `Throttling wrapper` - Rate limits DB operations

#### `pkg/typesystem/`
- Parses and validates authorization models (DSL)
- Validates type definitions, relations, type restrictions, TTU relations, CEL conditions
- Provides query methods for relation metadata
- `weighted_graph.go` - Analyzes models to optimize query paths

#### `internal/validation/`
- Validates tuples and requests against the type system
- Type restriction checking
- Tuple set relation validation
- Condition evaluation (CEL)

#### `internal/authn/` and `internal/authz/`
- `authn/` - Authentication: None, Preshared Key, OIDC
- `authz/` - Authorization for API methods (uses FGA store to check API access)

### Request Flow: Check API

```
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

### Resolver Chain Pattern

Resolvers are composed in a chain using circular linked list:

```
[CacheResolver] → [DispatchThrottlingResolver] → [ShadowResolver] → [LocalChecker] ⟲
```

Each resolver can:
- Handle the request directly (e.g., cache hit)
- Delegate to next resolver in chain
- Decorate the response (e.g., add metrics)

### Concurrency & Performance

- **Breadth limit**: Controls concurrent goroutines per recursion level
- **Depth limit**: Prevents stack overflow from deep model nesting
- **Dispatch throttling**: Queues requests when concurrent dispatches exceed threshold
- **Datastore throttling**: Suspends queries when DB concurrency is high
- **Caching**: Multi-layer (model cache, iterator cache, query cache)
- **Planner**: Thompson Sampling to select best resolver strategy
- **Shadow resolver**: A/B test new strategies without affecting production

### Testing Locations (from CONTRIBUTING.md)

1. **Storage layer**: Integration tests in `pkg/storage/test/storage.go`
2. **Commands**: Unit tests in `pkg/server/commands/*_test.go` (mock dependencies)
3. **Functional API tests**: `tests/functional_test.go` (validates API behavior)
4. **Query APIs** (Check, ListObjects, ListUsers):
   - Exported integration tests in `tests/{check,listobjects,listusers}/*.go`
   - YAML-based test cases for easy maintenance
   - Can be imported by external implementations
5. **Server flags**: Tests in `pkg/server/server_test.go`
6. **HTTP headers**: Test in `cmd/run/run_test.go` (`TestHTTPHeaders`)
7. **Docker integration**: Tests in `cmd/openfga/main_test.go`

## Important Patterns

### Functional Options
Most components use functional options pattern:
```go
server.MustNewServerWithOpts(
  server.WithDatastore(ds),
  server.WithResolveNodeLimit(100),
  server.WithCacheQueryCacheEnabled(true),
)
```

### Context Threading
- TypeSystem is loaded per-request and threaded through context
- Request metadata (store ID, model ID) propagated via context
- Telemetry (traces, request IDs) attached to context

### Interface-Driven Design
- Storage, authenticators, resolvers all use interfaces
- Enables testing with mocks (generated via `mockgen`)
- Supports multiple implementations (memory, postgres, mysql, sqlite)

## Key Files to Understand

- `pkg/server/server.go` - Server initialization and handler registration
- `pkg/server/check.go` - Check API handler (most important API)
- `internal/graph/check.go` - `LocalChecker` core resolution logic
- `pkg/storage/storage.go` - Storage interface definitions
- `pkg/typesystem/typesystem.go` - Authorization model parsing/validation
- `cmd/run/run.go` - Server startup, configuration, graceful shutdown
- `internal/graph/builder.go` - Resolver chain construction

## Configuration

Server configuration via:
- CLI flags: `./openfga run --help`
- Environment variables: `OPENFGA_*` prefix
- Config file: YAML format (see `.config-schema.json`)

Key configuration:
- `--datastore-engine`: Storage backend (memory/postgres/mysql/sqlite)
- `--datastore-uri`: Connection string
- `--grpc-addr`: gRPC server address
- `--http-addr`: HTTP gateway address
- `--playground-enabled`: Enable/disable local playground UI
- `--check-query-cache-enabled`: Enable query result caching
- `--experimentals`: Feature flags (comma-separated)

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

### MySQL
- Stricter length limits on tuple properties than other backends
- See docs: https://openfga.dev/docs/getting-started/setup-openfga/docker#configuring-data-storage

### Memory
- **For development only** - data is ephemeral
- Lost on server restart
- No persistence

### Postgres (recommended for production)
- Full feature support
- Connection pooling via pgx
- Requires PostgreSQL 14+

### SQLite (beta)
- Single-file database
- Good for testing and small deployments
- Limited concurrency

## Common Development Tasks

### Adding a New API Endpoint
1. Define protobuf in [openfga/api](https://github.com/openfga/api) repo
2. Add handler in `pkg/server/{method}.go`
3. Implement command in `pkg/server/commands/{method}_command.go`
4. Add unit tests in `pkg/server/commands/{method}_command_test.go`
5. Add functional tests in `tests/functional_test.go`
6. Update `TestHTTPHeaders` if adding custom headers

### Modifying Storage Interface
1. Update interface in `pkg/storage/storage.go`
2. Add integration test in `pkg/storage/test/storage.go`
3. Implement for all backends: memory, postgres, mysql, sqlite
4. Run full test suite to verify

### Adding Graph Resolver
1. Implement `CheckResolver` interface in `internal/graph/`
2. Add builder option in `internal/graph/builder.go`
3. Chain into resolver composition
4. Add unit tests with mocked dependencies

### Working with TypeSystem Changes
1. Changes to model validation go in `pkg/typesystem/typesystem.go`
2. Graph analysis changes go in `pkg/typesystem/weighted_graph.go`
3. Update validation tests
4. Consider backward compatibility with existing models

## Telemetry

### Logs
- Structured logging via zap
- Log levels: debug, info, warn, error
- Request-scoped fields (request_id, store_id)

### Metrics (Prometheus)
- Request duration histograms
- Dispatch count (recursive calls)
- Datastore query count/duration
- Cache hit/miss rates
- Throttled request counts

### Traces (OpenTelemetry)
- Full distributed tracing support
- Spans per operation
- Exported to OTLP collector
- Configurable sampling ratio

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
