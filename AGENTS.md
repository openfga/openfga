# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Goals and Non-Goals

### Goals

**Core Authorization Paradigm:**
- Relationship-based access control (ReBAC) inspired by Google Zanzibar
- Evaluate complex authorization graphs with set operations (union, intersection, exclusion)
- Support for direct relationships, computed usersets, and tuple-to-userset (TTU) relations

**Performance and Efficiency:**
- Low-latency authorization checks (sub-millisecond to single-digit millisecond response times)
- Minimal memory allocations in hot paths (efficient graph traversal algorithms)
- High but bounded concurrency (configurable breadth/depth limits, dispatch throttling)
- Multi-layer caching (model cache, iterator cache, query cache) for optimal performance
- Query optimization through weighted graph analysis

**Reliability and Resilience:**
- Graceful degradation under load (throttling mechanisms to prevent overload)
- Comprehensive error handling and recovery patterns
- Timeout controls for all operations
- Production-grade observability (Prometheus metrics, OpenTelemetry tracing, structured logging)

**Flexible Deployment Models:**
- **Service mode**: Standalone containerized deployment with HTTP/gRPC APIs
- **Library mode**: Embeddable as a Go library dependency in other applications
- Multiple authentication methods (OIDC, preshared keys, or custom)
- Horizontal scalability through stateless API design

**Data Storage:**
- SQL relational databases with ACID properties for data integrity
- Multi-backend support: PostgreSQL (recommended for production), MySQL, SQLite (beta)
- Abstract storage interface enabling pluggable implementations
- Database migration tooling for schema evolution

**API Design:**
- gRPC API for efficient, strongly-typed communication
- HTTP REST API via grpc-gateway for broad compatibility
- Consistent request/response patterns across all endpoints
- Support for contextual tuples (request-scoped relationships)

**Developer Experience:**
- Playground UI for local development and testing
- Multiple language SDKs (Java, Node.js, Go, Python, .NET)
- CLI tools for testing and management
- YAML-based test matrices for authorization model validation
- Clear documentation and examples

### Non-Goals

**Storage Backends:**
- NoSQL databases (MongoDB, DynamoDB, Cassandra, Redis as primary storage)
  - Focus on SQL backends ensures ACID properties and relational query efficiency
  - NoSQL backends may not provide the consistency guarantees required for authorization

**Identity and Authentication:**
- Identity provider functionality (user creation, password management, user profiles)
- User authentication and credential validation
- LDAP or SAML integration for user authentication
  - OpenFGA evaluates relationships between existing entities, assumes identity management is handled externally
  - For API authentication, OIDC client credentials flow is supported

**Authorization Paradigms:**
- Pure Attribute-Based Access Control (ABAC) without relationships
  - OpenFGA is relationship-based (ReBAC); attributes can be modeled as relationships but ABAC is not the primary paradigm

**API Protocols:**
- GraphQL API
  - gRPC and HTTP REST provide sufficient coverage for authorization use cases
  - Adding GraphQL would increase maintenance burden without clear benefits

**Audit and Compliance:**
- Unlimited historical audit logs with full change tracking
  - `ReadChanges` API provides paginated change history but not a full audit system
  - External audit systems should be used for compliance requirements

**General-Purpose Features:**
- General-purpose database or key-value store
- Business logic execution or workflow orchestration
- Rate limiting or API gateway features
  - OpenFGA is specialized for authorization checks, not general infrastructure

**Production Features:**
- Production deployment of Playground UI (playground is for localhost development only)
- Single-node in-memory storage for production (memory storage is ephemeral, dev-only)

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

## Code Conventions

This section defines code quality standards for the OpenFGA codebase. AI agents should follow these conventions when writing, reviewing, or modifying code. The emphasis is on writing code that is clear, maintainable, and consistent with existing patterns throughout the project.

### Documentation Comments

Documentation should explain **why** code exists and how to use it, not merely describe **what** the code does. Good documentation provides context, usage guidance, and explains non-obvious decisions.

**Rules**:

- **Exported types**: Must have doc comments explaining purpose and responsibility. Include usage context and what problem the type solves.

- **Exported functions**: Must have multi-paragraph comments with usage guidance, examples, and edge cases. Explain parameters, return values, and error conditions.

- **Interface methods**: Must document preconditions, error conditions, and behavioral guarantees. Specify what callers must provide and what they can expect.

- **Constants**: Should explain rationale ("why this value?") not just definition. Balance considerations and context matter more than the literal value.

- **Private functions**: Brief comments only when logic is non-obvious. Omit comments for self-explanatory code where the function name and signature clearly convey intent.

- **Inline comments**: Use sparingly, only for gotchas, threading issues, or architectural decisions that aren't obvious from the code itself.

- **Cross-references**: Use `[[package.Type]]` syntax for linking related concepts. This creates navigable documentation connections.

**Examples**:

1. **Constant with rationale** (from `pkg/storage/storage.go`):
```go
// DefaultMaxTuplesPerWrite specifies the default maximum number of tuples that can be written
// in a single write operation. This constant is used to limit the batch size in write operations
// to maintain performance and avoid overloading the system. The value is set to 100 tuples,
// which is a balance between efficiency and resource usage.
const DefaultMaxTuplesPerWrite = 100
```

2. **Function with cross-references** (from `internal/graph/check.go`):
```go
// NewLocalChecker constructs a LocalChecker that can be used to evaluate a Check
// request locally.
//
// Developers wanting a LocalChecker with other optional layers (e.g caching and others)
// are encouraged to use [[NewOrderedCheckResolvers]] instead.
func NewLocalChecker(opts ...LocalCheckerOption) *LocalChecker
```

3. **Interface method with contract** (from `pkg/storage/storage.go`):
```go
// Read the set of tuples associated with `store` and `tupleKey`, which may be nil or partially filled.
// If nil, Read will return an iterator over all the tuples in the given `store`. If the `tupleKey`
// is partially filled, it will return an iterator over those tuples which match the `tupleKey`.
// Note that at least one of `Object` or `User` (or both), must be specified in this case.
//
// The caller must be careful to close the [TupleIterator], either by consuming the entire iterator
// or by closing it. There is NO guarantee on the order of the tuples returned on the iterator.
Read(ctx context.Context, store string, filter ReadFilter, options ReadOptions) (TupleIterator, error)
```

4. **Inline comment for gotcha** (from `pkg/server/server.go`):
```go
// NOTE don't use this directly, use function resolveTypesystem. See https://github.com/openfga/openfga/issues/1527
typesystemResolver typesystem.TypesystemResolverFunc
```

### Naming Conventions

Consistent naming improves code readability and helps developers quickly understand component roles and relationships. Follow Go idioms and OpenFGA-specific patterns.

**Interfaces**:
- No "I" prefix (use `CheckResolver`, not `ICheckResolver`)
- Descriptive names indicating role or action
- Examples: `CheckResolver`, `RelationshipTupleReader`, `OpenFGADatastore`

**Structs**:
- PascalCase descriptive compound names
- Indicate responsibility or pattern (e.g., `CachedCheckResolver` shows caching decorator)
- Examples: `LocalChecker`, `CheckQuery`, `ResolveCheckResponse`

**Functions**:
- Constructors: `New<Type>()` pattern
- Options: `With<Property>()` for functional options
- Option types: `<Type>Option func(*<Type>)`
- Methods: Verb form for actions (e.g., `Execute`, `Resolve`, `Check`)

**Variables**:
- Short names in narrow scopes: `ctx`, `err`, `req`, `res`, `ds`
- Descriptive names for significant variables: `checkResolver`, `typesys`, `datastore`
- camelCase throughout (no snake_case except in test names)

**Constants**:
- Exported: PascalCase (e.g., `DefaultMaxTuplesPerWrite`)
- Unexported: camelCase (e.g., `relationshipTupleReaderCtxKey`)
- Context keys: `<name>CtxKey` suffix pattern

**Error Variables**:
- Sentinel errors: `Err<Name>` pattern (e.g., `ErrNotFound`, `ErrUnknown`)

**Examples**:

```go
// Good interface naming (no I prefix)
type CheckResolver interface { ... }
type RelationshipTupleReader interface { ... }

// Good constructor and option pattern
func NewCheckCommand(ds storage.OpenFGADatastore, opts ...CheckCommandOption) *CheckCommand

type CheckCommandOption func(*CheckCommand)

func WithCheckCommandLogger(l logger.Logger) CheckCommandOption {
    return func(c *CheckCommand) { c.logger = l }
}

// Constants: exported PascalCase, unexported camelCase
const DefaultMaxTuplesPerWrite = 100
const relationshipTupleReaderCtxKey ctxKey = "relationship-tuple-reader-context-key"

// Sentinel errors
var (
    ErrNotFound = errors.New("not found")
    ErrUnknown = errors.New("internal server error")
)
```

### Error Handling Conventions

OpenFGA uses Go's standard error handling with careful attention to error context and chaining. Always preserve error context for debugging while exposing appropriate errors at API boundaries.

**Sentinel Errors**:
- Use `var Err<Name> = errors.New("message")` pattern
- Group related errors in `var ()` blocks
- Define at package level for reusability

**Error Wrapping**:
- Use `fmt.Errorf("context: %w", err)` to add context while preserving error chain
- **Never** use `fmt.Errorf("context: %v", err)` - always use `%w` for wrapped errors
- Each layer should add meaningful context describing what operation failed

**Custom Error Types**:
- Must implement `Error() string` method
- Must implement `Unwrap() error` method for error chain support
- Optionally implement `Is(target error) bool` for sentinel comparison
- Use struct types to carry additional error metadata (e.g., tuple key, cause)

**Error Checking**:
- Use `errors.Is(err, sentinel)` for sentinel error checks
- Use `errors.As(err, &targetType)` for type-based error handling
- **Never** use `err == sentinel` direct comparison (breaks wrapping)

**Error Mapping**:
- Create converter functions to map domain errors to API errors
- Pattern: `<Command>ErrorToServerError` functions (e.g., `CheckCommandErrorToServerError`)
- Keep domain errors internal, expose sanitized API errors at boundaries

**Examples**:

1. **Sentinel error definition** (from `pkg/storage/errors.go`):
```go
var (
    ErrNotFound = errors.New("not found")
    ErrCollision = errors.New("item already exists")
    ErrInvalidContinuationToken = errors.New("invalid continuation token")
)
```

2. **Custom error type** (from `pkg/tuple/tuple_errors.go`):
```go
// InvalidTupleError is returned if the tuple is invalid.
type InvalidTupleError struct {
    Cause    error
    TupleKey TupleWithoutCondition
}

func (i *InvalidTupleError) Error() string {
    return fmt.Sprintf("Invalid tuple '%s'. Reason: %s", TupleKeyToString(i.TupleKey), i.Cause)
}

func (i *InvalidTupleError) Is(target error) bool {
    _, ok := target.(*InvalidTupleError)
    return ok
}
```

3. **Error wrapping** (showing good vs bad):
```go
// Good: wraps with context, preserves error chain
return fmt.Errorf("failed to read tuples: %w", err)

// Bad: loses error chain, can't use errors.Is or errors.As
return fmt.Errorf("failed to read tuples: %v", err)
```

4. **Error checking patterns** (showing proper usage):
```go
// Using errors.Is for sentinel errors
if errors.Is(err, storage.ErrNotFound) {
    return serverErrors.AuthorizationModelNotFound(modelID)
}

// Using errors.As for type-based handling
var invalidRelationError *tuple.InvalidTupleError
if errors.As(err, &invalidRelationError) {
    return serverErrors.ValidationError(invalidRelationError)
}
```

### Testing Conventions

Testing is a critical part of OpenFGA's quality standards. Follow test-driven development practices and maintain high test coverage for all code paths.

**Coverage Target**:
- **Target 95% test coverage** for new code (aspirational goal)
- CI currently enforces 85% via `codecov.yml` - aim to exceed this
- All exported functions **must** have unit tests
- All exported functions **must** have benchmark tests
- Critical paths (Check, ListObjects, ListUsers) **must** have integration tests

**Test Naming**:
- Pattern: `Test<FunctionName>` for main test function
- Use nested `t.Run("scenario_description")` for subtests
- Subtest names: `lowercase_with_underscores` describing scenario
- Be descriptive: test names should clearly indicate what is being tested

**Table-Driven Tests**:
- Use `[]struct{name, input, expected}` pattern for multiple scenarios
- **Always** include `name` field for clear test identification
- Use descriptive field names (not generic `in`/`out`)
- Makes it easy to add new test cases and understand failures

**Mocking**:
- Use `gomock` for interface mocking (never use concrete types in tests when interface exists)
- Generate mocks with `make generate-mocks` (uses `mockgen`)
- Pattern: `mockController := gomock.NewController(t)` with `defer mockController.Finish()`
- Use `EXPECT().Times(n).DoAndReturn(func)` for behavior specification
- Use `gomock.Any()` for arguments you don't care about checking

**Assertions**:
- Use `testify/require` for fatal assertions (test stops on failure)
- Use `testify/assert` for non-fatal assertions (test continues)
- Use `go-cmp/cmp` with `protocmp` for complex protobuf comparisons
- Choose the right assertion level: use `require` for setup, `assert` for multiple checks

**Benchmarks**:
- Pattern: `Benchmark<FunctionName>(b *testing.B)`
- Use `b.ResetTimer()` after setup to exclude initialization from measurements
- Loop with `for i := 0; i < b.N; i++` for operation under test
- Include `b.Cleanup()` for resource cleanup
- Run with `-benchmem` flag for memory profiling

**Cleanup and Leak Detection**:
- Use `t.Cleanup(func)` for resource cleanup (preferred over `defer` in tests)
- Use `goleak.VerifyNone(t)` for goroutine leak detection in critical tests
- Close datastores and connections properly in cleanup
- Cleanup runs in reverse order, ensuring proper teardown

**Integration Tests**:
- Storage layer: Use exported `RunAllTests()` pattern in `pkg/storage/test/storage.go`
- Run against all backends: memory, postgres, mysql, sqlite
- Use `storagetest` package for backend-agnostic tests

**Test Utilities**:
- Use helpers from `internal/testutils/` for common setup
- Mark helpers with `t.Helper()` to improve error reporting line numbers
- Extract common test data setup into reusable functions

**Examples**:

1. **Table-driven test** (from `pkg/server/commands/write_test.go`):
```go
func TestWriteCommand(t *testing.T) {
    tests := []struct {
        name             string
        writes           *openfgav1.WriteRequestWrites
        deletes          *openfgav1.WriteRequestDeletes
        expectedError    string
    }{
        {
            name:          "empty_writes_and_deletes",
            writes:        &openfgav1.WriteRequestWrites{},
            deletes:       &openfgav1.WriteRequestDeletes{},
            expectedError: "Invalid input...",
        },
        // more test cases...
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            // test implementation
        })
    }
}
```

2. **Mock usage** (from `pkg/server/commands/check_command_test.go`):
```go
func TestCheckQuery(t *testing.T) {
    mockController := gomock.NewController(t)
    defer mockController.Finish()

    mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
    mockCheckResolver := graph.NewMockCheckResolver(mockController)

    mockCheckResolver.EXPECT().
        ResolveCheck(gomock.Any(), gomock.Any()).
        Times(1).
        DoAndReturn(func(_ context.Context, _ *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
            return &graph.ResolveCheckResponse{Allowed: true}, nil
        })

    // test implementation using mocks
}
```

3. **Cleanup and leak detection** (from `pkg/server/commands/batch_check_command_test.go`):
```go
func TestBatchCheckCommand(t *testing.T) {
    t.Cleanup(func() {
        goleak.VerifyNone(t)
    })

    // test implementation - goleak will verify no goroutines leaked
}
```

4. **Benchmark pattern** (pattern from codebase):
```go
func BenchmarkListObjects(b *testing.B) {
    datastore := memory.New()
    b.Cleanup(datastore.Close)

    // Setup: create model, write tuples, etc.
    // ... setup code ...

    b.ResetTimer() // Don't measure setup time
    for i := 0; i < b.N; i++ {
        // Operation being benchmarked
        _, err := query.Execute(ctx, req)
        if err != nil {
            b.Fatal(err)
        }
    }
}
```

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

### Request Flow: BatchCheck API

BatchCheck processes multiple authorization checks concurrently with intelligent deduplication to maximize cache efficiency and minimize redundant graph traversals.

```
1. HTTP/gRPC request arrives with multiple check requests (each with correlation ID)
2. Middleware: Recovery → RequestID → StoreID → Validation → Logging → Auth
3. Server.BatchCheck() handler
4. Load TypeSystem from storage (cached)
5. BatchCheckCommand.Execute():
   - Validate batch size against MaxChecksPerBatchCheck
   - Validate each check request against TypeSystem
   - Build shared cache controller for all concurrent checks
6. Deduplication phase:
   - Generate xxhash cache key for each check (hash of user+relation+object+contextual tuples+context)
   - Map correlation IDs to deduplicated check parameters
   - Multiple correlation IDs may map to single deduplicated check
7. Concurrent execution (up to MaxConcurrentChecksPerBatchCheck goroutines):
   - Use sourcegraph/conc pool for bounded parallelism
   - Execute CheckCommand for each unique check (after deduplication)
   - Share cache controller across all concurrent checks to maximize hit rate
8. Per-check error handling:
   - Errors in individual checks don't fail entire batch
   - Each correlation ID gets result or error
9. Aggregate metadata (dispatch count, query count, duplicate count across all checks)
10. Return BatchCheckResponse with map of correlation ID → CheckResponse
```

**Key Features**:
- **Deduplication**: Uses xxhash to create cache keys; multiple correlation IDs can share results from a single check execution
- **Bounded Concurrency**: Configurable parallelism (default 50 concurrent checks) prevents resource exhaustion
- **Shared Cache Controller**: Single cache instance shared across all concurrent checks maximizes cache hit rate
- **Per-Check Errors**: Individual check failures don't impact other checks in the batch; errors returned per correlation ID
- **Metadata Aggregation**: Collects dispatch count, query count, throttle events, and duplicate count across all checks for observability

**Configuration Options**:
- `--max-checks-per-batch-check` (default 50) - Maximum number of checks allowed per batch request
- `--max-concurrent-checks-per-batch-check` (default 50) - Maximum concurrent check executions
- `--check-query-cache-enabled` - Enable query caching (applies to all checks in batch)
- `--datastore-max-concurrent-reads` - Controls datastore query concurrency
- `--check-dispatch-throttling-enabled` - Enable dispatch throttling for recursive checks

**Files for Implementation**:
- `pkg/server/batch_check.go` (192 lines) - BatchCheck gRPC handler
- `pkg/server/commands/batch_check_command.go` (305 lines) - Deduplication logic and concurrent execution orchestration

### Request Flow: ListObjects API

ListObjects uses **reverse expansion** to find all objects a user can access for a given relation, traversing the authorization graph backwards from the relation to leaf nodes containing actual tuples.

```
1. HTTP/gRPC request arrives (user, relation, object type, optional filters)
2. Middleware: Recovery → RequestID → StoreID → Validation → Logging → Auth
3. Server.ListObjects() or Server.StreamedListObjects() handler
4. Load TypeSystem from storage (cached)
5. Strategy selection (based on TypeSystem and feature flags):
   - Weighted Graph (optimized): Precomputed query path analysis
   - Pipeline (experimental): Message-passing concurrency with constant memory
   - Classic: Fallback recursive traversal
6. ListObjectsCommand.Execute():
   - Validate request against TypeSystem
   - Wrap storage with contextual tuples
7. Reverse expansion phase:
   - Traverse graph backwards from target relation to leaf nodes
   - Handle three edge types: DirectEdge, ComputedUsersetEdge, TupleToUsersetEdge
   - Apply set operations: union, intersection, exclusion
   - Yield candidate objects with status: NoFurtherEvalStatus or RequiresFurtherEvalStatus
8. Conditional evaluation phase (for RequiresFurtherEvalStatus results):
   - Execute Check API for each candidate object to validate conditions
   - Filter objects based on Check results (intersections/exclusions require validation)
9. Result streaming/pagination (respect ListObjectsMaxResults limit)
10. Collect metadata (dispatch count, query count, duration)
11. Return ListObjectsResponse or stream results
```

**Key Features**:
- **Reverse Expansion**: Traverses graph backwards from relation to tuples (unlike Check which traverses forwards from tuple to relation)
- **Three Algorithmic Strategies**: Weighted Graph (production default with cost-based optimization), Pipeline (experimental constant-memory message-passing), Classic (recursive fallback)
- **Two-Phase Processing**: First phase yields candidate objects via reverse expansion; second phase validates conditional results via Check API
- **Result Status**: Objects have NoFurtherEvalStatus (direct match, no Check needed) or RequiresFurtherEvalStatus (needs Check due to intersection/exclusion/condition)
- **Streaming Support**: StreamedListObjects provides server-side streaming for large result sets
- **Set Operations**: Handles union (OR), intersection (AND), exclusion (BUT NOT) with correct conditional evaluation
- **Weighted Optimization**: Analyzes TypeSystem to choose lowest-cost query paths based on estimated tuple counts
- **Pipeline Architecture**: Experimental message-passing worker graph with natural backpressure and O(1) memory

**Configuration Options**:
- `--listobjects-deadline` (default 3s) - Maximum duration for ListObjects operation
- `--listobjects-max-results` (default 1000) - Maximum objects returned per request
- `--resolve-node-limit` - Maximum recursive dispatches across entire operation
- `--resolve-node-breadth-limit` - Maximum concurrent goroutines per recursion level
- `--max-concurrent-reads-for-list-objects` - Concurrent database reads for ListObjects
- `--experimentals=pipeline-listobjects` - Enable experimental pipeline strategy
- `--experimentals=shadow-listobjects` - Enable A/B testing between strategies
- `--experimentals=listobjects-optimizations` - Enable weighted graph optimization

**Files for Implementation**:
- `pkg/server/list_objects.go` (356 lines) - ListObjects and StreamedListObjects gRPC handlers
- `pkg/server/commands/list_objects.go` (847 lines) - Strategy selection and orchestration
- `pkg/server/commands/reverseexpand/reverse_expand.go` (744 lines) - Classic recursive reverse expansion
- `pkg/server/commands/reverseexpand/reverse_expand_weighted.go` (812 lines) - Weighted graph optimization strategy
- `pkg/server/commands/reverseexpand/pipeline/` (4,270 lines total) - Pipeline message-passing architecture

### Request Flow: ListUsers API

ListUsers uses **reverse expansion** to find all users with access to a specific object for a given relation, with optional filtering by user type and relation.

```
1. HTTP/gRPC request arrives (object, relation, optional user filters)
2. Middleware: Recovery → RequestID → StoreID → Validation → Logging → Auth
3. Server.ListUsers() handler
4. Load TypeSystem from storage (cached)
5. ListUsersCommand.Execute():
   - Validate request against TypeSystem
   - Validate user filters (object type + relation pairs)
   - Wrap storage with contextual tuples
6. Reverse expansion phase:
   - Traverse graph backwards from object+relation to users
   - Handle three edge types: DirectEdge, ComputedUsersetEdge, TupleToUsersetEdge
   - Apply set operations: union, intersection, exclusion with special wildcard handling
7. Cycle detection:
   - Track visited usersets in visitedUsersetsMap
   - Prevent infinite loops in recursive expansion
8. User filtering (if user filters provided):
   - Filter results by user type and relation
   - Only return users matching specified filters
9. Relationship status tracking:
   - Mark users as explicitly related vs excluded (for correct exclusion handling)
   - Handle typed wildcards (type:*) with special union/intersection/exclusion logic
10. Collect metadata (dispatch count, query count, duration)
11. Return ListUsersResponse with User objects (object_user, userset, typed_wildcard)
```

**Key Features**:
- **Reverse Expansion**: Traverses graph backwards from object+relation to users who have that relationship
- **User Filtering**: Optional filters narrow results by user type and relation (e.g., only return users of type "user" with "member" relation)
- **Set Operations**: Handles union (OR), intersection (AND), exclusion (BUT NOT) with correct wildcard handling
- **Typed Wildcards**: Special handling for public access patterns (e.g., `type:*` grants access to all users of that type)
- **Relationship Status**: Tracks explicit relationships vs exclusions for correct set operation evaluation
- **Cycle Detection**: Uses visitedUsersetsMap to prevent infinite loops in recursive model traversal
- **Early Exit Optimization**: doesHavePossibleEdges() checks if relation can possibly yield users before expensive expansion
- **Standalone Architecture**: Unlike ListObjects, doesn't use CheckResolver chain (simpler, specialized implementation)

**Configuration Options**:
- `--listusers-deadline` (default 3s) - Maximum duration for ListUsers operation
- `--listusers-max-results` (default 1000) - Maximum users returned per request
- `--resolve-node-limit` - Maximum recursive dispatches across entire operation
- `--resolve-node-breadth-limit` - Maximum concurrent goroutines per recursion level
- `--max-concurrent-reads-for-list-users` - Concurrent database reads for ListUsers
- `--listusers-dispatch-throttling-enabled` (default false) - Enable dispatch throttling for ListUsers

**Files for Implementation**:
- `pkg/server/list_users.go` (174 lines) - ListUsers gRPC handler
- `pkg/server/commands/listusers/list_users_rpc.go` (992 lines) - Reverse expansion and user filtering logic
- `pkg/server/commands/listusers/validate.go` (95 lines) - Request validation logic

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
