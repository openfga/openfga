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
- `commands/reverseexpand/pipeline` - Core ListObjects pipeline implementation

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

**Storage changes**: Add integration tests in `pkg/storage/test/storage.go`

**API changes**:
- Command unit tests with mocks in `pkg/server/commands/*_test.go`
- Functional tests in `tests/functional_test.go`
- Query API tests (Check, ListObjects) in `tests/check/`, `tests/listobjects/` using YAML test cases

**Docker/CLI changes**: Test in `cmd/openfga/main_test.go`

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
