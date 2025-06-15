# Migration System Refactoring

This document describes the refactored migration system in OpenFGA that provides better support for applications using OpenFGA as a library.

## Overview

The migration system has been refactored to address challenges faced by applications (like Grafana) that embed OpenFGA as a library but have their own migration systems. The new architecture moves away from relying solely on goose's global registry and provides dependency injection capabilities.

## Key Improvements

### 1. Migration Provider Interface

A new `MigrationProvider` interface allows applications to inject their own migration implementations:

```go
type MigrationProvider interface {
    RunMigrations(ctx context.Context, config MigrationConfig) error
    GetCurrentVersion(ctx context.Context, config MigrationConfig) (int64, error)
    GetSupportedEngine() string
}
```

### 2. Per-Database Migration Providers

Migration logic has been moved from the central `migrate` package into individual database drivers:

- `pkg/storage/postgres/migrator.go` - PostgreSQL migrations
- `pkg/storage/mysql/migrator.go` - MySQL migrations  
- `pkg/storage/sqlite/migrator.go` - SQLite migrations

### 3. Migration Registry System

A registry system allows flexible management of migration providers:

```go
registry := storage.NewMigratorRegistry()
registry.RegisterProvider("postgres", postgres.NewPostgresMigrationProvider())
registry.RegisterProvider("custom-db", customProvider)
```

### 4. Backward Compatibility

Existing code continues to work without changes:

```go
// This still works exactly as before
err := migrate.RunMigrations(migrate.MigrationConfig{
    Engine: "postgres",
    URI:    "postgres://...",
})
```

## Usage Examples

### Standard Usage (Backward Compatible)

```go
import "github.com/openfga/openfga/pkg/storage/migrate"

config := migrate.MigrationConfig{
    Engine:        "postgres",
    URI:           "postgres://user:pass@localhost:5432/openfga",
    TargetVersion: 0, // Latest version
    Timeout:       time.Minute * 5,
    Verbose:       true,
}

err := migrate.RunMigrations(config)
```

### Custom Migration Provider

```go
type MyMigrationProvider struct{}

func (m *MyMigrationProvider) RunMigrations(ctx context.Context, config storage.MigrationConfig) error {
    // Integrate with your existing migration system
    return myApp.RunOpenFGAMigrations(config)
}

func (m *MyMigrationProvider) GetCurrentVersion(ctx context.Context, config storage.MigrationConfig) (int64, error) {
    return myApp.GetOpenFGASchemaVersion()
}

func (m *MyMigrationProvider) GetSupportedEngine() string {
    return "my-postgres"
}

// Register and use
migrate.RegisterMigrationProvider("my-postgres", &MyMigrationProvider{})

config := storage.MigrationConfig{
    Engine: "my-postgres",
    URI:    "postgres://...",
}
err := migrate.RunMigrations(config)
```

### Custom Registry

```go
registry := storage.NewMigratorRegistry()
registry.RegisterProvider("custom-engine", myProvider)

err := migrate.RunMigrationsWithRegistry(registry, config)
```

### Direct Provider Usage

```go
provider := postgres.NewPostgresMigrationProvider()
err := migrate.RunMigrationsWithProvider(provider, config)
```

## Migration Strategies for Library Users

### Strategy 1: Default Behavior
Continue using `migrate.RunMigrations()` - no changes needed.

### Strategy 2: Custom Provider Registration
Register a custom provider that integrates with your migration system before calling `migrate.RunMigrations()`.

### Strategy 3: Separate Registry
Create your own migration registry with custom providers and use `migrate.RunMigrationsWithRegistry()`.

### Strategy 4: Direct Provider Control
Use `migrate.RunMigrationsWithProvider()` for complete control over the migration process.

## Benefits for Library Users

1. **Integration Flexibility**: Easily integrate OpenFGA migrations with existing migration systems
2. **Version Control**: Better control over when and how OpenFGA schema updates are applied
3. **Custom Logic**: Inject custom migration logic while reusing OpenFGA's migration files
4. **Testability**: Mock migration providers for testing
5. **Backward Compatibility**: Existing code continues to work without changes

## Architecture

```
┌─────────────────────────────────────────┐
│            Application Layer            │
├─────────────────────────────────────────┤
│        migrate.RunMigrations()          │
├─────────────────────────────────────────┤
│          MigratorRegistry               │
├─────────────────────────────────────────┤
│  PostgresMigrationProvider │ MySQL...   │
├─────────────────────────────────────────┤
│         Database Drivers                │
└─────────────────────────────────────────┘
```

## Migration File Structure

Migration files remain in their current location:
- `assets/migrations/postgres/` - PostgreSQL migrations
- `assets/migrations/mysql/` - MySQL migrations
- `assets/migrations/sqlite/` - SQLite migrations

The refactoring only changes how these migrations are executed, not their content or location.

## Implementation Details

### Default Registry Initialization

The default registry is initialized lazily with built-in providers:

```go
func initDefaultRegistry() {
    registryOnce.Do(func() {
        defaultRegistry = storage.NewMigratorRegistry()
        defaultRegistry.RegisterProvider("postgres", postgres.NewPostgresMigrationProvider())
        defaultRegistry.RegisterProvider("mysql", mysql.NewMySQLMigrationProvider())
        defaultRegistry.RegisterProvider("sqlite", sqlite.NewSQLiteMigrationProvider())
    })
}
```

### Provider Implementation

Each database driver implements the `MigrationProvider` interface:

```go
type PostgresMigrationProvider struct{}

func (p *PostgresMigrationProvider) RunMigrations(ctx context.Context, config storage.MigrationConfig) error {
    // Database-specific migration logic
    // Uses goose internally but isolated to this provider
}
```

## Testing

The refactored system includes comprehensive tests:

```bash
# Test the migration system
go test ./pkg/storage/migrate

# Test individual providers
go test ./pkg/storage/postgres
go test ./pkg/storage/mysql  
go test ./pkg/storage/sqlite
```

## Future Enhancements

This refactoring enables future improvements:

1. **Plugin System**: Load migration providers from plugins
2. **Migration Callbacks**: Hook into migration events
3. **Custom Migration Files**: Support for application-specific migration files
4. **Migration Validation**: Pre-migration validation and rollback support
5. **Migration Metrics**: Detailed migration timing and success metrics

## Breaking Changes

**None.** This refactoring maintains full backward compatibility. Existing applications using `migrate.RunMigrations()` will continue to work exactly as before.

## Related Issues

- Addresses the requirements in the migration improvements issue
- Follows up on PR #2442 which exposed `RunMigrations` for library usage
- Enables smoother OpenFGA migrations for production applications
