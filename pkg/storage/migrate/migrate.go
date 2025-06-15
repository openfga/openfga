package migrate

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlite"
)

// MigrationConfig contains the configuration needed for running migrations
type MigrationConfig = storage.MigrationConfig

var (
	// defaultRegistry is the global migration provider registry
	defaultRegistry *storage.MigratorRegistry
	registryOnce    sync.Once
)

// initDefaultRegistry initializes the default migration registry with built-in providers
func initDefaultRegistry() {
	registryOnce.Do(func() {
		defaultRegistry = storage.NewMigratorRegistry()
		
		// Register built-in migration providers
		defaultRegistry.RegisterProvider("postgres", postgres.NewPostgresMigrationProvider())
		defaultRegistry.RegisterProvider("mysql", mysql.NewMySQLMigrationProvider())
		defaultRegistry.RegisterProvider("sqlite", sqlite.NewSQLiteMigrationProvider())
	})
}

// GetDefaultRegistry returns the default migration provider registry
func GetDefaultRegistry() *storage.MigratorRegistry {
	initDefaultRegistry()
	return defaultRegistry
}

// RegisterMigrationProvider allows applications to register custom migration providers
func RegisterMigrationProvider(engine string, provider storage.MigrationProvider) {
	initDefaultRegistry()
	defaultRegistry.RegisterProvider(engine, provider)
}

// RunMigrationsWithProvider runs migrations using a specific migration provider
func RunMigrationsWithProvider(provider storage.MigrationProvider, cfg storage.MigrationConfig) error {
	ctx := context.Background()
	return provider.RunMigrations(ctx, cfg)
}

// RunMigrationsWithRegistry runs migrations using a specific migration registry
func RunMigrationsWithRegistry(registry *storage.MigratorRegistry, cfg storage.MigrationConfig) error {
	if cfg.Engine == "memory" {
		log.Println("no migrations to run for `memory` datastore")
		return nil
	}

	provider, exists := registry.GetProvider(cfg.Engine)
	if !exists {
		return fmt.Errorf("no migration provider registered for engine: %s", cfg.Engine)
	}

	ctx := context.Background()
	return provider.RunMigrations(ctx, cfg)
}

// RunMigrations runs the migrations for the given config using the default registry.
// This function maintains backward compatibility while allowing for dependency injection.
// When OpenFGA is used as a library, the embedding application may have its own migration system
// that differs from OpenFGA's use of goose. This refactored approach allows applications to:
// 1. Use this function for standard migrations with built-in providers
// 2. Register custom providers via RegisterMigrationProvider before calling this
// 3. Use RunMigrationsWithProvider for complete control over the migration provider
// 4. Use RunMigrationsWithRegistry to use a custom registry
// The function handles migrations for multiple database engines (postgres, mysql, sqlite) and supports
// both upgrading and downgrading to specific versions through the individual migration providers.
func RunMigrations(cfg storage.MigrationConfig) error {
	return RunMigrationsWithRegistry(GetDefaultRegistry(), cfg)
}
