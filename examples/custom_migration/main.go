// Package main demonstrates how to use OpenFGA's refactored migration system
// with custom migration providers for better integration with applications
// that have their own migration systems.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/migrate"
)

// CustomMigrationProvider demonstrates how to implement a custom migration provider
// that integrates with your application's existing migration system
type CustomMigrationProvider struct {
	engine string
}

func NewCustomMigrationProvider(engine string) *CustomMigrationProvider {
	return &CustomMigrationProvider{engine: engine}
}

func (c *CustomMigrationProvider) GetSupportedEngine() string {
	return c.engine
}

func (c *CustomMigrationProvider) RunMigrations(ctx context.Context, config storage.MigrationConfig) error {
	// This is where you would integrate with your custom migration system
	// For example, you might call your application's migration runner here
	log.Printf("Running custom migrations for %s with URI: %s", c.engine, config.URI)
	
	// Example: Call your existing migration system
	// return yourApp.RunOpenFGAMigrations(config)
	
	// For demo purposes, just log that we would run migrations
	log.Printf("Custom migration provider would handle %s migrations here", c.engine)
	return nil
}

func (c *CustomMigrationProvider) GetCurrentVersion(ctx context.Context, config storage.MigrationConfig) (int64, error) {
	// This would query your migration system for the current OpenFGA schema version
	log.Printf("Getting current version for %s", c.engine)
	return 5, nil // Mock version
}

func main() {
	// Example 1: Using the default migration system (backward compatible)
	fmt.Println("=== Example 1: Default Migration System ===")
	defaultConfig := storage.MigrationConfig{
		Engine:        "postgres",
		URI:           "postgres://user:pass@localhost:5432/openfga_test",
		TargetVersion: 0, // Latest
		Timeout:       5 * time.Minute,
		Verbose:       true,
	}
	
	// This works exactly as before - no changes needed for existing applications
	if err := migrate.RunMigrations(defaultConfig); err != nil {
		log.Printf("Default migration failed: %v", err)
	}

	// Example 2: Registering a custom migration provider
	fmt.Println("\n=== Example 2: Custom Migration Provider ===")
	customProvider := NewCustomMigrationProvider("custom-postgres")
	migrate.RegisterMigrationProvider("custom-postgres", customProvider)
	
	customConfig := storage.MigrationConfig{
		Engine:        "custom-postgres",
		URI:           "postgres://user:pass@localhost:5432/openfga_custom",
		TargetVersion: 0,
		Timeout:       5 * time.Minute,
		Verbose:       true,
	}
	
	if err := migrate.RunMigrations(customConfig); err != nil {
		log.Printf("Custom migration failed: %v", err)
	}

	// Example 3: Using a completely custom registry
	fmt.Println("\n=== Example 3: Custom Registry ===")
	customRegistry := storage.NewMigratorRegistry()
	customRegistry.RegisterProvider("my-postgres", NewCustomMigrationProvider("my-postgres"))
	
	registryConfig := storage.MigrationConfig{
		Engine:        "my-postgres",
		URI:           "postgres://user:pass@localhost:5432/openfga_registry",
		TargetVersion: 3, // Specific version
		Timeout:       5 * time.Minute,
		Verbose:       true,
	}
	
	if err := migrate.RunMigrationsWithRegistry(customRegistry, registryConfig); err != nil {
		log.Printf("Custom registry migration failed: %v", err)
	}

	// Example 4: Direct provider usage
	fmt.Println("\n=== Example 4: Direct Provider Usage ===")
	directProvider := NewCustomMigrationProvider("direct")
	directConfig := storage.MigrationConfig{
		Engine:  "direct",
		URI:     "postgres://user:pass@localhost:5432/openfga_direct",
		Timeout: 5 * time.Minute,
		Verbose: true,
	}
	
	if err := migrate.RunMigrationsWithProvider(directProvider, directConfig); err != nil {
		log.Printf("Direct provider migration failed: %v", err)
	}

	fmt.Println("\n=== Migration Examples Completed ===")
	fmt.Println("The refactored migration system provides:")
	fmt.Println("1. Full backward compatibility with existing code")
	fmt.Println("2. Ability to inject custom migration providers")
	fmt.Println("3. Per-database migration logic in individual drivers")
	fmt.Println("4. Flexible migration registry system")
	fmt.Println("5. Support for applications with existing migration systems")
}
