package storage

import (
	"context"
	"time"
)

// MigrationProvider defines the interface for database migration providers.
// This allows applications to inject their own migration systems instead of
// relying solely on goose's global registry.
type MigrationProvider interface {
	// RunMigrations executes database migrations with the provided configuration
	RunMigrations(ctx context.Context, config MigrationConfig) error

	// GetCurrentVersion returns the current migration version of the database
	GetCurrentVersion(ctx context.Context, config MigrationConfig) (int64, error)

	// GetSupportedEngine returns the database engine this provider supports
	GetSupportedEngine() string
}

// MigrationConfig contains the configuration needed for running migrations.
type MigrationConfig struct {
	Engine        string
	URI           string
	TargetVersion uint
	Timeout       time.Duration
	Verbose       bool
	Username      string
	Password      string
}

// MigratorRegistry manages migration providers for different database engines.
type MigratorRegistry struct {
	providers map[string]MigrationProvider
}

// NewMigratorRegistry creates a new migration provider registry.
func NewMigratorRegistry() *MigratorRegistry {
	return &MigratorRegistry{
		providers: make(map[string]MigrationProvider),
	}
}

// RegisterProvider registers a migration provider for a specific database engine.
func (r *MigratorRegistry) RegisterProvider(engine string, provider MigrationProvider) {
	r.providers[engine] = provider
}

// GetProvider returns the migration provider for the specified engine.
func (r *MigratorRegistry) GetProvider(engine string) (MigrationProvider, bool) {
	provider, exists := r.providers[engine]
	return provider, exists
}

// GetSupportedEngines returns a list of all supported database engines.
func (r *MigratorRegistry) GetSupportedEngines() []string {
	engines := make([]string, 0, len(r.providers))
	for engine := range r.providers {
		engines = append(engines, engine)
	}
	return engines
}
