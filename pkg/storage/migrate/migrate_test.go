package migrate_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/migrate"
)

func TestMigrateCommandRollbacks(t *testing.T) {
	type EngineConfig struct {
		Engine     string
		MinVersion int64
	}
	engines := []EngineConfig{
		{Engine: "postgres"},
		{Engine: "mysql"},
		{Engine: "sqlite", MinVersion: 5},
	}

	for _, e := range engines {
		t.Run(e.Engine, func(t *testing.T) {
			container, _, uri := util.MustBootstrapDatastore(t, e.Engine)

			// going from version 3 to 4 when migration #4 doesn't exist is a no-op
			version := container.GetDatabaseSchemaVersion() + 1

			for version >= e.MinVersion {
				t.Logf("migrating to version %d", version)
				err := migrate.RunMigrations(migrate.MigrationConfig{
					Engine:        e.Engine,
					URI:           uri,
					TargetVersion: uint(version),
					Timeout:       5 * time.Second,
					Verbose:       true,
				})
				require.NoError(t, err)
				version--
			}
		})
	}
}

func TestMigrationRegistry(t *testing.T) {
	t.Run("DefaultRegistry", func(t *testing.T) {
		registry := migrate.GetDefaultRegistry()
		require.NotNil(t, registry)
		
		engines := registry.GetSupportedEngines()
		require.ElementsMatch(t, []string{"postgres", "mysql", "sqlite"}, engines)
	})

	t.Run("CustomProvider", func(t *testing.T) {
		// Create a mock provider for testing
		mockProvider := &mockMigrationProvider{engine: "mock"}
		
		migrate.RegisterMigrationProvider("mock", mockProvider)
		
		registry := migrate.GetDefaultRegistry()
		provider, exists := registry.GetProvider("mock")
		require.True(t, exists)
		require.Equal(t, mockProvider, provider)
	})

	t.Run("RunMigrationsWithRegistry", func(t *testing.T) {
		_, _, uri := util.MustBootstrapDatastore(t, "postgres")


		// Create custom registry
		customRegistry := migrate.GetDefaultRegistry()
		
		config := migrate.MigrationConfig{
			Engine:        "postgres",
			URI:           uri,
			TargetVersion: 0,
			Timeout:       5 * time.Second,
			Verbose:       true,
		}
		
		err := migrate.RunMigrationsWithRegistry(customRegistry, config)
		require.NoError(t, err)
	})

	t.Run("RunMigrationsWithProvider", func(t *testing.T) {
		_, _, uri := util.MustBootstrapDatastore(t, "postgres")


		registry := migrate.GetDefaultRegistry()
		provider, exists := registry.GetProvider("postgres")
		require.True(t, exists)
		
		config := migrate.MigrationConfig{
			Engine:        "postgres",
			URI:           uri,
			TargetVersion: 0,
			Timeout:       5 * time.Second,
			Verbose:       true,
		}
		
		err := migrate.RunMigrationsWithProvider(provider, config)
		require.NoError(t, err)
	})

	t.Run("UnsupportedEngine", func(t *testing.T) {
		registry := migrate.GetDefaultRegistry()
		
		config := migrate.MigrationConfig{
			Engine:        "unsupported",
			URI:           "test://uri",
			TargetVersion: 0,
			Timeout:       5 * time.Second,
		}
		
		err := migrate.RunMigrationsWithRegistry(registry, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no migration provider registered for engine: unsupported")
	})
}

// mockMigrationProvider for testing
type mockMigrationProvider struct {
	engine string
}

func (m *mockMigrationProvider) GetSupportedEngine() string {
	return m.engine
}

func (m *mockMigrationProvider) RunMigrations(ctx context.Context, config storage.MigrationConfig) error {
	return nil
}

func (m *mockMigrationProvider) GetCurrentVersion(ctx context.Context, config storage.MigrationConfig) (int64, error) {
	return 1, nil
}
