package migrate_test

import (
	"context"
	"errors"
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

	t.Run("MemoryEngine", func(t *testing.T) {
		config := migrate.MigrationConfig{
			Engine:        "memory",
			URI:           "memory://test",
			TargetVersion: 0,
			Timeout:       5 * time.Second,
		}

		err := migrate.RunMigrations(config)
		require.NoError(t, err)
	})
}

func TestMigrationRegistryEdgeCases(t *testing.T) {
	t.Run("RegisterProviderAfterGet", func(t *testing.T) {
		// Use a unique engine name to avoid conflicts
		engineName := "test-engine-" + t.Name()

		registry := migrate.GetDefaultRegistry()
		originalEngines := registry.GetSupportedEngines()

		mockProvider := &mockMigrationProvider{engine: engineName}
		migrate.RegisterMigrationProvider(engineName, mockProvider)

		newEngines := registry.GetSupportedEngines()
		require.Len(t, newEngines, len(originalEngines)+1)
		require.Contains(t, newEngines, engineName)

		provider, exists := registry.GetProvider(engineName)
		require.True(t, exists)
		require.Equal(t, mockProvider, provider)
	})

	t.Run("OverrideExistingProvider", func(t *testing.T) {
		// Use a unique engine name to avoid conflicts
		engineName := "test-override-" + t.Name()

		registry := migrate.GetDefaultRegistry()

		// Register first provider with ID 1
		originalProvider := &mockFailingMigrationProvider{engine: engineName, id: 1}
		migrate.RegisterMigrationProvider(engineName, originalProvider)

		// Get the registered provider to confirm it's there
		firstProvider, exists := registry.GetProvider(engineName)
		require.True(t, exists)
		require.Equal(t, originalProvider, firstProvider)

		// Register a new provider with the same engine name (different instance with ID 2)
		newProvider := &mockFailingMigrationProvider{engine: engineName, id: 2}
		migrate.RegisterMigrationProvider(engineName, newProvider)

		// Verify it was overridden
		finalProvider, exists := registry.GetProvider(engineName)
		require.True(t, exists)
		require.Equal(t, newProvider, finalProvider)
		require.NotEqual(t, originalProvider, finalProvider)
	})

	t.Run("MultipleRegistryInstances", func(t *testing.T) {
		// GetDefaultRegistry should return the same instance
		registry1 := migrate.GetDefaultRegistry()
		registry2 := migrate.GetDefaultRegistry()

		require.Same(t, registry1, registry2)
	})
}

func TestRunMigrationsAdvancedScenarios(t *testing.T) {
	t.Run("RunMigrationsWithCustomRegistry_ProviderError", func(t *testing.T) {
		customRegistry := storage.NewMigratorRegistry()

		// Register a provider that will fail
		mockProvider := &mockFailingMigrationProvider{
			engine:             "failing-engine",
			runMigrationsError: errors.New("migration failed"),
		}
		customRegistry.RegisterProvider("failing-engine", mockProvider)

		config := migrate.MigrationConfig{
			Engine:        "failing-engine",
			URI:           "test://uri",
			TargetVersion: 1,
			Timeout:       5 * time.Second,
		}

		err := migrate.RunMigrationsWithRegistry(customRegistry, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "migration failed")
	})

	t.Run("RunMigrationsWithProvider_Success", func(t *testing.T) {
		mockProvider := &mockMigrationProvider{engine: "mock"}

		config := migrate.MigrationConfig{
			Engine:        "mock",
			URI:           "test://uri",
			TargetVersion: 0,
			Timeout:       5 * time.Second,
		}

		err := migrate.RunMigrationsWithProvider(mockProvider, config)
		require.NoError(t, err)
	})

	t.Run("RunMigrationsWithProvider_Error", func(t *testing.T) {
		mockProvider := &mockFailingMigrationProvider{
			engine:             "failing",
			runMigrationsError: errors.New("provider error"),
		}

		config := migrate.MigrationConfig{
			Engine:        "failing",
			URI:           "test://uri",
			TargetVersion: 0,
			Timeout:       5 * time.Second,
		}

		err := migrate.RunMigrationsWithProvider(mockProvider, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "provider error")
	})

	t.Run("RunMigrations_DefaultRegistry", func(t *testing.T) {
		// Test the convenience function that uses the default registry
		engineName := "default-test-" + t.Name()
		mockProvider := &mockMigrationProvider{engine: engineName}
		migrate.RegisterMigrationProvider(engineName, mockProvider)

		config := migrate.MigrationConfig{
			Engine:        engineName,
			URI:           "test://uri",
			TargetVersion: 0,
			Timeout:       5 * time.Second,
		}

		err := migrate.RunMigrations(config)
		require.NoError(t, err)
	})

	t.Run("RunMigrations_InvalidEngine", func(t *testing.T) {
		config := migrate.MigrationConfig{
			Engine:        "completely-unknown-engine",
			URI:           "test://uri",
			TargetVersion: 0,
			Timeout:       5 * time.Second,
		}

		err := migrate.RunMigrations(config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no migration provider registered for engine: completely-unknown-engine")
	})

	t.Run("RunMigrations_EmptyEngine", func(t *testing.T) {
		config := migrate.MigrationConfig{
			Engine:        "",
			URI:           "test://uri",
			TargetVersion: 0,
			Timeout:       5 * time.Second,
		}

		err := migrate.RunMigrations(config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no migration provider registered for engine:")
	})
}

func TestGetDefaultRegistryFunctionality(t *testing.T) {
	t.Run("VerifyBuiltInProviders", func(t *testing.T) {
		registry := migrate.GetDefaultRegistry()

		engines := registry.GetSupportedEngines()
		// The registry may have additional providers registered from other tests
		// So let's just verify the built-in ones exist
		require.Contains(t, engines, "postgres")
		require.Contains(t, engines, "mysql")
		require.Contains(t, engines, "sqlite")

		// Verify each provider exists and has correct engine
		postgresProvider, exists := registry.GetProvider("postgres")
		require.True(t, exists)
		require.NotNil(t, postgresProvider)
		require.Equal(t, "postgres", postgresProvider.GetSupportedEngine())

		mysqlProvider, exists := registry.GetProvider("mysql")
		require.True(t, exists)
		require.NotNil(t, mysqlProvider)
		require.Equal(t, "mysql", mysqlProvider.GetSupportedEngine())

		sqliteProvider, exists := registry.GetProvider("sqlite")
		require.True(t, exists)
		require.NotNil(t, sqliteProvider)
		require.Equal(t, "sqlite", sqliteProvider.GetSupportedEngine())
	})
} // mockMigrationProvider for testing.
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

// mockFailingMigrationProvider for testing error scenarios.
type mockFailingMigrationProvider struct {
	engine                 string
	id                     int // Add ID to distinguish instances
	runMigrationsError     error
	getCurrentVersionError error
}

func (m *mockFailingMigrationProvider) GetSupportedEngine() string {
	return m.engine
}

func (m *mockFailingMigrationProvider) RunMigrations(ctx context.Context, config storage.MigrationConfig) error {
	if m.runMigrationsError != nil {
		return m.runMigrationsError
	}
	return nil
}

func (m *mockFailingMigrationProvider) GetCurrentVersion(ctx context.Context, config storage.MigrationConfig) (int64, error) {
	if m.getCurrentVersionError != nil {
		return 0, m.getCurrentVersionError
	}
	return 1, nil
}
