package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MockMigrationProvider for testing
type MockMigrationProvider struct {
	engine          string
	shouldFail      bool
	currentVersion  int64
	migrationsRun   bool
}

func NewMockMigrationProvider(engine string) *MockMigrationProvider {
	return &MockMigrationProvider{
		engine:         engine,
		currentVersion: 5,
	}
}

func (m *MockMigrationProvider) GetSupportedEngine() string {
	return m.engine
}

func (m *MockMigrationProvider) RunMigrations(ctx context.Context, config MigrationConfig) error {
	if m.shouldFail {
		return context.DeadlineExceeded
	}
	m.migrationsRun = true
	return nil
}

func (m *MockMigrationProvider) GetCurrentVersion(ctx context.Context, config MigrationConfig) (int64, error) {
	if m.shouldFail {
		return 0, context.DeadlineExceeded
	}
	return m.currentVersion, nil
}

func TestMigratorRegistry(t *testing.T) {
	t.Run("NewMigratorRegistry", func(t *testing.T) {
		registry := NewMigratorRegistry()
		require.NotNil(t, registry)
		require.Empty(t, registry.GetSupportedEngines())
	})

	t.Run("RegisterProvider", func(t *testing.T) {
		registry := NewMigratorRegistry()
		provider := NewMockMigrationProvider("test-engine")
		
		registry.RegisterProvider("test-engine", provider)
		
		engines := registry.GetSupportedEngines()
		require.Len(t, engines, 1)
		require.Contains(t, engines, "test-engine")
	})

	t.Run("GetProvider", func(t *testing.T) {
		registry := NewMigratorRegistry()
		provider := NewMockMigrationProvider("test-engine")
		
		registry.RegisterProvider("test-engine", provider)
		
		retrieved, exists := registry.GetProvider("test-engine")
		require.True(t, exists)
		require.Equal(t, provider, retrieved)
		
		nonExistent, exists := registry.GetProvider("non-existent")
		require.False(t, exists)
		require.Nil(t, nonExistent)
	})

	t.Run("GetSupportedEngines", func(t *testing.T) {
		registry := NewMigratorRegistry()
		
		provider1 := NewMockMigrationProvider("engine1")
		provider2 := NewMockMigrationProvider("engine2")
		
		registry.RegisterProvider("engine1", provider1)
		registry.RegisterProvider("engine2", provider2)
		
		engines := registry.GetSupportedEngines()
		require.Len(t, engines, 2)
		require.ElementsMatch(t, []string{"engine1", "engine2"}, engines)
	})

	t.Run("OverrideProvider", func(t *testing.T) {
		registry := NewMigratorRegistry()
		
		provider1 := NewMockMigrationProvider("test-engine")
		provider2 := NewMockMigrationProvider("test-engine")
		
		registry.RegisterProvider("test-engine", provider1)
		registry.RegisterProvider("test-engine", provider2) // Override
		
		retrieved, exists := registry.GetProvider("test-engine")
		require.True(t, exists)
		require.Equal(t, provider2, retrieved) // Should be the second provider
		require.NotEqual(t, provider1, retrieved)
	})
}

func TestMigrationConfig(t *testing.T) {
	t.Run("ValidConfig", func(t *testing.T) {
		config := MigrationConfig{
			Engine:        "postgres",
			URI:           "postgres://user:pass@localhost:5432/db",
			TargetVersion: 5,
			Timeout:       5 * time.Minute,
			Verbose:       true,
			Username:      "user",
			Password:      "pass",
		}
		
		require.Equal(t, "postgres", config.Engine)
		require.Equal(t, "postgres://user:pass@localhost:5432/db", config.URI)
		require.Equal(t, uint(5), config.TargetVersion)
		require.Equal(t, 5*time.Minute, config.Timeout)
		require.True(t, config.Verbose)
		require.Equal(t, "user", config.Username)
		require.Equal(t, "pass", config.Password)
	})
}

func TestMockMigrationProvider(t *testing.T) {
	t.Run("SuccessfulOperations", func(t *testing.T) {
		provider := NewMockMigrationProvider("mock-engine")
		
		require.Equal(t, "mock-engine", provider.GetSupportedEngine())
		
		config := MigrationConfig{
			Engine:  "mock-engine",
			URI:     "mock://uri",
			Timeout: 5 * time.Second,
		}
		
		ctx := context.Background()
		
		// Test RunMigrations
		err := provider.RunMigrations(ctx, config)
		require.NoError(t, err)
		require.True(t, provider.migrationsRun)
		
		// Test GetCurrentVersion
		version, err := provider.GetCurrentVersion(ctx, config)
		require.NoError(t, err)
		require.Equal(t, int64(5), version)
	})

	t.Run("FailureScenarios", func(t *testing.T) {
		provider := NewMockMigrationProvider("mock-engine")
		provider.shouldFail = true
		
		config := MigrationConfig{
			Engine:  "mock-engine",
			URI:     "mock://uri",
			Timeout: 5 * time.Second,
		}
		
		ctx := context.Background()
		
		// Test RunMigrations failure
		err := provider.RunMigrations(ctx, config)
		require.Error(t, err)
		require.Equal(t, context.DeadlineExceeded, err)
		
		// Test GetCurrentVersion failure
		version, err := provider.GetCurrentVersion(ctx, config)
		require.Error(t, err)
		require.Equal(t, context.DeadlineExceeded, err)
		require.Equal(t, int64(0), version)
	})
}
