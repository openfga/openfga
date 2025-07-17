package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MockMigrationProvider for testing.
type MockMigrationProvider struct {
	engine         string
	shouldFail     bool
	currentVersion int64
	migrationsRun  bool
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

		// Make providers distinguishable by setting different versions
		provider1.currentVersion = 1
		provider2.currentVersion = 2

		registry.RegisterProvider("test-engine", provider1)
		registry.RegisterProvider("test-engine", provider2) // Override

		retrieved, exists := registry.GetProvider("test-engine")
		require.True(t, exists)
		require.Equal(t, provider2, retrieved) // Should be the second provider
		require.NotEqual(t, provider1, retrieved)
	})
}

func TestMigratorRegistryEdgeCases(t *testing.T) {
	t.Run("EmptyProviderName", func(t *testing.T) {
		registry := NewMigratorRegistry()
		provider := NewMockMigrationProvider("test")

		// Test empty engine name
		registry.RegisterProvider("", provider)
		engines := registry.GetSupportedEngines()
		require.Len(t, engines, 1)
		require.Contains(t, engines, "")
	})

	t.Run("NilProvider", func(t *testing.T) {
		registry := NewMigratorRegistry()

		// Register nil provider
		registry.RegisterProvider("test", nil)

		provider, exists := registry.GetProvider("test")
		require.True(t, exists)
		require.Nil(t, provider)
	})

	t.Run("MultipleRegistrations", func(t *testing.T) {
		registry := NewMigratorRegistry()

		provider1 := NewMockMigrationProvider("engine1")
		provider2 := NewMockMigrationProvider("engine2")
		provider3 := NewMockMigrationProvider("engine3")

		registry.RegisterProvider("engine1", provider1)
		registry.RegisterProvider("engine2", provider2)
		registry.RegisterProvider("engine3", provider3)

		engines := registry.GetSupportedEngines()
		require.Len(t, engines, 3)
		require.ElementsMatch(t, []string{"engine1", "engine2", "engine3"}, engines)
	})

	t.Run("GetNonExistentProvider", func(t *testing.T) {
		registry := NewMigratorRegistry()

		provider, exists := registry.GetProvider("non-existent")
		require.False(t, exists)
		require.Nil(t, provider)
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

func TestMigrationConfigValidation(t *testing.T) {
	t.Run("ZeroValues", func(t *testing.T) {
		config := MigrationConfig{}

		require.Empty(t, config.Engine)
		require.Empty(t, config.URI)
		require.Equal(t, uint(0), config.TargetVersion)
		require.Equal(t, time.Duration(0), config.Timeout)
		require.False(t, config.Verbose)
		require.Empty(t, config.Username)
		require.Empty(t, config.Password)
	})

	t.Run("AllFieldsSet", func(t *testing.T) {
		config := MigrationConfig{
			Engine:        "postgres",
			URI:           "postgres://user:pass@localhost:5432/db",
			TargetVersion: 10,
			Timeout:       30 * time.Second,
			Verbose:       true,
			Username:      "testuser",
			Password:      "testpass",
		}

		require.Equal(t, "postgres", config.Engine)
		require.Equal(t, "postgres://user:pass@localhost:5432/db", config.URI)
		require.Equal(t, uint(10), config.TargetVersion)
		require.Equal(t, 30*time.Second, config.Timeout)
		require.True(t, config.Verbose)
		require.Equal(t, "testuser", config.Username)
		require.Equal(t, "testpass", config.Password)
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

func TestMockMigrationProviderExtendedScenarios(t *testing.T) {
	t.Run("CustomEngine", func(t *testing.T) {
		provider := NewMockMigrationProvider("custom-engine")
		require.Equal(t, "custom-engine", provider.GetSupportedEngine())
	})

	t.Run("ModifyCurrentVersion", func(t *testing.T) {
		provider := NewMockMigrationProvider("test")
		provider.currentVersion = 100

		config := MigrationConfig{
			Engine: "test",
			URI:    "test://uri",
		}

		version, err := provider.GetCurrentVersion(context.Background(), config)
		require.NoError(t, err)
		require.Equal(t, int64(100), version)
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		provider := NewMockMigrationProvider("test")

		config := MigrationConfig{
			Engine: "test",
			URI:    "test://uri",
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		// Mock provider doesn't check context, but we can test that it still works
		err := provider.RunMigrations(ctx, config)
		require.NoError(t, err)
	})

	t.Run("StateTracking", func(t *testing.T) {
		provider := NewMockMigrationProvider("test")
		require.False(t, provider.migrationsRun)

		config := MigrationConfig{
			Engine: "test",
			URI:    "test://uri",
		}

		err := provider.RunMigrations(context.Background(), config)
		require.NoError(t, err)
		require.True(t, provider.migrationsRun)
	})
}

func TestMigratorRegistryThreadSafety(t *testing.T) {
	t.Run("ConcurrentRegistrations", func(t *testing.T) {
		registry := NewMigratorRegistry()

		// Test concurrent registrations
		var wg sync.WaitGroup
		numGoroutines := 10

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				engineName := fmt.Sprintf("concurrent-engine-%d", id)
				provider := NewMockMigrationProvider(engineName)
				registry.RegisterProvider(engineName, provider)
			}(i)
		}

		wg.Wait()

		// Verify all providers were registered
		engines := registry.GetSupportedEngines()
		require.Len(t, engines, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			engineName := fmt.Sprintf("concurrent-engine-%d", i)
			provider, exists := registry.GetProvider(engineName)
			require.True(t, exists)
			require.NotNil(t, provider)
			require.Equal(t, engineName, provider.GetSupportedEngine())
		}
	})

	t.Run("ConcurrentGetProvider", func(t *testing.T) {
		registry := NewMigratorRegistry()
		provider := NewMockMigrationProvider("shared-engine")
		registry.RegisterProvider("shared-engine", provider)

		// Test concurrent access to the same provider
		var wg sync.WaitGroup
		numGoroutines := 50
		results := make([]MigrationProvider, numGoroutines)
		exists := make([]bool, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				p, ex := registry.GetProvider("shared-engine")
				results[id] = p
				exists[id] = ex
			}(i)
		}

		wg.Wait()

		// Verify all goroutines got the same provider and it existed
		for i := 0; i < numGoroutines; i++ {
			require.True(t, exists[i])
			require.Equal(t, provider, results[i])
		}
	})
}

func TestMigrationProviderInterface(t *testing.T) {
	t.Run("InterfaceCompliance", func(t *testing.T) {
		provider := NewMockMigrationProvider("test")

		// Verify it implements the MigrationProvider interface
		var _ MigrationProvider = provider

		// Test all interface methods
		require.Equal(t, "test", provider.GetSupportedEngine())

		config := MigrationConfig{
			Engine: "test",
			URI:    "test://localhost/db",
		}

		ctx := context.Background()

		err := provider.RunMigrations(ctx, config)
		require.NoError(t, err)

		version, err := provider.GetCurrentVersion(ctx, config)
		require.NoError(t, err)
		require.Equal(t, int64(5), version)
	})

	t.Run("NilProvider", func(t *testing.T) {
		registry := NewMigratorRegistry()

		// Register nil provider
		registry.RegisterProvider("nil-engine", nil)

		provider, exists := registry.GetProvider("nil-engine")
		require.True(t, exists)
		require.Nil(t, provider)

		engines := registry.GetSupportedEngines()
		require.Contains(t, engines, "nil-engine")
	})
}
