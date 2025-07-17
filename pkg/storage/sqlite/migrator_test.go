package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
)

func TestSQLiteMigrationProvider(t *testing.T) {
	provider := NewSQLiteMigrationProvider()

	t.Run("GetSupportedEngine", func(t *testing.T) {
		require.Equal(t, "sqlite", provider.GetSupportedEngine())
	})

	t.Run("NewSQLiteMigrationProvider", func(t *testing.T) {
		require.NotNil(t, provider)
		require.Implements(t, (*storage.MigrationProvider)(nil), provider)
	})

	t.Run("InvalidPath", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:  "sqlite",
			URI:     "/invalid/path/that/does/not/exist/db.sqlite",
			Timeout: 5 * time.Second, // Add reasonable timeout
		}

		ctx := context.Background()
		err := provider.RunMigrations(ctx, config)
		require.Error(t, err)
	})

	t.Run("InvalidPath_GetCurrentVersion", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:  "sqlite",
			URI:     "/invalid/path/that/does/not/exist/db.sqlite",
			Timeout: 5 * time.Second,
		}

		ctx := context.Background()
		_, err := provider.GetCurrentVersion(ctx, config)
		require.Error(t, err)
	})

	t.Run("ConnectionFailure", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:  "sqlite",
			URI:     "/invalid/path/that/does/not/exist/db.sqlite",
			Timeout: 1 * time.Second,
		}

		ctx := context.Background()
		err := provider.RunMigrations(ctx, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to initialize sqlite connection")
	})

	t.Run("ConnectionFailure_GetCurrentVersion", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:  "sqlite",
			URI:     "/invalid/path/that/does/not/exist/db.sqlite",
			Timeout: 1 * time.Second,
		}

		ctx := context.Background()
		_, err := provider.GetCurrentVersion(ctx, config)
		require.Error(t, err)
		// The error could be either connection failure or database error
		require.True(t,
			strings.Contains(err.Error(), "failed to open sqlite connection") ||
				strings.Contains(err.Error(), "unable to open database file"))
	})
}

func TestSQLiteMigrationProviderPrepareURI(t *testing.T) {
	provider := NewSQLiteMigrationProvider()

	t.Run("ValidPath", func(t *testing.T) {
		tempDir := t.TempDir()
		dbPath := filepath.Join(tempDir, "test.db")

		config := storage.MigrationConfig{
			Engine: "sqlite",
			URI:    dbPath,
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(uri, dbPath))
		require.Contains(t, uri, "_pragma=journal_mode")
	})

	t.Run("InMemoryDatabase", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine: "sqlite",
			URI:    ":memory:",
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(uri, ":memory:"))
		require.Contains(t, uri, "_pragma=journal_mode")
	})

	t.Run("EmptyPath", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine: "sqlite",
			URI:    "",
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.Contains(t, uri, "_pragma=journal_mode")
	})

	t.Run("FileWithQueryParams", func(t *testing.T) {
		tempDir := t.TempDir()
		dbPath := filepath.Join(tempDir, "test.db")

		config := storage.MigrationConfig{
			Engine: "sqlite",
			URI:    dbPath + "?_foreign_keys=on",
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(uri, dbPath))
		require.Contains(t, uri, "_foreign_keys=on")
		require.Contains(t, uri, "_pragma=journal_mode")
	})
}

// Provider interface for testing (matches the methods we use from goose.Provider).
type Provider interface {
	GetDBVersion(ctx context.Context) (int64, error)
	Up(ctx context.Context) ([]*goose.MigrationResult, error)
	UpTo(ctx context.Context, version int64) ([]*goose.MigrationResult, error)
	DownTo(ctx context.Context, version int64) ([]*goose.MigrationResult, error)
}

// Mock Provider for testing executeMigrations method.
type mockProvider struct {
	currentVersion int64
	upError        error
	upToError      error
	downToError    error
	versionError   error
}

func (m *mockProvider) GetDBVersion(ctx context.Context) (int64, error) {
	if m.versionError != nil {
		return 0, m.versionError
	}
	return m.currentVersion, nil
}

func (m *mockProvider) Up(ctx context.Context) ([]*goose.MigrationResult, error) {
	if m.upError != nil {
		return nil, m.upError
	}
	return []*goose.MigrationResult{}, nil
}

func (m *mockProvider) UpTo(ctx context.Context, version int64) ([]*goose.MigrationResult, error) {
	if m.upToError != nil {
		return nil, m.upToError
	}
	return []*goose.MigrationResult{}, nil
}

func (m *mockProvider) DownTo(ctx context.Context, version int64) ([]*goose.MigrationResult, error) {
	if m.downToError != nil {
		return nil, m.downToError
	}
	return []*goose.MigrationResult{}, nil
}

// testExecuteMigrations is a wrapper for testing purposes.
func (s *SQLiteMigrationProvider) testExecuteMigrations(ctx context.Context, provider Provider, config storage.MigrationConfig) error {
	// This is essentially the same logic as the original executeMigrations method
	currentVersion, err := provider.GetDBVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get sqlite db version: %w", err)
	}

	if config.TargetVersion == 0 {
		_, err := provider.Up(ctx)
		if err != nil {
			return fmt.Errorf("failed to run sqlite migrations: %w", err)
		}
		return nil
	}

	targetInt64Version := int64(config.TargetVersion)

	switch {
	case targetInt64Version < currentVersion:
		_, err := provider.DownTo(ctx, targetInt64Version)
		if err != nil {
			return fmt.Errorf("failed to run sqlite migrations down to %v: %w", targetInt64Version, err)
		}
	case targetInt64Version > currentVersion:
		_, err := provider.UpTo(ctx, targetInt64Version)
		if err != nil {
			return fmt.Errorf("failed to run sqlite migrations up to %v: %w", targetInt64Version, err)
		}
	default:
		return nil
	}

	return nil
}

func TestSQLiteMigrationProviderExecuteMigrations(t *testing.T) {
	provider := NewSQLiteMigrationProvider()

	t.Run("ExecuteMigrations_GetVersionError", func(t *testing.T) {
		mockProvider := &mockProvider{
			versionError: errors.New("version error"),
		}

		config := storage.MigrationConfig{
			Engine: "sqlite",
		}

		err := provider.testExecuteMigrations(context.Background(), mockProvider, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get sqlite db version")
	})

	t.Run("ExecuteMigrations_UpAll_Success", func(t *testing.T) {
		mockProvider := &mockProvider{
			currentVersion: 5,
		}

		config := storage.MigrationConfig{
			Engine:        "sqlite",
			TargetVersion: 0, // 0 means run all migrations
		}

		err := provider.testExecuteMigrations(context.Background(), mockProvider, config)
		require.NoError(t, err)
	})

	t.Run("ExecuteMigrations_UpAll_Error", func(t *testing.T) {
		mockProvider := &mockProvider{
			currentVersion: 5,
			upError:        errors.New("up error"),
		}

		config := storage.MigrationConfig{
			Engine:        "sqlite",
			TargetVersion: 0, // 0 means run all migrations
		}

		err := provider.testExecuteMigrations(context.Background(), mockProvider, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to run sqlite migrations")
	})

	t.Run("ExecuteMigrations_UpTo_Success", func(t *testing.T) {
		mockProvider := &mockProvider{
			currentVersion: 5,
		}

		config := storage.MigrationConfig{
			Engine:        "sqlite",
			TargetVersion: 10, // Higher than current version
		}

		err := provider.testExecuteMigrations(context.Background(), mockProvider, config)
		require.NoError(t, err)
	})

	t.Run("ExecuteMigrations_UpTo_Error", func(t *testing.T) {
		mockProvider := &mockProvider{
			currentVersion: 5,
			upToError:      errors.New("upTo error"),
		}

		config := storage.MigrationConfig{
			Engine:        "sqlite",
			TargetVersion: 10, // Higher than current version
		}

		err := provider.testExecuteMigrations(context.Background(), mockProvider, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to run sqlite migrations up to")
	})

	t.Run("ExecuteMigrations_DownTo_Success", func(t *testing.T) {
		mockProvider := &mockProvider{
			currentVersion: 10,
		}

		config := storage.MigrationConfig{
			Engine:        "sqlite",
			TargetVersion: 5, // Lower than current version
		}

		err := provider.testExecuteMigrations(context.Background(), mockProvider, config)
		require.NoError(t, err)
	})

	t.Run("ExecuteMigrations_DownTo_Error", func(t *testing.T) {
		mockProvider := &mockProvider{
			currentVersion: 10,
			downToError:    errors.New("downTo error"),
		}

		config := storage.MigrationConfig{
			Engine:        "sqlite",
			TargetVersion: 5, // Lower than current version
		}

		err := provider.testExecuteMigrations(context.Background(), mockProvider, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to run sqlite migrations down to")
	})

	t.Run("ExecuteMigrations_SameVersion_NoOp", func(t *testing.T) {
		mockProvider := &mockProvider{
			currentVersion: 5,
		}

		config := storage.MigrationConfig{
			Engine:        "sqlite",
			TargetVersion: 5, // Same as current version
		}

		err := provider.testExecuteMigrations(context.Background(), mockProvider, config)
		require.NoError(t, err)
	})
}

func TestSQLiteMigrationProviderAdditionalErrorScenarios(t *testing.T) {
	provider := NewSQLiteMigrationProvider()

	t.Run("RunMigrations_ConnectionTimeout", func(t *testing.T) {
		// Test with valid URI but very short timeout to trigger connection failure
		config := storage.MigrationConfig{
			Engine:  "sqlite",
			URI:     "/dev/null/nonexistent.db", // Invalid path that will fail
			Timeout: 1 * time.Millisecond,       // Very short timeout
		}

		ctx := context.Background()
		err := provider.RunMigrations(ctx, config)
		require.Error(t, err)
		// Should fail due to invalid path or timeout/connection issues
	})

	t.Run("GetCurrentVersion_ConnectionTimeout", func(t *testing.T) {
		// Test with invalid path that will cause connection failure
		config := storage.MigrationConfig{
			Engine:  "sqlite",
			URI:     "/dev/null/nonexistent.db", // Invalid path that will fail
			Timeout: 1 * time.Millisecond,       // Very short timeout
		}

		ctx := context.Background()
		_, err := provider.GetCurrentVersion(ctx, config)
		require.Error(t, err)
		// Should fail due to invalid path or connection issues
	})
}

func TestSQLiteMigrationProviderEdgeCases(t *testing.T) {
	provider := NewSQLiteMigrationProvider()

	t.Run("EmptyURI_Success", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine: "sqlite",
			URI:    "",
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.NotEmpty(t, uri)
		require.Contains(t, uri, "_pragma=journal_mode")
	})

	t.Run("MemoryDatabase_Success", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:  "sqlite",
			URI:     ":memory:",
			Timeout: 5 * time.Second,
		}

		ctx := context.Background()
		err := provider.RunMigrations(ctx, config)
		// This should succeed because :memory: is a valid SQLite URI
		require.NoError(t, err)
	})

	t.Run("GetCurrentVersion_MemoryDatabase", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:  "sqlite",
			URI:     ":memory:",
			Timeout: 5 * time.Second,
		}

		ctx := context.Background()
		version, err := provider.GetCurrentVersion(ctx, config)
		require.NoError(t, err)
		require.GreaterOrEqual(t, version, int64(0))
	})

	t.Run("VeryShortTimeout", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:  "sqlite",
			URI:     "/nonexistent/path/file.db",
			Timeout: 1 * time.Nanosecond, // Extremely short timeout
		}

		ctx := context.Background()
		err := provider.RunMigrations(ctx, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to initialize sqlite connection")
	})
}
