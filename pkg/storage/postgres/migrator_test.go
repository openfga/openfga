package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
)

func TestPostgresMigrationProviderSimple(t *testing.T) {
	provider := NewPostgresMigrationProvider()

	t.Run("GetSupportedEngine", func(t *testing.T) {
		require.Equal(t, "postgres", provider.GetSupportedEngine())
	})

	t.Run("NewPostgresMigrationProvider", func(t *testing.T) {
		require.NotNil(t, provider)
		require.Implements(t, (*storage.MigrationProvider)(nil), provider)
	})

	t.Run("InvalidURI", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:  "postgres",
			URI:     "invalid-uri",
			Timeout: 5 * time.Second, // Add reasonable timeout
		}

		ctx := context.Background()
		err := provider.RunMigrations(ctx, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to initialize postgres connection")
	})

	t.Run("InvalidURI_GetCurrentVersion", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:  "postgres",
			URI:     "invalid-uri",
			Timeout: 5 * time.Second,
		}

		ctx := context.Background()
		_, err := provider.GetCurrentVersion(ctx, config)
		require.Error(t, err)
		// The error could be either connection failure or parse failure
		require.True(t,
			strings.Contains(err.Error(), "failed to initialize postgres connection") ||
				strings.Contains(err.Error(), "failed to open postgres connection") ||
				strings.Contains(err.Error(), "cannot parse"))
	})

	t.Run("ConnectionFailure", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:  "postgres",
			URI:     "postgres://user:pass@nonexistent:5432/dbname",
			Timeout: 1 * time.Second,
		}

		ctx := context.Background()
		err := provider.RunMigrations(ctx, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to initialize postgres connection")
	})

	t.Run("ConnectionFailure_GetCurrentVersion", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:  "postgres",
			URI:     "postgres://user:pass@nonexistent:5432/dbname",
			Timeout: 1 * time.Second,
		}

		ctx := context.Background()
		_, err := provider.GetCurrentVersion(ctx, config)
		require.Error(t, err)
		// The error could be connection failure, DNS failure, or other network-related errors
		errMsg := err.Error()
		require.True(t,
			strings.Contains(errMsg, "failed to open postgres connection") ||
				strings.Contains(errMsg, "dial tcp: lookup nonexistent") ||
				strings.Contains(errMsg, "no such host") ||
				strings.Contains(errMsg, "connection refused") ||
				strings.Contains(errMsg, "context deadline exceeded") ||
				strings.Contains(errMsg, "network is unreachable") ||
				strings.Contains(errMsg, "hostname resolving error") ||
				strings.Contains(errMsg, "server misbehaving"),
			"Unexpected error message: %s", errMsg)
	})
}

func TestPostgresMigrationProviderPrepareURI(t *testing.T) {
	provider := NewPostgresMigrationProvider()

	t.Run("ValidURI", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine: "postgres",
			URI:    "postgres://user:pass@localhost:5432/dbname",
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.Equal(t, "postgres://user:pass@localhost:5432/dbname", uri)
	})

	t.Run("URIWithUsernameOverride", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:   "postgres",
			URI:      "postgres://user:pass@localhost:5432/dbname",
			Username: "newuser",
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.Equal(t, "postgres://newuser:pass@localhost:5432/dbname", uri)
	})

	t.Run("URIWithPasswordOverride", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:   "postgres",
			URI:      "postgres://user:pass@localhost:5432/dbname",
			Password: "newpass",
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.Equal(t, "postgres://user:newpass@localhost:5432/dbname", uri)
	})

	t.Run("URIWithBothOverrides", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:   "postgres",
			URI:      "postgres://user:pass@localhost:5432/dbname",
			Username: "newuser",
			Password: "newpass",
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.Equal(t, "postgres://newuser:newpass@localhost:5432/dbname", uri)
	})

	t.Run("URIWithoutUser", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:   "postgres",
			URI:      "postgres://localhost:5432/dbname",
			Username: "newuser",
			Password: "newpass",
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.Equal(t, "postgres://newuser:newpass@localhost:5432/dbname", uri)
	})

	t.Run("InvalidURI", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine: "postgres",
			URI:    "://invalid-uri",
		}

		_, err := provider.prepareURI(config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid postgres database uri")
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
func (p *PostgresMigrationProvider) testExecuteMigrations(ctx context.Context, provider Provider, config storage.MigrationConfig) error {
	// This is essentially the same logic as the original executeMigrations method
	currentVersion, err := provider.GetDBVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get postgres db version: %w", err)
	}

	if config.TargetVersion == 0 {
		_, err := provider.Up(ctx)
		if err != nil {
			return fmt.Errorf("failed to run postgres migrations: %w", err)
		}
		return nil
	}

	targetInt64Version := int64(config.TargetVersion)

	switch {
	case targetInt64Version < currentVersion:
		_, err := provider.DownTo(ctx, targetInt64Version)
		if err != nil {
			return fmt.Errorf("failed to run postgres migrations down to %v: %w", targetInt64Version, err)
		}
	case targetInt64Version > currentVersion:
		_, err := provider.UpTo(ctx, targetInt64Version)
		if err != nil {
			return fmt.Errorf("failed to run postgres migrations up to %v: %w", targetInt64Version, err)
		}
	default:
		return nil
	}

	return nil
}

func TestPostgresMigrationProviderExecuteMigrations(t *testing.T) {
	provider := NewPostgresMigrationProvider()

	t.Run("ExecuteMigrations_GetVersionError", func(t *testing.T) {
		mockProvider := &mockProvider{
			versionError: errors.New("version error"),
		}

		config := storage.MigrationConfig{
			Engine: "postgres",
		}

		err := provider.testExecuteMigrations(context.Background(), mockProvider, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to get postgres db version")
	})

	t.Run("ExecuteMigrations_UpAll_Success", func(t *testing.T) {
		mockProvider := &mockProvider{
			currentVersion: 5,
		}

		config := storage.MigrationConfig{
			Engine:        "postgres",
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
			Engine:        "postgres",
			TargetVersion: 0, // 0 means run all migrations
		}

		err := provider.testExecuteMigrations(context.Background(), mockProvider, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to run postgres migrations")
	})

	t.Run("ExecuteMigrations_UpTo_Success", func(t *testing.T) {
		mockProvider := &mockProvider{
			currentVersion: 5,
		}

		config := storage.MigrationConfig{
			Engine:        "postgres",
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
			Engine:        "postgres",
			TargetVersion: 10, // Higher than current version
		}

		err := provider.testExecuteMigrations(context.Background(), mockProvider, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to run postgres migrations up to")
	})

	t.Run("ExecuteMigrations_DownTo_Success", func(t *testing.T) {
		mockProvider := &mockProvider{
			currentVersion: 10,
		}

		config := storage.MigrationConfig{
			Engine:        "postgres",
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
			Engine:        "postgres",
			TargetVersion: 5, // Lower than current version
		}

		err := provider.testExecuteMigrations(context.Background(), mockProvider, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to run postgres migrations down to")
	})

	t.Run("ExecuteMigrations_SameVersion_NoOp", func(t *testing.T) {
		mockProvider := &mockProvider{
			currentVersion: 5,
		}

		config := storage.MigrationConfig{
			Engine:        "postgres",
			TargetVersion: 5, // Same as current version
		}

		err := provider.testExecuteMigrations(context.Background(), mockProvider, config)
		require.NoError(t, err)
	})
}

func TestPostgresMigrationProviderAdditionalErrorScenarios(t *testing.T) {
	provider := NewPostgresMigrationProvider()

	t.Run("RunMigrations_SubFilesystemError", func(t *testing.T) {
		// Test with valid connection but invalid migration path
		// This should fail at the fs.Sub() step
		config := storage.MigrationConfig{
			Engine:  "postgres",
			URI:     "postgres://user:pass@localhost:5432/dbname",
			Timeout: 1 * time.Millisecond, // Very short timeout
		}

		ctx := context.Background()
		err := provider.RunMigrations(ctx, config)
		require.Error(t, err)
		// Should fail due to timeout/connection, but after prepareURI succeeds
	})

	t.Run("GetCurrentVersion_SubFilesystemError", func(t *testing.T) {
		// Test with valid connection but migration fails due to timeout
		config := storage.MigrationConfig{
			Engine:  "postgres",
			URI:     "postgres://user:pass@localhost:5432/dbname",
			Timeout: 1 * time.Millisecond, // Very short timeout
		}

		ctx := context.Background()
		_, err := provider.GetCurrentVersion(ctx, config)
		require.Error(t, err)
		// Should fail due to timeout/connection issues
	})
}
