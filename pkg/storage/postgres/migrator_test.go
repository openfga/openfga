package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

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
				strings.Contains(errMsg, "network is unreachable"),
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
