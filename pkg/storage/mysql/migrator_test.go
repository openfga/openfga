package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
)

func TestMySQLMigrationProvider(t *testing.T) {
	provider := NewMySQLMigrationProvider()

	t.Run("GetSupportedEngine", func(t *testing.T) {
		require.Equal(t, "mysql", provider.GetSupportedEngine())
	})

	t.Run("NewMySQLMigrationProvider", func(t *testing.T) {
		require.NotNil(t, provider)
		require.Implements(t, (*storage.MigrationProvider)(nil), provider)
	})
}

func TestMySQLMigrationProviderErrors(t *testing.T) {
	provider := NewMySQLMigrationProvider()

	t.Run("InvalidURI", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine: "mysql",
			URI:    "invalid-uri",
		}

		ctx := context.Background()
		err := provider.RunMigrations(ctx, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid mysql database uri")
	})

	t.Run("InvalidURI_GetCurrentVersion", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine: "mysql",
			URI:    "invalid-uri",
		}

		ctx := context.Background()
		_, err := provider.GetCurrentVersion(ctx, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid mysql database uri")
	})

	t.Run("ConnectionFailure", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:  "mysql",
			URI:     "user:pass@tcp(nonexistent:3306)/dbname",
			Timeout: 1 * time.Second,
		}

		ctx := context.Background()
		err := provider.RunMigrations(ctx, config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to initialize mysql connection")
	})

	t.Run("ConnectionFailure_GetCurrentVersion", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:  "mysql",
			URI:     "user:pass@tcp(nonexistent:3306)/dbname",
			Timeout: 1 * time.Second,
		}

		ctx := context.Background()
		_, err := provider.GetCurrentVersion(ctx, config)
		require.Error(t, err)
		// The error could be either connection failure or DNS failure
		require.True(t,
			strings.Contains(err.Error(), "failed to open mysql connection") ||
				strings.Contains(err.Error(), "dial tcp: lookup nonexistent"))
	})
}

func TestMySQLMigrationProviderPrepareURI(t *testing.T) {
	provider := NewMySQLMigrationProvider()

	t.Run("ValidURI", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine: "mysql",
			URI:    "user:pass@tcp(localhost:3306)/dbname",
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.Equal(t, "user:pass@tcp(localhost:3306)/dbname", uri)
	})

	t.Run("URIWithUsernameOverride", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:   "mysql",
			URI:      "user:pass@tcp(localhost:3306)/dbname",
			Username: "newuser",
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.Equal(t, "newuser:pass@tcp(localhost:3306)/dbname", uri)
	})

	t.Run("URIWithPasswordOverride", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:   "mysql",
			URI:      "user:pass@tcp(localhost:3306)/dbname",
			Password: "newpass",
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.Equal(t, "user:newpass@tcp(localhost:3306)/dbname", uri)
	})

	t.Run("URIWithBothOverrides", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine:   "mysql",
			URI:      "user:pass@tcp(localhost:3306)/dbname",
			Username: "newuser",
			Password: "newpass",
		}

		uri, err := provider.prepareURI(config)
		require.NoError(t, err)
		require.Equal(t, "newuser:newpass@tcp(localhost:3306)/dbname", uri)
	})

	t.Run("InvalidURI", func(t *testing.T) {
		config := storage.MigrationConfig{
			Engine: "mysql",
			URI:    "invalid-uri",
		}

		_, err := provider.prepareURI(config)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid mysql database uri")
	})
}
