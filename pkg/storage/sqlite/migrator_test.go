package sqlite

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
