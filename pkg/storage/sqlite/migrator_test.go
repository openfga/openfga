package sqlite

import (
	"context"
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
}
