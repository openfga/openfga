package postgres

import (
	"context"
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
}
