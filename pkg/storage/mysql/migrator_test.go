package mysql

import (
	"context"
	"testing"

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
}
