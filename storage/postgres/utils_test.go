package postgres

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
)

func TestHandlePostgresError(t *testing.T) {
	t.Run("duplicate key value error with tuple key wraps ErrInvalidWriteInput", func(t *testing.T) {
		err := handlePostgresError(errors.New("duplicate key value"))
		require.ErrorIs(t, err, storage.ErrCollision)
	})

	t.Run("duplicate key value error without tuple key returns collision", func(t *testing.T) {
		duplicateKeyError := errors.New("duplicate key value")
		err := handlePostgresError(duplicateKeyError)
		require.ErrorIs(t, err, storage.ErrCollision)
	})

	t.Run("sql.ErrNoRows is converted to storage.ErrNotFound error", func(t *testing.T) {
		err := handlePostgresError(sql.ErrNoRows)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})
}
