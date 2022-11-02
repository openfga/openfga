package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestHandlePostgresError(t *testing.T) {
	t.Run("duplicate key value error with tuple key wraps ErrInvalidWriteInput", func(t *testing.T) {
		err := handlePostgresError(errors.New("duplicate key value"), &openfgapb.TupleKey{
			Object:   "object",
			Relation: "relation",
			User:     "user",
		})
		require.ErrorIs(t, err, storage.ErrInvalidWriteInput)
	})

	t.Run("duplicate key value error without tuple key returns collision", func(t *testing.T) {
		duplicateKeyError := errors.New("duplicate key value")
		err := handlePostgresError(duplicateKeyError)
		require.ErrorIs(t, err, storage.ErrCollision)
	})

	t.Run("pgx.ErrNoRows is converted to storage.ErrNotFound error", func(t *testing.T) {
		err := handlePostgresError(pgx.ErrNoRows)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})
}
