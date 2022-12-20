package mysql

import (
	"database/sql"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestHandleMySQLError(t *testing.T) {
	t.Run("duplicate_entry_value_error_with_tuple_key_wraps_ErrInvalidWriteInput", func(t *testing.T) {
		duplicateKeyError := &mysql.MySQLError{
			Number:  1062,
			Message: "Duplicate entry '' for key ''",
		}
		err := handleMySQLError(duplicateKeyError, &openfgapb.TupleKey{
			Object:   "object",
			Relation: "relation",
			User:     "user",
		})
		require.ErrorIs(t, err, storage.ErrInvalidWriteInput)
	})

	t.Run("duplicate_entry_value_error_without_tuple_key_returns_collision", func(t *testing.T) {
		duplicateKeyError := &mysql.MySQLError{
			Number:  1062,
			Message: "Duplicate entry '' for key ''",
		}
		err := handleMySQLError(duplicateKeyError)

		require.ErrorIs(t, err, storage.ErrCollision)
	})

	t.Run("sql.ErrNoRows_is_converted_to_storage.ErrNotFound_error", func(t *testing.T) {
		err := handleMySQLError(sql.ErrNoRows)

		require.ErrorIs(t, err, storage.ErrNotFound)
	})
}
