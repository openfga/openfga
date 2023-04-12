package sqlcommon

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestHandleSQLError(t *testing.T) {
	t.Run("duplicate_key_value_error_with_tuple_key_wraps_ErrInvalidWriteInput", func(t *testing.T) {
		err := HandleSQLError(errors.New("duplicate key value"), &openfgapb.TupleKey{
			Object:   "object",
			Relation: "relation",
			User:     "user",
		})
		require.ErrorIs(t, err, storage.ErrInvalidWriteInput)
	})

	t.Run("duplicate_entry_value_error_with_tuple_key_wraps_ErrInvalidWriteInput", func(t *testing.T) {
		duplicateKeyError := &mysql.MySQLError{
			Number:  1062,
			Message: "Duplicate entry '' for key ''",
		}
		err := HandleSQLError(duplicateKeyError, &openfgapb.TupleKey{
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
		err := HandleSQLError(duplicateKeyError)

		require.ErrorIs(t, err, storage.ErrCollision)
	})

	t.Run("duplicate_key_value_error_without_tuple_key_returns_collision", func(t *testing.T) {
		duplicateKeyError := errors.New("duplicate key value")
		err := HandleSQLError(duplicateKeyError)
		require.ErrorIs(t, err, storage.ErrCollision)
	})

	t.Run("sql.ErrNoRows_is_converted_to_storage.ErrNotFound_error", func(t *testing.T) {
		err := HandleSQLError(sql.ErrNoRows)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})
}

func TestFromUserParts(t *testing.T) {
	require.Equal(t, "jon", FromUserParts("", "jon", ""))
	require.Equal(t, "user:jon", FromUserParts("user", "jon", ""))
	require.Equal(t, "user:*", FromUserParts("user", "*", ""))
	require.Equal(t, "group:eng#member", FromUserParts("group", "eng", "member"))
}

func TestToUserParts(t *testing.T) {
	userObjectType, userObjectID, userRelation := ToUserParts("jon")
	require.Equal(t, "", userObjectType)
	require.Equal(t, "jon", userObjectID)
	require.Equal(t, "", userRelation)

	userObjectType, userObjectID, userRelation = ToUserParts("user:jon")
	require.Equal(t, "user", userObjectType)
	require.Equal(t, "jon", userObjectID)
	require.Equal(t, "", userRelation)

	userObjectType, userObjectID, userRelation = ToUserParts("user:*")
	require.Equal(t, "user", userObjectType)
	require.Equal(t, "*", userObjectID)
	require.Equal(t, "", userRelation)

	userObjectType, userObjectID, userRelation = ToUserParts("group:eng#member")
	require.Equal(t, "group", userObjectType)
	require.Equal(t, "eng", userObjectID)
	require.Equal(t, "member", userRelation)
}

func TestToUserPartsFromObjectRelation(t *testing.T) {
	userObjectType, userObjectID, userRelation := ToUserPartsFromObjectRelation(&openfgapb.ObjectRelation{Object: "group:eng", Relation: "member"})
	require.Equal(t, "group", userObjectType)
	require.Equal(t, "eng", userObjectID)
	require.Equal(t, "member", userRelation)

	userObjectType, userObjectID, userRelation = ToUserPartsFromObjectRelation(&openfgapb.ObjectRelation{Object: "user:*"})
	require.Equal(t, "user", userObjectType)
	require.Equal(t, "*", userObjectID)
	require.Equal(t, "", userRelation)
}
