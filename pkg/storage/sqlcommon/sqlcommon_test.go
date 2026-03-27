package sqlcommon

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

// changelogOperationIndex is the index of the "operation" field within each
// changeLogItems row produced by GetDeleteWriteChangelogItems.
// The row layout is: store, object_type, object_id, relation,
// _user, condition_name, condition_context, operation, ulid, inserted_at.
const changelogOperationIndex = 7

// Background: pgx uses the fmt.Stringer interface to encode values when it
// falls back to the "simple query" protocol (e.g. when behind PgBouncer or
// Azure Flexible Server's built-in connection pooler). Because
// openfgav1.TupleOperation implements fmt.Stringer it would be serialised as
// "TUPLE_OPERATION_WRITE" / "TUPLE_OPERATION_DELETE" — strings that PostgreSQL
// rejects for an INTEGER column (SQLSTATE 22P02). Passing a bare int32 avoids
// this code path entirely and is safe for both binary and text wire formats.
// See: https://github.com/openfga/openfga/issues/3011
func TestGetDeleteWriteChangelogItems_OperationIsInt32(t *testing.T) {
	t.Parallel()

	store := "test-store"
	now := time.Now()

	t.Run("write operation type is int32", func(t *testing.T) {
		t.Parallel()

		tk := tupleUtils.NewTupleKey("document:1", "viewer", "user:alice")
		_, _, changeLogItems, err := GetDeleteWriteChangelogItems(
			store,
			map[string]*openfgav1.Tuple{}, // no pre-existing tuples
			WriteData{
				Deletes: storage.Deletes{},
				Writes:  storage.Writes{tk},
				Opts:    storage.NewTupleWriteOptions(),
				Now:     now,
			},
		)
		require.NoError(t, err)
		require.Len(t, changeLogItems, 1)

		opVal := changeLogItems[0][changelogOperationIndex]

		assert.Equal(t, reflect.TypeOf(int32(0)), reflect.TypeOf(opVal),
			"operation value must be int32, got %T", opVal)
		assert.Equal(t, int32(openfgav1.TupleOperation_TUPLE_OPERATION_WRITE), opVal)
	})

	t.Run("delete operation type is int32", func(t *testing.T) {
		t.Parallel()

		tk := &openfgav1.TupleKeyWithoutCondition{Object: "document:1", Relation: "viewer", User: "user:alice"}
		existingTuple := &openfgav1.Tuple{
			Key: tupleUtils.NewTupleKey("document:1", "viewer", "user:alice"),
		}
		existing := map[string]*openfgav1.Tuple{
			tupleUtils.TupleKeyToString(existingTuple.GetKey()): existingTuple,
		}

		_, _, changeLogItems, err := GetDeleteWriteChangelogItems(
			store,
			existing,
			WriteData{
				Deletes: storage.Deletes{tk},
				Writes:  storage.Writes{},
				Opts:    storage.NewTupleWriteOptions(),
				Now:     now,
			},
		)
		require.NoError(t, err)
		require.Len(t, changeLogItems, 1)

		opVal := changeLogItems[0][changelogOperationIndex]

		assert.Equal(t, reflect.TypeOf(int32(0)), reflect.TypeOf(opVal),
			"operation value must be int32, got %T", opVal)
		assert.Equal(t, int32(openfgav1.TupleOperation_TUPLE_OPERATION_DELETE), opVal)
	})

	t.Run("mixed write and delete both produce int32 operations", func(t *testing.T) {
		t.Parallel()

		writeTk := tupleUtils.NewTupleKey("document:2", "editor", "user:bob")
		deleteTk := &openfgav1.TupleKeyWithoutCondition{Object: "document:1", Relation: "viewer", User: "user:alice"}
		existingTuple := &openfgav1.Tuple{
			Key: tupleUtils.NewTupleKey("document:1", "viewer", "user:alice"),
		}
		existing := map[string]*openfgav1.Tuple{
			tupleUtils.TupleKeyToString(existingTuple.GetKey()): existingTuple,
		}

		_, _, changeLogItems, err := GetDeleteWriteChangelogItems(
			store,
			existing,
			WriteData{
				Deletes: storage.Deletes{deleteTk},
				Writes:  storage.Writes{writeTk},
				Opts:    storage.NewTupleWriteOptions(),
				Now:     now,
			},
		)
		require.NoError(t, err)
		require.Len(t, changeLogItems, 2)

		for i, item := range changeLogItems {
			opVal := item[changelogOperationIndex]
			assert.Equal(t, reflect.TypeOf(int32(0)), reflect.TypeOf(opVal),
				"changeLogItems[%d] operation must be int32, got %T", i, opVal)

			intVal, ok := opVal.(int32)
			require.True(t, ok, "changeLogItems[%d] operation could not be asserted as int32", i)
			assert.Contains(t,
				[]int32{
					int32(openfgav1.TupleOperation_TUPLE_OPERATION_WRITE),
					int32(openfgav1.TupleOperation_TUPLE_OPERATION_DELETE),
				},
				intVal,
			)
		}
	})
}
