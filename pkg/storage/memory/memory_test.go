package memory

import (
	"context"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
)

func TestMemdbStorage(t *testing.T) {
	ds := New()
	test.RunAllTests(t, ds)
}

func TestStaticTupleIteratorNoRace(t *testing.T) {
	iter := &staticIterator{
		tuples: []*openfgav1.Tuple{
			{
				Key: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
			{
				Key: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
		},
	}
	defer iter.Stop()

	go func() {
		_, err := iter.Next()
		require.NoError(t, err)
	}()

	go func() {
		_, err := iter.Next()
		require.NoError(t, err)
	}()
}

func TestWithTuplesOption(t *testing.T) {

	storeID := ulid.Make().String()

	ds := New(
		WithTuples(storeID, []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		}),
	)

	expectedTupleKey := tuple.NewTupleKey("document:1", "viewer", "user:jon")
	iter, err := ds.Read(context.Background(), storeID, expectedTupleKey)
	require.NoError(t, err)

	expectedTupleKeyString := tuple.TupleKeyToString(expectedTupleKey)
	tp, err := iter.Next()
	require.NoError(t, err)
	require.Equal(t, expectedTupleKeyString, tuple.TupleKeyToString(tp.GetKey()))

	tp, err = iter.Next()
	require.ErrorIs(t, err, storage.ErrIteratorDone)
	require.Nil(t, tp)

	changes, _, err := ds.ReadChanges(context.Background(), storeID, "document", storage.PaginationOptions{}, 0*time.Second)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	require.Equal(t, expectedTupleKeyString, tuple.TupleKeyToString(changes[0].TupleKey))
	require.Equal(t, openfgav1.TupleOperation_TUPLE_OPERATION_WRITE, changes[0].Operation)
}
