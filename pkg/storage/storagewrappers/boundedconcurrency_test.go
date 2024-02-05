package storagewrappers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestBoundedConcurrencyWrapper(t *testing.T) {
	store := ulid.Make().String()
	slowBackend := mocks.NewMockSlowDataStorage(memory.New(), time.Second)

	err := slowBackend.Write(context.Background(), store, []*openfgav1.TupleKeyWithoutCondition{}, []*openfgav1.TupleKey{
		tuple.NewTupleKey("obj:1", "viewer", "user:anne"),
	})
	require.NoError(t, err)

	// Create a limited tuple reader that allows 1 concurrent read a time.
	limitedTupleReader := NewBoundedConcurrencyTupleReader(slowBackend, 1)

	// Do reads from 4 goroutines - each should be run serially. Should be >4 seconds.
	const numRoutine = 4

	var wg sync.WaitGroup
	wg.Add(numRoutine)

	start := time.Now()

	go func() {
		_, err := limitedTupleReader.ReadUserTuple(context.Background(), store, tuple.NewTupleKey("obj:1", "viewer", "user:anne"))
		require.NoError(t, err)
		wg.Done()
	}()

	go func() {
		_, err := limitedTupleReader.ReadUsersetTuples(context.Background(), store, storage.ReadUsersetTuplesFilter{
			Object:   "obj:1",
			Relation: "viewer",
		})
		require.NoError(t, err)
		wg.Done()
	}()

	go func() {
		_, err := limitedTupleReader.Read(context.Background(), store, nil)
		require.NoError(t, err)
		wg.Done()
	}()

	go func() {
		_, err := limitedTupleReader.ReadStartingWithUser(
			context.Background(),
			store,
			storage.ReadStartingWithUserFilter{
				UserFilter: []*openfgav1.ObjectRelation{
					{
						Object:   "obj",
						Relation: "viewer",
					},
				}})
		require.NoError(t, err)
		wg.Done()
	}()

	wg.Wait()

	end := time.Now()

	require.GreaterOrEqual(t, end.Sub(start), numRoutine*time.Second)
}
