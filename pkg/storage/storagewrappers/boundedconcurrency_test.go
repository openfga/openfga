package storagewrappers

import (
	"context"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

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

	var wg errgroup.Group

	start := time.Now()

	wg.Go(func() error {
		_, err := limitedTupleReader.ReadUserTuple(context.Background(), store, tuple.NewTupleKey("obj:1", "viewer", "user:anne"))
		return err
	})

	wg.Go(func() error {
		_, err := limitedTupleReader.ReadUsersetTuples(context.Background(), store, storage.ReadUsersetTuplesFilter{
			Object:   "obj:1",
			Relation: "viewer",
		})
		return err
	})

	wg.Go(func() error {
		_, err := limitedTupleReader.Read(context.Background(), store, nil)
		return err
	})

	wg.Go(func() error {
		_, err := limitedTupleReader.ReadStartingWithUser(
			context.Background(),
			store,
			storage.ReadStartingWithUserFilter{
				UserFilter: []*openfgav1.ObjectRelation{
					{
						Object:   "obj",
						Relation: "viewer",
					},
				},
			})
		return err
	})

	err = wg.Wait()
	require.NoError(t, err)

	end := time.Now()

	require.GreaterOrEqual(t, end.Sub(start), numRoutine*time.Second)
}
