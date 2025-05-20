package storagewrappers

import (
	"context"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestBoundedWrapper(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	store := ulid.Make().String()
	slowBackend := mocks.NewMockSlowDataStorage(memory.New(), time.Second)

	err := slowBackend.Write(context.Background(), store, []*openfgav1.TupleKeyWithoutCondition{}, []*openfgav1.TupleKey{
		tuple.NewTupleKey("obj:1", "viewer", "user:anne"),
	})
	require.NoError(t, err)

	t.Run("normal case", func(t *testing.T) {
		// Create a limited tuple reader that allows 1 concurrent read a time.
		limitedTupleReader := NewBoundedTupleReader(slowBackend, &Operation{Method: apimethod.Check, Concurrency: 1, ThrottleThreshold: 2, ThrottleDuration: 500 * time.Millisecond})

		// Do reads from 4 goroutines - each should be run serially. Should be >4 seconds.
		const numRoutine = 4

		var wg errgroup.Group

		start := time.Now()

		wg.Go(func() error {
			_, err := limitedTupleReader.ReadUserTuple(context.Background(), store, tuple.NewTupleKey("obj:1", "viewer", "user:anne"), storage.ReadUserTupleOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.ReadUsersetTuples(context.Background(), store, storage.ReadUsersetTuplesFilter{
				Object:   "obj:1",
				Relation: "viewer",
			}, storage.ReadUsersetTuplesOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(context.Background(), store, nil, storage.ReadOptions{})
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
					}}, storage.ReadStartingWithUserOptions{})
			return err
		})

		err = wg.Wait()
		require.NoError(t, err)

		end := time.Now()

		require.GreaterOrEqual(t, end.Sub(start), numRoutine*time.Second+1) // 2 throttles should add a full second
		require.Equal(t, uint32(4), limitedTupleReader.GetMetadata().DatastoreQueryCount)
		require.True(t, limitedTupleReader.GetMetadata().WasThrottled)
	})

	t.Run("ctx cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		// Create a limited tuple reader that allows 1 concurrent read a time.
		limitedTupleReader := NewBoundedTupleReader(slowBackend, &Operation{Method: apimethod.Check, Concurrency: 1, ThrottleThreshold: 2, ThrottleDuration: 500 * time.Millisecond})

		var wg errgroup.Group

		wg.Go(func() error {
			_, err := limitedTupleReader.ReadUserTuple(ctx, store, tuple.NewTupleKey("obj:1", "viewer", "user:anne"), storage.ReadUserTupleOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, nil, storage.ReadOptions{})
			return err
		})

		err = wg.Wait()
		require.NoError(t, err)

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, nil, storage.ReadOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, nil, storage.ReadOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, nil, storage.ReadOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, nil, storage.ReadOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, nil, storage.ReadOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, nil, storage.ReadOptions{})
			return err
		})

		// trigger cancellation
		cancel()
		err = wg.Wait()
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, limitedTupleReader.GetMetadata().WasThrottled)
	})
}

func TestBoundedConcurrencyWrapper_Exits_Early_If_Context_Error(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	// concurrency set to zero to allow zero calls to go through
	dut := NewBoundedTupleReader(mockDatastore, &Operation{Concurrency: 0, Method: apimethod.Check})

	var testCases = map[string]struct {
		requestFunc func(ctx context.Context) (any, error)
	}{
		`read`: {
			requestFunc: func(ctx context.Context) (any, error) {
				return dut.Read(ctx, ulid.Make().String(), &openfgav1.TupleKey{}, storage.ReadOptions{})
			},
		},
		`read_user_tuple`: {
			requestFunc: func(ctx context.Context) (any, error) {
				return dut.ReadUserTuple(ctx, ulid.Make().String(), &openfgav1.TupleKey{}, storage.ReadUserTupleOptions{})
			},
		},
		`read_userset_tuples`: {
			requestFunc: func(ctx context.Context) (any, error) {
				return dut.ReadUsersetTuples(ctx, ulid.Make().String(), storage.ReadUsersetTuplesFilter{}, storage.ReadUsersetTuplesOptions{})
			},
		},
		`read_starting_with_user`: {
			requestFunc: func(ctx context.Context) (any, error) {
				return dut.ReadStartingWithUser(ctx, ulid.Make().String(), storage.ReadStartingWithUserFilter{}, storage.ReadStartingWithUserOptions{})
			},
		},
	}

	for testName, test := range testCases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			resp, err := test.requestFunc(ctx)
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, resp)
		})
	}
}
