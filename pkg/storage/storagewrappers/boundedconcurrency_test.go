package storagewrappers

import (
	"context"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestBoundedConcurrencyWrapper(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
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
		_, err := limitedTupleReader.ReadUserTuple(context.Background(), store, tuple.NewTupleKey("obj:1", "viewer", "user:anne"), storage.QueryOptions{Consistency: openfgav1.ConsistencyPreference_UNSPECIFIED})
		return err
	})

	wg.Go(func() error {
		_, err := limitedTupleReader.ReadUsersetTuples(context.Background(), store, storage.ReadUsersetTuplesFilter{
			Object:   "obj:1",
			Relation: "viewer",
		}, storage.QueryOptions{Consistency: openfgav1.ConsistencyPreference_UNSPECIFIED})
		return err
	})

	wg.Go(func() error {
		_, err := limitedTupleReader.Read(context.Background(), store, nil, storage.QueryOptions{Consistency: openfgav1.ConsistencyPreference_UNSPECIFIED})
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
				}}, storage.QueryOptions{Consistency: openfgav1.ConsistencyPreference_UNSPECIFIED})
		return err
	})

	err = wg.Wait()
	require.NoError(t, err)

	end := time.Now()

	require.GreaterOrEqual(t, end.Sub(start), numRoutine*time.Second)
}

func TestBoundedConcurrencyWrapper_Exits_Early_If_Context_Error(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	// concurrency set to zero to allow zero calls to go through
	dut := NewBoundedConcurrencyTupleReader(mockDatastore, 0)

	var testCases = map[string]struct {
		requestFunc func(ctx context.Context) (any, error)
	}{
		`read`: {
			requestFunc: func(ctx context.Context) (any, error) {
				return dut.Read(ctx, ulid.Make().String(), &openfgav1.TupleKey{}, storage.QueryOptions{Consistency: openfgav1.ConsistencyPreference_UNSPECIFIED})
			},
		},
		`read_user_tuple`: {
			requestFunc: func(ctx context.Context) (any, error) {
				return dut.ReadUserTuple(ctx, ulid.Make().String(), &openfgav1.TupleKey{}, storage.QueryOptions{Consistency: openfgav1.ConsistencyPreference_UNSPECIFIED})
			},
		},
		`read_userset_tuples`: {
			requestFunc: func(ctx context.Context) (any, error) {
				return dut.ReadUsersetTuples(ctx, ulid.Make().String(), storage.ReadUsersetTuplesFilter{}, storage.QueryOptions{Consistency: openfgav1.ConsistencyPreference_UNSPECIFIED})
			},
		},
		`read_starting_with_user`: {
			requestFunc: func(ctx context.Context) (any, error) {
				return dut.ReadStartingWithUser(ctx, ulid.Make().String(), storage.ReadStartingWithUserFilter{}, storage.QueryOptions{Consistency: openfgav1.ConsistencyPreference_UNSPECIFIED})
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
