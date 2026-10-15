package storagewrappers

import (
	"context"
	"errors"
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
	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/query"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestCountingTupleIteratorIsOrdered(t *testing.T) {
	inner := storage.NewStaticTupleIterator([]*openfgav1.Tuple{})
	iter := &countingTupleIterator{TupleIterator: inner}
	defer iter.Stop()
	require.True(t, iter.IsOrdered())
}

func TestBoundedWrapper(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	store := ulid.Make().String()
	slowBackend := mocks.NewMockSlowDataStorage(memory.New(), time.Second)

	err := slowBackend.Write(context.Background(), store, []*openfgav1.TupleKeyWithoutCondition{}, []*openfgav1.TupleKey{
		tuple.NewTupleKey("obj:1", "viewer", "group:1#member"),
	})
	require.NoError(t, err)

	t.Run("normal case", func(t *testing.T) {
		// Create a limited tuple reader that allows 1 concurrent read a time.
		limitedTupleReader := NewBoundedTupleReader(slowBackend, &Operation{Method: apimethod.Check, Concurrency: 1, ThrottleThreshold: 2, ThrottleDuration: 500 * time.Millisecond, ThrottlingEnabled: true})

		// Do reads from 4 goroutines - each should be run serially. Should be >4 seconds.
		const numRoutine = 4

		var wg errgroup.Group

		start := time.Now()

		ctx := context.Background()

		wg.Go(func() error {
			_, err := limitedTupleReader.ReadUserTuple(context.Background(), store, storage.ReadUserTupleFilter{Object: "obj:1", Relation: "viewer", User: "group:1#member"}, storage.ReadUserTupleOptions{})
			return err
		})

		wg.Go(func() error {
			itr, err := limitedTupleReader.ReadUsersetTuples(context.Background(), store, storage.ReadUsersetTuplesFilter{
				Object:   "obj:1",
				Relation: "viewer",
			}, storage.ReadUsersetTuplesOptions{})
			if err != nil {
				return err
			}
			_, err = itr.Next(ctx)
			return err
		})

		wg.Go(func() error {
			itr, err := limitedTupleReader.Read(context.Background(), store, storage.ReadFilter{}, storage.ReadOptions{})
			if err != nil {
				return err
			}
			_, err = itr.Next(ctx)
			return err
		})

		wg.Go(func() error {
			itr, err := limitedTupleReader.ReadStartingWithUser(
				context.Background(),
				store,
				storage.ReadStartingWithUserFilter{
					ObjectType: "obj",
					Relation:   "viewer",
					UserFilter: []*openfgav1.ObjectRelation{
						{
							Object:   "group:1",
							Relation: "member",
						},
					}}, storage.ReadStartingWithUserOptions{})
			if err != nil {
				return err
			}
			_, err = itr.Next(ctx)
			return err
		})

		err = wg.Wait()
		require.NoError(t, err)

		end := time.Now()

		require.GreaterOrEqual(t, end.Sub(start), numRoutine*time.Second+1) // 2 throttles should add a full second
		require.Equal(t, uint32(4), limitedTupleReader.GetMetadata().DatastoreQueryCount)
		require.Equal(t, uint64(4), limitedTupleReader.GetMetadata().DatastoreItemCount)
		require.True(t, limitedTupleReader.GetMetadata().WasThrottled)
	})

	t.Run("ctx cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		// Create a limited tuple reader that allows 1 concurrent read a time.
		limitedTupleReader := NewBoundedTupleReader(slowBackend, &Operation{Method: apimethod.Check, Concurrency: 1, ThrottleThreshold: 2, ThrottleDuration: 500 * time.Millisecond, ThrottlingEnabled: true})

		var wg errgroup.Group

		wg.Go(func() error {
			_, err := limitedTupleReader.ReadUserTuple(ctx, store, storage.ReadUserTupleFilter{Object: "obj:1", Relation: "viewer", User: "group:1#member"}, storage.ReadUserTupleOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, storage.ReadFilter{}, storage.ReadOptions{})
			return err
		})

		err = wg.Wait()
		require.NoError(t, err)

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, storage.ReadFilter{}, storage.ReadOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, storage.ReadFilter{}, storage.ReadOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, storage.ReadFilter{}, storage.ReadOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, storage.ReadFilter{}, storage.ReadOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, storage.ReadFilter{}, storage.ReadOptions{})
			return err
		})

		wg.Go(func() error {
			_, err := limitedTupleReader.Read(ctx, store, storage.ReadFilter{}, storage.ReadOptions{})
			return err
		})

		// trigger cancellation
		cancel()
		err = wg.Wait()
		require.ErrorIs(t, err, context.Canceled)
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
				return dut.Read(ctx, ulid.Make().String(), storage.ReadFilter{}, storage.ReadOptions{})
			},
		},
		`read_user_tuple`: {
			requestFunc: func(ctx context.Context) (any, error) {
				return dut.ReadUserTuple(ctx, ulid.Make().String(), storage.ReadUserTupleFilter{}, storage.ReadUserTupleOptions{})
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

// fakeQuerier is a stub adapter.Querier that returns preconfigured results.
type fakeQuerier struct {
	rows adapter.Rows
	err  error
}

func (q *fakeQuerier) Execute(context.Context, *query.Statement) (adapter.Rows, error) {
	return q.rows, q.err
}

// fakeRows is a minimal adapter.Rows whose Next reports true `remaining` times.
type fakeRows struct {
	remaining int
	closed    bool
}

func (r *fakeRows) Next() bool {
	if r.remaining <= 0 {
		return false
	}
	r.remaining--
	return true
}
func (r *fakeRows) Scan(...any) error { return nil }
func (r *fakeRows) Close() error      { r.closed = true; return nil }
func (r *fakeRows) Err() error        { return nil }

func TestBoundedTupleReaderQuerier(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	t.Run("returns nil when the delegate returns nil", func(t *testing.T) {
		ds := mocks.NewMockOpenFGADatastore(ctrl)
		ds.EXPECT().Querier(openfgav1.ConsistencyPreference_UNSPECIFIED).Return(nil)
		b := NewBoundedTupleReader(ds, &Operation{Method: apimethod.Check, Concurrency: 1})
		require.Nil(t, b.Querier(openfgav1.ConsistencyPreference_UNSPECIFIED))
	})

	t.Run("wraps a non-nil delegate Querier", func(t *testing.T) {
		ds := mocks.NewMockOpenFGADatastore(ctrl)
		inner := &fakeQuerier{}
		ds.EXPECT().Querier(openfgav1.ConsistencyPreference_UNSPECIFIED).Return(inner)
		b := NewBoundedTupleReader(ds, &Operation{Method: apimethod.Check, Concurrency: 1})

		got := b.Querier(openfgav1.ConsistencyPreference_UNSPECIFIED)
		bq, ok := got.(*boundedQuerier)
		require.True(t, ok)
		require.Same(t, inner, bq.Querier)
		require.Same(t, b, bq.BoundedTupleReader)
	})
}

func TestBoundedQuerierExecute(t *testing.T) {
	ctx := context.Background()

	t.Run("streaming rows hold the slot until Close", func(t *testing.T) {
		rows := &fakeRows{remaining: 1}
		b := NewBoundedTupleReader(memory.New(), &Operation{Method: apimethod.Check, Concurrency: 1})
		q := &boundedQuerier{b, &fakeQuerier{rows: rows}}

		got, err := q.Execute(ctx, &query.Statement{})
		require.NoError(t, err)
		require.Len(t, b.limiter, 1, "slot held after Execute returns")

		br, ok := got.(*boundedRows)
		require.True(t, ok, "streaming rows are wrapped in boundedRows")

		require.NoError(t, br.Close())
		require.Empty(t, b.limiter, "slot released on Close")
		require.True(t, rows.closed, "underlying rows closed")
	})

	t.Run("delegate error releases the slot immediately", func(t *testing.T) {
		wantErr := errors.New("boom")
		b := NewBoundedTupleReader(memory.New(), &Operation{Method: apimethod.Check, Concurrency: 1})
		q := &boundedQuerier{b, &fakeQuerier{err: wantErr}}

		got, err := q.Execute(ctx, &query.Statement{})
		require.ErrorIs(t, err, wantErr)
		require.Nil(t, got)
		require.Empty(t, b.limiter, "slot released on error")
	})

	t.Run("cancelled context returns error and holds no slot", func(t *testing.T) {
		// Concurrency 0 makes the limiter send block, so only ctx.Done() is ready.
		b := NewBoundedTupleReader(memory.New(), &Operation{Method: apimethod.Check, Concurrency: 0})
		q := &boundedQuerier{b, &fakeQuerier{rows: &fakeRows{}}}
		cctx, cancel := context.WithCancel(ctx)
		cancel()

		got, err := q.Execute(cctx, &query.Statement{})
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, got)
		require.Empty(t, b.limiter)
	})
}

func TestBoundedRows(t *testing.T) {
	t.Run("Close releases once and closes the underlying rows", func(t *testing.T) {
		var released int
		inner := &fakeRows{}
		br := &boundedRows{Rows: inner, release: func() { released++ }}

		require.NoError(t, br.Close())
		require.True(t, inner.closed)
		require.Equal(t, 1, released)

		require.NoError(t, br.Close()) // release is idempotent
		require.Equal(t, 1, released)
	})

	t.Run("Next releases on exhaustion", func(t *testing.T) {
		var released int
		br := &boundedRows{Rows: &fakeRows{remaining: 1}, release: func() { released++ }}

		require.True(t, br.Next()) // first row
		require.Equal(t, 0, released)
		require.False(t, br.Next()) // exhausted
		require.Equal(t, 1, released)
		require.False(t, br.Next()) // still exhausted, release not repeated
		require.Equal(t, 1, released)
	})

	t.Run("drain then Close releases exactly once", func(t *testing.T) {
		var released int
		br := &boundedRows{Rows: &fakeRows{}, release: func() { released++ }}

		require.False(t, br.Next()) // exhausted -> release
		require.NoError(t, br.Close())
		require.Equal(t, 1, released)
	})
}
