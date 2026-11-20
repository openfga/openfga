package storagewrappers

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
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

// fakeQuerier is an adapter.Querier that returns preconfigured results.
type fakeQuerier struct {
	rows adapter.Rows
	err  error
}

func (q *fakeQuerier) Execute(context.Context, *query.Statement) (adapter.Rows, error) {
	return q.rows, q.err
}

// fakeRows is an adapter.Rows whose Next reports true `remaining` times.
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
		br := &boundedRows{Rows: &fakeRows{remaining: 1}, release: func() { released++ }, counter: &atomic.Uint64{}}

		require.True(t, br.Next()) // first row
		require.Equal(t, 0, released)
		require.False(t, br.Next()) // exhausted
		require.Equal(t, 1, released)
		require.False(t, br.Next()) // still exhausted, release not repeated
		require.Equal(t, 1, released)
	})

	t.Run("drain then Close releases exactly once", func(t *testing.T) {
		var released int
		br := &boundedRows{Rows: &fakeRows{}, release: func() { released++ }, counter: &atomic.Uint64{}}

		require.False(t, br.Next()) // exhausted -> release
		require.NoError(t, br.Close())
		require.Equal(t, 1, released)
	})
}

func TestBoundedQuerierExecuteCountsItems(t *testing.T) {
	b := NewBoundedTupleReader(memory.New(), &Operation{Method: apimethod.Check, Concurrency: 1})
	q := &boundedQuerier{b, &fakeQuerier{rows: &fakeRows{remaining: 3}}}

	rows, err := q.Execute(context.Background(), &query.Statement{})
	require.NoError(t, err)
	for rows.Next() {
	}
	require.NoError(t, rows.Close())

	require.Equal(t, uint64(3), b.GetMetadata().DatastoreItemCount)
}

// lazyTupleReader mimics the SQL datastores: Read returns at once, and the slow query runs
// inside the first Next or Head. It records how many queries ran at the same time.
// A non-nil pool models a finite connection pool: a query holds a connection until Stop.
type lazyTupleReader struct {
	storage.RelationshipTupleReader
	delay time.Duration
	pool  chan struct{}

	mu          sync.Mutex
	inFlight    int
	maxInFlight int
}

func (r *lazyTupleReader) Read(context.Context, string, storage.ReadFilter, storage.ReadOptions) (storage.TupleIterator, error) {
	return &lazyTupleIterator{
		TupleIterator: storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("obj:1", "viewer", "user:1")},
		}),
		reader: r,
	}, nil
}

func (r *lazyTupleReader) query() {
	r.mu.Lock()
	r.inFlight++
	r.maxInFlight = max(r.maxInFlight, r.inFlight)
	r.mu.Unlock()

	time.Sleep(r.delay)

	r.mu.Lock()
	r.inFlight--
	r.mu.Unlock()
}

type lazyTupleIterator struct {
	storage.TupleIterator
	reader  *lazyTupleReader
	queried bool
	conn    bool
}

func (itr *lazyTupleIterator) run(ctx context.Context) error {
	if itr.queried {
		return nil
	}
	if itr.reader.pool != nil {
		select {
		case itr.reader.pool <- struct{}{}:
			itr.conn = true
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	itr.queried = true
	itr.reader.query()
	return nil
}

func (itr *lazyTupleIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	if err := itr.run(ctx); err != nil {
		return nil, err
	}
	return itr.TupleIterator.Next(ctx)
}

func (itr *lazyTupleIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	if err := itr.run(ctx); err != nil {
		return nil, err
	}
	return itr.TupleIterator.Head(ctx)
}

func (itr *lazyTupleIterator) Stop() {
	if itr.conn {
		<-itr.reader.pool
		itr.conn = false
	}
	itr.TupleIterator.Stop()
}

func TestBoundedWrapper_LazyIterator(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	store := ulid.Make().String()

	t.Run("next_and_head_are_bounded", func(t *testing.T) {
		backend := &lazyTupleReader{delay: 20 * time.Millisecond}
		dut := NewBoundedTupleReader(backend, &Operation{Method: apimethod.Check, Concurrency: 1})

		var wg errgroup.Group
		for range 4 {
			wg.Go(func() error {
				itr, err := dut.Read(context.Background(), store, storage.ReadFilter{}, storage.ReadOptions{})
				if err != nil {
					return err
				}
				defer itr.Stop()
				if _, err := itr.Head(context.Background()); err != nil {
					return err
				}
				_, err = itr.Next(context.Background())
				return err
			})
		}
		require.NoError(t, wg.Wait())

		require.Equal(t, 1, backend.maxInFlight)
		require.Equal(t, uint32(4), dut.GetMetadata().DatastoreQueryCount)
		require.Equal(t, uint64(4), dut.GetMetadata().DatastoreItemCount)
	})

	t.Run("nested_read_with_open_iterator_does_not_deadlock", func(t *testing.T) {
		backend := &lazyTupleReader{delay: time.Millisecond}
		dut := NewBoundedTupleReader(backend, &Operation{Method: apimethod.Check, Concurrency: 1})

		// A deadlock shows up as a deadline error instead of a hung test.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		outer, err := dut.Read(ctx, store, storage.ReadFilter{}, storage.ReadOptions{})
		require.NoError(t, err)
		defer outer.Stop()
		_, err = outer.Next(ctx)
		require.NoError(t, err)

		inner, err := dut.Read(ctx, store, storage.ReadFilter{}, storage.ReadOptions{})
		require.NoError(t, err)
		_, err = inner.Next(ctx)
		require.NoError(t, err)
		inner.Stop()

		_, err = outer.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
	})

	t.Run("open_iterator_drains_while_other_read_waits_for_pool", func(t *testing.T) {
		backend := &lazyTupleReader{pool: make(chan struct{}, 1)}
		dut := NewBoundedTupleReader(backend, &Operation{Method: apimethod.Check, Concurrency: 1})

		// A deadlock shows up as a deadline error instead of a hung test.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		a, err := dut.Read(ctx, store, storage.ReadFilter{}, storage.ReadOptions{})
		require.NoError(t, err)
		_, err = a.Next(ctx) // a now holds the only pool connection
		require.NoError(t, err)

		var wg errgroup.Group
		wg.Go(func() error {
			b, err := dut.Read(ctx, store, storage.ReadFilter{}, storage.ReadOptions{})
			if err != nil {
				return err
			}
			defer b.Stop()
			_, err = b.Next(ctx)
			return err
		})
		// b holds the only slot while it waits for a's connection.
		require.Eventually(t, func() bool { return len(dut.limiter) == 1 }, time.Second, time.Millisecond)

		_, err = a.Next(ctx)
		require.ErrorIs(t, err, storage.ErrIteratorDone)
		a.Stop()
		require.NoError(t, wg.Wait())
	})

	t.Run("retry_after_failed_first_call_is_bounded", func(t *testing.T) {
		backend := &lazyTupleReader{pool: make(chan struct{}, 1)}
		dut := NewBoundedTupleReader(backend, &Operation{Method: apimethod.Check, Concurrency: 1})

		itr, err := dut.Read(context.Background(), store, storage.ReadFilter{}, storage.ReadOptions{})
		require.NoError(t, err)
		defer itr.Stop()

		backend.pool <- struct{}{} // exhaust the pool so the first call fails before opening rows
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		_, err = itr.Next(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		<-backend.pool

		require.NoError(t, dut.waitForLimiter(context.Background())) // occupy the only slot
		defer dut.done()

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		_, err = itr.Next(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 0, backend.maxInFlight)
	})

	t.Run("ctx_cancelled_while_waiting_for_slot", func(t *testing.T) {
		backend := &lazyTupleReader{}
		dut := NewBoundedTupleReader(backend, &Operation{Method: apimethod.Check, Concurrency: 1})

		itr, err := dut.Read(context.Background(), store, storage.ReadFilter{}, storage.ReadOptions{})
		require.NoError(t, err)
		defer itr.Stop()

		require.NoError(t, dut.waitForLimiter(context.Background())) // occupy the only slot
		defer dut.done()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = itr.Next(ctx)
		require.ErrorIs(t, err, context.Canceled)
		_, err = itr.Head(ctx)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 0, backend.maxInFlight)
	})
}
