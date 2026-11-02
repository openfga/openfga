package storagewrappers

import (
	"context"
	"sync"
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
