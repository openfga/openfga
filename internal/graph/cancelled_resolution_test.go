package graph

import (
	"context"
	"sync"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// cancellationTrials is how many times the reducer tests below repeat a trial in
// which a worker finishes as its context is cancelled.
const cancellationTrials = 64

// lateDoneContext is a context whose cancellation reaches the contexts derived
// from it (through AfterFunc) while its own Done channel stays open.
//
// A reducer or resolver derives its workers' context from the context it receives
// and also selects on that context's Done channel. Cancelling a lateDoneContext
// cancels the workers while the outer select can only leave through the worker
// channels, fixing the order of events that otherwise depends on goroutine
// scheduling.
type lateDoneContext struct {
	context.Context
	done chan struct{}

	mu         sync.Mutex
	err        error
	afterFuncs []func()
}

func newLateDoneContext() *lateDoneContext {
	return &lateDoneContext{Context: context.Background(), done: make(chan struct{})}
}

func (c *lateDoneContext) Done() <-chan struct{} { return c.done }

func (c *lateDoneContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// AfterFunc is what context.WithCancel uses to propagate this context's
// cancellation to the contexts derived from it.
func (c *lateDoneContext) AfterFunc(f func()) func() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.afterFuncs = append(c.afterFuncs, f)
	return func() bool { return false }
}

// cancelDerived cancels every context derived from c while leaving c.Done open.
func (c *lateDoneContext) cancelDerived() {
	c.mu.Lock()
	c.err = context.Canceled
	fs := c.afterFuncs
	c.afterFuncs = nil
	c.mu.Unlock()
	for _, f := range fs {
		f()
	}
}

func TestUnionReportsCancellationWhenAnOperandIsCancelled(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	for trial := 0; trial < cancellationTrials; trial++ {
		ctx := newLateDoneContext()

		// The only operand is cancelled while it runs, so the union never learns
		// whether it was true.
		interrupted := func(handlerCtx context.Context) (*ResolveCheckResponse, error) {
			ctx.cancelDerived()
			return nil, handlerCtx.Err()
		}

		resp, err := union(ctx, 1, interrupted)
		require.ErrorIs(t, err, context.Canceled,
			"trial %d: union answered %+v without a valid outcome from its operand", trial, resp)
		require.Nil(t, resp)
	}
}

func TestIntersectionReportsCancellationWhenAnOperandIsCancelled(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	for trial := 0; trial < cancellationTrials; trial++ {
		ctx := newLateDoneContext()

		// With a concurrency limit of 1 the first operand reports true before the
		// second starts. The second is cancelled while it runs, so the intersection
		// never learns whether it was false.
		interrupted := func(handlerCtx context.Context) (*ResolveCheckResponse, error) {
			ctx.cancelDerived()
			return nil, handlerCtx.Err()
		}

		resp, err := intersection(ctx, 1, trueHandler, interrupted)
		require.ErrorIs(t, err, context.Canceled,
			"trial %d: intersection answered %+v without an outcome from its second operand", trial, resp)
		require.Nil(t, resp)
	}
}

func TestExclusionReportsCancellationWhenAnOperandIsCancelled(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("subtract_outcome_cancelled", func(t *testing.T) {
		for trial := 0; trial < cancellationTrials; trial++ {
			ctx := newLateDoneContext()
			baseDone := make(chan struct{})

			base := func(context.Context) (*ResolveCheckResponse, error) {
				defer close(baseDone)
				return &ResolveCheckResponse{Allowed: true}, nil
			}
			// Once base is true, subtract is cancelled while it runs, so the
			// exclusion never learns whether subtract would have revoked access.
			interrupted := func(handlerCtx context.Context) (*ResolveCheckResponse, error) {
				<-baseDone
				ctx.cancelDerived()
				return nil, handlerCtx.Err()
			}

			resp, err := exclusion(ctx, 1, base, interrupted)
			require.ErrorIs(t, err, context.Canceled,
				"trial %d: exclusion answered %+v without an outcome from its subtract operand", trial, resp)
			require.Nil(t, resp)
		}
	})

	t.Run("base_outcome_cancelled", func(t *testing.T) {
		for trial := 0; trial < cancellationTrials; trial++ {
			ctx := newLateDoneContext()
			subDone := make(chan struct{})

			// Once subtract is false, base is cancelled while it runs, so the
			// exclusion never learns whether base was true.
			interrupted := func(handlerCtx context.Context) (*ResolveCheckResponse, error) {
				<-subDone
				ctx.cancelDerived()
				return nil, handlerCtx.Err()
			}
			sub := func(context.Context) (*ResolveCheckResponse, error) {
				defer close(subDone)
				return &ResolveCheckResponse{Allowed: false}, nil
			}

			resp, err := exclusion(ctx, 1, interrupted, sub)
			require.ErrorIs(t, err, context.Canceled,
				"trial %d: exclusion answered %+v without an outcome from its base operand", trial, resp)
			require.Nil(t, resp)
		}
	})
}

// cancellingStringIterator yields items in order, then cancels ctx and reports the
// cancellation error before any remaining items can be read.
type cancellingStringIterator struct {
	items  []string
	cancel func()
}

func (it *cancellingStringIterator) Next(ctx context.Context) (string, error) {
	if len(it.items) > 0 {
		next := it.items[0]
		it.items = it.items[1:]
		return next, nil
	}
	it.cancel()
	return "", ctx.Err()
}

func (it *cancellingStringIterator) Head(ctx context.Context) (string, error) {
	if len(it.items) > 0 {
		return it.items[0], nil
	}
	it.cancel()
	return "", ctx.Err()
}

func (it *cancellingStringIterator) Stop() {}

func (it *cancellingStringIterator) IsOrdered() bool { return true }

func TestWeight2ReportsCancellationWhenCancelledAfterNonMatchingItems(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("left_iterator_cancelled_after_non_matching_item", func(t *testing.T) {
		ctx := newLateDoneContext()

		// Right yields one non-matching userset ("group:2") and closes. Left yields one
		// non-matching userset ("group:1") so leftSet is non-empty, then is cancelled
		// before it can yield "group:2".
		leftChan := make(chan *iterator.Msg, 1)
		leftChan <- &iterator.Msg{
			Iter: &cancellingStringIterator{
				items:  []string{"group:1"},
				cancel: ctx.cancelDerived,
			},
		}
		close(leftChan)

		rightIter := storage.WrapIterator(storage.UsersetKind, storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "group:2#member"),
		}))

		resp, err := NewLocalChecker().weight2(ctx, []<-chan *iterator.Msg{leftChan}, rightIter)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, resp)
	})

	t.Run("fast_path_difference_subtract_stream_cancelled_mid_stream", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		// Base has {"group:1", "group:2"}. Subtract has {"group:1"} and is then
		// cancelled before it can yield "group:2". fastPathDifference must report the
		// cancellation error rather than treating the subtract stream as exhausted and
		// draining "group:2" from base.
		baseProducer := make(chan *iterator.Msg, 1)
		baseProducer <- &iterator.Msg{
			Iter: storage.NewStaticIterator[string]([]string{"group:1", "group:2"}),
		}
		close(baseProducer)

		diffProducer := make(chan *iterator.Msg, 1)
		diffProducer <- &iterator.Msg{
			Iter: &cancellingStringIterator{
				items:  []string{"group:1"},
				cancel: cancel,
			},
		}
		close(diffProducer)

		streams := iterator.NewStreams([]*iterator.Stream{
			iterator.NewStream(BaseIndex, baseProducer),
			iterator.NewStream(DifferenceIndex, diffProducer),
		})
		leftChan := make(chan *iterator.Msg, 2)
		go fastPathDifference(ctx, streams, leftChan)

		rightIter := storage.WrapIterator(storage.UsersetKind, storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "group:2#member"),
		}))

		resp, err := NewLocalChecker().weight2(ctx, []<-chan *iterator.Msg{leftChan}, rightIter)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, resp)
	})

	t.Run("fast_path_difference_nested_subtract_producer_closed_on_cancellation", func(t *testing.T) {
		ctx := newLateDoneContext()

		// Base has {"group:1", "group:2"}. Subtract is a nested fastPathUnion that
		// yields {"group:1"} in its first batch and is then cancelled before its
		// second batch {"group:2"} can be read, causing fastPathUnion to close its
		// output channel. fastPathDifference must report context.Canceled rather
		// than treating the closed subtract channel as normal exhaustion and
		// draining "group:2" from base.
		baseProducer := make(chan *iterator.Msg, 1)
		baseProducer <- &iterator.Msg{
			Iter: storage.NewStaticIterator[string]([]string{"group:1", "group:2"}),
		}
		close(baseProducer)

		nestedUnionInput := make(chan *iterator.Msg, 2)
		nestedUnionInput <- &iterator.Msg{
			Iter: storage.NewStaticIterator[string]([]string{"group:1"}),
		}
		nestedUnionInput <- &iterator.Msg{
			Iter: &cancellingStringIterator{
				items:  nil,
				cancel: ctx.cancelDerived,
			},
		}
		close(nestedUnionInput)

		diffProducer := make(chan *iterator.Msg, 1)
		go fastPathUnion(ctx, iterator.NewStreams([]*iterator.Stream{
			iterator.NewStream(0, nestedUnionInput),
		}), diffProducer)

		streams := iterator.NewStreams([]*iterator.Stream{
			iterator.NewStream(BaseIndex, baseProducer),
			iterator.NewStream(DifferenceIndex, diffProducer),
		})
		leftChan := make(chan *iterator.Msg, 2)
		go fastPathDifference(ctx, streams, leftChan)

		rightIter := storage.WrapIterator(storage.UsersetKind, storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "group:2#member"),
		}))

		resp, err := NewLocalChecker().weight2(ctx, []<-chan *iterator.Msg{leftChan}, rightIter)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, resp)
	})

	t.Run("fast_path_set_operators_do_not_flush_partial_batch_on_cancellation", func(t *testing.T) {
		ctx := newLateDoneContext()

		// Producer yields "group:1" (accumulated in batch < IteratorMinBatchThreshold)
		// and then cancels on the next item. fastPathUnion must only send the
		// cancellation error and must not flush the partial batch in defer.
		producer := make(chan *iterator.Msg, 1)
		producer <- &iterator.Msg{
			Iter: &cancellingStringIterator{
				items:  []string{"group:1"},
				cancel: ctx.cancelDerived,
			},
		}
		close(producer)

		outChan := make(chan *iterator.Msg, 2)
		fastPathUnion(ctx, iterator.NewStreams([]*iterator.Stream{
			iterator.NewStream(0, producer),
		}), outChan)

		var gotErr error
		var gotIter bool
		for msg := range outChan {
			if msg.Err != nil {
				gotErr = msg.Err
			}
			if msg.Iter != nil {
				gotIter = true
				msg.Iter.Stop()
			}
		}
		require.ErrorIs(t, gotErr, context.Canceled)
		require.False(t, gotIter, "partial batch should not be flushed after cancellation")
	})
}

type staticObjectProvider struct {
	ch <-chan usersetMessage
}

func (p *staticObjectProvider) End() {}

func (p *staticObjectProvider) Begin(context.Context, *ResolveCheckRequest) (<-chan usersetMessage, error) {
	return p.ch, nil
}

func TestRecursiveFastPathReportsCancellationWhenChannelClosesOnCancellation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := newLateDoneContext()

	userChan := make(chan usersetMessage, 1)
	userChan <- usersetMessage{userset: "group:1"}
	close(userChan)
	ctx.cancelDerived()

	iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{
		tuple.NewTupleKey("group:2", "member", "group:3#member"),
	})

	resp, err := NewLocalChecker().recursiveFastPath(
		ctx,
		&ResolveCheckRequest{
			StoreID:         ulid.Make().String(),
			TupleKey:        tuple.NewTupleKey("group:2", "member", "user:maria"),
			RequestMetadata: NewCheckRequestMetadata(),
		},
		iter,
		&recursiveMapping{kind: storage.UsersetKind},
		&staticObjectProvider{ch: userChan},
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, resp)
}

// cancellingOnRelationTupleReader cancels the check's derived contexts when reading
// a specific relation and returns the resulting context cancellation error.
type cancellingOnRelationTupleReader struct {
	storage.RelationshipTupleReader
	relation string
	cancel   func()
}

func (r *cancellingOnRelationTupleReader) ReadUserTuple(
	ctx context.Context,
	store string,
	filter storage.ReadUserTupleFilter,
	options storage.ReadUserTupleOptions,
) (*openfgav1.Tuple, error) {
	if filter.Relation == r.relation {
		r.cancel()
		return nil, context.Canceled
	}
	return r.RelationshipTupleReader.ReadUserTuple(ctx, store, filter, options)
}

func TestCachedCheckResolverDoesNotCacheResolutionWithCancelledContext(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ds := memory.New()
	t.Cleanup(ds.Close)

	storeID := ulid.Make().String()
	require.NoError(t, ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:budget", "viewer", "user:maria"),
	}))

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1

		type user

		type document
			relations
				define viewer: [user]
				define approved: [user]
				define can_read: viewer and approved
	`)
	ts, err := typesystem.New(model)
	require.NoError(t, err)

	cachedResolver, err := NewCachedCheckResolver()
	require.NoError(t, err)
	t.Cleanup(cachedResolver.Close)

	localChecker := NewLocalChecker(WithResolveNodeBreadthLimit(1))
	cachedResolver.SetDelegate(localChecker)
	localChecker.SetDelegate(cachedResolver)

	req, err := NewResolveCheckRequest(ResolveCheckRequestParams{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("document:budget", "can_read", "user:maria"),
	})
	require.NoError(t, err)

	// On the first check, viewer is true and approved is cancelled mid-flight.
	// LocalChecker propagates context.Canceled rather than returning a partial
	// {Allowed: true}, so CachedCheckResolver does not cache a false grant.
	cancelledCtx := newLateDoneContext()
	firstReqCtx := setRequestContext(
		cancelledCtx,
		ts,
		&cancellingOnRelationTupleReader{
			RelationshipTupleReader: ds,
			relation:                "approved",
			cancel:                  cancelledCtx.cancelDerived,
		},
		nil,
	)
	_, cancelledErr := cachedResolver.ResolveCheck(firstReqCtx, req.clone())
	require.ErrorIs(t, cancelledErr, context.Canceled)

	// A subsequent check for the same tuple evaluates cleanly and returns false.
	secondReqCtx := setRequestContext(context.Background(), ts, ds, nil)
	resp, err := cachedResolver.ResolveCheck(secondReqCtx, req.clone())
	require.NoError(t, err)
	require.False(t, resp.GetAllowed(), "the cache served a partial result from the cancelled check")
}
