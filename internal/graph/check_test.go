package graph

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

var (
	falseHandler = func(context.Context) (*ResolveCheckResponse, error) {
		return &ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				DatastoreQueryCount: 1,
			},
		}, nil
	}

	trueHandler = func(context.Context) (*ResolveCheckResponse, error) {
		return &ResolveCheckResponse{
			Allowed: true,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				DatastoreQueryCount: 1,
			},
		}, nil
	}

	depthExceededHandler = func(context.Context) (*ResolveCheckResponse, error) {
		return &ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				DatastoreQueryCount: 2,
			},
		}, ErrResolutionDepthExceeded
	}

	cyclicErrorHandler = func(context.Context) (*ResolveCheckResponse, error) {
		return &ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				DatastoreQueryCount: 2,
			},
		}, ErrCycleDetected
	}
)

func TestExclusionCheckFuncReducer(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	concurrencyLimit := uint32(10)

	t.Run("requires_exactly_two_handlers", func(t *testing.T) {
		require.Panics(t, func() {
			_, _ = exclusion(ctx, concurrencyLimit)
		})

		require.Panics(t, func() {
			_, _ = exclusion(ctx, concurrencyLimit, falseHandler)
		})

		require.Panics(t, func() {
			_, _ = exclusion(ctx, concurrencyLimit, falseHandler, falseHandler, falseHandler)
		})

		require.NotPanics(t, func() {
			_, _ = exclusion(ctx, concurrencyLimit, falseHandler, falseHandler)
		})
	})

	t.Run("base_handler_is_falsy_return_allowed:false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(1+1))
	})

	t.Run("base_handler_is_falsy_subtract_handler_with_error_return_allowed:false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, depthExceededHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(1+2))
	})

	t.Run("base_handler_with_error_subtract_handler_is_truthy_return_allowed:false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, depthExceededHandler, trueHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(2+1))
	})

	t.Run("base_handler_with_error_subtract_handler_with_error_return_error", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, depthExceededHandler, depthExceededHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.False(t, resp.GetAllowed())
		require.Equal(t, uint32(0), resp.GetResolutionMetadata().DatastoreQueryCount)
	})

	t.Run("base_handler_with_cycle_subtract_handler_is_falsy_return_allowed:false_with_a_nil_error", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, cyclicErrorHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(2+1))
	})

	t.Run("base_handler_with_cycle_subtract_handler_is_truthy_return_allowed:false_with_a_nil_error", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, cyclicErrorHandler, trueHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(2+1))
	})

	t.Run("base_handler_is_falsy_subtract_handler_with_cycle_return_allowed:false_with_a_nil_error", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(1+2))
	})

	t.Run("base_handler_is_truthy_subtract_handler_with_cycle_return_allowed:true_with_a_nil_error", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, trueHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(1+2))
	})

	t.Run("base_handler_with_cycle_subtract_handler_with_cycle_return_allowed:false_with_a_nil_error", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, cyclicErrorHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(2+2))
	})

	t.Run("aggregate_truthy_and_falsy_handlers_datastore_query_count", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, trueHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(1+1))
	})

	t.Run("return_allowed:false_if_base_handler_evaluated_before_context_deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		t.Cleanup(cancel)

		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})

	t.Run("return_allowed:false_if_base_handler_evaluated_before_context_cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())

		wg.Wait() // just to make sure to avoid test leaks
	})

	t.Run("return_allowed:false_if_subtract_handler_evaluated_before_context_cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		resp, err := exclusion(ctx, concurrencyLimit, trueHandler, trueHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())

		wg.Wait() // just to make sure to avoid test leaks
	})

	t.Run("return_error_if_context_deadline_before_resolution", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		t.Cleanup(cancel)

		slowHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(50 * time.Millisecond)
			return nil, nil
		}

		resp, err := exclusion(ctx, concurrencyLimit, slowHandler, slowHandler)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, resp.GetAllowed())
	})

	t.Run("return_error_if_context_cancelled_before_resolution", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		slowHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(50 * time.Millisecond)
			return nil, nil
		}

		resp, err := exclusion(ctx, concurrencyLimit, slowHandler, slowHandler)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, resp.GetAllowed())

		wg.Wait() // just to make sure to avoid test leaks
	})
}

func TestIntersectionCheckFuncReducer(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	concurrencyLimit := uint32(10)

	t.Run("no_handlers_return_allowed:false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.NotNil(t, resp.GetResolutionMetadata())
	})

	t.Run("one_handler_is_falsy_return_allowed:false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.Equal(t, uint32(1), resp.GetResolutionMetadata().DatastoreQueryCount)
	})

	t.Run("one_handler_is_truthy_and_others_are_falsy_return_allowed:false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, trueHandler, falseHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(2))
	})

	t.Run("all_handlers_are_falsy_return_allowed:false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, falseHandler, falseHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.Equal(t, uint32(1), resp.GetResolutionMetadata().DatastoreQueryCount)
	})

	t.Run("all_handlers_with_error_return_error", func(t *testing.T) {
		_, err := intersection(ctx, concurrencyLimit, depthExceededHandler, depthExceededHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
	})

	t.Run("one_handler_returns_error_but_other_handler_is_truthy_return_error_and_allowed:false", func(t *testing.T) {
		_, err := intersection(ctx, concurrencyLimit, depthExceededHandler, trueHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
	})

	t.Run("one_handler_returns_error_but_other_handler_is_falsy_return_allowed:false_with_a_nil_error", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, depthExceededHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(1+2))
	})

	t.Run("one_handler_errors_with_cycle_but_other_handler_is_falsy_return_allowed:false_with_a_nil_error", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(1+2))
	})

	t.Run("one_handler_errors_with_cycle_but_other_handler_is_truthy_return_allowed:false_with_a_nil_error", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, trueHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(1))
	})

	t.Run("both_handlers_errors_with_cycle_return_allowed:false_with_a_nil_error", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.Equal(t, uint32(0), resp.GetResolutionMetadata().DatastoreQueryCount)
	})

	t.Run("aggregate_truthy_and_falsy_handlers_datastore_query_count", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, falseHandler, trueHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(1+1))
	})

	t.Run("return_allowed:false_if_falsy_handler_evaluated_before_context_deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		t.Cleanup(cancel)

		resp, err := intersection(ctx, concurrencyLimit, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})

	t.Run("return_allowed:false_if_falsy_handler_evaluated_before_context_cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		resp, err := intersection(ctx, concurrencyLimit, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())

		wg.Wait() // just to make sure to avoid test leaks
	})

	t.Run("return_error_if_context_deadline_before_resolution", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		t.Cleanup(cancel)

		slowHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(50 * time.Millisecond)
			return nil, nil
		}

		resp, err := intersection(ctx, concurrencyLimit, slowHandler)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, resp.GetAllowed())
	})

	t.Run("return_error_if_context_cancelled_before_resolution", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		slowHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(50 * time.Millisecond)
			return nil, nil
		}

		resp, err := intersection(ctx, concurrencyLimit, slowHandler)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, resp.GetAllowed())

		wg.Wait() // just to make sure to avoid test leaks
	})
}

func TestResolveCheckDeterministic(t *testing.T) {
	t.Run("resolution_depth_resolves_deterministically", func(t *testing.T) {
		t.Parallel()

		ds := memory.New()

		storeID := ulid.Make().String()

		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			tuple.NewTupleKey("document:1", "editor", "group:other1#member"),
			tuple.NewTupleKey("document:2", "editor", "group:eng#member"),
			tuple.NewTupleKey("document:2", "allowed", "user:jon"),
			tuple.NewTupleKey("document:2", "allowed", "user:x"),
			tuple.NewTupleKey("group:eng", "member", "group:fga#member"),
			tuple.NewTupleKey("group:eng", "member", "user:jon"),
			tuple.NewTupleKey("group:other1", "member", "group:other2#member"),
		})
		require.NoError(t, err)

		checker := NewLocalCheckerWithCycleDetection()
		t.Cleanup(checker.Close)

		model := testutils.MustTransformDSLToProtoWithID(`model
	schema 1.1
type user

type group
  relations
	define member: [user, group#member]

type document
  relations
	define allowed: [user]
	define viewer: [group#member] or editor
	define editor: [group#member] and allowed`)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			typesystem.New(model),
		)

		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:         storeID,
			TupleKey:        tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			RequestMetadata: NewCheckRequestMetadata(2),
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:         storeID,
			TupleKey:        tuple.NewTupleKey("document:2", "editor", "user:x"),
			RequestMetadata: NewCheckRequestMetadata(2),
		})
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("exclusion_resolves_deterministically_1", func(t *testing.T) {
		t.Parallel()

		ds := memory.New()

		storeID := ulid.Make().String()

		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:budget", "admin", "user:*"),
			tuple.NewTupleKeyWithCondition("document:budget", "viewer", "user:maria", "condX", nil),
		})
		require.NoError(t, err)

		model := testutils.MustTransformDSLToProtoWithID(`model
	schema 1.1
type user

type document
  relations
	define admin: [user:*]
	define viewer: [user with condX] but not admin

condition condX(x: int) {
	x < 100
}
`)

		checker := NewLocalCheckerWithCycleDetection()
		t.Cleanup(checker.Close)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			typesystem.New(model),
		)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		for i := 0; i < 2000; i++ {
			// subtract branch resolves to {allowed: true} even though the base branch
			// results in an error. Outcome should be falsey, not an error.
			resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:         storeID,
				TupleKey:        tuple.NewTupleKey("document:budget", "viewer", "user:maria"),
				RequestMetadata: NewCheckRequestMetadata(defaultResolveNodeLimit),
			})
			require.NoError(t, err)
			require.False(t, resp.GetAllowed())
		}
	})

	t.Run("exclusion_resolves_deterministically_2", func(t *testing.T) {
		t.Parallel()

		ds := memory.New()

		storeID := ulid.Make().String()

		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKeyWithCondition("document:budget", "admin", "user:maria", "condX", nil),
		})
		require.NoError(t, err)

		model := testutils.MustTransformDSLToProtoWithID(`model
			schema 1.1
		type user

		type document
		  relations
			define admin: [user with condX]
			define viewer: [user] but not admin

		condition condX(x: int) {
			x < 100
		}
		`)

		checker := NewLocalCheckerWithCycleDetection()
		t.Cleanup(checker.Close)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			typesystem.New(model),
		)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		for i := 0; i < 2000; i++ {
			// base should resolve to {allowed: false} even though the subtract branch
			// results in an error. Outcome should be falsey, not an error.
			resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:         storeID,
				TupleKey:        tuple.NewTupleKey("document:budget", "viewer", "user:maria"),
				RequestMetadata: NewCheckRequestMetadata(defaultResolveNodeLimit),
			})
			require.NoError(t, err)
			require.False(t, resp.GetAllowed())
		}
	})
}

func TestCheckWithOneConcurrentGoroutineCausesNoDeadlock(t *testing.T) {
	const concurrencyLimit = 1
	ds := memory.New()
	defer ds.Close()

	storeID := ulid.Make().String()

	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "group:1#member"),
		tuple.NewTupleKey("document:1", "viewer", "group:2#member"),
		tuple.NewTupleKey("group:1", "member", "group:1a#member"),
		tuple.NewTupleKey("group:1", "member", "group:1b#member"),
		tuple.NewTupleKey("group:2", "member", "group:2a#member"),
		tuple.NewTupleKey("group:2", "member", "group:2b#member"),
		tuple.NewTupleKey("group:2b", "member", "user:jon"),
	})
	require.NoError(t, err)

	checker := NewLocalCheckerWithCycleDetection(WithResolveNodeBreadthLimit(concurrencyLimit))
	t.Cleanup(checker.Close)

	model := testutils.MustTransformDSLToProtoWithID(`model
	schema 1.1
type user
type group
  relations
	define member: [user, group#member]
type document
  relations
	define viewer: [group#member]`)

	ctx := typesystem.ContextWithTypesystem(
		context.Background(),
		typesystem.New(model),
	)
	ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

	resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:         storeID,
		TupleKey:        tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		RequestMetadata: NewCheckRequestMetadata(25),
	})
	require.NoError(t, err)
	require.True(t, resp.Allowed)
}

func TestCheckDatastoreQueryCount(t *testing.T) {
	ds := memory.New()
	defer ds.Close()

	storeID := ulid.Make().String()

	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:x", "a", "user:jon"),
		tuple.NewTupleKey("document:x", "a", "user:maria"),
		tuple.NewTupleKey("document:x", "b", "user:maria"),
		tuple.NewTupleKey("document:x", "parent", "org:fga"),
		tuple.NewTupleKey("org:fga", "member", "user:maria"),
	})
	require.NoError(t, err)

	model := parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type org
  relations
	define member: [user]

type document
  relations
	define a: [user]
	define b: [user]
	define union: a or b
	define union_rewrite: union
	define intersection: a and b
	define difference: a but not b
	define ttu: member from parent
	define union_and_ttu: union and ttu
	define union_or_ttu: union or ttu or union_rewrite
	define intersection_of_ttus: union_or_ttu and union_and_ttu
	define parent: [org]
`)

	ctx := typesystem.ContextWithTypesystem(
		context.Background(),
		typesystem.New(model),
	)

	tests := []struct {
		name             string
		check            *openfgav1.TupleKey
		contextualTuples []*openfgav1.TupleKey
		allowed          bool
		minDBReads       uint32
		maxDBReads       uint32
	}{
		{
			name:       "no_direct_access",
			check:      tuple.NewTupleKey("document:x", "a", "user:unknown"),
			allowed:    false,
			minDBReads: 1, // both checkDirectUserTuple
			maxDBReads: 1,
		},
		{
			name:       "direct_access",
			check:      tuple.NewTupleKey("document:x", "a", "user:maria"),
			allowed:    true,
			minDBReads: 1, // checkDirectUserTuple needs to run
			maxDBReads: 1,
		},
		{
			name:             "direct_access_thanks_to_contextual_tuple", // NOTE: this is counting the read from memory as a database read!
			check:            tuple.NewTupleKey("document:x", "a", "user:unknown"),
			contextualTuples: []*openfgav1.TupleKey{tuple.NewTupleKey("document:x", "a", "user:unknown")},
			allowed:          true,
			minDBReads:       1, // checkDirectUserTuple needs to run
			maxDBReads:       1,
		},
		{
			name:       "union",
			check:      tuple.NewTupleKey("document:x", "union", "user:maria"),
			allowed:    true,
			minDBReads: 1, // checkDirectUserTuple needs to run
			maxDBReads: 1,
		},
		{
			name:       "union_no_access",
			check:      tuple.NewTupleKey("document:x", "union", "user:unknown"),
			allowed:    false,
			minDBReads: 2, // need to check all the conditions in the union
			maxDBReads: 2,
		},
		{
			name:       "intersection",
			check:      tuple.NewTupleKey("document:x", "intersection", "user:maria"),
			allowed:    true,
			minDBReads: 2, // need at minimum two direct tuple checks
			maxDBReads: 2, // at most two tuple checks
		},
		{
			name:       "intersection_no_access",
			check:      tuple.NewTupleKey("document:x", "intersection", "user:unknown"),
			allowed:    false,
			minDBReads: 1, // need at minimum one direct tuple checks (short circuit the ohter path)
			maxDBReads: 1,
		},
		{
			name:       "difference",
			check:      tuple.NewTupleKey("document:x", "difference", "user:jon"),
			allowed:    true,
			minDBReads: 2, // need at minimum two direct tuple checks
			maxDBReads: 2,
		},
		{
			name:       "difference_no_access",
			check:      tuple.NewTupleKey("document:x", "difference", "user:maria"),
			allowed:    false,
			minDBReads: 1, // if the "but not" condition returns quickly with "false", no need to evaluate the first branch
			maxDBReads: 2, // at most two tuple checks
		},
		{
			name:       "ttu",
			check:      tuple.NewTupleKey("document:x", "ttu", "user:maria"),
			allowed:    true,
			minDBReads: 2, // one read to find org:fga + one direct check if user:maria is a member of org:fga
			maxDBReads: 3, // one read to find org:fga + (one direct check + userset check) if user:maria is a member of org:fga
		},
		{
			name:       "ttu_no_access",
			check:      tuple.NewTupleKey("document:x", "ttu", "user:jon"),
			allowed:    false,
			minDBReads: 2, // one read to find org:fga + (one direct check) to see if user:jon is a member of org:fga
			maxDBReads: 2,
		},
		// more complex scenarios
		{
			name:       "union_and_ttu",
			check:      tuple.NewTupleKey("document:x", "union_and_ttu", "user:maria"),
			allowed:    true,
			minDBReads: 3, // union (1 read) + ttu (2 reads)
			maxDBReads: 5, // union (2 reads) + ttu (3 reads)
		},
		{
			name:       "union_and_ttu_no_access",
			check:      tuple.NewTupleKey("document:x", "union_and_ttu", "user:unknown"),
			allowed:    false,
			minDBReads: 2, // min(union (2 reads), ttu (4 reads))
			maxDBReads: 4, // max(union (2 reads), ttu (4 reads))
		},
		{
			name:       "union_or_ttu",
			check:      tuple.NewTupleKey("document:x", "union_or_ttu", "user:maria"),
			allowed:    true,
			minDBReads: 1, // min(union (1 read), ttu (2 reads))
			maxDBReads: 3, // max(union (2 reads), ttu (3 reads))
		},
		{
			name:       "union_or_ttu_no_access",
			check:      tuple.NewTupleKey("document:x", "union_or_ttu", "user:unknown"),
			allowed:    false,
			minDBReads: 6, // union (2 reads) + ttu (2 reads) + union rewrite (2 reads)
			maxDBReads: 6,
		},
		{
			name:       "intersection_of_ttus", // union_or_ttu and union_and_ttu
			check:      tuple.NewTupleKey("document:x", "intersection_of_ttus", "user:maria"),
			allowed:    true,
			minDBReads: 4, // union_or_ttu (1 read) + union_and_ttu (3 reads)
			maxDBReads: 8, // union_or_ttu (3 reads) + union_and_ttu (5 reads)
		},
	}

	checker := NewLocalCheckerWithCycleDetection(
		WithMaxConcurrentReads(1),
	)
	t.Cleanup(checker.Close)

	// run the test many times to exercise all the possible DBReads
	for i := 1; i < 1000; i++ {
		t.Run(fmt.Sprintf("iteration_%v", i), func(t *testing.T) {
			t.Parallel()
			for _, test := range tests {
				test := test
				t.Run(test.name, func(t *testing.T) {
					t.Parallel()

					ctx := storage.ContextWithRelationshipTupleReader(
						ctx,
						storagewrappers.NewCombinedTupleReader(
							ds,
							test.contextualTuples,
						),
					)

					res, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
						StoreID:          storeID,
						TupleKey:         test.check,
						ContextualTuples: test.contextualTuples,
						RequestMetadata:  NewCheckRequestMetadata(25),
					})
					require.NoError(t, err)
					require.Equal(t, res.Allowed, test.allowed)
					// minDBReads <= dbReads <= maxDBReads
					require.GreaterOrEqual(t, res.ResolutionMetadata.DatastoreQueryCount, test.minDBReads)
					require.LessOrEqual(t, res.ResolutionMetadata.DatastoreQueryCount, test.maxDBReads)
				})
			}
		})
	}
}

// TestCheckWithUnexpectedCycle tests the LocalChecker to make sure that if a model includes a cycle
// that should have otherwise been invalid according to the typesystem, then the check resolution will
// consider the cycle a falsey allowed result and not bubble-up a cycle detected error.
func TestCheckWithUnexpectedCycle(t *testing.T) {
	ds := memory.New()
	defer ds.Close()

	storeID := ulid.Make().String()

	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("resource:1", "parent", "resource:1"),
	})
	require.NoError(t, err)

	tests := []struct {
		name     string
		model    string
		tupleKey *openfgav1.TupleKey
	}{
		{
			name: "test_1",
			model: `model
	schema 1.1
type user

type resource
  relations
	define x: [user] but not y
	define y: [user] but not z
	define z: [user] or x`,
			tupleKey: tuple.NewTupleKey("resource:1", "x", "user:jon"),
		},
		{
			name: "test_2",
			model: `model
	schema 1.1
type user

type resource
  relations
	define x: [user] and y
	define y: [user] and z
	define z: [user] or x`,
			tupleKey: tuple.NewTupleKey("resource:1", "x", "user:jon"),
		},
		{
			name: "test_3",
			model: `model
	schema 1.1
type resource
  relations
	define x: y
	define y: x`,
			tupleKey: tuple.NewTupleKey("resource:1", "x", "user:jon"),
		},
		{
			name: "test_4",
			model: `model
	schema 1.1
type resource
  relations
	define parent: [resource]
	define x: [user] or x from parent`,
			tupleKey: tuple.NewTupleKey("resource:1", "x", "user:jon"),
		},
	}

	checker := NewLocalCheckerWithCycleDetection()
	t.Cleanup(checker.Close)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)

			ctx := typesystem.ContextWithTypesystem(
				context.Background(),
				typesystem.New(model),
			)
			ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

			resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:         storeID,
				TupleKey:        test.tupleKey,
				RequestMetadata: NewCheckRequestMetadata(25),
			})

			require.NoError(t, err)
			require.False(t, resp.GetAllowed())

			require.GreaterOrEqual(t, resp.ResolutionMetadata.DatastoreQueryCount, uint32(0)) // min of 0 (x) if x is cycle. TODO: accurately report datastore query count of cycle branches
			require.LessOrEqual(t, resp.ResolutionMetadata.DatastoreQueryCount, uint32(3))    // max of 3 (x, y, z) before the cycle
		})
	}
}

func TestCheckConditions(t *testing.T) {
	ds := memory.New()

	storeID := ulid.Make().String()

	tkConditionContext, err := structpb.NewStruct(map[string]interface{}{
		"param1": "ok",
	})
	require.NoError(t, err)

	model := parser.MustTransformDSLToProto(`model
  schema 1.1

type user

type folder
  relations
    define viewer: [user]

type group
  relations
    define member: [user, group#member with condition1]

type document
  relations
    define parent: [folder with condition1]
	define viewer: [group#member] or viewer from parent

condition condition1(param1: string) {
  param1 == "ok"
}`)

	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKeyWithCondition("document:x", "parent", "folder:x", "condition1", tkConditionContext),
		tuple.NewTupleKeyWithCondition("document:x", "parent", "folder:y", "condition1", nil),
		tuple.NewTupleKey("folder:y", "viewer", "user:bob"),
		tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
		tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
		tuple.NewTupleKeyWithCondition("group:eng", "member", "group:fga#member", "condition1", nil),
		tuple.NewTupleKey("group:fga", "member", "user:jon"),
	}

	err = ds.Write(context.Background(), storeID, nil, tuples)
	require.NoError(t, err)

	checker := NewLocalCheckerWithCycleDetection()
	t.Cleanup(checker.Close)

	typesys, err := typesystem.NewAndValidate(
		context.Background(),
		model,
	)
	require.NoError(t, err)

	ctx := typesystem.ContextWithTypesystem(context.Background(), typesys)
	ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

	conditionContext, err := structpb.NewStruct(map[string]interface{}{
		"param1": "notok",
	})
	require.NoError(t, err)

	resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("document:x", "parent", "folder:x"),
		RequestMetadata:      NewCheckRequestMetadata(1),
		Context:              conditionContext,
	})
	require.NoError(t, err)
	require.True(t, resp.Allowed)

	resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		RequestMetadata:      NewCheckRequestMetadata(defaultResolveNodeLimit),
		Context:              conditionContext,
	})
	require.NoError(t, err)
	require.False(t, resp.Allowed)

	resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:bob"),
		RequestMetadata:      NewCheckRequestMetadata(defaultResolveNodeLimit),
		Context:              conditionContext,
	})
	require.NoError(t, err)
	require.False(t, resp.Allowed)
}

func TestCheckDispatchCount(t *testing.T) {
	ds := memory.New()
	ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)

	t.Run("dispatch_count_ttu", func(t *testing.T) {
		storeID := ulid.Make().String()

		model := parser.MustTransformDSLToProto(`model
  schema 1.1

type user

type folder
  	relations
		define viewer: [user] or viewer from parent
		define parent: [folder]

type doc
	relations
		define viewer: [user] or viewer from parent
		define parent: [folder]
`)

		err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("folder:C", "viewer", "user:jon"),
			tuple.NewTupleKey("folder:B", "parent", "folder:C"),
			tuple.NewTupleKey("folder:A", "parent", "folder:B"),
			tuple.NewTupleKey("doc:readme", "parent", "folder:A"),
		})
		require.NoError(t, err)

		checker := NewLocalChecker()

		typesys, err := typesystem.NewAndValidate(
			context.Background(),
			model,
		)
		require.NoError(t, err)

		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("doc:readme", "viewer", "user:jon"),
			RequestMetadata:      NewCheckRequestMetadata(5),
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		require.Equal(t, uint32(3), resp.GetResolutionMetadata().DispatchCount)

		t.Run("direct_lookup_requires_no_dispatch", func(t *testing.T) {
			resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: model.GetId(),
				TupleKey:             tuple.NewTupleKey("doc:readme", "parent", "folder:A"),
				RequestMetadata:      NewCheckRequestMetadata(5),
			})
			require.NoError(t, err)
			require.True(t, resp.Allowed)

			require.Zero(t, resp.GetResolutionMetadata().DispatchCount)
		})
	})

	t.Run("dispatch_count_multiple_direct_userset_lookups", func(t *testing.T) {
		storeID := ulid.Make().String()

		model := parser.MustTransformDSLToProto(`model
	  schema 1.1
	
	type user
	
	type group
	  relations
	    define member: [user, group#member]

	type document
	  relations
		define viewer: [group#member]
	`)

		err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("group:1", "member", "user:jon"),
			tuple.NewTupleKey("group:eng", "member", "group:1#member"),
			tuple.NewTupleKey("group:eng", "member", "group:2#member"),
			tuple.NewTupleKey("group:eng", "member", "group:3#member"),
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
		})
		require.NoError(t, err)

		checker := NewLocalChecker()

		typesys, err := typesystem.NewAndValidate(
			context.Background(),
			model,
		)
		require.NoError(t, err)

		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			RequestMetadata:      NewCheckRequestMetadata(5),
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		require.GreaterOrEqual(t, resp.GetResolutionMetadata().DispatchCount, uint32(2))
		require.LessOrEqual(t, resp.GetResolutionMetadata().DispatchCount, uint32(4))

		resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:other"),
			RequestMetadata:      NewCheckRequestMetadata(5),
		})
		require.NoError(t, err)
		require.False(t, resp.Allowed)

		require.Equal(t, uint32(4), resp.GetResolutionMetadata().DispatchCount)
	})

	t.Run("dispatch_count_computed_userset_lookups", func(t *testing.T) {
		storeID := ulid.Make().String()

		model := parser.MustTransformDSLToProto(`model
		schema 1.1

		type user
  	
		type document
			relations
		   		define owner: [user]
		   		define editor: [user] or owner`)

		err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "owner", "user:jon"),
			tuple.NewTupleKey("document:2", "editor", "user:will"),
		})
		require.NoError(t, err)

		checker := NewLocalChecker()

		typesys, err := typesystem.NewAndValidate(
			context.Background(),
			model,
		)
		require.NoError(t, err)

		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:1", "owner", "user:jon"),
			RequestMetadata:      NewCheckRequestMetadata(5),
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		require.Zero(t, resp.GetResolutionMetadata().DispatchCount)

		resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:2", "editor", "user:will"),
			RequestMetadata:      NewCheckRequestMetadata(5),
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		require.LessOrEqual(t, resp.GetResolutionMetadata().DispatchCount, uint32(1))
		require.GreaterOrEqual(t, resp.GetResolutionMetadata().DispatchCount, uint32(0))

		resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:2", "editor", "user:jon"),
			RequestMetadata:      NewCheckRequestMetadata(5),
		})
		require.NoError(t, err)
		require.False(t, resp.Allowed)
		require.Equal(t, uint32(1), resp.GetResolutionMetadata().DispatchCount)
	})
}

func TestUnionCheckFuncReducer(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	concurrencyLimit := uint32(10)

	falseHandler := func(context.Context) (*ResolveCheckResponse, error) {
		return &ResolveCheckResponse{
			Allowed:            false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{},
		}, nil
	}

	trueHandler := func(context.Context) (*ResolveCheckResponse, error) {
		return &ResolveCheckResponse{
			Allowed:            true,
			ResolutionMetadata: &ResolveCheckResponseMetadata{},
		}, nil
	}

	t.Run("if_no_handlers_return_allowed_false", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})

	t.Run("if_one_handler_is_truthy_and_others_are_falsey_return_allowed_true", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, trueHandler, falseHandler, falseHandler)
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
	})

	t.Run("if_all_handlers_are_falsey_return_allowed_false", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, falseHandler, falseHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})

	t.Run("if_a_handler_errors_but_other_handler_are_truthy_return_allowed_true", func(t *testing.T) {
		depthExceededHandler := func(context.Context) (*ResolveCheckResponse, error) {
			return nil, ErrResolutionDepthExceeded
		}

		resp, err := union(ctx, concurrencyLimit, depthExceededHandler, trueHandler)
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
	})

	t.Run("if_a_handler_errors_but_other_handler_is_falsey_return_error_and_allowed_false", func(t *testing.T) {
		depthExceededHandler := func(context.Context) (*ResolveCheckResponse, error) {
			return nil, ErrResolutionDepthExceeded
		}

		resp, err := union(ctx, concurrencyLimit, depthExceededHandler, falseHandler)
		require.ErrorIs(t, ErrResolutionDepthExceeded, err)
		require.False(t, resp.GetAllowed())
	})

	t.Run("if_a_handler_errors_with_cycle_but_other_handler_is_falsey_return_allowed_false_with_a_nil_error", func(t *testing.T) {
		cyclicErrorHandler := func(context.Context) (*ResolveCheckResponse, error) {
			return nil, ErrCycleDetected
		}

		resp, err := union(ctx, concurrencyLimit, cyclicErrorHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})

	t.Run("both_handlers_with_cycle_return_allowed_false_with_nil_error", func(t *testing.T) {
		cyclicErrorHandler := func(context.Context) (*ResolveCheckResponse, error) {
			return nil, ErrCycleDetected
		}

		resp, err := union(ctx, concurrencyLimit, cyclicErrorHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})

	t.Run("if_a_handler_errors_with_cycle_but_other_handler_is_truthy_return_allowed_true_with_a_nil_error", func(t *testing.T) {
		cyclicErrorHandler := func(context.Context) (*ResolveCheckResponse, error) {
			return nil, ErrCycleDetected
		}

		resp, err := union(ctx, concurrencyLimit, cyclicErrorHandler, trueHandler)
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
	})

	t.Run("should_aggregate_DatastoreQueryCount_of_non_error handlers", func(t *testing.T) {
		trueHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(5 * time.Millisecond) // forces `trueHandler` to be resolved after `falseHandler`
			return &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: uint32(1),
				},
			}, nil
		}

		falseHandler := func(context.Context) (*ResolveCheckResponse, error) {
			return &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: uint32(5),
				},
			}, nil
		}

		errorHandler := func(context.Context) (*ResolveCheckResponse, error) {
			return &ResolveCheckResponse{
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: uint32(9999999),
				},
			}, ErrResolutionDepthExceeded
		}

		resp, err := union(ctx, concurrencyLimit, falseHandler, trueHandler, errorHandler)
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
		require.Equal(t, uint32(5+1), resp.GetResolutionMetadata().DatastoreQueryCount)
	})

	t.Run("should_aggregate_DatastoreQueryCount_of_all_falsey_handlers", func(t *testing.T) {
		handler := func(context.Context) (*ResolveCheckResponse, error) {
			return &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: uint32(3),
				},
			}, nil
		}

		resp, err := union(ctx, concurrencyLimit, handler, handler, handler) // three handlers
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.Equal(t, uint32(3*3), resp.GetResolutionMetadata().DatastoreQueryCount)
	})

	t.Run("should_return_allowed_true_if_truthy_handler_evaluated_before_handler_cancels_via_context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		t.Cleanup(cancel)

		resp, err := union(ctx, concurrencyLimit, trueHandler)
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
	})
	t.Run("should_handle_cancellations_through_context", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		t.Cleanup(cancel)

		slowHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(50 * time.Millisecond)
			return nil, nil
		}

		resp, err := union(ctx, concurrencyLimit, slowHandler, falseHandler)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, resp.GetAllowed())
	})

	t.Run("should_handle_context_timeouts_even_with_eventual_truthy_handler", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		t.Cleanup(cancel)

		trueHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(25 * time.Millisecond)
			return &ResolveCheckResponse{
				Allowed: true,
			}, nil
		}

		resp, err := union(ctx, concurrencyLimit, trueHandler, falseHandler)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, resp.GetAllowed())
	})
}
