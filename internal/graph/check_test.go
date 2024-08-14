package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/structpb"

	openfgaErrors "github.com/openfga/openfga/internal/errors"
	"github.com/openfga/openfga/pkg/logger"
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
		return nil, ErrResolutionDepthExceeded
	}

	cyclicErrorHandler = func(context.Context) (*ResolveCheckResponse, error) {
		return &ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: &ResolveCheckResponseMetadata{
				DatastoreQueryCount: 1,
				CycleDetected:       true,
			},
		}, nil
	}

	simulatedDBErrorMessage = "simulated db error"

	generalErrorHandler = func(context.Context) (*ResolveCheckResponse, error) {
		return nil, errors.New(simulatedDBErrorMessage)
	}
)

func TestCheck_CorrectContext(t *testing.T) {
	checker := NewLocalChecker()
	t.Cleanup(checker.Close)

	t.Run("typesystem_missing_returns_error", func(t *testing.T) {
		_, err := checker.ResolveCheck(context.Background(), &ResolveCheckRequest{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			RequestMetadata:      NewCheckRequestMetadata(20),
		})
		require.ErrorContains(t, err, "typesystem missing in context")
	})

	t.Run("datastore_missing_returns_error", func(t *testing.T) {
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type document
				relations
					define viewer: [user]`)
		ctx := typesystem.ContextWithTypesystem(context.Background(), typesystem.New(model))

		_, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			RequestMetadata:      NewCheckRequestMetadata(20),
		})
		require.ErrorContains(t, err, "relationship tuple reader datastore missing in context")
	})
}

func TestExclusionCheckFuncReducer(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	concurrencyLimit := uint32(10)

	t.Run("requires_exactly_two_handlers", func(t *testing.T) {
		_, err := exclusion(ctx, concurrencyLimit)
		require.ErrorIs(t, err, openfgaErrors.ErrUnknown)

		_, err = exclusion(ctx, concurrencyLimit, falseHandler)
		require.ErrorIs(t, err, openfgaErrors.ErrUnknown)

		_, err = exclusion(ctx, concurrencyLimit, falseHandler, falseHandler, falseHandler)
		require.ErrorIs(t, err, openfgaErrors.ErrUnknown)

		_, err = exclusion(ctx, concurrencyLimit, falseHandler, falseHandler)
		require.NoError(t, err)
	})

	t.Run("true_butnot_true_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, trueHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(1+1))
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_butnot_false_return_true", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, trueHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.GetAllowed())
		require.Equal(t, uint32(1+1), resp.GetResolutionMetadata().DatastoreQueryCount)
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_butnot_true_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(1+1))
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_butnot_false_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(1+1))
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_butnot_err_return_err", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, trueHandler, generalErrorHandler)
		require.EqualError(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("true_butnot_errResolutionDepth_return_errResolutionDepth", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, trueHandler, depthExceededHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("true_butnot_cycle_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, trueHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("false_butnot_err_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, generalErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_butnot_errResolutionDepth_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, depthExceededHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_butnot_cycle_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
	})

	t.Run("err_butnot_true_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, generalErrorHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("errResolutionDepth_butnot_true_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, depthExceededHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("cycle_butnot_true_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, cyclicErrorHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
	})

	t.Run("err_butnot_false_return_err", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, generalErrorHandler, falseHandler)
		require.EqualError(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("errResolutionDepth_butnot_false_return_errResolutionDepth", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, depthExceededHandler, falseHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("cycle_butnot_false_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, cyclicErrorHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("cycle_butnot_err_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, cyclicErrorHandler, generalErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("cycle_butnot_errResolutionDepth_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, cyclicErrorHandler, depthExceededHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("err_butnot_cycle_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, generalErrorHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("errResolutionDepth_butnot_cycle_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, depthExceededHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("cycle_butnot_cycle_return_false", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, cyclicErrorHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("err_butnot_err_return_err", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, generalErrorHandler, generalErrorHandler)
		require.ErrorContains(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("errResolutionDepth_butnot_errResolutionDepth_return_errResolutionDepth", func(t *testing.T) {
		resp, err := exclusion(ctx, concurrencyLimit, depthExceededHandler, depthExceededHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
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

	t.Run("return_allowed:false_if_sub_handler_evaluated_before_context_cancelled", func(t *testing.T) {
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

	t.Run("return_allowed:false_if_sub_handler_evaluated_before_base_cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		var wg sync.WaitGroup

		slowTrueHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(50 * time.Millisecond)

			return &ResolveCheckResponse{
				Allowed: true,
			}, nil
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		resp, err := exclusion(ctx, concurrencyLimit, slowTrueHandler, trueHandler)
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

		slowTrueHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(50 * time.Millisecond)
			return &ResolveCheckResponse{
				Allowed: true,
			}, nil
		}

		slowFalseHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(50 * time.Millisecond)
			return &ResolveCheckResponse{
				Allowed: false,
			}, nil
		}

		resp, err := exclusion(ctx, concurrencyLimit, slowTrueHandler, slowFalseHandler)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, resp)
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
			return &ResolveCheckResponse{}, nil
		}

		resp, err := exclusion(ctx, concurrencyLimit, slowHandler, slowHandler)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, resp)

		wg.Wait() // just to make sure to avoid test leaks
	})
}

func TestIntersectionCheckFuncReducer(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ctx := context.Background()

	concurrencyLimit := uint32(10)

	t.Run("no_handlers_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.NotNil(t, resp.GetResolutionMetadata())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.Equal(t, uint32(1), resp.GetResolutionMetadata().DatastoreQueryCount)
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_and_true_return_true", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, trueHandler, trueHandler)
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
		require.Equal(t, uint32(2), resp.GetResolutionMetadata().DatastoreQueryCount)
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_and_false_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, trueHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(2))
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_and_true_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, falseHandler, trueHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(2))
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_and_false_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, falseHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.Equal(t, uint32(1), resp.GetResolutionMetadata().DatastoreQueryCount)
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_and_err_return_err", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, trueHandler, generalErrorHandler)
		require.EqualError(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("true_and_errResolutionDepth_return_err", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, trueHandler, depthExceededHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("true_and_cycle_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, trueHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("false_and_err_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, falseHandler, generalErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_and_errResolutionDepth_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, falseHandler, depthExceededHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_and_cycle_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, falseHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
	})

	t.Run("err_and_true_return_err", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, generalErrorHandler, trueHandler)
		require.EqualError(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("errResolutionDepth_and_true_return_err", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, depthExceededHandler, trueHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("cycle_and_true_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("err_and_false_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, generalErrorHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("errResolutionDepth_and_false_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, depthExceededHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("cycle_and_false_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
	})

	t.Run("cycle_and_err_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, generalErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("cycle_and_errResolutionDepth_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, depthExceededHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("err_and_cycle_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, generalErrorHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("errResolutionDepth_and_cycle_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, depthExceededHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("cycle_and_cycle_return_false", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("err_and_err_return_err", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, generalErrorHandler, generalErrorHandler)
		require.ErrorContains(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("errResolutionDepth_and_errResolutionDepth_return_errResolutionDepth", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, depthExceededHandler, depthExceededHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("true_and_cycle_and_err_return_err", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, trueHandler, cyclicErrorHandler, generalErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("aggregate_truthy_and_falsy_handlers_datastore_query_count", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, falseHandler, trueHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.LessOrEqual(t, resp.GetResolutionMetadata().DatastoreQueryCount, uint32(1+1))
	})

	t.Run("cycle_and_false_reports_correct_datastore_query_count", func(t *testing.T) {
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, falseHandler)
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

	t.Run("return_true_if_truthy_handler_evaluated_before_context_deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		t.Cleanup(cancel)

		resp, err := intersection(ctx, concurrencyLimit, trueHandler)
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
	})

	t.Run("return_error_if_context_deadline_before_truthy_handler", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		t.Cleanup(cancel)

		slowTrueHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(50 * time.Millisecond)
			return &ResolveCheckResponse{
				Allowed: true,
			}, nil
		}

		resp, err := intersection(ctx, concurrencyLimit, slowTrueHandler)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, resp)
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

		slowTrueHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(50 * time.Millisecond)
			return &ResolveCheckResponse{
				Allowed: true,
			}, nil
		}

		resp, err := intersection(ctx, concurrencyLimit, slowTrueHandler)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, resp)
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

		slowTrueHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(50 * time.Millisecond)
			return &ResolveCheckResponse{
				Allowed: true,
			}, nil
		}

		resp, err := intersection(ctx, concurrencyLimit, slowTrueHandler)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, resp)

		wg.Wait() // just to make sure to avoid test leaks
	})
}

func TestNonStratifiableCheckQueries(t *testing.T) {
	checker, checkResolverCloser := NewOrderedCheckResolvers().Build()
	t.Cleanup(checkResolverCloser)

	t.Run("example_1", func(t *testing.T) {
		ds := memory.New()

		storeID := ulid.Make().String()

		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			tuple.NewTupleKey("document:1", "restricted", "document:1#viewer"),
		})
		require.NoError(t, err)

		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1

			type user


			type document
				relations
					define viewer: [user] but not restricted
					define restricted: [user, document#viewer]`)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			typesystem.New(model),
		)

		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:         storeID,
			TupleKey:        tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			RequestMetadata: NewCheckRequestMetadata(10),
		})
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})

	t.Run("example_2", func(t *testing.T) {
		ds := memory.New()

		storeID := ulid.Make().String()

		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			tuple.NewTupleKey("document:1", "restrictedb", "document:1#viewer"),
		})
		require.NoError(t, err)

		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1

			type user


			type document
				relations
					define viewer: [user] but not restricteda
					define restricteda: restrictedb
					define restrictedb: [user, document#viewer]
			`)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			typesystem.New(model),
		)

		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:         storeID,
			TupleKey:        tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			RequestMetadata: NewCheckRequestMetadata(10),
		})
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})
}

func TestResolveCheckDeterministic(t *testing.T) {
	checker, checkResolverCloser := NewOrderedCheckResolvers().Build()
	t.Cleanup(checkResolverCloser)

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

		model := testutils.MustTransformDSLToProtoWithID(`
			model
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

		model := testutils.MustTransformDSLToProtoWithID(`
			model
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

		model := testutils.MustTransformDSLToProtoWithID(`
			model
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

	checker, checkResolverCloser := NewOrderedCheckResolvers(
		WithLocalCheckerOpts(WithResolveNodeBreadthLimit(concurrencyLimit)),
	).Build()
	t.Cleanup(checkResolverCloser)

	model := testutils.MustTransformDSLToProtoWithID(`
		model
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
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	defer ds.Close()

	storeID := ulid.Make().String()

	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:x", "a", "user:jon"),
		tuple.NewTupleKey("document:x", "a", "user:maria"),
		tuple.NewTupleKey("document:x", "b", "user:maria"),
		tuple.NewTupleKey("document:x", "parent", "org:fga"),
		tuple.NewTupleKey("org:fga", "member", "user:maria"),
		tuple.NewTupleKey("company:fga", "member", "user:maria"),
		tuple.NewTupleKey("document:x", "userset", "org:fga#member"),
		tuple.NewTupleKey("document:x", "multiple_userset", "org:fga#member"),
		tuple.NewTupleKey("document:x", "multiple_userset", "company:fga#member"),
		tuple.NewTupleKey("document:public", "wildcard", "user:*"),
	})
	require.NoError(t, err)

	model := parser.MustTransformDSLToProto(`
		model
			schema 1.1

		type user

		type company
			relations
				define member: [user]

		type org
			relations
				define member: [user]

		type document
			relations
				define wildcard: [user:*]
				define userset: [org#member]
				define multiple_userset: [org#member, company#member]
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
		minDBReads       uint32 // expected lowest value for number returned in the metadata
		maxDBReads       uint32 // expected highest value for number returned in the metadata. Actual db reads may be higher
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
		{
			name:       "userset_no_access_1",
			check:      tuple.NewTupleKey("document:no_access", "userset", "user:maria"),
			allowed:    false,
			minDBReads: 1, // 1 userset read (none found)
			maxDBReads: 1,
		},
		{
			name:       "userset_no_access_2",
			check:      tuple.NewTupleKey("document:x", "userset", "user:no_access"),
			allowed:    false,
			minDBReads: 2, // 1 userset read (1 found) follow by 1 direct tuple check (not found)
			maxDBReads: 2,
		},
		{
			name:       "userset_access",
			check:      tuple.NewTupleKey("document:x", "userset", "user:maria"),
			allowed:    true,
			minDBReads: 2, // 1 userset read (1 found) follow by 1 direct tuple check (found)
			maxDBReads: 2,
		},
		{
			name:       "multiple_userset_no_access",
			check:      tuple.NewTupleKey("document:x", "multiple_userset", "user:no_access"),
			allowed:    false,
			minDBReads: 3, // 1 userset read (2 found) follow by 2 direct tuple check (not found)
			maxDBReads: 3,
		},
		{
			name:       "multiple_userset_access",
			check:      tuple.NewTupleKey("document:x", "multiple_userset", "user:maria"),
			allowed:    true,
			minDBReads: 2, // 1 userset read (2 found) follow by 1 direct tuple check (found, returns immediately)
			maxDBReads: 2,
		},
		{
			name:       "wildcard_no_access",
			check:      tuple.NewTupleKey("document:x", "wildcard", "user:maria"),
			allowed:    false,
			minDBReads: 1, // 1 direct tuple read (not found)
			maxDBReads: 1,
		},
		{
			name:       "wildcard_access",
			check:      tuple.NewTupleKey("document:public", "wildcard", "user:maria"),
			allowed:    true,
			minDBReads: 1, // 1 direct tuple read (found)
			maxDBReads: 1,
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

	checker, checkResolverCloser := NewOrderedCheckResolvers(
		WithLocalCheckerOpts(WithMaxConcurrentReads(1)),
	).Build()
	t.Cleanup(checkResolverCloser)

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

func TestCheckConditions(t *testing.T) {
	ds := memory.New()

	storeID := ulid.Make().String()

	tkConditionContext, err := structpb.NewStruct(map[string]interface{}{
		"param1": "ok",
	})
	require.NoError(t, err)

	model := parser.MustTransformDSLToProto(`
		model
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

	checker, checkResolverCloser := NewOrderedCheckResolvers().Build()
	t.Cleanup(checkResolverCloser)

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

		model := parser.MustTransformDSLToProto(`
			model
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

		checkRequestMetadata := NewCheckRequestMetadata(5)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("doc:readme", "viewer", "user:jon"),
			RequestMetadata:      checkRequestMetadata,
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		require.Equal(t, uint32(3), checkRequestMetadata.DispatchCounter.Load())

		t.Run("direct_lookup_requires_no_dispatch", func(t *testing.T) {
			checkRequestMetadata := NewCheckRequestMetadata(5)

			resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: model.GetId(),
				TupleKey:             tuple.NewTupleKey("doc:readme", "parent", "folder:A"),
				RequestMetadata:      checkRequestMetadata,
			})
			require.NoError(t, err)
			require.True(t, resp.Allowed)

			require.Zero(t, checkRequestMetadata.DispatchCounter.Load())
		})
	})

	t.Run("dispatch_count_multiple_direct_userset_lookups", func(t *testing.T) {
		storeID := ulid.Make().String()

		model := parser.MustTransformDSLToProto(`
			model
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
		checkRequestMetadata := NewCheckRequestMetadata(5)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			RequestMetadata:      checkRequestMetadata,
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		require.GreaterOrEqual(t, checkRequestMetadata.DispatchCounter.Load(), uint32(2))
		require.LessOrEqual(t, checkRequestMetadata.DispatchCounter.Load(), uint32(4))

		checkRequestMetadata = NewCheckRequestMetadata(5)

		resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:other"),
			RequestMetadata:      checkRequestMetadata,
		})
		require.NoError(t, err)
		require.False(t, resp.Allowed)

		require.Equal(t, uint32(4), checkRequestMetadata.DispatchCounter.Load())
	})

	t.Run("dispatch_count_computed_userset_lookups", func(t *testing.T) {
		storeID := ulid.Make().String()

		model := parser.MustTransformDSLToProto(`
			model
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
		checkRequestMetadata := NewCheckRequestMetadata(5)
		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:1", "owner", "user:jon"),
			RequestMetadata:      checkRequestMetadata,
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		require.Zero(t, checkRequestMetadata.DispatchCounter.Load())

		checkRequestMetadata = NewCheckRequestMetadata(5)

		resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:2", "editor", "user:will"),
			RequestMetadata:      checkRequestMetadata,
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		require.LessOrEqual(t, checkRequestMetadata.DispatchCounter.Load(), uint32(1))
		require.GreaterOrEqual(t, checkRequestMetadata.DispatchCounter.Load(), uint32(0))

		checkRequestMetadata = NewCheckRequestMetadata(5)
		resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:2", "editor", "user:jon"),
			RequestMetadata:      checkRequestMetadata,
		})
		require.NoError(t, err)
		require.False(t, resp.Allowed)
		require.Equal(t, uint32(0), checkRequestMetadata.DispatchCounter.Load())
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

	t.Run("no_handlers_return_allowed_false", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_or_true_return_true", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, trueHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_or_false_return_true", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, trueHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_or_true_return_true", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, falseHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_or_false_return_false", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, falseHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_or_err_return_true", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, trueHandler, generalErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_or_errResolutionDepth_return_true", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, trueHandler, depthExceededHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_or_cycle_return_true", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, trueHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_or_err_return_err", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, falseHandler, generalErrorHandler)
		require.EqualError(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("false_or_errResolutionDepth_return_err", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, falseHandler, depthExceededHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("false_or_cycle_return_false", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, falseHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("err_or_true_return_true", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, generalErrorHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("cycle_or_true_return_true", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, cyclicErrorHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("err_or_false_return_err", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, generalErrorHandler, falseHandler)
		require.EqualError(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("errResolutionDepth_or_false_return_err", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, depthExceededHandler, falseHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("cycle_or_false_return_false", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, cyclicErrorHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("cycle_or_err_return_err", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, cyclicErrorHandler, generalErrorHandler)
		require.EqualError(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("cycle_or_errResolutionDepth_return_err", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, cyclicErrorHandler, depthExceededHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("err_or_cycle_return_err", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, generalErrorHandler, cyclicErrorHandler)
		require.EqualError(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("errResolutionDepth_or_cycle_return_err", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, depthExceededHandler, cyclicErrorHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("cycle_or_cycle_return_false", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, cyclicErrorHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("false_or_cycle_or_err", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, falseHandler, cyclicErrorHandler, generalErrorHandler)
		require.EqualError(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("err_or_err_return_err", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, falseHandler, generalErrorHandler, generalErrorHandler)
		require.ErrorContains(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("errResolutionDepth_or_errResolutionDepth_return_errResolutionDepth", func(t *testing.T) {
		resp, err := union(ctx, concurrencyLimit, falseHandler, depthExceededHandler, depthExceededHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("should_aggregate_DatastoreQueryCount_of_non_error_handlers", func(t *testing.T) {
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
			return &ResolveCheckResponse{}, nil
		}

		resp, err := union(ctx, concurrencyLimit, slowHandler, falseHandler)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, resp)
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
		require.Nil(t, resp)
	})

	t.Run("should_return_true_with_slow_falsey_handler", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		t.Cleanup(cancel)

		falseSlowHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(25 * time.Millisecond)
			return &ResolveCheckResponse{
				Allowed: false,
			}, nil
		}

		resp, err := union(ctx, concurrencyLimit, trueHandler, falseSlowHandler)
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
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

		slowTrueHandler := func(context.Context) (*ResolveCheckResponse, error) {
			time.Sleep(50 * time.Millisecond)
			return &ResolveCheckResponse{
				Allowed: true,
			}, nil
		}

		resp, err := union(ctx, concurrencyLimit, slowTrueHandler)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, resp)

		wg.Wait() // just to make sure to avoid test leaks
	})

	t.Run("return_allowed:true_if_truthy_handler_evaluated_before_context_cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()

		resp, err := intersection(ctx, concurrencyLimit, trueHandler)
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())

		wg.Wait() // just to make sure to avoid test leaks
	})
}

func TestCheckWithFastPathOptimization(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	usersetBatchSize := uint32(10)
	ds := memory.New()
	t.Cleanup(ds.Close)
	storeID := ulid.Make().String()
	model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type directory
				relations
					define viewer: [user]
			type folder
				relations
					define viewer: [user]
			type doc
				relations
					define viewer: viewer from parent
					define parent: [folder, directory]`)

	// add some folders as parents of the document
	maxFolderID := int(usersetBatchSize * 5)
	maxDirectoryID := int(usersetBatchSize * 5)
	for i := 0; i <= maxFolderID; i++ {
		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("doc:1", "parent", fmt.Sprintf("folder:%d", i)),
		})
		require.NoError(t, err)
	}
	// having 2 types will force a flush when there is a change in types "seen"
	for i := 0; i <= maxDirectoryID; i++ {
		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("doc:1", "parent", fmt.Sprintf("directory:%d", i)),
		})
		require.NoError(t, err)
	}

	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("folder:1", "viewer", "user:a"),
		tuple.NewTupleKey(fmt.Sprintf("folder:%d", maxFolderID), "viewer", "user:b"),
	})
	require.NoError(t, err)

	ts, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)

	ctx := typesystem.ContextWithTypesystem(storage.ContextWithRelationshipTupleReader(context.Background(), ds), ts)

	newL, _ := logger.NewLogger(logger.WithFormat("text"), logger.WithLevel("debug"))
	checker := NewLocalChecker(WithUsersetBatchSize(usersetBatchSize), WithLocalCheckerLogger(newL))
	t.Cleanup(checker.Close)

	var testCases = map[string]struct {
		request       *openfgav1.TupleKey
		expectAllowed bool
	}{
		// first folder so the producer is forced to abort iteration early
		`first_folder`: {
			request:       tuple.NewTupleKey("doc:1", "viewer", "user:a"),
			expectAllowed: true,
		},
		// last folder so the producer has to read the entire iterator
		`last_folder`: {
			request:       tuple.NewTupleKey("doc:1", "viewer", "user:b"),
			expectAllowed: true,
		},
	}

	for testname, test := range testCases {
		t.Run(testname, func(t *testing.T) {
			t.Run("without_context_timeout", func(t *testing.T) {
				resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
					StoreID:              storeID,
					AuthorizationModelID: model.GetId(),
					TupleKey:             test.request,
					RequestMetadata:      NewCheckRequestMetadata(20),
				})
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Equal(t, test.expectAllowed, resp.Allowed)
			})

			t.Run("with_context_timeout", func(t *testing.T) {
				for i := 0; i < 100; i++ {
					// run in a for loop to hopefully trigger context cancellations at different points in execution
					t.Run(fmt.Sprintf("iteration_%d", i), func(t *testing.T) {
						newCtx, cancel := context.WithTimeout(ctx, 10*time.Microsecond)
						defer cancel()
						resp, err := checker.ResolveCheck(newCtx, &ResolveCheckRequest{
							StoreID:              storeID,
							AuthorizationModelID: model.GetId(),
							TupleKey:             test.request,
							RequestMetadata:      NewCheckRequestMetadata(20),
						})
						if err != nil {
							require.ErrorIs(t, err, context.DeadlineExceeded)
						} else {
							require.NotNil(t, resp)
							require.Equal(t, test.expectAllowed, resp.Allowed)
						}
					})
				}
			})
		})
	}
}

func TestCloneResolveCheckResponse(t *testing.T) {
	resp1 := &ResolveCheckResponse{
		Allowed: true,
		ResolutionMetadata: &ResolveCheckResponseMetadata{
			DatastoreQueryCount: 1,
			CycleDetected:       false,
		},
	}
	clonedResp1 := CloneResolveCheckResponse(resp1)

	require.Equal(t, resp1, clonedResp1)
	require.NotSame(t, resp1, clonedResp1)

	// mutate the clone and ensure the original reference is
	// unchanged
	clonedResp1.Allowed = false
	clonedResp1.ResolutionMetadata.DatastoreQueryCount = 2
	clonedResp1.ResolutionMetadata.CycleDetected = true
	require.True(t, resp1.GetAllowed())
	require.Equal(t, uint32(1), resp1.GetResolutionMetadata().DatastoreQueryCount)
	require.False(t, resp1.GetResolutionMetadata().CycleDetected)

	resp2 := &ResolveCheckResponse{
		Allowed: true,
	}
	clonedResp2 := CloneResolveCheckResponse(resp2)

	require.NotSame(t, resp2, clonedResp2)
	require.Equal(t, resp2.GetAllowed(), clonedResp2.GetAllowed())
	require.NotNil(t, clonedResp2.ResolutionMetadata)
	require.Equal(t, uint32(0), clonedResp2.GetResolutionMetadata().DatastoreQueryCount)
	require.False(t, clonedResp2.GetResolutionMetadata().CycleDetected)
}

func TestCycleDetection(t *testing.T) {
	ds := memory.New()
	t.Cleanup(ds.Close)

	checker := NewLocalChecker()
	t.Cleanup(checker.Close)

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockDelegate := NewMockCheckResolver(ctrl)
	checker.SetDelegate(mockDelegate)

	// assert that we never call dispatch
	mockDelegate.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(0)

	t.Run("returns_true_if_path_visited", func(t *testing.T) {
		cyclicalTuple := tuple.NewTupleKey("document:1", "viewer", "user:maria")

		resp, err := checker.ResolveCheck(context.Background(), &ResolveCheckRequest{
			StoreID:         ulid.Make().String(),
			TupleKey:        cyclicalTuple, // here
			RequestMetadata: NewCheckRequestMetadata(defaultResolveNodeLimit),
			VisitedPaths: map[string]struct{}{
				tuple.TupleKeyToString(cyclicalTuple): {}, // and here
			},
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
		require.NotNil(t, resp.ResolutionMetadata)
	})

	t.Run("returns_true_if_computed_userset", func(t *testing.T) {
		storeID := ulid.Make().String()
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type document
				relations
					define x: y
					define y: x`)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			typesystem.New(model),
		)

		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		t.Run("disconnected_types_in_query", func(t *testing.T) {
			resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: model.GetId(),
				TupleKey:             tuple.NewTupleKey("document:1", "y", "user:maria"),
				RequestMetadata:      NewCheckRequestMetadata(20),
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.False(t, resp.GetAllowed())
			require.True(t, resp.GetCycleDetected())
		})

		t.Run("connected_types_in_query", func(t *testing.T) {
			resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: model.GetId(),
				TupleKey:             tuple.NewTupleKey("document:1", "y", "document:2#x"),
				RequestMetadata:      NewCheckRequestMetadata(20),
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.False(t, resp.GetAllowed())
			require.True(t, resp.GetCycleDetected())
		})
	})
}

func TestGetComputedRelation(t *testing.T) {
	tests := []struct {
		name             string
		model            string
		objectType       string
		relation         string
		expectedError    bool
		expectedRelation string
	}{
		{
			name: "direct_assignment",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]`,
			objectType:       "group",
			relation:         "member",
			expectedRelation: "member",
			expectedError:    false,
		},
		{
			name: "computed_relation",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define viewable_member1: member
					define viewable_member2: viewable_member1`,
			objectType:       "group",
			relation:         "viewable_member2",
			expectedRelation: "member",
			expectedError:    false,
		},
		{
			name: "deep_computed_relation",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define viewable_member1: member
					define viewable_member2: viewable_member1
					define viewable_member3: viewable_member2
					define viewable_member4: viewable_member3`,

			objectType:       "group",
			relation:         "viewable_member4",
			expectedRelation: "member",
			expectedError:    false,
		},
		{
			name: "unexpected_rel",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define viewable_member1: member
					define viewable_member2: [user] and viewable_member1`,
			objectType:       "group",
			relation:         "viewable_member2",
			expectedRelation: "",
			expectedError:    true,
		},
		{
			name: "rel_not_found",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]`,
			objectType:       "group",
			relation:         "not_found",
			expectedRelation: "",
			expectedError:    true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := typesystem.New(testutils.MustTransformDSLToProtoWithID(tt.model))
			output, err := getComputedRelation(ts, tt.objectType, tt.relation)
			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expectedRelation, output)
		})
	}
}

func TestTupleIDInSortedSet(t *testing.T) {
	filter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
		if tupleKey.GetCondition().GetName() == "condition1" {
			return true, nil
		}
		return false, fmt.Errorf("condition not found")
	}

	tests := []struct {
		name          string
		tuples        []*openfgav1.TupleKey
		objectIDs     []string
		expectedError bool
		expected      bool
	}{
		{
			name: "no_match",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:2#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc2", "viewer", "group:2#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc3", "viewer", "group:2#member", "condition1", nil),
			},
			objectIDs:     []string{"doc0", "doc5", "doc6"},
			expected:      false,
			expectedError: false,
		},
		{
			name: "match",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:2#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc2", "viewer", "group:2#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc3", "viewer", "group:2#member", "condition1", nil),
			},
			objectIDs:     []string{"doc0", "doc2", "doc6"},
			expected:      true,
			expectedError: false,
		},
		{
			name: "error",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:2#member", "badCondition", nil),
			},
			objectIDs:     []string{"doc0", "doc2", "doc6"},
			expected:      false,
			expectedError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := storage.NewConditionsFilteredTupleKeyIterator(storage.NewStaticTupleKeyIterator(tt.tuples), filter)
			objectIDs := storage.NewSortedSet()
			for _, item := range tt.objectIDs {
				objectIDs.Add(item)
			}
			result, err := tupleIDInSortedSet(context.Background(), iter, objectIDs)
			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildUsersetDetailsUserset(t *testing.T) {
	tests := []struct {
		name             string
		model            string
		tuple            *openfgav1.TupleKey
		expectedHasError bool
		expectedRelation string
		expectedObjectID string
	}{
		{
			name: "userset_direct_assignment",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
			type folder
				relations
					define viewer: [group#member]
`,
			tuple:            tuple.NewTupleKey("folder:1", "viewer", "group:2#member"),
			expectedHasError: false,
			expectedRelation: "group#member",
			expectedObjectID: "2",
		},
		{
			name: "userset_computed_userset",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define computed_member: member
			type folder
				relations
					define viewer: [group#computed_member]
`,
			tuple:            tuple.NewTupleKey("folder:1", "viewer", "group:2#computed_member"),
			expectedHasError: false,
			expectedRelation: "group#member",
			expectedObjectID: "2",
		},
		{
			name: "relation_not_found",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
			type folder
				relations
					define viewer: [group#computed_member]
`,
			tuple:            tuple.NewTupleKey("folder:1", "viewer", "group:2#computed_member"),
			expectedHasError: true,
			expectedRelation: "",
			expectedObjectID: "",
		},
		{
			name: "nonuserset_model",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define owner: [user]
					define viewer: member or owner
			type folder
				relations
					define viewer: [group#viewer]
`,
			tuple:            tuple.NewTupleKey("folder:1", "viewer", "group:2#viewer"),
			expectedHasError: true,
			expectedRelation: "",
			expectedObjectID: "",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := typesystem.New(testutils.MustTransformDSLToProtoWithID(tt.model))
			usersetFunc := buildUsersetDetailsUserset(ts)
			rel, obj, err := usersetFunc(tt.tuple)
			if tt.expectedHasError {
				// details of the error doesn't really matter
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expectedRelation, rel)
			require.Equal(t, tt.expectedObjectID, obj)
		})
	}
}

func TestBuildUsersetDetailsTTU(t *testing.T) {
	tests := []struct {
		name             string
		model            string
		tuple            *openfgav1.TupleKey
		computedRelation string
		expectedHasError bool
		expectedRelation string
		expectedObjectID string
	}{
		{
			name: "ttu_direct_assignment",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
			type folder
				relations
					define owner: [group]
					define viewer: member from owner
`,
			tuple:            tuple.NewTupleKey("folder:1", "owner", "group:2"),
			computedRelation: "member",
			expectedHasError: false,
			expectedRelation: "group#member",
			expectedObjectID: "2",
		},
		{
			name: "ttu_computed_userset",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define viewable_member: member
			type folder
				relations
					define owner: [group]
					define viewer: viewable_member from owner
`,
			tuple:            tuple.NewTupleKey("folder:1", "owner", "group:2"),
			computedRelation: "viewable_member",
			expectedHasError: false,
			expectedRelation: "group#member",
			expectedObjectID: "2",
		},
		{
			name: "ttu_not_found",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define viewable_member: member
			type folder
				relations
					define owner: [group]
					define viewer: viewable_member from owner
`,
			tuple:            tuple.NewTupleKey("folder:1", "owner", "group:2"),
			computedRelation: "not_found",
			expectedHasError: true,
			expectedRelation: "",
			expectedObjectID: "",
		},
		{
			name: "ttu_not_assignable",
			model: `
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]
					define viewable_member: [user] or member
			type folder
				relations
					define owner: [group]
					define viewer: viewable_member from owner
`,
			tuple:            tuple.NewTupleKey("folder:1", "owner", "group:2"),
			computedRelation: "viewable_member",
			expectedHasError: true,
			expectedRelation: "",
			expectedObjectID: "",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts := typesystem.New(testutils.MustTransformDSLToProtoWithID(tt.model))
			usersetFunc := buildUsersetDetailsTTU(ts, tt.computedRelation)
			rel, obj, err := usersetFunc(tt.tuple)
			if tt.expectedHasError {
				// details of the error doesn't really matter
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expectedRelation, rel)
			require.Equal(t, tt.expectedObjectID, obj)
		})
	}
}
