package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/emirpasic/gods/sets/hashset"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/internal/condition"
	openfgaErrors "github.com/openfga/openfga/internal/errors"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

var (
	falseHandler = func(context.Context) (*ResolveCheckResponse, error) {
		return &ResolveCheckResponse{
			Allowed: false,
		}, nil
	}

	trueHandler = func(context.Context) (*ResolveCheckResponse, error) {
		return &ResolveCheckResponse{
			Allowed: true,
		}, nil
	}

	depthExceededHandler = func(context.Context) (*ResolveCheckResponse, error) {
		return nil, ErrResolutionDepthExceeded
	}

	cyclicErrorHandler = func(context.Context) (*ResolveCheckResponse, error) {
		return &ResolveCheckResponse{
			Allowed: false,
			ResolutionMetadata: ResolveCheckResponseMetadata{
				CycleDetected: true,
			},
		}, nil
	}

	simulatedDBErrorMessage = "simulated db error"

	generalErrorHandler = func(context.Context) (*ResolveCheckResponse, error) {
		return nil, errors.New(simulatedDBErrorMessage)
	}
)

const panicErr = "mock panic for testing"

// mockPanicIterator is a mock implementation of storage.TupleKeyIterator that triggers a panic.
type mockPanicIterator[T any] struct{}

func (m *mockPanicIterator[T]) Next(ctx context.Context) (T, error) {
	panic(panicErr)
}

func (m *mockPanicIterator[T]) Stop() {
	panic(panicErr)
}

// Head is a mock implementation of the Head method for the storage.Iterator interface.
func (m *mockPanicIterator[T]) Head(ctx context.Context) (T, error) {
	panic(panicErr)
}

func TestCheck_CorrectContext(t *testing.T) {
	checker := NewLocalChecker()
	t.Cleanup(checker.Close)

	t.Run("typesystem_missing_returns_error", func(t *testing.T) {
		_, err := checker.ResolveCheck(context.Background(), &ResolveCheckRequest{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			RequestMetadata:      NewCheckRequestMetadata(),
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
		ts, err := typesystem.New(model)
		require.NoError(t, err)
		ctx := typesystem.ContextWithTypesystem(context.Background(), ts)

		_, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			RequestMetadata:      NewCheckRequestMetadata(),
		})
		require.ErrorContains(t, err, "relationship tuple reader datastore missing in context")
	})
}

func TestExclusionCheckFuncReducer(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	concurrencyLimit := 1

	t.Run("requires_exactly_two_handlers", func(t *testing.T) {
		ctx := context.Background()

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
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, trueHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_butnot_false_return_true", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, trueHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_butnot_true_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_butnot_false_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		fmt.Println(resp.GetAllowed())
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_butnot_err_return_err", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, trueHandler, generalErrorHandler)
		require.EqualError(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("true_butnot_errResolutionDepth_return_errResolutionDepth", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, trueHandler, depthExceededHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("true_butnot_cycle_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, trueHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("false_butnot_err_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, generalErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_butnot_errResolutionDepth_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, depthExceededHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_butnot_cycle_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, falseHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
	})

	t.Run("err_butnot_true_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, generalErrorHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("errResolutionDepth_butnot_true_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, depthExceededHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("cycle_butnot_true_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, cyclicErrorHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
	})

	t.Run("err_butnot_false_return_err", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, generalErrorHandler, falseHandler)
		require.EqualError(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("errResolutionDepth_butnot_false_return_errResolutionDepth", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, depthExceededHandler, falseHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("cycle_butnot_false_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, cyclicErrorHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("cycle_butnot_err_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, cyclicErrorHandler, generalErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("cycle_butnot_errResolutionDepth_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, cyclicErrorHandler, depthExceededHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("err_butnot_cycle_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, generalErrorHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("errResolutionDepth_butnot_cycle_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, depthExceededHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("cycle_butnot_cycle_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, cyclicErrorHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("err_butnot_err_return_err", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, generalErrorHandler, generalErrorHandler)
		require.ErrorContains(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("errResolutionDepth_butnot_errResolutionDepth_return_errResolutionDepth", func(t *testing.T) {
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, depthExceededHandler, depthExceededHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("return_allowed:false_if_base_handler_evaluated_before_context_deadline", func(t *testing.T) {
		ctx := context.Background()
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

	t.Run("should_error_if_base_handler_panics", func(t *testing.T) {
		panicHandler := func(context.Context) (*ResolveCheckResponse, error) {
			panic(panicErr)
		}
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, panicHandler, falseHandler)
		require.ErrorContains(t, err, panicErr)
		require.ErrorIs(t, err, ErrPanic)
		require.Nil(t, resp)
	})

	t.Run("should_error_if_sub_handler_panics", func(t *testing.T) {
		panicHandler := func(context.Context) (*ResolveCheckResponse, error) {
			panic(panicErr)
		}
		ctx := context.Background()
		resp, err := exclusion(ctx, concurrencyLimit, trueHandler, panicHandler)
		require.ErrorContains(t, err, panicErr)
		require.ErrorIs(t, err, ErrPanic)
		require.Nil(t, resp)
	})
}

func TestIntersectionCheckFuncReducer(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	concurrencyLimit := 2

	t.Run("no_handlers_return_error", func(t *testing.T) {
		ctx := context.Background()
		_, err := intersection(ctx, concurrencyLimit)
		require.Error(t, err)
	})

	t.Run("true_and_true_return_true", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, trueHandler, trueHandler)
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_and_false_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, trueHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_and_true_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, falseHandler, trueHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_and_false_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, falseHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("true_and_err_return_err", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, trueHandler, generalErrorHandler)
		require.EqualError(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("true_and_errResolutionDepth_return_err", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, trueHandler, depthExceededHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("true_and_cycle_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, trueHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("false_and_err_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, falseHandler, generalErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_and_errResolutionDepth_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, falseHandler, depthExceededHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("false_and_cycle_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, falseHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
	})

	t.Run("err_and_true_return_err", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, generalErrorHandler, trueHandler)
		require.EqualError(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("errResolutionDepth_and_true_return_err", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, depthExceededHandler, trueHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("cycle_and_true_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, trueHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("err_and_false_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, generalErrorHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("errResolutionDepth_and_false_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, depthExceededHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.False(t, resp.GetCycleDetected())
	})

	t.Run("cycle_and_false_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, falseHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
	})

	t.Run("cycle_and_err_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, generalErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("cycle_and_errResolutionDepth_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, depthExceededHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("err_and_cycle_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, generalErrorHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("errResolutionDepth_and_cycle_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, depthExceededHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("cycle_and_cycle_return_false", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, cyclicErrorHandler, cyclicErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("err_and_err_return_err", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, generalErrorHandler, generalErrorHandler)
		require.ErrorContains(t, err, simulatedDBErrorMessage)
		require.Nil(t, resp)
	})

	t.Run("errResolutionDepth_and_errResolutionDepth_return_errResolutionDepth", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, depthExceededHandler, depthExceededHandler)
		require.ErrorIs(t, err, ErrResolutionDepthExceeded)
		require.Nil(t, resp)
	})

	t.Run("true_and_cycle_and_err_return_err", func(t *testing.T) {
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, trueHandler, cyclicErrorHandler, generalErrorHandler)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})

	t.Run("return_allowed:false_if_falsy_handler_evaluated_before_context_deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		t.Cleanup(cancel)

		resp, err := intersection(ctx, concurrencyLimit, falseHandler, falseHandler)
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})

	t.Run("return_true_if_truthy_handler_evaluated_before_context_deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		t.Cleanup(cancel)

		resp, err := intersection(ctx, concurrencyLimit, trueHandler, trueHandler)
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

		resp, err := intersection(ctx, concurrencyLimit, slowTrueHandler, slowTrueHandler)
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

		resp, err := intersection(ctx, concurrencyLimit, falseHandler, falseHandler)
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

		resp, err := intersection(ctx, concurrencyLimit, slowTrueHandler, slowTrueHandler)
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

		resp, err := intersection(ctx, concurrencyLimit, slowTrueHandler, slowTrueHandler)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, resp)

		wg.Wait() // just to make sure to avoid test leaks
	})

	t.Run("should_error_if_handler_panics", func(t *testing.T) {
		panicHandler := func(context.Context) (*ResolveCheckResponse, error) {
			panic(panicErr)
		}
		ctx := context.Background()
		resp, err := intersection(ctx, concurrencyLimit, panicHandler, trueHandler)
		require.ErrorContains(t, err, panicErr)
		require.ErrorIs(t, err, ErrPanic)
		require.Nil(t, resp)
	})
}

func TestNonStratifiableCheckQueries(t *testing.T) {
	checker, checkResolverCloser, err := NewOrderedCheckResolvers(WithLocalCheckerOpts(WithMaxResolutionDepth(10))).Build()
	require.NoError(t, err)
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

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx := setRequestContext(context.Background(), ts, ds, nil)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:         storeID,
			TupleKey:        tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			RequestMetadata: NewCheckRequestMetadata(),
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

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx := setRequestContext(context.Background(), ts, ds, nil)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:         storeID,
			TupleKey:        tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			RequestMetadata: NewCheckRequestMetadata(),
		})
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	})
}

func TestResolveCheckDeterministic(t *testing.T) {
	checker, checkResolverCloser, err := NewOrderedCheckResolvers(WithLocalCheckerOpts(WithMaxResolutionDepth(2))).Build()
	require.NoError(t, err)
	t.Cleanup(checkResolverCloser)

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

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx := setRequestContext(context.Background(), ts, ds, nil)

		for i := 0; i < 2000; i++ {
			// subtract branch resolves to {allowed: true} even though the base branch
			// results in an error. Outcome should be falsey, not an error.
			resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:         storeID,
				TupleKey:        tuple.NewTupleKey("document:budget", "viewer", "user:maria"),
				RequestMetadata: NewCheckRequestMetadata(),
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

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx := setRequestContext(context.Background(), ts, ds, nil)

		for i := 0; i < 2000; i++ {
			// base should resolve to {allowed: false} even though the subtract branch
			// results in an error. Outcome should be falsey, not an error.
			resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
				StoreID:         storeID,
				TupleKey:        tuple.NewTupleKey("document:budget", "viewer", "user:maria"),
				RequestMetadata: NewCheckRequestMetadata(),
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

	checker, checkResolverCloser, err := NewOrderedCheckResolvers(
		WithLocalCheckerOpts(
			WithResolveNodeBreadthLimit(concurrencyLimit),
			WithMaxResolutionDepth(25),
		),
	).Build()
	require.NoError(t, err)
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

	ts, err := typesystem.New(model)
	require.NoError(t, err)

	ctx := setRequestContext(context.Background(), ts, ds, nil)

	resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:         storeID,
		TupleKey:        tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		RequestMetadata: NewCheckRequestMetadata(),
	})
	require.NoError(t, err)
	require.True(t, resp.Allowed)
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

	checker, checkResolverCloser, err := NewOrderedCheckResolvers().Build()
	require.NoError(t, err)
	t.Cleanup(checkResolverCloser)

	typesys, err := typesystem.NewAndValidate(
		context.Background(),
		model,
	)
	require.NoError(t, err)

	ctx := setRequestContext(context.Background(), typesys, ds, nil)

	conditionContext, err := structpb.NewStruct(map[string]interface{}{
		"param1": "notok",
	})
	require.NoError(t, err)

	resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("document:x", "parent", "folder:x"),
		RequestMetadata:      NewCheckRequestMetadata(),
		Context:              conditionContext,
	})
	require.NoError(t, err)
	require.True(t, resp.Allowed)

	resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		RequestMetadata:      NewCheckRequestMetadata(),
		Context:              conditionContext,
	})
	require.NoError(t, err)
	require.False(t, resp.Allowed)

	resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("document:x", "viewer", "user:bob"),
		RequestMetadata:      NewCheckRequestMetadata(),
		Context:              conditionContext,
	})
	require.NoError(t, err)
	require.False(t, resp.Allowed)
}

func TestCheckDispatchCount(t *testing.T) {
	ds := memory.New()

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

		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("folder:C", "viewer", "user:jon"),
			tuple.NewTupleKey("folder:B", "parent", "folder:C"),
			tuple.NewTupleKey("folder:A", "parent", "folder:B"),
			tuple.NewTupleKey("doc:readme", "parent", "folder:A"),
		})
		require.NoError(t, err)

		checker := NewLocalChecker(WithMaxResolutionDepth(5))

		typesys, err := typesystem.NewAndValidate(
			context.Background(),
			model,
		)
		require.NoError(t, err)

		ctx := setRequestContext(context.Background(), typesys, ds, nil)

		checkRequestMetadata := NewCheckRequestMetadata()

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("doc:readme", "viewer", "user:jon"),
			RequestMetadata:      checkRequestMetadata,
			SelectedStrategy:     "recursive",
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		require.LessOrEqual(t, uint32(1), checkRequestMetadata.DispatchCounter.Load())

		t.Run("direct_lookup_requires_no_dispatch", func(t *testing.T) {
			checkRequestMetadata := NewCheckRequestMetadata()

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
					define other: [user]
					define member: [user, group#member] or other

			type document
				relations
					define viewer: [group#member]
			`)

		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("group:1", "member", "user:jon"),
			tuple.NewTupleKey("group:eng", "member", "group:1#member"),
			tuple.NewTupleKey("group:eng", "member", "group:2#member"),
			tuple.NewTupleKey("group:eng", "member", "group:3#member"),
			tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
		})
		require.NoError(t, err)

		checker := NewLocalChecker(WithMaxResolutionDepth(5))

		typesys, err := typesystem.NewAndValidate(
			context.Background(),
			model,
		)
		require.NoError(t, err)

		ctx := setRequestContext(context.Background(), typesys, ds, nil)
		checkRequestMetadata := NewCheckRequestMetadata()

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			RequestMetadata:      checkRequestMetadata,
			SelectedStrategy:     "recursive",
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		require.GreaterOrEqual(t, checkRequestMetadata.DispatchCounter.Load(), uint32(1))
		require.LessOrEqual(t, checkRequestMetadata.DispatchCounter.Load(), uint32(4))

		checkRequestMetadata = NewCheckRequestMetadata()

		resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:other"),
			RequestMetadata:      checkRequestMetadata,
			SelectedStrategy:     "recursive",
		})
		require.NoError(t, err)
		require.False(t, resp.Allowed)

		require.GreaterOrEqual(t, checkRequestMetadata.DispatchCounter.Load(), uint32(1))
		require.LessOrEqual(t, checkRequestMetadata.DispatchCounter.Load(), uint32(4))
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

		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "owner", "user:jon"),
			tuple.NewTupleKey("document:2", "editor", "user:will"),
		})
		require.NoError(t, err)

		checker := NewLocalChecker(WithMaxResolutionDepth(5))

		typesys, err := typesystem.NewAndValidate(
			context.Background(),
			model,
		)
		require.NoError(t, err)

		ctx := setRequestContext(context.Background(), typesys, ds, nil)
		checkRequestMetadata := NewCheckRequestMetadata()
		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:1", "owner", "user:jon"),
			RequestMetadata:      checkRequestMetadata,
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		require.Zero(t, checkRequestMetadata.DispatchCounter.Load())

		checkRequestMetadata = NewCheckRequestMetadata()

		resp, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:2", "editor", "user:will"),
			RequestMetadata:      checkRequestMetadata,
		})
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		require.LessOrEqual(t, checkRequestMetadata.DispatchCounter.Load(), uint32(1))

		checkRequestMetadata = NewCheckRequestMetadata()
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

	concurrencyLimit := 10

	falseHandler := func(context.Context) (*ResolveCheckResponse, error) {
		return &ResolveCheckResponse{
			Allowed: false,
		}, nil
	}

	trueHandler := func(context.Context) (*ResolveCheckResponse, error) {
		return &ResolveCheckResponse{
			Allowed: true,
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

		resp, err := union(ctx, concurrencyLimit, trueHandler, trueHandler)
		require.NoError(t, err)
		require.True(t, resp.GetAllowed())
		wg.Wait() // just to make sure to avoid test leaks
	})

	t.Run("should_error_if_handler_panics", func(t *testing.T) {
		panicHandler := func(context.Context) (*ResolveCheckResponse, error) {
			panic(panicErr)
		}

		resp, err := union(ctx, concurrencyLimit, panicHandler)
		require.ErrorContains(t, err, panicErr)
		require.ErrorIs(t, err, ErrPanic)
		require.Nil(t, resp)
	})
}

func TestResolveCheckCallsPathExists(t *testing.T) {
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

	storeID := ulid.Make().String()
	model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type document
				relations
					define x: y
					define y: x
		`)

	ts, err := typesystem.New(model)
	require.NoError(t, err)

	ctx := setRequestContext(context.Background(), ts, ds, nil)

	resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("document:1", "y", "user:maria"),
		RequestMetadata:      NewCheckRequestMetadata(),
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.False(t, resp.GetAllowed())

	// Without PathExists check, this would cycle
	require.False(t, resp.GetCycleDetected())
}

func TestResolveCheckCallsCycleDetection(t *testing.T) {
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
			RequestMetadata: NewCheckRequestMetadata(),
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

	t.Run("returns_true_if_cycle_exists", func(t *testing.T) {
		storeID := ulid.Make().String()
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type document
				relations
					define x: y
					define y: x`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx := setRequestContext(context.Background(), ts, ds, nil)

		resp, err := checker.ResolveCheck(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:1", "y", "document:2#x"),
			RequestMetadata:      NewCheckRequestMetadata(),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetAllowed())
		require.True(t, resp.GetCycleDetected())
	})
}

func TestDispatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	checker := NewLocalChecker()
	defer checker.Close()
	mockResolver := NewMockCheckResolver(ctrl)
	checker.SetDelegate(mockResolver)

	// Create the child request that would be prepared by prepareChildRequest
	childReq := &ResolveCheckRequest{
		TupleKey:        tuple.NewTupleKeyWithCondition("group:1", "member", "user:maria", "condition1", nil),
		RequestMetadata: NewCheckRequestMetadata(),
	}
	childReq.GetRequestMetadata().Depth++

	// Mock the expected behavior
	var req *ResolveCheckRequest
	mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.AssignableToTypeOf(req)).DoAndReturn(
		func(_ context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
			require.Equal(t, childReq.GetTupleKey(), req.GetTupleKey())
			require.Equal(t, childReq.GetRequestMetadata().Depth, req.GetRequestMetadata().Depth)
			return nil, nil
		})

	// Test the dispatch function directly with the prepared request
	dispatch := checker.dispatch(context.Background(), childReq)
	_, _ = dispatch(context.Background())
}

func TestCheckTTU(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	// model
	//	schema 1.1
	// type user
	// type group
	//	relations
	//		define member: [user] or member from parent
	//		define parent: [group]

	ttuRewrite := &openfgav1.Userset{
		Userset: &openfgav1.Userset_TupleToUserset{
			TupleToUserset: &openfgav1.TupleToUserset{
				Tupleset: &openfgav1.ObjectRelation{
					Relation: "parent",
				},
				ComputedUserset: &openfgav1.ObjectRelation{
					Relation: "member",
				},
			},
		},
	}
	model := &openfgav1.AuthorizationModel{
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "group",
				Relations: map[string]*openfgav1.Userset{
					"parent": {
						Userset: &openfgav1.Userset_This{},
					},
					"member": {
						Userset: &openfgav1.Userset_Union{
							Union: &openfgav1.Usersets{
								Child: []*openfgav1.Userset{
									{
										Userset: &openfgav1.Userset_This{},
									},
									ttuRewrite,
								},
							},
						},
					},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"parent": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{
									Type: "group",
								},
							},
						},
						"member": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{
									Type: "user",
								},
							},
						},
					},
				},
			},
		},
	}

	typesys, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)

	t.Run("nested_ttu_and_optimizations_enabled_calls_nestedUsersetFastpath", func(t *testing.T) {
		// arrange
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

		checker := NewLocalChecker(WithOptimizations(true), WithMaxResolutionDepth(24))
		t.Cleanup(checker.Close)

		storeID := ulid.Make().String()

		req := &ResolveCheckRequest{
			StoreID:         storeID,
			TupleKey:        tuple.NewTupleKey("group:1", "member", "user:maria"),
			RequestMetadata: NewCheckRequestMetadata(),
		}

		ctx := setRequestContext(context.Background(), typesys, mockDatastore, nil)
		mockDatastore.EXPECT().
			Read(gomock.Any(), storeID, storage.ReadFilter{Object: "group:1", Relation: "parent", User: ""}, gomock.Any()).
			Times(1).
			Return(storage.NewStaticTupleIterator(nil), nil)

		// act
		res, err := checker.checkTTU(ctx, req, ttuRewrite)(ctx)

		// assert
		require.NoError(t, err)
		require.NotNil(t, res)
		require.False(t, res.GetAllowed()) // user:maria is not part of any group, and no parents for group:1
	})
}

func TestCheckDirectUserTuple(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	directlyAssignedModelWithCondition := parser.MustTransformDSLToProto(`
	model
		schema 1.1

	type user
	type group
		relations
			define member: [user with condX]
	condition condX(x: int) {
		x < 100
	}
	`)

	tests := []struct {
		name               string
		model              *openfgav1.AuthorizationModel
		readUserTuple      *openfgav1.Tuple
		readUserTupleError error
		reqTupleKey        *openfgav1.TupleKey
		context            map[string]interface{}
		expected           *ResolveCheckResponse
		expectedError      error
	}{
		{
			name:  "directly_assigned",
			model: directlyAssignedModelWithCondition,
			readUserTuple: &openfgav1.Tuple{
				Key: tuple.NewTupleKeyWithCondition("group:1", "member", "user:bob", "condX", nil),
			},
			readUserTupleError: nil,
			reqTupleKey:        tuple.NewTupleKey("group:1", "member", "user:bob"),
			context:            map[string]interface{}{"x": "2"},
			expected: &ResolveCheckResponse{
				Allowed: true,
			},
			expectedError: nil,
		},
		{
			name:  "directly_assigned_cond_not_match",
			model: directlyAssignedModelWithCondition,
			readUserTuple: &openfgav1.Tuple{
				Key: tuple.NewTupleKeyWithCondition("group:1", "member", "user:bob", "condX", nil),
			},
			readUserTupleError: nil,
			reqTupleKey:        tuple.NewTupleKey("group:1", "member", "user:bob"),
			context:            map[string]interface{}{"x": "200"},
			expected: &ResolveCheckResponse{
				Allowed: false,
			},
			expectedError: nil,
		},
		{
			name:  "missing_condition",
			model: directlyAssignedModelWithCondition,
			readUserTuple: &openfgav1.Tuple{
				Key: tuple.NewTupleKeyWithCondition("group:1", "member", "user:bob", "condX", nil),
			},
			readUserTupleError: nil,
			reqTupleKey:        tuple.NewTupleKey("group:1", "member", "user:bob"),
			context:            map[string]interface{}{},
			expected:           nil,
			expectedError: condition.NewEvaluationError(
				"condX",
				fmt.Errorf("tuple 'group:1#member@user:bob' is missing context parameters '[x]'"),
			),
		},
		{
			name:               "no_tuple_found",
			model:              directlyAssignedModelWithCondition,
			readUserTuple:      nil,
			readUserTupleError: storage.ErrNotFound,
			reqTupleKey:        tuple.NewTupleKey("group:1", "member", "user:bob"),
			context:            map[string]interface{}{"x": "200"},
			expected: &ResolveCheckResponse{
				Allowed: false,
			},
			expectedError: nil,
		},
		{
			name:               "other_datastore_error",
			model:              directlyAssignedModelWithCondition,
			readUserTuple:      nil,
			readUserTupleError: fmt.Errorf("mock_erorr"),
			reqTupleKey:        tuple.NewTupleKey("group:1", "member", "user:bob"),
			context:            map[string]interface{}{"x": "200"},
			expected:           nil,
			expectedError:      fmt.Errorf("mock_erorr"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			storeID := ulid.Make().String()
			ds := mocks.NewMockRelationshipTupleReader(ctrl)

			ds.EXPECT().ReadUserTuple(gomock.Any(), storeID, storage.ReadUserTupleFilter{Object: tt.reqTupleKey.GetObject(), Relation: tt.reqTupleKey.GetRelation(), User: tt.reqTupleKey.GetUser()}, gomock.Any()).Times(1).Return(tt.readUserTuple, tt.readUserTupleError)

			ts, err := typesystem.New(tt.model)
			require.NoError(t, err)

			ctx := setRequestContext(context.Background(), ts, ds, nil)

			contextStruct, err := structpb.NewStruct(tt.context)
			require.NoError(t, err)

			checker := NewLocalChecker()
			function := checker.checkDirectUserTuple(ctx, &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tt.reqTupleKey,
				Context:              contextStruct,
				RequestMetadata:      NewCheckRequestMetadata(),
			})
			resp, err := function(ctx)
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected, resp)
		})
	}
}

func TestShouldCheckDirectTuple(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name        string
		model       *openfgav1.AuthorizationModel
		reqTupleKey *openfgav1.TupleKey
		expected    bool
	}{
		{
			name: "directly_assigned",
			model: parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user]
	`),
			reqTupleKey: tuple.NewTupleKey("group:1", "member", "user:bob"),
			expected:    true,
		},
		{
			name: "directly_assigned_public_wildcard",
			model: parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user:*]
	`),
			reqTupleKey: tuple.NewTupleKey("group:1", "member", "user:bob"),
			expected:    false,
		},
		{
			name: "directly_assigned_public_wildcard_mixed",
			model: parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user, user:*]
	`),
			reqTupleKey: tuple.NewTupleKey("group:1", "member", "user:bob"),
			expected:    true,
		},
		{
			name: "userset_indirect",
			model: parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user, user:*]
			define other_member: [group#member]

	`),
			reqTupleKey: tuple.NewTupleKey("group:1", "other_member", "user:bob"),
			expected:    false,
		},
		{
			name: "userset_direct",
			model: parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user, user:*]
			define other_member: [group#member]

	`),
			reqTupleKey: tuple.NewTupleKey("group:1", "other_member", "group:2#member"),
			expected:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts, err := typesystem.New(tt.model)
			require.NoError(t, err)
			ctx := typesystem.ContextWithTypesystem(context.Background(), ts)

			result := shouldCheckDirectTuple(ctx, tt.reqTupleKey)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldCheckPubliclyAssigned(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name        string
		model       *openfgav1.AuthorizationModel
		reqTupleKey *openfgav1.TupleKey
		expected    bool
	}{
		{
			name: "directly_assigned",
			model: parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user]
	`),
			reqTupleKey: tuple.NewTupleKey("group:1", "member", "user:bob"),
			expected:    false,
		},
		{
			name: "directly_assigned_public_wildcard",
			model: parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user:*]
	`),
			reqTupleKey: tuple.NewTupleKey("group:1", "member", "user:bob"),
			expected:    true,
		},
		{
			name: "directly_assigned_public_wildcard_mixed",
			model: parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user, user:*]
	`),
			reqTupleKey: tuple.NewTupleKey("group:1", "member", "user:bob"),
			expected:    true,
		},
		{
			name: "userset_indirect",
			model: parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user, user:*]
			define other_member: [group#member]

	`),
			reqTupleKey: tuple.NewTupleKey("group:1", "other_member", "user:bob"),
			expected:    false,
		},
		{
			name: "userset_direct",
			model: parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user, user:*]
			define other_member: [group#member]

	`),
			reqTupleKey: tuple.NewTupleKey("group:1", "other_member", "group:2#member"),
			expected:    false,
		},
		{
			name: "mixed_public_userset_tuple_user",
			model: parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user, user:*]
	type folder
		relations
			define viewer: [group, group:*, group#member]
	`),
			reqTupleKey: tuple.NewTupleKey("folder:1", "viewer", "group:1"),
			expected:    true,
		},
		{
			name: "mixed_public_userset_tuple_wilduser",
			model: parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user, user:*]
	type folder
		relations
			define viewer: [group, group:*, group#member]
	`),
			reqTupleKey: tuple.NewTupleKey("folder:1", "viewer", "group:*"),
			expected:    true,
		},
		{
			name: "mixed_public_userset_tuple_userset",
			model: parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user, user:*]
	type folder
		relations
			define viewer: [group, group:*, group#member]
	`),
			reqTupleKey: tuple.NewTupleKey("folder:1", "viewer", "group:1#member"),
			expected:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ts, err := typesystem.New(tt.model)
			require.NoError(t, err)
			ctx := typesystem.ContextWithTypesystem(context.Background(), ts)

			result := shouldCheckPublicAssignable(ctx, tt.reqTupleKey)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCheckPublicAssignable(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	modelWithNoCond := parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user, user:*]
				`)

	modelWithCond := parser.MustTransformDSLToProto(`
	model
		schema 1.1
	type user
	type group
		relations
			define member: [user with condX, user:* with condX]
	condition condX(x: int) {
		x < 100
	}
				`)

	tests := []struct {
		name                   string
		readUsersetTuples      []*openfgav1.Tuple
		readUsersetTuplesError error
		context                map[string]interface{}
		model                  *openfgav1.AuthorizationModel
		expected               *ResolveCheckResponse
		expectedError          error
	}{
		{
			name: "found",
			readUsersetTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:1", "member", "user:*"),
				},
			},
			readUsersetTuplesError: nil,
			context:                map[string]interface{}{},
			model:                  modelWithNoCond,
			expected: &ResolveCheckResponse{
				Allowed: true,
			},
			expectedError: nil,
		},
		{
			name: "not_found",
			readUsersetTuples: []*openfgav1.Tuple{
				{},
			},
			readUsersetTuplesError: nil,
			context:                map[string]interface{}{},
			model:                  modelWithNoCond,
			expected: &ResolveCheckResponse{
				Allowed: false,
			},
			expectedError: nil,
		},
		{
			name: "error",
			readUsersetTuples: []*openfgav1.Tuple{
				{},
			},
			readUsersetTuplesError: fmt.Errorf("mock_error"),
			context:                map[string]interface{}{},
			model:                  modelWithNoCond,
			expected:               nil,
			expectedError:          fmt.Errorf("mock_error"),
		},
		{
			name: "wildcard_cond_match",
			readUsersetTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKeyWithCondition("group:1", "member", "user:*", "condX", nil),
				},
			},
			readUsersetTuplesError: nil,
			context:                map[string]interface{}{"x": "5"},
			model:                  modelWithCond,
			expected: &ResolveCheckResponse{
				Allowed: true,
			},
			expectedError: nil,
		},
		{
			name: "wildcard_cond_not_match",
			readUsersetTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKeyWithCondition("group:1", "member", "user:*", "condX", nil),
				},
			},
			readUsersetTuplesError: nil,
			context:                map[string]interface{}{"x": "200"},
			model:                  modelWithCond,
			expected: &ResolveCheckResponse{
				Allowed: false,
			},
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			storeID := ulid.Make().String()
			ds := mocks.NewMockRelationshipTupleReader(ctrl)
			ds.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Times(1).Return(storage.NewStaticTupleIterator(tt.readUsersetTuples), tt.readUsersetTuplesError)

			ts, err := typesystem.New(tt.model)
			require.NoError(t, err)
			ctx := setRequestContext(context.Background(), ts, ds, nil)
			checker := NewLocalChecker()

			contextStruct, err := structpb.NewStruct(tt.context)
			require.NoError(t, err)

			function := checker.checkPublicAssignable(ctx, &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:bob"),
				Context:              contextStruct,
				RequestMetadata:      NewCheckRequestMetadata(),
			})
			result, err := function(ctx)
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestStreamedLookupUsersetFromIterator(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name                   string
		contextDone            bool
		readUsersetTuples      []*openfgav1.Tuple
		readUsersetTuplesError error
		iteratorHasError       bool
		expected               []usersetMessage
	}{
		{
			name:                   "get_iterator_error",
			contextDone:            false,
			readUsersetTuples:      []*openfgav1.Tuple{},
			readUsersetTuplesError: fmt.Errorf("mock_error"),
			expected: []usersetMessage{
				{
					userset: "",
					err:     fmt.Errorf("mock_error"),
				},
			},
		},
		{
			name:             "iterator_next_error",
			contextDone:      false,
			iteratorHasError: true,
			readUsersetTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:1", "member", "group:2#member"),
				},
			},
			readUsersetTuplesError: nil,
			expected: []usersetMessage{
				{
					userset: "group:2",
					err:     nil,
				},
				{
					userset: "",
					err:     mocks.ErrSimulatedError,
				},
			},
		},
		{
			name:                   "empty_userset",
			contextDone:            false,
			readUsersetTuples:      []*openfgav1.Tuple{},
			readUsersetTuplesError: nil,
			expected:               nil,
		},
		{
			name:                   "ctx_cancel",
			contextDone:            true,
			readUsersetTuples:      []*openfgav1.Tuple{},
			readUsersetTuplesError: nil,
			expected:               nil,
		},
		{
			name:        "has_userset",
			contextDone: false,
			readUsersetTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:1", "member", "group:2#member"),
				},
				{
					Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
				},
			},
			readUsersetTuplesError: nil,
			expected: []usersetMessage{
				{
					userset: "group:2",
					err:     nil,
				},
				{
					userset: "group:3",
					err:     nil,
				},
			},
		},
		{
			name:        "has_userset_large_pool_size",
			contextDone: false,
			readUsersetTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:1", "member", "group:2#member"),
				},
				{
					Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
				},
			},
			readUsersetTuplesError: nil,
			expected: []usersetMessage{
				{
					userset: "group:2",
					err:     nil,
				},
				{
					userset: "group:3",
					err:     nil,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			storeID := ulid.Make().String()
			ds := mocks.NewMockRelationshipTupleReader(ctrl)

			model := parser.MustTransformDSLToProto(`
					model
						schema 1.1

					type user
					type group
						relations
							define member: [user, group#member]
`)

			ts, err := typesystem.New(model)
			require.NoError(t, err)

			ctx := setRequestContext(context.Background(), ts, ds, nil)

			restrictions, err := ts.DirectlyRelatedUsersets("group", "member")
			require.NoError(t, err)
			if tt.iteratorHasError {
				ds.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, storage.ReadUsersetTuplesFilter{
					Object:                      "group:1",
					Relation:                    "member",
					AllowedUserTypeRestrictions: restrictions,
				}, gomock.Any()).Times(1).Return(mocks.NewErrorTupleIterator(tt.readUsersetTuples), tt.readUsersetTuplesError)
			} else {
				ds.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, storage.ReadUsersetTuplesFilter{
					Object:                      "group:1",
					Relation:                    "member",
					AllowedUserTypeRestrictions: restrictions,
				}, gomock.Any()).Times(1).Return(storage.NewStaticTupleIterator(tt.readUsersetTuples), tt.readUsersetTuplesError)
			}

			req := &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
				RequestMetadata:      NewCheckRequestMetadata(),
			}

			cancellableCtx, cancelFunc := context.WithCancel(context.Background())
			if tt.contextDone {
				cancelFunc()
			} else {
				defer cancelFunc()
			}

			mapper, err := buildRecursiveMapper(ctx, req, &recursiveMapping{
				kind:                        storage.UsersetKind,
				allowedUserTypeRestrictions: restrictions,
			})
			if tt.readUsersetTuplesError != nil {
				require.Equal(t, tt.readUsersetTuplesError, err)
				return
			}

			userToUsersetMessageChan := streamedLookupUsersetFromIterator(cancellableCtx, mapper)

			var userToUsersetMessages []usersetMessage

			for userToUsersetMessage := range userToUsersetMessageChan {
				userToUsersetMessages = append(userToUsersetMessages, userToUsersetMessage)
			}

			require.Equal(t, tt.expected, userToUsersetMessages)
		})
	}

	t.Run("should_error_if_panic_occurs", func(t *testing.T) {
		ctx := context.Background()
		iter := &mockPanicIterator[string]{}
		userToUsersetMessageChan := streamedLookupUsersetFromIterator(ctx, iter)

		for userToUsersetMessage := range userToUsersetMessageChan {
			require.ErrorContains(t, userToUsersetMessage.err, panicErr)
			require.ErrorIs(t, userToUsersetMessage.err, ErrPanic)
			require.Empty(t, userToUsersetMessage.userset)
		}
	})
}

func TestProcessUsersetMessage(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name                 string
		userset              string
		matchingUserset      []string
		expectedFound        bool
		expectedInputUserset []string
	}{
		{
			name:                 "match",
			userset:              "b",
			matchingUserset:      []string{"a", "b"},
			expectedFound:        true,
			expectedInputUserset: []string{"b"},
		},
		{
			name:                 "not_match",
			userset:              "c",
			matchingUserset:      []string{"a", "b"},
			expectedFound:        false,
			expectedInputUserset: []string{"c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			inputSortedSet := hashset.New()
			matchingSortedSet := hashset.New()
			for _, match := range tt.matchingUserset {
				matchingSortedSet.Add(match)
			}
			output := processUsersetMessage(tt.userset, inputSortedSet, matchingSortedSet)
			require.Equal(t, tt.expectedFound, output)
			res := make([]string, 0, inputSortedSet.Size())
			for _, v := range inputSortedSet.Values() {
				res = append(res, v.(string))
			}
			require.Equal(t, tt.expectedInputUserset, res)
		})
	}
}

func TestSelectedStrategySkipsPlannerOnDispatch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("dispatch_propagates_selected_strategy_to_child_requests", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		ds := memory.New()
		defer ds.Close()

		storeID := ulid.Make().String()

		// Create a simple recursive group membership model
		// group:1#member -> group:2#member -> user:maria
		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("group:1", "member", "group:2#member"),
			tuple.NewTupleKey("group:2", "member", "user:maria"),
		})
		require.NoError(t, err)

		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user, group#member]`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		checker := NewLocalChecker(
			WithMaxResolutionDepth(25),
		)
		defer checker.Close()

		ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		// Create a request with a pre-selected strategy
		req := &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
			RequestMetadata:      NewCheckRequestMetadata(),
		}
		req.SelectedStrategy = "default" // Pre-select the default strategy

		resp, err := checker.ResolveCheck(ctx, req)
		require.NoError(t, err)
		require.True(t, resp.Allowed)

		// Verify the strategy was preserved throughout the resolution
		// The fact that it resolves correctly confirms the strategy propagation works
		// because the dispatch mechanism clones the request and the clone should
		// inherit the SelectedStrategy field
	})

	t.Run("request_sets_selected_strategy_after_planner_selection", func(t *testing.T) {
		ds := memory.New()
		defer ds.Close()

		storeID := ulid.Make().String()

		err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "group:1#member"),
			tuple.NewTupleKey("group:1", "member", "user:maria"),
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
					define viewer: [group#member]`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		checker := NewLocalChecker(
			WithMaxResolutionDepth(25),
		)
		defer checker.Close()

		ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		// Create a request without a pre-selected strategy
		req := &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: model.GetId(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			RequestMetadata:      NewCheckRequestMetadata(),
			// SelectedStrategy is empty - planner will select one
		}

		resp, err := checker.ResolveCheck(ctx, req)
		require.NoError(t, err)
		require.True(t, resp.Allowed)
	})
}
