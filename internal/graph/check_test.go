package graph

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openfga/openfga/internal/checkutil"

	"github.com/openfga/openfga/internal/mocks"

	"github.com/openfga/openfga/internal/concurrency"
	serverconfig "github.com/openfga/openfga/internal/server/config"

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

// usersetsChannelStruct is a helper data structure to allow initializing objectIDs with slices.
type usersetsChannelStruct struct {
	err            error
	objectRelation string
	objectIDs      []string
}

func usersetsChannelFromUsersetsChannelStruct(orig []usersetsChannelStruct) []usersetsChannelType {
	output := make([]usersetsChannelType, len(orig))
	for i, result := range orig {
		output[i] = usersetsChannelType{
			err:            result.err,
			objectRelation: result.objectRelation,
			objectIDs:      storage.NewSortedSet(),
		}
		for _, objectID := range result.objectIDs {
			output[i].objectIDs.Add(objectID)
		}
	}
	return output
}

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
		ts, err := typesystem.New(model)
		require.NoError(t, err)
		ctx := typesystem.ContextWithTypesystem(context.Background(), ts)

		_, err = checker.ResolveCheck(ctx, &ResolveCheckRequest{
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

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			ts,
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

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			ts,
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
					define other: [user]
					define member: [user, group#member] or other

			type document
				relations
					define allowed: [user]
					define viewer: [group#member] or editor
					define editor: [group#member] and allowed`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			ts,
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

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			ts,
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

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			ts,
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

	ts, err := typesystem.New(model)
	require.NoError(t, err)

	ctx := typesystem.ContextWithTypesystem(
		context.Background(),
		ts,
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

	ts, err := typesystem.New(model)
	require.NoError(t, err)

	ctx := typesystem.ContextWithTypesystem(
		context.Background(),
		ts,
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
			maxDBReads: 4,
		},
		{
			name:       "multiple_userset_access",
			check:      tuple.NewTupleKey("document:x", "multiple_userset", "user:maria"),
			allowed:    true,
			minDBReads: 2, // 2 userset read (2 found) follow by 2 direct tuple check (found, returns immediately)
			maxDBReads: 4,
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
			minDBReads: 2, // min(union (2 reads), ttu (2 read))
			maxDBReads: 4, // max(union (2 reads), ttu (2 read))
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
					define other: [user]
					define member: [user, group#member] or other

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

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx := typesystem.ContextWithTypesystem(
			context.Background(),
			ts,
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

func TestProduceUsersets(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	filter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
		if tupleKey.GetCondition().GetName() == "condition1" {
			return true, nil
		}
		return false, fmt.Errorf("condition not found")
	}

	tests := []struct {
		name                  string
		tuples                []*openfgav1.TupleKey
		usersetDetails        checkutil.UsersetDetailsFunc
		usersetBatchSize      uint32
		usersetsChannelResult []usersetsChannelStruct
	}{
		{
			name:   "no_tuple_match",
			tuples: []*openfgav1.TupleKey{},
			usersetDetails: func(*openfgav1.TupleKey) (string, string, error) {
				return "", "", fmt.Errorf("do not expect any tuples")
			},
			usersetBatchSize:      serverconfig.DefaultUsersetBatchSize,
			usersetsChannelResult: []usersetsChannelStruct{},
		},
		{
			name: "single_tuple_match",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:2#member", "condition1", nil),
			},
			usersetDetails: func(t *openfgav1.TupleKey) (string, string, error) {
				if t.GetObject() != "document:doc1" || t.GetRelation() != "viewer" || t.GetUser() != "group:2#member" {
					return "", "", fmt.Errorf("do not expect  tuples %v", t.String())
				}
				return "group#member", "2", nil
			},
			usersetBatchSize: serverconfig.DefaultUsersetBatchSize,
			usersetsChannelResult: []usersetsChannelStruct{
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"2"},
				},
			},
		},
		{
			name: "error_in_iterator",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:2#member", "error_iterator", nil),
			},
			usersetDetails: func(t *openfgav1.TupleKey) (string, string, error) {
				if t.GetObject() != "document:doc1" || t.GetRelation() != "viewer" || t.GetUser() != "group:2#member" {
					return "", "", fmt.Errorf("do not expect  tuples %v", t.String())
				}
				return "group#member", "2", nil
			},
			usersetBatchSize: serverconfig.DefaultUsersetBatchSize,
			usersetsChannelResult: []usersetsChannelStruct{
				{
					err:            fmt.Errorf("condition not found"),
					objectRelation: "",
					objectIDs:      []string{""},
				},
			},
		},
		{
			name: "multi_items",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:1#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:2#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:3#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:4#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:5#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:6#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:7#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:8#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:9#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:10#member", "condition1", nil),
			},
			usersetDetails: func(t *openfgav1.TupleKey) (string, string, error) {
				if t.GetObject() != "document:doc1" || t.GetRelation() != "viewer" {
					return "", "", fmt.Errorf("do not expect  tuples %v", t.String())
				}
				objectIDWithType, _ := tuple.SplitObjectRelation(t.GetUser())
				_, objectID := tuple.SplitObject(objectIDWithType)
				return "group#member", objectID, nil
			},
			usersetBatchSize: serverconfig.DefaultUsersetBatchSize,
			usersetsChannelResult: []usersetsChannelStruct{
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"},
				},
			},
		},
		{
			name: "multi_items_greater_than_batch_size",

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:1#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:2#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:3#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:4#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:5#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:6#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:7#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:8#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:9#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:10#member", "condition1", nil),
			},
			usersetDetails: func(t *openfgav1.TupleKey) (string, string, error) {
				if t.GetObject() != "document:doc1" || t.GetRelation() != "viewer" {
					return "", "", fmt.Errorf("do not expect  tuples %v", t.String())
				}
				objectIDWithType, _ := tuple.SplitObjectRelation(t.GetUser())
				_, objectID := tuple.SplitObject(objectIDWithType)
				return "group#member", objectID, nil
			},
			usersetBatchSize: 3,
			usersetsChannelResult: []usersetsChannelStruct{
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"1", "2", "3"},
				},
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"4", "5", "6"},
				},
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"7", "8", "9"},
				},
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"10"},
				},
			},
		},
		{
			name: "mixture_type",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:1#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:2#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:3#owner", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:4#owner", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:5#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:6#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:7#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:8#owner", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:9#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "group:10#member", "condition1", nil),
			},
			usersetDetails: func(t *openfgav1.TupleKey) (string, string, error) {
				if t.GetObject() != "document:doc1" || t.GetRelation() != "viewer" {
					return "", "", fmt.Errorf("do not expect  tuples %v", t.String())
				}
				objectIDWithType, rel := tuple.SplitObjectRelation(t.GetUser())
				objectType, objectID := tuple.SplitObject(objectIDWithType)
				return objectType + "#" + rel, objectID, nil
			},
			usersetBatchSize: serverconfig.DefaultUsersetBatchSize,
			usersetsChannelResult: []usersetsChannelStruct{

				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"1", "2"},
				},
				{
					err:            nil,
					objectRelation: "group#owner",
					objectIDs:      []string{"3", "4"},
				},
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"5", "6", "7"},
				},
				{
					err:            nil,
					objectRelation: "group#owner",
					objectIDs:      []string{"8"},
				},
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"9", "10"},
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			expectedUsersetsChannelResult := usersetsChannelFromUsersetsChannelStruct(tt.usersetsChannelResult)

			iter := storage.NewConditionsFilteredTupleKeyIterator(storage.NewStaticTupleKeyIterator(tt.tuples), filter)

			localChecker := NewLocalChecker(WithUsersetBatchSize(tt.usersetBatchSize))
			usersetsChan := make(chan usersetsChannelType)

			// sending to channel in batches up to a pre-configured value to subsequently checkMembership for.
			pool := concurrency.NewPool(context.Background(), 2)

			pool.Go(func(ctx context.Context) error {
				localChecker.produceUsersets(ctx, usersetsChan, iter, tt.usersetDetails)
				return nil
			})
			var results []usersetsChannelType
			pool.Go(func(ctx context.Context) error {
				for {
					select {
					case <-ctx.Done():
						return nil
					case newBatch, channelOpen := <-usersetsChan:
						if !channelOpen {
							return nil
						}
						results = append(results, usersetsChannelType{
							err:            newBatch.err,
							objectRelation: newBatch.objectRelation,
							objectIDs:      newBatch.objectIDs,
						})
					}
				}
			})
			err := pool.Wait()
			require.NoError(t, err)
			require.Len(t, results, len(expectedUsersetsChannelResult))
			for idx, result := range results {
				require.Equal(t, expectedUsersetsChannelResult[idx].err, result.err)
				if expectedUsersetsChannelResult[idx].err == nil {
					require.Equal(t, expectedUsersetsChannelResult[idx].objectRelation, result.objectRelation)
					require.EqualValues(t, expectedUsersetsChannelResult[idx].objectIDs.Values(), result.objectIDs.Values())
				}
			}
		})
	}
}

func TestCheckAssociatedObjects(t *testing.T) {
	tests := []struct {
		name                         string
		model                        *openfgav1.AuthorizationModel
		tuples                       []*openfgav1.Tuple
		context                      map[string]interface{}
		dsError                      error
		objectIDs                    []string
		expectedError                bool
		expectedResolveCheckResponse *ResolveCheckResponse
	}{
		{
			name: "empty_iterator",
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [group#member]`),
			tuples:  []*openfgav1.Tuple{},
			dsError: nil,
			objectIDs: []string{
				"2", "3",
			},
			context: map[string]interface{}{},
			expectedResolveCheckResponse: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
				},
			},
			expectedError: false,
		},
		{
			name: "empty_object_ids",
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [group#member]`),
			tuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:1", "member", "user:maria"),
				},
			},
			dsError:   nil,
			objectIDs: []string{},
			context:   map[string]interface{}{},
			expectedResolveCheckResponse: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
				},
			},
			expectedError: false,
		},
		{
			name: "bad_ds_call",
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [group#member]`),
			tuples:                       []*openfgav1.Tuple{},
			dsError:                      fmt.Errorf("bad_ds_call"),
			objectIDs:                    []string{"1"},
			context:                      map[string]interface{}{},
			expectedResolveCheckResponse: nil,
			expectedError:                true,
		},
		{
			name: "non_empty_iterator_match_objectIDs",
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [group#member]`),
			tuples: []*openfgav1.Tuple{
				{Key: tuple.NewTupleKey("group:1", "member", "user:maria")},
			},
			dsError: nil,
			objectIDs: []string{
				"1",
			},
			context: map[string]interface{}{},
			expectedResolveCheckResponse: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
				},
			},
			expectedError: false,
		},
		{
			name: "non_empty_iterator_match_objectIDs_ttu",
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define owner: [group]
						define viewer: member from owner`),
			tuples: []*openfgav1.Tuple{
				{Key: tuple.NewTupleKey("group:1", "member", "user:maria")},
			},
			dsError: nil,
			objectIDs: []string{
				"1",
			},
			context: map[string]interface{}{},
			expectedResolveCheckResponse: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
				},
			},
			expectedError: false,
		},
		{
			name: "non_empty_iterator_not_match_objectIDs",
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [group#member]`),
			tuples: []*openfgav1.Tuple{
				{Key: tuple.NewTupleKey("group:1", "member", "user:maria")},
			},
			dsError:   nil,
			objectIDs: []string{"8"},
			context:   map[string]interface{}{},
			expectedResolveCheckResponse: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
				},
			},
			expectedError: false,
		},
		{
			name: "non_empty_iterator_match_cond_not_match",
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user with condX]
				type document
					relations
						define viewer: [group#member]

				condition condX(x: int) {
					x < 100
				}
`),
			tuples: []*openfgav1.Tuple{
				{Key: tuple.NewTupleKeyWithCondition("group:1", "member", "user:maria", "condX", nil)},
			},
			dsError:   nil,
			objectIDs: []string{"1"},
			context: map[string]interface{}{
				"x": 200,
			},
			expectedResolveCheckResponse: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
				},
			},
			expectedError: false,
		},
		{
			name: "non_empty_iterator_match_cond_match",
			model: parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user with condX]
				type document
					relations
						define viewer: [group#member]

				condition condX(x: int) {
					x < 100
				}
`),
			tuples: []*openfgav1.Tuple{
				{Key: tuple.NewTupleKeyWithCondition("group:1", "member", "user:maria", "condX", nil)},
			},
			dsError:   nil,
			objectIDs: []string{"1"},
			context: map[string]interface{}{
				"x": 10,
			},
			expectedResolveCheckResponse: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
				},
			},
			expectedError: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			objectIDs := storage.NewSortedSet()
			for _, objectID := range tt.objectIDs {
				objectIDs.Add(objectID)
			}

			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)

			ds := mocks.NewMockRelationshipTupleReader(ctrl)
			ds.EXPECT().ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(storage.NewStaticTupleIterator(tt.tuples), tt.dsError)

			ts, err := typesystem.New(tt.model)
			require.NoError(t, err)
			ctx := context.Background()
			ctx = typesystem.ContextWithTypesystem(ctx, ts)
			ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

			contextStruct, err := structpb.NewStruct(tt.context)
			require.NoError(t, err)

			result, err := checkAssociatedObjects(ctx, &ResolveCheckRequest{
				StoreID:              ulid.Make().String(),
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
				RequestMetadata:      NewCheckRequestMetadata(20),
				Context:              contextStruct,
			}, "group#member", objectIDs)
			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expectedResolveCheckResponse, result)
		})
	}
}

func TestConsumeUsersets(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	model := parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
				type document
					relations
						define viewer: [group#member]`)
	type dsResults struct {
		tuples []*openfgav1.Tuple
		err    error
	}
	tests := []struct {
		name                         string
		tuples                       []dsResults
		usersetsChannelResult        []usersetsChannelStruct
		ctxCancelled                 bool
		expectedResolveCheckResponse *ResolveCheckResponse
		errorExpected                error
	}{
		{
			name: "userset_tuple_found",
			tuples: []dsResults{
				{
					tuples: []*openfgav1.Tuple{
						{Key: tuple.NewTupleKey("group:2", "member", "user:maria")},
					},
					err: nil,
				},
			},
			usersetsChannelResult: []usersetsChannelStruct{
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"2"},
				},
			},
			ctxCancelled: false,
			expectedResolveCheckResponse: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
				},
			},
			errorExpected: nil,
		},
		{
			name: "userset_tuple_found_multi_batches",
			tuples: []dsResults{
				// we expect 3 ds.ReadStartingWithUser to be called in response to 3 batches from usersetsChannel
				{
					tuples: []*openfgav1.Tuple{
						{Key: tuple.NewTupleKey("group:11", "member", "user:maria")},
					},
					err: nil,
				},
				{
					tuples: []*openfgav1.Tuple{
						{Key: tuple.NewTupleKey("group:11", "member", "user:maria")},
					},
					err: nil,
				},
				{
					tuples: []*openfgav1.Tuple{
						{Key: tuple.NewTupleKey("group:11", "member", "user:maria")},
					},
					err: nil,
				},
			},
			usersetsChannelResult: []usersetsChannelStruct{
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"1"},
				},
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"2"},
				},
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"11"},
				},
			},
			ctxCancelled: false,
			expectedResolveCheckResponse: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1, // since order is not guaranteed the allowed might come from the first answer
				},
			},
			errorExpected: nil,
		},
		{
			name: "userset_tuple_not_found",
			tuples: []dsResults{
				{
					tuples: []*openfgav1.Tuple{},
					err:    nil,
				},
			},
			usersetsChannelResult: []usersetsChannelStruct{
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"5"},
				},
			},
			ctxCancelled: false,
			expectedResolveCheckResponse: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
				},
			},
			errorExpected: nil,
		},
		{
			name: "userset_tuple_not_found_multiset",
			tuples: []dsResults{
				{
					tuples: []*openfgav1.Tuple{},
					err:    nil,
				},
				{
					tuples: []*openfgav1.Tuple{},
					err:    nil,
				},
				{
					tuples: []*openfgav1.Tuple{},
					err:    nil,
				},
			},
			usersetsChannelResult: []usersetsChannelStruct{
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"5"},
				},
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"6"},
				},
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"7"},
				},
			},
			ctxCancelled: false,
			expectedResolveCheckResponse: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 3,
				},
			},
			errorExpected: nil,
		},
		{
			name:                         "ctx_cancelled",
			tuples:                       []dsResults{},
			usersetsChannelResult:        []usersetsChannelStruct{},
			ctxCancelled:                 true,
			expectedResolveCheckResponse: nil,
			errorExpected:                context.Canceled,
		},
		{
			name: "iterator_error_first_batch",
			tuples: []dsResults{
				{
					tuples: []*openfgav1.Tuple{},
					err:    fmt.Errorf("mock_error"),
				},
			},
			usersetsChannelResult: []usersetsChannelStruct{
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"10"},
				},
			},
			ctxCancelled:                 false,
			expectedResolveCheckResponse: nil,
			errorExpected:                fmt.Errorf("mock_error"),
		},
		{
			name: "iterator_error_first_batch_and_second_batch",
			tuples: []dsResults{
				{
					tuples: []*openfgav1.Tuple{},
					err:    fmt.Errorf("mock_error"),
				},
				{
					tuples: []*openfgav1.Tuple{},
					err:    fmt.Errorf("mock_error"),
				},
			},
			usersetsChannelResult: []usersetsChannelStruct{
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"10"},
				},
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"13"},
				},
			},
			ctxCancelled:                 false,
			expectedResolveCheckResponse: nil,
			errorExpected:                fmt.Errorf("mock_error"),
		},
		{
			name: "iterator_error_first_batch_but_success_second",
			tuples: []dsResults{
				{
					tuples: []*openfgav1.Tuple{},
					err:    fmt.Errorf("mock_error"),
				},
				{
					tuples: []*openfgav1.Tuple{
						{Key: tuple.NewTupleKey("group:11", "member", "user:maria")},
					},
					err: nil,
				},
			},
			usersetsChannelResult: []usersetsChannelStruct{
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"1"},
				},
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"11"},
				},
			},
			ctxCancelled: false,
			expectedResolveCheckResponse: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
				},
			},
			errorExpected: nil,
		},
		{
			name: "iterator_error_first_batch_but_not_found_second",
			tuples: []dsResults{
				{
					tuples: []*openfgav1.Tuple{},
					err:    fmt.Errorf("mock_error"),
				},
				{
					tuples: []*openfgav1.Tuple{},
					err:    nil,
				},
			},
			usersetsChannelResult: []usersetsChannelStruct{
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"1"},
				},
				{
					err:            nil,
					objectRelation: "group#member",
					objectIDs:      []string{"4"},
				},
			},
			ctxCancelled:                 false,
			expectedResolveCheckResponse: nil,
			errorExpected:                fmt.Errorf("mock_error"),
		},
		{
			name:   "userset_chan_error",
			tuples: []dsResults{},
			usersetsChannelResult: []usersetsChannelStruct{
				{
					err:            fmt.Errorf("mock_error"),
					objectRelation: "group#member",
					objectIDs:      []string{"0", "2", "8"},
				},
			},
			ctxCancelled:                 false,
			expectedResolveCheckResponse: nil,
			errorExpected:                fmt.Errorf("mock_error"),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			checker := NewLocalChecker()
			t.Cleanup(checker.Close)

			ctrl := gomock.NewController(t)
			t.Cleanup(ctrl.Finish)
			ds := mocks.NewMockRelationshipTupleReader(ctrl)

			for _, curTuples := range tt.tuples {
				// Note that we need to return a new iterator for each DS call
				ds.EXPECT().ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(1).Return(
					storage.NewStaticTupleIterator(curTuples.tuples), curTuples.err)
			}

			ts, err := typesystem.New(model)
			require.NoError(t, err)
			var ctx context.Context
			var cancel context.CancelFunc
			ctx = context.Background()
			if tt.ctxCancelled {
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			ctx = typesystem.ContextWithTypesystem(ctx, ts)
			ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

			usersetsChannelItems := usersetsChannelFromUsersetsChannelStruct(tt.usersetsChannelResult)

			usersetChan := make(chan usersetsChannelType)
			pool := concurrency.NewPool(context.Background(), 1)
			pool.Go(func(ctx context.Context) error {
				for _, item := range usersetsChannelItems {
					usersetChan <- item
				}
				close(usersetChan)
				return nil
			})

			result, err := checker.consumeUsersets(ctx, &ResolveCheckRequest{
				StoreID:              ulid.Make().String(),
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:maria"),
				RequestMetadata:      NewCheckRequestMetadata(20),
			}, usersetChan)

			require.NoError(t, pool.Wait())
			require.Equal(t, tt.errorExpected, err)
			if tt.errorExpected == nil {
				require.Equal(t, tt.expectedResolveCheckResponse.Allowed, result.Allowed)
				require.Equal(t, tt.expectedResolveCheckResponse.GetCycleDetected(), result.GetCycleDetected())
				require.LessOrEqual(t, tt.expectedResolveCheckResponse.GetResolutionMetadata().DatastoreQueryCount, result.GetResolutionMetadata().DatastoreQueryCount)
			}
		})
	}
}

func TestDispatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	checker := NewLocalChecker()
	defer checker.Close()
	mockResolver := NewMockCheckResolver(ctrl)
	checker.SetDelegate(mockResolver)

	parentReq := &ResolveCheckRequest{
		TupleKey:        tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:maria", "condition1", nil),
		RequestMetadata: NewCheckRequestMetadata(20),
	}
	tk := tuple.NewTupleKeyWithCondition("group:1", "member", "user:maria", "condition1", nil)

	expectedReq := &ResolveCheckRequest{
		TupleKey:        tuple.NewTupleKeyWithCondition("group:1", "member", "user:maria", "condition1", nil),
		RequestMetadata: NewCheckRequestMetadata(19),
	}

	var req *ResolveCheckRequest
	mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.AssignableToTypeOf(req)).DoAndReturn(
		func(_ context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
			require.Equal(t, expectedReq.GetTupleKey(), req.GetTupleKey())
			require.Equal(t, expectedReq.GetRequestMetadata().Depth, req.GetRequestMetadata().Depth)
			require.Equal(t, uint32(1), req.GetRequestMetadata().DispatchCounter.Load())
			return nil, nil
		})
	dispatch := checker.dispatch(context.Background(), parentReq, tk)
	_, _ = dispatch(context.Background())
}

func collectMessagesFromChannel(dispatchChan chan dispatchMsg) []dispatchMsg {
	var receivedDispatches []dispatchMsg
	for msg := range dispatchChan {
		receivedDispatches = append(receivedDispatches, msg)
	}
	return receivedDispatches
}

func TestProduceUsersetDispatches(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	filter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
		if tupleKey.GetCondition().GetName() == "condition1" {
			return true, nil
		}
		return false, fmt.Errorf("condition not found")
	}

	// model does not matter for this unit test.  All we care about is schema 1.1+.
	model := parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user with condX, group#member]
				type document
					relations
						define viewer: [group#member]

				condition condX(x: int) {
					x < 100
				}`)
	ts, err := typesystem.New(model)
	require.NoError(t, err)
	ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
	req := &ResolveCheckRequest{
		TupleKey:        tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:maria", "condition1", nil),
		RequestMetadata: NewCheckRequestMetadata(20),
	}

	tests := []struct {
		name               string
		tuples             []*openfgav1.TupleKey
		expectedDispatches []dispatchMsg
	}{
		{
			name:               "empty_iterator",
			tuples:             nil,
			expectedDispatches: nil,
		},
		{
			name: "iterator_error_first_tuple",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("group:1", "member", "user:maria", "badCondition", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:            fmt.Errorf("condition not found"),
					shortCircuit:   false,
					dispatchParams: nil,
				},
			},
		},
		{
			name: "good_condition_wildcard",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("group:1", "member", "user:*", "condition1", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:            nil,
					shortCircuit:   true,
					dispatchParams: nil,
				},
			},
		},
		{
			name: "good_condition_non_wildcard",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("group:1", "member", "group:2#member", "condition1", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:2", "member", "user:maria"),
					},
				},
			},
		},
		{
			name: "multiple_tuples",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("group:1", "member", "group:2#member", "condition1", nil),
				tuple.NewTupleKeyWithCondition("group:1", "member", "group:3#member", "condition1", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:2", "member", "user:maria"),
					},
				},
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:3", "member", "user:maria"),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			iter := storage.NewConditionsFilteredTupleKeyIterator(storage.NewStaticTupleKeyIterator(tt.tuples), filter)
			checker := NewLocalChecker()
			defer checker.Close()
			mockResolver := NewMockCheckResolver(ctrl)
			checker.SetDelegate(mockResolver)

			pool := concurrency.NewPool(ctx, 1)

			dispatchChan := make(chan dispatchMsg, 1)

			pool.Go(func(ctx context.Context) error {
				checker.produceUsersetDispatches(ctx, req, dispatchChan, iter)
				return nil
			})

			receivedMsgs := collectMessagesFromChannel(dispatchChan)
			_ = pool.Wait()
			require.Equal(t, tt.expectedDispatches, receivedMsgs)
		})
	}
}

func TestProduceTTUDispatches(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	filter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
		if tupleKey.GetCondition().GetName() == "condition1" {
			return true, nil
		}
		return false, fmt.Errorf("condition not found")
	}

	// model does not matter for this unit test.  All we care about is schema 1.1+ and computedRelation is defined for type
	model := parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user with condX]
				type team
					relations
						define teammate: [user with condX]
				type document
					relations
						define viewer: member from owner
						define owner: [group, team]

				condition condX(x: int) {
					x < 100
				}`)

	ts, err := typesystem.New(model)
	require.NoError(t, err)
	ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
	req := &ResolveCheckRequest{
		TupleKey:        tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:maria", "condition1", nil),
		RequestMetadata: NewCheckRequestMetadata(20),
	}

	tests := []struct {
		name               string
		computedRelation   string
		tuples             []*openfgav1.TupleKey
		expectedDispatches []dispatchMsg
	}{
		{
			name:               "empty_iterator",
			computedRelation:   "member",
			tuples:             nil,
			expectedDispatches: nil,
		},
		{
			name:             "iterator_error_first_tuple",
			computedRelation: "member",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "badCondition", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:            fmt.Errorf("condition not found"),
					shortCircuit:   false,
					dispatchParams: nil,
				},
			},
		},
		{
			name:             "relation_not_found",
			computedRelation: "member",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "team:1", "condition1", nil),
			},
			expectedDispatches: nil,
		},
		{
			name:             "single_match",
			computedRelation: "member",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "condition1", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
			},
		},
		{
			name:             "multiple_matches",
			computedRelation: "member",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:2", "condition1", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:2", "member", "user:maria"),
					},
				},
			},
		},
		{
			name:             "mix_relation_found_not_found",
			computedRelation: "member",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "team:1", "condition1", nil),
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "condition1", nil),
			},
			expectedDispatches: []dispatchMsg{
				{
					err:          nil,
					shortCircuit: false,
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			iter := storage.NewConditionsFilteredTupleKeyIterator(storage.NewStaticTupleKeyIterator(tt.tuples), filter)
			checker := NewLocalChecker()
			defer checker.Close()
			mockResolver := NewMockCheckResolver(ctrl)
			checker.SetDelegate(mockResolver)

			pool := concurrency.NewPool(ctx, 1)

			dispatchChan := make(chan dispatchMsg, 1)

			pool.Go(func(ctx context.Context) error {
				checker.produceTTUDispatches(ctx, tt.computedRelation, req, dispatchChan, iter)
				return nil
			})

			receivedMsgs := collectMessagesFromChannel(dispatchChan)
			_ = pool.Wait()
			require.Equal(t, tt.expectedDispatches, receivedMsgs)
		})
	}
}

// helperReceivedOutcome is a helper function that listen to chan checkOutcome and return
// all the checkOutcomes when channel is closed.
func helperReceivedOutcome(outcomes chan checkOutcome) []checkOutcome {
	var checkOutcome []checkOutcome
	for outcome := range outcomes {
		checkOutcome = append(checkOutcome, outcome)
	}
	return checkOutcome
}

func TestProcessDispatch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	const datastoreQueryCount = 30
	req := &ResolveCheckRequest{
		TupleKey:        tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:maria", "condition1", nil),
		RequestMetadata: NewCheckRequestMetadata(20),
	}

	tests := []struct {
		name                   string
		poolSize               int
		ctxCancelled           bool
		dispatchMsgs           []dispatchMsg
		mockedDispatchResponse []*ResolveCheckResponse
		expectedOutcomes       []checkOutcome
	}{
		{
			name:             "ctx_cancelled",
			poolSize:         1,
			ctxCancelled:     true,
			dispatchMsgs:     []dispatchMsg{},
			expectedOutcomes: nil,
		},
		{
			name:         "two_error",
			poolSize:     1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					err: fmt.Errorf("error_1"),
				},
				{
					err: fmt.Errorf("error_2"),
				},
			},
			expectedOutcomes: []checkOutcome{
				{
					err: fmt.Errorf("error_1"),
				},
				{
					err: fmt.Errorf("error_2"),
				},
			},
		},
		{
			name:         "shortcut_with_only",
			poolSize:     1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					shortCircuit: true,
				},
			},
			expectedOutcomes: []checkOutcome{
				{
					resp: &ResolveCheckResponse{
						Allowed: true,
						ResolutionMetadata: &ResolveCheckResponseMetadata{
							DatastoreQueryCount: 0,
						},
					},
				},
			},
		},
		{
			name:         "shortcut_with_error_at_end",
			poolSize:     1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					shortCircuit: true,
				},
				{
					err: fmt.Errorf("should_not_process_this"),
				},
			},
			expectedOutcomes: []checkOutcome{
				{
					resp: &ResolveCheckResponse{
						Allowed: true,
						ResolutionMetadata: &ResolveCheckResponseMetadata{
							DatastoreQueryCount: 0,
						},
					},
				},
			},
		},
		{
			name:         "multiple_dispatches",
			poolSize:     1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:2", "member", "user:maria"),
					},
				},
			},
			mockedDispatchResponse: []*ResolveCheckResponse{
				{
					Allowed: true,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: datastoreQueryCount,
					},
				},
				{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: datastoreQueryCount,
					},
				},
			},
			expectedOutcomes: []checkOutcome{
				{
					resp: &ResolveCheckResponse{
						Allowed: true,
						ResolutionMetadata: &ResolveCheckResponseMetadata{
							DatastoreQueryCount: datastoreQueryCount,
						},
					},
				},
				{
					resp: &ResolveCheckResponse{
						Allowed: false,
						ResolutionMetadata: &ResolveCheckResponseMetadata{
							DatastoreQueryCount: datastoreQueryCount,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			var cancel context.CancelFunc
			if tt.ctxCancelled {
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			checker := NewLocalChecker()
			defer checker.Close()
			mockResolver := NewMockCheckResolver(ctrl)
			checker.SetDelegate(mockResolver)

			for _, mockedDispatchResponse := range tt.mockedDispatchResponse {
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1).Return(mockedDispatchResponse, nil)
			}

			dispatchMsgChan := make(chan dispatchMsg, 100)
			for _, dispatchMsg := range tt.dispatchMsgs {
				dispatchMsgChan <- dispatchMsg
			}

			outcomeChan := checker.processDispatches(ctx, uint32(tt.poolSize), dispatchMsgChan)

			// now, close the channel to simulate everything is sent
			close(dispatchMsgChan)
			outcomes := helperReceivedOutcome(outcomeChan)

			require.Equal(t, tt.expectedOutcomes, outcomes)
		})
	}
}

func TestConsumeDispatch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	const datastoreQueryCount = 30
	req := &ResolveCheckRequest{
		TupleKey:        tuple.NewTupleKeyWithCondition("document:doc1", "viewer", "user:maria", "condition1", nil),
		RequestMetadata: NewCheckRequestMetadata(20),
	}
	req.RequestMetadata.DatastoreQueryCount = datastoreQueryCount
	tests := []struct {
		name                   string
		limit                  uint32
		ctxCancelled           bool
		dispatchMsgs           []dispatchMsg
		mockedDispatchResponse []*ResolveCheckResponse
		expected               *ResolveCheckResponse
		expectedError          error
	}{
		{
			name:          "ctx_cancelled",
			limit:         1,
			ctxCancelled:  true,
			dispatchMsgs:  []dispatchMsg{},
			expected:      nil,
			expectedError: context.Canceled,
		},
		{
			name:         "single_error",
			limit:        1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					err: fmt.Errorf("error_1"),
				},
			},
			expected:      nil,
			expectedError: fmt.Errorf("error_1"),
		},
		{
			name:         "false_cycle_detected",
			limit:        1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
			},
			mockedDispatchResponse: []*ResolveCheckResponse{
				{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 2,
						CycleDetected:       true,
					},
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: datastoreQueryCount + 2,
					CycleDetected:       true,
				},
			},
			expectedError: nil,
		},
		{
			name:         "two_false_no_cycle",
			limit:        1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:2", "member", "user:maria"),
					},
				},
			},
			mockedDispatchResponse: []*ResolveCheckResponse{
				{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 2,
						CycleDetected:       false,
					},
				},
				{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 3,
						CycleDetected:       false,
					},
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: datastoreQueryCount + 2 + 3,
					CycleDetected:       false,
				},
			},
			expectedError: nil,
		},
		{
			name:         "false_true",
			limit:        1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
			},
			mockedDispatchResponse: []*ResolveCheckResponse{
				{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 2,
						CycleDetected:       false,
					},
				},
				{
					Allowed: true,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 3,
						CycleDetected:       false,
					},
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: datastoreQueryCount + 2 + 3,
					CycleDetected:       false,
				},
			},
			expectedError: nil,
		},
		{
			name:         "single_true",
			limit:        1,
			ctxCancelled: false,
			dispatchMsgs: []dispatchMsg{
				{
					dispatchParams: &dispatchParams{
						parentReq: req,
						tk:        tuple.NewTupleKey("group:1", "member", "user:maria"),
					},
				},
			},
			mockedDispatchResponse: []*ResolveCheckResponse{
				{
					Allowed: true,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 2,
						CycleDetected:       false,
					},
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: datastoreQueryCount + 2,
					CycleDetected:       false,
				},
			},
			expectedError: nil,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ctx := context.Background()
			var cancel context.CancelFunc
			if tt.ctxCancelled {
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			checker := NewLocalChecker()
			defer checker.Close()
			mockResolver := NewMockCheckResolver(ctrl)
			checker.SetDelegate(mockResolver)
			for _, mockedDispatchResponse := range tt.mockedDispatchResponse {
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1).Return(mockedDispatchResponse, nil)
			}

			dispatchMsgChan := make(chan dispatchMsg, 100)
			for _, dispatchMsg := range tt.dispatchMsgs {
				dispatchMsgChan <- dispatchMsg
			}
			close(dispatchMsgChan)

			resp, err := checker.consumeDispatches(ctx, req, tt.limit, dispatchMsgChan)
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected, resp)
		})
	}
}

func TestCheckUsersetSlowPath(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	filter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
		if tupleKey.GetCondition().GetName() == "condition1" {
			return true, nil
		}
		return false, fmt.Errorf("condition not found")
	}

	// model does not matter for this unit test.  All we care about is schema 1.1+.
	model := parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user with condX, user:* with condX, group#member]
				type document
					relations
						define viewer: [group#member]

				condition condX(x: int) {
					x < 100
				}`)
	ts, err := typesystem.New(model)
	require.NoError(t, err)
	ctx := typesystem.ContextWithTypesystem(context.Background(), ts)

	const initialDSCount = 20
	tests := []struct {
		name          string
		tuples        []*openfgav1.TupleKey
		expected      *ResolveCheckResponse
		expectedError error
	}{
		{
			name: "shortcut",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("group:1", "member", "user:*", "condition1", nil),
				tuple.NewTupleKeyWithCondition("group:1", "member", "group:2#member", "condition1", nil),
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: initialDSCount + 1,
				},
			},
			expectedError: nil,
		},
		{
			name: "error",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("group:1", "member", "user:*", "badCondition", nil),
			},
			expected:      nil,
			expectedError: fmt.Errorf("condition not found"),
		},
		{
			name:   "notFound",
			tuples: []*openfgav1.TupleKey{},
			expected: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: initialDSCount + 1,
				},
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			iter := storage.NewConditionsFilteredTupleKeyIterator(storage.NewStaticTupleKeyIterator(tt.tuples), filter)
			checker := NewLocalChecker()
			defer checker.Close()

			req := &ResolveCheckRequest{
				TupleKey:        tuple.NewTupleKey("group:1", "member", "user:maria"),
				RequestMetadata: NewCheckRequestMetadata(20),
			}
			req.RequestMetadata.DatastoreQueryCount = initialDSCount
			resp, err := checker.checkUsersetSlowPath(ctx, req, iter)
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected, resp)
		})
	}
}

func TestCheckTTUSlowPath(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	filter := func(tupleKey *openfgav1.TupleKey) (bool, error) {
		if tupleKey.GetCondition().GetName() == "condition1" {
			return true, nil
		}
		return false, fmt.Errorf("condition not found")
	}

	// model does not matter for this unit test.  All we care about is schema 1.1+ and computedRelation is defined for type
	model := parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user with condX]
				type team
					relations
						define teammate: [user with condX]
				type document
					relations
						define viewer: member from owner
						define owner: [group, team]

				condition condX(x: int) {
					x < 100
				}`)

	ts, err := typesystem.New(model)
	require.NoError(t, err)
	ctx := typesystem.ContextWithTypesystem(context.Background(), ts)
	const initialDSCount = 20

	tests := []struct {
		name             string
		rewrite          *openfgav1.Userset
		tuples           []*openfgav1.TupleKey
		dispatchResponse *ResolveCheckResponse
		expected         *ResolveCheckResponse
		expectedError    error
	}{
		{
			name:    "no_tuple",
			rewrite: typesystem.TupleToUserset("owner", "member"),
			tuples:  []*openfgav1.TupleKey{},
			expected: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: initialDSCount + 1, // 1 for getting the parent
				},
			},
			expectedError: nil,
		},
		{
			name:    "error",
			rewrite: typesystem.TupleToUserset("owner", "member"),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "badCondition", nil),
			},
			expected:      nil,
			expectedError: fmt.Errorf("condition not found"),
		},
		{
			name:    "dispatcher_found",
			rewrite: typesystem.TupleToUserset("owner", "member"),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "condition1", nil),
			},
			dispatchResponse: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: initialDSCount + 1 + 1, // 1 for getting the parent and 1 for the dispatch
				},
			},
			expectedError: nil,
		},
		{
			name:    "dispatcher_not_found",
			rewrite: typesystem.TupleToUserset("owner", "member"),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:doc1", "owner", "group:1", "condition1", nil),
			},
			dispatchResponse: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: initialDSCount + 1 + 1, // 1 for getting the parent and 1 for the dispatch
				},
			},
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			iter := storage.NewConditionsFilteredTupleKeyIterator(storage.NewStaticTupleKeyIterator(tt.tuples), filter)
			checker := NewLocalChecker()
			defer checker.Close()
			mockResolver := NewMockCheckResolver(ctrl)
			checker.SetDelegate(mockResolver)

			if tt.dispatchResponse != nil {
				mockResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Return(tt.dispatchResponse, nil)
			}

			req := &ResolveCheckRequest{
				TupleKey:        tuple.NewTupleKey("group:1", "member", "user:maria"),
				RequestMetadata: NewCheckRequestMetadata(20),
			}
			req.RequestMetadata.DatastoreQueryCount = initialDSCount
			resp, err := checker.checkTTUSlowPath(ctx, req, tt.rewrite, iter)
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected, resp)
		})
	}
}

type parallelRecursiveTest struct {
	slowRequests               bool
	numTimeFuncExecuted        *atomic.Uint32
	returnResolveCheckResponse []*ResolveCheckResponse
	returnError                []error
}

func (p *parallelRecursiveTest) testParallelizeRecursive(context.Context,
	*ResolveCheckRequest,
	*recursiveMatchUserUsersetCommonData,
	TupleMapper) (*ResolveCheckResponse, error) {
	if p.slowRequests {
		time.Sleep(5 * time.Millisecond)
	}
	currentCounter := p.numTimeFuncExecuted.Add(1)
	return p.returnResolveCheckResponse[currentCounter-1], p.returnError[currentCounter-1]
}

func TestParallelizeRecursiveMatchUserUserset(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("regular_test", func(t *testing.T) {
		tests := []struct {
			name                  string
			usersetItems          []string
			usersetError          error
			maxConcurrentReads    int
			visitedItems          []string
			parallelRecursiveTest parallelRecursiveTest
			expectedResponse      *ResolveCheckResponse
			expectedError         error
		}{
			{
				name:               "empty_userset",
				usersetItems:       []string{},
				visitedItems:       []string{},
				maxConcurrentReads: 20,
				parallelRecursiveTest: parallelRecursiveTest{
					numTimeFuncExecuted: &atomic.Uint32{},
				},
				expectedResponse: &ResolveCheckResponse{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 15,
						CycleDetected:       false,
					},
				},
				expectedError: nil,
			},
			{
				name:               "multiple_userset_last_true_concurrent_read_large",
				usersetItems:       []string{"group:1", "group:2", "group:3"},
				visitedItems:       []string{},
				maxConcurrentReads: 20,
				parallelRecursiveTest: parallelRecursiveTest{
					numTimeFuncExecuted: &atomic.Uint32{},
					returnResolveCheckResponse: []*ResolveCheckResponse{
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: true,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
					},
					returnError: []error{
						nil,
						nil,
						nil,
					},
				},
				expectedResponse: &ResolveCheckResponse{
					Allowed: true,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 20,
						CycleDetected:       false,
					},
				},
				expectedError: nil,
			},
			{
				name:               "multiple_userset_last_true_concurrent_read_large_slow_requests",
				usersetItems:       []string{"group:1", "group:2", "group:3"},
				visitedItems:       []string{},
				maxConcurrentReads: 20,
				parallelRecursiveTest: parallelRecursiveTest{
					slowRequests:        true,
					numTimeFuncExecuted: &atomic.Uint32{},
					returnResolveCheckResponse: []*ResolveCheckResponse{
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: true,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
					},
					returnError: []error{
						nil,
						nil,
						nil,
					},
				},
				expectedResponse: &ResolveCheckResponse{
					Allowed: true,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 20,
						CycleDetected:       false,
					},
				},
				expectedError: nil,
			},
			{
				name:               "multiple_userset_last_true_concurrent_read_small",
				usersetItems:       []string{"group:1", "group:2", "group:3"},
				visitedItems:       []string{},
				maxConcurrentReads: 1,
				parallelRecursiveTest: parallelRecursiveTest{
					numTimeFuncExecuted: &atomic.Uint32{},
					returnResolveCheckResponse: []*ResolveCheckResponse{
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: true,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
					},
					returnError: []error{
						nil,
						nil,
						nil,
					},
				},
				expectedResponse: &ResolveCheckResponse{
					Allowed: true,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 20,
						CycleDetected:       false,
					},
				},
				expectedError: nil,
			},
			{
				name:               "multiple_userset_last_true_concurrent_read_small_slow_request",
				usersetItems:       []string{"group:1", "group:2", "group:3"},
				visitedItems:       []string{},
				maxConcurrentReads: 1,
				parallelRecursiveTest: parallelRecursiveTest{
					slowRequests:        true,
					numTimeFuncExecuted: &atomic.Uint32{},
					returnResolveCheckResponse: []*ResolveCheckResponse{
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: true,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
					},
					returnError: []error{
						nil,
						nil,
						nil,
					},
				},
				expectedResponse: &ResolveCheckResponse{
					Allowed: true,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 20,
						CycleDetected:       false,
					},
				},
				expectedError: nil,
			},
			{
				name:               "multiple_userset_first_true_concurrent_read_large",
				usersetItems:       []string{"group:1", "group:2", "group:3"},
				visitedItems:       []string{},
				maxConcurrentReads: 20,
				parallelRecursiveTest: parallelRecursiveTest{
					numTimeFuncExecuted: &atomic.Uint32{},
					returnResolveCheckResponse: []*ResolveCheckResponse{
						{
							Allowed: true,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
					},
					returnError: []error{
						nil,
						nil,
						nil,
					},
				},
				expectedResponse: &ResolveCheckResponse{
					Allowed: true,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 20,
						CycleDetected:       false,
					},
				},
				expectedError: nil,
			},
			{
				name:               "multiple_userset_first_true_concurrent_read_small",
				usersetItems:       []string{"group:1", "group:2", "group:3"},
				visitedItems:       []string{},
				maxConcurrentReads: 1,
				parallelRecursiveTest: parallelRecursiveTest{
					numTimeFuncExecuted: &atomic.Uint32{},
					returnResolveCheckResponse: []*ResolveCheckResponse{
						{
							Allowed: true,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
					},
					returnError: []error{
						nil,
						nil,
						nil,
					},
				},
				expectedResponse: &ResolveCheckResponse{
					Allowed: true,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 20,
						CycleDetected:       false,
					},
				},
				expectedError: nil,
			},
			{
				name:               "multiple_userset_none_true_concurrent_read_small",
				usersetItems:       []string{"group:1", "group:2", "group:3"},
				visitedItems:       []string{},
				maxConcurrentReads: 1,
				parallelRecursiveTest: parallelRecursiveTest{
					numTimeFuncExecuted: &atomic.Uint32{},
					returnResolveCheckResponse: []*ResolveCheckResponse{
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
						{
							Allowed: false,
							ResolutionMetadata: &ResolveCheckResponseMetadata{
								DatastoreQueryCount: 20,
								CycleDetected:       false,
							},
						},
					},
					returnError: []error{
						nil,
						nil,
						nil,
					},
				},
				expectedResponse: &ResolveCheckResponse{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 15,
						CycleDetected:       false,
					},
				},
				expectedError: nil,
			},
			{
				name:               "error",
				usersetItems:       []string{"group:1"},
				visitedItems:       []string{},
				maxConcurrentReads: 20,
				parallelRecursiveTest: parallelRecursiveTest{
					numTimeFuncExecuted: &atomic.Uint32{},
					returnResolveCheckResponse: []*ResolveCheckResponse{
						nil,
					},
					returnError: []error{
						fmt.Errorf("mock_error"),
					},
				},
				expectedResponse: nil,
				expectedError:    fmt.Errorf("mock_error"),
			},
			{
				name:               "do_not_run_visited_item",
				usersetItems:       []string{"group:1"},
				visitedItems:       []string{"group:1"},
				maxConcurrentReads: 20,
				parallelRecursiveTest: parallelRecursiveTest{
					// notice there are no items being returned as group:1 is skipped.
					returnResolveCheckResponse: []*ResolveCheckResponse{},
					returnError:                []error{},
				},
				expectedResponse: &ResolveCheckResponse{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 15,
						CycleDetected:       false,
					},
				},
				expectedError: nil,
			},
			{
				name:               "mapper_build_error",
				usersetItems:       []string{"group:1"},
				usersetError:       fmt.Errorf("mock_error"),
				maxConcurrentReads: 20,
				parallelRecursiveTest: parallelRecursiveTest{
					returnResolveCheckResponse: []*ResolveCheckResponse{},
					returnError:                []error{},
				},
				expectedResponse: nil,
				expectedError:    fmt.Errorf("mock_error"),
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				ds := mocks.NewMockRelationshipTupleReader(ctrl)
				ds.EXPECT().ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(storage.NewStaticTupleIterator(nil), tt.usersetError)
				commonParameters := &recursiveMatchUserUsersetCommonData{
					concurrencyLimit: tt.maxConcurrentReads,
					visitedUserset:   &sync.Map{},
					dsCount:          &atomic.Uint32{},
					ds:               ds,
					tupleMapperKind:  NestedUsersetKind,
				}
				for _, item := range tt.visitedItems {
					commonParameters.visitedUserset.Store(item, struct{}{})
				}
				commonParameters.dsCount.Store(15)
				resp, err := parallelizeRecursiveMatchUserUserset(context.Background(),
					tt.usersetItems,
					&ResolveCheckRequest{
						RequestMetadata: NewCheckRequestMetadata(20),
					},
					commonParameters,
					tt.parallelRecursiveTest.testParallelizeRecursive)
				require.Equal(t, tt.expectedResponse, resp)
				require.Equal(t, tt.expectedError, err)
			})
		}
	})
	t.Run("very_large_slow_userset", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name         string
			checkAllowed bool
		}{
			{
				name:         "not_allowed",
				checkAllowed: false,
			},
			{
				name:         "allowed",
				checkAllowed: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				maxConcurrentRead := 20
				numUsersetItem := 5000
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				ds := mocks.NewMockRelationshipTupleReader(ctrl)
				ds.EXPECT().ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().Return(storage.NewStaticTupleIterator(nil), nil)
				commonParameters := &recursiveMatchUserUsersetCommonData{
					concurrencyLimit: maxConcurrentRead,
					visitedUserset:   &sync.Map{},
					dsCount:          &atomic.Uint32{},
					ds:               ds,
					tupleMapperKind:  NestedUsersetKind,
				}
				commonParameters.dsCount.Store(15)

				usersetItems := make([]string, numUsersetItem)
				returnResolveCheckResponse := make([]*ResolveCheckResponse, numUsersetItem)
				returnError := make([]error, numUsersetItem)
				for i := 0; i < numUsersetItem; i++ {
					usersetItems[i] = fmt.Sprintf("group:%d", i)
					returnResolveCheckResponse[i] = &ResolveCheckResponse{
						Allowed: false,
						ResolutionMetadata: &ResolveCheckResponseMetadata{
							DatastoreQueryCount: 15,
							CycleDetected:       false,
						},
					}
					returnError[i] = nil
				}
				if tt.checkAllowed {
					returnResolveCheckResponse[numUsersetItem/2] = &ResolveCheckResponse{
						Allowed: true,
						ResolutionMetadata: &ResolveCheckResponseMetadata{
							DatastoreQueryCount: 15,
							CycleDetected:       false,
						},
					}
				}

				recursiveTestResult := parallelRecursiveTest{
					slowRequests:               true,
					numTimeFuncExecuted:        &atomic.Uint32{},
					returnResolveCheckResponse: returnResolveCheckResponse,
					returnError:                returnError,
				}
				resp, err := parallelizeRecursiveMatchUserUserset(context.Background(),
					usersetItems,
					&ResolveCheckRequest{
						RequestMetadata: NewCheckRequestMetadata(20),
					},
					commonParameters,
					recursiveTestResult.testParallelizeRecursive)
				require.NoError(t, err)
				require.Equal(t, &ResolveCheckResponse{
					Allowed: tt.checkAllowed,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 15,
						CycleDetected:       false,
					},
				}, resp)
			})
		}
	})
}

func TestRecursiveMatchUserUserset(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name               string
		tuples             [][]*openfgav1.Tuple
		tupleIteratorError error
		expected           *ResolveCheckResponse
		expectedError      error
	}{
		{
			name: "empty_recursive_userset",
			tuples: [][]*openfgav1.Tuple{
				{},
			},
			expected: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
					CycleDetected:       false,
				},
			},
		},
		{
			name: "first_item_match",
			tuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:a#member"),
					},
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:x#member"),
					},
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
					CycleDetected:       false,
				},
			},
		},
		{
			name: "second_item_match",
			tuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:x#member"),
					},
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:a#member"),
					},
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 1,
					CycleDetected:       false,
				},
			},
		},
		{
			name: "iter_error",
			tuples: [][]*openfgav1.Tuple{
				{},
			},
			tupleIteratorError: fmt.Errorf("mock_error"),
			expected:           nil,
			expectedError:      fmt.Errorf("mock_error"),
		},
		{
			name: "recursive_linear_not_found",
			tuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:x#member"),
					},
				},
				{
					{
						Key: tuple.NewTupleKey("group:x", "member", "group:y#member"),
					},
				},
				{},
			},
			expected: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 3,
					CycleDetected:       false,
				},
			},
		},
		{
			name: "recursive_linear_found",
			tuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:x#member"),
					},
				},
				{
					{
						Key: tuple.NewTupleKey("group:x", "member", "group:y#member"),
					},
				},
				{
					{
						Key: tuple.NewTupleKey("group:y", "member", "group:b#member"),
					},
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 3,
					CycleDetected:       false,
				},
			},
		},
		{
			name: "recursive_breath_not_found",
			tuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:x#member"),
					},
					{
						Key: tuple.NewTupleKey("group:x", "member", "group:y#member"),
					},
				},
				{},
				{},
			},
			expected: &ResolveCheckResponse{
				Allowed: false,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 3,
					CycleDetected:       false,
				},
			},
		},
		{
			name: "found_second_level",
			tuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:x#member"),
					},
					{
						Key: tuple.NewTupleKey("group:x", "member", "group:y#member"),
					},
				},
				{},
				{
					{
						Key: tuple.NewTupleKey("group:y", "member", "group:b#member"),
					},
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
				ResolutionMetadata: &ResolveCheckResponseMetadata{
					DatastoreQueryCount: 3,
					CycleDetected:       false,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ds := mocks.NewMockRelationshipTupleReader(ctrl)
			for _, tuples := range tt.tuples {
				ds.EXPECT().ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(storage.NewStaticTupleIterator(tuples), tt.tupleIteratorError)
			}
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

			req := &ResolveCheckRequest{
				StoreID:              ulid.Make().String(),
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
				RequestMetadata:      NewCheckRequestMetadata(20),
			}

			userUsersetMapping := storage.NewSortedSet()
			userUsersetMapping.Add("group:a")
			userUsersetMapping.Add("group:b")

			commonData := &recursiveMatchUserUsersetCommonData{
				typesys:              ts,
				ds:                   ds,
				dsCount:              &atomic.Uint32{},
				concurrencyLimit:     10,
				tupleMapperKind:      NestedUsersetKind,
				userToUsersetMapping: userUsersetMapping,
				visitedUserset:       &sync.Map{},
				allowedUserTypeRestrictions: []*openfgav1.RelationReference{
					{
						Type: "group",
						RelationOrWildcard: &openfgav1.RelationReference_Relation{
							Relation: "member",
						},
					},
				},
			}
			mapper, err := buildMapper(context.Background(), req, commonData)
			if tt.tupleIteratorError != nil {
				require.Equal(t, tt.tupleIteratorError, err)
				return
			}

			result, err := recursiveMatchUserUserset(context.Background(), req, commonData, mapper)
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestStreamedLookupUsersetForUser(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name                            string
		contextDone                     bool
		publiclyAssignable              bool
		readStartingWithUserTuples      []*openfgav1.Tuple
		readStartingWithUserTuplesError error
		iteratorHasError                bool
		expected                        []usersetMessage
		poolSize                        int
	}{
		{
			name:                            "get_iterator_error",
			contextDone:                     false,
			poolSize:                        1,
			readStartingWithUserTuples:      []*openfgav1.Tuple{},
			readStartingWithUserTuplesError: fmt.Errorf("mock_error"),
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
			poolSize:         1,
			iteratorHasError: true,
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:1", "member", "user:maria"),
				},
			},
			readStartingWithUserTuplesError: nil,
			expected: []usersetMessage{
				{
					userset: "group:1",
					err:     nil,
				},
				{
					userset: "",
					err:     mocks.ErrSimulatedError,
				},
			},
		},
		{
			name:                            "empty_user",
			contextDone:                     false,
			poolSize:                        1,
			readStartingWithUserTuples:      []*openfgav1.Tuple{},
			readStartingWithUserTuplesError: nil,
			expected:                        nil,
		},
		{
			name:                            "ctx_cancel",
			contextDone:                     true,
			poolSize:                        1,
			readStartingWithUserTuples:      []*openfgav1.Tuple{},
			readStartingWithUserTuplesError: nil,
			expected:                        nil,
		},
		{
			name:        "has_users",
			contextDone: false,
			poolSize:    1,
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:1", "member", "user:maria"),
				},
				{
					Key: tuple.NewTupleKey("group:2", "member", "user:maria"),
				},
			},
			readStartingWithUserTuplesError: nil,
			expected: []usersetMessage{
				{
					userset: "group:1",
					err:     nil,
				},
				{
					userset: "group:2",
					err:     nil,
				},
			},
		},
		{
			name:        "has_users_large_pool_size",
			contextDone: false,
			poolSize:    5,
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:1", "member", "user:maria"),
				},
				{
					Key: tuple.NewTupleKey("group:2", "member", "user:maria"),
				},
			},
			readStartingWithUserTuplesError: nil,
			expected: []usersetMessage{
				{
					userset: "group:1",
					err:     nil,
				},
				{
					userset: "group:2",
					err:     nil,
				},
			},
		},
		{
			name:        "non_publicly_assignable",
			contextDone: false,
			poolSize:    5,
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:1", "member", "user:maria"),
				},
				{
					Key: tuple.NewTupleKey("group:2", "member", "user:*"),
				},
			},
			readStartingWithUserTuplesError: nil,
			expected: []usersetMessage{
				{
					userset: "group:1",
					err:     nil,
				},
			},
		},
		{
			name:               "publicly_assignable",
			contextDone:        false,
			publiclyAssignable: true,
			poolSize:           5,
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:1", "member", "user:maria"),
				},
				{
					Key: tuple.NewTupleKey("group:2", "member", "user:*"),
				},
			},
			readStartingWithUserTuplesError: nil,
			expected: []usersetMessage{
				{
					userset: "group:1",
					err:     nil,
				},
				{
					userset: "group:2",
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
			ds := mocks.NewMockRelationshipTupleReader(ctrl)
			if tt.iteratorHasError {
				ds.EXPECT().ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(mocks.NewErrorTupleIterator(tt.readStartingWithUserTuples), tt.readStartingWithUserTuplesError)
			} else {
				ds.EXPECT().ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuples), tt.readStartingWithUserTuplesError)
			}
			var model *openfgav1.AuthorizationModel
			if tt.publiclyAssignable {
				model = parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user, user:*, group#member]
`)
			} else {
				model = parser.MustTransformDSLToProto(`
				model
					schema 1.1

				type user
				type group
					relations
						define member: [user, group#member]
`)
			}
			ts, err := typesystem.New(model)
			require.NoError(t, err)

			req := &ResolveCheckRequest{
				StoreID:              ulid.Make().String(),
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
				RequestMetadata:      NewCheckRequestMetadata(20),
			}

			cancellableCtx, cancelFunc := context.WithCancel(context.Background())
			if tt.contextDone {
				cancelFunc()
			} else {
				defer cancelFunc()
			}

			dsCount := &atomic.Uint32{}
			commonData := &recursiveMatchUserUsersetCommonData{
				typesys:                     ts,
				ds:                          ds,
				dsCount:                     dsCount,
				userToUsersetMapping:        nil, // not used
				concurrencyLimit:            tt.poolSize,
				visitedUserset:              &sync.Map{},
				allowedUserTypeRestrictions: nil, // not used
			}

			userToUsersetMessageChan := streamedLookupUsersetForUser(cancellableCtx, commonData, req)

			var userToUsersetMessages []usersetMessage

			for userToUsersetMessage := range userToUsersetMessageChan {
				userToUsersetMessages = append(userToUsersetMessages, userToUsersetMessage)
			}

			require.Equal(t, tt.expected, userToUsersetMessages)
		})
	}
}

func TestStreamedLookupUsersetForObject(t *testing.T) {
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
		poolSize               int
	}{
		{
			name:                   "get_iterator_error",
			contextDone:            false,
			poolSize:               1,
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
			poolSize:         1,
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
			poolSize:               1,
			readUsersetTuples:      []*openfgav1.Tuple{},
			readUsersetTuplesError: nil,
			expected:               nil,
		},
		{
			name:                   "ctx_cancel",
			contextDone:            true,
			poolSize:               1,
			readUsersetTuples:      []*openfgav1.Tuple{},
			readUsersetTuplesError: nil,
			expected:               nil,
		},
		{
			name:        "has_userset",
			contextDone: false,
			poolSize:    1,
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
			poolSize:    5,
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
			ds := mocks.NewMockRelationshipTupleReader(ctrl)
			if tt.iteratorHasError {
				ds.EXPECT().ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(mocks.NewErrorTupleIterator(tt.readUsersetTuples), tt.readUsersetTuplesError)
			} else {
				ds.EXPECT().ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1).Return(storage.NewStaticTupleIterator(tt.readUsersetTuples), tt.readUsersetTuplesError)
			}
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

			req := &ResolveCheckRequest{
				StoreID:              ulid.Make().String(),
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
				RequestMetadata:      NewCheckRequestMetadata(20),
			}

			cancellableCtx, cancelFunc := context.WithCancel(context.Background())
			if tt.contextDone {
				cancelFunc()
			} else {
				defer cancelFunc()
			}

			dsCount := &atomic.Uint32{}
			commonData := &recursiveMatchUserUsersetCommonData{
				typesys:              ts,
				ds:                   ds,
				dsCount:              dsCount,
				userToUsersetMapping: nil, // not used
				concurrencyLimit:     tt.poolSize,
				visitedUserset:       &sync.Map{},
				tupleMapperKind:      NestedUsersetKind,
				allowedUserTypeRestrictions: []*openfgav1.RelationReference{
					{
						Type: "group",
						RelationOrWildcard: &openfgav1.RelationReference_Relation{
							Relation: "member",
						},
					},
				},
			}

			mapper, err := buildMapper(context.Background(), req, commonData)
			if tt.readUsersetTuplesError != nil {
				require.Equal(t, tt.readUsersetTuplesError, err)
				return
			}

			userToUsersetMessageChan := streamedLookupUsersetForObject(cancellableCtx, commonData, mapper)

			var userToUsersetMessages []usersetMessage

			for userToUsersetMessage := range userToUsersetMessageChan {
				userToUsersetMessages = append(userToUsersetMessages, userToUsersetMessage)
			}

			require.Equal(t, tt.expected, userToUsersetMessages)
		})
	}
}

func TestProcessUsersetMessage(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name                 string
		message              usersetMessage
		matchingUserset      []string
		expectedFound        bool
		expectedError        error
		expectedInputUserset []string
	}{
		{
			name: "error_input",
			message: usersetMessage{
				userset: "",
				err:     fmt.Errorf("mock_error"),
			},
			matchingUserset:      []string{"a", "b"},
			expectedFound:        false,
			expectedError:        fmt.Errorf("mock_error"),
			expectedInputUserset: []string{},
		},
		{
			name: "match",
			message: usersetMessage{
				userset: "b",
				err:     nil,
			},
			matchingUserset:      []string{"a", "b"},
			expectedFound:        true,
			expectedError:        nil,
			expectedInputUserset: []string{"b"},
		},
		{
			name: "not_match",
			message: usersetMessage{
				userset: "c",
				err:     nil,
			},
			matchingUserset:      []string{"a", "b"},
			expectedFound:        false,
			expectedError:        nil,
			expectedInputUserset: []string{"c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			inputSortedSet := storage.NewSortedSet()
			matchingSortedSet := storage.NewSortedSet()
			for _, match := range tt.matchingUserset {
				matchingSortedSet.Add(match)
			}
			output, err := processUsersetMessage(tt.message, inputSortedSet, matchingSortedSet)
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expectedFound, output)
			require.Equal(t, tt.expectedInputUserset, inputSortedSet.Values())
		})
	}
}

func TestMatchUsersetFromUserAndUsersetFromObject(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("non_cancel_context", func(t *testing.T) {
		tests := []struct {
			name                         string
			userToUsersetMessages        []usersetMessage
			objectToUsersetMessages      []usersetMessage
			expectedResolveCheckResponse *ResolveCheckResponse
			expectedUserToUserset        []string
			expectedObjectToUserset      []string
			expectedError                error
		}{
			{
				name:                    "empty_lists",
				userToUsersetMessages:   []usersetMessage{},
				objectToUsersetMessages: []usersetMessage{},
				expectedResolveCheckResponse: &ResolveCheckResponse{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 2,
						CycleDetected:       false,
					},
				},
				expectedUserToUserset:   nil,
				expectedObjectToUserset: nil,
				expectedError:           nil,
			},
			{
				name: "userToUsersetMessages_not_nil_but_object_nil",
				userToUsersetMessages: []usersetMessage{
					{
						userset: "group:2",
						err:     nil,
					},
				},
				objectToUsersetMessages: []usersetMessage{},
				expectedResolveCheckResponse: &ResolveCheckResponse{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 2,
						CycleDetected:       false,
					},
				},
				expectedUserToUserset:   nil,
				expectedObjectToUserset: nil,
				expectedError:           nil,
			},
			{
				name:                  "objectToUsersetMessages_not_nil_but_user_nil",
				userToUsersetMessages: []usersetMessage{},
				objectToUsersetMessages: []usersetMessage{
					{
						userset: "group:2",
						err:     nil,
					},
				},
				expectedResolveCheckResponse: &ResolveCheckResponse{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 2,
						CycleDetected:       false,
					},
				},
				expectedUserToUserset:   nil,
				expectedObjectToUserset: nil,
				expectedError:           nil,
			},
			{
				name: "userToUsersetMessages_error",
				userToUsersetMessages: []usersetMessage{
					{
						userset: "",
						err:     fmt.Errorf("mock_error"),
					},
				},
				objectToUsersetMessages: []usersetMessage{
					{
						userset: "group:1",
						err:     nil,
					},
				},
				expectedResolveCheckResponse: nil,
				expectedUserToUserset:        nil,
				expectedObjectToUserset:      nil,
				expectedError:                fmt.Errorf("mock_error"),
			},
			{
				name: "objectToUsersetMessages_error",
				userToUsersetMessages: []usersetMessage{
					{
						userset: "group:3",
						err:     nil,
					},
				},
				objectToUsersetMessages: []usersetMessage{
					{
						userset: "",
						err:     fmt.Errorf("mock_error"),
					},
				},
				expectedResolveCheckResponse: nil,
				expectedUserToUserset:        nil,
				expectedObjectToUserset:      nil,
				expectedError:                fmt.Errorf("mock_error"),
			},
			{
				name: "items_not_match",
				userToUsersetMessages: []usersetMessage{
					{
						userset: "group:2",
						err:     nil,
					},
				},
				objectToUsersetMessages: []usersetMessage{
					{
						userset: "group:3",
						err:     nil,
					},
				},
				expectedResolveCheckResponse: nil,
				expectedUserToUserset:        []string{"group:2"},
				expectedObjectToUserset:      []string{"group:3"},
				expectedError:                nil,
			},
			{
				name: "items_match",
				userToUsersetMessages: []usersetMessage{
					{
						userset: "group:2",
						err:     nil,
					},
				},
				objectToUsersetMessages: []usersetMessage{
					{
						userset: "group:2",
						err:     nil,
					},
				},
				expectedResolveCheckResponse: &ResolveCheckResponse{
					Allowed: true,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 2,
						CycleDetected:       false,
					},
				},
				expectedUserToUserset:   nil,
				expectedObjectToUserset: nil,
				expectedError:           nil,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				userToUsersetMessagesChan := make(chan usersetMessage, 5)
				objectToUsersetMessagesChan := make(chan usersetMessage, 5)

				pool := concurrency.NewPool(context.Background(), 2)
				pool.Go(func(ctx context.Context) error {
					time.Sleep(1 * time.Millisecond)

					for _, userToUsersetMessage := range tt.userToUsersetMessages {
						concurrency.TrySendThroughChannel(ctx, userToUsersetMessage, userToUsersetMessagesChan)
					}
					close(userToUsersetMessagesChan)
					return nil
				})

				pool.Go(func(ctx context.Context) error {
					time.Sleep(1 * time.Millisecond)

					for _, objectToUsersetMessage := range tt.objectToUsersetMessages {
						concurrency.TrySendThroughChannel(ctx, objectToUsersetMessage, objectToUsersetMessagesChan)
					}
					close(objectToUsersetMessagesChan)
					return nil
				})
				ctx := context.Background()

				req := &ResolveCheckRequest{
					StoreID:              ulid.Make().String(),
					AuthorizationModelID: ulid.Make().String(),
					TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
					RequestMetadata:      NewCheckRequestMetadata(20),
				}

				resp, userToUserset, objectToUserset, err := matchUsersetFromUserAndUsersetFromObject(ctx, req, userToUsersetMessagesChan, objectToUsersetMessagesChan)
				_ = pool.Wait()
				require.Equal(t, tt.expectedError, err)
				require.Equal(t, tt.expectedResolveCheckResponse, resp)
				if tt.expectedUserToUserset != nil {
					require.Equal(t, tt.expectedUserToUserset, userToUserset.Values())
				} else {
					require.Nil(t, userToUserset)
				}
				if tt.expectedObjectToUserset != nil {
					require.Equal(t, tt.expectedObjectToUserset, objectToUserset.Values())
				} else {
					require.Nil(t, objectToUserset)
				}
			})
		}
	})
	t.Run("cancel_context", func(t *testing.T) {
		t.Parallel()
		userToUsersetMessagesChan := make(chan usersetMessage)
		objectToUsersetMessagesChan := make(chan usersetMessage)
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		req := &ResolveCheckRequest{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
			RequestMetadata:      NewCheckRequestMetadata(20),
		}

		resp, userToUserset, objectToUserset, err := matchUsersetFromUserAndUsersetFromObject(ctx, req, userToUsersetMessagesChan, objectToUsersetMessagesChan)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, resp)
		require.Nil(t, userToUserset)
		require.Nil(t, objectToUserset)
	})
}

func TestNestedUsersetFastpath(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("normal_test_cases", func(t *testing.T) {
		tests := []struct {
			name                            string
			readStartingWithUserTuples      []*openfgav1.Tuple
			readStartingWithUserTuplesError error
			readUsersetTuples               [][]*openfgav1.Tuple
			readUsersetTuplesError          error
			expected                        *ResolveCheckResponse
			expectedError                   error
		}{
			{
				name:                       "no_user_assigned_to_group",
				readStartingWithUserTuples: []*openfgav1.Tuple{},
				readUsersetTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
						},
					},
					{},
				},
				expected: &ResolveCheckResponse{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 2,
						CycleDetected:       false,
					},
				},
			},
			{
				name: "user_assigned_to_first_level_sub_group",
				readStartingWithUserTuples: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
					},
					{
						Key: tuple.NewTupleKey("group:4", "member", "user:maria"),
					},
				},
				readUsersetTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
						},
					},
				},
				expected: &ResolveCheckResponse{
					Allowed: true,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 2,
						CycleDetected:       false,
					},
				},
			},
			{
				name: "user_assigned_to_second_level_sub_group",
				readStartingWithUserTuples: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
					},
					{
						Key: tuple.NewTupleKey("group:4", "member", "user:maria"),
					},
				},
				readUsersetTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:6", "member", "group:5#member"),
						},
					},
					{
						{
							Key: tuple.NewTupleKey("group:5", "member", "group:3#member"),
						},
					},
				},
				expected: &ResolveCheckResponse{
					Allowed: true,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 3,
						CycleDetected:       false,
					},
				},
			},
			{
				name: "user_not_assigned_to_sub_group",
				readStartingWithUserTuples: []*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
					},
					{
						Key: tuple.NewTupleKey("group:4", "member", "user:maria"),
					},
				},
				readUsersetTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:1", "member", "group:2#member"),
						},
					},
					{},
				},
				expected: &ResolveCheckResponse{
					Allowed: false,
					ResolutionMetadata: &ResolveCheckResponseMetadata{
						DatastoreQueryCount: 3,
						CycleDetected:       false,
					},
				},
			},
			{
				name:                            "error_getting_tuple",
				readStartingWithUserTuples:      []*openfgav1.Tuple{},
				readStartingWithUserTuplesError: fmt.Errorf("mock error"),
				readUsersetTuples: [][]*openfgav1.Tuple{
					{
						{
							Key: tuple.NewTupleKey("group:1", "member", "group:2#member"),
						},
					},
					{},
				},
				expected:      nil,
				expectedError: fmt.Errorf("mock error"),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				ds := mocks.NewMockRelationshipTupleReader(ctrl)
				ds.EXPECT().ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuples), tt.readStartingWithUserTuplesError)

				for _, tuples := range tt.readUsersetTuples {
					ds.EXPECT().ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tuples), tt.readUsersetTuplesError)
				}
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

				req := &ResolveCheckRequest{
					StoreID:              ulid.Make().String(),
					AuthorizationModelID: ulid.Make().String(),
					TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
					RequestMetadata:      NewCheckRequestMetadata(20),
				}

				typeRes := []*openfgav1.RelationReference{typesystem.DirectRelationReference("group", "member")}

				result, err := nestedUsersetFastpath(context.Background(), ts, ds, req, NestedUsersetKind, typeRes, 10)
				require.Equal(t, tt.expectedError, err)
				require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
				require.Equal(t, tt.expected.GetResolutionMetadata(), result.GetResolutionMetadata())
			})
		}
	})
	t.Run("resolution_depth_exceeded", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		ds := mocks.NewMockRelationshipTupleReader(ctrl)
		ds.EXPECT().ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(1).Return(
			storage.NewStaticTupleIterator([]*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:bad", "member", "user:maria"),
				},
			}), nil)

		for i := 0; i < 26; i++ {
			ds.EXPECT().ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).MaxTimes(1).Return(
				storage.NewStaticTupleIterator([]*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:"+strconv.Itoa(i+1), "member", "group:"+strconv.Itoa(i)+"#member"),
					},
				}), nil)
		}
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

		req := &ResolveCheckRequest{
			StoreID:              ulid.Make().String(),
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
			RequestMetadata:      NewCheckRequestMetadata(20),
		}

		typeRes := []*openfgav1.RelationReference{typesystem.DirectRelationReference("group", "member")}

		result, err := nestedUsersetFastpath(context.Background(), ts, ds, req, NestedUsersetKind, typeRes, 10)
		require.Nil(t, result)
		require.Equal(t, ErrResolutionDepthExceeded, err)
	})
}
