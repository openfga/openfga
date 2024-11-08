package commands

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/internal/condition"
	ofga_errors "github.com/openfga/openfga/internal/errors"
	"github.com/openfga/openfga/internal/graph"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestCheckQuery(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)

	mockCheckResolver := graph.NewMockCheckResolver(mockController)
	model := testutils.MustTransformDSLToProtoWithID(`
model
	schema 1.1
type user
type doc
	relations
		define viewer: [user]
		define viewer_computed: viewer
`)
	ts, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)

	t.Run("validates_input_user", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, ts)
		_, _, err := cmd.Execute(context.Background(), &CheckCommandParams{
			StoreID: ulid.Make().String(),
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     "invalid:1",
				Relation: "viewer",
				Object:   "doc:1",
			},
		})
		require.ErrorContains(t, err, "type 'invalid' not found")
	})

	t.Run("validates_input_relation", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, ts)
		_, _, err := cmd.Execute(context.Background(), &CheckCommandParams{
			StoreID: ulid.Make().String(),
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     "user:1",
				Relation: "invalid",
				Object:   "doc:1",
			},
		})
		require.ErrorContains(t, err, "relation 'doc#invalid' not found")
	})

	t.Run("validates_input_object", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, ts)
		_, _, err := cmd.Execute(context.Background(), &CheckCommandParams{
			StoreID: ulid.Make().String(),
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     "user:1",
				Relation: "viewer",
				Object:   "invalid:1",
			},
		})
		require.ErrorContains(t, err, "type 'invalid' not found")
	})

	t.Run("validates_input_contextual_tuple", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, ts)
		_, _, err := cmd.Execute(context.Background(), &CheckCommandParams{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewCheckRequestTupleKey("invalid:1", "viewer", "user:1"),
			ContextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("invalid:1", "viewer", "user:1"),
				},
			},
		})
		require.ErrorContains(t, err, "type 'invalid' not found")
	})

	t.Run("validates_tuple_key_less_strictly_than_contextual_tuples", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, ts)
		_, _, err := cmd.Execute(context.Background(), &CheckCommandParams{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewCheckRequestTupleKey("doc:1", "viewer_computed", "user:1"),
			ContextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					// this isn't a tuple that you can write
					tuple.NewTupleKey("doc:1", "viewer_computed", "user:1"),
				},
			},
		})
		require.ErrorContains(t, err, "type 'user' is not an allowed type restriction for 'doc#viewer_computed'")
	})

	t.Run("no_validation_error_and_call_to_resolver_goes_through", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, ts)
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			Return(nil, nil)
		_, _, err := cmd.Execute(context.Background(), &CheckCommandParams{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
		})
		require.NoError(t, err)
	})

	t.Run("returns_db_metrics", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, ts)
		mockDatastore.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(1)
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			DoAndReturn(func(ctx context.Context, req *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
				_, _ = cmd.datastore.Read(ctx, req.StoreID, nil, storage.ReadOptions{})
				return &graph.ResolveCheckResponse{}, nil
			})
		checkResp, _, err := cmd.Execute(context.Background(), &CheckCommandParams{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
		})
		require.NoError(t, err)
		require.Equal(t, uint32(1), checkResp.GetResolutionMetadata().DatastoreQueryCount)
	})

	t.Run("no_validation_error_but_call_to_resolver_fails", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, ts)
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1).Return(nil, ofga_errors.ErrUnknown)
		_, _, err := cmd.Execute(context.Background(), &CheckCommandParams{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
		})
		require.ErrorIs(t, err, ofga_errors.ErrUnknown)
	})

	t.Run("ignores_cache_controller_with_high_consistency", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, ts)
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context, req *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
			require.Zero(t, req.GetLastCacheInvalidationTime())
			return nil, nil
		})
		_, _, err := cmd.Execute(context.Background(), &CheckCommandParams{
			StoreID:     ulid.Make().String(),
			TupleKey:    tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		})
		require.NoError(t, err)
	})

	t.Run("cache_controller_sets_invalidation_time", func(t *testing.T) {
		storeID := ulid.Make().String()
		invalidationTime := time.Now().UTC()
		cacheController := mockstorage.NewMockCacheController(mockController)
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, ts, WithCacheController(cacheController))
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1).DoAndReturn(func(ctx context.Context, req *graph.ResolveCheckRequest) (*graph.ResolveCheckResponse, error) {
			require.Equal(t, req.GetLastCacheInvalidationTime(), invalidationTime)
			return nil, nil
		})
		cacheController.EXPECT().DetermineInvalidation(gomock.Any(), storeID).Return(invalidationTime)
		_, _, err := cmd.Execute(context.Background(), &CheckCommandParams{
			StoreID:  storeID,
			TupleKey: tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
		})
		require.NoError(t, err)
	})
}

func TestBuildCheckContext(t *testing.T) {
	model := parser.MustTransformDSLToProto(`
model
	schema 1.1
type user
type doc
	relations
		define viewer: [user]
`)
	ts, err := typesystem.New(model)
	require.NoError(t, err)
	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	contextualTuples := []*openfgav1.TupleKey{}
	ctx := context.Background()

	// act
	actualContext := buildCheckContext(ctx, ts, mockDatastore, 1, contextualTuples)

	// assert
	tsFromContext, ok := typesystem.TypesystemFromContext(actualContext)
	require.True(t, ok)
	require.Equal(t, ts, tsFromContext)

	dsFromContext, ok := storage.RelationshipTupleReaderFromContext(actualContext)
	require.True(t, ok)
	// first layer is the concurrency tuple reader
	bctr, ok := dsFromContext.(*storagewrappers.BoundedConcurrencyTupleReader)
	require.True(t, ok)

	// second layer is the combined tuple reader
	_, ok = bctr.RelationshipTupleReader.(*storagewrappers.CombinedTupleReader)
	require.True(t, ok)
}

func TestCheckCommandErrorToServerError(t *testing.T) {
	testcases := map[string]struct {
		inputError    error
		expectedError error
	}{
		`1`: {
			inputError:    graph.ErrResolutionDepthExceeded,
			expectedError: serverErrors.AuthorizationModelResolutionTooComplex,
		},
		`2`: {
			inputError:    condition.ErrEvaluationFailed,
			expectedError: serverErrors.ValidationError(condition.ErrEvaluationFailed),
		},
		`3`: {
			inputError:    &ThrottledError{},
			expectedError: serverErrors.ThrottledTimeout,
		},
		`4`: {
			inputError:    context.DeadlineExceeded,
			expectedError: serverErrors.RequestDeadlineExceeded,
		},
		`5`: {
			inputError: &InvalidTupleError{Cause: errors.New("oh no")},
			expectedError: serverErrors.HandleTupleValidateError(
				&tuple.InvalidTupleError{
					Cause: &InvalidTupleError{Cause: errors.New("oh no")},
				},
			),
		},
		`6`: {
			inputError:    &InvalidRelationError{Cause: errors.New("oh no")},
			expectedError: serverErrors.ValidationError(&InvalidRelationError{Cause: errors.New("oh no")}),
		},
		`7`: {
			inputError:    ofga_errors.ErrUnknown,
			expectedError: ofga_errors.ErrUnknown,
		},
	}

	for name, testCase := range testcases {
		t.Run(name, func(t *testing.T) {
			actualError := CheckCommandErrorToServerError(testCase.inputError)
			require.ErrorIs(t, actualError, testCase.expectedError)
		})
	}
}
