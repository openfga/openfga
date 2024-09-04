package commands

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/pkg/testutils"

	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/errors"
	"github.com/openfga/openfga/internal/graph"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
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

	t.Run("validates_store_id", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, nil)
		_, _, err := cmd.Execute(context.Background(), &openfgav1.CheckRequest{
			StoreId: "invalid",
		})
		require.ErrorContains(t, err, "invalid CheckRequest.StoreId: value does not match regex pattern \"^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$\"")
	})

	t.Run("validates_model_id", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, nil)
		_, _, err := cmd.Execute(context.Background(), &openfgav1.CheckRequest{
			StoreId: ulid.Make().String(),
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     "user:1",
				Relation: "viewer",
				Object:   "invalid:1",
			},
			AuthorizationModelId: "invalid",
		})
		require.ErrorContains(t, err, "invalid CheckRequest.AuthorizationModelId: value does not match regex pattern \"^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$\"")
	})

	t.Run("validates_input_user", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, ts)
		_, _, err := cmd.Execute(context.Background(), &openfgav1.CheckRequest{
			StoreId:              ulid.Make().String(),
			AuthorizationModelId: ulid.Make().String(),
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
		_, _, err := cmd.Execute(context.Background(), &openfgav1.CheckRequest{
			StoreId:              ulid.Make().String(),
			AuthorizationModelId: ulid.Make().String(),
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
		_, _, err := cmd.Execute(context.Background(), &openfgav1.CheckRequest{
			StoreId:              ulid.Make().String(),
			AuthorizationModelId: ulid.Make().String(),
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
		_, _, err := cmd.Execute(context.Background(), &openfgav1.CheckRequest{
			StoreId:              ulid.Make().String(),
			AuthorizationModelId: ulid.Make().String(),
			TupleKey:             tuple.NewCheckRequestTupleKey("invalid:1", "viewer", "user:1"),
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
		_, _, err := cmd.Execute(context.Background(), &openfgav1.CheckRequest{
			StoreId:              ulid.Make().String(),
			AuthorizationModelId: ulid.Make().String(),
			TupleKey:             tuple.NewCheckRequestTupleKey("doc:1", "viewer_computed", "user:1"),
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
		_, _, err := cmd.Execute(context.Background(), &openfgav1.CheckRequest{
			StoreId:              ulid.Make().String(),
			AuthorizationModelId: ulid.Make().String(),
			TupleKey:             tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
		})
		require.NoError(t, err)
	})

	t.Run("no_validation_error_but_call_to_resolver_fails", func(t *testing.T) {
		cmd := NewCheckCommand(mockDatastore, mockCheckResolver, ts)
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.ErrUnknown)
		_, _, err := cmd.Execute(context.Background(), &openfgav1.CheckRequest{
			StoreId:              ulid.Make().String(),
			AuthorizationModelId: ulid.Make().String(),
			TupleKey:             tuple.NewCheckRequestTupleKey("doc:1", "viewer", "user:1"),
		})
		require.ErrorIs(t, err, errors.ErrUnknown)
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
	ts := typesystem.New(model)
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

func TestTranslateError(t *testing.T) {
	throttledRequestMetadata := &graph.ResolveCheckRequestMetadata{
		WasThrottled: &atomic.Bool{},
	}
	throttledRequestMetadata.WasThrottled.Store(true)

	nonThrottledRequestMedata := &graph.ResolveCheckRequestMetadata{
		WasThrottled: &atomic.Bool{},
	}
	nonThrottledRequestMedata.WasThrottled.Store(false)

	testcases := map[string]struct {
		inputError    error
		reqMetadata   *graph.ResolveCheckRequestMetadata
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
			inputError:    context.DeadlineExceeded,
			reqMetadata:   throttledRequestMetadata,
			expectedError: serverErrors.ThrottledTimeout,
		},
		`4`: {
			inputError:    context.DeadlineExceeded,
			reqMetadata:   nonThrottledRequestMedata,
			expectedError: serverErrors.RequestDeadlineExceeded,
		},
		`5`: {
			inputError:    errors.ErrUnknown,
			expectedError: errors.ErrUnknown,
		},
	}

	for name, test := range testcases {
		t.Run(name, func(t *testing.T) {
			actualError := translateError(test.reqMetadata, test.inputError)
			require.ErrorIs(t, actualError, test.expectedError)
		})
	}
}
