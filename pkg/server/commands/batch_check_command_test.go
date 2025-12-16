package commands

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestBatchCheckCommand(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	maxChecks := uint32(50)
	mockController := gomock.NewController(t)
	defer mockController.Finish()
	ds := mockstorage.NewMockOpenFGADatastore(mockController)

	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1
		type user
		type doc
			relations
				define viewer: [user]
	`)
	ts, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)

	t.Run("calls_check_once_for_each_tuple_in_batch", func(t *testing.T) {
		mockCheckResolver := graph.NewMockCheckResolver(mockController)
		cmd := NewBatchCheckCommand(ds, mockCheckResolver, ts)
		numChecks := int(maxChecks)
		checks := make([]*openfgav1.BatchCheckItem, numChecks)
		for i := 0; i < numChecks; i++ {
			checks[i] = &openfgav1.BatchCheckItem{
				TupleKey: &openfgav1.CheckRequestTupleKey{
					Object:   fmt.Sprintf("doc:doc%d", i),
					Relation: "viewer",
					User:     "user:justin",
				},
				CorrelationId: fmt.Sprintf("fakeid%d", i),
			}
		}

		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
			Times(numChecks).
			DoAndReturn(func(_ any, _ any) (*graph.ResolveCheckResponse, error) {
				// Need this DoAndReturn or the test will use a single instance of &graph.ResolveCheckResponse{}
				// which will create a race condition
				return &graph.ResolveCheckResponse{}, nil
			})

		params := &BatchCheckCommandParams{
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			Checks:               checks,
			StoreID:              ulid.Make().String(),
		}

		result, meta, err := cmd.Execute(context.Background(), params)

		require.NoError(t, err)
		require.Equal(t, len(result), numChecks)

		// No actual datastore queries should have been run since we're mocking
		require.Equal(t, 0, int(meta.DatastoreQueryCount))
		require.Equal(t, 0, meta.DuplicateCheckCount)
	})

	t.Run("returns_a_result_for_each_correlation_id", func(t *testing.T) {
		mockCheckResolver := graph.NewMockCheckResolver(mockController)
		cmd := NewBatchCheckCommand(ds, mockCheckResolver, ts)
		numChecks := 10
		checks := make([]*openfgav1.BatchCheckItem, numChecks)
		var ids []string
		for i := 0; i < numChecks; i++ {
			correlationID := fmt.Sprintf("fakeid%d", i)
			ids = append(ids, correlationID)
			checks[i] = &openfgav1.BatchCheckItem{
				TupleKey: &openfgav1.CheckRequestTupleKey{
					Object:   "doc:doc1",
					Relation: "viewer",
					User:     "user:justin",
				},
				CorrelationId: correlationID,
			}
		}

		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			Return(&graph.ResolveCheckResponse{}, nil)

		params := &BatchCheckCommandParams{
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			Checks:               checks,
			StoreID:              ulid.Make().String(),
		}

		result, meta, err := cmd.Execute(context.Background(), params)
		require.NoError(t, err)

		// Quantity of correlation IDs should be equal
		require.Len(t, result, len(ids))
		require.Equal(t, 9, meta.DuplicateCheckCount)

		// And each ID should appear in the response
		for _, id := range ids {
			_, ok := result[CorrelationID(id)]
			require.True(t, ok)
		}
	})

	t.Run("fails_with_validation_error_if_too_many_tuples", func(t *testing.T) {
		mockCheckResolver := graph.NewMockCheckResolver(mockController)
		cmd := NewBatchCheckCommand(
			ds,
			mockCheckResolver,
			ts,
			WithBatchCheckMaxChecksPerBatch(maxChecks),
		)
		numChecks := int(maxChecks) + 1
		checks := make([]*openfgav1.BatchCheckItem, numChecks)
		for i := 0; i < numChecks; i++ {
			checks[i] = &openfgav1.BatchCheckItem{
				TupleKey: &openfgav1.CheckRequestTupleKey{
					Object:   "doc:doc1",
					Relation: "viewer",
					User:     "user:justin",
				},
				CorrelationId: fmt.Sprintf("fakeid%d", i),
			}
		}

		params := &BatchCheckCommandParams{
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			Checks:               checks,
			StoreID:              ulid.Make().String(),
		}

		_, _, err := cmd.Execute(context.Background(), params)

		var expectedErr *BatchCheckValidationError
		require.ErrorAs(t, err, &expectedErr)
	})

	t.Run("fails_with_validation_error_if_no_tuples", func(t *testing.T) {
		mockCheckResolver := graph.NewMockCheckResolver(mockController)
		cmd := NewBatchCheckCommand(ds, mockCheckResolver, ts)
		params := &BatchCheckCommandParams{
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			Checks:               []*openfgav1.BatchCheckItem{},
			StoreID:              ulid.Make().String(),
		}

		_, _, err := cmd.Execute(context.Background(), params)

		var expectedErr *BatchCheckValidationError
		require.ErrorAs(t, err, &expectedErr)
	})

	t.Run("fails_with_validation_error_if_duplicated_correlation_ids", func(t *testing.T) {
		mockCheckResolver := graph.NewMockCheckResolver(mockController)
		cmd := NewBatchCheckCommand(ds, mockCheckResolver, ts)
		numChecks := 2
		checks := make([]*openfgav1.BatchCheckItem, numChecks)
		for i := 0; i < numChecks; i++ {
			checks[i] = &openfgav1.BatchCheckItem{
				TupleKey: &openfgav1.CheckRequestTupleKey{
					Object:   "doc:doc1",
					Relation: "viewer",
					User:     "user:justin",
				},
				CorrelationId: "hardcoded_id",
			}
		}

		params := &BatchCheckCommandParams{
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			Checks:               checks,
			StoreID:              ulid.Make().String(),
		}

		_, _, err := cmd.Execute(context.Background(), params)
		require.ErrorContains(t, err, "hardcoded_id")

		var expectedErr *BatchCheckValidationError
		require.ErrorAs(t, err, &expectedErr)
	})

	t.Run("fails_with_validation_error_if_empty_correlation_id", func(t *testing.T) {
		mockCheckResolver := graph.NewMockCheckResolver(mockController)
		cmd := NewBatchCheckCommand(ds, mockCheckResolver, ts)
		numChecks := 3
		checks := make([]*openfgav1.BatchCheckItem, numChecks)
		for i := 0; i < numChecks; i++ {
			checks[i] = &openfgav1.BatchCheckItem{
				TupleKey: &openfgav1.CheckRequestTupleKey{
					Object:   "doc:doc1",
					Relation: "viewer",
					User:     "user:justin",
				},
				CorrelationId: "",
			}
		}

		params := &BatchCheckCommandParams{
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			Checks:               checks,
			StoreID:              ulid.Make().String(),
		}

		_, _, err := cmd.Execute(context.Background(), params)

		var expectedErr *BatchCheckValidationError
		require.ErrorAs(t, err, &expectedErr)
		require.ErrorContains(t, err, "received empty correlation id for tuple")
	})

	t.Run("returns_errors_per_check_if_context_cancelled", func(t *testing.T) {
		mockCheckResolver := graph.NewMockCheckResolver(mockController)
		cmd := NewBatchCheckCommand(ds, mockCheckResolver, ts)
		numChecks := 3
		checks := make([]*openfgav1.BatchCheckItem, numChecks)
		for i := 0; i < numChecks; i++ {
			checks[i] = &openfgav1.BatchCheckItem{
				TupleKey: &openfgav1.CheckRequestTupleKey{
					Object:   "doc:doc1",
					Relation: "viewer",
					User:     "user:justin",
				},
				CorrelationId: fmt.Sprintf("fakeid%d", i),
			}
		}

		params := &BatchCheckCommandParams{
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			Checks:               checks,
			StoreID:              ulid.Make().String(),
		}

		// create context and cancel immediately
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		cancel()

		response, meta, err := cmd.Execute(ctx, params)
		require.NoError(t, err) // request itself should not error

		// But all checks should return a context cancelled error
		for _, v := range response {
			require.Nil(t, v.CheckResponse)
			require.Equal(t, v.Err, context.Canceled)
		}

		// response should have 1 key per check
		require.Equal(t, len(response), numChecks)

		// The context was canceled, but we still marked 2 duplicates before processing began
		require.Equal(t, 2, meta.DuplicateCheckCount)
		require.EqualValues(t, 0, meta.DatastoreQueryCount)
	})

	t.Run("uses_command_cache_to_resolve_dupe_checks", func(t *testing.T) {
		mockCheckResolver := graph.NewMockCheckResolver(mockController)
		cmd := NewBatchCheckCommand(ds, mockCheckResolver, ts)

		justinTuple := &openfgav1.CheckRequestTupleKey{
			Object:   "doc:doc1",
			Relation: "viewer",
			User:     "user:justin",
		}

		ewanTuple := &openfgav1.CheckRequestTupleKey{
			Object:   "doc:doc1",
			Relation: "viewer",
			User:     "user:ewan",
		}

		// 4 items but only 2 distinct checks
		checks := []*openfgav1.BatchCheckItem{
			{TupleKey: justinTuple, CorrelationId: "qwe"},
			{TupleKey: justinTuple, CorrelationId: "rty"},
			{TupleKey: ewanTuple, CorrelationId: "asd"},
			{TupleKey: ewanTuple, CorrelationId: "fgh"},
		}

		// The check resolver should only receive two distinct checks
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
			Times(2).
			DoAndReturn(func(_ any, _ any) (*graph.ResolveCheckResponse, error) {
				// Need this DoAndReturn or the test will use a single instance of &graph.ResolveCheckResponse{}
				// which will create a race condition
				return &graph.ResolveCheckResponse{}, nil
			})

		params := &BatchCheckCommandParams{
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			Checks:               checks,
			StoreID:              ulid.Make().String(),
		}

		result, meta, err := cmd.Execute(context.Background(), params)

		require.NoError(t, err)
		require.Len(t, result, len(checks))
		require.Equal(t, 2, meta.DuplicateCheckCount)
	})

	t.Run("handles_check_failures_gracefully", func(t *testing.T) {
		mockCheckResolver := graph.NewMockCheckResolver(mockController)
		cmd := NewBatchCheckCommand(ds, mockCheckResolver, ts)

		justinTuple := &openfgav1.CheckRequestTupleKey{
			Object:   "doc:doc1",
			Relation: "viewer",
			User:     "user:justin",
		}

		// return error
		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
			Times(1).
			Return(nil, fmt.Errorf("some error"))

		params := &BatchCheckCommandParams{
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			Checks:               []*openfgav1.BatchCheckItem{{TupleKey: justinTuple, CorrelationId: "qwe"}},
			StoreID:              ulid.Make().String(),
		}

		result, meta, err := cmd.Execute(context.Background(), params)

		// The check errored, this should be 0
		require.EqualValues(t, 0, meta.DatastoreQueryCount)

		// but BatchCheck as a whole did not error
		require.NoError(t, err)
		require.Len(t, result, 1)
	})
}

func BenchmarkBatchCheckCommand(b *testing.B) {
	ds := memory.New()
	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1
		type user
		type doc
			relations
				define viewer: [user]
	`)
	ts, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(b, err)
	checkResolver, checkResolverCloser, err := graph.NewOrderedCheckResolvers().Build()
	require.NoError(b, err)
	b.Cleanup(checkResolverCloser)

	maxChecks := config.DefaultMaxChecksPerBatchCheck
	cmd := NewBatchCheckCommand(
		ds,
		checkResolver,
		ts,
		WithBatchCheckMaxChecksPerBatch(uint32(maxChecks)),
	)

	checks := make([]*openfgav1.BatchCheckItem, maxChecks)
	for i := 0; i < maxChecks; i++ {
		correlationID := fmt.Sprintf("fakeid%d", i)
		checks[i] = &openfgav1.BatchCheckItem{
			TupleKey: &openfgav1.CheckRequestTupleKey{
				Object:   "doc:doc1",
				Relation: "viewer",
				User:     "user:justin",
			},
			CorrelationId: correlationID,
		}
	}

	params := &BatchCheckCommandParams{
		AuthorizationModelID: ts.GetAuthorizationModelID(),
		Checks:               checks,
		StoreID:              ulid.Make().String(),
	}

	b.Run("benchmark_batch_check_with_max_checks", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, err := cmd.Execute(context.Background(), params)
			require.NoError(b, err)
		}
	})
}
