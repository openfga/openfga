package commands

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/cachecontroller"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/graph"
	mockstorage "github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestBatchCheckCommand(t *testing.T) {
	maxChecks := uint32(50)
	mockController := gomock.NewController(t)
	defer mockController.Finish()
	ds := mockstorage.NewMockOpenFGADatastore(mockController)

	mockCheckResolver := graph.NewMockCheckResolver(mockController)
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

	cmd := NewBatchCheckCommand(
		ds,
		mockCheckResolver,
		ts,
		WithBatchCheckMaxChecksPerBatch(maxChecks),
		WithBatchCheckCommandCacheController(cachecontroller.NewNoopCacheController()),
	)

	t.Run("calls_check_once_for_each_tuple_in_batch", func(t *testing.T) {
		numChecks := 50
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

		mockCheckResolver.EXPECT().ResolveCheck(gomock.Any(), gomock.Any()).
			Times(numChecks).
			Return(nil, nil)

		params := &BatchCheckCommandParams{
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			Checks:               checks,
			StoreID:              ulid.Make().String(),
		}

		result, err := cmd.Execute(context.Background(), params)
		require.NoError(t, err)
		require.Equal(t, len(result), numChecks)
	})

	t.Run("returns_a_result_for_each_correlation_id", func(t *testing.T) {
		numChecks := 10
		checks := make([]*openfgav1.BatchCheckItem, numChecks)
		var ids []string
		for i := 0; i < numChecks; i++ {
			correlationID := fmt.Sprintf("fakeid%d", i)
			ids = append(ids, correlationID) // todo here
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
			Times(numChecks).
			Return(nil, nil)

		params := &BatchCheckCommandParams{
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			Checks:               checks,
			StoreID:              ulid.Make().String(),
		}

		result, err := cmd.Execute(context.Background(), params)
		require.NoError(t, err)
		require.Equal(t, len(result), numChecks)
	})

	t.Run("fails_with_validation_error_if_too_many_tuples", func(t *testing.T) {
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

		_, err := cmd.Execute(context.Background(), params)
		require.Error(t, err)
	})

	t.Run("fails_with_validation_error_if_duplicated_correlation_ids", func(t *testing.T) {
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

		_, err := cmd.Execute(context.Background(), params)
		require.ErrorContains(t, err, "hardcoded_id")
	})
}

// then some where we assert a specific allowed true and allowed false
// then some where we hit a known error for individual check (like bad relation?)
