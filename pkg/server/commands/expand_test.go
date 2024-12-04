package commands

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlite"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestExpand(t *testing.T) {
	modelStr := `
		model
			schema 1.1
		type user

		type document
			relations
				define viewer: [user]
				define can_view: viewer
	`

	var tuples []string
	t.Run("invalid_contextual_tuple_fails_validation", func(t *testing.T) {
		contextualTuples := []*openfgav1.TupleKey{
			{Object: "document:1", Relation: "invalid_relation", User: "user:bob"},
		}
		expandTupleKey := &openfgav1.ExpandRequestTupleKey{
			Object:   "document:1",
			Relation: "can_view",
		}
		expectedErr := serverErrors.HandleTupleValidateError(
			&tuple.InvalidTupleError{
				Cause: &tuple.RelationNotFoundError{
					Relation: "invalid_relation",
					TypeName: "document",
				},
				TupleKey: &openfgav1.TupleKey{
					User:     "user:bob",
					Relation: "invalid_relation",
					Object:   "document:1",
				},
			},
		)

		ds := sqlite.MustNewInMemory()
		t.Cleanup(ds.Close)
		storeID, model := storagetest.BootstrapFGAStore(t, ds, modelStr, tuples)
		ctx := context.Background()
		ts, err := typesystem.NewAndValidate(
			ctx,
			model,
		)
		ctx = typesystem.ContextWithTypesystem(ctx, ts)
		require.NoError(t, err)

		expandQuery := NewExpandQuery(ds)

		_, err = expandQuery.Execute(ctx, &openfgav1.ExpandRequest{
			StoreId:  storeID,
			TupleKey: expandTupleKey,
			ContextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: contextualTuples,
			},
		})
		require.Equal(t, expectedErr, err)
	})

	t.Run("expand_with_only_contextual_tuples", func(t *testing.T) {
		contextualTuples := []*openfgav1.TupleKey{{Object: "document:1", Relation: "viewer", User: "user:bob"}}
		expandTupleKey := &openfgav1.ExpandRequestTupleKey{
			Object: "document:1", Relation: "viewer",
		}

		ds := sqlite.MustNewInMemory()
		t.Cleanup(ds.Close)
		storeID, model := storagetest.BootstrapFGAStore(t, ds, modelStr, tuples)
		ctx := context.Background()
		ts, err := typesystem.NewAndValidate(
			ctx,
			model,
		)
		ctx = typesystem.ContextWithTypesystem(ctx, ts)
		require.NoError(t, err)

		expandQuery := NewExpandQuery(ds)

		resp, err := expandQuery.Execute(ctx, &openfgav1.ExpandRequest{
			StoreId:  storeID,
			TupleKey: expandTupleKey,
			ContextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: contextualTuples,
			},
		})

		require.NoError(t, err)
		root := resp.GetTree().GetRoot()
		require.Equal(t, "document:1#viewer", root.GetName())

		finalUsers := root.GetLeaf().GetUsers().GetUsers()
		require.Equal(t, []string{"user:bob"}, finalUsers)
	})

	t.Run("multiple_contextual_tuples_expand_correctly", func(t *testing.T) {
		contextualTuples := []*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "user:bob"},
			{Object: "document:1", Relation: "viewer", User: "user:alice"},
		}
		expandTupleKey := &openfgav1.ExpandRequestTupleKey{
			Object:   "document:1",
			Relation: "viewer",
		}

		ds := sqlite.MustNewInMemory()
		t.Cleanup(ds.Close)
		storeID, model := storagetest.BootstrapFGAStore(t, ds, modelStr, tuples)
		ctx := context.Background()
		ts, err := typesystem.NewAndValidate(
			ctx,
			model,
		)
		ctx = typesystem.ContextWithTypesystem(ctx, ts)
		require.NoError(t, err)

		expandQuery := NewExpandQuery(ds)

		resp, err := expandQuery.Execute(ctx, &openfgav1.ExpandRequest{
			StoreId:  storeID,
			TupleKey: expandTupleKey,
			ContextualTuples: &openfgav1.ContextualTupleKeys{
				TupleKeys: contextualTuples,
			},
		})

		require.NoError(t, err)

		root := resp.GetTree().GetRoot()
		require.Equal(t, "document:1#viewer", root.GetName())

		finalUsers := root.GetLeaf().GetUsers().GetUsers()
		require.ElementsMatch(t, finalUsers, []string{"user:bob", "user:alice"})
	})

	t.Run("respects_consistency_preference", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		ctx := context.Background()

		// arrange: write model
		model := testutils.MustTransformDSLToProtoWithID(modelStr)

		typesys, err := typesystem.NewAndValidate(ctx, model)
		require.NoError(t, err)

		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		query := NewExpandQuery(mockDatastore)

		// No Consistency Specified in this request
		expandRequest := &openfgav1.ExpandRequest{
			StoreId: ulid.Make().String(),
			TupleKey: &openfgav1.ExpandRequestTupleKey{
				Object: "document:1", Relation: "viewer",
			},
		}
		unspecified := storage.ReadOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		}

		// expect to receive UNSPECIFIED
		mockDatastore.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), unspecified).Times(1)
		_, err = query.Execute(ctx, expandRequest)
		require.NoError(t, err)

		// Now run it again with HIGHER_CONSISTENCY
		expandRequest.Consistency = openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY
		higherConsistency := storage.ReadOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
		}

		// expect to receive HIGHER_CONSISTENCY
		mockDatastore.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), higherConsistency).Times(1)
		_, err = query.Execute(ctx, &openfgav1.ExpandRequest{
			StoreId: ulid.Make().String(),
			TupleKey: &openfgav1.ExpandRequestTupleKey{
				Object: "document:1", Relation: "viewer",
			},
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		})
		require.NoError(t, err)
	})
}
