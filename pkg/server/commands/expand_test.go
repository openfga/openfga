package commands

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage/memory"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
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

		ds := memory.New()
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

		ds := memory.New()
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

		ds := memory.New()
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
}
