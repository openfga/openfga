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
	tests := []struct {
		name             string
		modelStr         string
		tuples           []string
		contextualTuples []*openfgav1.TupleKey
		expandTupleKey   *openfgav1.ExpandRequestTupleKey
		expected         *openfgav1.UsersetTree_Node
		expectedErr      error
	}{
		{
			name: "invalid_contextual_tuple_fails_validation",
			modelStr: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [user]
						define can_view: viewer`,
			tuples: []string{},
			contextualTuples: []*openfgav1.TupleKey{
				{
					Object:   "document:1",
					Relation: "invalid_relation",
					User:     "user:bob",
				},
			},
			expandTupleKey: &openfgav1.ExpandRequestTupleKey{
				Object:   "document:1",
				Relation: "can_view",
			},
			expectedErr: serverErrors.HandleTupleValidateError(
				&tuple.InvalidTupleError{
					Cause: &tuple.RelationNotFoundError{
						Relation: "invalid_relation",
						TypeName: "document",
					},
					TupleKey: &openfgav1.TupleKey{
						User:     "user:bob",
						Relation: "invalid_relation",
						Object:   "document:1",
					}}),
		},
		{
			name: "expand_with_only_contextual_tuples",
			modelStr: `
				model
					schema 1.1

				type user

				type document
					relations
						define viewer: [user]
						define can_view: viewer
			`,
			tuples: []string{},
			contextualTuples: []*openfgav1.TupleKey{
				{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:bob",
				},
			},
			expandTupleKey: &openfgav1.ExpandRequestTupleKey{
				Object:   "document:1",
				Relation: "viewer",
			},
			expected: &openfgav1.UsersetTree_Node{
				Name: "document:1#viewer",
				Value: &openfgav1.UsersetTree_Node_Leaf{
					Leaf: &openfgav1.UsersetTree_Leaf{
						Value: &openfgav1.UsersetTree_Leaf_Users{
							Users: &openfgav1.UsersetTree_Users{
								Users: []string{"user:bob"},
							},
						},
					},
				},
			},
			expectedErr: nil,
		},
		{
			name: "multiple_contextual_tuples_expand_correctly",
			modelStr: `
					model
						schema 1.1
					type user

					type document
					relations
						define viewer: [user]
						define can_view: viewer`,
			tuples: []string{},
			contextualTuples: []*openfgav1.TupleKey{
				{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:bob",
				},
				{
					Object:   "document:1",
					Relation: "viewer",
					User:     "user:alice",
				},
			},
			expandTupleKey: &openfgav1.ExpandRequestTupleKey{
				Object:   "document:1",
				Relation: "viewer",
			},
			expected: &openfgav1.UsersetTree_Node{
				Name: "document:1#viewer",
				Value: &openfgav1.UsersetTree_Node_Leaf{
					Leaf: &openfgav1.UsersetTree_Leaf{
						Value: &openfgav1.UsersetTree_Leaf_Users{
							Users: &openfgav1.UsersetTree_Users{
								Users: []string{"user:bob", "user:alice"},
							},
						},
					},
				},
			},
			expectedErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ds := memory.New()
			t.Cleanup(ds.Close)
			storeID, model := storagetest.BootstrapFGAStore(t, ds, tc.modelStr, tc.tuples)
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
				TupleKey: tc.expandTupleKey,
				ContextualTuples: &openfgav1.ContextualTupleKeys{
					TupleKeys: tc.contextualTuples,
				},
			})

			if tc.expectedErr != nil {
				require.ErrorIs(t, err, tc.expectedErr)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, tc.expected, resp.GetTree().GetRoot())
		})
	}
}
