package listusers

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/testing/protocmp"
)

type ListUsersTests []struct {
	name          string
	req           *openfgav1.ListUsersRequest
	model         string
	tuples        []*openfgav1.TupleKey
	expectedUsers []*openfgav1.User
	expectedError error
}

func TestListUsersDirectRelationship(t *testing.T) {
	model := `model
	schema 1.1
	type user
	type document
		relations
			define editor: [user]
			define viewer: editor`

	tests := ListUsersTests{
		{
			name: "direct_relationship",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
			},
			model: model,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "editor", "user:will"),
				tuple.NewTupleKey("document:1", "editor", "user:maria"),
				tuple.NewTupleKey("document:2", "editor", "user:jon"),
			},
			expectedUsers: []*openfgav1.User{
				{
					User: &openfgav1.User_Object{
						Object: &openfgav1.Object{Type: "user", Id: "will"},
					},
				},
				{
					User: &openfgav1.User_Object{
						Object: &openfgav1.Object{Type: "user", Id: "will"},
					},
				},
			},
		},
		{
			name: "direct_relationship_unapplicable_filter",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "folder",
					},
				},
			},
			model:         model,
			tuples:        []*openfgav1.TupleKey{},
			expectedUsers: []*openfgav1.User{},
		},
		{
			name: "direct_relationship_no_tuples",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
			},
			model:         model,
			tuples:        []*openfgav1.TupleKey{},
			expectedUsers: []*openfgav1.User{},
		},
		{
			name: "direct_relationship_unapplicable_tuples",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
			},
			model: model,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:2", "viewer", "user:will"),
				tuple.NewTupleKey("document:3", "viewer", "user:will"),
				tuple.NewTupleKey("document:4", "viewer", "user:will"),
			},
			expectedUsers: []*openfgav1.User{},
		},
		{
			name: "direct_relationship_contextual_tuples",
			req: &openfgav1.ListUsersRequest{
				Object: &openfgav1.Object{Type: "document", Id: "1"},
				ContextualTuples: &openfgav1.ContextualTupleKeys{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey("document:1", "viewer", "user:will"),
						tuple.NewTupleKey("document:1", "viewer", "user:maria"),
						tuple.NewTupleKey("document:2", "viewer", "user:jon"),
					},
				},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
			},
			model:  model,
			tuples: []*openfgav1.TupleKey{},
			expectedUsers: []*openfgav1.User{
				{
					User: &openfgav1.User_Object{
						Object: &openfgav1.Object{Type: "user", Id: "will"},
					},
				},
				{
					User: &openfgav1.User_Object{
						Object: &openfgav1.Object{Type: "user", Id: "maria"},
					},
				},
			},
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsers(t *testing.T) {
	tests := ListUsersTests{
		{
			name: "direct_relationship_through_userset",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
			},
			model: `model
            schema 1.1
			type user
			type group
			  relations
			    define member: [user]
			type document
			  relations
			    define viewer: [group#member]
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("group:eng", "member", "user:jon"),
			},
			expectedUsers: []*openfgav1.User{
				{
					User: &openfgav1.User_Object{
						Object: &openfgav1.Object{Type: "user", Id: "jon"},
					},
				},
			},
		},
		{
			name: "direct_relationship_through_multiple_usersets",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
			},
			model: `model
            schema 1.1
			type user
			type group
			  relations
			    define member: [user, group#member]
			type document
			  relations
			    define viewer: [group#member]
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("group:eng", "member", "user:hawker"),
				tuple.NewTupleKey("group:eng", "member", "group:fga#member"),
				tuple.NewTupleKey("group:fga", "member", "user:jon"),
			},
			expectedUsers: []*openfgav1.User{
				{
					User: &openfgav1.User_Object{
						Object: &openfgav1.Object{Type: "user", Id: "jon"},
					},
				},
				{
					User: &openfgav1.User_Object{
						Object: &openfgav1.Object{Type: "user", Id: "hawker"},
					},
				},
			},
		},
		{
			name: "rewritten_direct_relationship_through_computed_userset",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
			},
			model: `model
            schema 1.1
			type user
			type document
			  relations
			    define editor: [user]
			    define viewer: editor
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "editor", "user:jon"),
			},
			expectedUsers: []*openfgav1.User{
				{
					User: &openfgav1.User_Object{
						Object: &openfgav1.Object{Type: "user", Id: "jon"},
					},
				},
			},
		},
		{
			name: "rewritten_direct_relationship_through_ttu",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
			},
			model: `model
            schema 1.1
			type user
			type folder
			  relations
			    define viewer: [user]
			type document
			  relations
			    define parent: [folder]
			    define viewer: viewer from parent
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:x"),
				tuple.NewTupleKey("folder:x", "viewer", "user:jon"),
			},
			expectedUsers: []*openfgav1.User{
				{
					User: &openfgav1.User_Object{
						Object: &openfgav1.Object{Type: "user", Id: "jon"},
					},
				},
			},
		},
		{
			name: "userset_defines_itself",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "document",
					},
				},
			},
			model: `model
            schema 1.1
			type user
			type document
			  relations
			    define viewer: [user]
			`,
			tuples: []*openfgav1.TupleKey{},
			expectedUsers: []*openfgav1.User{
				{
					User: &openfgav1.User_Object{
						Object: &openfgav1.Object{Type: "document", Id: "1"},
					},
				},
			},
		},
		// {
		// 	name: "direct_userset_relationship_with_cycle",
		// 	req: &ListUsersRequest{
		// 		Object:               &openfgav1.Object{Type: "document", Id: "1"},
		// 		Relation:             "viewer",
		// 		TargetUserObjectTypes: []string{"user"},
		// 	},
		// 	model: `model
		//schema 1.1
		// 	type user

		// 	type group
		// 	  relations
		// 	    define member: [user, group#member]

		// 	type document
		// 	  relations
		// 	    define viewer: [group#member]
		// 	`,
		// 	tuples: []*openfgav1.TupleKey{
		// 		tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
		// 		tuple.NewTupleKey("group:eng", "member", "group:fga#member"),
		// 		tuple.NewTupleKey("group:fga", "member", "group:eng#member"),
		// 		tuple.NewTupleKey("group:fga", "member", "user:jon"),
		// 	},
		// 	expectedError: fmt.Errorf("cycle detected"),
		// },
	}
	tests.runListUsersTestCases(t)
}

func (testCases ListUsersTests) runListUsersTestCases(t *testing.T) {
	storeID := ulid.Make().String()

	for _, test := range testCases {
		ds := memory.New()
		defer ds.Close()
		model := testutils.MustTransformDSLToProtoWithID(test.model)

		typesys, err := typesystem.NewAndValidate(context.Background(), model)
		require.NoError(t, err)

		err = ds.WriteAuthorizationModel(context.Background(), storeID, model)
		require.NoError(t, err)

		if len(test.tuples) > 0 {
			err = ds.Write(context.Background(), storeID, nil, test.tuples)
			require.NoError(t, err)
		}

		l := NewListUsersQuery(ds)

		ctx := typesystem.ContextWithTypesystem(context.Background(), typesys)

		t.Run(test.name, func(t *testing.T) {
			test.req.AuthorizationModelId = model.GetId()
			test.req.StoreId = storeID

			resp, err := l.ListUsers(ctx, test.req)
			require.ErrorIs(t, err, test.expectedError)

			ignoredFieldsOpts := protocmp.IgnoreFields(protoadapt.MessageV2Of(&openfgav1.Object{}), "type", "id")

			returnedUsers := resp.GetUsers()
			expected := test.expectedUsers

			if diff := cmp.Diff(expected, returnedUsers, ignoredFieldsOpts, protocmp.Transform()); diff != "" {
				require.FailNowf(t, "(-want +got):\n%s", diff)
			}
		})
	}
}
