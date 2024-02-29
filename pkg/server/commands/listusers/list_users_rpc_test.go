package listusers

import (
	"context"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/testing/protocmp"
)

func TestListUsers(t *testing.T) {

	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	tests := []struct {
		name            string
		req             *openfgav1.ListUsersRequest
		typeDefinitions []*openfgav1.TypeDefinition
		tuples          []*openfgav1.TupleKey
		expectedObjects []*openfgav1.Object
		expectedError   error
	}{
		{
			name: "direct_relationship",
			req: &openfgav1.ListUsersRequest{
				StoreId:               storeID,
				AuthorizationModelId:  modelID,
				Object:                &openfgav1.Object{Type: "document", Id: "1"},
				Relation:              "viewer",
				TargetUserObjectTypes: []string{"user"},
			},
			typeDefinitions: parser.MustParse(`
			type user
			type document
			  relations
			    define viewer: [user] as self
			`),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
			expectedObjects: []*openfgav1.Object{
				{Type: "user", Id: "jon"},
			},
		},
		{
			name: "direct_relationship_through_userset",
			req: &openfgav1.ListUsersRequest{
				StoreId:               storeID,
				AuthorizationModelId:  modelID,
				Object:                &openfgav1.Object{Type: "document", Id: "1"},
				Relation:              "viewer",
				TargetUserObjectTypes: []string{"user"},
			},
			typeDefinitions: parser.MustParse(`
			type user
			type group
			  relations
			    define member: [user] as self
			type document
			  relations
			    define viewer: [group#member] as self
			`),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("group:eng", "member", "user:jon"),
			},
			expectedObjects: []*openfgav1.Object{
				{Type: "user", Id: "jon"},
			},
		},
		{
			name: "direct_relationship_through_multiple_usersets",
			req: &openfgav1.ListUsersRequest{
				StoreId:               storeID,
				AuthorizationModelId:  modelID,
				Object:                &openfgav1.Object{Type: "document", Id: "1"},
				Relation:              "viewer",
				TargetUserObjectTypes: []string{"user"},
			},
			typeDefinitions: parser.MustParse(`
			type user
			type group
			  relations
			    define member: [user, group#member] as self
			type document
			  relations
			    define viewer: [group#member] as self
			`),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("group:eng", "member", "user:hawker"),
				tuple.NewTupleKey("group:eng", "member", "group:fga#member"),
				tuple.NewTupleKey("group:fga", "member", "user:jon"),
			},
			expectedObjects: []*openfgav1.Object{
				{Type: "user", Id: "hawker"},
				{Type: "user", Id: "jon"},
			},
		},
		{
			name: "rewritten_direct_relationship_through_computed_userset",
			req: &openfgav1.ListUsersRequest{
				StoreId:               storeID,
				AuthorizationModelId:  modelID,
				Object:                &openfgav1.Object{Type: "document", Id: "1"},
				Relation:              "viewer",
				TargetUserObjectTypes: []string{"user"},
			},
			typeDefinitions: parser.MustParse(`
			type user
			type document
			  relations
			    define editor: [user] as self
			    define viewer as editor
			`),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "editor", "user:jon"),
			},
			expectedObjects: []*openfgav1.Object{
				{Type: "user", Id: "jon"},
			},
		},
		{
			name: "rewritten_direct_relationship_through_ttu",
			req: &openfgav1.ListUsersRequest{
				StoreId:               storeID,
				AuthorizationModelId:  modelID,
				Object:                &openfgav1.Object{Type: "document", Id: "1"},
				Relation:              "viewer",
				TargetUserObjectTypes: []string{"user"},
			},
			typeDefinitions: parser.MustParse(`
			type user
			type folder
			  relations
			    define viewer: [user] as self
			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`),
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:x"),
				tuple.NewTupleKey("folder:x", "viewer", "user:jon"),
			},
			expectedObjects: []*openfgav1.Object{
				{Type: "user", Id: "jon"},
			},
		},
		{
			name: "userset_defines_itself",
			req: &openfgav1.ListUsersRequest{
				StoreId:               storeID,
				AuthorizationModelId:  modelID,
				Object:                &openfgav1.Object{Type: "document", Id: "1"},
				Relation:              "viewer",
				TargetUserObjectTypes: []string{"document"},
			},
			typeDefinitions: parser.MustParse(`
			type user
			type document
			  relations
			    define viewer: [user] as self
			`),
			tuples: []*openfgav1.TupleKey{},
			expectedObjects: []*openfgav1.Object{
				{Type: "document", Id: "1"},
			},
		},
		// {
		// 	name: "direct_userset_relationship_with_cycle",
		// 	req: &ListUsersRequest{
		// 		StoreId:              storeID,
		// 		AuthorizationModelId: modelID,
		// 		Object:               &openfgav1.Object{Type: "document", Id: "1"},
		// 		Relation:             "viewer",
		// 		TargetUserObjectTypes: []string{"user"},
		// 	},
		// 	typeDefinitions: parser.MustParse(`
		// 	type user

		// 	type group
		// 	  relations
		// 	    define member: [user, group#member] as self

		// 	type document
		// 	  relations
		// 	    define viewer: [group#member] as self
		// 	`),
		// 	tuples: []*openfgav1.TupleKey{
		// 		tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
		// 		tuple.NewTupleKey("group:eng", "member", "group:fga#member"),
		// 		tuple.NewTupleKey("group:fga", "member", "group:eng#member"),
		// 		tuple.NewTupleKey("group:fga", "member", "user:jon"),
		// 	},
		// 	expectedError: fmt.Errorf("cycle detected"),
		// },
	}

	for _, test := range tests {

		ds := memory.New()
		defer ds.Close()

		model := &openfgav1.AuthorizationModel{
			Id:              modelID,
			TypeDefinitions: test.typeDefinitions,
			SchemaVersion:   typesystem.SchemaVersion1_1,
		}

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

			resp, err := l.ListUsers(ctx, test.req)
			require.ErrorIs(t, err, test.expectedError)

			ignoredFieldsOpts := protocmp.IgnoreFields(protoadapt.MessageV2Of(&openfgav1.Object{}), "type", "id")

			returnedUsers := resp.GetUsers()
			expected := test.expectedObjects

			if diff := cmp.Diff(expected, returnedUsers, ignoredFieldsOpts, protocmp.Transform()); diff != "" {
				require.FailNowf(t, "(-want +got):\n%s", diff)
			}
		})
	}
}
