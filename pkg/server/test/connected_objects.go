package test

import (
	"context"
	"testing"
	"time"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/server/commands/connectedobjects"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
)

func ConnectedObjectsTest(t *testing.T, ds storage.OpenFGADatastore) {

	tests := []struct {
		name             string
		model            string
		tuples           []*openfgav1.TupleKey
		request          *connectedobjects.ConnectedObjectsRequest
		resolveNodeLimit uint32
		expectedResult   []*connectedobjects.ConnectedObjectsResult
		expectedError    error
	}{
		{
			name: "basic_intersection",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
				ContextualTuples: []*openfgav1.TupleKey{},
			},
			model: `
				type user

				type document
				  relations
				    define allowed: [user] as self
				    define viewer: [user] as self and allowed
				`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:2", "viewer", "user:jon"),
				tuple.NewTupleKey("document:3", "allowed", "user:jon"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.RequiresFurtherEvalStatus,
				},
				{
					Object:       "document:2",
					ResultStatus: connectedobjects.RequiresFurtherEvalStatus,
				},
			},
		},
		{
			name: "indirect_intersection",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
				ContextualTuples: []*openfgav1.TupleKey{},
			},
			model: `
			type user

			type folder
			  relations
				define writer: [user] as self
				define editor: [user] as self
				define viewer as writer and editor

			type document
			  relations
				define parent: [folder] as self
				define viewer as viewer from parent
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:X"),
				tuple.NewTupleKey("folder:X", "writer", "user:jon"),
				tuple.NewTupleKey("folder:X", "editor", "user:jon"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.RequiresFurtherEvalStatus,
				},
			},
		},

		{
			name: "resolve_direct_relationships_with_tuples_and_contextual_tuples",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:doc2", "viewer", "user:bob"),
					tuple.NewTupleKey("document:doc3", "viewer", "user:jon"),
				},
			},
			model: `
			type user

			type document
			  relations
			    define viewer: [user] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:doc1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
				{
					Object:       "document:doc3",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "direct_relations_involving_relationships_with_users_and_usersets",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "jon",
				}},
			},
			model: `
			type user

			type group
			  relations
			    define member: [user] as self

			type document
			  relations
			    define viewer: [user, group#member] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:doc2", "viewer", "user:bob"),
				tuple.NewTupleKey("document:doc3", "viewer", "group:openfga#member"),
				tuple.NewTupleKey("group:openfga", "member", "user:jon"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:doc1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
				{
					Object:       "document:doc3",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "success_with_direct_relationships_and_computed_usersets",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "jon",
				}},
			},
			model: `
			type user

			type group
			  relations
			    define member: [user] as self

			type document
			  relations
			    define owner: [user, group#member] as self
			    define viewer as owner
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:doc1", "owner", "user:jon"),
				tuple.NewTupleKey("document:doc2", "owner", "user:bob"),
				tuple.NewTupleKey("document:doc3", "owner", "group:openfga#member"),
				tuple.NewTupleKey("group:openfga", "member", "user:jon"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:doc1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
				{
					Object:       "document:doc3",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "success_with_many_tuples",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("folder:folder5", "parent", "folder:folder4"),
					tuple.NewTupleKey("folder:folder6", "viewer", "user:bob"),
				},
			},
			model: `
			type user

			type group
			  relations
			    define member: [user, group#member] as self

			type folder
			  relations
			    define parent: [folder] as self
			    define viewer: [user, group#member] as self or viewer from parent

			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:folder2", "parent", "folder:folder1"),
				tuple.NewTupleKey("folder:folder3", "parent", "folder:folder2"),
				tuple.NewTupleKey("folder:folder4", "viewer", "group:eng#member"),

				tuple.NewTupleKey("document:doc1", "parent", "folder:folder3"),
				tuple.NewTupleKey("document:doc2", "parent", "folder:folder5"),
				tuple.NewTupleKey("document:doc3", "parent", "folder:folder6"),

				tuple.NewTupleKey("group:eng", "member", "group:openfga#member"),
				tuple.NewTupleKey("group:openfga", "member", "user:jon"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:doc1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
				{
					Object:       "document:doc2",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "resolve_objects_involved_in_recursive_hierarchy",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "folder",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			model: `
			type user

			type folder
			  relations
			    define parent: [folder] as self
			    define viewer: [user] as self or viewer from parent
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:folder2", "parent", "folder:folder1"),
				tuple.NewTupleKey("folder:folder3", "parent", "folder:folder2"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "folder:folder1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
				{
					Object:       "folder:folder2",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
				{
					Object:       "folder:folder3",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "resolution_depth_exceeded_failure",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "folder",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			resolveNodeLimit: 2,
			model: `
			type user

			type folder
			  relations
			    define parent: [folder] as self
			    define viewer: [user] as self or viewer from parent
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:folder2", "parent", "folder:folder1"),
				tuple.NewTupleKey("folder:folder3", "parent", "folder:folder2"),
			},
			expectedError: serverErrors.AuthorizationModelResolutionTooComplex,
		},
		{
			name: "objects_connected_to_a_userset",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "group",
				Relation:   "member",
				User: &connectedobjects.UserRefObjectRelation{
					ObjectRelation: &openfgav1.ObjectRelation{
						Object:   "group:iam",
						Relation: "member",
					},
				},
			},
			model: `
			type user

			type group
			  relations
			    define member: [user, group#member] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:opensource", "member", "group:eng#member"),
				tuple.NewTupleKey("group:eng", "member", "group:iam#member"),
				tuple.NewTupleKey("group:iam", "member", "user:jon"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "group:opensource",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
				{
					Object:       "group:eng",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "objects_connected_to_a_userset_self_referencing",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "group",
				Relation:   "member",
				User: &connectedobjects.UserRefObjectRelation{
					ObjectRelation: &openfgav1.ObjectRelation{
						Object:   "group:iam",
						Relation: "member",
					},
				},
			},
			model: `
			type group
			  relations
			    define member: [group#member] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:iam", "member", "group:iam#member"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "group:iam",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "objects_connected_through_a_computed_userset_1",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			model: `
			type user

			type document
			  relations
			    define owner: [user] as self
			    define editor as owner
			    define viewer: [document#editor] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "document:1#editor"),
				tuple.NewTupleKey("document:1", "owner", "user:jon"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "objects_connected_through_a_computed_userset_2",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			model: `
			type user

			type group
			  relations
			    define manager: [user] as self
			    define member as manager

			type document
			  relations
			    define viewer: [group#member] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("group:eng", "manager", "user:jon"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "objects_connected_through_a_computed_userset_3",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "trial",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "fede",
					},
				},
			},
			model: `
			type user

			type team
			  relations
			    define admin: [user] as self
			    define member as admin

			type trial
			  relations
			    define editor: [team#member] as self
			    define viewer as editor
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("trial:1", "editor", "team:devs#member"),
				tuple.NewTupleKey("team:devs", "admin", "user:fede"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "trial:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "objects_connected_indirectly_through_a_ttu",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "view",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "organization",
						Id:   "2",
					},
				},
			},
			model: `
			type organization
			  relations
			    define viewer: [organization] as self
			    define can_view as viewer

			type document
			  relations
			    define parent: [organization] as self
			    define view as can_view from parent
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "organization:1"),
				tuple.NewTupleKey("organization:1", "viewer", "organization:2"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "directly_related_typed_wildcard",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User:       &connectedobjects.UserRefTypedWildcard{Type: "user"},
			},
			model: `
			type user

			type document
			  relations
			    define viewer: [user, user:*] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:2", "viewer", "user:*"),
				tuple.NewTupleKey("document:3", "viewer", "user:jon"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
				{
					Object:       "document:2",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "indirectly_related_typed_wildcard",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User:       &connectedobjects.UserRefTypedWildcard{Type: "user"},
			},
			model: `
			type user
			type group
			  relations
			    define member: [user:*] as self
			type document
			  relations
			    define viewer: [group#member] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("document:2", "viewer", "group:fga#member"),
				tuple.NewTupleKey("group:eng", "member", "user:*"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "relationship_through_multiple_indirections",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			model: `
			type user
			type team
			  relations
			    define member: [user] as self
			type group
			  relations
			    define member: [team#member] as self
			type document
			  relations
			    define viewer: [group#member] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("team:tigers", "member", "user:jon"),
				tuple.NewTupleKey("group:eng", "member", "team:tigers#member"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "typed_wildcard_relationship_through_multiple_indirections",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			model: `
			type user
			type group
			  relations
			    define member: [team#member] as self
			type team
			  relations
			    define member: [user:*] as self
			type document
			  relations
			    define viewer: [group#member] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("team:tigers", "member", "user:*"),
				tuple.NewTupleKey("group:eng", "member", "team:tigers#member"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "simple_typed_wildcard_and_direct_relation",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{Type: "user", Id: "jon"},
				},
			},
			model: `
			type user
			type document
			  relations
			    define viewer: [user, user:*] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:2", "viewer", "user:jon"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
				{
					Object:       "document:2",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "simple_typed_wildcard_and_indirect_relation",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			model: `
			type user
			type group
			  relations
			    define member: [user, user:*] as self
			type document
			  relations
			    define viewer: [group#member] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "user:*"),
				tuple.NewTupleKey("group:fga", "member", "user:jon"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("document:2", "viewer", "group:fga#member"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
				{
					Object:       "document:2",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "connected_objects_with_public_user_access_1",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "*",
					},
				},
			},
			model: `
			type user
			type group
			  relations
			    define member: [user:*] as self
			type document
			  relations
			    define viewer: [group#member] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "user:*"),
				tuple.NewTupleKey("group:other", "member", "employee:*"), // assume this comes from a prior model
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("document:2", "viewer", "group:fga#member"),
				tuple.NewTupleKey("document:3", "viewer", "group:other#member"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "connected_objects_with_public_user_access_2",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "resource",
				Relation:   "reader",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "bev",
					},
				},
			},
			model: `
			type user
			type group
			  relations
			    define member: [user] as self
			type resource
			  relations
			    define reader: [user, user:*, group#member] as self or writer
				define writer: [user, user:*, group#member] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("resource:x", "writer", "user:*"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "resource:x",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "simple_typed_wildcard_with_contextual_tuples_1",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{
					Object: &openfgav1.Object{Type: "user", Id: "jon"},
				},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "user:*"),
					tuple.NewTupleKey("document:2", "viewer", "user:jon"),
				},
			},
			model: `
			type user
			type document
			  relations
			    define viewer: [user, user:*] as self
			`,
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
				{
					Object:       "document:2",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "simple_typed_wildcard_with_contextual_tuples_2",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User:       &connectedobjects.UserRefTypedWildcard{Type: "user"},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "employee:*"),
					tuple.NewTupleKey("document:2", "viewer", "user:*"),
				},
			},
			model: `
			type user
			type employee
			type document
			  relations
			    define viewer: [user:*] as self
			`,
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:2",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "simple_typed_wildcard_with_contextual_tuples_3",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObjectRelation{
					ObjectRelation: &openfgav1.ObjectRelation{
						Object:   "group:eng",
						Relation: "member",
					},
				},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				},
			},
			model: `
			type user

			type group
			  relations
			    define member: [user] as self

			type document
			  relations
			    define viewer: [group#member] as self
			`,
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "non-assignable_ttu_relationship",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "jon",
				}},
			},
			model: `
			type user

			type folder
			  relations
			    define viewer: [user, user:*] as self

			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:1"),
				tuple.NewTupleKey("document:2", "parent", "folder:2"),
				tuple.NewTupleKey("folder:1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:2", "viewer", "user:*"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
				{
					Object:       "document:2",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "non-assignable_ttu_relationship_without_wildcard_connectivity",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "jon",
				}},
			},
			model: `
			type user
			type employee

			type folder
			  relations
			    define viewer: [user, employee:*] as self

			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:1"),
				tuple.NewTupleKey("document:2", "parent", "folder:2"),
				tuple.NewTupleKey("folder:1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:2", "viewer", "user:*"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "non-assignable_ttu_relationship_through_indirection_1",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "jon",
				}},
			},
			model: `
			type user

			type group
			  relations
			    define member: [user:*] as self
			type folder
			  relations
			    define viewer: [group#member] as self

			type document
			  relations
			    define parent: [folder] as self
			    define viewer as viewer from parent
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:1"),
				tuple.NewTupleKey("folder:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("group:eng", "member", "user:*"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "non-assignable_ttu_relationship_through_indirection_2",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "resource",
				Relation:   "writer",
				User: &connectedobjects.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "anne",
				}},
			},
			model: `
			type user

			type org
			  relations
			    define dept: [group] as self
			    define dept_member as member from dept

			type group
			  relations
			    define member: [user] as self

			type resource
			  relations
			    define writer: [org#dept_member] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("resource:eng_handbook", "writer", "org:eng#dept_member"),
				tuple.NewTupleKey("org:eng", "dept", "group:fga"),
				tuple.NewTupleKey("group:fga", "member", "user:anne"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "resource:eng_handbook",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "non-assignable_ttu_relationship_through_indirection_3",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "resource",
				Relation:   "reader",
				User: &connectedobjects.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "anne",
				}},
			},
			model: `
			type user

			type org
			  relations
			    define dept: [group] as self
			    define dept_member as member from dept

			type group
			  relations
			    define member: [user] as self

			type resource
			  relations
			    define writer: [org#dept_member] as self
			    define reader: [org#dept_member] as self or writer
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("resource:eng_handbook", "writer", "org:eng#dept_member"),
				tuple.NewTupleKey("org:eng", "dept", "group:fga"),
				tuple.NewTupleKey("group:fga", "member", "user:anne"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "resource:eng_handbook",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "cyclical_tupleset_relation_terminates",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "node",
				Relation:   "editor",
				User: &connectedobjects.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "wonder",
				}},
			},
			model: `
			type user

			type node
			  relations
			    define parent: [node] as self
			    define editor: [user] as self or editor from parent
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("node:abc", "editor", "user:wonder"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "node:abc",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
		{
			name: "does_not_send_duplicate_even_though_there_are_two_paths_to_same_solution",
			request: &connectedobjects.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &connectedobjects.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "jon",
				}},
			},
			model: `
			type user

			type group
			  relations
				define member: [user] as self
				define maintainer: [user] as self

			type document
			  relations
				define viewer: [group#member,group#maintainer] as self
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "group:example1#maintainer"),
				tuple.NewTupleKey("group:example1", "maintainer", "user:jon"),
				tuple.NewTupleKey("group:example1", "member", "user:jon"),
			},
			expectedResult: []*connectedobjects.ConnectedObjectsResult{
				{
					Object:       "document:1",
					ResultStatus: connectedobjects.NoFurtherEvalStatus,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			ctx := context.Background()
			store := ulid.Make().String()
			test.request.StoreID = store

			model := &openfgav1.AuthorizationModel{
				Id:              ulid.Make().String(),
				SchemaVersion:   typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(test.model),
			}
			err := ds.WriteAuthorizationModel(ctx, store, model)
			require.NoError(err)

			err = ds.Write(ctx, store, nil, test.tuples)
			require.NoError(err)

			var opts []connectedobjects.ConnectedObjectsQueryOption

			if test.resolveNodeLimit != 0 {
				opts = append(opts, connectedobjects.WithResolveNodeLimit(test.resolveNodeLimit))
			}

			connectedObjectsCmd := connectedobjects.NewConnectedObjectsQuery(ds, typesystem.New(model), opts...)

			resultChan := make(chan *connectedobjects.ConnectedObjectsResult, 100)
			done := make(chan struct{})

			var results []*connectedobjects.ConnectedObjectsResult
			go func() {
				for result := range resultChan {
					results = append(results, result)
				}

				done <- struct{}{}
			}()

			timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			go func() {
				err = connectedObjectsCmd.Execute(timeoutCtx, test.request, resultChan)
				require.ErrorIs(err, test.expectedError)
				close(resultChan)
			}()

			select {
			case <-timeoutCtx.Done():
				require.FailNow("timed out waiting for response")
			case <-done:
			}

			if test.expectedError == nil {
				require.ElementsMatch(test.expectedResult, results)
			}
		})
	}
}
