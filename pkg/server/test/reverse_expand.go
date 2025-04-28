package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/server/commands/reverseexpand"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReverseExpand(t *testing.T, ds storage.OpenFGADatastore) {
	tests := []struct {
		name                 string
		model                string
		tuples               []string
		request              *reverseexpand.ReverseExpandRequest
		resolveNodeLimit     uint32
		expectedResult       []*reverseexpand.ReverseExpandResult
		expectedError        error
		expectedDSQueryCount uint32
	}{
		{
			name: "basic_intersection",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
				ContextualTuples: []*openfgav1.TupleKey{},
			},
			model: `
			model
			  schema 1.1

			type user

			type document
			  relations
			    define allowed: [user]
			    define viewer: [user] and allowed`,
			tuples: []string{
				"document:1#viewer@user:jon",
				"document:2#viewer@user:jon",
				"document:3#allowed@user:jon",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.RequiresFurtherEvalStatus,
				},
				{
					Object:       "document:2",
					ResultStatus: reverseexpand.RequiresFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 1,
		},
		{
			name: "indirect_intersection",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
				ContextualTuples: []*openfgav1.TupleKey{},
			},
			model: `
			model
			  schema 1.1

			type user

			type folder
			  relations
			    define writer: [user]
			    define editor: [user]
			    define viewer: writer and editor

			type document
			  relations
			    define parent: [folder]
			    define viewer: viewer from parent`,
			tuples: []string{
				"document:1#parent@folder:X",
				"folder:X#writer@user:jon",
				"folder:X#editor@user:jon",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.RequiresFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 2,
		},

		{
			name: "resolve_direct_relationships_with_tuples_and_contextual_tuples",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
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
			model
			  schema 1.1

			type user

			type document
			  relations
			    define viewer: [user]`,
			tuples: []string{
				"document:doc1#viewer@user:jon",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:doc1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
				{
					Object:       "document:doc3",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 1,
		},
		{
			name: "direct_relations_involving_relationships_with_users_and_usersets",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "jon",
				}},
			},
			model: `
			model
			  schema 1.1

			type user

			type group
		  	  relations
			    define member: [user]

			type document
			  relations
			    define viewer: [user, group#member]`,
			tuples: []string{
				"document:doc1#viewer@user:jon",
				"document:doc2#viewer@user:bob",
				"document:doc3#viewer@group:openfga#member",
				"group:openfga#member@user:jon",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:doc1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
				{
					Object:       "document:doc3",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 3,
		},
		{
			name: "success_with_direct_relationships_and_computed_usersets",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "jon",
				}},
			},
			model: `
			model
			  schema 1.1

			type user

			type group
			  relations
			    define member: [user]

			type document
			  relations
			    define owner: [user, group#member]
			    define viewer: owner`,
			tuples: []string{
				"document:doc1#owner@user:jon",
				"document:doc2#owner@user:bob",
				"document:doc3#owner@group:openfga#member",
				"group:openfga#member@user:jon",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:doc1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
				{
					Object:       "document:doc3",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 3,
		},
		{
			name: "success_with_many_tuples",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
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
			model
			  schema 1.1

			type user

			type group
			  relations
			    define member: [user, group#member]

			type folder
			  relations
			    define parent: [folder]
			    define viewer: [user, group#member] or viewer from parent

			type document
			  relations
			    define parent: [folder]
			    define viewer: viewer from parent`,
			tuples: []string{
				"folder:folder1#viewer@user:jon",
				"folder:folder2#parent@folder:folder1",
				"folder:folder3#parent@folder:folder2",
				"folder:folder4#viewer@group:eng#member",
				"document:doc1#parent@folder:folder3",
				"document:doc2#parent@folder:folder5",
				"document:doc3#parent@folder:folder6",
				"group:eng#member@group:openfga#member",
				"group:openfga#member@user:jon",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:doc1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
				{
					Object:       "document:doc2",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 16,
		},
		{
			name: "resolve_objects_involved_in_recursive_hierarchy",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "folder",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			model: `
			model
			  schema 1.1

			type user

			type folder
			  relations
			    define parent: [folder]
			    define viewer: [user] or viewer from parent`,
			tuples: []string{
				"folder:folder1#viewer@user:jon",
				"folder:folder2#parent@folder:folder1",
				"folder:folder3#parent@folder:folder2",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "folder:folder1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
				{
					Object:       "folder:folder2",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
				{
					Object:       "folder:folder3",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 4,
		},
		{
			name: "resolution_depth_exceeded_failure",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "folder",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			resolveNodeLimit: 2,
			model: `
			model
			  schema 1.1

			type user

			type folder
			  relations
			    define parent: [folder]
			    define viewer: [user] or viewer from parent`,
			tuples: []string{
				"folder:folder1#viewer@user:jon",
				"folder:folder2#parent@folder:folder1",
				"folder:folder3#parent@folder:folder2",
			},
			expectedError:        graph.ErrResolutionDepthExceeded,
			expectedDSQueryCount: 0,
		},
		{
			name: "objects_connected_to_a_userset",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "group",
				Relation:   "member",
				User: &reverseexpand.UserRefObjectRelation{
					ObjectRelation: &openfgav1.ObjectRelation{
						Object:   "group:iam",
						Relation: "member",
					},
				},
			},
			model: `
			model
			  schema 1.1

			type user

			type group
			  relations
			    define member: [user, group#member]`,
			tuples: []string{
				"group:opensource#member@group:eng#member",
				"group:eng#member@group:iam#member",
				"group:iam#member@user:jon",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "group:opensource",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
				{
					Object:       "group:eng",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
				{
					Object:       "group:iam",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 3,
		},
		{
			name: "objects_connected_to_a_userset_self_referencing",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "group",
				Relation:   "member",
				User: &reverseexpand.UserRefObjectRelation{
					ObjectRelation: &openfgav1.ObjectRelation{
						Object:   "group:iam",
						Relation: "member",
					},
				},
			},
			model: `
			model
			  schema 1.1

			type group
			  relations
			    define member: [group#member]`,
			tuples: []string{
				"group:iam#member@group:iam#member",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "group:iam",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 2,
		},
		{
			name: "objects_connected_through_a_computed_userset_1",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			model: `
			model
			  schema 1.1

			type user

			type document
			  relations
			    define owner: [user]
			    define editor: owner
			    define viewer: [document#editor]`,
			tuples: []string{
				"document:1#viewer@document:1#editor",
				"document:1#owner@user:jon",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 2,
		},
		{
			name: "objects_connected_through_a_computed_userset_2",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			model: `
			model
			  schema 1.1

			  type user

			type group
			  relations
			    define manager: [user]
			    define member: manager

			type document
			  relations
			    define viewer: [group#member]`,
			tuples: []string{
				"document:1#viewer@group:eng#member",
				"group:eng#manager@user:jon",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 2,
		},
		{
			name: "objects_connected_through_a_computed_userset_3",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "trial",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "fede",
					},
				},
			},
			model: `
			model
			  schema 1.1

			type user

			type team
			  relations
			    define admin: [user]
			    define member: admin

			type trial
			  relations
			    define editor: [team#member]
			    define viewer: editor`,
			tuples: []string{
				"trial:1#editor@team:devs#member",
				"team:devs#admin@user:fede",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "trial:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 2,
		},
		{
			name: "objects_connected_indirectly_through_a_ttu",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "view",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{
						Type: "organization",
						Id:   "2",
					},
				},
			},
			model: `
			model
			  schema 1.1

			type organization
			  relations
			    define viewer: [organization]
			    define can_view: viewer

			type document
			  relations
			    define parent: [organization]
			    define view: can_view from parent`,
			tuples: []string{
				"document:1#parent@organization:1",
				"organization:1#viewer@organization:2",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 2,
		},
		{
			name: "directly_related_typed_wildcard",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User:       &reverseexpand.UserRefTypedWildcard{Type: "user"},
			},
			model: `
			model
			  schema 1.1

			type user

			type document
			  relations
			    define viewer: [user, user:*]`,
			tuples: []string{
				"document:1#viewer@user:*",
				"document:2#viewer@user:*",
				"document:3#viewer@user:jon",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
				{
					Object:       "document:2",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 1,
		},
		{
			name: "indirectly_related_typed_wildcard",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User:       &reverseexpand.UserRefTypedWildcard{Type: "user"},
			},
			model: `
			model
			  schema 1.1

			type user

			type group
			  relations
			    define member: [user:*]

			type document
			  relations
			    define viewer: [group#member]`,
			tuples: []string{
				"document:1#viewer@group:eng#member",
				"document:2#viewer@group:fga#member",
				"group:eng#member@user:*",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 2,
		},
		{
			name: "relationship_through_multiple_indirections",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			model: `
			model
			  schema 1.1

			type user

			type team
			  relations
			    define member: [user]

			type group
			  relations
			    define member: [team#member]

			type document
			  relations
			    define viewer: [group#member]`,
			tuples: []string{
				"team:tigers#member@user:jon",
				"group:eng#member@team:tigers#member",
				"document:1#viewer@group:eng#member",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 3,
		},
		{
			name: "typed_wildcard_relationship_through_multiple_indirections",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			model: `
			model
			  schema 1.1

			type user

			type group
			  relations
			    define member: [team#member]

			type team
			  relations
			    define member: [user:*]

			type document
			  relations
			    define viewer: [group#member]`,
			tuples: []string{
				"team:tigers#member@user:*",
				"group:eng#member@team:tigers#member",
				"document:1#viewer@group:eng#member",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 3,
		},
		{
			name: "simple_typed_wildcard_and_direct_relation",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{Type: "user", Id: "jon"},
				},
			},
			model: `
			model
			  schema 1.1

			type user

			type document
			  relations
			    define viewer: [user, user:*]`,
			tuples: []string{
				"document:1#viewer@user:*",
				"document:2#viewer@user:jon",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
				{
					Object:       "document:2",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 1,
		},
		{
			name: "simple_typed_wildcard_and_indirect_relation",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "jon",
					},
				},
			},
			model: `
			model
			  schema 1.1

			type user

			type group
			  relations
			    define member: [user, user:*]

			type document
			  relations
			    define viewer: [group#member]`,
			tuples: []string{
				"group:eng#member@user:*",
				"group:fga#member@user:jon",
				"document:1#viewer@group:eng#member",
				"document:2#viewer@group:fga#member",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
				{
					Object:       "document:2",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 3,
		},
		{
			name: "with_public_user_access_1",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "*",
					},
				},
			},
			model: `
			model
			  schema 1.1

			type user

			type group
			  relations
			    define member: [user:*]

			type document
			  relations
			    define viewer: [group#member]`,
			tuples: []string{
				"group:eng#member@user:*",
				"group:other#member@employee:*", // assume this comes from a prior model
				"document:1#viewer@group:eng#member",
				"document:2#viewer@group:fga#member",
				"document:3#viewer@group:other#member",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 2,
		},
		{
			name: "with_public_user_access_2",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "resource",
				Relation:   "reader",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "bev",
					},
				},
			},
			model: `
			model
			  schema 1.1

			type user

			type group
			  relations
			    define member: [user]

			type resource
			  relations
			    define reader: [user, user:*, group#member] or writer
			    define writer: [user, user:*, group#member]`,
			tuples: []string{
				"resource:x#writer@user:*",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "resource:x",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 3,
		},
		{
			name: "simple_typed_wildcard_with_contextual_tuples_1",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{
					Object: &openfgav1.Object{Type: "user", Id: "jon"},
				},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "user:*"),
					tuple.NewTupleKey("document:2", "viewer", "user:jon"),
				},
			},
			model: `
			model
			  schema 1.1

			type user
			type document
			  relations
			    define viewer: [user, user:*]`,
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
				{
					Object:       "document:2",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 1,
		},
		{
			name: "simple_typed_wildcard_with_contextual_tuples_2",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User:       &reverseexpand.UserRefTypedWildcard{Type: "user"},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "employee:*"),
					tuple.NewTupleKey("document:2", "viewer", "user:*"),
				},
			},
			model: `
			model
			  schema 1.1

			type user

			type employee

			type document
			  relations
			    define viewer: [user:*]`,
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:2",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 1,
		},
		{
			name: "simple_typed_wildcard_with_contextual_tuples_3",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObjectRelation{
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
			model
			  schema 1.1

			type user

			type group
			  relations
			    define member: [user]

			type document
			  relations
			    define viewer: [group#member]`,
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 1,
		},
		{
			name: "non-assignable_ttu_relationship",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "jon",
				}},
			},
			model: `
			model
			  schema 1.1

			type user

			type folder
			  relations
			    define viewer: [user, user:*]

			type document
			  relations
			    define parent: [folder]
			    define viewer: viewer from parent`,
			tuples: []string{
				"document:1#parent@folder:1",
				"document:2#parent@folder:2",
				"folder:1#viewer@user:jon",
				"folder:2#viewer@user:*",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
				{
					Object:       "document:2",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 3,
		},
		{
			name: "non-assignable_ttu_relationship_without_wildcard_connectivity",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "jon",
				}},
			},
			model: `
			model
			  schema 1.1

			type user
			type employee

			type folder
			  relations
			    define viewer: [user, employee:*]

			type document
			  relations
			    define parent: [folder]
			    define viewer: viewer from parent`,
			tuples: []string{
				"document:1#parent@folder:1",
				"document:2#parent@folder:2",
				"folder:1#viewer@user:jon",
				"folder:2#viewer@user:*",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 2,
		},
		{
			name: "non-assignable_ttu_relationship_through_indirection_1",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "jon",
				}},
			},
			model: `
			model
			  schema 1.1

			type user

			type group
			  relations
			    define member: [user:*]

			type folder
			  relations
			    define viewer: [group#member]

			type document
			  relations
			    define parent: [folder]
			    define viewer: viewer from parent`,
			tuples: []string{
				"document:1#parent@folder:1",
				"folder:1#viewer@group:eng#member",
				"group:eng#member@user:*",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 3,
		},
		{
			name: "non-assignable_ttu_relationship_through_indirection_2",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "resource",
				Relation:   "writer",
				User: &reverseexpand.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "anne",
				}},
			},
			model: `
			model
			  schema 1.1

			type user

			type org
			  relations
			    define dept: [group]
			    define dept_member: member from dept

			type group
			  relations
			    define member: [user]

			type resource
			  relations
			    define writer: [org#dept_member]`,
			tuples: []string{
				"resource:eng_handbook#writer@org:eng#dept_member",
				"org:eng#dept@group:fga",
				"group:fga#member@user:anne",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "resource:eng_handbook",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 3,
		},
		{
			name: "non-assignable_ttu_relationship_through_indirection_3",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "resource",
				Relation:   "reader",
				User: &reverseexpand.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "anne",
				}},
			},
			model: `
			model
			  schema 1.1

			type user

			type org
			  relations
			    define dept: [group]
			    define dept_member: member from dept

			type group
			  relations
			    define member: [user]

			type resource
			  relations
			    define writer: [org#dept_member]
			    define reader: [org#dept_member] or writer`,
			tuples: []string{
				"resource:eng_handbook#writer@org:eng#dept_member",
				"org:eng#dept@group:fga",
				"group:fga#member@user:anne",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "resource:eng_handbook",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 4,
		},
		{
			name: "cyclical_tupleset_relation_terminates",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "node",
				Relation:   "editor",
				User: &reverseexpand.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "wonder",
				}},
			},
			model: `
			model
			  schema 1.1

			type user

			type node
			  relations
			    define parent: [node]
			    define editor: [user] or editor from parent`,
			tuples: []string{
				"node:abc#editor@user:wonder",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "node:abc",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 2,
		},
		{
			name: "does_not_send_duplicate_even_though_there_are_two_paths_to_same_solution",
			request: &reverseexpand.ReverseExpandRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &reverseexpand.UserRefObject{Object: &openfgav1.Object{
					Type: "user",
					Id:   "jon",
				}},
			},
			model: `
			model
			  schema 1.1

			type user

			type group
			  relations
			    define member: [user]
			    define maintainer: [user]

			type document
			  relations
			    define viewer: [group#member,group#maintainer]`,
			tuples: []string{
				"document:1#viewer@group:example1#maintainer",
				"group:example1#maintainer@user:jon",
				"group:example1#member@user:jon",
			},
			expectedResult: []*reverseexpand.ReverseExpandResult{
				{
					Object:       "document:1",
					ResultStatus: reverseexpand.NoFurtherEvalStatus,
				},
			},
			expectedDSQueryCount: 4,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storeID, model := storagetest.BootstrapFGAStore(t, ds, test.model, test.tuples)
			test.request.StoreID = storeID

			var opts []reverseexpand.ReverseExpandQueryOption

			if test.resolveNodeLimit != 0 {
				opts = append(opts, reverseexpand.WithResolveNodeLimit(test.resolveNodeLimit))
			}

			ts, err := typesystem.New(model)
			require.NoError(t, err)

			ds := storagewrappers.NewRequestStorageWrapper(ds, test.request.ContextualTuples, 30)
			reverseExpandQuery := reverseexpand.NewReverseExpandQuery(ds, ts, opts...)

			resultChan := make(chan *reverseexpand.ReverseExpandResult, 100)

			timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			resolutionMetadata := reverseexpand.NewResolutionMetadata()

			reverseExpandErrCh := make(chan error, 1)
			go func() {
				errReverseExpand := reverseExpandQuery.Execute(timeoutCtx, test.request, resultChan, resolutionMetadata)
				if errReverseExpand != nil {
					reverseExpandErrCh <- errReverseExpand
				}
			}()

			var results []*reverseexpand.ReverseExpandResult

			for {
				select {
				case errFromChannel := <-reverseExpandErrCh:
					if errors.Is(errFromChannel, context.DeadlineExceeded) {
						require.FailNow(t, "unexpected timeout")
					}
					require.ErrorIs(t, errFromChannel, test.expectedError)
					return
				case res, channelOpen := <-resultChan:
					if !channelOpen {
						t.Log("channel closed")
						if test.expectedError == nil {
							require.ElementsMatch(t, test.expectedResult, results)
						} else {
							require.FailNow(t, "expected an error, got none")
						}
						return
					} else {
						t.Logf("appending result %s", res.Object)
						results = append(results, res)
					}
				}
			}
		})
	}
}
