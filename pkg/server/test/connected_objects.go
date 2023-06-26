package test

import (
	"context"
	"sort"
	"testing"
	"time"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func ConnectedObjectsTest(t *testing.T, ds storage.OpenFGADatastore) {

	tests := []struct {
		name             string
		model            string
		tuples           []*openfgapb.TupleKey
		request          *commands.ConnectedObjectsRequest
		resolveNodeLimit uint32
		limit            uint32
		expectedObjects  []string
		expectedError    error
	}{
		{
			name: "restrict_results_based_on_limit",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "folder",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
						Type: "user",
						Id:   "jon",
					},
				},
				ContextualTuples: []*openfgapb.TupleKey{},
			},
			limit: 2,
			model: `
			type user

			type folder
			  relations
			    define viewer: [user] as self
			`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:folder2", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:folder3", "viewer", "user:jon"),
			},
			expectedObjects: []string{"folder:folder1", "folder:folder2"},
		},
		{
			name: "resolve_direct_relationships_with_tuples_and_contextual_tuples",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
						Type: "user",
						Id:   "jon",
					},
				},
				ContextualTuples: []*openfgapb.TupleKey{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
			},
			expectedObjects: []string{"document:doc1", "document:doc3"},
		},
		{
			name: "direct_relations_involving_relationships_with_users_and_usersets",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:doc2", "viewer", "user:bob"),
				tuple.NewTupleKey("document:doc3", "viewer", "group:openfga#member"),
				tuple.NewTupleKey("group:openfga", "member", "user:jon"),
			},
			expectedObjects: []string{"document:doc1", "document:doc3"},
		},
		{
			name: "success_with_direct_relationships_and_computed_usersets",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:doc1", "owner", "user:jon"),
				tuple.NewTupleKey("document:doc2", "owner", "user:bob"),
				tuple.NewTupleKey("document:doc3", "owner", "group:openfga#member"),
				tuple.NewTupleKey("group:openfga", "member", "user:jon"),
			},
			expectedObjects: []string{"document:doc1", "document:doc3"},
		},
		{
			name: "success_with_many_tuples",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
						Type: "user",
						Id:   "jon",
					},
				},
				ContextualTuples: []*openfgapb.TupleKey{
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
			tuples: []*openfgapb.TupleKey{
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
			expectedObjects: []string{"document:doc1", "document:doc2"},
		},
		{
			name: "resolve_objects_involved_in_recursive_hierarchy",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "folder",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:folder2", "parent", "folder:folder1"),
				tuple.NewTupleKey("folder:folder3", "parent", "folder:folder2"),
			},
			expectedObjects: []string{"folder:folder1", "folder:folder2", "folder:folder3"},
		},
		{
			name: "resolution_depth_exceeded_failure",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "folder",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:folder2", "parent", "folder:folder1"),
				tuple.NewTupleKey("folder:folder3", "parent", "folder:folder2"),
			},
			expectedError: serverErrors.AuthorizationModelResolutionTooComplex,
		},
		{
			name: "objects_connected_to_a_userset",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "group",
				Relation:   "member",
				User: &commands.UserRefObjectRelation{
					ObjectRelation: &openfgapb.ObjectRelation{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("group:opensource", "member", "group:eng#member"),
				tuple.NewTupleKey("group:eng", "member", "group:iam#member"),
				tuple.NewTupleKey("group:iam", "member", "user:jon"),
			},
			expectedObjects: []string{"group:opensource", "group:eng"},
		},
		{
			name: "objects_connected_through_a_computed_userset_1",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "document:1#editor"),
				tuple.NewTupleKey("document:1", "owner", "user:jon"),
			},
			expectedObjects: []string{"document:1"},
		},
		{
			name: "objects_connected_through_a_computed_userset_2",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("group:eng", "manager", "user:jon"),
			},
			expectedObjects: []string{"document:1"},
		},
		{
			name: "objects_connected_through_a_computed_userset_3",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "trial",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
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
			    define member: [user,team#member] as self or admin

			type trial
			  relations
			    define editor: [user,team#member] as self or owner
			    define owner: [user] as self
			    define viewer: [user,team#member] as self or editor
			`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("trial:1", "editor", "team:devs#member"),
				tuple.NewTupleKey("team:devs", "admin", "user:fede"),
			},
			expectedObjects: []string{"trial:1"},
		},
		{
			name: "objects_connected_indirectly_through_a_ttu",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "view",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "organization:1"),
				tuple.NewTupleKey("organization:1", "viewer", "organization:2"),
			},
			expectedObjects: []string{"document:1"},
		},
		{
			name: "directly_related_typed_wildcard",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User:       &commands.UserRefTypedWildcard{Type: "user"},
			},
			model: `
			type user

			type document
			  relations
			    define viewer: [user, user:*] as self
			`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:2", "viewer", "user:*"),
				tuple.NewTupleKey("document:3", "viewer", "user:jon"),
			},
			expectedObjects: []string{"document:1", "document:2"},
		},
		{
			name: "indirectly_related_typed_wildcard",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User:       &commands.UserRefTypedWildcard{Type: "user"},
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("document:2", "viewer", "group:fga#member"),
				tuple.NewTupleKey("group:eng", "member", "user:*"),
			},
			expectedObjects: []string{"document:1"},
		},
		{
			name: "relationship_through_multiple_indirections",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("team:tigers", "member", "user:jon"),
				tuple.NewTupleKey("group:eng", "member", "team:tigers#member"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			},
			expectedObjects: []string{"document:1"},
		},
		{
			name: "typed_wildcard_relationship_through_multiple_indirections",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("team:tigers", "member", "user:*"),
				tuple.NewTupleKey("group:eng", "member", "team:tigers#member"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			},
			expectedObjects: []string{"document:1"},
		},
		{
			name: "simple_typed_wildcard_and_direct_relation",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{Type: "user", Id: "jon"},
				},
			},
			model: `
			type user
			type document
			  relations
			    define viewer: [user, user:*] as self
			`,
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:2", "viewer", "user:jon"),
			},
			expectedObjects: []string{"document:1", "document:2"},
		},
		{
			name: "simple_typed_wildcard_and_indirect_relation",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "user:*"),
				tuple.NewTupleKey("group:fga", "member", "user:jon"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("document:2", "viewer", "group:fga#member"),
			},
			expectedObjects: []string{"document:1", "document:2"},
		},
		{
			name: "connected_objects_with_public_user_access_1",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "user:*"),
				tuple.NewTupleKey("group:other", "member", "employee:*"), // assume this comes from a prior model
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("document:2", "viewer", "group:fga#member"),
				tuple.NewTupleKey("document:3", "viewer", "group:other#member"),
			},
			expectedObjects: []string{"document:1"},
		},
		{
			name: "connected_objects_with_public_user_access_2",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "resource",
				Relation:   "reader",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("resource:x", "writer", "user:*"),
			},
			expectedObjects: []string{"resource:x"},
		},
		{
			name: "simple_typed_wildcard_with_contextual_tuples_1",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{
					Object: &openfgapb.Object{Type: "user", Id: "jon"},
				},
				ContextualTuples: []*openfgapb.TupleKey{
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
			expectedObjects: []string{"document:1", "document:2"},
		},
		{
			name: "simple_typed_wildcard_with_contextual_tuples_2",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User:       &commands.UserRefTypedWildcard{Type: "user"},
				ContextualTuples: []*openfgapb.TupleKey{
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
			expectedObjects: []string{"document:2"},
		},
		{
			name: "simple_typed_wildcard_with_contextual_tuples_3",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObjectRelation{
					ObjectRelation: &openfgapb.ObjectRelation{
						Object:   "group:eng",
						Relation: "member",
					},
				},
				ContextualTuples: []*openfgapb.TupleKey{
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
			expectedObjects: []string{"document:1"},
		},
		{
			name: "non-assignable_ttu_relationship",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:1"),
				tuple.NewTupleKey("document:2", "parent", "folder:2"),
				tuple.NewTupleKey("folder:1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:2", "viewer", "user:*"),
			},
			expectedObjects: []string{"document:1", "document:2"},
		},
		{
			name: "non-assignable_ttu_relationship_without_wildcard_connectivity",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:1"),
				tuple.NewTupleKey("document:2", "parent", "folder:2"),
				tuple.NewTupleKey("folder:1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:2", "viewer", "user:*"),
			},
			expectedObjects: []string{"document:1"},
		},
		{
			name: "non-assignable_ttu_relationship_through_indirection_1",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User: &commands.UserRefObject{Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:1"),
				tuple.NewTupleKey("folder:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("group:eng", "member", "user:*"),
			},
			expectedObjects: []string{"document:1"},
		},
		{
			name: "non-assignable_ttu_relationship_through_indirection_2",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "resource",
				Relation:   "writer",
				User: &commands.UserRefObject{Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("resource:eng_handbook", "writer", "org:eng#dept_member"),
				tuple.NewTupleKey("org:eng", "dept", "group:fga"),
				tuple.NewTupleKey("group:fga", "member", "user:anne"),
			},
			expectedObjects: []string{"resource:eng_handbook"},
		},
		{
			name: "non-assignable_ttu_relationship_through_indirection_3",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "resource",
				Relation:   "reader",
				User: &commands.UserRefObject{Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("resource:eng_handbook", "writer", "org:eng#dept_member"),
				tuple.NewTupleKey("org:eng", "dept", "group:fga"),
				tuple.NewTupleKey("group:fga", "member", "user:anne"),
			},
			expectedObjects: []string{"resource:eng_handbook"},
		},
		{
			name: "cyclical_tupleset_relation_terminates",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "node",
				Relation:   "editor",
				User: &commands.UserRefObject{Object: &openfgapb.Object{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("node:abc", "editor", "user:wonder"),
			},
			expectedObjects: []string{"node:abc"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			ctx := context.Background()
			store := ulid.Make().String()
			test.request.StoreID = store

			model := &openfgapb.AuthorizationModel{
				Id:              ulid.Make().String(),
				SchemaVersion:   typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(test.model),
			}
			err := ds.WriteAuthorizationModel(ctx, store, model)
			require.NoError(err)
			test.request.Typesystem = typesystem.New(model)

			err = ds.Write(ctx, store, nil, test.tuples)
			require.NoError(err)

			if test.resolveNodeLimit == 0 {
				test.resolveNodeLimit = DefaultResolveNodeLimit
			}

			connectedObjectsCmd := commands.ConnectedObjectsCommand{
				Datastore:        ds,
				ResolveNodeLimit: test.resolveNodeLimit,
				Limit:            test.limit,
			}

			resultChan := make(chan commands.ListObjectsResult, 100)
			done := make(chan struct{})

			var results []string
			go func() {
				for result := range resultChan {
					results = append(results, result.ObjectID)
				}

				done <- struct{}{}
			}()

			timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			go func() {
				err = connectedObjectsCmd.StreamedConnectedObjects(timeoutCtx, test.request, resultChan)
				require.ErrorIs(err, test.expectedError)
				close(resultChan)
			}()

			select {
			case <-timeoutCtx.Done():
				require.FailNow("timed out waiting for response")
			case <-done:
			}

			if test.expectedError == nil {
				sort.Strings(results)
				sort.Strings(test.expectedObjects)

				require.Equal(test.expectedObjects, results)
			}
		})
	}
}
