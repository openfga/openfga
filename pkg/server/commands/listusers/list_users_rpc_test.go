package listusers

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/openfga/openfga/internal/mocks"

	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type ListUsersTests []struct {
	name                  string
	TemporarilySkipReason string // Temporarily skip test until functionality is fixed
	req                   *openfgav1.ListUsersRequest
	model                 string
	tuples                []*openfgav1.TupleKey
	expectedUsers         []string
	butNot                []string
	expectedErrorMsg      string
}

func TestListUsersDirectRelationship(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	model := `model
	schema 1.1
	type user
	type document
		relations
			define viewer: [user]`

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
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKey("document:1", "viewer", "user:maria"),
				tuple.NewTupleKey("document:2", "viewer", "user:jon"),
			},
			expectedUsers: []string{"user:will", "user:maria"},
		},
		{
			name:                  "direct_relationship_with_userset_subjects_and_userset_filter",
			TemporarilySkipReason: "because reflexive relationships not supported yet",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "group", Id: "eng"},
				Relation: "member",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type:     "group",
						Relation: "member",
					},
				},
			},
			model: `model
			schema 1.1
			type user
			type group
				relations
					define member: [user, group#member]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "group:fga#member"),
				tuple.NewTupleKey("group:fga", "member", "group:fga-backend#member"),
			},
			expectedUsers: []string{"group:fga#member", "group:fga-backend#member", "group:eng#member"},
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
			expectedUsers: []string{},
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
			expectedUsers: []string{},
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
			model:         model,
			tuples:        []*openfgav1.TupleKey{},
			expectedUsers: []string{"user:will", "user:maria"},
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsersComputedRelationship(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "computed_relationship",
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
					define owner: [user]
					define viewer: owner`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "owner", "user:will"),
				tuple.NewTupleKey("document:1", "owner", "user:maria"),
				tuple.NewTupleKey("document:2", "viewer", "user:jon"),
			},
			expectedUsers: []string{"user:will", "user:maria"},
		},
		{
			name: "computed_relationship_with_possible_direct_relationship",
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
					define owner: [user]
					define editor: [user] or owner
					define viewer: owner or editor`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "owner", "user:will"),
				tuple.NewTupleKey("document:1", "editor", "user:maria"),
				tuple.NewTupleKey("document:2", "viewer", "user:jon"),
			},
			expectedUsers: []string{"user:will", "user:maria"},
		},
		{
			name: "computed_relationship_with_contextual_tuples",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
				ContextualTuples: &openfgav1.ContextualTupleKeys{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey("document:1", "owner", "user:will"),
						tuple.NewTupleKey("document:1", "owner", "user:maria"),
						tuple.NewTupleKey("document:2", "viewer", "user:jon"),
					},
				},
			},
			model: `model
			schema 1.1
			type user
			type document
				relations
					define owner: [user]
					define viewer: owner`,
			tuples:        []*openfgav1.TupleKey{},
			expectedUsers: []string{"user:will", "user:maria"},
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsersUsersets(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	model := `model
	schema 1.1
	type user
	type group
		relations
			define member: [user]
	type document
		relations
			define viewer: [group#member]`

	tests := ListUsersTests{
		{
			name: "userset_user_granularity",
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
				tuple.NewTupleKey("group:eng", "member", "user:will"),
				tuple.NewTupleKey("group:eng", "member", "user:maria"),
				tuple.NewTupleKey("group:marketing", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			},
			expectedUsers: []string{"user:will", "user:maria"},
		},
		{
			name: "userset_group_granularity",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type:     "group",
						Relation: "member",
					},
				},
			},
			model: model,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "user:will"),
				tuple.NewTupleKey("group:eng", "member", "user:maria"),
				tuple.NewTupleKey("group:marketing", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			},
			expectedUsers: []string{"group:eng#member"},
		},
		{
			name: "userset_group_granularity_with_incorrect_user_filter",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type:     "group",
						Relation: "", // Would return results if "member"
					},
				},
			},
			model: model,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "user:will"),
				tuple.NewTupleKey("group:eng", "member", "user:maria"),
				tuple.NewTupleKey("group:marketing", "member", "user:jon"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
			},
			expectedUsers: []string{},
		},
		{
			name: "userset_group_granularity_with_direct_user_relationships",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type:     "group",
						Relation: "member",
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
					define viewer: [ user, group#member ]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "user:will"),
				tuple.NewTupleKey("group:eng", "member", "user:maria"),
				tuple.NewTupleKey("group:marketing", "member", "user:jon"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),

				tuple.NewTupleKey("document:1", "viewer", "user:poovam"),
			},
			expectedUsers: []string{"group:eng#member"},
		},
		{
			name: "userset_multiple_usersets",
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
			    define viewer: [group#member]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "user:hawker"),
				tuple.NewTupleKey("group:fga", "member", "user:jon"),
				tuple.NewTupleKey("group:eng", "member", "group:fga#member"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("document:1", "viewer", "group:other#member"),
				tuple.NewTupleKey("group:other", "member", "user:will"),
			},
			expectedUsers: []string{"user:jon", "user:hawker", "user:will"},
		},
		{
			name: "userset_multiple_usersets_group_granularity",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type:     "group",
						Relation: "member",
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
			    define viewer: [group#member]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "user:hawker"),
				tuple.NewTupleKey("group:eng", "member", "group:fga#member"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("document:1", "viewer", "group:other#member"),
			},
			expectedUsers: []string{"group:fga#member", "group:eng#member", "group:other#member"},
		},
		{
			name: "userset_user_granularity_with_contextual_tuples",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
				ContextualTuples: &openfgav1.ContextualTupleKeys{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey("group:marketing", "member", "user:jon"),
						tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
					},
				},
			},
			model: model,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "user:will"),
				tuple.NewTupleKey("group:eng", "member", "user:maria"),
			},
			expectedUsers: []string{"user:will", "user:maria"},
		},
		{
			name: "userset_user_assigned_multiple_groups",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
				ContextualTuples: &openfgav1.ContextualTupleKeys{
					TupleKeys: []*openfgav1.TupleKey{},
				},
			},
			model: model,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "user:maria"),
				tuple.NewTupleKey("group:eng", "member", "user:will"),
				tuple.NewTupleKey("group:fga", "member", "user:will"),
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("document:1", "viewer", "group:fga#member"),
			},
			expectedUsers: []string{"user:will", "user:maria"},
		},
		{
			name: "tuple_defines_itself",
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
			tuples:        []*openfgav1.TupleKey{},
			expectedUsers: []string{},
		},
		{
			name:                  "userset_defines_itself",
			TemporarilySkipReason: "because reflexive relationships not supported yet",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type:     "document",
						Relation: "viewer",
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
			tuples:        []*openfgav1.TupleKey{},
			expectedUsers: []string{"document:1#viewer"},
		},
		{
			name: "evaluate_userset_in_computed_relation_of_ttu",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "repo", Id: "fga"},
				Relation: "reader",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type:     "org",
						Relation: "member",
					},
				},
			},
			model: `model
			schema 1.1
		  type user
		  
		  type org
			relations
			  define member: [user]
			  define admin: [org#member]
		  
		  type repo
			relations
			  define owner: [org]
			  define reader: admin from owner`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:fga", "owner", "org:x"),
				tuple.NewTupleKey("org:x", "admin", "org:x#member"),
				tuple.NewTupleKey("org:x", "member", "user:will"),
			},
			expectedUsers: []string{"org:x#member"},
		},
		{
			name: "userset_with_intersection_in_computed_relation_of_ttu",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "repo", Id: "fga"},
				Relation: "reader",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type:     "org",
						Relation: "member",
					},
				},
			},
			model: `model
            schema 1.1
          type user
          
          type org
            relations
              define member: [user]
              define admin: [org#member]
		  type repo
			relations
			  define owner: [org]
			  define allowed: [user]
			  define reader: admin from owner and allowed`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:x", "owner", "org:fga"),
				tuple.NewTupleKey("org:fga", "admin", "org:fga#member"),
				tuple.NewTupleKey("repo:x", "allowed", "user:will"),
			},
			expectedUsers: []string{},
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsersTTU(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	model := `model
	schema 1.1
  type user

  type folder
	relations
	  define viewer: [user]

  type document
	relations
	  define parent: [folder]
	  define viewer: viewer from parent`

	tests := ListUsersTests{
		{
			name: "ttu_user_granularity",
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
				tuple.NewTupleKey("document:1", "parent", "folder:x"),
				tuple.NewTupleKey("folder:x", "viewer", "user:maria"),
				tuple.NewTupleKey("folder:no-doc", "viewer", "user:maria"),
				tuple.NewTupleKey("document:1", "parent", "folder:no-user"),
			},
			expectedUsers: []string{"user:maria"},
		},
		{
			name: "ttu_folder_granularity",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "folder",
					},
				},
			},
			model: model,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:x"),
				tuple.NewTupleKey("folder:x", "viewer", "user:maria"),
			},
			expectedUsers: []string{},
		},
		{
			name: "ttu_with_computed_relation_user_granularity",
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
				define owner: [user]
				define editor: [user] or owner
				define viewer: [user] or owner or editor
				define unrelated_not_computed: [user]
		
		  type document
			relations
			  define parent: [folder]
			  define viewer: viewer from parent`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:x"),
				tuple.NewTupleKey("folder:x", "viewer", "user:maria"),
				tuple.NewTupleKey("folder:x", "editor", "user:will"),
				tuple.NewTupleKey("folder:x", "owner", "user:jon"),
				tuple.NewTupleKey("folder:x", "unrelated_not_computed", "user:poovam"),

				tuple.NewTupleKey("folder:no-doc", "viewer", "user:maria"),
				tuple.NewTupleKey("document:1", "parent", "folder:no-user"),
			},
			expectedUsers: []string{"user:maria", "user:will", "user:jon"},
		},
		{
			name: "ttu_multiple_levels",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "folder", Id: "c"},
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
					define parent: [folder]
					define viewer: [user] or viewer from parent`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("folder:a", "viewer", "user:will"),
				tuple.NewTupleKey("folder:b", "parent", "folder:a"),
				tuple.NewTupleKey("folder:c", "parent", "folder:b"),

				tuple.NewTupleKey("folder:c", "parent", "folder:other"),
				tuple.NewTupleKey("folder:other", "viewer", "user:jon"),
			},
			expectedUsers: []string{"user:will", "user:jon"},
		},
	}

	tests.runListUsersTestCases(t)
}

func TestListUsersCycles(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "cycle_materialized_by_tuples",
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
					define viewer: [user, document#viewer]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "document:1#viewer"),
			},
			expectedUsers: []string{},
		},
		{
			name: "cycle_when_model_has_two_parallel_edges",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "transition", Id: "1"},
				Relation: "can_view_3",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
			model
				schema 1.1

			type user

			type state
				relations
					define can_view: [user] or can_view_3 from associated_transition
					define associated_transition: [transition]

			type transition
				relations
					define start: [state]
					define end: [state]
					define can_view: can_view from start or can_view from end
					define can_view_2: can_view
					define can_view_3: can_view_2`,
			tuples:        []*openfgav1.TupleKey{},
			expectedUsers: []string{},
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsersConditions(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	model := `model
	schema 1.1
  
  type user

  type document
	relations
	  define viewer: [user with isTrue]
  
  condition isTrue(param: string) {
	param == "true"
  }`

	conditionContextWithTrueParam, err := structpb.NewStruct(map[string]interface{}{
		"param": "true",
	})
	require.NoError(t, err)

	conditionContextWithFalseParam, err := structpb.NewStruct(map[string]interface{}{
		"param": "false",
	})
	require.NoError(t, err)

	tests := ListUsersTests{
		{
			name: "conditions_with_true_evaluation",
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
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:jon", "isTrue", conditionContextWithTrueParam),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "isTrue", conditionContextWithTrueParam),
			},
			expectedUsers: []string{"user:jon", "user:maria"},
		},
		{
			name:                  "conditions_with_false_evaluation",
			TemporarilySkipReason: "because conditions that evaluate false don't get excluded from results",
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
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:jon", "isTrue", conditionContextWithFalseParam),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "isTrue", conditionContextWithFalseParam),
			},
			expectedUsers: []string{},
		},
		{
			name:                  "conditions_with_usersets",
			TemporarilySkipReason: "because conditions that evaluate false don't get excluded from results",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type:     "group",
						Relation: "member",
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
			  define viewer: [group#member with isTrue, user]
		  
		  condition isTrue(param: string) {
			param == "true"
		  }`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "group:eng#member", "isTrue", conditionContextWithTrueParam),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "group:fga#member", "isTrue", conditionContextWithFalseParam),
				tuple.NewTupleKey("group:eng", "member", "user:jon"),
				tuple.NewTupleKey("group:eng", "member", "user:maria"),
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
			},
			expectedUsers: []string{"group:eng#member"},
		},
		{
			name:                  "conditions_with_computed_relationships",
			TemporarilySkipReason: "because conditions that evaluate false don't get excluded from results",
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
			  define owner: [user]
			  define editor: [user] or owner
			  define viewer: [user with isTrue] or editor or owner
		  
		  condition isTrue(param: string) {
			param == "true"
		  }`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "isTrue", conditionContextWithTrueParam),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:jon", "isTrue", conditionContextWithFalseParam),
				tuple.NewTupleKey("document:1", "owner", "user:will"),
				tuple.NewTupleKey("document:1", "editor", "user:poovam"),
			},
			expectedUsers: []string{"user:will", "user:poovam", "user:maria"},
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsersIntersection(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "intersection",
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
					define required: [user]
					define required_other: [user]
					define viewer: required and required_other`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "required", "user:will"),
				tuple.NewTupleKey("document:1", "required_other", "user:will"),

				tuple.NewTupleKey("document:1", "required", "user:jon"),
				tuple.NewTupleKey("document:1", "required_other", "user:maria"),
			},
			expectedUsers: []string{"user:will"},
		},
		{
			name: "intersection_multiple",
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
					define required_1: [user]
					define required_2: [user]
					define required_3: [user]
					define viewer: [user] and required_1 and required_2 and required_3`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKey("document:1", "required_1", "user:will"),
				tuple.NewTupleKey("document:1", "required_2", "user:will"),
				tuple.NewTupleKey("document:1", "required_3", "user:will"),

				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "required_1", "user:jon"),
				tuple.NewTupleKey("document:1", "required_2", "user:jon"),

				tuple.NewTupleKey("document:1", "viewer", "user:maria"),
				tuple.NewTupleKey("document:1", "required_1", "user:maria"),

				tuple.NewTupleKey("document:1", "viewer", "user:poovam"),
			},
			expectedUsers: []string{"user:will"},
		},
		{
			name: "intersection_at_multiple_levels",
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
				define required: [user]
				define owner: [user] and required
				define editor: [user] and owner
				define viewer: [user] and editor`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "required", "user:will"),
				tuple.NewTupleKey("document:1", "owner", "user:will"),
				tuple.NewTupleKey("document:1", "editor", "user:will"),
				tuple.NewTupleKey("document:1", "viewer", "user:will"),

				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "owner", "user:jon"),
				tuple.NewTupleKey("document:1", "editor", "user:jon"),

				tuple.NewTupleKey("document:1", "viewer", "user:maria"),
				tuple.NewTupleKey("document:1", "owner", "user:maria"),
				tuple.NewTupleKey("document:1", "required", "user:maria"),
			},
			expectedUsers: []string{"user:will"},
		},
		{
			name: "intersection_and_ttu",
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
			  define required: [user]
			  define parent: [folder]
			  define viewer: (viewer from parent) and required`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "required", "user:will"),
				tuple.NewTupleKey("folder:x", "viewer", "user:will"),
				tuple.NewTupleKey("document:1", "parent", "folder:x"),

				tuple.NewTupleKey("document:1", "required", "user:maria"),
				tuple.NewTupleKey("folder:x", "viewer", "user:jon"),
			},
			expectedUsers: []string{"user:will"},
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsersUnion(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "union",
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
					define optional_1: [user]
					define optional_2: [user]
					define viewer: optional_1 or optional_2`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "optional_1", "user:will"),
				tuple.NewTupleKey("document:1", "optional_2", "user:will"),

				tuple.NewTupleKey("document:1", "optional_1", "user:jon"),
				tuple.NewTupleKey("document:1", "optional_2", "user:maria"),
			},
			expectedUsers: []string{"user:will", "user:jon", "user:maria"},
		},
		{
			name: "union_and_ttu",
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
			  define optional: [user]
			  define parent: [folder]
			  define viewer: (viewer from parent) or optional`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "optional", "user:will"),
				tuple.NewTupleKey("folder:x", "viewer", "user:will"),
				tuple.NewTupleKey("document:1", "parent", "folder:x"),

				tuple.NewTupleKey("document:1", "optional", "user:maria"),
				tuple.NewTupleKey("folder:x", "viewer", "user:jon"),
			},
			expectedUsers: []string{"user:will", "user:maria", "user:jon"},
		},
		{
			name: "union_all_possible_rewrites",
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
					define editor: [user]
					define viewer: [user] or editor or viewer from parent`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("folder:x", "viewer", "user:will"),
				tuple.NewTupleKey("document:1", "parent", "folder:x"),
				tuple.NewTupleKey("document:1", "editor", "user:maria"),
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:other", "viewer", "user:poovam"),
			},
			expectedUsers: []string{"user:jon", "user:maria", "user:will"},
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsersExclusion(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "exclusion",
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
					define blocked: [user]
					define viewer: [user] but not blocked`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:blocked_user"),
				tuple.NewTupleKey("document:1", "blocked", "user:blocked_user"),
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKey("document:1", "blocked", "user:another_blocked_user"),
			},
			expectedUsers: []string{"user:will"},
		},
		{
			name: "exclusion_multiple",
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
			  define blocked_1: [user]
			  define blocked_2: [user]
			  define viewer: ([user] but not blocked_1) but not blocked_2`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:will"),

				tuple.NewTupleKey("document:1", "viewer", "user:maria"),
				tuple.NewTupleKey("document:1", "blocked_1", "user:maria"),

				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "blocked_2", "user:jon"),

				tuple.NewTupleKey("document:1", "viewer", "user:poovam"),
				tuple.NewTupleKey("document:1", "blocked_1", "user:poovam"),
				tuple.NewTupleKey("document:1", "blocked_2", "user:poovam"),
			},
			expectedUsers: []string{"user:will"},
		},
		{
			name: "exclusion_chained_computed",
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
		  
		  type org
			relations
			  define blocked: [user]
		  
		  type user    
		  
		  type document
			relations
			  define parent: [org]
			  define owner: [user]
			  define blocked: blocked from parent
			  define editor: owner but not blocked
			  define viewer: editor`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "org:x"),
				tuple.NewTupleKey("document:1", "owner", "user:will"),
				tuple.NewTupleKey("document:1", "owner", "user:poovam"),

				tuple.NewTupleKey("org:x", "blocked", "user:poovam"),
			},
			expectedUsers: []string{"user:will"},
		},
		{
			name: "exclusion_and_ttu",
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
		  
		  type org
			relations
			  define blocked: [user]
		  
		  type folder
			relations
			  define blocked: blocked from org
			  define org: [org]
			  define viewer: [user]
		  
		  type document
			relations
			  define parent: [folder]
			  define viewer: viewer from parent but not blocked from parent
		  `,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("folder:x", "org", "org:x"),

				tuple.NewTupleKey("document:1", "parent", "folder:x"),
				tuple.NewTupleKey("folder:x", "viewer", "user:will"),

				tuple.NewTupleKey("folder:x", "viewer", "user:maria"),
				tuple.NewTupleKey("org:x", "blocked", "user:maria"),
			},
			expectedUsers: []string{"user:will"},
		},
		{
			name:                  "exclusion_and_self_referential_tuples_1",
			TemporarilySkipReason: "because reflexive relationships not supported yet",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "group", Id: "1"},
				Relation: "member",
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
			  define member: [user, group#member] but not other
			  define other: [user, group#member]`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "other", "group:1#member"),
				tuple.NewTupleKey("group:1", "member", "user:will"),
			},
			expectedUsers: []string{},
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsersExclusionWildcards(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	model := `model
	schema 1.1
  
  type user
  
  type document
	relations
	  define blocked: [user:*,user]
	  define viewer: [user:*,user] but not blocked`

	tests := ListUsersTests{
		{
			name: "exclusion_and_wildcards_1",
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
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKey("document:1", "blocked", "user:*"),
			},
			expectedUsers: []string{},
		},
		{
			name: "exclusion_and_wildcards_2",
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
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKey("document:1", "blocked", "user:will"),
				tuple.NewTupleKey("document:1", "blocked", "user:*"),
			},
			expectedUsers: []string{},
		},
		{
			name: "exclusion_and_wildcards_3",
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
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "blocked", "user:will"),
				tuple.NewTupleKey("document:1", "blocked", "user:*"),
			},
			expectedUsers: []string{},
		},
		{
			name: "exclusion_and_wildcards_4",
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
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "blocked", "user:maria"),
			},
			expectedUsers: []string{"user:*"},
			butNot:        []string{"user:maria"},
		},
		{
			name: "exclusion_and_wildcards_5",
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
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "viewer", "user:maria"),
				tuple.NewTupleKey("document:1", "blocked", "user:*"),
			},
			expectedUsers: []string{},
		},
		{
			name: "exclusion_and_wildcards_6",
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
			  define blocked: [user:*,user]
			  define viewer: [user:*,user] but not blocked`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"), // base wildcard
				tuple.NewTupleKey("document:1", "viewer", "user:maria"),
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "blocked", "user:jon"),
				tuple.NewTupleKey("document:1", "blocked", "user:will"),
			},
			expectedUsers: []string{"user:*", "user:maria"},
			butNot:        []string{"user:jon", "user:will"},
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsersWildcards(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "direct_relationship_wildcard",
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
					define viewer: [user:*]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:2", "viewer", "user:*"),
			},
			expectedUsers: []string{"user:*"},
		},
		{
			name: "direct_relationship_wildcard_with_direct_relationships_also",
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
					define viewer: [user:*,user]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKey("document:2", "viewer", "user:maria"),
			},
			expectedUsers: []string{"user:*", "user:will"},
		},
		{
			name: "multiple_possible_wildcards_user_granularity",
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
			type document
				relations
					define viewer: [ group:*, user:*]`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "viewer", "group:*"),
			},
			expectedUsers: []string{"user:*"},
		},
		{
			name: "wildcard_with_indirection",
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
					define member: [user:*]
			type document
				relations
					define viewer: [group#member]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
				tuple.NewTupleKey("group:eng", "member", "user:*"),
			},
			expectedUsers: []string{"user:*"},
		},
		{
			name: "wildcard_computed_ttu",
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
              define member: [user:*]
          type folder
            relations
              define can_view: viewer or can_view from parent
              define parent: [folder]
              define viewer: [group#member]
          type document
            relations
              define parent: [folder]
              define viewer: can_view from parent`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:eng", "member", "user:*"),
				tuple.NewTupleKey("folder:eng", "viewer", "group:eng#member"),
				tuple.NewTupleKey("folder:x", "parent", "folder:eng"),
				tuple.NewTupleKey("document:1", "parent", "folder:x"),
				tuple.NewTupleKey("document:1", "parent", "folder:eng"),
			},
			expectedUsers: []string{"user:*"},
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsersEdgePruning(t *testing.T) {
	tests := ListUsersTests{
		{
			name: "valid_edges",
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
			  define viewer: [user]`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			},
			expectedUsers: []string{"user:maria"},
		},
		{
			name: "valid_edge_several_computed_relations_away",
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
				define parent: [user]
				define owner: parent
				define editor: owner
				define viewer: editor`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "user:maria"),
			},
			expectedUsers: []string{"user:maria"},
		},

		{
			name: "user_filter_has_invalid_edge",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "folder",
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
				define viewer: viewer from parent`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:x"),
				tuple.NewTupleKey("folder:x", "viewer", "user:1"),
			},
			expectedUsers: []string{},
		},
		{
			name: "user_filter_has_valid_edge",
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
				define viewer: viewer from parent`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:x"),
				tuple.NewTupleKey("folder:x", "viewer", "user:maria"),
			},
			expectedUsers: []string{"user:maria"},
		},
		{
			name: "user_filter_has_invalid_edge_because_relation",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "INVALID_RELATION",
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
				define viewer: viewer from parent`,
			expectedErrorMsg: "'document#INVALID_RELATION' relation is undefined",
		},
	}

	tests.runListUsersTestCases(t)
}

func TestListUsersWildcardsAndIntersection(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "wildcard_and_intersection",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "can_view",
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
              define allowed: [user]
              define viewer: [user:*,user] and allowed
              define can_view: viewer`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "allowed", "user:jon"),
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
			expectedUsers: []string{"user:jon"},
		},
		{
			name: "with_multiple_wildcards_1",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "is_public",
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
			  define public_1: [user:*,user]
			  define public_2: [user:*,user]
			  define is_public: public_1 and public_2`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "public_1", "user:maria"),
				tuple.NewTupleKey("document:1", "public_2", "user:maria"),

				tuple.NewTupleKey("document:1", "public_1", "user:*"),
				tuple.NewTupleKey("document:1", "public_2", "user:*"),

				tuple.NewTupleKey("document:1", "public_1", "user:jon"),
			},
			expectedUsers: []string{"user:maria", "user:*", "user:jon"},
		},
		{
			name: "with_multiple_wildcards_2",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "can_view",
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
		      define viewer: [user:*,user]
		      define this_is_not_assigned_to_any_user: [user]
		      define can_view: viewer and this_is_not_assigned_to_any_user`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
			},
			expectedUsers: []string{},
		},
		{
			name: "with_multiple_wildcards_3",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "can_view",
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
		      define viewer: [user:*,user]
		      define required: [user:*,user]
		      define can_view: viewer and required`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "required", "user:*"),
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKey("document:1", "required", "user:will"),
			},
			expectedUsers: []string{"user:*", "user:will"},
		},
		{
			name: "with_multiple_wildcards_4",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "can_view",
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
		      define required_1: [user]
		      define required_2: [user]
		      define can_view: required_1 and required_2`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "required_1", "user:*"), // Invalid tuple, wildcard not allowed
				tuple.NewTupleKey("document:1", "required_2", "user:*"), // Invalid tuple, wildcard not allowed
				tuple.NewTupleKey("document:1", "required_1", "user:maria"),
				tuple.NewTupleKey("document:1", "required_2", "user:maria"),
			},
			expectedUsers: []string{"user:maria"},
		},

		{
			name: "with_multiple_wildcards_5",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "can_view",
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
		      define required_1: [user]
		      define required_2: [user:*]
		      define can_view: required_1 and required_2`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "required_1", "user:maria"),
				tuple.NewTupleKey("document:1", "required_2", "user:*"),
			},
			expectedUsers: []string{"user:maria"},
		},
		{
			name: "with_multiple_wildcards_6",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "can_view",
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
		      define required_1: [user]
		      define required_2: [user]
		      define can_view: required_1 and required_2`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "required_1", "user:maria"),
				tuple.NewTupleKey("document:1", "required_1", "user:jon"),

				tuple.NewTupleKey("document:1", "required_1", "user:will"),
				tuple.NewTupleKey("document:1", "required_2", "user:will"),

				tuple.NewTupleKey("document:1", "required_1", "user:*"), // Invalid tuple, wildcard not allowed
				tuple.NewTupleKey("document:1", "required_2", "user:*"), // Invalid tuple, wildcard not allowed
			},
			expectedUsers: []string{"user:will"},
		},
		{
			name: "with_multiple_wildcards_7",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "can_view",
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
		      define required_1: [user,user:*]
		      define required_2: [user,user:*]
		      define can_view: required_1 and required_2`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "required_1", "user:maria"),
				tuple.NewTupleKey("document:1", "required_1", "user:jon"),
				tuple.NewTupleKey("document:1", "required_1", "user:*"),
				tuple.NewTupleKey("document:1", "required_2", "user:will"),
			},
			expectedUsers: []string{"user:will"},
		},
		{
			name: "wildcard_intermediate_expansion",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "can_view",
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
			  define member: [user:*, user]
		  
		  type document
			relations
			  define group: [group]
			  define viewer: [group#member] and member from group
			  define can_view: viewer`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "group:y#member"),
				tuple.NewTupleKey("document:1", "group", "group:x"),
				tuple.NewTupleKey("group:x", "member", "user:*"),
				tuple.NewTupleKey("group:y", "member", "user:jon"),
			},
			expectedUsers: []string{"user:jon"},
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsersCycleDetection(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	storeID := ulid.Make().String()
	modelID := ulid.Make().String()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	// Times(0) ensures that we exit quickly
	mockDatastore.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	l := NewListUsersQuery(mockDatastore)
	channelDone := make(chan struct{})
	channelWithResults := make(chan foundUser)
	channelWithError := make(chan error, 1)
	model := testutils.MustTransformDSLToProtoWithID(`
	model
		schema 1.1
	type user
	type document
		relations
			define viewer: [user]
	`)
	typesys := typesystem.New(model)
	ctx := typesystem.ContextWithTypesystem(context.Background(), typesys)

	t.Run("enters_loop_detection", func(t *testing.T) {
		visitedUserset := &openfgav1.UsersetUser{
			Type:     "document",
			Id:       "1",
			Relation: "viewer",
		}
		visitedUsersetKey := fmt.Sprintf("%s:%s#%s", visitedUserset.GetType(), visitedUserset.GetId(), visitedUserset.GetRelation())
		visitedUsersets := make(map[string]struct{})
		visitedUsersets[visitedUsersetKey] = struct{}{}

		go func() {
			err := l.expand(ctx, &internalListUsersRequest{
				ListUsersRequest: &openfgav1.ListUsersRequest{
					StoreId:              storeID,
					AuthorizationModelId: modelID,
					Object: &openfgav1.Object{
						Type: visitedUserset.GetType(),
						Id:   visitedUserset.GetId(),
					},
					Relation: visitedUserset.GetRelation(),
					UserFilters: []*openfgav1.ListUsersFilter{{
						Type: "user",
					}},
				},
				visitedUsersetsMap: visitedUsersets,
			}, channelWithResults)
			if err != nil {
				channelWithError <- err
				return
			}
			channelDone <- struct{}{}
		}()

		select {
		case <-channelWithError:
			require.FailNow(t, "expected 0 errors")
		case <-channelWithResults:
			require.FailNow(t, "expected 0 results")
		case <-channelDone:
			break
		}
	})
}

func TestListUsersChainedNegation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	model := `model
		schema 1.1
	type user
	type document
		relations
			define viewer: [user, user:*] but not blocked
			define blocked: [user, user:*] but not unblocked
			define unblocked: [user, user:*]`

	tests := ListUsersTests{
		{
			name: "chained_negation_1",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
			},
			expectedUsers: []string{"user:jon"},
		},
		{
			name: "chained_negation_2",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "unblocked", "user:jon"),
			},
			expectedUsers: []string{"user:jon"},
		},
		{
			name: "chained_negation_3",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "unblocked", "user:*"),
			},
			expectedUsers: []string{"user:jon"},
		},
		{
			name: "chained_negation_4",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "unblocked", "user:jon"),
			},
			expectedUsers: []string{"user:*"},
		},
		{
			name: "chained_negation_5",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "blocked", "user:*"),
				tuple.NewTupleKey("document:1", "unblocked", "user:*"),
			},
			expectedUsers: []string{"user:*"},
		},
		{
			name: "chained_negation_6",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "blocked", "user:jon"),
				tuple.NewTupleKey("document:1", "unblocked", "user:jon"),
			},
			expectedUsers: []string{"user:jon"},
		},
		{
			name: "chained_negation_7",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "blocked", "user:jon"),
				tuple.NewTupleKey("document:1", "unblocked", "user:*"),
			},
			expectedUsers: []string{"user:jon"},
		},
		{
			name: "chained_negation_8",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "blocked", "user:*"),
				tuple.NewTupleKey("document:1", "unblocked", "user:*"),
				tuple.NewTupleKey("document:1", "unblocked", "user:maria"),
			},
			expectedUsers: []string{"user:jon"},
		},
		{
			name: "chained_negation_9",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "blocked", "user:*"),
				tuple.NewTupleKey("document:1", "unblocked", "user:jon"),
			},
			expectedUsers: []string{},
		},
		{
			name: "chained_negation_10",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "blocked", "user:jon"),
				tuple.NewTupleKey("document:1", "blocked", "user:maria"),
				tuple.NewTupleKey("document:1", "unblocked", "user:jon"),
			},
			expectedUsers: []string{"user:*"},
			butNot:        []string{"user:maria"},
		},
		{
			name: "chained_negation_11",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "blocked", "user:*"),
				tuple.NewTupleKey("document:1", "unblocked", "user:jon"),
			},
			expectedUsers: []string{"user:jon"},
		},
		{
			name: "chained_negation_12",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "blocked", "user:jon"),
				tuple.NewTupleKey("document:1", "unblocked", "user:*"),
			},
			expectedUsers: []string{"user:*", "user:jon"},
		},
		{
			name: "chained_negation_13",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:1", "viewer", "user:maria"),
				tuple.NewTupleKey("document:1", "blocked", "user:maria"),
				tuple.NewTupleKey("document:1", "unblocked", "user:*"),
			},
			expectedUsers: []string{"user:*", "user:jon", "user:maria"},
		},
	}

	for i := range tests {
		tests[i].model = model
		tests[i].req = &openfgav1.ListUsersRequest{
			Object:      &openfgav1.Object{Type: "document", Id: "1"},
			Relation:    "viewer",
			UserFilters: []*openfgav1.ListUsersFilter{{Type: "user"}},
		}
	}

	tests.runListUsersTestCases(t)
}

func (testCases ListUsersTests) runListUsersTestCases(t *testing.T) {
	storeID := ulid.Make().String()

	for _, test := range testCases {
		ds := memory.New()
		t.Cleanup(ds.Close)
		model := testutils.MustTransformDSLToProtoWithID(test.model)

		t.Run(test.name, func(t *testing.T) {
			if test.TemporarilySkipReason != "" {
				t.Skip()
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

			test.req.AuthorizationModelId = model.GetId()
			test.req.StoreId = storeID

			resp, err := l.ListUsers(ctx, test.req)

			actualErrorMsg := ""
			if err != nil {
				actualErrorMsg = err.Error()
			}
			require.Equal(t, test.expectedErrorMsg, actualErrorMsg)

			actualUsers := resp.GetUsers()
			actualCompare := make([]string, len(actualUsers))
			for i, u := range actualUsers {
				actualCompare[i] = tuple.UserProtoToString(u)
			}
			require.ElementsMatch(t, actualCompare, test.expectedUsers)

			exceptUsers := resp.GetExcludedUsers()
			actualCompare = make([]string, len(exceptUsers))
			for i, u := range exceptUsers {
				actualCompare[i] = tuple.UserProtoToString(u)
			}
			require.ElementsMatch(t, actualCompare, test.butNot)
		})
	}
}
