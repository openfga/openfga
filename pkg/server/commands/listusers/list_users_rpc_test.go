package listusers

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/structpb"
)

type ListUsersTests []struct {
	name                  string
	TemporarilySkipReason string // Temporarily skip test until functionality is fixed
	req                   *openfgav1.ListUsersRequest
	model                 string
	tuples                []*openfgav1.TupleKey
	expectedUsers         []string
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
			TemporarilySkipReason: "because group:eng,group:fga,group:fga-backend being returned",
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
			expectedUsers: []string{"group:fga#member", "group:fga-backend#member"},
		},
		{
			name:                  "direct_relationship_unapplicable_filter",
			TemporarilySkipReason: "because this should return an error",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type: "folder",
					},
				},
			},
			model:            model,
			tuples:           []*openfgav1.TupleKey{},
			expectedUsers:    []string{},
			expectedErrorMsg: "impossible relationship between `folder` and `document#viewer`",
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
			name:                  "computed_relationship_with_possible_direct_relationship",
			TemporarilySkipReason: "because results aren't deduplicated yet",
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
			name:                  "userset_group_granularity",
			TemporarilySkipReason: "because `group:eng` is being returned instead of `group:eng#member`",
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
			name:                  "userset_group_granularity_with_incorrect_user_filter",
			TemporarilySkipReason: "because returns `group:eng` when group itself cannot be viewer of document (but group#member can)",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.ListUsersFilter{
					{
						Type:     "group",
						Relation: "", // should be "member"
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
			name:                  "userset_group_granularity_with_direct_user_relationships",
			TemporarilySkipReason: "because `group:eng` is being returned instead of `group:eng#member`",
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
			name:                  "userset_multiple_usersets_group_granularity",
			TemporarilySkipReason: "because `group:eng`,`group:fga`,`group:other` is being returned instead of `group:eng#member`,`group:fga#member` and `group:other#member`",
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
			name:                  "userset_user_assigned_multiple_groups",
			TemporarilySkipReason: "because results not deduplicated",
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
			name:                  "tuple_defines_itself",
			TemporarilySkipReason: "because it wants to return `document:1`",
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
			name:                  "ttu_with_computed_relation_user_granularity",
			TemporarilySkipReason: "because results not deduplicated",
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
			name:                  "ttu_multiple_levels",
			TemporarilySkipReason: "because deduplication not implemented yet (intermittent)",
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
			name:                  "cycle_by_userset",
			TemporarilySkipReason: "because cycle detection not implemented yet",
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
			name:                  "cycle_by_ttu",
			TemporarilySkipReason: "because cycle detection not implemented yet",
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
				define viewer: [user, document#viewer]

			type document
			relations
				define parent: [folder]
				define viewer: viewer from parent`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:x"),
				tuple.NewTupleKey("folder:x", "viewer", "document:1#viewer"),
			},
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
			TemporarilySkipReason: "because usersets don't return correct type and conditions that evaluate false don't get excluded from results",
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
			TemporarilySkipReason: "because deduplication isn't implemented and conditions that evaluate false don't get excluded from results",
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
			name:                  "intersection",
			TemporarilySkipReason: "because incurring `panic: unexpected userset rewrite encountered`",
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
			name:                  "intersection_and_ttu",
			TemporarilySkipReason: "because incurring `panic: unexpected userset rewrite encountered`",
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
			name:                  "union",
			TemporarilySkipReason: "because deduplication not implemented yet",
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
			name:                  "union_and_ttu",
			TemporarilySkipReason: "because deduplication not implemented yet",
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
			name:                  "union_all_possible_rewrites",
			TemporarilySkipReason: "because `user:maria` not being returned *and* deduplication not implemented yet",
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
			name:                  "exclusion",
			TemporarilySkipReason: "because incurring `panic: unexpected userset rewrite encountered`",
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
			name:                  "exclusion_and_ttu",
			TemporarilySkipReason: "because incurring `panic: unexpected userset rewrite encountered`",
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
			relations
			  define blocked: [user]
		  type folder
			relations
			  define viewer: [user]
			  define blocked: blocked from viewer
		  type document
			relations
			  define parent: [folder]
			  define viewer: (viewer from parent) but not blocked from parent`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "parent", "folder:x"),
				tuple.NewTupleKey("folder:x", "viewer", "user:will"),

				tuple.NewTupleKey("folder:x", "viewer", "user:maria"),
				tuple.NewTupleKey("user:maria", "blocked", "user:maria"),
			},
			expectedUsers: []string{"user:will"},
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
			name:                  "wildcard_computed_ttu",
			TemporarilySkipReason: "because results not deduplicated and data race occurring",
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
			name: "impossible_edge_several_computed_relations_away",
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
			for i, u := range resp.GetUsers() {
				if relation := u.GetUserset().GetRelation(); relation != "" {
					actualCompare[i] = fmt.Sprintf("%s:%s#%s", u.GetUserset().GetType(), u.GetUserset().GetId(), relation)
					continue
				}

				if userType := u.GetObject().GetType(); userType != "" {
					actualCompare[i] = fmt.Sprintf("%s:%s", userType, u.GetObject().GetId())
					continue
				}

				actualCompare[i] = fmt.Sprintf("%s:*", u.GetObject().GetType())
			}

			require.ElementsMatch(t, actualCompare, test.expectedUsers)
		})
	}
}
