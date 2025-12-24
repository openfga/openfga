package listusers

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type NewListUsersQueryHandler func(ds storage.RelationshipTupleReader, contextualTuples []*openfgav1.TupleKey, opts ...ListUsersQueryOption) *listUsersQuery
type ListUsersTests []struct {
	name              string
	req               *openfgav1.ListUsersRequest
	model             string
	tuples            []*openfgav1.TupleKey
	expectedUsers     []string
	expectedErrorMsg  string
	newListUsersQuery NewListUsersQueryHandler
}

const maximumRecursiveDepth = 25

var emptyContextualTuples []*openfgav1.TupleKey

func TestListUsersDirectRelationship(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	model := `
		model
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
			name: "direct_relationship_with_userset_subjects_and_userset_filter",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "group", Id: "eng"},
				Relation: "member",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type:     "group",
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "user:will"),
					tuple.NewTupleKey("document:1", "viewer", "user:maria"),
					tuple.NewTupleKey("document:2", "viewer", "user:jon"),
				},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "owner", "user:will"),
					tuple.NewTupleKey("document:1", "owner", "user:maria"),
					tuple.NewTupleKey("document:2", "viewer", "user:jon"),
				},
			},
			model: `
				model
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
	model := `
		model
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type:     "group",
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type:     "group",
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("group:marketing", "member", "user:jon"),
					tuple.NewTupleKey("document:1", "viewer", "group:eng#member"),
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
				ContextualTuples: []*openfgav1.TupleKey{},
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "document",
					},
				},
			},
			model: `
				model
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
			name: "userset_defines_itself",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type:     "document",
						Relation: "viewer",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type:     "org",
						Relation: "member",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type:     "org",
						Relation: "member",
					},
				},
			},
			model: `
				model
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
	model := `
		model
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
			name: "cycle_and_union",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "can_view",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer1: [user, document#viewer1]
						define viewer2: [user, document#viewer2]
						define can_view: viewer1 or viewer2`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer1", "document:1#viewer1"),
				tuple.NewTupleKey("document:1", "viewer2", "document:1#viewer2"),
			},
			expectedUsers: []string{},
		},
		{
			name: "cycle_and_intersection",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "can_view",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define viewer1: [user, document#viewer1]
						define viewer2: [user, document#viewer2]
						define can_view: viewer1 and viewer2`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer1", "document:1#viewer1"),
				tuple.NewTupleKey("document:1", "viewer2", "document:1#viewer2"),
			},
			expectedUsers: []string{},
		},
		{
			name: "cycle_when_model_has_two_parallel_edges",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "transition", Id: "1"},
				Relation: "can_view_3",
				UserFilters: []*openfgav1.UserTypeFilter{
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
	model := `
		model
			schema 1.1
		type user

		type document
			relations
				define viewer: [user with isTrue]

		condition isTrue(param: bool) {
			param
		}`

	tests := ListUsersTests{
		{
			name: "conditions_with_true_evaluation",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
				Context: testutils.MustNewStruct(t, map[string]interface{}{"param": true}),
			},
			model: model,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:jon", "isTrue", nil),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "isTrue", nil),
			},
			expectedUsers: []string{"user:jon", "user:maria"},
		},
		{
			name: "conditions_with_false_evaluation",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
				Context: testutils.MustNewStruct(t, map[string]interface{}{"param": false}),
			},
			model: model,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:jon", "isTrue", nil),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "isTrue", nil),
			},
			expectedUsers: []string{},
		},
		{
			name: "conditions_with_usersets",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type:     "group",
						Relation: "member",
					},
				},
				Context: testutils.MustNewStruct(t, map[string]interface{}{"param": true}),
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
						define viewer: [group#member with isTrue, user]

				condition isTrue(param: bool) {
					param
				}`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "group:eng#member", "isTrue", nil),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "group:fga#member", "isTrue", nil),
				tuple.NewTupleKey("group:eng", "member", "user:jon"),
				tuple.NewTupleKey("group:eng", "member", "user:maria"),
				tuple.NewTupleKey("document:1", "viewer", "user:will"),
			},
			expectedUsers: []string{"group:eng#member", "group:fga#member"},
		},
		{
			name: "conditions_with_computed_relationships",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
				Context: testutils.MustNewStruct(t, map[string]interface{}{"param": true}),
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
						define owner: [user]
						define editor: [user] or owner
						define viewer: [user with isTrue] or editor or owner

				condition isTrue(param: bool) {
					param
				}`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "isTrue", nil),
				tuple.NewTupleKey("document:1", "owner", "user:will"),
				tuple.NewTupleKey("document:1", "editor", "user:poovam"),
			},
			expectedUsers: []string{"user:will", "user:poovam", "user:maria"},
		},
		{
			name: "conditions_with_ttu",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
				Context: testutils.MustNewStruct(t, map[string]interface{}{"param": true}),
			},
			model: `
				model
					schema 1.1
				type user

				type folder
					relations
						define viewer: [user]

				type document
					relations
					define parent: [folder with isTrue]
					define viewer: viewer from parent

				condition isTrue(param: bool) {
					param
				}`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:1", "parent", "folder:x", "isTrue", nil),
				tuple.NewTupleKey("folder:x", "viewer", "user:jon"),
				tuple.NewTupleKeyWithCondition("document:1", "parent", "folder:y", "isTrue", nil),
				tuple.NewTupleKey("folder:y", "viewer", "user:maria"),
			},
			expectedUsers: []string{"user:jon", "user:maria"},
		},
		{
			name: "multiple_conditions_no_param_provided",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
				Context: testutils.MustNewStruct(t, map[string]interface{}{}),
			},
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define viewer: [user]

				condition isEqualToFive(param1: int) {
					param1 == 5
				}

				condition isEqualToTen(param2: int) {
					param2 == 10
				}`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:will", "isEqualToFive", nil),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "isEqualToTen", nil),
			},
			expectedUsers: []string{},
		},
		{
			name: "multiple_conditions_some_params_provided",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
				Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": 5}),
			},
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [user with isEqualToFive, user with isEqualToTen]

				condition isEqualToFive(param1: int) {
					param1 == 5
				}

				condition isEqualToTen(param2: int) {
					param2 == 10
				}`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:will", "isEqualToFive", nil),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "isEqualToTen", nil),
			},
			expectedErrorMsg: "failed to evaluate relationship condition: 'isEqualToTen' - tuple 'document:1#viewer@user:maria' is missing context parameters '[param2]",
		},
		{
			name: "multiple_conditions_all_params_provided",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
				Context: testutils.MustNewStruct(t, map[string]interface{}{"param1": 5, "param2": 10}),
			},
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [user with isEqualToFive, user with isEqualToTen]

				condition isEqualToFive(param1: int) {
					param1 == 5
				}

				condition isEqualToTen(param2: int) {
					param2 == 10
				}`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:will", "isEqualToFive", nil),
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "isEqualToTen", nil),
			},
			expectedUsers: []string{"user:will", "user:maria"},
		},
		{
			name: "error_in_direct_eval",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
				Context: testutils.MustNewStruct(t, map[string]interface{}{"x": "1.79769313486231570814527423731704356798070e+309"}),
			},
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [user with condFloat]

				condition condFloat(x: double) {
					x > 0.0
				}`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "condFloat", nil),
			},
			expectedErrorMsg: "failed to evaluate relationship condition: parameter type error on condition 'condFloat' - failed to convert context parameter 'x': number cannot be represented as a float64: 1.797693135e+309",
		},
		{
			name: "error_in_ttu_eval",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
				Context: testutils.MustNewStruct(t, map[string]interface{}{"x": "1.79769313486231570814527423731704356798070e+309"}),
			},
			model: `
				model
					schema 1.1
				type user
				type folder
					relations
						define viewer: [user with condFloat]
				type document
					relations
						define parent: [folder with condFloat]
						define viewer: viewer from parent

				condition condFloat(x: double) {
					x > 0.0
				}`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("document:1", "parent", "folder:x", "condFloat", nil),
			},
			expectedErrorMsg: "failed to evaluate relationship condition: parameter type error on condition 'condFloat' - failed to convert context parameter 'x': number cannot be represented as a float64: 1.797693135e+309",
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
		{
			name: "excluded_subjects_of_subtracted_branches_propagate",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: viewer1 and viewer2
						define viewer1: [user,user:*] but not blocked
						define viewer2: [user,user:*] but not blocked
						define blocked: [user,user:*]`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer1", "user:*"),
				tuple.NewTupleKey("document:1", "viewer2", "user:*"),

				tuple.NewTupleKey("document:1", "blocked", "user:maria"),
			},
			expectedUsers: []string{"user:*"},
		},
		{
			name: "intersection_and_wildcards_with_negation_1",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
					schema 1.1

				type user
				type document
					relations
						define b: [user:*] but not b1
						define b1: [user]
						define a: [user:*] but not a1
						define a1: [user]
						define viewer: a and b`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "b", "user:*"),
				tuple.NewTupleKey("document:1", "b1", "user:will"),
				tuple.NewTupleKey("document:1", "a", "user:*"),
				tuple.NewTupleKey("document:1", "a1", "user:maria"),
			},
			expectedUsers: []string{"user:*"},
		},
		{
			name: "intersection_and_wildcards_with_negation_2",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: viewer1 and viewer2
						define viewer1: [user,user:*] but not blocked1
						define viewer2: [user,user:*] but not blocked2
						define blocked1: [user,user:*]
						define blocked2: [user,user:*]`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer1", "user:*"),
				tuple.NewTupleKey("document:1", "viewer2", "user:*"),

				tuple.NewTupleKey("document:1", "blocked1", "user:will"),

				tuple.NewTupleKey("document:1", "blocked1", "user:maria"),
				tuple.NewTupleKey("document:1", "blocked2", "user:maria"),
			},
			expectedUsers: []string{"user:*"},
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
		{
			name: "union_and_wildcards_with_negation_1",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: viewer1 or viewer2
						define viewer1: [user,user:*] but not blocked
						define viewer2: [user,user:*] but not blocked
						define blocked: [user,user:*]`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer1", "user:*"),
				tuple.NewTupleKey("document:1", "viewer2", "user:*"),

				tuple.NewTupleKey("document:1", "blocked", "user:maria"),
			},
			expectedUsers: []string{"user:*"},
		},
		{
			name: "union_and_wildcards_with_negation_2",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: viewer1 or viewer2
						define viewer1: [user,user:*] but not blocked1
						define viewer2: [user,user:*] but not blocked2
						define blocked1: [user]
						define blocked2: [user]`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer1", "user:*"),
				tuple.NewTupleKey("document:1", "viewer2", "user:*"),

				tuple.NewTupleKey("document:1", "blocked1", "user:will"),

				tuple.NewTupleKey("document:1", "blocked1", "user:maria"),
				tuple.NewTupleKey("document:1", "blocked2", "user:maria"),
			},
			expectedUsers: []string{"user:*"},
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
			name: "exclusion_and_self_referential_tuples_1",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "group", Id: "1"},
				Relation: "member",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
					schema 1.1

				type user

				type group
					relations
						define member: [user, group#member] but not blocked
						define blocked: [user, group#member]`,

			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("group:1", "blocked", "group:1#member"),
				tuple.NewTupleKey("group:1", "member", "user:will"),
			},
			expectedUsers: []string{},
		},
		{
			name: "exclusion_with_chained_negation",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "2"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define unblocked: [user]
						define blocked: [user, document#viewer] but not unblocked
						define viewer: [user, document#blocked] but not blocked
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "document:2#blocked"),
				tuple.NewTupleKey("document:2", "blocked", "document:1#viewer"),
				tuple.NewTupleKey("document:2", "viewer", "user:jon"),
				tuple.NewTupleKey("document:2", "unblocked", "user:jon"),
			},
			expectedUsers: []string{"user:jon"},
		},
		{
			name: "non_stratifiable_exclusion_containing_cycle_1",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type:     "document",
						Relation: "blocked",
					},
				},
			},
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define blocked: [user, document#viewer]
						define viewer: [user, document#blocked] but not blocked
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "document:2#blocked"),
				tuple.NewTupleKey("document:2", "blocked", "document:1#viewer"),
			},
			expectedUsers: []string{"document:2#blocked"},
		},
	}
	tests.runListUsersTestCases(t)
}

func TestListUsersExclusionWildcards(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	model := `
		model
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
				UserFilters: []*openfgav1.UserTypeFilter{
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
		},
		{
			name: "exclusion_and_wildcards_5",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
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
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "valid_edges",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "folder",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
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
	mockDatastore.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	l := NewListUsersQuery(mockDatastore, emptyContextualTuples, WithResolveNodeLimit(maximumRecursiveDepth))
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
	typesys, err := typesystem.New(model)
	require.NoError(t, err)
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
			resp := l.expand(ctx, &internalListUsersRequest{
				ListUsersRequest: &openfgav1.ListUsersRequest{
					StoreId:              storeID,
					AuthorizationModelId: modelID,
					Object: &openfgav1.Object{
						Type: visitedUserset.GetType(),
						Id:   visitedUserset.GetId(),
					},
					Relation: visitedUserset.GetRelation(),
					UserFilters: []*openfgav1.UserTypeFilter{{
						Type: "user",
					}},
				},
				visitedUsersetsMap: visitedUsersets,
			}, channelWithResults)
			if resp.err != nil {
				channelWithError <- resp.err
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

	model := `
		model
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
				tuple.NewTupleKey("document:1", "unblocked", "user:maria"),
			},
			expectedUsers: []string{"user:jon"},
		},
		{
			name: "chained_negation_4",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "viewer", "user:maria"),
				tuple.NewTupleKey("document:1", "unblocked", "user:jon"),
			},
			expectedUsers: []string{"user:*", "user:maria"},
		},
		{
			name: "chained_negation_5",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "blocked", "user:*"),
				tuple.NewTupleKey("document:1", "unblocked", "user:*"),

				tuple.NewTupleKey("document:1", "blocked", "user:jon"),

				tuple.NewTupleKey("document:1", "blocked", "user:maria"),
				tuple.NewTupleKey("document:1", "unblocked", "user:maria"),
			},
			expectedUsers: []string{"user:*", "user:maria", "user:jon"},
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
			expectedUsers: []string{"user:jon"},
		},
		{
			name: "chained_negation_10",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "blocked", "user:jon"),
				tuple.NewTupleKey("document:1", "blocked", "user:maria"),
				tuple.NewTupleKey("document:1", "unblocked", "user:jon"),
				tuple.NewTupleKey("document:1", "unblocked", "user:poovam"),
			},
			expectedUsers: []string{"user:*", "user:jon"},
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
		{
			name: "chained_negation_14",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "blocked", "user:*"),
				tuple.NewTupleKey("document:1", "blocked", "user:maria"),
				tuple.NewTupleKey("document:1", "unblocked", "user:*"),
			},
			expectedUsers: []string{"user:*", "user:maria"},
		},
		{
			name: "chained_negation_15",
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "user:*"),
				tuple.NewTupleKey("document:1", "blocked", "user:maria"),
				tuple.NewTupleKey("document:1", "unblocked", "user:*"),
			},
			expectedUsers: []string{"user:*", "user:maria"},
		},
	}

	for i := range tests {
		tests[i].model = model
		tests[i].req = &openfgav1.ListUsersRequest{
			Object:      &openfgav1.Object{Type: "document", Id: "1"},
			Relation:    "viewer",
			UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
		}
	}

	tests.runListUsersTestCases(t)
}

func TestListUsersDepthExceeded(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	model := `
		model
			schema 1.1
		type user

		type folder
			relations
				define parent: [folder]
				define viewer: [user] or viewer from parent`

	tuples := []*openfgav1.TupleKey{
		tuple.NewTupleKey("folder:26", "viewer", "user:maria"),
		tuple.NewTupleKey("folder:25", "parent", "folder:26"),
		tuple.NewTupleKey("folder:24", "parent", "folder:25"),
		tuple.NewTupleKey("folder:23", "parent", "folder:24"),
		tuple.NewTupleKey("folder:22", "parent", "folder:23"),
		tuple.NewTupleKey("folder:21", "parent", "folder:22"),
		tuple.NewTupleKey("folder:20", "parent", "folder:21"),
		tuple.NewTupleKey("folder:19", "parent", "folder:20"),
		tuple.NewTupleKey("folder:18", "parent", "folder:19"),
		tuple.NewTupleKey("folder:17", "parent", "folder:18"),
		tuple.NewTupleKey("folder:16", "parent", "folder:17"),
		tuple.NewTupleKey("folder:15", "parent", "folder:16"),
		tuple.NewTupleKey("folder:14", "parent", "folder:15"),
		tuple.NewTupleKey("folder:13", "parent", "folder:14"),
		tuple.NewTupleKey("folder:12", "parent", "folder:13"),
		tuple.NewTupleKey("folder:11", "parent", "folder:12"),
		tuple.NewTupleKey("folder:10", "parent", "folder:11"),
		tuple.NewTupleKey("folder:9", "parent", "folder:10"),
		tuple.NewTupleKey("folder:8", "parent", "folder:9"),
		tuple.NewTupleKey("folder:7", "parent", "folder:8"),
		tuple.NewTupleKey("folder:6", "parent", "folder:7"),
		tuple.NewTupleKey("folder:5", "parent", "folder:6"),
		tuple.NewTupleKey("folder:4", "parent", "folder:5"),
		tuple.NewTupleKey("folder:3", "parent", "folder:4"),
		tuple.NewTupleKey("folder:2", "parent", "folder:3"), // folder:2 will not exceed depth limit of 25
		tuple.NewTupleKey("folder:1", "parent", "folder:2"), // folder:1 will exceed depth limit of 25
	}

	tests := ListUsersTests{
		{
			name: "depth_should_exceed_limit",
			req: &openfgav1.ListUsersRequest{
				Object: &openfgav1.Object{
					Type: "folder",
					Id:   "1", // Exceeded because we expand up until folder:1, beyond 25 allowable levels
				},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model:            model,
			tuples:           tuples,
			expectedErrorMsg: graph.ErrResolutionDepthExceeded.Error(),
		},
		{
			name: "depth_should_not_exceed_limit",
			req: &openfgav1.ListUsersRequest{
				Object: &openfgav1.Object{
					Type: "folder",
					Id:   "2", // Does not exceed limit because we expand up until folder:2, up to the allowable 25 levels
				},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model:         model,
			tuples:        tuples,
			expectedUsers: []string{"user:maria"},
		},
	}

	tests.runListUsersTestCases(t)
}

func TestListUsersStorageErrors(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	testCases := map[string]struct {
		req *openfgav1.ListUsersRequest
	}{
		`union`: {
			req: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "document", Id: "1"},
				Relation:    "union",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
		},
		`exclusion`: {
			req: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "document", Id: "1"},
				Relation:    "exclusion",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
		},
		`intersection`: {
			req: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "document", Id: "1"},
				Relation:    "intersection",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			mockController := gomock.NewController(t)
			t.Cleanup(func() {
				mockController.Finish()
			})
			mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
			mockDatastore.EXPECT().
				Read(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
				Return(nil, fmt.Errorf("storage err")).
				MinTimes(1).
				MaxTimes(2) // Because DB errors will immediately halt the execution of the API function, it's possible that only one read is made

			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user

				type document
					relations
						define a: [user]
						define b: [user]
						define union: a or b
						define exclusion: a but not b
						define intersection: a and b`)
			typesys, err := typesystem.New(model)
			require.NoError(t, err)

			l := NewListUsersQuery(mockDatastore, emptyContextualTuples)

			ctx := typesystem.ContextWithTypesystem(context.Background(), typesys)
			resp, err := l.ListUsers(ctx, test.req)
			require.Nil(t, resp)
			require.ErrorContains(t, err, "storage err")
		})
	}
}

func (testCases ListUsersTests) runListUsersTestCases(t *testing.T) {
	storeID := ulid.Make().String()

	for _, test := range testCases {
		ds := memory.New()
		t.Cleanup(ds.Close)
		model := testutils.MustTransformDSLToProtoWithID(test.model)

		t.Run(test.name, func(t *testing.T) {
			typesys, err := typesystem.NewAndValidate(context.Background(), model)
			require.NoError(t, err)

			err = ds.WriteAuthorizationModel(context.Background(), storeID, model)
			require.NoError(t, err)

			if len(test.tuples) > 0 {
				err = ds.Write(context.Background(), storeID, nil, test.tuples)
				require.NoError(t, err)
			}

			contructor := test.newListUsersQuery
			if contructor == nil {
				contructor = NewListUsersQuery
			}

			l := contructor(ds, test.req.GetContextualTuples(), WithResolveNodeLimit(maximumRecursiveDepth))

			ctx := typesystem.ContextWithTypesystem(context.Background(), typesys)

			test.req.AuthorizationModelId = model.GetId()
			test.req.StoreId = storeID

			resp, err := l.ListUsers(ctx, test.req)

			actualErrorMsg := ""
			if err != nil {
				actualErrorMsg = err.Error()
			}
			require.Contains(t, actualErrorMsg, test.expectedErrorMsg)

			actualUsers := resp.GetUsers()
			actualCompare := make([]string, len(actualUsers))
			for i, u := range actualUsers {
				actualCompare[i] = tuple.UserProtoToString(u)
			}
			require.ElementsMatch(t, actualCompare, test.expectedUsers)
		})
	}
}

func TestListUsersReadFails_NoLeaks(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	store := ulid.Make().String()
	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1
		type user
		type group
			relations
				define member: [user]
		type document
			relations
				define viewer: [group#member]`)

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	gomock.InOrder(
		mockDatastore.EXPECT().Read(gomock.Any(), store, storage.ReadFilter{
			Relation: "viewer",
			Object:   "document:1",
		}, gomock.Any()).DoAndReturn(func(_ context.Context, _ string, _ storage.ReadFilter, _ storage.ReadOptions) (storage.TupleIterator, error) {
			return mocks.NewErrorTupleIterator([]*openfgav1.Tuple{
				{Key: tuple.NewTupleKey("document:1", "viewer", "group:fga#member")},
				{Key: tuple.NewTupleKey("document:1", "viewer", "group:eng#member")},
			}), nil
		}),
		mockDatastore.EXPECT().Read(gomock.Any(), store, gomock.Any(), gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ storage.ReadFilter, _ storage.ReadOptions) (storage.TupleIterator, error) {
				return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
			}),
	)

	typesys, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)
	ctx := typesystem.ContextWithTypesystem(context.Background(), typesys)
	resp, err := NewListUsersQuery(mockDatastore, emptyContextualTuples).ListUsers(ctx, &openfgav1.ListUsersRequest{
		StoreId:     store,
		Object:      &openfgav1.Object{Type: "document", Id: "1"},
		Relation:    "viewer",
		UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
	})

	require.ErrorContains(t, err, "simulated errors")
	require.Nil(t, resp)
}

func TestListUsersReadFails_NoLeaks_TTU(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	store := ulid.Make().String()
	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1
		type user
		type folder
			relations
				define viewer: [user]
		type document
			relations
				define parent: [folder]
				define viewer: viewer from parent`)

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	gomock.InOrder(
		mockDatastore.EXPECT().Read(gomock.Any(), store, storage.ReadFilter{
			Object:   "document:1",
			Relation: "parent",
		}, gomock.Any()).DoAndReturn(func(_ context.Context, _ string, _ storage.ReadFilter, _ storage.ReadOptions) (storage.TupleIterator, error) {
			return mocks.NewErrorTupleIterator([]*openfgav1.Tuple{
				{Key: tuple.NewTupleKey("document:1", "parent", "folder:1")},
				{Key: tuple.NewTupleKey("document:1", "parent", "folder:2")},
			}), nil
		}),
		mockDatastore.EXPECT().Read(gomock.Any(), store, storage.ReadFilter{
			Object:   "folder:1",
			Relation: "viewer",
		}, gomock.Any()).DoAndReturn(func(_ context.Context, _ string, _ storage.ReadFilter, _ storage.ReadOptions) (storage.TupleIterator, error) {
			return storage.NewStaticTupleIterator([]*openfgav1.Tuple{}), nil
		}),
	)

	typesys, err := typesystem.NewAndValidate(context.Background(), model)
	require.NoError(t, err)
	ctx := typesystem.ContextWithTypesystem(context.Background(), typesys)
	resp, err := NewListUsersQuery(mockDatastore, emptyContextualTuples).ListUsers(ctx, &openfgav1.ListUsersRequest{
		StoreId:     store,
		Object:      &openfgav1.Object{Type: "document", Id: "1"},
		Relation:    "viewer",
		UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
	})

	require.ErrorContains(t, err, "simulated errors")
	require.Nil(t, resp)
}

func TestListUsersDatastoreQueryCountAndDispatchCount(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	defer ds.Close()

	storeID := ulid.Make().String()

	err := ds.Write(context.Background(), storeID, nil, []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:x", "a", "user:jon"),
		tuple.NewTupleKey("document:x", "a", "user:maria"),
		tuple.NewTupleKey("document:x", "b", "user:maria"),
		tuple.NewTupleKey("document:x", "parent", "org:fga"),
		tuple.NewTupleKey("org:fga", "member", "user:maria"),
		tuple.NewTupleKey("company:fga", "member", "user:maria"),
		tuple.NewTupleKey("document:x", "userset", "org:fga#member"),
		tuple.NewTupleKey("document:x", "multiple_userset", "org:fga#member"),
		tuple.NewTupleKey("document:x", "multiple_userset", "company:fga#member"),
		tuple.NewTupleKey("document:public", "wildcard", "user:*"),
	})
	require.NoError(t, err)

	model := parser.MustTransformDSLToProto(`
		model
			schema 1.1
		type user

		type company
			relations
				define member: [user]

		type org
			relations
				define member: [user]

		type document
			relations
				define wildcard: [user:*]
				define userset: [org#member]
				define multiple_userset: [org#member, company#member]
				define a: [user]
				define b: [user]
				define union: a or b
				define union_rewrite: union
				define intersection: a and b
				define difference: a but not b
				define ttu: member from parent
				define union_and_ttu: union and ttu
				define union_or_ttu: union or ttu or union_rewrite
				define intersection_of_ttus: union_or_ttu and union_and_ttu
				define parent: [org]
		`)

	ts, err := typesystem.New(model)
	require.NoError(t, err)
	ctx := typesystem.ContextWithTypesystem(
		context.Background(),
		ts,
	)

	tests := []struct {
		name             string
		relation         string
		object           *openfgav1.Object
		userFilters      []*openfgav1.UserTypeFilter
		contextualTuples []*openfgav1.TupleKey
		dbReads          uint32
		dispatches       uint32
	}{
		{
			name:        "no_direct_access",
			relation:    "a",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     1,
			dispatches:  0,
		},
		{
			name:        "direct_access",
			relation:    "a",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     1,
			dispatches:  0,
		},
		{
			name:             "direct_access_thanks_to_contextual_tuple",
			relation:         "a",
			contextualTuples: []*openfgav1.TupleKey{tuple.NewTupleKey("document:x", "a", "user:unknown")},
			object:           &openfgav1.Object{Type: "document", Id: "1"},
			userFilters:      []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:          1,
			dispatches:       0,
		},
		{
			name:        "union",
			relation:    "union",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     2,
			dispatches:  2,
		},
		{
			name:        "union_no_access",
			relation:    "union",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     2,
			dispatches:  2,
		},
		{
			name:        "intersection",
			relation:    "intersection",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     2,
			dispatches:  2,
		},
		{
			name:        "intersection_no_access",
			relation:    "intersection",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     2,
			dispatches:  2,
		},
		{
			name:        "difference",
			relation:    "difference",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     2,
			dispatches:  2,
		},
		{
			name:        "difference_no_access",
			relation:    "difference",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     2,
			dispatches:  2,
		},
		{
			name:        "ttu",
			relation:    "ttu",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     1,
			dispatches:  0,
		},
		{
			name:        "ttu_no_access",
			relation:    "ttu",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     1,
			dispatches:  0,
		},
		{
			name:        "userset_no_access_1",
			relation:    "userset",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     1,
			dispatches:  0,
		},
		{
			name:        "userset_no_access_2",
			relation:    "userset",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     1,
			dispatches:  0,
		},
		{
			name:        "userset_access",
			relation:    "userset",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     1,
			dispatches:  0,
		},
		{
			name:        "multiple_userset_no_access",
			relation:    "multiple_userset",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     1,
			dispatches:  0,
		},
		{
			name:        "multiple_userset_access",
			relation:    "multiple_userset",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     1,
			dispatches:  0,
		},
		{
			name:        "wildcard_no_access",
			relation:    "wildcard",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     1,
			dispatches:  0,
		},
		{
			name:        "wildcard_access",
			relation:    "wildcard",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     1,
			dispatches:  0,
		},

		{
			name:        "union_and_ttu",
			relation:    "union_and_ttu",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     3,
			dispatches:  4,
		},
		{
			name:        "union_and_ttu_no_access",
			relation:    "union_and_ttu",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     3,
			dispatches:  4,
		},
		{
			name:        "union_or_ttu",
			relation:    "union_or_ttu",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     5,
			dispatches:  8,
		},
		{
			name:        "union_or_ttu_no_access",
			relation:    "union_or_ttu",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     5,
			dispatches:  8,
		},
		{
			name:        "intersection_of_ttus",
			relation:    "intersection_of_ttus",
			object:      &openfgav1.Object{Type: "document", Id: "1"},
			userFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			dbReads:     8,
			dispatches:  14,
		},
	}

	// run the test many times to exercise all the possible DBReads
	for i := 1; i < 100; i++ {
		for _, test := range tests {
			t.Run(fmt.Sprintf("%s_iteration_%v", test.name, i), func(t *testing.T) {
				l := NewListUsersQuery(ds, emptyContextualTuples)
				resp, err := l.ListUsers(ctx, &openfgav1.ListUsersRequest{
					Relation:         test.relation,
					Object:           test.object,
					UserFilters:      test.userFilters,
					ContextualTuples: test.contextualTuples,
				})
				require.NoError(t, err)
				require.Equal(t, test.dbReads, resp.GetMetadata().DatastoreQueryCount)
				require.Equal(t, test.dispatches, resp.GetMetadata().DispatchCounter.Load())
			})
		}
	}
}

func TestListUsersConfig_MaxResults(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ds := memory.New()
	t.Cleanup(ds.Close)

	testCases := map[string]struct {
		inputTuples           []*openfgav1.TupleKey
		inputModel            string
		inputRequest          *openfgav1.ListUsersRequest
		inputConfigMaxResults uint32
		allResults            []*openfgav1.User // all the results. the server may return less
		expectMinResults      uint32
	}{
		`max_results_infinite`: {
			inputModel: `
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: [user]`,
			inputTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:target", "admin", "user:1"),
				tuple.NewTupleKey("repo:target", "admin", "user:2"),
			},
			inputRequest: &openfgav1.ListUsersRequest{
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("repo:target", "admin", "user:3"),
				},
				Object:      &openfgav1.Object{Type: "repo", Id: "target"},
				Relation:    "admin",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			inputConfigMaxResults: 0,
			allResults: []*openfgav1.User{
				{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "1"}}},
				{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "2"}}},
				{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "3"}}},
			},
			expectMinResults: 3,
		},
		`max_results_less_than_actual_results`: {
			inputModel: `
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: [user]`,
			inputTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:target", "admin", "user:1"),
				tuple.NewTupleKey("repo:target", "admin", "user:2"),
			},
			inputRequest: &openfgav1.ListUsersRequest{
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("repo:target", "admin", "user:3"),
				},
				Object:      &openfgav1.Object{Type: "repo", Id: "target"},
				Relation:    "admin",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			inputConfigMaxResults: 2,
			allResults: []*openfgav1.User{
				{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "1"}}},
				{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "2"}}},
				{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "3"}}},
			},
			expectMinResults: 2,
		},
		`max_results_more_than_actual_results`: {
			inputModel: `
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: [user]`,
			inputTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:target", "admin", "user:1"),
			},
			inputRequest: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "repo", Id: "target"},
				Relation:    "admin",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			inputConfigMaxResults: 2,
			allResults: []*openfgav1.User{
				{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "1"}}},
			},
			expectMinResults: 1,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			// arrange: write model
			model := testutils.MustTransformDSLToProtoWithID(test.inputModel)

			storeID := ulid.Make().String()

			err := ds.WriteAuthorizationModel(ctx, storeID, model)
			require.NoError(t, err)

			// arrange: write tuples
			err = ds.Write(context.Background(), storeID, nil, test.inputTuples)
			require.NoError(t, err)

			typesys, err := typesystem.NewAndValidate(context.Background(), model)
			require.NoError(t, err)
			ctx = typesystem.ContextWithTypesystem(context.Background(), typesys)

			// assertions
			test.inputRequest.StoreId = storeID
			res, err := NewListUsersQuery(ds,
				test.inputRequest.GetContextualTuples(),
				WithListUsersMaxResults(test.inputConfigMaxResults),
				WithListUsersDeadline(10*time.Second),
			).ListUsers(ctx, test.inputRequest)

			require.NotNil(t, res)
			require.NoError(t, err)
			if test.inputConfigMaxResults != 0 { // don't get all results
				require.LessOrEqual(t, len(res.GetUsers()), int(test.inputConfigMaxResults))
			}
			require.GreaterOrEqual(t, len(res.GetUsers()), int(test.expectMinResults))
			require.Subset(t, test.allResults, res.GetUsers())
		})
	}
}

func TestListUsersConfig_Deadline(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ds := memory.New()
	t.Cleanup(ds.Close)

	testCases := map[string]struct {
		inputTuples         []*openfgav1.TupleKey
		inputModel          string
		inputRequest        *openfgav1.ListUsersRequest
		inputConfigDeadline time.Duration     // request can only take this time
		inputReadDelay      time.Duration     // to be able to hit the deadline at a predictable time
		allResults          []*openfgav1.User // all the results. the server may return less
		expectMinResults    uint32
		expectError         string
	}{
		`infinite_deadline_does_not_block_return_of_errors`: {
			inputModel: `
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: [user with condX]
				condition condX(x: int) {
					x > 0
				}`,
			inputTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition("repo:target", "admin", "user:1", "condX", nil),
			},
			inputRequest: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "repo", Id: "target"},
				Relation:    "admin",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			inputConfigDeadline: 0 * time.Millisecond, // infinite
			inputReadDelay:      50 * time.Millisecond,
			expectError:         "failed to evaluate relationship condition: 'condX' - tuple 'repo:target#admin@user:1' is missing context parameters '[x]",
		},
		`deadline_very_small_returns_nothing`: {
			inputModel: `
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: [user]`,
			inputTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:target", "admin", "user:1"),
			},
			inputRequest: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "repo", Id: "target"},
				Relation:    "admin",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			inputConfigDeadline: 1 * time.Millisecond,
			inputReadDelay:      50 * time.Millisecond,
			allResults: []*openfgav1.User{
				{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "1"}}},
			},
		},
		`deadline_very_high_returns_everything`: {
			inputModel: `
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: [user]`,
			inputTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:target", "admin", "user:1"),
			},
			inputRequest: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "repo", Id: "target"},
				Relation:    "admin",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			inputConfigDeadline: 1 * time.Second,
			inputReadDelay:      0 * time.Second,
			allResults: []*openfgav1.User{
				{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "1"}}},
			},
			expectMinResults: 1,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			// arrange: write model
			model := testutils.MustTransformDSLToProtoWithID(test.inputModel)

			storeID := ulid.Make().String()

			err := ds.WriteAuthorizationModel(ctx, storeID, model)
			require.NoError(t, err)

			// arrange: write tuples
			err = ds.Write(context.Background(), storeID, nil, test.inputTuples)
			require.NoError(t, err)

			typesys, err := typesystem.NewAndValidate(context.Background(), model)
			require.NoError(t, err)
			ctx = typesystem.ContextWithTypesystem(context.Background(), typesys)

			// assertions
			t.Run("regular_endpoint", func(t *testing.T) {
				test.inputRequest.StoreId = storeID
				res, err := NewListUsersQuery(
					mocks.NewMockSlowDataStorage(ds, test.inputReadDelay),
					emptyContextualTuples,
					WithListUsersDeadline(test.inputConfigDeadline),
				).ListUsers(ctx, test.inputRequest)

				if test.expectError != "" {
					require.ErrorContains(t, err, test.expectError)
				} else {
					require.NoError(t, err)
					require.GreaterOrEqual(t, len(res.GetUsers()), int(test.expectMinResults))
					require.Subset(t, test.allResults, res.GetUsers())
				}
			})
		})
	}
}

func TestListUsersConfig_MaxConcurrency(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ds := memory.New()
	t.Cleanup(ds.Close)

	testCases := map[string]struct {
		inputTuples                   []*openfgav1.TupleKey
		inputModel                    string
		inputRequest                  *openfgav1.ListUsersRequest
		inputConfigMaxConcurrentReads uint32
		inputReadDelay                time.Duration     // to be able to hit the deadline at a predictable time
		allResults                    []*openfgav1.User // all the results. the server may return less
		expectMinResults              uint32
		expectMinExecutionTime        time.Duration
	}{
		`max_concurrent_reads_does_not_delay_response_if_only_contextual_tuples_are_in_place`: {
			inputModel: `
				model
					schema 1.1
				type user
				type repo
					relations
						define admin: [user]`,
			inputTuples: []*openfgav1.TupleKey{},
			inputRequest: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "repo", Id: "target"},
				Relation:    "admin",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
				ContextualTuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("repo:target", "admin", "user:1"),
				},
			},
			inputConfigMaxConcurrentReads: 1,
			allResults: []*openfgav1.User{
				{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "1"}}},
			},
			expectMinResults:       1,
			expectMinExecutionTime: 0 * time.Millisecond,
		},
		`max_concurrent_reads_delays_response`: {
			inputModel: `
				model
					schema 1.1
				type user
				type folder
					relations
						define admin: [user]
				type repo
					relations
						define parent: [folder]
						define admin: [user] or admin from parent`, // two parallel reads will have to be made
			inputTuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:target", "admin", "user:1"),
			},
			inputRequest: &openfgav1.ListUsersRequest{
				Object:      &openfgav1.Object{Type: "repo", Id: "target"},
				Relation:    "admin",
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			inputReadDelay:                1 * time.Second,
			inputConfigMaxConcurrentReads: 1,
			allResults: []*openfgav1.User{
				{User: &openfgav1.User_Object{Object: &openfgav1.Object{Type: "user", Id: "1"}}},
			},
			expectMinExecutionTime: 2 * time.Second,
		},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			// arrange: write model
			model := testutils.MustTransformDSLToProtoWithID(test.inputModel)

			storeID := ulid.Make().String()

			err := ds.WriteAuthorizationModel(ctx, storeID, model)
			require.NoError(t, err)

			// arrange: write tuples
			err = ds.Write(context.Background(), storeID, nil, test.inputTuples)
			require.NoError(t, err)

			typesys, err := typesystem.NewAndValidate(context.Background(), model)
			require.NoError(t, err)
			ctx = typesystem.ContextWithTypesystem(context.Background(), typesys)

			// assertions
			t.Run("regular_endpoint", func(t *testing.T) {
				test.inputRequest.StoreId = storeID
				start := time.Now()
				res, err := NewListUsersQuery(
					mocks.NewMockSlowDataStorage(ds, test.inputReadDelay),
					test.inputRequest.GetContextualTuples(),
					WithListUsersMaxConcurrentReads(test.inputConfigMaxConcurrentReads),
				).ListUsers(ctx, test.inputRequest)

				require.NoError(t, err)
				require.GreaterOrEqual(t, len(res.GetUsers()), int(test.expectMinResults))
				require.Subset(t, test.allResults, res.GetUsers())
				require.GreaterOrEqual(t, time.Since(start), test.expectMinExecutionTime)
			})
		})
	}
}

func TestListUsers_ExpandExclusionHandler(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("avoid_producing_explicitly_negated_subjects", func(t *testing.T) {
		modelStr := `
			model
				schema 1.1

			type user

			type document
				relations
					define restricted: [user, user:*]
					define viewer: [user, user:*] but not restricted
		`
		storeID, model := storagetest.BootstrapFGAStore(t, ds, modelStr, []string{
			"document:1#viewer@user:*",
			"document:1#viewer@user:jon",
			"document:1#restricted@user:jon",
		})

		l := NewListUsersQuery(ds, emptyContextualTuples, WithResolveNodeLimit(maximumRecursiveDepth))
		channelDone := make(chan struct{})
		channelWithResults := make(chan foundUser)
		channelWithError := make(chan error, 1)

		typesys, err := typesystem.New(model)
		require.NoError(t, err)
		ctx := typesystem.ContextWithTypesystem(context.Background(), typesys)

		relation, err := typesys.GetRelation("document", "viewer")
		require.NoError(t, err)

		rewrite := relation.GetRewrite().GetUserset().(*openfgav1.Userset_Difference)

		go func() {
			resp := l.expandExclusion(ctx, &internalListUsersRequest{
				ListUsersRequest: &openfgav1.ListUsersRequest{
					StoreId:              storeID,
					AuthorizationModelId: model.GetId(),
					Object: &openfgav1.Object{
						Type: "document",
						Id:   "1",
					},
					Relation: "viewer",
					UserFilters: []*openfgav1.UserTypeFilter{{
						Type: "user",
					}},
				},
				visitedUsersetsMap: map[string]struct{}{},
			}, rewrite, channelWithResults)
			if resp.err != nil {
				channelWithError <- resp.err
				return
			}

			channelDone <- struct{}{}
		}()

		var actualResults []struct {
			user               string
			relationshipStatus userRelationshipStatus
		}

	OUTER:
		for {
			select {
			case <-channelWithError:
				require.FailNow(t, "expected 0 errors")
			case result := <-channelWithResults:
				actualResults = append(actualResults, struct {
					user               string
					relationshipStatus userRelationshipStatus
				}{user: tuple.UserProtoToString(result.user), relationshipStatus: result.relationshipStatus})
			case <-channelDone:
				break OUTER
			}
		}

		require.Len(t, actualResults, 3)
		require.ElementsMatch(t, []struct {
			user               string
			relationshipStatus userRelationshipStatus
		}{
			{
				user:               "user:*",
				relationshipStatus: HasRelationship,
			},
			{
				user:               "user:jon",
				relationshipStatus: NoRelationship,
			},
			{
				user:               "user:jon",
				relationshipStatus: NoRelationship,
			},
		}, actualResults)
	})
}

func TestListUsers_CorrectContext(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("typesystem_missing_returns_error", func(t *testing.T) {
		l := NewListUsersQuery(ds, emptyContextualTuples)
		_, err := l.ListUsers(context.Background(), &openfgav1.ListUsersRequest{})

		require.ErrorContains(t, err, "typesystem missing in context")
	})
}

func TestListUsersRespectsConsistency(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	modelStr := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]`

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	ctx := context.Background()

	// arrange: write model
	model := testutils.MustTransformDSLToProtoWithID(modelStr)

	typesys, err := typesystem.NewAndValidate(ctx, model)
	require.NoError(t, err)

	query := NewListUsersQuery(mockDatastore, emptyContextualTuples)
	ctx = typesystem.ContextWithTypesystem(ctx, typesys)

	t.Run("uses_passed_consistency_preference", func(t *testing.T) {
		higherConsistency := storage.ReadOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
		}
		mockDatastore.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), higherConsistency).Times(1)

		_, err = query.ListUsers(ctx, &openfgav1.ListUsersRequest{
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			Object:      &openfgav1.Object{Type: "document", Id: "1"},
			Relation:    "viewer",
			StoreId:     ulid.Make().String(),
		})
		require.NoError(t, err)
	})

	t.Run("unspecified_consistency_if_user_didnt_specify", func(t *testing.T) {
		unspecified := storage.ReadOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			},
		}
		mockDatastore.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any(), unspecified).Times(1)

		_, err = query.ListUsers(ctx, &openfgav1.ListUsersRequest{
			UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			Object:      &openfgav1.Object{Type: "document", Id: "1"},
			Relation:    "viewer",
			StoreId:     ulid.Make().String(),
		})
		require.NoError(t, err)
	})
}

func TestListUsersExclusionPanicExpandDirect(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := ListUsersTests{
		{
			name: "exclusion_with_chained_negation_panic_expand_direct",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "2"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type: "user",
					},
				},
			},
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define unblocked: [user]
						define blocked: [user, document#viewer] but not unblocked
						define viewer: [user, document#blocked] but not blocked
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "document:2#blocked"),
				tuple.NewTupleKey("document:2", "blocked", "document:1#viewer"),
				tuple.NewTupleKey("document:2", "viewer", "user:jon"),
				tuple.NewTupleKey("document:2", "unblocked", "user:jon"),
			},
			expectedUsers:     []string{},
			expectedErrorMsg:  ErrPanic.Error(),
			newListUsersQuery: NewListUsersQueryPanicExpandDirect,
		},
		{
			name: "non_stratifiable_exclusion_containing_cycle_1_panic_expand_direct",
			req: &openfgav1.ListUsersRequest{
				Object:   &openfgav1.Object{Type: "document", Id: "1"},
				Relation: "viewer",
				UserFilters: []*openfgav1.UserTypeFilter{
					{
						Type:     "document",
						Relation: "blocked",
					},
				},
			},
			model: `
				model
					schema 1.1

				type user

				type document
					relations
						define blocked: [user, document#viewer]
						define viewer: [user, document#blocked] but not blocked
			`,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:1", "viewer", "document:2#blocked"),
				tuple.NewTupleKey("document:2", "blocked", "document:1#viewer"),
			},
			expectedUsers:     []string{},
			expectedErrorMsg:  ErrPanic.Error(),
			newListUsersQuery: NewListUsersQueryPanicExpandDirect,
		},
	}
	tests.runListUsersTestCases(t)
}

func NewListUsersQueryPanicExpandDirect(ds storage.RelationshipTupleReader, contextualTuples []*openfgav1.TupleKey, opts ...ListUsersQueryOption) *listUsersQuery {
	l := &listUsersQuery{
		logger:                  logger.NewNoopLogger(),
		resolveNodeBreadthLimit: serverconfig.DefaultResolveNodeBreadthLimit,
		resolveNodeLimit:        serverconfig.DefaultResolveNodeLimit,
		deadline:                serverconfig.DefaultListUsersDeadline,
		maxResults:              serverconfig.DefaultListUsersMaxResults,
		maxConcurrentReads:      serverconfig.DefaultMaxConcurrentReadsForListUsers,
		wasDatastoreThrottled:   new(atomic.Bool),
		expandDirectDispatch: func(ctx context.Context, listUsersQuery *listUsersQuery, req *internalListUsersRequest, userObjectType, userObjectID, userRelation string, resp expandResponse, foundUsersChan chan<- foundUser, hasCycle *atomic.Bool) expandResponse {
			panic(ErrPanic)
		},
	}

	for _, opt := range opts {
		opt(l)
	}

	l.datastore = storagewrappers.NewRequestStorageWrapper(ds, contextualTuples, &storagewrappers.Operation{
		Method:      apimethod.ListUsers,
		Concurrency: l.maxConcurrentReads,
	})

	return l
}

func TestWithListUsersDatastoreThrottler(t *testing.T) {
	t.Run("option_sets_all_fields_correctly", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

		enabled := true
		threshold := 100
		duration := 50 * time.Millisecond

		query := NewListUsersQuery(
			mockDatastore,
			emptyContextualTuples,
			WithListUsersDatastoreThrottler(enabled, threshold, duration),
		)

		require.Equal(t, enabled, query.datastoreThrottlingEnabled)
		require.Equal(t, threshold, query.datastoreThrottleThreshold)
		require.Equal(t, duration, query.datastoreThrottleDuration)
	})

	t.Run("option_can_disable_throttling", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

		query := NewListUsersQuery(
			mockDatastore,
			emptyContextualTuples,
			WithListUsersDatastoreThrottler(false, 100, 50*time.Millisecond),
		)

		require.False(t, query.datastoreThrottlingEnabled)
	})

	t.Run("multiple_options_can_be_combined", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

		query := NewListUsersQuery(
			mockDatastore,
			emptyContextualTuples,
			WithListUsersDatastoreThrottler(true, 100, 50*time.Millisecond),
			WithListUsersMaxResults(10),
			WithListUsersDeadline(5*time.Second),
		)

		require.True(t, query.datastoreThrottlingEnabled)
		require.Equal(t, 100, query.datastoreThrottleThreshold)
		require.Equal(t, 50*time.Millisecond, query.datastoreThrottleDuration)
		require.Equal(t, uint32(10), query.maxResults)
		require.Equal(t, 5*time.Second, query.deadline)
	})
}

func TestListUsersDatastoreThrottler(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	ds := memory.New()
	t.Cleanup(ds.Close)

	storeID := ulid.Make().String()
	modelStr := `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]`

	model := testutils.MustTransformDSLToProtoWithID(modelStr)
	ctx := context.Background()

	err := ds.WriteAuthorizationModel(ctx, storeID, model)
	require.NoError(t, err)

	// Write multiple tuples to trigger throttling
	tuples := []*openfgav1.TupleKey{}
	for i := 1; i <= 10; i++ {
		tuples = append(tuples, tuple.NewTupleKey("document:1", "viewer", fmt.Sprintf("user:%d", i)))
	}
	err = ds.Write(ctx, storeID, nil, tuples)
	require.NoError(t, err)

	typesys, err := typesystem.NewAndValidate(ctx, model)
	require.NoError(t, err)
	ctx = typesystem.ContextWithTypesystem(ctx, typesys)

	t.Run("throttling_disabled_by_default", func(t *testing.T) {
		// Create query without throttling option
		query := NewListUsersQuery(ds, emptyContextualTuples)

		resp, err := query.ListUsers(ctx, &openfgav1.ListUsersRequest{
			StoreId:     storeID,
			Object:      &openfgav1.Object{Type: "document", Id: "1"},
			Relation:    "viewer",
			UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetMetadata().WasDatastoreThrottled.Load(), "Should not be throttled when throttling is disabled")
	})

	t.Run("throttling_enabled_with_high_threshold_does_not_throttle", func(t *testing.T) {
		// Enable throttling with threshold higher than expected reads
		query := NewListUsersQuery(
			ds,
			emptyContextualTuples,
			WithListUsersDatastoreThrottler(true, 1000, 10*time.Millisecond),
		)

		resp, err := query.ListUsers(ctx, &openfgav1.ListUsersRequest{
			StoreId:     storeID,
			Object:      &openfgav1.Object{Type: "document", Id: "1"},
			Relation:    "viewer",
			UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetMetadata().WasDatastoreThrottled.Load(), "Should not be throttled when threshold is high")
	})

	t.Run("throttling_enabled_with_low_threshold_triggers_throttle", func(t *testing.T) {
		// Create a model with union that causes multiple reads
		multiReadModelStr := `
			model
				schema 1.1
			type user
			type document
				relations
					define viewer: [user]
					define editor: [user]
					define can_access: viewer or editor`

		multiReadModel := testutils.MustTransformDSLToProtoWithID(multiReadModelStr)
		multiReadStoreID := ulid.Make().String()

		err := ds.WriteAuthorizationModel(ctx, multiReadStoreID, multiReadModel)
		require.NoError(t, err)

		// Write tuples
		multiReadTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("document:1", "viewer", "user:1"),
			tuple.NewTupleKey("document:1", "editor", "user:2"),
		}
		err = ds.Write(ctx, multiReadStoreID, nil, multiReadTuples)
		require.NoError(t, err)

		multiReadTypesys, err := typesystem.NewAndValidate(ctx, multiReadModel)
		require.NoError(t, err)
		multiReadCtx := typesystem.ContextWithTypesystem(ctx, multiReadTypesys)

		// Enable throttling with very low threshold (will throttle after 1 read)
		query := NewListUsersQuery(
			ds,
			emptyContextualTuples,
			WithListUsersDatastoreThrottler(true, 1, 10*time.Millisecond),
		)

		resp, err := query.ListUsers(multiReadCtx, &openfgav1.ListUsersRequest{
			StoreId:     multiReadStoreID,
			Object:      &openfgav1.Object{Type: "document", Id: "1"},
			Relation:    "can_access",
			UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.True(t, resp.GetMetadata().WasDatastoreThrottled.Load(), "Should be throttled when threshold is exceeded")
	})

	t.Run("throttling_disabled_explicitly_does_not_throttle", func(t *testing.T) {
		// Explicitly disable throttling even with low threshold
		query := NewListUsersQuery(
			ds,
			emptyContextualTuples,
			WithListUsersDatastoreThrottler(false, 1, 10*time.Millisecond),
		)

		resp, err := query.ListUsers(ctx, &openfgav1.ListUsersRequest{
			StoreId:     storeID,
			Object:      &openfgav1.Object{Type: "document", Id: "1"},
			Relation:    "viewer",
			UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetMetadata().WasDatastoreThrottled.Load(), "Should not be throttled when throttling is explicitly disabled")
	})

	t.Run("throttling_with_zero_threshold_does_not_throttle", func(t *testing.T) {
		// Enable throttling but with zero threshold (should be treated as disabled)
		query := NewListUsersQuery(
			ds,
			emptyContextualTuples,
			WithListUsersDatastoreThrottler(true, 0, 10*time.Millisecond),
		)

		resp, err := query.ListUsers(ctx, &openfgav1.ListUsersRequest{
			StoreId:     storeID,
			Object:      &openfgav1.Object{Type: "document", Id: "1"},
			Relation:    "viewer",
			UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		require.False(t, resp.GetMetadata().WasDatastoreThrottled.Load(), "Should not be throttled when threshold is zero")
	})
}
