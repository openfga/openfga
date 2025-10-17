package reverseexpand

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/stack"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReverseExpandWithWeightedGraph(t *testing.T) {
	ds := memory.New()
	t.Cleanup(ds.Close)
	tests := []struct {
		name                       string
		model                      string
		tuples                     []string
		objectType                 string
		relation                   string
		user                       *UserRefObject
		expectedOptimizedObjects   []string
		expectedUnoptimizedObjects []string
	}{
		{
			name: "direct_and_algebraic",
			model: `model
			  schema 1.1

			type user
			type repo
			  relations
				define member: [user]
				define computed_member: member
				define owner: [user]
				define admin: [user] or computed_member
				define or_admin: owner or admin
		`,
			tuples: []string{
				"repo:fga#member@user:justin",
				"repo:fga#owner@user:z",
			},
			objectType:                 "repo",
			relation:                   "or_admin",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedOptimizedObjects:   []string{"repo:fga"},
			expectedUnoptimizedObjects: []string{"repo:fga"},
		},
		{
			name: "simple_ttu",
			model: `model
				  schema 1.1

				type organization
				  relations
					define member: [user]
					define repo_admin: [organization#member]
				type repo
				  relations
					define admin: repo_admin from owner
					define owner: [organization]
				type user
		`,
			tuples: []string{
				"repo:fga#owner@organization:jz",
				"organization:jz#repo_admin@organization:j#member",
				"organization:j#member@user:justin",
			},
			objectType:                 "repo",
			relation:                   "admin",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedOptimizedObjects:   []string{"repo:fga"},
			expectedUnoptimizedObjects: []string{"repo:fga"},
		},
		{
			name: "ttu_from_union",
			model: `model
				  schema 1.1

				type organization
				  relations
					define member: [user]
					define repo_admin: [user, organization#member]
				type repo
				  relations
					define admin: [user, team#member] or repo_admin from owner
					define owner: [organization]
				type team
				  relations
					define member: [user, team#member]

				type user
		`,
			tuples: []string{
				"repo:fga#owner@organization:justin_and_zee",
				"organization:justin_and_zee#repo_admin@user:justin",
			},
			objectType:                 "repo",
			relation:                   "admin",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedOptimizedObjects:   []string{"repo:fga"},
			expectedUnoptimizedObjects: []string{"repo:fga"},
		},
		{
			name: "ttu_multiple_types_with_rewrites",
			model: `model
				  schema 1.1

				type organization
				  relations
					define member: [user]
					define repo_admin: [team#member] or member
				type repo
				  relations
					define admin: [team#member] or repo_admin from owner
					define owner: [organization]
				type team
				  relations
				    define member: [user]
				type user
		`,
			tuples: []string{
				"team:jz#member@user:justin",
				"organization:jz#repo_admin@team:jz#member",
				"repo:fga#owner@organization:jz",
			},
			objectType:                 "repo",
			relation:                   "admin",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedOptimizedObjects:   []string{"repo:fga"},
			expectedUnoptimizedObjects: []string{"repo:fga"},
		},
		{
			name: "ttu_recursive",
			model: `model
				  schema 1.1

				type user
				type org
				  relations
					define parent: [org]
					define ttu_recursive: [user] or ttu_recursive from parent
		`,
			tuples: []string{
				"org:a#ttu_recursive@user:justin",
				"org:b#parent@org:a", // org:a is parent of b
				"org:c#parent@org:b", // org:b is parent of org:c
				"org:d#parent@org:c", // org:c is parent of org:d
			},
			objectType:                 "org",
			relation:                   "ttu_recursive",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedOptimizedObjects:   []string{"org:a", "org:b", "org:c", "org:d"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:c", "org:d"},
		},
		{
			name: "ttu_with_cycle",
			model: `model
				  schema 1.1

				type user
				type org
				  relations
					define org_to_company: [company]
					define org_cycle: [user] or company_cycle from org_to_company
				type company
				  relations
					define company_to_org: [org]
					define company_cycle: [user] or org_cycle from company_to_org
		`,
			tuples: []string{
				"company:b#company_to_org@org:a",
				"org:a#org_to_company@company:b",
				"company:b#company_to_org@org:b",
				"org:b#org_to_company@company:c",
				"company:c#company_cycle@user:bob",
			},
			objectType:                 "org",
			relation:                   "org_cycle",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:b"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b"},
		},
		{
			name: "ttu_with_3_model_cycle",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define team_to_company: [company]
					define can_access: [user] or can_access from team_to_company
				type org
				  relations
					define org_to_team: [team]
					define can_access: [user] or can_access from org_to_team
				type company
				  relations
					define company_to_org: [org]
					define can_access: [user] or can_access from company_to_org
		`,
			tuples: []string{
				// Tuples to create a long cycle
				"company:a_corp#company_to_org@org:a_org",
				"org:a_org#org_to_team@team:a_team",
				"team:a_team#team_to_company@company:b_corp",
				"company:b_corp#company_to_org@org:b_org",
				"org:b_org#org_to_team@team:b_team",
				"team:b_team#team_to_company@company:a_corp",

				// Tuple to grant user:bob access into the cycle
				"company:a_corp#can_access@user:bob",
			},
			objectType:                 "org",
			relation:                   "can_access",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a_org", "org:b_org"},
			expectedUnoptimizedObjects: []string{"org:a_org", "org:b_org"},
		},
		{
			name: "intersection_with_multiple_directs",
			model: `model
				  schema 1.1
		
				type user
				type doc
				  relations
					define can_view: owner or viewer from parent
					define owner: [user]
					define parent: [project]
				type team
				  relations
					define member: [user]
				type project
				  relations
    				define viewer: [user, team#member] and contributor
					define contributor: [user]
		`,
			tuples: []string{
				"team:fga#member@user:justin",
				"project:fga#viewer@team:fga#member",
				"doc:one#parent@project:fga",
			},
			objectType:                 "doc",
			relation:                   "can_view",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedOptimizedObjects:   []string{},
			expectedUnoptimizedObjects: []string{"doc:one"},
		},
		{
			name: "simple_userset",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define teammate: [user, team#member]
		`,
			tuples: []string{
				"team:fga#member@user:justin",
				"org:j#teammate@team:fga#member",
				"org:z#teammate@user:justin",
			},
			objectType:                 "org",
			relation:                   "teammate",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedOptimizedObjects:   []string{"org:j", "org:z"},
			expectedUnoptimizedObjects: []string{"org:j", "org:z"},
		},
		{
			name: "userset_to_union",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: admin or boss
					define admin: [user]
					define boss: [user]
				type org
				  relations
					define teammate: [team#member]
		`,
			tuples: []string{
				"team:fga#admin@user:justin",
				"org:j#teammate@team:fga#member",
			},
			objectType:                 "org",
			relation:                   "teammate",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedOptimizedObjects:   []string{"org:j"},
			expectedUnoptimizedObjects: []string{"org:j"},
		},
		{
			name: "recursive_userset",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user, team#member]
		`,
			tuples: []string{
				"team:fga#member@user:justin",
				"team:cncf#member@team:fga#member",
				"team:lnf#member@team:cncf#member",
			},
			objectType:                 "team",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedOptimizedObjects:   []string{"team:fga", "team:cncf", "team:lnf"},
			expectedUnoptimizedObjects: []string{"team:fga", "team:cncf", "team:lnf"},
		},
		{
			name: "userset_ttu_mix",
			model: `model
				  schema 1.1
					type user
				  type group
					relations
					  define member: [user, user:*]
				  type folder
					relations
					  define viewer: [user,group#member]
				  type document
					relations
					  define parent: [folder]
					  define viewer: viewer from parent
		`,
			tuples: []string{
				"group:1#member@user:anne",
				"group:1#member@user:charlie",
				"group:2#member@user:anne",
				"group:2#member@user:bob",
				"group:3#member@user:elle",
				"group:public#member@user:*",
				"document:a#parent@folder:a",
				"document:public#parent@folder:public",
				"folder:a#viewer@group:1#member",
				"folder:a#viewer@group:2#member",
				"folder:a#viewer@user:daemon",
				"folder:public#viewer@group:public#member",
			},
			objectType:                 "document",
			relation:                   "viewer",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "anne"}},
			expectedOptimizedObjects:   []string{"document:a", "document:public"},
			expectedUnoptimizedObjects: []string{"document:a", "document:public"},
		},
		{
			name: "simple_union",
			model: `model
				  schema 1.1

				type user
				type org
				  relations
					define allowed: [user]
					define member: [user] or allowed
		`,
			tuples: []string{
				"org:a#allowed@user:bob",
				"org:b#allowed@user:bob", // org:a is parent of b
				"org:a#member@user:bob",  // org:b is parent of org:c
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:b"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b"},
		},
		{
			name: "nested_simple_union",
			model: `model
				  schema 1.1

				type user
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define member: [user] or (allowed or granted)
		`,
			tuples: []string{
				"org:a#allowed@user:bob",
				"org:b#allowed@user:bob",
				"org:a#member@user:bob",
				"org:c#granted@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:b", "org:c"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:c"},
		},
		{
			name: "simple_intersection",
			model: `model
				  schema 1.1

				type user
				type user2
				type org
				  relations
					define allowed: [user, user2]
					define member: [user] and allowed
		`,
			tuples: []string{
				"org:a#allowed@user:bob",
				"org:b#member@user:bob",
				"org:a#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b"},
		},
		{
			name: "simple_intersection_multiple_direct_assignments",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define member: [user, team#member] and allowed
		`,
			tuples: []string{
				"org:a#allowed@user:bob",
				"org:b#member@user:bob",
				"org:a#member@user:bob",
				"org:c#member@team:c#member",
				"team:c#member@user:bob",
				"org:c#allowed@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:c"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:c"},
		},
		{
			name: "intersection_direct_assign_not_lowest",
			model: `model
				  schema 1.1

				type user
				type user_group
				  relations
					define x: [user]
				type team
				  relations
					define manager: [user, user_group#x] and assigned
					define assigned: [user]
				type org
				  relations
					define teams: [team]
					define member: manager from teams
		`,
			tuples: []string{
				"team:a#assigned@user:bob",
				"team:a#manager@user:bob",
				"org:a#teams@team:a",
				"team:b#assigned@user:bob",
				"user_group:b#x@user:bob",
				"team:b#manager@user_group:b#x",
				"org:b#teams@team:b",
				"team:c#manager@user:bob",
				"org:c#teams@team:c",
				"team:d#assigned@user:bob",
				"org:d#teams@team:d",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:b"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:c"},
		},
		{
			name: "intersection_ttu_different_name",
			model: `model
				  schema 1.1

				type user
				type user_group
				  relations
					define x: [user]
				type team
				  relations
					define manager: assigned and x from mygroup
					define assigned: [user]
					define mygroup: [user_group]
				type org
				  relations
					define teams: [team]
					define member: manager from teams
		`,
			tuples: []string{
				"team:a#assigned@user:bob",
				"team:a#mygroup@user_group:a",
				"user_group:a#x@user:bob",
				"org:a#teams@team:a",
				"team:b#assigned@user:bob",
				"team:b#mygroup@user_group:b",
				"org:b#teams@team:b",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b"},
		},
		{
			name: "simple_intersection_multiple_direct_assignments_not_linked_1",
			model: `model
				  schema 1.1

				type user
				type user2
				type org
				  relations
					define allowed: [user]
					define member: [user, user2] and allowed
		`,
			tuples: []string{
				"org:a#allowed@user:bob",
				"org:b#member@user:bob",
				"org:a#member@user:bob",
				"org:c#allowed@user:bob",
				"org:d#member@user2:bob", // bob is user2 and there should be no link
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b"},
		},
		{
			name: "simple_intersection_multiple_direct_assignments_not_linked_2",
			model: `model
				  schema 1.1

				type user
				type user2
				type org
				  relations
					define allowed: [user]
					define member: [user, user2] and allowed
		`,
			tuples: []string{
				"org:a#allowed@user:bob",
				"org:b#member@user:bob",
				"org:a#member@user:bob",
				"org:c#allowed@user:bob",
				"org:d#member@user2:bob", // bob is user2 and there should be no link
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user2", Id: "bob"}},
			expectedOptimizedObjects:   []string{},
			expectedUnoptimizedObjects: []string{"org:d"},
		},
		{
			name: "simple_intersection_with_3_children",
			model: `model
				  schema 1.1

				type user
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define member: [user] and allowed and granted
		`,
			tuples: []string{
				"org:a#allowed@user:bob",
				"org:a#member@user:bob",
				"org:a#granted@user:bob",
				"org:b#member@user:bob",
				"org:c#allowed@user:bob",
				"org:c#granted@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b"},
		},
		{
			name: "simple_intersection_nested",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define member: [team#member] and (allowed and granted)
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"org:a#member@team:a#member",
				"org:a#allowed@user:bob",
				"org:a#granted@user:bob",
				// negative cases
				"org:b#member@team:a#member",
				"org:b#allowed@user:bob",
				"org:c#allowed@user:bob",
				"org:c#granted@user:bob",
				"team:b#member@user:bob",
				"org:d#member@team:b#member",
				"team:c#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:d"},
		},
		{
			name: "intersection_has_no_direct_assignment",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define member: [team#member]
					define can_access: member and (allowed and granted)
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"org:a#member@team:a#member",
				"org:a#allowed@user:bob",
				"org:a#granted@user:bob",
				// negative cases
				"org:b#member@team:a#member",
				"org:b#allowed@user:bob",
				"org:c#allowed@user:bob",
				"org:c#granted@user:bob",
				"team:b#member@user:bob",
				"org:d#member@team:b#member",
				"team:c#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "can_access",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:d"},
		},
		{
			name: "complex_intersection_nested",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define also_allowed: [user]
					define also_also_allowed: [user]
					define member: [team#member] and (((allowed or also_also_allowed) and also_allowed) and granted)
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"org:a#member@team:a#member",
				"org:b#member@team:a#member",
				"org:a#allowed@user:bob",
				"org:b#also_also_allowed@user:bob",
				"org:a#granted@user:bob",
				"org:b#granted@user:bob",
				"org:a#also_allowed@user:bob",
				"org:b#also_allowed@user:bob",
				"org:b#allowed@user:bob",
				// negative cases
				"org:c#allowed@user:bob",
				"org:c#granted@user:bob",
				"team:b#member@user:bob",
				"org:d#member@team:b#member",
				"team:c#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:b"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:d"},
		},
		{
			name: "complex_intersection_nested_and_union",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define also_allowed: [user]
					define member: [team#member] and ((allowed and also_allowed) or granted)
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"org:a#member@team:a#member",
				"org:b#member@team:a#member",
				"org:a#allowed@user:bob",
				"org:a#also_allowed@user:bob",
				"org:b#allowed@user:bob",
				"org:b#granted@user:bob",
				// negative cases
				"org:c#allowed@user:bob",
				"org:c#granted@user:bob",
				"team:b#member@user:bob",
				"org:d#member@team:b#member",
				"team:c#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:b"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:d"},
		},
		{
			name: "lowest_weight_is_TTU_intersection",
			model: `model
				  schema 1.1

				type user
				type dept
		          relations
		            define member: [user]
				type team
				  relations
					define member: [user]
					define dept_member: [dept#member]
				type org
				  relations
					define team: [team]
					define member: [team#dept_member] and member from team
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"org:a#team@team:a",
				"org:a#member@team:a#dept_member",
				"team:a#dept_member@dept:a#member",
				"dept:a#member@user:bob",
				// negative cases
				"team:b#member@user:bob",
				"org:b#team@team:b",
				"dept:b#member@user:bob",
				"org:c#member@team:c#dept_member",
				"team:c#dept_member@dept:c#member",
				"dept:c#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a"},
			expectedUnoptimizedObjects: []string{"org:a", "org:c"},
		},
		{
			name: "lowest_weight_is_w3_intersection",
			model: `model
				  schema 1.1

				type user
				type dept
		          relations
		            define member: [user]
				type team
				  relations
					define member: [dept#member]
					define dept_member: [dept#member]
				type org
				  relations
					define team: [team]
					define member: [team#dept_member] and member from team
		`,
			tuples: []string{
				"dept:a#member@user:bob",
				"team:a#member@dept:a#member",
				"team:a#dept_member@dept:a#member",
				"org:a#member@team:a#dept_member",
				"org:a#team@team:a",
				"org:x#member@team:a#dept_member",
				"org:x#team@team:a",
				// negative cases
				"dept:b#member@user:bob",
				"team:b#member@dept:b#member",
				"team:b#dept_member@dept:b#member",
				"org:b#team@team:b",
				"dept:c#member@user:bob",
				"team:c#member@dept:c#member",
				"team:c#dept_member@dept:c#member",
				"org:c#member@team:c#dept_member",
				"dept:d#member@user:bob",
				"team:d#dept_member@dept:d#member",
				"org:d#member@team:d#dept_member",
				"org:d#team@team:d",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:x"},
			expectedUnoptimizedObjects: []string{"org:a", "org:x", "org:c", "org:d"},
		},
		// TODO: use test when optimized supports infinite weight
		// {
		// 	name: "intersection_one_side_pointing_to_infinite_weight_ttu",
		// 	model: `model
		// 		    schema 1.1

		// 			type user
		// 			type team
		// 				relations
		// 					define parent: [team]
		// 					define member: [user] or member from parent
		// 			type org
		// 				relations
		// 					define allowed: [user]
		// 					define member: [team#member] and allowed
		// `,
		// 	tuples: []string{
		// 		"team:a#member@user:bob",
		// 		"team:b#parent@team:a",
		// 		"team:c#parent@team:b",
		// 		"org:a#allowed@user:bob",
		// 		"org:a#member@team:c#member",
		// 		// negative cases
		// 		"org:d#member@team:c#member", // allowed is false
		// 		"org:e#allowed@user:bob",     // no team member
		// 	},
		// 	objectType:      "org",
		// 	relation:        "member",
		// 	user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
		// 	expectedOptimizedObjects: []string{"org:a"},
		// 	expectedUnoptimizedObjects: []string{"org:a", "org:d"},
		// },
		{
			name: "intersection_both_side_infinite_weight_oneside_userset_other_ttu",
			model: `model
				    schema 1.1

					type user
					type team
						relations
							define parent: [team]
							define member: [user] or member from parent
					type org
						relations
							define team: [team]
							define allowed: member from team
							define member: [team#member] and allowed
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"team:b#parent@team:a",
				"team:c#parent@team:b",
				"org:a#team@team:c",
				"org:a#member@team:c#member",
				"org:a#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "allowed",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a"},
			expectedUnoptimizedObjects: []string{"org:a"},
		},
		{
			name: "intersection_both_side_infinite_weight_ttu",
			model: `model
				    schema 1.1

					type user
					type team
						relations
							define parent: [team]
							define member: [user] or member from parent
							define rewrite: member
					type org
						relations
							define team: [team]
							define allowed: member from team
							define team_rewrite: rewrite from team
							define member: team_rewrite and allowed
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"team:b#parent@team:a",
				"team:c#parent@team:b",
				"org:a#team@team:c",
			},
			objectType:                 "org",
			relation:                   "allowed",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a"},
			expectedUnoptimizedObjects: []string{"org:a"},
		},
		{
			name: "intersection_both_side_infinite_weight_ttu_rewrite",
			model: `model
				    schema 1.1

					type user
					type team
						relations
							define parent: [team]
							define member: [user] or member from parent
					type org
						relations
							define team: [team]
							define allowed: member from team
							define member: [team#member]
							define foo: member and allowed
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"team:b#parent@team:a",
				"team:c#parent@team:b",
				"org:a#team@team:c",
				"org:a#member@team:c#member",
			},
			objectType:                 "org",
			relation:                   "foo",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a"},
			expectedUnoptimizedObjects: []string{"org:a"},
		},
		{
			name: "lowest_weight_is_TTU_intersection_with_intersections",
			model: `model
				  schema 1.1

				type user
				type dept
		          relations
		            define member: [user]
				type team
				  relations
					define member: [user]
					define dept_member: [dept#member] and member
				type org
				  relations
					define team: [team]
					define member: [team#dept_member] and member from team
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"org:a#team@team:a",
				"org:a#member@team:a#dept_member",
				"team:a#dept_member@dept:a#member",
				"team:a#member@user:bob",
				"dept:a#member@user:bob",
				// negative cases
				"team:b#member@user:bob",
				"org:b#team@team:b",
				"dept:b#member@user:bob",
				"org:c#member@team:c#dept_member",
				"team:c#dept_member@dept:c#member",
				"dept:c#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a"},
			expectedUnoptimizedObjects: []string{"org:a", "org:c"},
		},
		{
			name: "ttus_with_multiple_parents",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
				type team
					relations
						define member: [user]
				type type1
					relations
						define member: [user]
				type type2
					relations
						define member: [type1#member]
				type doc
					relations
						define parent: [group, team]
						define owner: [type2#member] and member from parent	
		`,
			tuples: []string{
				"doc:1#parent@group:1",
				"group:1#member@user:1",
				"doc:1#parent@team:1",
				"team:1#member@user:2",
				"doc:1#owner@type2:1#member",
				"type2:1#member@type1:1#member",
				"type1:1#member@user:1",
				"type1:1#member@user:2",
			},
			objectType: "doc",
			relation:   "owner",
			user:       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "2"}},
			expectedOptimizedObjects: []string{
				"doc:1",
			},
			expectedUnoptimizedObjects: []string{
				"doc:1",
			},
		},
		{
			name: "intersection_other_edge_no_connection",
			model: `model
					  schema 1.1
					type user
					type user2
					type group
						relations
							define allowed: member and member2
							define member: [user, user2]
							define member2: [user2]
		`,
			tuples: []string{
				"group:a#member@user:bob",
				"group:a#member2@user2:bob",
			},
			objectType:                 "group",
			relation:                   "allowed",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{},
			expectedUnoptimizedObjects: []string{"group:a"},
		},
		{
			name: "direct_edge_no_connection",
			model: `model
					  schema 1.1
					type user
					type user2
					type group
						relations
							define allowed: [user2] and member
							define member: [user, user2]
		`,
			tuples: []string{
				"group:a#member@user:bob",
				"group:a#allowed@user2:bob",
			},
			objectType:                 "group",
			relation:                   "allowed",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{},
			expectedUnoptimizedObjects: []string{},
		},
		{
			name: "simple_exclusion",
			model: `model
				  schema 1.1

				type user
				type org
				  relations
					define banned: [user]
					define member: [user] but not banned
		`,
			tuples: []string{
				"org:a#banned@user:bob",
				"org:b#member@user:bob",
				"org:a#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:b"},
			expectedUnoptimizedObjects: []string{"org:b", "org:a"},
		},
		{
			name: "exclusion_on_itself",
			model: `model
					schema 1.1

				type user
				type org
				  relations
					define banned: [user]
					define member: banned but not banned
		`,
			tuples: []string{
				"org:a#banned@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{},
			expectedUnoptimizedObjects: []string{"org:a"},
		},
		{
			name: "exclusion_no_connection_base",
			model: `model
					schema 1.1

				type user
				type user2
				type org
				  relations
					define banned: [user2]
					define member: [user] but not banned
		`,
			tuples: []string{
				"org:a#banned@user:bob",
				"org:b#member@user:bob",
				"org:a#member@user:bob",
				"org:c#banned@user2:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user2", Id: "bob"}},
			expectedOptimizedObjects:   []string{},
			expectedUnoptimizedObjects: []string{},
		},
		{
			name: "exclusion_no_connection_exclusion_path",
			model: `model
					schema 1.1

				type user
				type user2
				type org
				  relations
					define banned: [user2]
					define member: [user] but not banned
		`,
			tuples: []string{
				"org:a#banned@user:bob",
				"org:b#member@user:bob",
				"org:a#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:b"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b"},
		},
		{
			name: "simple_exclusion_no_direct_assignment",
			model: `model
				  schema 1.1

				type user
				type org
				  relations
					define banned: [user]
					define member: [user]
					define viewer: member but not banned
		`,
			tuples: []string{
				"org:a#banned@user:bob",
				"org:b#member@user:bob",
				"org:a#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "viewer",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:b"},
			expectedUnoptimizedObjects: []string{"org:b", "org:a"},
		},
		{
			name: "simple_exclusion_multiple_direct_assignments",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define member: [user, team#member] but not allowed
		`,
			tuples: []string{
				"org:a#allowed@user:bob",
				"org:b#member@user:bob",
				"org:a#member@user:bob",
				"org:c#member@team:c#member",
				"team:c#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:b", "org:c"},
			expectedUnoptimizedObjects: []string{"org:a", "org:c", "org:b"},
		},
		{
			name: "simple_exclusion_multiple_direct_assignments_not_linked_1",
			model: `model
				  schema 1.1

				type user
				type user2
				type org
				  relations
					define allowed: [user]
					define member: [user, user2] but not allowed
		`,
			tuples: []string{
				"org:a#allowed@user:bob",
				"org:b#member@user:bob",
				"org:a#member@user:bob",
				"org:c#allowed@user:bob",
				"org:d#member@user2:bob", // bob is user2 and there should be no link
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:b"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b"},
		},
		{
			name: "simple_exclusion_multiple_direct_assignments_not_linked_2",
			model: `model
				  schema 1.1

				type user
				type user2
				type org
				  relations
					define allowed: [user]
					define member: [user, user2] but not allowed
		`,
			tuples: []string{
				"org:a#allowed@user:bob",
				"org:b#member@user:bob",
				"org:a#member@user:bob",
				"org:c#allowed@user:bob",
				"org:d#member@user2:bob", // even if right side not connected, it should still be good
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user2", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:d"},
			expectedUnoptimizedObjects: []string{"org:d"},
		},
		{
			name: "exclusion_direct_assign_not_lowest",
			model: `model
				  schema 1.1

				type user
				type user_group
				  relations
					define x: [user]
				type team
				  relations
					define manager: [user, user_group#x] but not banned
					define banned: [user]
				type org
				  relations
					define teams: [team]
					define member: manager from teams
		`,
			tuples: []string{
				"team:a#banned@user:bob",
				"team:a#manager@user:bob",
				"org:a#teams@team:a",
				"team:b#manager@user:bob",
				"org:b#teams@team:b",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:b"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b"},
		},
		{
			name: "exclusion_ttu",
			model: `model
				  schema 1.1

				type user
				type user_group
				  relations
					define x: [user]
				type team
				  relations
					define manager: assigned but not x from mygroup
					define assigned: [user]
					define mygroup: [user_group]
				type org
				  relations
					define teams: [team]
					define member: manager from teams
		`,
			tuples: []string{
				"team:a#assigned@user:bob",
				"team:a#mygroup@user_group:a",
				"user_group:a#x@user:bob",
				"org:a#teams@team:a",
				"team:b#assigned@user:bob",
				"user_group:b#x@user:bob",
				"org:b#teams@team:b",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:b"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b"},
		},
		{
			name: "simple_exclusion_with_double_negative",
			model: `model
				  schema 1.1

				type user
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define member: [user] but not (allowed but not granted)
		`,
			tuples: []string{
				"org:a#member@user:bob",
				"org:c#member@user:bob",
				"org:c#allowed@user:bob",
				"org:c#granted@user:bob",
				"org:d#member@user:bob",
				"org:d#granted@user:bob",
				// negative cases
				"org:b#member@user:bob",
				"org:b#allowed@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:c", "org:d"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:c", "org:d"},
		},
		{
			name: "exclusion_has_no_direct_assignment",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define member: [team#member]
					define can_access: member but not (allowed but not granted)
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"org:a#member@team:a#member",
				"org:a#member@user:bob",
				"team:c#member@user:bob",
				"org:c#member@team:c#member",
				"org:c#allowed@user:bob",
				"org:c#granted@user:bob",
				"team:d#member@user:bob",
				"org:d#member@team:d#member",
				"org:d#granted@user:bob",
				// negative cases
				"team:b#member@user:bob",
				"org:b#member@team:b#member",
				"org:b#allowed@user:bob",
			},
			objectType:                 "org",
			relation:                   "can_access",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:c", "org:d"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:c", "org:d"},
		},
		{
			name: "complex_exclusion_nested",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define also_allowed: [user]
					define also_also_allowed: [user]
					define member: [team#member] but not (((allowed or also_also_allowed) but not also_allowed) but not granted)
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"org:a#member@team:a#member",
				"team:c#member@user:bob",
				"org:c#member@team:c#member",
				"org:c#also_also_allowed@user:bob",
				"org:c#also_allowed@user:bob",
				"team:d#member@user:bob",
				"org:d#member@team:d#member",
				"org:d#also_also_allowed@user:bob",
				"org:d#granted@user:bob",
				// negative cases
				"team:b#member@user:bob",
				"org:b#member@team:b#member",
				"org:b#also_also_allowed@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:c", "org:d"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:c", "org:d"},
		},
		{
			name: "complex_exclusion_nested_and_union",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define granted: [user]
					define also_allowed: [user]
					define member: [team#member] but not ((allowed but not also_allowed) or granted)
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"org:a#member@team:a#member",
				"team:c#member@user:bob",
				"org:c#member@team:c#member",
				"org:c#allowed@user:bob",
				"org:c#also_allowed@user:bob",
				// negative cases
				"team:b#member@user:bob",
				"org:b#member@team:b#member",
				"org:b#allowed@user:bob",
				"team:d#member@user:bob",
				"org:d#member@team:d#member",
				"org:d#granted@user:bob",
				"org:e#granted@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:c"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:c", "org:d"},
		},
		{
			name: "exclusion_intersection_1",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define also_allowed: [user]
					define member: [team#member] but not (allowed and also_allowed)
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"org:a#member@team:a#member",
				"team:b#member@user:bob",
				"org:b#member@team:b#member",
				"org:b#allowed@user:bob",
				"team:c#member@user:bob",
				"org:c#member@team:c#member",
				"org:c#also_allowed@user:bob",
				// negative cases
				"team:d#member@user:bob",
				"org:d#member@team:d#member",
				"org:d#allowed@user:bob",
				"org:d#also_allowed@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:b", "org:c"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:c", "org:d"},
		},
		{
			name: "exclusion_intersection_2",
			model: `model
				  schema 1.1

				type user
				type team
				  relations
					define member: [user]
				type org
				  relations
					define allowed: [user]
					define also_allowed: [user]
					define member: [team#member] and (allowed but not also_allowed)
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"org:a#member@team:a#member",
				// negative cases
				"team:b#member@user:bob",
				"org:b#member@team:b#member",
				"org:b#allowed@user:bob",
				"team:c#member@user:bob",
				"org:c#member@team:c#member",
				"org:c#allowed@user:bob",
				"org:c#also_allowed@user:bob",
				"team:d#member@user:bob",
				"org:d#member@team:d#member",
				"org:d#also_allowed@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:b"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:c", "org:d"},
		},
		{
			name: "exclusion_lowest_weight_is_TTU",
			model: `model
				  schema 1.1

				type user
				type dept
		       relations
		         define member: [user]
				type team
				  relations
					define member: [user]
					define dept_member: [dept#member]
				type org
				  relations
					define team: [team]
					define member: [team#dept_member] but not member from team
		`,
			tuples: []string{
				"org:a#member@team:a#dept_member",
				"team:a#dept_member@dept:a#member",
				"dept:a#member@user:bob",
				"org:c#member@team:c#dept_member",
				"team:c#dept_member@dept:c#member",
				"dept:c#member@user:bob",
				"org:c#team@team:c",
				// negative cases
				"org:b#member@team:b#dept_member",
				"team:b#dept_member@dept:b#member",
				"dept:b#member@user:bob",
				"org:b#team@team:b",
				"team:b#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:c"},
			expectedUnoptimizedObjects: []string{"org:a", "org:c", "org:b"},
		},
		{
			name: "exclusion_ttu_multipleparents",
			model: `model
				  schema 1.1
			    type user
				type subteam
		          relations
		            define member: [user]
				type team
				  relations
					define member: [user]
					define dept_member: [user]
				type org
				  relations
					define parent: [team, subteam]
					define member: [user, team#dept_member] but not member from parent
		`,
			tuples: []string{
				"org:b#member@user:bob",
				"org:a#member@team:t1#dept_member",
				"team:t1#dept_member@user:bob",
				"org:c#member@team:t2#dept_member",
				"team:t2#dept_member@user:bob",
				"org:b#parent@team:t1",
				"org:b#parent@team:t2",
				"org:a#parent@subteam:st1",
				"org:a#parent@subteam:st2",
				"team:t1#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:c"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:c"},
		},
		{
			name: "lowest_weight_is_TTU_intersection_with_intersections",
			model: `model
				  schema 1.1

				type user
				type dept
		         relations
		           define member: [user]
				type team
				  relations
					define member: [user]
					define dept_member: [dept#member] and member
				type org
				  relations
					define team: [team]
					define member: [team#dept_member] and member from team
		`,
			tuples: []string{
				"team:a#member@user:bob",
				"org:a#team@team:a",
				"org:a#member@team:a#dept_member",
				"team:a#dept_member@dept:a#member",
				"team:a#member@user:bob",
				"dept:a#member@user:bob",
				// negative cases
				"team:b#member@user:bob",
				"org:b#team@team:b",
				"dept:b#member@user:bob",
				"org:c#member@team:c#dept_member",
				"team:c#dept_member@dept:c#member",
				"dept:c#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a"},
			expectedUnoptimizedObjects: []string{"org:a", "org:c"},
		},
		{
			name: "mix_of_union_intersection_and_exclusion",
			model: `model
				  schema 1.1

				type user
				type dept
		         relations
		           define member: [user]
				type team
				  relations
					define member: [user]
					define allowed: [user]
				type org
				  relations
					define team: [team]
					define dept: [dept]
					define member: [user] or ((member from team and allowed from team ) but not member from dept)
		`,
			tuples: []string{
				"org:a#member@user:bob",
				"org:b#team@team:b",
				"team:b#member@user:bob",
				"team:b#allowed@user:bob",
				// negative cases
				"org:c#team@team:c",
				"team:c#member@user:bob",
				"team:c#allowed@user:bob",
				"org:c#dept@dept:c",
				"dept:c#member@user:bob",
				"org:d#dept@dept:d",
				"dept:d#member@user:bob",
			},
			objectType:                 "org",
			relation:                   "member",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedOptimizedObjects:   []string{"org:a", "org:b"},
			expectedUnoptimizedObjects: []string{"org:a", "org:b", "org:c"},
		},
		{
			name: "intersection_with_TTU",
			model: `model
				schema 1.1
			  type user

			  type folder
				relations
				  define viewer: [user]

			  type document
				relations
				  define parent: [folder]
				  define writer: [user]
				  define viewer: writer and viewer from parent
		`,
			tuples: []string{
				"document:1#parent@folder:X",
				"folder:X#viewer@user:a",
				"document:1#writer@user:a",
				// negative cases
				"folder:X#viewer@user:b",
				"document:2#writer@user:c",
			},
			objectType:                 "document",
			relation:                   "viewer",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}},
			expectedOptimizedObjects:   []string{"document:1"},
			expectedUnoptimizedObjects: []string{"document:1"},
		},
		{
			name: "intersection_with_high_weights",
			model: `model
				schema 1.1
			  type user

			  type folder
				relations
				  define viewer: [user]

			  type document
				relations
				  define other_parent: [folder]
				  define parent: [folder]
				  define viewer: viewer from parent and viewer from other_parent
		`,
			tuples: []string{
				"document:1#parent@folder:X",
				"folder:X#viewer@user:a",
				"document:1#other_parent@folder:X",
				"document:3#parent@folder:A",
				"folder:A#viewer@user:a",
				"document:3#other_parent@folder:B",
				"folder:B#viewer@user:a",
				// negative cases
				"folder:X#viewer@user:b",
				"document:2#parent@folder:Y",
				"folder:Y#viewer@user:a",
				"document:2#other_parent@folder:Z",
			},
			objectType:                 "document",
			relation:                   "viewer",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}},
			expectedOptimizedObjects:   []string{"document:1", "document:3"},
			expectedUnoptimizedObjects: []string{"document:1", "document:2", "document:3"},
		},
		{
			name: "exclusion_with_TTU",
			model: `model
				schema 1.1
			  type user

			  type folder
				relations
				  define viewer: [user]

			  type document
				relations
				  define parent: [folder]
				  define writer: [user]
				  define viewer: writer but not viewer from parent
		`,
			tuples: []string{
				"document:2#writer@user:a",
				"document:3#writer@user:a",
				"document:3#parent@folder:Z",
				// negative cases
				"document:1#parent@folder:X",
				"folder:X#viewer@user:a",
				"document:1#writer@user:a",
				"folder:Y#viewer@user:a",
			},
			objectType:                 "document",
			relation:                   "viewer",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}},
			expectedOptimizedObjects:   []string{"document:2", "document:3"},
			expectedUnoptimizedObjects: []string{"document:1", "document:2", "document:3"},
		},
		{
			name: "exclusion_with_high_weights",
			model: `model
				schema 1.1
			  type user

			  type folder
				relations
				  define viewer: [user]

			  type document
				relations
				  define other_parent: [folder]
				  define parent: [folder]
				  define viewer: viewer from parent but not viewer from other_parent
		`,
			tuples: []string{
				"document:2#parent@folder:Y",
				"folder:Y#viewer@user:a",
				"document:4#parent@folder:D",
				"folder:D#viewer@user:a",
				"document:4#other_parent@folder:E",
				// negative cases
				"document:1#parent@folder:X",
				"folder:X#viewer@user:a",
				"document:1#other_parent@folder:X",
				"document:3#parent@folder:A",
				"folder:A#viewer@user:a",
				"document:3#other_parent@folder:B",
				"folder:B#viewer@user:a",
				"document:2#other_parent@folder:Z",
				"document:5#other_parent@folder:F",
				"folder:F#viewer@user:a",
			},
			objectType:                 "document",
			relation:                   "viewer",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}},
			expectedOptimizedObjects:   []string{"document:2", "document:4"},
			expectedUnoptimizedObjects: []string{"document:1", "document:2", "document:3", "document:4"},
		},
		{
			name: "tuple_to_userset_intersection",
			model: `model
				schema 1.1
			  type user

			  type and_folder
				relations
				  define writer: [user]
				  define editor: [user]
				  define viewer: writer and editor

			  type document
				relations
				  define and_parent: [and_folder]
				  define viewer: viewer from and_parent
		`,
			tuples: []string{
				"document:a#and_parent@and_folder:a",
				"and_folder:a#writer@user:a",
				"and_folder:a#editor@user:a",
				// negative cases
				"document:b#and_parent@and_folder:b",
				"and_folder:b#writer@user:b",
				"document:c#and_parent@and_folder:c",
				"and_folder:c#editor@user:c",
				"document:d#and_parent@and_folder:d",
				"and_folder:e#editor@user:e",
				"and_folder:e#editor@user:e",
			},
			objectType:                 "document",
			relation:                   "viewer",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}},
			expectedOptimizedObjects:   []string{"document:a"},
			expectedUnoptimizedObjects: []string{"document:a"},
		},
		{
			name: "tuple_to_userset_exclusion",
			model: `model
				schema 1.1
			  type user

			  type but_not_folder
				relations
				  define writer: [user]
				  define editor: [user]
				  define viewer: writer but not editor

			  type document
				relations
				  define but_not_parent: [but_not_folder]
				  define viewer: viewer from but_not_parent
		`,
			tuples: []string{
				"document:a#but_not_parent@but_not_folder:a",
				"but_not_folder:a#writer@user:a",
				// negative cases
				"document:b#but_not_parent@but_not_folder:b",
				"but_not_folder:b#writer@user:b",
				"but_not_folder:b#editor@user:b",
				"document:c#but_not_parent@but_not_folder:c",
				"but_not_folder:c#editor@user:c",
				"but_not_folder:d#writer@user:d",
			},
			objectType:                 "document",
			relation:                   "viewer",
			user:                       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}},
			expectedOptimizedObjects:   []string{"document:a"},
			expectedUnoptimizedObjects: []string{"document:a"},
		},
		{
			name: "duplicate_parent_ttu",
			model: `
				model
					schema 1.1
				type user
				type thing
					relations
						define account: [account]
						define parent: [account]
						define can_view: super_admin from account or super_admin from parent
				type account
					relations
						define super_admin: [user]
		`,
			tuples: []string{
				"thing:4#parent@account:4",
				"account:4#super_admin@user:1",
			},
			objectType: "thing",
			relation:   "can_view",
			user:       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "1"}},
			expectedOptimizedObjects: []string{
				"thing:4",
			},
			expectedUnoptimizedObjects: []string{
				"thing:4",
			},
		},
		{
			name: "multiple_ttus_going_to_same_terminal_typerel",
			model: `
				model
					schema 1.1
				type user
				type thing
					relations
						define resource: [resource]
						define can_view: can_view from parent or admin from resource
						define parent: [document]
				type document
					relations
						define resource: [resource]
						define can_view: admin from resource
				type resource
					relations
						define owner: [user] and also_user
						define admin: ([user] and also_user) or owner
						define also_user: [user]
		`,
			tuples: []string{
				"thing:1#resource@resource:1",
				"resource:1#also_user@user:1",
				"resource:1#owner@user:1",

				"thing:2#resource@resource:2",
				"resource:2#also_user@user:1",
				"resource:2#owner@user:1",

				"thing:3#resource@resource:3",
				"resource:3#also_user@user:1",
				"resource:3#owner@user:1",

				"thing:4#resource@resource:4",
				"resource:4#also_user@user:1",
				"resource:4#owner@user:1",

				"thing:5#resource@resource:5",
				"resource:5#also_user@user:1",
				"resource:5#owner@user:1",
			},
			objectType: "thing",
			relation:   "can_view",
			user:       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "1"}},
			expectedOptimizedObjects: []string{
				"thing:1",
				"thing:2",
				"thing:3",
				"thing:4",
				"thing:5",
			},
			expectedUnoptimizedObjects: []string{
				"thing:1",
				"thing:2",
				"thing:3",
				"thing:4",
				"thing:5",
			},
		},
		{
			name: "multiple_ttus_same_terminal_typerel_additional_paths",
			model: `
				model
					schema 1.1
				type user
				type thing
					relations
						define resource: [resource]
						define can_view: owner or super_admin from resource or can_view from parent
						define owner: [user] and also_user from resource
						define parent: [document]
				type document
					relations
						define _also_user: also_user from resource
						define resource: [resource]
						define can_view: viewer or editor or owner or super_admin from resource
						define editor: [user, team#member, resource#member, resource#admin] and _also_user
						define owner: [user] and _also_user
						define viewer: [user, team#member, resource#member, resource#admin] and _also_user
				type resource
					relations
						define admin: ([user] and also_user) or super_admin
						define member: ([user] and also_user) or admin
						define owner: [user] and also_user
						define super_admin: ([user] and also_user) or owner
						define also_user: [user]
				type team
					relations
						define member: [user]
		`,
			tuples: []string{
				// This satisfies can_view: 'owner'
				"thing:1#resource@resource:1",
				"resource:1#also_user@user:1",
				"thing:1#owner@user:1",

				// satisfies one of resource#super_admin edges for can_view: 'super_admin from resource'
				"thing:2#resource@resource:2",
				"resource:2#also_user@user:1",
				"resource:2#super_admin@user:1",

				// satisfies OR edge of resource#super_admin
				"thing:3#resource@resource:3",
				"resource:3#also_user@user:1",
				"resource:3#owner@user:1",

				// satisfies one of parent#can_view #viewer relation
				"thing:4#parent@document:1",
				"document:1#viewer@user:1",
				"document:1#resource@resource:4",
				"resource:4#also_user@user:1",

				// satisfies team#member of parent#can_view #viewer relation
				"thing:5#parent@document:2",
				"document:2#viewer@team:1#member",
				"team:1#member@user:1",
				"document:2#resource@resource:5",
				"resource:5#also_user@user:1",

				// satisfies resource#member of parent#can_view #viewer relation
				"thing:6#parent@document:3",
				"document:3#resource@resource:6",
				"resource:6#also_user@user:1",
				"resource:6#member@user:1",
				"document:3#viewer@resource:6#member",

				// satisfies resource#member of parent#can_view #viewer relation via resource#member
				// when the also_user is from a different resource
				"thing:7#parent@document:4",
				"document:4#resource@resource:7",
				"resource:7#also_user@user:1",
				"resource:7#member@user:1",
				"resource:8#also_user@user:1",
				"resource:8#member@user:1",
				"document:4#viewer@resource:8#member",
			},
			objectType: "thing",
			relation:   "can_view",
			user:       &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "1"}},
			expectedOptimizedObjects: []string{
				"thing:1",
				"thing:2",
				"thing:3",
				"thing:4",
				"thing:5",
				"thing:6",
				"thing:7",
			},
			expectedUnoptimizedObjects: []string{
				"thing:1",
				"thing:2",
				"thing:3",
				"thing:4",
				"thing:5",
				"thing:6",
				"thing:7",
			},
		},
		// TODO: add these when optimization supports infinite weight
		// intersection with ttu recursive
		// intersection with userset recursive
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer goleak.VerifyNone(t)
			storeID, model := storagetest.BootstrapFGAStore(t, ds, test.model, test.tuples)
			errChan := make(chan error, 1)
			typesys, err := typesystem.NewAndValidate(
				context.Background(),
				model,
			)
			require.NoError(t, err)
			ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
			ctx = typesystem.ContextWithTypesystem(ctx, typesys)

			// Once with optimization enabled
			optimizedResultsChan := make(chan *ReverseExpandResult)
			go func() {
				q := NewReverseExpandQuery(ds, typesys, WithListObjectOptimizationsEnabled(true))

				newErr := q.Execute(ctx, &ReverseExpandRequest{
					StoreID:    storeID,
					ObjectType: test.objectType,
					Relation:   test.relation,
					User:       test.user,
				}, optimizedResultsChan, NewResolutionMetadata())

				if newErr != nil {
					errChan <- newErr
				}
			}()

			// once without optimization enabled
			unoptimizedResultsChan := make(chan *ReverseExpandResult)
			go func() {
				q := NewReverseExpandQuery(ds, typesys)

				newErr := q.Execute(ctx, &ReverseExpandRequest{
					StoreID:    storeID,
					ObjectType: test.objectType,
					Relation:   test.relation,
					User:       test.user,
				}, unoptimizedResultsChan, NewResolutionMetadata())

				if newErr != nil {
					errChan <- newErr
				}
			}()

			var optimizedResults []string
			var unoptimizedResults []string
		ConsumerLoop:
			for {
				select {
				case result, open := <-unoptimizedResultsChan:
					if !open {
						unoptimizedResultsChan = nil
						break
					}
					unoptimizedResults = append(unoptimizedResults, result.Object)
				case result, open := <-optimizedResultsChan:
					if !open {
						optimizedResultsChan = nil
						break
					}
					optimizedResults = append(optimizedResults, result.Object)
				case err := <-errChan:
					require.FailNow(t, "unexpected error received on error channel:"+err.Error())
					break ConsumerLoop
				case <-ctx.Done():
					break ConsumerLoop
				}

				// When both channels have completed, break the loop
				if unoptimizedResultsChan == nil && optimizedResultsChan == nil {
					break ConsumerLoop
				}
			}
			require.ElementsMatch(t, test.expectedOptimizedObjects, optimizedResults)
			require.ElementsMatch(t, test.expectedUnoptimizedObjects, unoptimizedResults)
		})
	}
}

func TestLoopOverEdges(t *testing.T) {
	t.Run("returns_error_when_cannot_get_edges_from_intersection", func(t *testing.T) {
		brokenModel := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user2]
				  define editor: [user]
				  define admin: viewer and editor
		`
		workingModel := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user]
				  define editor: [user]
				  define admin: viewer and editor
		`
		tuples := []string{}
		objectType := "document"
		relation := "admin"
		user := &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}}

		ds := memory.New()
		t.Cleanup(ds.Close)
		storeID, authModel := storagetest.BootstrapFGAStore(t, ds, brokenModel, tuples)
		typesys, err := typesystem.New(
			authModel,
		)
		require.NoError(t, err)
		ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		q := NewReverseExpandQuery(ds, typesys)

		typesys2, err := typesystem.New(
			testutils.MustTransformDSLToProtoWithID(workingModel),
		)
		require.NoError(t, err)

		node, ok := typesys2.GetNode("document#admin")
		require.True(t, ok)

		edges, err := typesys2.GetEdgesFromNode(node, "user")
		require.NoError(t, err)

		newErr := q.loopOverEdges(ctx, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: nil,
			//OptimizationsEnabled: true, // turn on weighted graph
		}, edges, false, NewResolutionMetadata(), make(chan *ReverseExpandResult), "")

		require.Error(t, newErr)
		require.ErrorContains(t, newErr, "weighted graph is nil")
	})
	t.Run("returns_error_when_intersectionHandler_errors", func(t *testing.T) {
		model := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user]
				  define editor: [user]
				  define admin: viewer and editor
		`
		tuples := []string{
			"document:1#viewer@user:a",
			"document:2#editor@user:a",
		}
		objectType := "document"
		relation := "admin"
		user := &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}}

		ds := memory.New()
		t.Cleanup(ds.Close)
		storeID, authModel := storagetest.BootstrapFGAStore(t, ds, model, tuples)
		typesys, err := typesystem.New(
			authModel,
		)
		require.NoError(t, err)
		ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		q := NewReverseExpandQuery(ds, typesys)

		node, ok := typesys.GetNode("document#admin")
		require.True(t, ok)

		edges, err := typesys.GetEdgesFromNode(node, "user")
		require.NoError(t, err)

		newErr := q.loopOverEdges(ctx, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: nil,
			//OptimizationsEnabled: true, // turn on weighted graph
		}, edges, false, NewResolutionMetadata(), make(chan *ReverseExpandResult), "user")

		require.Error(t, newErr)
		require.ErrorContains(t, newErr, "unexpected empty stack")
	})
	t.Run("returns_error_when_cannot_get_edges_from_exclusion", func(t *testing.T) {
		brokenModel := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user2]
				  define editor: [user]
				  define admin: viewer but not editor
		`
		workingModel := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user]
				  define editor: [user]
				  define admin: viewer but not editor
		`
		tuples := []string{}
		objectType := "document"
		relation := "admin"
		user := &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}}

		ds := memory.New()
		t.Cleanup(ds.Close)
		storeID, authModel := storagetest.BootstrapFGAStore(t, ds, brokenModel, tuples)
		typesys, err := typesystem.New(
			authModel,
		)
		require.NoError(t, err)
		ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		q := NewReverseExpandQuery(ds, typesys)

		typesys2, err := typesystem.New(
			testutils.MustTransformDSLToProtoWithID(workingModel),
		)
		require.NoError(t, err)

		node, ok := typesys2.GetNode("document#admin")
		require.True(t, ok)

		edges, err := typesys2.GetEdgesFromNode(node, "user")
		require.NoError(t, err)

		newErr := q.loopOverEdges(ctx, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: nil,
			//OptimizationsEnabled: true, // turn on weighted graph
		}, edges, false, NewResolutionMetadata(), make(chan *ReverseExpandResult), "")

		require.Error(t, newErr)
		require.ErrorContains(t, newErr, "invalid number of edges in an exclusion operation: expected 2, got 0")
	})
	t.Run("returns_error_when_exclusionHandler_errors", func(t *testing.T) {
		model := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user]
				  define editor: [user]
				  define admin: viewer but not editor
		`
		tuples := []string{
			"document:1#viewer@user:a",
			"document:2#editor@user:a",
		}
		objectType := "document"
		relation := "admin"
		user := &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}}

		ds := memory.New()
		t.Cleanup(ds.Close)
		storeID, authModel := storagetest.BootstrapFGAStore(t, ds, model, tuples)
		typesys, err := typesystem.New(
			authModel,
		)
		require.NoError(t, err)
		ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		q := NewReverseExpandQuery(ds, typesys)

		node, ok := typesys.GetNode("document#admin")
		require.True(t, ok)

		edges, err := typesys.GetEdgesFromNode(node, "user")
		require.NoError(t, err)

		newErr := q.loopOverEdges(ctx, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: nil,
			//OptimizationsEnabled: true, // turn on weighted graph
		}, edges, false, NewResolutionMetadata(), make(chan *ReverseExpandResult), "user")

		require.Error(t, newErr)
		require.ErrorContains(t, newErr, "unexpected empty stack")
	})
}

func TestIntersectionHandler(t *testing.T) {
	t.Run("return_error_when_GetEdgesForIntersection_errors", func(t *testing.T) {
		brokenModel := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user2]
				  define editor: [user]
				  define admin: viewer and editor
		`
		workingModel := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user]
				  define editor: [user]
				  define admin: viewer and editor
		`
		tuples := []string{}
		objectType := "document"
		relation := "admin"
		user := &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}}

		ds := memory.New()
		t.Cleanup(ds.Close)
		storeID, authModel := storagetest.BootstrapFGAStore(t, ds, brokenModel, tuples)
		typesys, err := typesystem.New(
			authModel,
		)
		require.NoError(t, err)
		ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		q := NewReverseExpandQuery(ds, typesys)

		typesys2, err := typesystem.New(
			testutils.MustTransformDSLToProtoWithID(workingModel),
		)
		require.NoError(t, err)

		node, ok := typesys2.GetNode("document#admin")
		require.True(t, ok)

		pool := concurrency.NewPool(ctx, 2)
		err = q.intersectionHandler(pool, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: nil,
			//OptimizationsEnabled: true, // turn on weighted graph
		}, make(chan *ReverseExpandResult), node, "", NewResolutionMetadata())
		require.Error(t, err)
		require.ErrorContains(t, err, "invalid intersection node")
		err = pool.Wait()
		require.NoError(t, err)
	})

	t.Run("return_error_when_check_errors", func(t *testing.T) {
		model := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user]
				  define editor: [user]
				  define admin: viewer and editor
		`
		tuples := []string{
			"document:1#viewer@user:a",
			"document:2#editor@user:a",
		}
		objectType := "document"
		relation := "admin"
		user := &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}}

		ds := memory.New()
		t.Cleanup(ds.Close)
		storeID, authModel := storagetest.BootstrapFGAStore(t, ds, model, tuples)
		typesys, err := typesystem.New(
			authModel,
		)
		require.NoError(t, err)
		ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		errorRet := errors.New("test")

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockCheckResolver := graph.NewMockCheckRewriteResolver(ctrl)
		mockCheckResolver.EXPECT().CheckRewrite(gomock.Any(), gomock.Any(), gomock.Any()).Return(func(ctx context.Context) (*graph.ResolveCheckResponse, error) {
			return nil, errorRet
		})

		q := NewReverseExpandQuery(ds, typesys)
		q.localCheckResolver = mockCheckResolver

		node, ok := typesys.GetNode("document#admin")
		require.True(t, ok)

		edges, err := typesys.GetEdgesFromNode(node, "user")
		require.NoError(t, err)

		newStack := stack.Push(nil, typeRelEntry{typeRel: "document#admin"})

		pool := concurrency.NewPool(ctx, 2)
		err = q.intersectionHandler(pool, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: newStack,
			//OptimizationsEnabled: true, // turn on weighted graph
		}, make(chan *ReverseExpandResult), edges[0].GetTo(), "user", NewResolutionMetadata())
		require.NoError(t, err)
		err = pool.Wait()
		require.ErrorContains(t, err, "test")
		var execError *ExecutionError
		require.ErrorAs(t, err, &execError)
	})

	// This is to maintain the same functionality as old ListObjects, which returns partial results
	// in the case of a context cancellation or timeout.
	t.Run("return_nil_when_context_is_canceled", func(t *testing.T) {
		model := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user]
				  define editor: [user]
				  define admin: viewer and editor
		`
		tuples := []string{
			"document:1#viewer@user:a",
			"document:2#editor@user:a",
		}
		objectType := "document"
		relation := "admin"
		user := &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}}

		ds := memory.New()
		t.Cleanup(ds.Close)
		storeID, authModel := storagetest.BootstrapFGAStore(t, ds, model, tuples)
		typesys, err := typesystem.New(
			authModel,
		)
		require.NoError(t, err)
		ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockCheckResolver := graph.NewMockCheckRewriteResolver(ctrl)
		mockCheckResolver.EXPECT().CheckRewrite(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes().
			DoAndReturn(func(ctx context.Context, req *graph.ResolveCheckRequest, rewrite *openfgav1.Userset) graph.CheckHandlerFunc {
				_, cancel := context.WithCancel(ctx)
				defer cancel()
				return func(ctx context.Context) (*graph.ResolveCheckResponse, error) {
					return nil, nil
				}
			})
		q := NewReverseExpandQuery(ds, typesys)
		q.localCheckResolver = mockCheckResolver

		node, ok := typesys.GetNode("document#admin")
		require.True(t, ok)

		edges, err := typesys.GetEdgesFromNode(node, "user")
		require.NoError(t, err)

		newStack := stack.Push(nil, typeRelEntry{typeRel: "document#admin"})

		pool := concurrency.NewPool(ctx, 2)
		err = q.intersectionHandler(pool, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: newStack,
			//OptimizationsEnabled: true, // turn on weighted graph
		}, make(chan *ReverseExpandResult), edges[0].GetTo(), "user", NewResolutionMetadata())
		require.NoError(t, err)
		err = pool.Wait()
		require.NoError(t, err)
	})

	t.Run("return_error_when_queryForTuples_errors", func(t *testing.T) {
		model := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user]
				  define editor: [user]
				  define admin: viewer and editor
		`
		tuples := []string{
			"document:1#viewer@user:a",
			"document:2#editor@user:a",
		}
		objectType := "document"
		relation := "admin"
		user := &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}}
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		errorRet := errors.New("test")
		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any())
		mockDatastore.EXPECT().MaxTuplesPerWrite().Return(40)
		mockDatastore.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errorRet)
		storeID, authModel := storagetest.BootstrapFGAStore(t, mockDatastore, model, tuples)
		typesys, err := typesystem.New(
			authModel,
		)
		require.NoError(t, err)
		ctx := storage.ContextWithRelationshipTupleReader(context.Background(), mockDatastore)
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		mockCheckResolver := graph.NewMockCheckRewriteResolver(ctrl)
		mockCheckResolver.EXPECT().CheckRewrite(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
		q := NewReverseExpandQuery(mockDatastore, typesys)
		q.localCheckResolver = mockCheckResolver

		node, ok := typesys.GetNode("document#admin")
		require.True(t, ok)

		edges, err := typesys.GetEdgesFromNode(node, "user")
		require.NoError(t, err)

		newStack := stack.Push(nil, typeRelEntry{typeRel: "document#admin"})

		pool := concurrency.NewPool(ctx, 2)
		err = q.intersectionHandler(pool, &ReverseExpandRequest{
			StoreID:    storeID,
			ObjectType: objectType,
			Relation:   relation,
			User:       user,
			//OptimizationsEnabled: true, // turn on weighted graph
			relationStack: newStack,
		}, make(chan *ReverseExpandResult), edges[0].GetTo(), "user", NewResolutionMetadata())
		require.NoError(t, err)
		err = pool.Wait()
		require.ErrorIs(t, err, errorRet)
	})
}

func TestExclusionHandler(t *testing.T) {
	t.Run("return_error_when_GetEdgesForExclusion_errors", func(t *testing.T) {
		brokenModel := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user2]
				  define editor: [user]
				  define admin: viewer and editor
		`
		workingModel := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user]
				  define editor: [user]
				  define admin: viewer and editor
		`
		tuples := []string{}
		objectType := "document"
		relation := "admin"
		user := &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}}

		ds := memory.New()
		t.Cleanup(ds.Close)
		storeID, authModel := storagetest.BootstrapFGAStore(t, ds, brokenModel, tuples)
		typesys, err := typesystem.New(
			authModel,
		)
		require.NoError(t, err)
		ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		q := NewReverseExpandQuery(ds, typesys)

		typesys2, err := typesystem.New(
			testutils.MustTransformDSLToProtoWithID(workingModel),
		)
		require.NoError(t, err)

		node, ok := typesys2.GetNode("document#admin")
		require.True(t, ok)

		pool := concurrency.NewPool(ctx, 2)
		err = q.exclusionHandler(ctx, pool, &ReverseExpandRequest{
			StoreID:    storeID,
			ObjectType: objectType,
			Relation:   relation,
			User:       user,
			//OptimizationsEnabled: true, // turn on weighted graph
			relationStack: nil,
		}, make(chan *ReverseExpandResult), node, "", NewResolutionMetadata())
		require.Error(t, err)
		require.ErrorContains(t, err, "invalid exclusion node")
		err = pool.Wait()
		require.NoError(t, err)
	})

	t.Run("return_error_when_check_errors", func(t *testing.T) {
		model := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user]
				  define editor: [user]
				  define admin: viewer but not editor
		`
		tuples := []string{
			"document:1#viewer@user:a",
			"document:2#editor@user:a",
		}
		objectType := "document"
		relation := "admin"
		user := &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}}

		ds := memory.New()
		t.Cleanup(ds.Close)
		storeID, authModel := storagetest.BootstrapFGAStore(t, ds, model, tuples)
		typesys, err := typesystem.New(
			authModel,
		)
		require.NoError(t, err)
		ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		errorRet := errors.New("test")

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockCheckResolver := graph.NewMockCheckRewriteResolver(ctrl)
		mockCheckResolver.EXPECT().CheckRewrite(gomock.Any(), gomock.Any(), gomock.Any()).Return(func(ctx context.Context) (*graph.ResolveCheckResponse, error) {
			return nil, errorRet
		})
		q := NewReverseExpandQuery(ds, typesys)
		q.localCheckResolver = mockCheckResolver

		node, ok := typesys.GetNode("document#admin")
		require.True(t, ok)

		edges, err := typesys.GetEdgesFromNode(node, "user")
		require.NoError(t, err)

		newStack := stack.Push(nil, typeRelEntry{typeRel: "document#admin"})

		pool := concurrency.NewPool(ctx, 2)
		err = q.exclusionHandler(ctx, pool, &ReverseExpandRequest{
			StoreID:    storeID,
			ObjectType: objectType,
			Relation:   relation,
			User:       user,
			//OptimizationsEnabled: true, // turn on weighted graph
			relationStack: newStack,
		}, make(chan *ReverseExpandResult), edges[0].GetTo(), "user", NewResolutionMetadata())
		require.NoError(t, err)
		err = pool.Wait()
		require.ErrorContains(t, err, "test")
		var execError *ExecutionError
		require.ErrorAs(t, err, &execError)
	})

	t.Run("return_error_when_queryForTuples_errors", func(t *testing.T) {
		model := `
			model
				schema 1.1
			  type user

			  type document
				relations
				  define viewer: [user]
				  define editor: [user]
				  define admin: viewer but not editor
		`
		tuples := []string{
			"document:1#viewer@user:a",
			"document:2#editor@user:a",
		}
		objectType := "document"
		relation := "admin"
		user := &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "a"}}
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		errorRet := errors.New("test")
		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().WriteAuthorizationModel(gomock.Any(), gomock.Any(), gomock.Any())
		mockDatastore.EXPECT().MaxTuplesPerWrite().Return(40)
		mockDatastore.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errorRet)
		storeID, authModel := storagetest.BootstrapFGAStore(t, mockDatastore, model, tuples)
		typesys, err := typesystem.New(
			authModel,
		)
		require.NoError(t, err)
		ctx := storage.ContextWithRelationshipTupleReader(context.Background(), mockDatastore)
		ctx = typesystem.ContextWithTypesystem(ctx, typesys)

		mockCheckResolver := graph.NewMockCheckRewriteResolver(ctrl)
		mockCheckResolver.EXPECT().CheckRewrite(gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()
		q := NewReverseExpandQuery(mockDatastore, typesys)
		q.localCheckResolver = mockCheckResolver

		node, ok := typesys.GetNode("document#admin")
		require.True(t, ok)

		edges, err := typesys.GetEdgesFromNode(node, "user")
		require.NoError(t, err)

		newStack := stack.Push(nil, typeRelEntry{typeRel: "document#admin"})

		pool := concurrency.NewPool(ctx, 2)
		err = q.exclusionHandler(ctx, pool, &ReverseExpandRequest{
			StoreID:    storeID,
			ObjectType: objectType,
			Relation:   relation,
			User:       user,
			//OptimizationsEnabled: true, // turn on weighted graph
			relationStack: newStack,
		}, make(chan *ReverseExpandResult), edges[0].GetTo(), "user", NewResolutionMetadata())
		require.NoError(t, err)
		err = pool.Wait()
		require.ErrorIs(t, err, errorRet)
	})
}
