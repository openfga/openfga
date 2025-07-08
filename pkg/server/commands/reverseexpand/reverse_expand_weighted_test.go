package reverseexpand

import (
	"context"
	"errors"
	"testing"
	"time"

	lls "github.com/emirpasic/gods/stacks/linkedliststack"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/internal/mocks"
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
				q := NewReverseExpandQuery(
					ds,
					typesys,

					// turn on weighted graph functionality
					WithListObjectOptimizationsEnabled(true),
				)

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

		q := NewReverseExpandQuery(
			ds,
			typesys,

			// turn on weighted graph functionality
			WithListObjectOptimizationsEnabled(true),
		)

		typesys2, err := typesystem.New(
			testutils.MustTransformDSLToProtoWithID(workingModel),
		)
		require.NoError(t, err)

		edges, _, err := typesys2.GetEdgesFromWeightedGraph("document#admin", "user")
		require.NoError(t, err)

		newErr := q.loopOverEdges(ctx, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: *lls.New(),
		}, edges, false, NewResolutionMetadata(), make(chan *ReverseExpandResult), "")

		require.Error(t, newErr)
		require.ErrorContains(t, newErr, "weighted graph is nil")
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

		q := NewReverseExpandQuery(
			ds,
			typesys,

			// turn on weighted graph functionality
			WithListObjectOptimizationsEnabled(true),
		)

		typesys2, err := typesystem.New(
			testutils.MustTransformDSLToProtoWithID(workingModel),
		)
		require.NoError(t, err)

		edges, _, err := typesys2.GetEdgesFromWeightedGraph("document#admin", "user")
		require.NoError(t, err)

		newErr := q.loopOverEdges(ctx, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: *lls.New(),
		}, edges, false, NewResolutionMetadata(), make(chan *ReverseExpandResult), "")

		require.Error(t, newErr)
		require.ErrorContains(t, newErr, "could not find node with label")
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

		q := NewReverseExpandQuery(
			ds,
			typesys,

			// turn on weighted graph functionality
			WithListObjectOptimizationsEnabled(true),
		)

		typesys2, err := typesystem.New(
			testutils.MustTransformDSLToProtoWithID(workingModel),
		)
		require.NoError(t, err)

		edges, _, err := typesys2.GetEdgesFromWeightedGraph("document#admin", "user")
		require.NoError(t, err)

		newErr := q.intersectionHandler(ctx, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: *lls.New(),
		}, make(chan *ReverseExpandResult), edges, "", NewResolutionMetadata())
		require.Error(t, newErr)
		require.ErrorContains(t, newErr, "invalid edges for source type")
	})

	t.Run("return_nil_when_there_are_no_connections_for_the_path", func(t *testing.T) {
		model := `
			model
				schema 1.1
			type user
			type user2
			type subteam
				relations
					define member: [user]
			type adhoc
				relations
					define member: [user]
			type team
				relations
					define member: [subteam#member]
			type group
				relations
					define team: [team]
					define subteam: [subteam]
					define adhoc_member: [adhoc#member]
					define member: [user2] and member from team and adhoc_member and member from subteam
		`
		tuples := []string{}
		objectType := "group"
		relation := "member"
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

		resultChan := make(chan *ReverseExpandResult)
		errChan := make(chan error, 1)
		q := NewReverseExpandQuery(
			ds,
			typesys,

			// turn on weighted graph functionality
			WithListObjectOptimizationsEnabled(true),
		)

		edges, _, err := typesys.GetEdgesFromWeightedGraph("group#member", "user")
		require.NoError(t, err)
		edges, _, err = typesys.GetEdgesFromWeightedGraph(edges[0].GetTo().GetUniqueLabel(), "user")
		require.NoError(t, err)

		go func() {
			newErr := q.intersectionHandler(ctx, &ReverseExpandRequest{
				StoreID:       storeID,
				ObjectType:    objectType,
				Relation:      relation,
				User:          user,
				relationStack: *lls.New(),
			}, resultChan, edges, "", NewResolutionMetadata())

			if newErr != nil {
				errChan <- newErr
			}
		}()

		select {
		case res := <-resultChan:
			require.Fail(t, "expected no result, but got one", "received: %+v", res)
		case <-time.After(300 * time.Millisecond):
			// Success: no result received within timeout
		case err := <-errChan:
			require.Fail(t, "unexpected error received on error channel: "+err.Error())
		}
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
		q := NewReverseExpandQuery(
			ds,
			typesys,

			// turn on weighted graph functionality
			WithListObjectOptimizationsEnabled(true),
		)
		q.localCheckResolver = mockCheckResolver

		edges, _, err := typesys.GetEdgesFromWeightedGraph("document#admin", "user")
		require.NoError(t, err)
		edges, _, err = typesys.GetEdgesFromWeightedGraph(edges[0].GetTo().GetUniqueLabel(), "user")
		require.NoError(t, err)

		stack := lls.New()
		stack.Push("document#admin")

		newErr := q.intersectionHandler(ctx, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: *stack,
		}, make(chan *ReverseExpandResult), edges, "user", NewResolutionMetadata())
		require.ErrorIs(t, newErr, errorRet)
	})

	t.Run("return_error_when_queryForTuplesWithItems_errors", func(t *testing.T) {
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
		q := NewReverseExpandQuery(
			mockDatastore,
			typesys,

			// turn on weighted graph functionality
			WithListObjectOptimizationsEnabled(true),
		)
		q.localCheckResolver = mockCheckResolver

		edges, _, err := typesys.GetEdgesFromWeightedGraph("document#admin", "user")
		require.NoError(t, err)
		edges, _, err = typesys.GetEdgesFromWeightedGraph(edges[0].GetTo().GetUniqueLabel(), "user")
		require.NoError(t, err)

		stack := lls.New()
		stack.Push("document#admin")

		newErr := q.intersectionHandler(ctx, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: *stack,
		}, make(chan *ReverseExpandResult), edges, "user", NewResolutionMetadata())
		require.ErrorIs(t, newErr, errorRet)
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

		q := NewReverseExpandQuery(
			ds,
			typesys,

			// turn on weighted graph functionality
			WithListObjectOptimizationsEnabled(true),
		)

		typesys2, err := typesystem.New(
			testutils.MustTransformDSLToProtoWithID(workingModel),
		)
		require.NoError(t, err)

		edges, _, err := typesys2.GetEdgesFromWeightedGraph("document#admin", "user")
		require.NoError(t, err)

		newErr := q.exclusionHandler(ctx, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: *lls.New(),
		}, make(chan *ReverseExpandResult), edges, "", NewResolutionMetadata())
		require.Error(t, newErr)
		require.ErrorContains(t, newErr, "invalid exclusion edges for source type")
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
		q := NewReverseExpandQuery(
			ds,
			typesys,

			// turn on weighted graph functionality
			WithListObjectOptimizationsEnabled(true),
		)
		q.localCheckResolver = mockCheckResolver

		edges, _, err := typesys.GetEdgesFromWeightedGraph("document#admin", "user")
		require.NoError(t, err)
		edges, _, err = typesys.GetEdgesFromWeightedGraph(edges[0].GetTo().GetUniqueLabel(), "user")
		require.NoError(t, err)

		stack := lls.New()
		stack.Push("document#admin")

		newErr := q.exclusionHandler(ctx, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: *stack,
		}, make(chan *ReverseExpandResult), edges, "user", NewResolutionMetadata())
		require.ErrorIs(t, newErr, errorRet)
	})

	t.Run("return_error_when_queryForTuplesWithItems_errors", func(t *testing.T) {
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
		q := NewReverseExpandQuery(
			mockDatastore,
			typesys,

			// turn on weighted graph functionality
			WithListObjectOptimizationsEnabled(true),
		)
		q.localCheckResolver = mockCheckResolver

		edges, _, err := typesys.GetEdgesFromWeightedGraph("document#admin", "user")
		require.NoError(t, err)
		edges, _, err = typesys.GetEdgesFromWeightedGraph(edges[0].GetTo().GetUniqueLabel(), "user")
		require.NoError(t, err)

		stack := lls.New()
		stack.Push("document#admin")

		newErr := q.exclusionHandler(ctx, &ReverseExpandRequest{
			StoreID:       storeID,
			ObjectType:    objectType,
			Relation:      relation,
			User:          user,
			relationStack: *stack,
		}, make(chan *ReverseExpandResult), edges, "user", NewResolutionMetadata())
		require.ErrorIs(t, newErr, errorRet)
	})
}

func TestCloneStack(t *testing.T) {
	// Create stack and push two elements
	original := lls.New()
	original.Push(1)
	original.Push(2)

	// Clone
	clone := cloneStack(*original)

	// Now pop from original and clone, both should return
	// their results in the correct LIFO order
	val, ok := original.Pop()
	require.True(t, ok)
	require.Equal(t, 2, val)

	val, ok = clone.Pop()
	require.True(t, ok)
	require.Equal(t, 2, val)

	val, ok = original.Pop()
	require.True(t, ok)
	require.Equal(t, 1, val)

	val, ok = clone.Pop()
	require.True(t, ok)
	require.Equal(t, 1, val)
}

func TestCreateStackCloneAndStackWithTopItem(t *testing.T) {
	// Create stack and push two elements
	original := lls.New()
	original.Push(1)
	original.Push(2)
	original.Push(3)

	// Clone
	clone, topItemStack, err := createStackCloneAndStackWithTopItem(*original)
	require.NoError(t, err)

	require.Equal(t, original.Size()-1, clone.Size())
	require.Equal(t, 1, topItemStack.Size())

	values := original.Values()
	cloneValues := clone.Values()
	for i, v := range cloneValues {
		require.Equal(t, values[i+1], v) // clone should have all but the top item
	}

	val, ok := topItemStack.Peek()
	require.True(t, ok)
	require.Equal(t, values[0], val)

	// Error case
	original2 := lls.New()
	_, _, err = createStackCloneAndStackWithTopItem(*original2)
	require.Error(t, err)
	require.ErrorContains(t, err, "cannot create stack clone and stack with top item from an empty stack")
}
