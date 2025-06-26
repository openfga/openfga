package reverseexpand

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReverseExpandWithWeightedGraph(t *testing.T) {
	ds := memory.New()
	t.Cleanup(ds.Close)
	tests := []struct {
		name            string
		model           string
		tuples          []string
		objectType      string
		relation        string
		user            *UserRefObject
		expectedObjects []string
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
			objectType:      "repo",
			relation:        "or_admin",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"repo:fga"},
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
			objectType:      "repo",
			relation:        "admin",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"repo:fga"},
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
			objectType:      "repo",
			relation:        "admin",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"repo:fga"},
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
			objectType:      "repo",
			relation:        "admin",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"repo:fga"},
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
			objectType:      "org",
			relation:        "ttu_recursive",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"org:a", "org:b", "org:c", "org:d"},
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
			objectType:      "org",
			relation:        "org_cycle",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedObjects: []string{"org:a", "org:b"},
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
			objectType:      "org",
			relation:        "can_access",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "bob"}},
			expectedObjects: []string{"org:a_org", "org:b_org"},
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
			objectType:      "org",
			relation:        "teammate",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"org:j", "org:z"},
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
			objectType:      "org",
			relation:        "teammate",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"org:j"},
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
			objectType:      "team",
			relation:        "member",
			user:            &UserRefObject{Object: &openfgav1.Object{Type: "user", Id: "justin"}},
			expectedObjects: []string{"team:fga", "team:cncf", "team:lnf"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storeID, model := storagetest.BootstrapFGAStore(t, ds, test.model, test.tuples)
			resultChan := make(chan *ReverseExpandResult)
			errChan := make(chan error, 1)
			typesys, err := typesystem.NewAndValidate(
				context.Background(),
				model,
			)
			require.NoError(t, err)
			ctx := storage.ContextWithRelationshipTupleReader(context.Background(), ds)
			ctx = typesystem.ContextWithTypesystem(ctx, typesys)
			resolutionMetadata := NewResolutionMetadata()
			go func() {
				q := NewReverseExpandQuery(
					ds,
					typesys,

					// turn on weighted graph functionality
					WithListObjectOptimizationsEnabled(true),
				)

				err = q.Execute(ctx, &ReverseExpandRequest{
					StoreID:    storeID,
					ObjectType: test.objectType,
					Relation:   test.relation,
					User:       test.user,
				}, resultChan, resolutionMetadata)

				if err != nil {
					errChan <- err
				}
			}()

			var results []string
		ConsumerLoop:
			for {
				select {
				case result, open := <-resultChan:
					if !open {
						break ConsumerLoop
					}
					results = append(results, result.Object)
				case err := <-errChan:
					require.FailNowf(t, "unexpected error received on error channel :%v", err.Error())
					break ConsumerLoop
				case <-ctx.Done():
					break ConsumerLoop
				}
			}
			require.ElementsMatch(t, test.expectedObjects, results)
		})
	}
}
