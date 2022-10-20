package test

import (
	"context"
	"os"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultResolveNodeLimit = 25
	gitHubTestDataFile      = "testdata/github.json" // relative to project root
)

var githubTuples = []*openfgapb.TupleKey{
	tuple.NewTupleKey("org:openfga", "member", "erik"),
	tuple.NewTupleKey("org:openfga", "repo_admin", "org:openfga#member"),
	tuple.NewTupleKey("repo:openfga/openfga", "admin", "team:openfga/iam#member"),
	tuple.NewTupleKey("repo:openfga/openfga", "owner", "org:openfga"),
	tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne"),
	tuple.NewTupleKey("repo:openfga/openfga", "writer", "beth"),
	tuple.NewTupleKey("team:openfga/iam", "member", "charles"),
	tuple.NewTupleKey("team:openfga/iam", "member", "team:openfga/protocols#member"),
	tuple.NewTupleKey("team:openfga/protocols", "member", "diane"),
}

func TestCheckQuery(t *testing.T, datastore storage.OpenFGADatastore) {
	var tests = []struct {
		name             string
		typeDefinitions  []*openfgapb.TypeDefinition
		tuples           []*openfgapb.TupleKey
		resolveNodeLimit uint32
		request          *openfgapb.CheckRequest
		err              error
		response         *openfgapb.CheckResponse
	}{
		{
			name: "ExecuteWithEmptyTupleKey",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type:      "repo",
				Relations: map[string]*openfgapb.Userset{},
			}},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: &openfgapb.TupleKey{},
			},
			// output
			err: serverErrors.InvalidCheckInput,
		},
		{
			name: "ExecuteWithEmptyObject",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type:      "repo",
				Relations: map[string]*openfgapb.Userset{},
			}},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("", "reader", "someUser"),
			},
			// output
			err: serverErrors.InvalidCheckInput,
		},
		{
			name: "ExecuteWithEmptyRelation",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type:      "repo",
				Relations: map[string]*openfgapb.Userset{},
			}},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "", "someUser"),
			},
			// output
			err: serverErrors.InvalidCheckInput,
		},
		{
			name: "ExecuteWithEmptyUser",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type:      "repo",
				Relations: map[string]*openfgapb.Userset{},
			}},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", ""),
			},
			// output
			err: serverErrors.InvalidCheckInput,
		},
		{
			name: "ExecuteWithRequestRelationInexistentInTypeDefinition",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type:      "repo",
				Relations: map[string]*openfgapb.Userset{},
			}},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "inexistent", "someUser"),
			},
			// output
			err: serverErrors.RelationNotFound("inexistent", "repo", tuple.NewTupleKey("repo:openfga/openfga", "inexistent", "someUser")),
		},
		{
			name: "ExecuteFailsWithInvalidUser",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin": {},
				},
			}},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "john:albert:doe"),
			},
			// output
			err: serverErrors.InvalidUser("john:albert:doe"),
		},
		{
			name: "ExecuteReturnsErrorNotStackOverflowForInfinitelyRecursiveResolution",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"reader": {
						Userset: &openfgapb.Userset_ComputedUserset{
							ComputedUserset: &openfgapb.ObjectRelation{
								Relation: "writer",
							},
						}},
					"writer": {
						Userset: &openfgapb.Userset_ComputedUserset{
							ComputedUserset: &openfgapb.ObjectRelation{
								Relation: "reader",
							},
						}},
				},
			}},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "someUser"),
			},
			// output
			err: serverErrors.AuthorizationModelResolutionTooComplex,
		},
		{
			name: "ExecuteReturnsResolutionTooComplexErrorForComplexResolution",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"reader": {
						Userset: &openfgapb.Userset_This{},
					},
					"writer": {
						Userset: &openfgapb.Userset_ComputedUserset{
							ComputedUserset: &openfgapb.ObjectRelation{
								Relation: "reader",
							},
						}},
				},
			}},
			resolveNodeLimit: 2,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "writer", "someUser"),
			},
			// output
			err: serverErrors.AuthorizationModelResolutionTooComplex,
		},
		{
			name: "ExecuteReturnsResolutionTooComplexErrorForComplexUnionResolution",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"writer": {
						Userset: &openfgapb.Userset_This{},
					},
					"reader": {
						Userset: &openfgapb.Userset_Union{
							Union: &openfgapb.Usersets{
								Child: []*openfgapb.Userset{
									{
										Userset: &openfgapb.Userset_This{},
									},
									{
										Userset: &openfgapb.Userset_ComputedUserset{
											ComputedUserset: &openfgapb.ObjectRelation{
												Relation: "writer",
											},
										},
									},
								},
							},
						},
					},
				},
			}},
			resolveNodeLimit: 2,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "someUser"),
			},
			// output
			err: serverErrors.AuthorizationModelResolutionTooComplex,
		},
		{
			name: "ExecuteWithExistingTupleKeyAndEmptyUserSetReturnsAllowed",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin": {},
				},
			}},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				Trace:    true,
			},
			// output
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".(direct).",
			},
		},
		{
			name: "ExecuteWithAllowAllTupleKeyAndEmptyUserSetReturnsAllowed",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin": {},
				},
			}},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "*"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				Trace:    true,
			},
			// output
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".(direct).",
			},
		},
		{
			name: "ExecuteWithNonExistingTupleKeyAndEmptyUserSetReturnsNotAllowed",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin": {},
				},
			}},
			tuples:           []*openfgapb.TupleKey{},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
			},
			// output
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name: "ExecuteWithUnionAndDirectUserSetReturnsAllowedIfDirectTupleExists",
			// state
			//relation {
			//	name: "admin"
			//	userset_rewrite {
			//		child { _this {  }}
			//	}
			//}
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin": {
						Userset: &openfgapb.Userset_Union{
							Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
								{Userset: &openfgapb.Userset_This{
									This: &openfgapb.DirectUserset{},
								}},
							}},
						},
					},
				},
			}},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				Trace:    true,
			},
			// output
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.0(direct).",
			},
		},
		{
			name: "ExecuteWithUnionAndDirectUserSetReturnsAllowedIfAllUsersTupleExists",
			// state
			//relation {
			//	name: "admin"
			//	userset_rewrite {
			//		child { _this {  }}
			//	}
			//}
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin": {
						Userset: &openfgapb.Userset_Union{
							Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
								{Userset: &openfgapb.Userset_This{
									This: &openfgapb.DirectUserset{},
								}},
							}},
						},
					},
				},
			}},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "*"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				Trace:    true,
			},
			// output
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.0(direct).",
			},
		},
		{
			name: "ExecuteWithUnionAndComputedUserSetReturnsNotAllowedIfComputedUsersetDoesNotIncludeUser",
			// state
			//relation {
			//	name: "admin"
			//	userset_rewrite {
			//    child { computed_userset { relation: "owner" }}
			//	}
			//}
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin": {
						Userset: &openfgapb.Userset_Union{
							Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
								{Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "owner",
									},
								}},
							}},
						},
					},
					"owner": {},
				},
			}},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "owner", "team/iam"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
			},
			// output
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name: "ExecuteWithUnionAndComputedUserSetReturnsAllowedIfComputedUsersetIncludesUser",
			// state
			//relation {
			//	name: "reader"
			//	userset_rewrite {
			//    child { _this {  }}
			//    child { computed_userset { relation: "writer" }}
			//	}
			//}
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"reader": {
						Userset: &openfgapb.Userset_Union{
							Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
								{Userset: &openfgapb.Userset_This{}},
								{Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "writer",
									},
								}},
							}},
						},
					},
					"writer": {},
				},
			}},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "github|jose@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "github|jose@openfga"),
				Trace:    true,
			},
			// output
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(computed-userset).repo:openfga/openfga#writer.(direct).",
			},
		},
		{
			name: "ExecuteDirectSetReturnsAllowedIfUserHasRelationWithAnObjectThatHasUserAccessToTheTargetObject",
			// state
			//relation {
			//	name: "reader"
			//	userset_rewrite {
			//    child { _this {  }}
			//	}
			//}
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"admin": {
						Userset: &openfgapb.Userset_This{},
					},
				},
			}, {
				Type: "team",
				Relations: map[string]*openfgapb.Userset{
					"team_member": {},
				},
			}},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("team:iam", "team_member", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "team:iam#team_member"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "github|jose@openfga"),
				Trace:    true,
			},
			// output
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".(direct).team:iam#team_member.(direct).",
			},
		},
		{
			name: "ExecuteReturnsAllowedIfUserIsHasRelationToAnObjectThatIsInComputedUserSetForAnotherObject",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"reader": {
						Userset: &openfgapb.Userset_Union{
							Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
								{Userset: &openfgapb.Userset_This{}},
								{Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "writer",
									},
								}},
							}},
						},
					},
					"writer": {},
				},
			}, {
				Type: "team",
				Relations: map[string]*openfgapb.Userset{
					"team_member": {},
				},
			}},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("team:iam", "team_member", "github|jose@openfga"),
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:iam#team_member"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "github|jose@openfga"),
				Trace:    true,
			},
			// output
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(computed-userset).repo:openfga/openfga#writer.(direct).team:iam#team_member.(direct).",
			},
		},
		{
			name: "ExecuteReturnsNotAllowedIfIntersectionIsRequiredAndUserIsInOneUserSetButNotTheOther",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "openfga-store",
				// pretend you can only create an organization user in an openfga store if
				// you can create a user AND write an organization in a store
				Relations: map[string]*openfgapb.Userset{
					"create_organization_user": {
						Userset: &openfgapb.Userset_Intersection{
							Intersection: &openfgapb.Usersets{Child: []*openfgapb.Userset{
								{Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "create_user",
									},
								}},
								{Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "write_organization",
									},
								}},
							}},
						},
					},
					"create_user":        {},
					"write_organization": {},
				},
			}},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("openfga-store:yenkel-dev", "create_user", "github|yenkel@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("openfga-store:yenkel-dev", "create_organization_user", "github|yenkel@openfga"),
				Trace:    true,
			},
			// output
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name: "ExecuteReturnsAllowedIfIntersectionIsRequiredAndUserIsInAllUserSets",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "openfga-store",
				// pretend you can only create an organization user in an openfga store if
				// you can create a user AND write an organization in a store
				Relations: map[string]*openfgapb.Userset{
					"create_organization_user": {
						Userset: &openfgapb.Userset_Intersection{
							Intersection: &openfgapb.Usersets{Child: []*openfgapb.Userset{
								{Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "create_user",
									},
								}},
								{Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "write_organization",
									},
								}},
							}},
						},
					},
					"write_organization": {},
					"create_user":        {},
				},
			}},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("openfga-store:yenkel-dev", "create_user", "github|yenkel@openfga"),
				tuple.NewTupleKey("openfga-store:yenkel-dev", "write_organization", "github|yenkel@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("openfga-store:yenkel-dev", "create_organization_user", "github|yenkel@openfga"),
				Trace:    true,
			},
			// output
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".[.0(computed-userset).openfga-store:yenkel-dev#create_user.(direct).,.1(computed-userset).openfga-store:yenkel-dev#write_organization.(direct).]",
			},
		},
		{
			name: "ExecuteSupportsNestedIntersectionAndCorrectlyTraces",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{{
				Type: "openfga-store",
				// pretend you can only create an organization user in an openfga store if
				// you can create a user AND write an organization in a store
				Relations: map[string]*openfgapb.Userset{
					"create_organization_user": {
						Userset: &openfgapb.Userset_Intersection{
							Intersection: &openfgapb.Usersets{Child: []*openfgapb.Userset{
								{Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "create_user",
									},
								}},
								{Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "write_organization",
									},
								}},
							}},
						},
					},
					"create_user": {
						Userset: &openfgapb.Userset_Intersection{
							Intersection: &openfgapb.Usersets{Child: []*openfgapb.Userset{
								{Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "create_user_a",
									},
								}},
								{Userset: &openfgapb.Userset_ComputedUserset{
									ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "create_user_b",
									},
								}},
							}},
						},
					},
					"write_organization": {},
					"create_user_a":      {},
					"create_user_b":      {},
				},
			}},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("openfga-store:yenkel-dev", "create_user_a", "github|yenkel@openfga"),
				tuple.NewTupleKey("openfga-store:yenkel-dev", "create_user_b", "github|yenkel@openfga"),
				tuple.NewTupleKey("openfga-store:yenkel-dev", "write_organization", "github|yenkel@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			// input
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("openfga-store:yenkel-dev", "create_organization_user", "github|yenkel@openfga"),
				Trace:    true,
			},
			// output
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".[.0(computed-userset).openfga-store:yenkel-dev#create_user.[.0(computed-userset).openfga-store:yenkel-dev#create_user.0(computed-userset).openfga-store:yenkel-dev#create_user_a.(direct).,.0(computed-userset).openfga-store:yenkel-dev#create_user.1(computed-userset).openfga-store:yenkel-dev#create_user_b.(direct).],.1(computed-userset).openfga-store:yenkel-dev#write_organization.(direct).]",
			},
		},
		{
			name: "ExecuteReturnsAllowedForUserNotRemovedByDifference",
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {
							Userset: &openfgapb.Userset_Difference{
								Difference: &openfgapb.Difference{
									Base: &openfgapb.Userset{
										Userset: &openfgapb.Userset_This{},
									},
									Subtract: &openfgapb.Userset{
										Userset: &openfgapb.Userset_ComputedUserset{
											ComputedUserset: &openfgapb.ObjectRelation{
												Relation: "banned",
											},
										},
									},
								},
							},
						},
						"banned": {},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/canaveral", "admin", "github|jon.allie@openfga"),
				tuple.NewTupleKey("repo:openfga/canaveral", "banned", "github|jose@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/canaveral", "admin", "github|jon.allie@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".0(direct).",
			},
		},
		{
			name: "ExecuteReturnsNotAllowedForUserRemovedByDifference",
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {
							Userset: &openfgapb.Userset_Difference{
								Difference: &openfgapb.Difference{
									Base: &openfgapb.Userset{
										Userset: &openfgapb.Userset_This{},
									},
									Subtract: &openfgapb.Userset{
										Userset: &openfgapb.Userset_ComputedUserset{
											ComputedUserset: &openfgapb.ObjectRelation{
												Relation: "banned",
											},
										},
									},
								},
							},
						},
						"banned": {},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/canaveral", "admin", "github|jon.allie@openfga"),
				tuple.NewTupleKey("repo:openfga/canaveral", "banned", "github|jon.allie@openfga"),
				tuple.NewTupleKey("repo:openfga/canaveral", "banned", "github|jose@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/canaveral", "admin", "github|jon.allie@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name: "ExecuteReturnsAllowedForTupleToUserset",
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {
							Userset: &openfgapb.Userset_Union{
								Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
									{Userset: &openfgapb.Userset_This{}},
									{Userset: &openfgapb.Userset_TupleToUserset{TupleToUserset: &openfgapb.TupleToUserset{
										Tupleset: &openfgapb.ObjectRelation{
											Relation: "manager",
										},
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "$TUPLE_USERSET_OBJECT",
											Relation: "repo_admin",
										},
									}}},
								}},
							},
						},
					},
				},
				{
					Type: "org",
					Relations: map[string]*openfgapb.Userset{
						// implicit direct?
						"repo_admin": {},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/canaveral", "manager", "org:openfga#repo_admin"),
				tuple.NewTupleKey("org:openfga", "repo_admin", "github|jose@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/canaveral", "admin", "github|jose@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(tuple-to-userset).repo:openfga/canaveral#manager.org:openfga#repo_admin.(direct).",
			},
		},
		{
			name: "ExecuteCanResolveRecursiveComputedUserSets",
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {
							Userset: &openfgapb.Userset_Union{
								Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
									{Userset: &openfgapb.Userset_This{}},
									{Userset: &openfgapb.Userset_TupleToUserset{TupleToUserset: &openfgapb.TupleToUserset{
										Tupleset: &openfgapb.ObjectRelation{
											Relation: "manager",
										},
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "$TUPLE_USERSET_OBJECT",
											Relation: "repo_admin",
										},
									}}},
								}},
							},
						},
						"maintainer": {
							Userset: &openfgapb.Userset_Union{
								Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
									{Userset: &openfgapb.Userset_This{}},
									{Userset: &openfgapb.Userset_ComputedUserset{ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "admin",
									}}},
								}},
							},
						},
						"writer": {
							Userset: &openfgapb.Userset_Union{
								Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
									{Userset: &openfgapb.Userset_This{}},
									{Userset: &openfgapb.Userset_ComputedUserset{ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "maintainer",
									}}},
									{Userset: &openfgapb.Userset_TupleToUserset{TupleToUserset: &openfgapb.TupleToUserset{
										Tupleset: &openfgapb.ObjectRelation{
											Relation: "manager",
										},
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "$TUPLE_USERSET_OBJECT",
											Relation: "repo_writer",
										},
									}}},
								}},
							},
						},
						"triager": {
							Userset: &openfgapb.Userset_Union{
								Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
									{Userset: &openfgapb.Userset_This{}},
									{Userset: &openfgapb.Userset_ComputedUserset{ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "writer",
									}}},
								}},
							},
						},
						"reader": {
							Userset: &openfgapb.Userset_Union{
								Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
									{Userset: &openfgapb.Userset_This{}},
									{Userset: &openfgapb.Userset_ComputedUserset{ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "triager",
									}}},
									{Userset: &openfgapb.Userset_TupleToUserset{TupleToUserset: &openfgapb.TupleToUserset{
										Tupleset: &openfgapb.ObjectRelation{
											Relation: "manager",
										},
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "$TUPLE_USERSET_OBJECT",
											Relation: "repo_reader",
										},
									}}},
								}},
							},
						},
					},
				},
				{
					Type: "team",
					Relations: map[string]*openfgapb.Userset{
						"member": {},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "writer", "team:openfga#member"),
				tuple.NewTupleKey("team:openfga", "member", "github|iaco@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "github|iaco@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(computed-userset).repo:openfga/openfga#triager.union.1(computed-userset).repo:openfga/openfga#writer.union.0(direct).team:openfga#member.(direct).",
			},
		},
		{
			name: "ExecuteCanResolveRecursiveTupleToUserSets",
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"parent": {Userset: &openfgapb.Userset_This{}},
						"owner":  {Userset: &openfgapb.Userset_This{}},
						"editor": {
							Userset: &openfgapb.Userset_Union{
								Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
									{Userset: &openfgapb.Userset_This{}},
									{Userset: &openfgapb.Userset_ComputedUserset{ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "owner",
									}}},
								}},
							},
						},
						"viewer": {
							Userset: &openfgapb.Userset_Union{
								Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
									{Userset: &openfgapb.Userset_This{}},
									{Userset: &openfgapb.Userset_ComputedUserset{ComputedUserset: &openfgapb.ObjectRelation{
										Relation: "editor",
									}}},
									{Userset: &openfgapb.Userset_TupleToUserset{TupleToUserset: &openfgapb.TupleToUserset{
										Tupleset: &openfgapb.ObjectRelation{
											Relation: "parent",
										},
										ComputedUserset: &openfgapb.ObjectRelation{
											Object:   "$TUPLE_USERSET_OBJECT",
											Relation: "viewer",
										},
									}}},
								}},
							},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:octo_v2_draft", "parent", "document:octo_folder"),
				tuple.NewTupleKey("document:octo_folder", "editor", "google|iaco@openfga"),
			},
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("document:octo_v2_draft", "viewer", "google|iaco@openfga"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.2(tuple-to-userset).document:octo_v2_draft#parent.document:octo_folder#viewer.union.1(computed-userset).document:octo_folder#editor.union.0(direct).",
			},
		},
		{
			name:             "CheckWithUsersetAsUser",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("team:iam", "member", "org:openfga#member"),
			},
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "team",
					Relations: map[string]*openfgapb.Userset{
						"member": {Userset: &openfgapb.Userset_This{}},
					},
				},
				{
					Type: "org",
					Relations: map[string]*openfgapb.Userset{
						"member": {Userset: &openfgapb.Userset_This{}},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("team:iam", "member", "team:engineering#member"),
				tuple.NewTupleKey("team:engineering", "member", "org:openfga#member"),
			},
			response: &openfgapb.CheckResponse{
				Allowed: true,
			},
		},
		{
			name:             "CheckUsersetAsUser_WithContextualTuples",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("team:iam", "member", "org:openfga#member"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("team:iam", "member", "team:engineering#member"),
						tuple.NewTupleKey("team:engineering", "member", "org:openfga#member"),
					},
				},
			},
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "team",
					Relations: map[string]*openfgapb.Userset{
						"member": {Userset: &openfgapb.Userset_This{}},
					},
				},
				{
					Type: "org",
					Relations: map[string]*openfgapb.Userset{
						"member": {Userset: &openfgapb.Userset_This{}},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{},
			response: &openfgapb.CheckResponse{
				Allowed: true,
			},
		},
		{
			name:             "CheckUsersetAsUser_WithContextualTuples",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("team:iam", "member", "org:openfga#member"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("team:iam", "member", "team:engineering#member"),
						tuple.NewTupleKey("team:engineering", "member", "org:openfga#member"),
					},
				},
			},
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "team",
					Relations: map[string]*openfgapb.Userset{
						"member": {Userset: &openfgapb.Userset_This{}},
					},
				},
				{
					Type: "org",
					Relations: map[string]*openfgapb.Userset{
						"member": {Userset: &openfgapb.Userset_This{}},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{},
			response: &openfgapb.CheckResponse{
				Allowed: true,
			},
		},
		{
			name:             "CheckUsersetAsUser_WithContextualTuples",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("team:iam", "member", "org:openfga#member"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("team:iam", "member", "team:engineering#member"),
						tuple.NewTupleKey("team:engineering", "member", "org:openfga#member"),
					},
				},
			},
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "team",
					Relations: map[string]*openfgapb.Userset{
						"member": {Userset: &openfgapb.Userset_This{}},
					},
				},
				{
					Type: "org",
					Relations: map[string]*openfgapb.Userset{
						"member": {Userset: &openfgapb.Userset_This{}},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{},
			response: &openfgapb.CheckResponse{
				Allowed: true,
			},
		},
		{
			name:             "Check with TupleToUserset involving no object or userset",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("document:doc1", "viewer", "anne"),
			},
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"parent": {
							Userset: &openfgapb.Userset_This{},
						},
						"viewer": {
							Userset: &openfgapb.Userset_TupleToUserset{
								TupleToUserset: &openfgapb.TupleToUserset{
									Tupleset:        &openfgapb.ObjectRelation{Relation: "parent"},
									ComputedUserset: &openfgapb.ObjectRelation{Relation: "viewer"},
								},
							},
						},
					},
				},
				{
					Type: "folder",
					Relations: map[string]*openfgapb.Userset{
						"viewer": {
							Userset: &openfgapb.Userset_This{},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:doc1", "parent", "folder1"), // folder1 isn't an object or userset
				tuple.NewTupleKey("folder:folder1", "viewer", "anne"),
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name:             "TupleToUserset Check Passes when at least one tupleset relation resolves",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("document:doc1", "viewer", "anne"),
			},
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"parent": {
							Userset: &openfgapb.Userset_This{},
						},
						"viewer": {
							Userset: &openfgapb.Userset_TupleToUserset{
								TupleToUserset: &openfgapb.TupleToUserset{
									Tupleset:        &openfgapb.ObjectRelation{Relation: "parent"},
									ComputedUserset: &openfgapb.ObjectRelation{Relation: "viewer"},
								},
							},
						},
					},
				},
				{
					Type: "folder",
					Relations: map[string]*openfgapb.Userset{
						"viewer": {
							Userset: &openfgapb.Userset_This{},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:doc1", "parent", "folder1"), // folder1 isn't an object or userset
				tuple.NewTupleKey("document:doc1", "parent", "folder:folder1"),
				tuple.NewTupleKey("folder:folder1", "viewer", "anne"),
			},
			response: &openfgapb.CheckResponse{
				Allowed: true,
			},
		},
		{
			name:             "Error if * encountered in TupleToUserset evaluation",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("document:doc1", "viewer", "user:anne"),
			},
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"parent": typesystem.This(),
						"viewer": typesystem.TupleToUserset("parent", "viewer"),
					},
					Metadata: &openfgapb.Metadata{
						Relations: map[string]*openfgapb.RelationMetadata{
							"parent": {
								DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
									typesystem.RelationReference("folder", ""),
								},
							},
						},
					},
				},
				{
					Type: "folder",
					Relations: map[string]*openfgapb.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgapb.Metadata{
						Relations: map[string]*openfgapb.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
									typesystem.RelationReference("user", ""),
								},
							},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:doc1", "parent", "*"), // wildcard not allowed on tupleset relations
				tuple.NewTupleKey("folder:folder1", "viewer", "user:anne"),
			},
			err: serverErrors.InvalidTuple(
				"unexpected wildcard evaluated on relation 'document#parent'",
				tuple.NewTupleKey("document:doc1", "parent", commands.Wildcard),
			),
		},
		{
			name:             "Error if * encountered in TTU evaluation including ContextualTuples",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("document:doc1", "viewer", "user:anne"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("document:doc1", "parent", "*"), // wildcard not allowed on tupleset relations
						tuple.NewTupleKey("folder:folder1", "viewer", "user:anne"),
					},
				},
			},
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "document",
					Relations: map[string]*openfgapb.Userset{
						"parent": typesystem.This(),
						"viewer": typesystem.TupleToUserset("parent", "viewer"),
					},
					Metadata: &openfgapb.Metadata{
						Relations: map[string]*openfgapb.RelationMetadata{
							"parent": {
								DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
									typesystem.RelationReference("folder", ""),
								},
							},
						},
					},
				},
				{
					Type: "folder",
					Relations: map[string]*openfgapb.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgapb.Metadata{
						Relations: map[string]*openfgapb.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
									typesystem.RelationReference("user", ""),
								},
							},
						},
					},
				},
			},
			err: serverErrors.InvalidTuple(
				"unexpected wildcard evaluated on relation 'document#parent'",
				tuple.NewTupleKey("document:doc1", "parent", commands.Wildcard),
			),
		},
	}

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	logger := logger.NewNoopLogger()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := id.Must(id.New()).String()
			model := &openfgapb.AuthorizationModel{
				Id:              id.Must(id.New()).String(),
				SchemaVersion:   typesystem.SchemaVersion1_0,
				TypeDefinitions: test.typeDefinitions,
			}

			err := datastore.WriteAuthorizationModel(ctx, store, model)
			require.NoError(t, err)

			if test.tuples != nil {
				err := datastore.Write(ctx, store, nil, test.tuples)
				require.NoError(t, err)
			}

			cmd := commands.NewCheckQuery(datastore, tracer, meter, logger, test.resolveNodeLimit)
			test.request.StoreId = store
			test.request.AuthorizationModelId = model.Id
			resp, gotErr := cmd.Execute(ctx, test.request)

			require.ErrorIs(t, gotErr, test.err)

			if test.response != nil {
				require.NoError(t, gotErr)

				require.Equal(t, test.response.Allowed, resp.Allowed)

				if test.response.Allowed {
					require.Equal(t, test.response.Resolution, resp.Resolution)
				}
			}
		})
	}
}

func TestCheckQueryAgainstGitHubModel(t *testing.T, datastore storage.OpenFGADatastore) {
	var tests = []struct {
		name             string
		resolveNodeLimit uint32
		request          *openfgapb.CheckRequest
		err              error
		response         *openfgapb.CheckResponse
	}{
		{
			name:             "GitHubAssertion1",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.0(direct).",
			},
		},
		{
			name:             "GitHubAssertion2",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "triager", "anne"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name:             "GitHubAssertion3",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "diane"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.0(direct).team:openfga/iam#member.(direct).team:openfga/protocols#member.(direct).",
			},
		},
		{
			name:             "GitHubAssertion4",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "erik"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(computed-userset).repo:openfga/openfga#triager.union.1(computed-userset).repo:openfga/openfga#writer.union.1(computed-userset).repo:openfga/openfga#maintainer.union.1(computed-userset).repo:openfga/openfga#admin.union.1(tuple-to-userset).repo:openfga/openfga#owner.org:openfga#repo_admin.(direct).org:openfga#member.union.0(direct).",
			},
		},
		{
			name:             "GitHubAssertion5",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "writer", "charles"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(computed-userset).repo:openfga/openfga#maintainer.union.1(computed-userset).repo:openfga/openfga#admin.union.0(direct).team:openfga/iam#member.(direct).",
			},
		},
		{
			name:             "GitHubAssertion6",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "beth"),
				Trace:    true,
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
	}

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	logger := logger.NewNoopLogger()

	data, err := os.ReadFile(gitHubTestDataFile)
	require.NoError(t, err)

	var gitHubDefinition openfgapb.WriteAuthorizationModelRequest
	err = protojson.Unmarshal(data, &gitHubDefinition)
	require.NoError(t, err)

	store := id.Must(id.New()).String()
	model := &openfgapb.AuthorizationModel{
		Id:              id.Must(id.New()).String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: gitHubDefinition.GetTypeDefinitions(),
	}

	err = datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(t, err)

	err = datastore.Write(ctx, store, nil, githubTuples)
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := commands.NewCheckQuery(datastore, tracer, meter, logger, test.resolveNodeLimit)
			test.request.StoreId = store
			test.request.AuthorizationModelId = model.Id
			resp, gotErr := cmd.Execute(ctx, test.request)

			if test.err == nil {
				require.NoError(t, gotErr)
			}

			if test.err != nil {
				require.EqualError(t, test.err, gotErr.Error())
			}

			if test.response != nil {
				require.NoError(t, gotErr)

				require.Equal(t, test.response.Allowed, resp.Allowed)

				if test.response.Allowed {
					require.Equal(t, test.response.Resolution, resp.Resolution)
				}
			}
		})
	}
}

func TestCheckQueryWithContextualTuplesAgainstGitHubModel(t *testing.T, datastore storage.OpenFGADatastore) {
	var tests = []struct {
		name             string
		resolveNodeLimit uint32
		request          *openfgapb.CheckRequest
		err              error
		response         *openfgapb.CheckResponse
	}{
		{
			name:             "ContextualTuplesGitHubAssertion1",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey:         tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: githubTuples},
				Trace:            true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.0(direct).",
			},
		},
		{
			name:             "ContextualTuplesGitHubAssertion2",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey:         tuple.NewTupleKey("repo:openfga/openfga", "triager", "anne"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: githubTuples},
				Trace:            true,
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name:             "ContextualTuplesGitHubAssertion3",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey:         tuple.NewTupleKey("repo:openfga/openfga", "admin", "diane"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: githubTuples},
				Trace:            true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.0(direct).team:openfga/iam#member.(direct).team:openfga/protocols#member.(direct).",
			},
		},
		{
			name:             "ContextualTuplesGitHubAssertion4",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey:         tuple.NewTupleKey("repo:openfga/openfga", "reader", "erik"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: githubTuples},
				Trace:            true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(computed-userset).repo:openfga/openfga#triager.union.1(computed-userset).repo:openfga/openfga#writer.union.1(computed-userset).repo:openfga/openfga#maintainer.union.1(computed-userset).repo:openfga/openfga#admin.union.1(tuple-to-userset).repo:openfga/openfga#owner.org:openfga#repo_admin.(direct).org:openfga#member.union.0(direct).",
			},
		},
		{
			name:             "ContextualTuplesGitHubAssertion5",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey:         tuple.NewTupleKey("repo:openfga/openfga", "writer", "charles"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: githubTuples},
				Trace:            true,
			},
			response: &openfgapb.CheckResponse{
				Allowed:    true,
				Resolution: ".union.1(computed-userset).repo:openfga/openfga#maintainer.union.1(computed-userset).repo:openfga/openfga#admin.union.0(direct).team:openfga/iam#member.(direct).",
			},
		},
		{
			name:             "ContextualTuplesGitHubAssertion6",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey:         tuple.NewTupleKey("repo:openfga/openfga", "admin", "beth"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: githubTuples},
				Trace:            true,
			},
			response: &openfgapb.CheckResponse{
				Allowed: false,
			},
		},
		{
			name:             "RepeatedContextualTuplesShouldError",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne"),
						tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne"),
					},
				},
				Trace: true,
			},
			err: serverErrors.DuplicateContextualTuple(tuple.NewTupleKey("repo:openfga/openfga", "reader", "anne")),
		},
		{
			name:             "ContextualTuplesWithEmptyUserFails",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "beth"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("org:openfga", "member", ""),
					},
				},
				Trace: true,
			},
			err: serverErrors.InvalidContextualTuple(tuple.NewTupleKey("org:openfga", "member", "")),
		},
		{
			name:             "ContextualTuplesWithEmptyRelationFails",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "beth"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("org:openfga", "", "anne"),
					},
				},
				Trace: true,
			},
			err: serverErrors.InvalidContextualTuple(tuple.NewTupleKey("org:openfga", "", "anne")),
		},
		{
			name:             "ContextualTuplesWithEmptyObjectFails",
			resolveNodeLimit: defaultResolveNodeLimit,
			request: &openfgapb.CheckRequest{
				TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "beth"),
				ContextualTuples: &openfgapb.ContextualTupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("", "member", "anne"),
					},
				},
				Trace: true,
			},
			err: serverErrors.InvalidContextualTuple(tuple.NewTupleKey("", "member", "anne")),
		},
	}

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	logger := logger.NewNoopLogger()

	data, err := os.ReadFile(gitHubTestDataFile)
	require.NoError(t, err)

	var gitHubDefinition openfgapb.WriteAuthorizationModelRequest
	err = protojson.Unmarshal(data, &gitHubDefinition)
	require.NoError(t, err)

	storeID := id.Must(id.New()).String()
	model := &openfgapb.AuthorizationModel{
		Id:              id.Must(id.New()).String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: gitHubDefinition.GetTypeDefinitions(),
	}

	err = datastore.WriteAuthorizationModel(ctx, storeID, model)
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd := commands.NewCheckQuery(datastore, tracer, meter, logger, test.resolveNodeLimit)
			test.request.StoreId = storeID
			test.request.AuthorizationModelId = model.Id
			resp, gotErr := cmd.Execute(ctx, test.request)

			if test.err == nil {
				require.NoError(t, gotErr)
			}

			if test.err != nil {
				require.EqualError(t, test.err, gotErr.Error())
			}

			if test.response != nil {
				require.NoError(t, gotErr)

				require.Equal(t, test.response.Allowed, resp.Allowed)

				if test.response.Allowed {
					require.Equal(t, test.response.Resolution, resp.Resolution)
				}
			}
		})
	}
}

func TestCheckQueryAuthorizationModelsVersioning(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	logger := logger.NewNoopLogger()
	store := id.Must(id.New()).String()

	oldModel := &openfgapb.AuthorizationModel{
		Id:            id.Must(id.New()).String(),
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"owner": {Userset: &openfgapb.Userset_This{}},
					"editor": {
						Userset: &openfgapb.Userset_Union{
							Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
								{Userset: &openfgapb.Userset_This{}},
								{Userset: &openfgapb.Userset_ComputedUserset{ComputedUserset: &openfgapb.ObjectRelation{Relation: "owner"}}},
							}},
						},
					},
				},
			},
		},
	}

	err := datastore.WriteAuthorizationModel(ctx, store, oldModel)
	require.NoError(t, err)

	updatedModel := &openfgapb.AuthorizationModel{
		Id:            id.Must(id.New()).String(),
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"owner":  {Userset: &openfgapb.Userset_This{}},
					"editor": {Userset: &openfgapb.Userset_This{}},
				},
			},
		},
	}

	err = datastore.WriteAuthorizationModel(ctx, store, updatedModel)
	require.NoError(t, err)

	err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, []*openfgapb.TupleKey{{Object: "repo:openfgapb", Relation: "owner", User: "yenkel"}})
	require.NoError(t, err)

	oldResp, err := commands.NewCheckQuery(datastore, tracer, meter, logger, defaultResolveNodeLimit).Execute(ctx, &openfgapb.CheckRequest{
		StoreId:              store,
		AuthorizationModelId: oldModel.Id,
		TupleKey: &openfgapb.TupleKey{
			Object:   "repo:openfgapb",
			Relation: "editor",
			User:     "yenkel",
		},
	})
	require.NoError(t, err)
	require.True(t, oldResp.Allowed)

	updatedResp, err := commands.NewCheckQuery(datastore, tracer, meter, logger, defaultResolveNodeLimit).Execute(ctx, &openfgapb.CheckRequest{
		StoreId:              store,
		AuthorizationModelId: updatedModel.Id,
		TupleKey: &openfgapb.TupleKey{
			Object:   "repo:openfgapb",
			Relation: "editor",
			User:     "yenkel",
		},
	})
	require.NoError(t, err)
	require.False(t, updatedResp.Allowed)
}

var tuples = []*openfgapb.TupleKey{
	tuple.NewTupleKey("repo:openfga/openfga", "reader", "team:openfga#member"),
	tuple.NewTupleKey("team:openfga", "member", "github|iaco@openfga"),
}

// Used to avoid compiler optimizations (see https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go)
var result *openfgapb.CheckResponse //nolint

func BenchmarkCheckWithoutTrace(b *testing.B, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	logger := logger.NewNoopLogger()
	store := id.Must(id.New()).String()

	data, err := os.ReadFile(gitHubTestDataFile)
	require.NoError(b, err)

	var gitHubTypeDefinitions openfgapb.WriteAuthorizationModelRequest
	err = protojson.Unmarshal(data, &gitHubTypeDefinitions)
	require.NoError(b, err)

	model := &openfgapb.AuthorizationModel{
		Id:              id.Must(id.New()).String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: gitHubTypeDefinitions.GetTypeDefinitions(),
	}

	err = datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(b, err)

	err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, tuples)
	require.NoError(b, err)

	checkQuery := commands.NewCheckQuery(datastore, tracer, meter, logger, defaultResolveNodeLimit)

	var r *openfgapb.CheckResponse

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = checkQuery.Execute(ctx, &openfgapb.CheckRequest{
			StoreId:              store,
			AuthorizationModelId: model.Id,
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "reader",
				User:     "github|iaco@openfga",
			},
		})
	}

	result = r
}

func BenchmarkWithTrace(b *testing.B, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	logger := logger.NewNoopLogger()
	store := id.Must(id.New()).String()

	data, err := os.ReadFile(gitHubTestDataFile)
	require.NoError(b, err)

	var gitHubTypeDefinitions openfgapb.WriteAuthorizationModelRequest
	err = protojson.Unmarshal(data, &gitHubTypeDefinitions)
	require.NoError(b, err)

	model := &openfgapb.AuthorizationModel{
		Id:              id.Must(id.New()).String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: gitHubTypeDefinitions.GetTypeDefinitions(),
	}

	err = datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(b, err)

	err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, tuples)
	require.NoError(b, err)

	checkQuery := commands.NewCheckQuery(datastore, tracer, meter, logger, defaultResolveNodeLimit)

	var r *openfgapb.CheckResponse

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = checkQuery.Execute(ctx, &openfgapb.CheckRequest{
			StoreId:              store,
			AuthorizationModelId: model.Id,
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "reader",
				User:     "github|iaco@openfga",
			},
			Trace: true,
		})
	}

	result = r
}
