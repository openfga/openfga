package test

import (
	"context"
	"os"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultResolveNodeLimit = 25
	gitHubTestDataFile      = "testdata/github.json" // relative to project root
)

var gitHubTuples = []*openfgapb.TupleKey{
	{
		User:     "erik",
		Relation: "member",
		Object:   "org:openfga",
	},
	{
		User:     "org:openfga#member",
		Relation: "repo_admin",
		Object:   "org:openfga",
	},
	{
		User:     "team:openfga/iam#member",
		Relation: "admin",
		Object:   "repo:openfga/openfga",
	},
	{
		User:     "org:openfga",
		Relation: "owner",
		Object:   "repo:openfga/openfga",
	},
	{
		User:     "anne",
		Relation: "reader",
		Object:   "repo:openfga/openfga",
	},
	{
		User:     "beth",
		Relation: "writer",
		Object:   "repo:openfga/openfga",
	},
	{
		User:     "charles",
		Relation: "member",
		Object:   "team:openfga/iam",
	},
	{
		User:     "team:openfga/protocols#member",
		Relation: "member",
		Object:   "team:openfga/iam",
	},
	{
		User:     "diane",
		Relation: "member",
		Object:   "team:openfga/protocols",
	},
}

type checkQueryTest struct {
	_name                   string
	useGitHubTypeDefinition bool
	typeDefinitions         []*openfgapb.TypeDefinition
	tuples                  []*openfgapb.TupleKey
	resolveNodeLimit        uint32
	request                 *openfgapb.CheckRequest
	err                     error
	response                *openfgapb.CheckResponse
}

var checkQueryTests = []checkQueryTest{
	{
		_name: "ExecuteWithEmptyTupleKey",
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
		_name: "ExecuteWithEmptyObject",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgapb.Userset{},
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Relation: "reader",
				User:     "someUser",
			},
		},
		// output
		err: serverErrors.InvalidCheckInput,
	},
	{
		_name: "ExecuteWithEmptyRelation",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgapb.Userset{},
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object: "repo:openfga/openfga",
				User:   "someUser",
			},
		},
		// output
		err: serverErrors.InvalidCheckInput,
	},
	{
		_name: "ExecuteWithEmptyUser",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgapb.Userset{},
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Relation: "reader",
				Object:   "repo:openfga/openfga",
			},
		},
		// output
		err: serverErrors.InvalidCheckInput,
	},
	{
		_name: "ExecuteWithRequestRelationInexistentInTypeDefinition",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgapb.Userset{},
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "inexistent",
				User:     "someUser",
			},
		},
		// output
		err: serverErrors.RelationNotFound("inexistent", "repo", &openfgapb.TupleKey{
			Object:   "repo:openfga/openfga",
			Relation: "inexistent",
			User:     "someUser",
		}),
	},
	{
		_name: "ExecuteFailsWithInvalidUser",
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
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "admin",
				User:     "john:albert:doe",
			},
		},
		// output
		err: serverErrors.InvalidUser("john:albert:doe"),
	},
	{
		_name: "ExecuteReturnsErrorNotStackOverflowForInfinitelyRecursiveResolution",
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
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "reader",
				User:     "someUser",
			},
		},
		// output
		err: serverErrors.AuthorizationModelResolutionTooComplex,
	},
	{
		_name: "ExecuteReturnsResolutionTooComplexErrorForComplexResolution",
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
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "writer",
				User:     "someUser",
			},
		},
		// output
		err: serverErrors.AuthorizationModelResolutionTooComplex,
	},
	{
		_name: "ExecuteReturnsResolutionTooComplexErrorForComplexUnionResolution",
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
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "reader",
				User:     "someUser",
			},
		},
		// output
		err: serverErrors.AuthorizationModelResolutionTooComplex,
	},
	{
		_name: "ExecuteWithExistingTupleKeyAndEmptyUserSetReturnsAllowed",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgapb.Userset{
				"admin": {},
			},
		}},
		tuples: []*openfgapb.TupleKey{{
			Object:   "repo:openfga/openfga",
			Relation: "admin",
			User:     "github|jose@openfga",
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "admin",
				User:     "github|jose@openfga",
			},
			Trace: true,
		},
		// output
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".(direct).",
		},
	},
	{
		_name: "ExecuteWithAllowAllTupleKeyAndEmptyUserSetReturnsAllowed",
		// state
		typeDefinitions: []*openfgapb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgapb.Userset{
				"admin": {},
			},
		}},
		tuples: []*openfgapb.TupleKey{{
			Object:   "repo:openfga/openfga",
			Relation: "admin",
			User:     "*",
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "admin",
				User:     "github|jose@openfga",
			},
			Trace: true,
		},
		// output
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".(direct).",
		},
	},
	{
		_name: "ExecuteWithNonExistingTupleKeyAndEmptyUserSetReturnsNotAllowed",
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
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "admin",
				User:     "github|jose@openfga",
			},
		},
		// output
		response: &openfgapb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name: "ExecuteWithUnionAndDirectUserSetReturnsAllowedIfDirectTupleExists",
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
		tuples: []*openfgapb.TupleKey{{
			Object:   "repo:openfga/openfga",
			Relation: "admin",
			User:     "github|jose@openfga",
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "admin",
				User:     "github|jose@openfga",
			},
			Trace: true,
		},
		// output
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.0(direct).",
		},
	},
	{
		_name: "ExecuteWithUnionAndDirectUserSetReturnsAllowedIfAllUsersTupleExists",
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
		tuples: []*openfgapb.TupleKey{{
			Object:   "repo:openfga/openfga",
			Relation: "admin",
			User:     "*",
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "admin",
				User:     "github|jose@openfga",
			},
			Trace: true,
		},
		// output
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.0(direct).",
		},
	},
	{
		_name: "ExecuteWithUnionAndComputedUserSetReturnsNotAllowedIfComputedUsersetDoesNotIncludeUser",
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
			{
				Object:   "repo:openfga/openfga",
				Relation: "admin",
				User:     "github|jose@openfga",
			},
			{
				Object:   "repo:openfga/openfga",
				Relation: "owner",
				User:     "team/iam",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "admin",
				User:     "github|jose@openfga",
			},
		},
		// output
		response: &openfgapb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name: "ExecuteWithUnionAndComputedUserSetReturnsAllowedIfComputedUsersetIncludesUser",
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
			{
				Object:   "repo:openfga/openfga",
				Relation: "writer",
				User:     "github|jose@openfga",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "reader",
				User:     "github|jose@openfga",
			},
			Trace: true,
		},
		// output
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:openfga/openfga#writer.(direct).",
		},
	},
	{
		_name: "ExecuteDirectSetReturnsAllowedIfUserHasRelationWithAnObjectThatHasUserAccessToTheTargetObject",
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
			{
				Object:   "team:iam",
				Relation: "team_member",
				User:     "github|jose@openfga",
			},
			{
				Object:   "repo:openfga/openfga",
				Relation: "admin",
				User:     "team:iam#team_member",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "admin",
				User:     "github|jose@openfga",
			},
			Trace: true,
		},
		// output
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".(direct).team:iam#team_member.(direct).",
		},
	},
	{
		_name: "ExecuteReturnsAllowedIfUserIsHasRelationToAnObjectThatIsInComputedUserSetForAnotherObject",
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
			{
				Object:   "team:iam",
				Relation: "team_member",
				User:     "github|jose@openfga",
			},
			{
				Object:   "repo:openfga/openfga",
				Relation: "writer",
				User:     "team:iam#team_member",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "reader",
				User:     "github|jose@openfga",
			},
			Trace: true,
		},
		// output
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:openfga/openfga#writer.(direct).team:iam#team_member.(direct).",
		},
	},
	{
		_name: "ExecuteReturnsNotAllowedIfIntersectionIsRequiredAndUserIsInOneUserSetButNotTheOther",
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
			{
				Object:   "openfga-store:yenkel-dev",
				Relation: "create_user",
				User:     "github|yenkel@openfga",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "openfga-store:yenkel-dev",
				Relation: "create_organization_user",
				User:     "github|yenkel@openfga",
			},
			Trace: true,
		},
		// output
		response: &openfgapb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name: "ExecuteReturnsAllowedIfIntersectionIsRequiredAndUserIsInAllUserSets",
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
			{
				Object:   "openfga-store:yenkel-dev",
				Relation: "create_user",
				User:     "github|yenkel@openfga",
			},
			{
				Object:   "openfga-store:yenkel-dev",
				Relation: "write_organization",
				User:     "github|yenkel@openfga",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "openfga-store:yenkel-dev",
				Relation: "create_organization_user",
				User:     "github|yenkel@openfga",
			},
			Trace: true,
		},
		// output
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".[.0(computed-userset).openfga-store:yenkel-dev#create_user.(direct).,.1(computed-userset).openfga-store:yenkel-dev#write_organization.(direct).]",
		},
	},
	{
		_name: "ExecuteSupportsNestedIntersectionAndCorrectlyTraces",
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
			{
				Object:   "openfga-store:yenkel-dev",
				Relation: "create_user_a",
				User:     "github|yenkel@openfga",
			},
			{
				Object:   "openfga-store:yenkel-dev",
				Relation: "create_user_b",
				User:     "github|yenkel@openfga",
			},
			{
				Object:   "openfga-store:yenkel-dev",
				Relation: "write_organization",
				User:     "github|yenkel@openfga",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "openfga-store:yenkel-dev",
				Relation: "create_organization_user",
				User:     "github|yenkel@openfga",
			},
			Trace: true,
		},
		// output
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".[.0(computed-userset).openfga-store:yenkel-dev#create_user.[.0(computed-userset).openfga-store:yenkel-dev#create_user.0(computed-userset).openfga-store:yenkel-dev#create_user_a.(direct).,.0(computed-userset).openfga-store:yenkel-dev#create_user.1(computed-userset).openfga-store:yenkel-dev#create_user_b.(direct).],.1(computed-userset).openfga-store:yenkel-dev#write_organization.(direct).]",
		},
	},
	{
		_name: "ExecuteReturnsAllowedForUserNotRemovedByDifference",
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
			{
				Object:   "repo:openfga/canaveral",
				Relation: "admin",
				User:     "github|jon.allie@openfga",
			},
			{
				Object:   "repo:openfga/canaveral",
				Relation: "banned",
				User:     "github|jose@openfga",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/canaveral",
				Relation: "admin",
				User:     "github|jon.allie@openfga",
			},
			Trace: true,
		},
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".0(direct).",
		},
	},
	{
		_name: "ExecuteReturnsNotAllowedForUserRemovedByDifference",
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
			{
				Object:   "repo:openfga/canaveral",
				Relation: "admin",
				User:     "github|jon.allie@openfga",
			},
			{
				Object:   "repo:openfga/canaveral",
				Relation: "banned",
				User:     "github|jon.allie@openfga",
			},
			{
				Object:   "repo:openfga/canaveral",
				Relation: "banned",
				User:     "github|jose@openfga",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/canaveral",
				Relation: "admin",
				User:     "github|jon.allie@openfga",
			},
			Trace: true,
		},
		response: &openfgapb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name: "ExecuteReturnsAllowedForTupleToUserset",
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
			{
				Object:   "repo:openfga/canaveral",
				Relation: "manager",
				User:     "org:openfga#repo_admin",
			},
			{
				Object:   "org:openfga",
				Relation: "repo_admin",
				User:     "github|jose@openfga",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/canaveral",
				Relation: "admin",
				User:     "github|jose@openfga",
			},
			Trace: true,
		},
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(tuple-to-userset).repo:openfga/canaveral#manager.org:openfga#repo_admin.(direct).",
		},
	},
	{
		_name: "ExecuteCanResolveRecursiveComputedUserSets",
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
			{
				Object:   "repo:openfga/openfga",
				Relation: "writer",
				User:     "team:openfga#member",
			},
			{
				Object:   "team:openfga",
				Relation: "member",
				User:     "github|iaco@openfga",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "repo:openfga/openfga",
				Relation: "reader",
				User:     "github|iaco@openfga",
			},
			Trace: true,
		},
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:openfga/openfga#triager.union.1(computed-userset).repo:openfga/openfga#writer.union.0(direct).team:openfga#member.(direct).",
		},
	},
	{
		_name: "ExecuteCanResolveRecursiveTupleToUserSets",
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
			{
				Object:   "document:octo_v2_draft",
				Relation: "parent",
				User:     "document:octo_folder",
			},
			{
				Object:   "document:octo_folder",
				Relation: "editor",
				User:     "google|iaco@openfga",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				Object:   "document:octo_v2_draft",
				Relation: "viewer",
				User:     "google|iaco@openfga",
			},
			Trace: true,
		},
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.2(tuple-to-userset).document:octo_v2_draft#parent.document:octo_folder#viewer.union.1(computed-userset).document:octo_folder#editor.union.0(direct).",
		},
	},
	{
		_name:                   "GitHubAssertion1",
		useGitHubTypeDefinition: true,
		tuples:                  gitHubTuples,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "anne",
				Relation: "reader",
				Object:   "repo:openfga/openfga",
			},
			Trace: true,
		},
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.0(direct).",
		},
	},
	{
		_name:                   "GitHubAssertion2",
		useGitHubTypeDefinition: true,
		tuples:                  gitHubTuples,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "anne",
				Relation: "triager",
				Object:   "repo:openfga/openfga",
			},
			Trace: true,
		},
		response: &openfgapb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name:                   "GitHubAssertion3",
		useGitHubTypeDefinition: true,
		tuples:                  gitHubTuples,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "diane",
				Relation: "admin",
				Object:   "repo:openfga/openfga",
			},
			Trace: true,
		},
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.0(direct).team:openfga/iam#member.(direct).team:openfga/protocols#member.(direct).",
		},
	},
	{
		_name:                   "GitHubAssertion4",
		useGitHubTypeDefinition: true,
		tuples:                  gitHubTuples,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "erik",
				Relation: "reader",
				Object:   "repo:openfga/openfga",
			},
			Trace: true,
		},
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:openfga/openfga#triager.union.1(computed-userset).repo:openfga/openfga#writer.union.1(computed-userset).repo:openfga/openfga#maintainer.union.1(computed-userset).repo:openfga/openfga#admin.union.1(tuple-to-userset).repo:openfga/openfga#owner.org:openfga#repo_admin.(direct).org:openfga#member.union.0(direct).",
		},
	},
	{
		_name:                   "GitHubAssertion5",
		useGitHubTypeDefinition: true,
		tuples:                  gitHubTuples,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "charles",
				Relation: "writer",
				Object:   "repo:openfga/openfga",
			},
			Trace: true,
		},
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:openfga/openfga#maintainer.union.1(computed-userset).repo:openfga/openfga#admin.union.0(direct).team:openfga/iam#member.(direct).",
		},
	},
	{
		_name:                   "GitHubAssertion6",
		useGitHubTypeDefinition: true,
		tuples:                  gitHubTuples,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "beth",
				Relation: "admin",
				Object:   "repo:openfga/openfga",
			},
			Trace: true,
		},
		response: &openfgapb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name:                   "RepeatedContextualTuplesShouldError",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "anne",
				Relation: "reader",
				Object:   "repo:openfga/openfga",
			},
			ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: []*openfgapb.TupleKey{
				{
					User:     "anne",
					Relation: "reader",
					Object:   "repo:openfga/openfga",
				},
				{
					User:     "anne",
					Relation: "reader",
					Object:   "repo:openfga/openfga",
				},
			}},
			Trace: true,
		},
		err: serverErrors.DuplicateContextualTuple(&openfgapb.TupleKey{
			User:     "anne",
			Relation: "reader",
			Object:   "repo:openfga/openfga",
		}),
	},
	{
		_name:                   "ContextualTuplesGitHubAssertion1",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "anne",
				Relation: "reader",
				Object:   "repo:openfga/openfga",
			},
			ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: gitHubTuples},
			Trace:            true,
		},
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.0(direct).",
		},
	},
	{
		_name:                   "ContextualTuplesGitHubAssertion2",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "anne",
				Relation: "triager",
				Object:   "repo:openfga/openfga",
			},
			ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: gitHubTuples},
			Trace:            true,
		},
		response: &openfgapb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name:                   "ContextualTuplesGitHubAssertion3",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "diane",
				Relation: "admin",
				Object:   "repo:openfga/openfga",
			},
			ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: gitHubTuples},
			Trace:            true,
		},
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.0(direct).team:openfga/iam#member.(direct).team:openfga/protocols#member.(direct).",
		},
	},
	{
		_name:                   "ContextualTuplesGitHubAssertion4",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "erik",
				Relation: "reader",
				Object:   "repo:openfga/openfga",
			},
			ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: gitHubTuples},
			Trace:            true,
		},
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:openfga/openfga#triager.union.1(computed-userset).repo:openfga/openfga#writer.union.1(computed-userset).repo:openfga/openfga#maintainer.union.1(computed-userset).repo:openfga/openfga#admin.union.1(tuple-to-userset).repo:openfga/openfga#owner.org:openfga#repo_admin.(direct).org:openfga#member.union.0(direct).",
		},
	},
	{
		_name:                   "ContextualTuplesGitHubAssertion5",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "charles",
				Relation: "writer",
				Object:   "repo:openfga/openfga",
			},
			ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: gitHubTuples},
			Trace:            true,
		},
		response: &openfgapb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:openfga/openfga#maintainer.union.1(computed-userset).repo:openfga/openfga#admin.union.0(direct).team:openfga/iam#member.(direct).",
		},
	},
	{
		_name:                   "ContextualTuplesGitHubAssertion6",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "beth",
				Relation: "admin",
				Object:   "repo:openfga/openfga",
			},
			ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: gitHubTuples},
			Trace:            true,
		},
		response: &openfgapb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name:                   "ContextualTuplesWithEmptyUserFails",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "beth",
				Relation: "admin",
				Object:   "repo:openfga/openfga",
			},
			ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				User:     "",
				Relation: "member",
				Object:   "org:openfga",
			}}},
			Trace: true,
		},
		err: serverErrors.InvalidContextualTuple(&openfgapb.TupleKey{User: "", Relation: "member", Object: "org:openfga"}),
	},
	{
		_name:                   "ContextualTuplesWithEmptyRelationFails",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "beth",
				Relation: "admin",
				Object:   "repo:openfga/openfga",
			},
			ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				User:     "anne",
				Relation: "",
				Object:   "org:openfga",
			}}},
			Trace: true,
		},
		err: serverErrors.InvalidContextualTuple(&openfgapb.TupleKey{User: "anne", Relation: "", Object: "org:openfga"}),
	},
	{
		_name:                   "ContextualTuplesWithEmptyObjectFails",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgapb.CheckRequest{
			TupleKey: &openfgapb.TupleKey{
				User:     "beth",
				Relation: "admin",
				Object:   "repo:openfga/openfga",
			},
			ContextualTuples: &openfgapb.ContextualTupleKeys{TupleKeys: []*openfgapb.TupleKey{{
				User:     "anne",
				Relation: "member",
				Object:   "",
			}}},
			Trace: true,
		},
		err: serverErrors.InvalidContextualTuple(&openfgapb.TupleKey{User: "anne", Relation: "member", Object: ""}),
	},
}

func TestCheckQuery(t *testing.T, datastore storage.OpenFGADatastore) {
	data, err := os.ReadFile(gitHubTestDataFile)
	if err != nil {
		t.Fatal(err)
	}
	var gitHubTypeDefinitions openfgapb.TypeDefinitions
	if err := protojson.Unmarshal(data, &gitHubTypeDefinitions); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	for _, test := range checkQueryTests {
		t.Run(test._name, func(t *testing.T) {
			store := testutils.CreateRandomString(20)
			modelID, err := id.NewString()
			if err != nil {
				t.Fatal(err)
			}
			if test.useGitHubTypeDefinition {
				err = datastore.WriteAuthorizationModel(ctx, store, modelID, &openfgapb.TypeDefinitions{TypeDefinitions: gitHubTypeDefinitions.GetTypeDefinitions()})
			} else {
				err = datastore.WriteAuthorizationModel(ctx, store, modelID, &openfgapb.TypeDefinitions{TypeDefinitions: test.typeDefinitions})
			}
			if err != nil {
				t.Fatalf("%s: WriteAuthorizationModel: err was %v, want nil", test._name, err)
			}

			if test.tuples != nil {
				if err := datastore.Write(ctx, store, nil, test.tuples); err != nil {
					t.Fatalf("[%s] failed to write test tuples: %v", test._name, err)
				}
			}

			cmd := commands.NewCheckQuery(datastore, tracer, telemetry.NewNoopMeter(), logger, test.resolveNodeLimit)
			test.request.StoreId = store
			test.request.AuthorizationModelId = modelID
			resp, gotErr := cmd.Execute(ctx, test.request)

			if test.err != nil && gotErr == nil {
				t.Fatalf("[%s] Expected error '%s', but got nil", test._name, test.err)
			}

			if test.err != nil && test.err.Error() != gotErr.Error() {
				t.Fatalf("[%s] Expected error '%s', got '%s'", test._name, test.err, gotErr)
			}

			if test.response != nil {
				if gotErr != nil {
					t.Fatalf("[%s] Expected no error but got '%s'", test._name, gotErr)
				}

				if test.response.Allowed != resp.Allowed {
					t.Fatalf("[%s] Expected allowed '%t', got '%t'", test._name, test.response.Allowed, resp.Allowed)
				}

				if test.response.Allowed {
					if test.response.Resolution != resp.Resolution {
						t.Errorf("[%s] Expected resolution '%s', got '%s'", test._name, test.response.Resolution, resp.Resolution)
					}
				}
			}
		})
	}
}

func TestCheckQueryAuthorizationModelsVersioning(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	originalTD := []*openfgapb.TypeDefinition{{
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
	}}

	updatedTD := []*openfgapb.TypeDefinition{{
		Type: "repo",
		Relations: map[string]*openfgapb.Userset{
			"owner":  {Userset: &openfgapb.Userset_This{}},
			"editor": {Userset: &openfgapb.Userset_This{}},
		},
	}}

	store := testutils.CreateRandomString(10)
	originalModelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}

	if err := datastore.WriteAuthorizationModel(ctx, store, originalModelID, &openfgapb.TypeDefinitions{TypeDefinitions: originalTD}); err != nil {
		t.Fatalf("%s: WriteAuthorizationModel: err was %v, want nil", originalTD, err)
	}

	updatedModelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}

	if err := datastore.WriteAuthorizationModel(ctx, store, updatedModelID, &openfgapb.TypeDefinitions{TypeDefinitions: updatedTD}); err != nil {
		t.Fatalf("%s: WriteAuthorizationModel: err was %v, want nil", updatedTD, err)
	}

	if err := datastore.Write(ctx, store, []*openfgapb.TupleKey{}, []*openfgapb.TupleKey{{Object: "repo:openfgapb", Relation: "owner", User: "yenkel"}}); err != nil {
		t.Fatalf("failed to write test tuple: %v", err)
	}

	originalCheckQuery := commands.NewCheckQuery(datastore, tracer, telemetry.NewNoopMeter(), logger, defaultResolveNodeLimit)
	originalNSResponse, err := originalCheckQuery.Execute(ctx, &openfgapb.CheckRequest{
		StoreId:              store,
		AuthorizationModelId: originalModelID,
		TupleKey: &openfgapb.TupleKey{
			Object:   "repo:openfgapb",
			Relation: "editor",
			User:     "yenkel",
		},
	})
	if err != nil {
		t.Fatalf("%s: NewCheckQuery: err was %v, want nil", updatedTD, err)
	}

	if originalNSResponse.Allowed {
		t.Errorf("[%s] Expected allowed '%t', actual '%t'", "originalNS", true, originalNSResponse.Allowed)
	}

	updatedCheckQuery := commands.NewCheckQuery(datastore, tracer, telemetry.NewNoopMeter(), logger, defaultResolveNodeLimit)
	updatedNSResponse, err := updatedCheckQuery.Execute(ctx, &openfgapb.CheckRequest{
		StoreId:              store,
		AuthorizationModelId: updatedModelID,
		TupleKey: &openfgapb.TupleKey{
			Object:   "repo:openfgapb",
			Relation: "editor",
			User:     "yenkel",
		},
	})
	if err != nil {
		t.Errorf("Unexpected error, got '%v' but expected nil", err)
	}

	if !updatedNSResponse.Allowed {
		t.Errorf("[%s] Expected allowed '%t', actual '%t'", "updatedNS", false, updatedNSResponse.Allowed)
	}
}

var tuples = []*openfgapb.TupleKey{
	{
		Object:   "repo:openfga/openfga",
		Relation: "reader",
		User:     "team:openfga#member",
	},
	{
		Object:   "team:openfga",
		Relation: "member",
		User:     "github|iaco@openfga",
	},
}

// Used to avoid compiler optimizations (see https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go)
var result *openfgapb.CheckResponse //nolint

func BenchmarkCheckWithoutTrace(b *testing.B, datastore storage.OpenFGADatastore) {

	data, err := os.ReadFile(gitHubTestDataFile)
	if err != nil {
		b.Fatal(err)
	}
	var gitHubTypeDefinitions openfgapb.TypeDefinitions
	if err := protojson.Unmarshal(data, &gitHubTypeDefinitions); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	store := testutils.CreateRandomString(10)

	modelID, err := id.NewString()
	if err != nil {
		b.Fatal(err)
	}
	err = datastore.WriteAuthorizationModel(ctx, store, modelID, &gitHubTypeDefinitions)
	if err != nil {
		b.Fatal(err)
	}
	err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, tuples)
	if err != nil {
		b.Fatal(err)
	}

	checkQuery := commands.NewCheckQuery(datastore, tracer, telemetry.NewNoopMeter(), logger, defaultResolveNodeLimit)

	var r *openfgapb.CheckResponse

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = checkQuery.Execute(ctx, &openfgapb.CheckRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
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
	data, err := os.ReadFile(gitHubTestDataFile)
	if err != nil {
		b.Fatal(err)
	}
	var gitHubTypeDefinitions openfgapb.TypeDefinitions
	if err := protojson.Unmarshal(data, &gitHubTypeDefinitions); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	store := testutils.CreateRandomString(10)

	modelID, err := id.NewString()
	if err != nil {
		b.Fatal(err)
	}
	err = datastore.WriteAuthorizationModel(ctx, store, modelID, &gitHubTypeDefinitions)
	if err != nil {
		b.Fatal(err)
	}
	err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, tuples)
	if err != nil {
		b.Fatal(err)
	}

	checkQuery := commands.NewCheckQuery(datastore, tracer, telemetry.NewNoopMeter(), logger, defaultResolveNodeLimit)

	var r *openfgapb.CheckResponse

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = checkQuery.Execute(ctx, &openfgapb.CheckRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
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
