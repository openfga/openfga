package test

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/queries"
	teststorage "github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultResolveNodeLimit = 25
	gitHubTestDataFile      = "../testdata/github.json"
)

var gitHubTuples = []*openfga.TupleKey{
	{
		User:     "erik",
		Relation: "member",
		Object:   "org:auth0",
	},
	{
		User:     "org:auth0#member",
		Relation: "repo_admin",
		Object:   "org:auth0",
	},
	{
		User:     "team:auth0/iam#member",
		Relation: "admin",
		Object:   "repo:auth0/express-jwt",
	},
	{
		User:     "org:auth0",
		Relation: "owner",
		Object:   "repo:auth0/express-jwt",
	},
	{
		User:     "anne",
		Relation: "reader",
		Object:   "repo:auth0/express-jwt",
	},
	{
		User:     "beth",
		Relation: "writer",
		Object:   "repo:auth0/express-jwt",
	},
	{
		User:     "charles",
		Relation: "member",
		Object:   "team:auth0/iam",
	},
	{
		User:     "team:auth0/protocols#member",
		Relation: "member",
		Object:   "team:auth0/iam",
	},
	{
		User:     "diane",
		Relation: "member",
		Object:   "team:auth0/protocols",
	},
}

type checkQueryTest struct {
	_name                   string
	useGitHubTypeDefinition bool
	typeDefinitions         []*openfgav1pb.TypeDefinition
	tuples                  []*openfga.TupleKey
	resolveNodeLimit        uint32
	request                 *openfgav1pb.CheckRequest
	err                     error
	response                *openfgav1pb.CheckResponse
}

var checkQueryTests = []checkQueryTest{
	{
		_name: "ExecuteWithEmptyTupleKey",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgav1pb.Userset{},
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{},
		},
		// output
		err: serverErrors.InvalidCheckInput,
	},
	{
		_name: "ExecuteWithEmptyObject",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgav1pb.Userset{},
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
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
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgav1pb.Userset{},
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object: "repo:auth0/express-jwt",
				User:   "someUser",
			},
		},
		// output
		err: serverErrors.InvalidCheckInput,
	},
	{
		_name: "ExecuteWithEmptyUser",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgav1pb.Userset{},
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Relation: "reader",
				Object:   "repo:auth0/express-jwt",
			},
		},
		// output
		err: serverErrors.InvalidCheckInput,
	},
	{
		_name: "ExecuteWithRequestRelationInexistentInTypeDefinition",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type:      "repo",
			Relations: map[string]*openfgav1pb.Userset{},
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "inexistent",
				User:     "someUser",
			},
		},
		// output
		err: serverErrors.RelationNotFound("inexistent", "repo", &openfga.TupleKey{
			Object:   "repo:auth0/express-jwt",
			Relation: "inexistent",
			User:     "someUser",
		}),
	},
	{
		_name: "ExecuteFailsWithInvalidUser",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"admin": {},
			},
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
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
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"reader": {
					Userset: &openfgav1pb.Userset_ComputedUserset{
						ComputedUserset: &openfgav1pb.ObjectRelation{
							Relation: "writer",
						},
					}},
				"writer": {
					Userset: &openfgav1pb.Userset_ComputedUserset{
						ComputedUserset: &openfgav1pb.ObjectRelation{
							Relation: "reader",
						},
					}},
			},
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
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
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"reader": {
					Userset: &openfgav1pb.Userset_This{},
				},
				"writer": {
					Userset: &openfgav1pb.Userset_ComputedUserset{
						ComputedUserset: &openfgav1pb.ObjectRelation{
							Relation: "reader",
						},
					}},
			},
		}},
		resolveNodeLimit: 2,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
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
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"writer": {
					Userset: &openfgav1pb.Userset_This{},
				},
				"reader": {
					Userset: &openfgav1pb.Userset_Union{
						Union: &openfgav1pb.Usersets{
							Child: []*openfgav1pb.Userset{
								{
									Userset: &openfgav1pb.Userset_This{},
								},
								{
									Userset: &openfgav1pb.Userset_ComputedUserset{
										ComputedUserset: &openfgav1pb.ObjectRelation{
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
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
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
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"admin": {},
			},
		}},
		tuples: []*openfga.TupleKey{{
			Object:   "repo:auth0/express-jwt",
			Relation: "admin",
			User:     "github|jose@auth0.com",
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
				User:     "github|jose@auth0.com",
			},
			Trace: true,
		},
		// output
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".(direct).",
		},
	},
	{
		_name: "ExecuteWithAllowAllTupleKeyAndEmptyUserSetReturnsAllowed",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"admin": {},
			},
		}},
		tuples: []*openfga.TupleKey{{
			Object:   "repo:auth0/express-jwt",
			Relation: "admin",
			User:     "*",
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
				User:     "github|jose@auth0.com",
			},
			Trace: true,
		},
		// output
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".(direct).",
		},
	},
	{
		_name: "ExecuteWithNonExistingTupleKeyAndEmptyUserSetReturnsNotAllowed",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"admin": {},
			},
		}},
		tuples:           []*openfga.TupleKey{},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
				User:     "github|jose@auth0.com",
			},
		},
		// output
		response: &openfgav1pb.CheckResponse{
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
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"admin": {
					Userset: &openfgav1pb.Userset_Union{
						Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
							{Userset: &openfgav1pb.Userset_This{
								This: &openfgav1pb.DirectUserset{},
							}},
						}},
					},
				},
			},
		}},
		tuples: []*openfga.TupleKey{{
			Object:   "repo:auth0/express-jwt",
			Relation: "admin",
			User:     "github|jose@auth0.com",
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
				User:     "github|jose@auth0.com",
			},
			Trace: true,
		},
		// output
		response: &openfgav1pb.CheckResponse{
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
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"admin": {
					Userset: &openfgav1pb.Userset_Union{
						Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
							{Userset: &openfgav1pb.Userset_This{
								This: &openfgav1pb.DirectUserset{},
							}},
						}},
					},
				},
			},
		}},
		tuples: []*openfga.TupleKey{{
			Object:   "repo:auth0/express-jwt",
			Relation: "admin",
			User:     "*",
		}},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
				User:     "github|jose@auth0.com",
			},
			Trace: true,
		},
		// output
		response: &openfgav1pb.CheckResponse{
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
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"admin": {
					Userset: &openfgav1pb.Userset_Union{
						Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
							{Userset: &openfgav1pb.Userset_ComputedUserset{
								ComputedUserset: &openfgav1pb.ObjectRelation{
									Relation: "owner",
								},
							}},
						}},
					},
				},
				"owner": {},
			},
		}},
		tuples: []*openfga.TupleKey{
			{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
				User:     "github|jose@auth0.com",
			},
			{
				Object:   "repo:auth0/express-jwt",
				Relation: "owner",
				User:     "team/iam",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
				User:     "github|jose@auth0.com",
			},
		},
		// output
		response: &openfgav1pb.CheckResponse{
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
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"reader": {
					Userset: &openfgav1pb.Userset_Union{
						Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
							{Userset: &openfgav1pb.Userset_This{}},
							{Userset: &openfgav1pb.Userset_ComputedUserset{
								ComputedUserset: &openfgav1pb.ObjectRelation{
									Relation: "writer",
								},
							}},
						}},
					},
				},
				"writer": {},
			},
		}},
		tuples: []*openfga.TupleKey{
			{
				Object:   "repo:auth0/express-jwt",
				Relation: "writer",
				User:     "github|jose@auth0.com",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "reader",
				User:     "github|jose@auth0.com",
			},
			Trace: true,
		},
		// output
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:auth0/express-jwt#writer.(direct).",
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
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"admin": {
					Userset: &openfgav1pb.Userset_This{},
				},
			},
		}, {
			Type: "team",
			Relations: map[string]*openfgav1pb.Userset{
				"team_member": {},
			},
		}},
		tuples: []*openfga.TupleKey{
			{
				Object:   "team:iam",
				Relation: "team_member",
				User:     "github|jose@auth0.com",
			},
			{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
				User:     "team:iam#team_member",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
				User:     "github|jose@auth0.com",
			},
			Trace: true,
		},
		// output
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".(direct).team:iam#team_member.(direct).",
		},
	},
	{
		_name: "ExecuteReturnsAllowedIfUserIsHasRelationToAnObjectThatIsInComputedUserSetForAnotherObject",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "repo",
			Relations: map[string]*openfgav1pb.Userset{
				"reader": {
					Userset: &openfgav1pb.Userset_Union{
						Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
							{Userset: &openfgav1pb.Userset_This{}},
							{Userset: &openfgav1pb.Userset_ComputedUserset{
								ComputedUserset: &openfgav1pb.ObjectRelation{
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
			Relations: map[string]*openfgav1pb.Userset{
				"team_member": {},
			},
		}},
		tuples: []*openfga.TupleKey{
			{
				Object:   "team:iam",
				Relation: "team_member",
				User:     "github|jose@auth0.com",
			},
			{
				Object:   "repo:auth0/express-jwt",
				Relation: "writer",
				User:     "team:iam#team_member",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "reader",
				User:     "github|jose@auth0.com",
			},
			Trace: true,
		},
		// output
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:auth0/express-jwt#writer.(direct).team:iam#team_member.(direct).",
		},
	},
	{
		_name: "ExecuteReturnsNotAllowedIfIntersectionIsRequiredAndUserIsInOneUserSetButNotTheOther",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "auth0-store",
			// pretend you can only create an organization user in an auth0 store if
			// you can create a user AND write an organization in a store
			Relations: map[string]*openfgav1pb.Userset{
				"create_organization_user": {
					Userset: &openfgav1pb.Userset_Intersection{
						Intersection: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
							{Userset: &openfgav1pb.Userset_ComputedUserset{
								ComputedUserset: &openfgav1pb.ObjectRelation{
									Relation: "create_user",
								},
							}},
							{Userset: &openfgav1pb.Userset_ComputedUserset{
								ComputedUserset: &openfgav1pb.ObjectRelation{
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
		tuples: []*openfga.TupleKey{
			{
				Object:   "auth0-store:yenkel-dev",
				Relation: "create_user",
				User:     "github|yenkel@auth0.com",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "auth0-store:yenkel-dev",
				Relation: "create_organization_user",
				User:     "github|yenkel@auth0.com",
			},
			Trace: true,
		},
		// output
		response: &openfgav1pb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name: "ExecuteReturnsAllowedIfIntersectionIsRequiredAndUserIsInAllUserSets",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "auth0-store",
			// pretend you can only create an organization user in an auth0 store if
			// you can create a user AND write an organization in a store
			Relations: map[string]*openfgav1pb.Userset{
				"create_organization_user": {
					Userset: &openfgav1pb.Userset_Intersection{
						Intersection: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
							{Userset: &openfgav1pb.Userset_ComputedUserset{
								ComputedUserset: &openfgav1pb.ObjectRelation{
									Relation: "create_user",
								},
							}},
							{Userset: &openfgav1pb.Userset_ComputedUserset{
								ComputedUserset: &openfgav1pb.ObjectRelation{
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
		tuples: []*openfga.TupleKey{
			{
				Object:   "auth0-store:yenkel-dev",
				Relation: "create_user",
				User:     "github|yenkel@auth0.com",
			},
			{
				Object:   "auth0-store:yenkel-dev",
				Relation: "write_organization",
				User:     "github|yenkel@auth0.com",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "auth0-store:yenkel-dev",
				Relation: "create_organization_user",
				User:     "github|yenkel@auth0.com",
			},
			Trace: true,
		},
		// output
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".[.0(computed-userset).auth0-store:yenkel-dev#create_user.(direct).,.1(computed-userset).auth0-store:yenkel-dev#write_organization.(direct).]",
		},
	},
	{
		_name: "ExecuteSupportsNestedIntersectionAndCorrectlyTraces",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{{
			Type: "auth0-store",
			// pretend you can only create an organization user in an auth0 store if
			// you can create a user AND write an organization in a store
			Relations: map[string]*openfgav1pb.Userset{
				"create_organization_user": {
					Userset: &openfgav1pb.Userset_Intersection{
						Intersection: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
							{Userset: &openfgav1pb.Userset_ComputedUserset{
								ComputedUserset: &openfgav1pb.ObjectRelation{
									Relation: "create_user",
								},
							}},
							{Userset: &openfgav1pb.Userset_ComputedUserset{
								ComputedUserset: &openfgav1pb.ObjectRelation{
									Relation: "write_organization",
								},
							}},
						}},
					},
				},
				"create_user": {
					Userset: &openfgav1pb.Userset_Intersection{
						Intersection: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
							{Userset: &openfgav1pb.Userset_ComputedUserset{
								ComputedUserset: &openfgav1pb.ObjectRelation{
									Relation: "create_user_a",
								},
							}},
							{Userset: &openfgav1pb.Userset_ComputedUserset{
								ComputedUserset: &openfgav1pb.ObjectRelation{
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
		tuples: []*openfga.TupleKey{
			{
				Object:   "auth0-store:yenkel-dev",
				Relation: "create_user_a",
				User:     "github|yenkel@auth0.com",
			},
			{
				Object:   "auth0-store:yenkel-dev",
				Relation: "create_user_b",
				User:     "github|yenkel@auth0.com",
			},
			{
				Object:   "auth0-store:yenkel-dev",
				Relation: "write_organization",
				User:     "github|yenkel@auth0.com",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		// input
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "auth0-store:yenkel-dev",
				Relation: "create_organization_user",
				User:     "github|yenkel@auth0.com",
			},
			Trace: true,
		},
		// output
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".[.0(computed-userset).auth0-store:yenkel-dev#create_user.[.0(computed-userset).auth0-store:yenkel-dev#create_user.0(computed-userset).auth0-store:yenkel-dev#create_user_a.(direct).,.0(computed-userset).auth0-store:yenkel-dev#create_user.1(computed-userset).auth0-store:yenkel-dev#create_user_b.(direct).],.1(computed-userset).auth0-store:yenkel-dev#write_organization.(direct).]",
		},
	},
	{
		_name: "ExecuteReturnsAllowedForUserNotRemovedByDifference",
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {
						Userset: &openfgav1pb.Userset_Difference{
							Difference: &openfgav1pb.Difference{
								Base: &openfgav1pb.Userset{
									Userset: &openfgav1pb.Userset_This{},
								},
								Subtract: &openfgav1pb.Userset{
									Userset: &openfgav1pb.Userset_ComputedUserset{
										ComputedUserset: &openfgav1pb.ObjectRelation{
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
		tuples: []*openfga.TupleKey{
			{
				Object:   "repo:auth0/canaveral",
				Relation: "admin",
				User:     "github|jon.allie@auth0.com",
			},
			{
				Object:   "repo:auth0/canaveral",
				Relation: "banned",
				User:     "github|jose@auth0.com",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/canaveral",
				Relation: "admin",
				User:     "github|jon.allie@auth0.com",
			},
			Trace: true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".0(direct).",
		},
	},
	{
		_name: "ExecuteReturnsNotAllowedForUserRemovedByDifference",
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {
						Userset: &openfgav1pb.Userset_Difference{
							Difference: &openfgav1pb.Difference{
								Base: &openfgav1pb.Userset{
									Userset: &openfgav1pb.Userset_This{},
								},
								Subtract: &openfgav1pb.Userset{
									Userset: &openfgav1pb.Userset_ComputedUserset{
										ComputedUserset: &openfgav1pb.ObjectRelation{
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
		tuples: []*openfga.TupleKey{
			{
				Object:   "repo:auth0/canaveral",
				Relation: "admin",
				User:     "github|jon.allie@auth0.com",
			},
			{
				Object:   "repo:auth0/canaveral",
				Relation: "banned",
				User:     "github|jon.allie@auth0.com",
			},
			{
				Object:   "repo:auth0/canaveral",
				Relation: "banned",
				User:     "github|jose@auth0.com",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/canaveral",
				Relation: "admin",
				User:     "github|jon.allie@auth0.com",
			},
			Trace: true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name: "ExecuteReturnsAllowedForTupleToUserset",
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {
						Userset: &openfgav1pb.Userset_Union{
							Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
								{Userset: &openfgav1pb.Userset_This{}},
								{Userset: &openfgav1pb.Userset_TupleToUserset{TupleToUserset: &openfgav1pb.TupleToUserset{
									Tupleset: &openfgav1pb.ObjectRelation{
										Relation: "manager",
									},
									ComputedUserset: &openfgav1pb.ObjectRelation{
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
				Relations: map[string]*openfgav1pb.Userset{
					// implicit direct?
					"repo_admin": {},
				},
			},
		},
		tuples: []*openfga.TupleKey{
			{
				Object:   "repo:auth0/canaveral",
				Relation: "manager",
				User:     "org:auth0#repo_admin",
			},
			{
				Object:   "org:auth0",
				Relation: "repo_admin",
				User:     "github|jose@auth0.com",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/canaveral",
				Relation: "admin",
				User:     "github|jose@auth0.com",
			},
			Trace: true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(tuple-to-userset).repo:auth0/canaveral#manager.org:auth0#repo_admin.(direct).",
		},
	},
	{
		_name: "ExecuteCanResolveRecursiveComputedUserSets",
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {
						Userset: &openfgav1pb.Userset_Union{
							Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
								{Userset: &openfgav1pb.Userset_This{}},
								{Userset: &openfgav1pb.Userset_TupleToUserset{TupleToUserset: &openfgav1pb.TupleToUserset{
									Tupleset: &openfgav1pb.ObjectRelation{
										Relation: "manager",
									},
									ComputedUserset: &openfgav1pb.ObjectRelation{
										Object:   "$TUPLE_USERSET_OBJECT",
										Relation: "repo_admin",
									},
								}}},
							}},
						},
					},
					"maintainer": {
						Userset: &openfgav1pb.Userset_Union{
							Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
								{Userset: &openfgav1pb.Userset_This{}},
								{Userset: &openfgav1pb.Userset_ComputedUserset{ComputedUserset: &openfgav1pb.ObjectRelation{
									Relation: "admin",
								}}},
							}},
						},
					},
					"writer": {
						Userset: &openfgav1pb.Userset_Union{
							Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
								{Userset: &openfgav1pb.Userset_This{}},
								{Userset: &openfgav1pb.Userset_ComputedUserset{ComputedUserset: &openfgav1pb.ObjectRelation{
									Relation: "maintainer",
								}}},
								{Userset: &openfgav1pb.Userset_TupleToUserset{TupleToUserset: &openfgav1pb.TupleToUserset{
									Tupleset: &openfgav1pb.ObjectRelation{
										Relation: "manager",
									},
									ComputedUserset: &openfgav1pb.ObjectRelation{
										Object:   "$TUPLE_USERSET_OBJECT",
										Relation: "repo_writer",
									},
								}}},
							}},
						},
					},
					"triager": {
						Userset: &openfgav1pb.Userset_Union{
							Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
								{Userset: &openfgav1pb.Userset_This{}},
								{Userset: &openfgav1pb.Userset_ComputedUserset{ComputedUserset: &openfgav1pb.ObjectRelation{
									Relation: "writer",
								}}},
							}},
						},
					},
					"reader": {
						Userset: &openfgav1pb.Userset_Union{
							Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
								{Userset: &openfgav1pb.Userset_This{}},
								{Userset: &openfgav1pb.Userset_ComputedUserset{ComputedUserset: &openfgav1pb.ObjectRelation{
									Relation: "triager",
								}}},
								{Userset: &openfgav1pb.Userset_TupleToUserset{TupleToUserset: &openfgav1pb.TupleToUserset{
									Tupleset: &openfgav1pb.ObjectRelation{
										Relation: "manager",
									},
									ComputedUserset: &openfgav1pb.ObjectRelation{
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
				Relations: map[string]*openfgav1pb.Userset{
					"member": {},
				},
			},
		},
		tuples: []*openfga.TupleKey{
			{
				Object:   "repo:auth0/express-jwt",
				Relation: "writer",
				User:     "team:auth0#member",
			},
			{
				Object:   "team:auth0",
				Relation: "member",
				User:     "github|iaco@auth0.com",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "reader",
				User:     "github|iaco@auth0.com",
			},
			Trace: true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:auth0/express-jwt#triager.union.1(computed-userset).repo:auth0/express-jwt#writer.union.0(direct).team:auth0#member.(direct).",
		},
	},
	{
		_name: "ExecuteCanResolveRecursiveTupleToUserSets",
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "document",
				Relations: map[string]*openfgav1pb.Userset{
					"parent": {Userset: &openfgav1pb.Userset_This{}},
					"owner":  {Userset: &openfgav1pb.Userset_This{}},
					"editor": {
						Userset: &openfgav1pb.Userset_Union{
							Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
								{Userset: &openfgav1pb.Userset_This{}},
								{Userset: &openfgav1pb.Userset_ComputedUserset{ComputedUserset: &openfgav1pb.ObjectRelation{
									Relation: "owner",
								}}},
							}},
						},
					},
					"viewer": {
						Userset: &openfgav1pb.Userset_Union{
							Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
								{Userset: &openfgav1pb.Userset_This{}},
								{Userset: &openfgav1pb.Userset_ComputedUserset{ComputedUserset: &openfgav1pb.ObjectRelation{
									Relation: "editor",
								}}},
								{Userset: &openfgav1pb.Userset_TupleToUserset{TupleToUserset: &openfgav1pb.TupleToUserset{
									Tupleset: &openfgav1pb.ObjectRelation{
										Relation: "parent",
									},
									ComputedUserset: &openfgav1pb.ObjectRelation{
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
		tuples: []*openfga.TupleKey{
			{
				Object:   "document:octo_v2_draft",
				Relation: "parent",
				User:     "document:octo_folder",
			},
			{
				Object:   "document:octo_folder",
				Relation: "editor",
				User:     "google|iaco@auth0.com",
			},
		},
		resolveNodeLimit: defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "document:octo_v2_draft",
				Relation: "viewer",
				User:     "google|iaco@auth0.com",
			},
			Trace: true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.2(tuple-to-userset).document:octo_v2_draft#parent.document:octo_folder#viewer.union.1(computed-userset).document:octo_folder#editor.union.0(direct).",
		},
	},
	{
		_name:                   "GitHubAssertion1",
		useGitHubTypeDefinition: true,
		tuples:                  gitHubTuples,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "anne",
				Relation: "reader",
				Object:   "repo:auth0/express-jwt",
			},
			Trace: true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.0(direct).",
		},
	},
	{
		_name:                   "GitHubAssertion2",
		useGitHubTypeDefinition: true,
		tuples:                  gitHubTuples,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "anne",
				Relation: "triager",
				Object:   "repo:auth0/express-jwt",
			},
			Trace: true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name:                   "GitHubAssertion3",
		useGitHubTypeDefinition: true,
		tuples:                  gitHubTuples,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "diane",
				Relation: "admin",
				Object:   "repo:auth0/express-jwt",
			},
			Trace: true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.0(direct).team:auth0/iam#member.(direct).team:auth0/protocols#member.(direct).",
		},
	},
	{
		_name:                   "GitHubAssertion4",
		useGitHubTypeDefinition: true,
		tuples:                  gitHubTuples,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "erik",
				Relation: "reader",
				Object:   "repo:auth0/express-jwt",
			},
			Trace: true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:auth0/express-jwt#triager.union.1(computed-userset).repo:auth0/express-jwt#writer.union.1(computed-userset).repo:auth0/express-jwt#maintainer.union.1(computed-userset).repo:auth0/express-jwt#admin.union.1(tuple-to-userset).repo:auth0/express-jwt#owner.org:auth0#repo_admin.(direct).org:auth0#member.union.0(direct).",
		},
	},
	{
		_name:                   "GitHubAssertion5",
		useGitHubTypeDefinition: true,
		tuples:                  gitHubTuples,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "charles",
				Relation: "writer",
				Object:   "repo:auth0/express-jwt",
			},
			Trace: true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:auth0/express-jwt#maintainer.union.1(computed-userset).repo:auth0/express-jwt#admin.union.0(direct).team:auth0/iam#member.(direct).",
		},
	},
	{
		_name:                   "GitHubAssertion6",
		useGitHubTypeDefinition: true,
		tuples:                  gitHubTuples,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "beth",
				Relation: "admin",
				Object:   "repo:auth0/express-jwt",
			},
			Trace: true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name:                   "RepeatedContextualTuplesShouldError",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "anne",
				Relation: "reader",
				Object:   "repo:auth0/express-jwt",
			},
			ContextualTuples: &openfga.ContextualTupleKeys{TupleKeys: []*openfga.TupleKey{
				{
					User:     "anne",
					Relation: "reader",
					Object:   "repo:auth0/express-jwt",
				},
				{
					User:     "anne",
					Relation: "reader",
					Object:   "repo:auth0/express-jwt",
				},
			}},
			Trace: true,
		},
		err: serverErrors.DuplicateContextualTuple(&openfga.TupleKey{
			User:     "anne",
			Relation: "reader",
			Object:   "repo:auth0/express-jwt",
		}),
	},
	{
		_name:                   "ContextualTuplesGitHubAssertion1",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "anne",
				Relation: "reader",
				Object:   "repo:auth0/express-jwt",
			},
			ContextualTuples: &openfga.ContextualTupleKeys{TupleKeys: gitHubTuples},
			Trace:            true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.0(direct).",
		},
	},
	{
		_name:                   "ContextualTuplesGitHubAssertion2",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "anne",
				Relation: "triager",
				Object:   "repo:auth0/express-jwt",
			},
			ContextualTuples: &openfga.ContextualTupleKeys{TupleKeys: gitHubTuples},
			Trace:            true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name:                   "ContextualTuplesGitHubAssertion3",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "diane",
				Relation: "admin",
				Object:   "repo:auth0/express-jwt",
			},
			ContextualTuples: &openfga.ContextualTupleKeys{TupleKeys: gitHubTuples},
			Trace:            true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.0(direct).team:auth0/iam#member.(direct).team:auth0/protocols#member.(direct).",
		},
	},
	{
		_name:                   "ContextualTuplesGitHubAssertion4",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "erik",
				Relation: "reader",
				Object:   "repo:auth0/express-jwt",
			},
			ContextualTuples: &openfga.ContextualTupleKeys{TupleKeys: gitHubTuples},
			Trace:            true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:auth0/express-jwt#triager.union.1(computed-userset).repo:auth0/express-jwt#writer.union.1(computed-userset).repo:auth0/express-jwt#maintainer.union.1(computed-userset).repo:auth0/express-jwt#admin.union.1(tuple-to-userset).repo:auth0/express-jwt#owner.org:auth0#repo_admin.(direct).org:auth0#member.union.0(direct).",
		},
	},
	{
		_name:                   "ContextualTuplesGitHubAssertion5",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "charles",
				Relation: "writer",
				Object:   "repo:auth0/express-jwt",
			},
			ContextualTuples: &openfga.ContextualTupleKeys{TupleKeys: gitHubTuples},
			Trace:            true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed:    true,
			Resolution: ".union.1(computed-userset).repo:auth0/express-jwt#maintainer.union.1(computed-userset).repo:auth0/express-jwt#admin.union.0(direct).team:auth0/iam#member.(direct).",
		},
	},
	{
		_name:                   "ContextualTuplesGitHubAssertion6",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "beth",
				Relation: "admin",
				Object:   "repo:auth0/express-jwt",
			},
			ContextualTuples: &openfga.ContextualTupleKeys{TupleKeys: gitHubTuples},
			Trace:            true,
		},
		response: &openfgav1pb.CheckResponse{
			Allowed: false,
		},
	},
	{
		_name:                   "ContextualTuplesWithEmptyUserFails",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "beth",
				Relation: "admin",
				Object:   "repo:auth0/express-jwt",
			},
			ContextualTuples: &openfga.ContextualTupleKeys{TupleKeys: []*openfga.TupleKey{{
				User:     "",
				Relation: "member",
				Object:   "org:auth0",
			}}},
			Trace: true,
		},
		err: serverErrors.InvalidContextualTuple(&openfga.TupleKey{User: "", Relation: "member", Object: "org:auth0"}),
	},
	{
		_name:                   "ContextualTuplesWithEmptyRelationFails",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "beth",
				Relation: "admin",
				Object:   "repo:auth0/express-jwt",
			},
			ContextualTuples: &openfga.ContextualTupleKeys{TupleKeys: []*openfga.TupleKey{{
				User:     "anne",
				Relation: "",
				Object:   "org:auth0",
			}}},
			Trace: true,
		},
		err: serverErrors.InvalidContextualTuple(&openfga.TupleKey{User: "anne", Relation: "", Object: "org:auth0"}),
	},
	{
		_name:                   "ContextualTuplesWithEmptyObjectFails",
		useGitHubTypeDefinition: true,
		resolveNodeLimit:        defaultResolveNodeLimit,
		request: &openfgav1pb.CheckRequest{
			TupleKey: &openfga.TupleKey{
				User:     "beth",
				Relation: "admin",
				Object:   "repo:auth0/express-jwt",
			},
			ContextualTuples: &openfga.ContextualTupleKeys{TupleKeys: []*openfga.TupleKey{{
				User:     "anne",
				Relation: "member",
				Object:   "",
			}}},
			Trace: true,
		},
		err: serverErrors.InvalidContextualTuple(&openfga.TupleKey{User: "anne", Relation: "member", Object: ""}),
	},
}

func TestCheckQuery(t *testing.T, dbTester teststorage.DatastoreTester) {
	data, err := ioutil.ReadFile(gitHubTestDataFile)
	if err != nil {
		t.Fatal(err)
	}
	var gitHubTypeDefinitions openfgav1pb.TypeDefinitions
	if err := protojson.Unmarshal(data, &gitHubTypeDefinitions); err != nil {
		t.Fatal(err)
	}

	require := require.New(t)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	for _, test := range checkQueryTests {
		t.Run(test._name, func(t *testing.T) {
			store := testutils.CreateRandomString(20)
			modelID, err := id.NewString()
			if err != nil {
				t.Fatal(err)
			}
			if test.useGitHubTypeDefinition {
				err = datastore.WriteAuthorizationModel(ctx, store, modelID, &openfgav1pb.TypeDefinitions{TypeDefinitions: gitHubTypeDefinitions.GetTypeDefinitions()})
			} else {
				err = datastore.WriteAuthorizationModel(ctx, store, modelID, &openfgav1pb.TypeDefinitions{TypeDefinitions: test.typeDefinitions})
			}
			if err != nil {
				t.Fatalf("%s: WriteAuthorizationModel: err was %v, want nil", test._name, err)
			}

			if test.tuples != nil {
				if err := datastore.Write(ctx, store, nil, test.tuples); err != nil {
					t.Fatalf("[%s] failed to write test tuples: %v", test._name, err)
				}
			}

			cmd := queries.NewCheckQuery(datastore, datastore, tracer, telemetry.NewNoopMeter(), logger, test.resolveNodeLimit)
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

func TestCheckQueryAuthorizationModelsVersioning(t *testing.T, dbTester teststorage.DatastoreTester) {
	require := require.New(t)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	originalTD := []*openfgav1pb.TypeDefinition{{
		Type: "repo",
		Relations: map[string]*openfgav1pb.Userset{
			"owner": {Userset: &openfgav1pb.Userset_This{}},
			"editor": {
				Userset: &openfgav1pb.Userset_Union{
					Union: &openfgav1pb.Usersets{Child: []*openfgav1pb.Userset{
						{Userset: &openfgav1pb.Userset_This{}},
						{Userset: &openfgav1pb.Userset_ComputedUserset{ComputedUserset: &openfgav1pb.ObjectRelation{Relation: "owner"}}},
					}},
				},
			},
		},
	}}

	updatedTD := []*openfgav1pb.TypeDefinition{{
		Type: "repo",
		Relations: map[string]*openfgav1pb.Userset{
			"owner":  {Userset: &openfgav1pb.Userset_This{}},
			"editor": {Userset: &openfgav1pb.Userset_This{}},
		},
	}}

	store := testutils.CreateRandomString(10)
	originalModelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}

	if err := datastore.WriteAuthorizationModel(ctx, store, originalModelID, &openfgav1pb.TypeDefinitions{TypeDefinitions: originalTD}); err != nil {
		t.Fatalf("%s: WriteAuthorizationModel: err was %v, want nil", originalTD, err)
	}

	updatedModelID, err := id.NewString()
	if err != nil {
		t.Fatal(err)
	}

	if err := datastore.WriteAuthorizationModel(ctx, store, updatedModelID, &openfgav1pb.TypeDefinitions{TypeDefinitions: updatedTD}); err != nil {
		t.Fatalf("%s: WriteAuthorizationModel: err was %v, want nil", updatedTD, err)
	}

	if err := datastore.Write(ctx, store, []*openfga.TupleKey{}, []*openfga.TupleKey{{Object: "repo:openfga", Relation: "owner", User: "yenkel"}}); err != nil {
		t.Fatalf("failed to write test tuple: %v", err)
	}

	originalCheckQuery := queries.NewCheckQuery(datastore, datastore, tracer, telemetry.NewNoopMeter(), logger, defaultResolveNodeLimit)
	originalNSResponse, err := originalCheckQuery.Execute(ctx, &openfgav1pb.CheckRequest{
		StoreId:              store,
		AuthorizationModelId: originalModelID,
		TupleKey: &openfga.TupleKey{
			Object:   "repo:openfga",
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

	updatedCheckQuery := queries.NewCheckQuery(datastore, datastore, tracer, telemetry.NewNoopMeter(), logger, defaultResolveNodeLimit)
	updatedNSResponse, err := updatedCheckQuery.Execute(ctx, &openfgav1pb.CheckRequest{
		StoreId:              store,
		AuthorizationModelId: updatedModelID,
		TupleKey: &openfga.TupleKey{
			Object:   "repo:openfga",
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

var tuples = []*openfga.TupleKey{
	{
		Object:   "repo:auth0/express-jwt",
		Relation: "reader",
		User:     "team:auth0#member",
	},
	{
		Object:   "team:auth0",
		Relation: "member",
		User:     "github|iaco@auth0.com",
	},
}

// Used to avoid compiler optimizations (see https://dave.cheney.net/2013/06/30/how-to-write-benchmarks-in-go)
var result *openfgav1pb.CheckResponse //nolint

func BenchmarkCheckWithoutTrace(b *testing.B, dbTester teststorage.DatastoreTester) {

	data, err := ioutil.ReadFile(gitHubTestDataFile)
	if err != nil {
		b.Fatal(err)
	}
	var gitHubTypeDefinitions openfgav1pb.TypeDefinitions
	if err := protojson.Unmarshal(data, &gitHubTypeDefinitions); err != nil {
		b.Fatal(err)
	}

	require := require.New(b)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	store := testutils.CreateRandomString(10)

	modelID, err := id.NewString()
	if err != nil {
		b.Fatal(err)
	}
	err = datastore.WriteAuthorizationModel(ctx, store, modelID, &gitHubTypeDefinitions)
	if err != nil {
		b.Fatal(err)
	}
	err = datastore.Write(ctx, store, []*openfga.TupleKey{}, tuples)
	if err != nil {
		b.Fatal(err)
	}

	checkQuery := queries.NewCheckQuery(datastore, datastore, tracer, telemetry.NewNoopMeter(), logger, defaultResolveNodeLimit)

	var r *openfgav1pb.CheckResponse

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = checkQuery.Execute(ctx, &openfgav1pb.CheckRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "reader",
				User:     "github|iaco@auth0.com",
			},
		})
	}

	result = r
}

func BenchmarkWithTrace(b *testing.B, dbTester teststorage.DatastoreTester) {
	data, err := ioutil.ReadFile(gitHubTestDataFile)
	if err != nil {
		b.Fatal(err)
	}
	var gitHubTypeDefinitions openfgav1pb.TypeDefinitions
	if err := protojson.Unmarshal(data, &gitHubTypeDefinitions); err != nil {
		b.Fatal(err)
	}

	require := require.New(b)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	store := testutils.CreateRandomString(10)

	modelID, err := id.NewString()
	if err != nil {
		b.Fatal(err)
	}
	err = datastore.WriteAuthorizationModel(ctx, store, modelID, &gitHubTypeDefinitions)
	if err != nil {
		b.Fatal(err)
	}
	err = datastore.Write(ctx, store, []*openfga.TupleKey{}, tuples)
	if err != nil {
		b.Fatal(err)
	}

	checkQuery := queries.NewCheckQuery(datastore, datastore, tracer, telemetry.NewNoopMeter(), logger, defaultResolveNodeLimit)

	var r *openfgav1pb.CheckResponse

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ = checkQuery.Execute(ctx, &openfgav1pb.CheckRequest{
			StoreId:              store,
			AuthorizationModelId: modelID,
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "reader",
				User:     "github|iaco@auth0.com",
			},
			Trace: true,
		})
	}

	result = r
}
