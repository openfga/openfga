package queries

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const (
	readTestStore = "auth0"
)

type readQueryTest struct {
	_name           string
	typeDefinitions []*openfgav1pb.TypeDefinition
	tuples          []*openfga.TupleKey
	request         *openfgav1pb.ReadRequest
	err             error
	response        *openfgav1pb.ReadResponse
}

// TODO: review which of these tests should be moved to validation/types in gRPC rather than execution. e.g.: invalid relation in authorizationmodel is fine, but tuple without authorizationmodel is should be required before. see issue: https://github.com/auth0/sandcastle/issues/13
var readQueryTests = []readQueryTest{
	{
		_name: "ExecuteErrorsIfOneTupleKeyHasNeitherUserObjectNorRelation",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {},
				},
			},
		},
		tuples: nil,
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{},
		},
		// output
		err:      serverErrors.InvalidTupleSet,
		response: nil,
	},
	{
		_name: "ExecuteErrorsIfOneTupleKeyHasObjectWithoutType",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {},
				},
			},
		},
		tuples: nil,
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Object: "auth0/iam",
			},
		},
		// output
		err:      serverErrors.InvalidTupleSet,
		response: nil,
	},
	{
		_name: "ExecuteErrorsIfOneTupleKeyObjectIs':'",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {},
				},
			},
		},
		tuples: nil,
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Object: ":",
			},
		},
		// output
		err:      serverErrors.InvalidTupleSet,
		response: nil,
	},
	{
		_name: "ExecuteErrorsIfOneTupleSetHasNoObjectAndThusNoType",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {},
				},
			},
		},
		tuples: nil,
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Relation: "admin",
				User:     "github|jon.allie@auth0.com",
			},
		},
		// output
		err:      serverErrors.InvalidTupleSet,
		response: nil,
	},
	{
		_name: "ExecuteErrorsIfOneTupleKeyHasNoObjectIdAndNoUserSetButHasAType",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {},
				},
			},
		},
		tuples: nil,
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:",
				Relation: "writer",
			},
		},
		// output
		err: serverErrors.InvalidTuple("missing objectId and user", &openfga.TupleKey{
			Object:   "repo:",
			Relation: "writer",
		}),
		response: nil,
	},
	{
		_name: "ExecuteErrorsIfOneTupleKeyInTupleSetOnlyHasRelation",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {},
				},
			},
		},
		tuples: nil,
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Relation: "writer",
			},
		},
		// output
		err:      serverErrors.InvalidTupleSet,
		response: nil,
	},
	{
		_name: "ExecuteErrorsIfTypeDoesNotExist",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {},
				},
			},
			{
				Type: "org",
				Relations: map[string]*openfgav1pb.Userset{
					"manages": {},
				},
			}},
		tuples: nil,
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "team:auth0/iam",
				Relation: "member",
				User:     "github|jose@auth0.com",
			},
		},
		// output
		err:      serverErrors.TypeNotFound("team"),
		response: nil,
	},
	{
		_name: "ExecuteErrorsIfOneTupleHasRelationThatDoesNotExistInAuthorizationModel",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {},
				},
			},
			{
				Type: "org",
				Relations: map[string]*openfgav1pb.Userset{
					"manages": {},
				},
			}},
		tuples: nil,
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "org:",
				Relation: "owner",
				User:     "github|jose@auth0.com",
			},
		},
		// output
		err: serverErrors.RelationNotFound("owner", "org", &openfga.TupleKey{
			Object:   "org:",
			Relation: "owner",
			User:     "github|jose@auth0.com",
		}),
		response: nil,
	},
	{
		_name: "ExecuteReturnsExactMatchingTupleKey",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {},
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
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
				User:     "github|jose@auth0.com",
			},
		},
		// output
		err: nil,
		response: &openfgav1pb.ReadResponse{
			Tuples: []*openfga.Tuple{
				{Key: &openfga.TupleKey{
					Object:   "repo:auth0/express-jwt",
					Relation: "admin",
					User:     "github|jose@auth0.com",
				}},
			},
		},
	},
	{
		_name: "ExecuteReturnsTuplesWithProvidedUserAndObjectIdInAuthorizationModelRegardlessOfRelationIfNoRelation",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin": {},
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
				User:     "github|jose@auth0.com",
			},
			{
				Object:   "repo:auth0/openfga",
				Relation: "owner",
				User:     "github|jose@auth0.com",
			},
		},
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Object: "repo:auth0/express-jwt",
				User:   "github|jose@auth0.com",
			},
		},
		// output
		err: nil,
		response: &openfgav1pb.ReadResponse{
			Tuples: []*openfga.Tuple{
				{Key: &openfga.TupleKey{
					Object:   "repo:auth0/express-jwt",
					Relation: "admin",
					User:     "github|jose@auth0.com",
				}},
				{Key: &openfga.TupleKey{
					Object:   "repo:auth0/express-jwt",
					Relation: "owner",
					User:     "github|jose@auth0.com",
				}},
			},
		},
	},
	{
		_name: "ExecuteReturnsTuplesWithProvidedUserInAuthorizationModelRegardlessOfRelationAndObjectIdIfNoRelationAndNoObjectId",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin":  {},
					"writer": {},
				},
			}},
		tuples: []*openfga.TupleKey{
			{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
				User:     "github|jose@auth0.com",
			},
			{
				Object:   "repo:auth0/auth0-server",
				Relation: "writer",
				User:     "github|jose@auth0.com",
			},
			{
				Object:   "org:auth0",
				Relation: "member",
				User:     "github|jose@auth0.com",
			},
		},
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Object: "repo:",
				User:   "github|jose@auth0.com",
			},
		},
		// output
		err: nil,
		response: &openfgav1pb.ReadResponse{
			Tuples: []*openfga.Tuple{
				{Key: &openfga.TupleKey{
					Object:   "repo:auth0/express-jwt",
					Relation: "admin",
					User:     "github|jose@auth0.com",
				}},
				{Key: &openfga.TupleKey{
					Object:   "repo:auth0/auth0-server",
					Relation: "writer",
					User:     "github|jose@auth0.com",
				}},
			},
		},
	},
	{
		_name: "ExecuteReturnsTuplesWithProvidedUserAndRelationInAuthorizationModelRegardlessOfObjectIdIfNoObjectId",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin":  {},
					"writer": {},
				},
			}},
		tuples: []*openfga.TupleKey{
			{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
				User:     "github|jose@auth0.com",
			},
			{
				Object:   "repo:auth0/auth0-server",
				Relation: "writer",
				User:     "github|jose@auth0.com",
			},
			{
				Object:   "repo:auth0/auth0-users",
				Relation: "writer",
				User:     "github|jose@auth0.com",
			},
			{
				Object:   "org:auth0",
				Relation: "member",
				User:     "github|jose@auth0.com",
			},
		},
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:",
				Relation: "writer",
				User:     "github|jose@auth0.com",
			},
		},
		// output
		err: nil,
		response: &openfgav1pb.ReadResponse{
			Tuples: []*openfga.Tuple{
				{Key: &openfga.TupleKey{
					Object:   "repo:auth0/auth0-server",
					Relation: "writer",
					User:     "github|jose@auth0.com",
				}},
				{Key: &openfga.TupleKey{
					Object:   "repo:auth0/auth0-users",
					Relation: "writer",
					User:     "github|jose@auth0.com",
				}},
			},
		},
	},
	{
		_name: "ExecuteReturnsTuplesWithProvidedObjectIdAndRelationInAuthorizationModelRegardlessOfUser",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin":  {},
					"writer": {},
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
				Relation: "admin",
				User:     "github|yenkel@auth0.com",
			},
			{
				Object:   "repo:auth0/auth0-users",
				Relation: "writer",
				User:     "github|jose@auth0.com",
			},
			{
				Object:   "org:auth0",
				Relation: "member",
				User:     "github|jose@auth0.com",
			},
		},
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Object:   "repo:auth0/express-jwt",
				Relation: "admin",
			},
		},
		// output
		err: nil,
		response: &openfgav1pb.ReadResponse{
			Tuples: []*openfga.Tuple{
				{Key: &openfga.TupleKey{
					Object:   "repo:auth0/express-jwt",
					Relation: "admin",
					User:     "github|jose@auth0.com",
				}},
				{Key: &openfga.TupleKey{
					Object:   "repo:auth0/express-jwt",
					Relation: "admin",
					User:     "github|yenkel@auth0.com",
				}},
			},
		},
	},
	{
		_name: "ExecuteReturnsTuplesWithProvidedObjectIdInAuthorizationModelRegardlessOfUserAndRelation",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin":  {},
					"writer": {},
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
				Relation: "writer",
				User:     "github|yenkel@auth0.com",
			},
			{
				Object:   "repo:auth0/auth0-users",
				Relation: "writer",
				User:     "github|jose@auth0.com",
			},
			{
				Object:   "org:auth0",
				Relation: "member",
				User:     "github|jose@auth0.com",
			},
		},
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Object: "repo:auth0/express-jwt",
			},
		},
		// output
		err: nil,
		response: &openfgav1pb.ReadResponse{
			Tuples: []*openfga.Tuple{
				{Key: &openfga.TupleKey{
					Object:   "repo:auth0/express-jwt",
					Relation: "admin",
					User:     "github|jose@auth0.com",
				}},
				{Key: &openfga.TupleKey{
					Object:   "repo:auth0/express-jwt",
					Relation: "writer",
					User:     "github|yenkel@auth0.com",
				}},
			},
		},
	},
	{
		_name: "ExecuteErrorsIfOneTupleIsUnauthorized",
		// state
		typeDefinitions: []*openfgav1pb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgav1pb.Userset{
					"admin":  {},
					"writer": {},
				},
			}},
		tuples: nil,
		// input
		request: &openfgav1pb.ReadRequest{
			TupleKey: &openfga.TupleKey{
				Object: "repo:auth0/express-jwt",
			},
			ContinuationToken: "foo",
		},
		// output
		err: serverErrors.InvalidContinuationToken,
	},
}

func TestReadQuery(t *testing.T) {
	tracer := telemetry.NewNoopTracer()
Tests:
	for _, test := range readQueryTests {
		backend, err := testutils.BuildAllBackends(tracer)
		if err != nil {
			t.Fatalf("Error building backend: %v", err)
		}

		encoder := encoder.NewNoopEncoder()

		ctx := context.Background()
		modelID, err := id.NewString()
		if err != nil {
			t.Fatal(err)
		}
		if err := backend.AuthorizationModelBackend.WriteAuthorizationModel(ctx, readTestStore, modelID, &openfgav1pb.TypeDefinitions{TypeDefinitions: test.typeDefinitions}); err != nil {
			t.Fatalf("%s: WriteAuthorizationModel: err was %v, want nil", test._name, err)
		}

		if test.tuples != nil {
			if err := backend.TupleBackend.Write(ctx, readTestStore, []*openfga.TupleKey{}, test.tuples); err != nil {
				t.Fatalf("[%s] failed to write test tuples: %v", test._name, err)
			}
		}

		readQuery := NewReadQuery(backend.TupleBackend, backend.AuthorizationModelBackend, tracer, logger.NewNoopLogger(), encoder)
		req := &openfgav1pb.ReadRequest{
			StoreId:              readTestStore,
			AuthorizationModelId: modelID,
			TupleKey:             test.request.TupleKey,
		}
		actualResponse, actualError := readQuery.Execute(ctx, req)

		if test.err != nil && actualError != nil && test.err.Error() != actualError.Error() {
			t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.err, actualError)
			continue Tests
		}

		if test.err == nil && actualError != nil {
			t.Errorf("[%s] Expected error to be nil, actual '%s'", test._name, actualError.Error())
			continue Tests
		}

		if test.response != nil {
			if actualError != nil {
				t.Errorf("[%s] Expected no error but got '%s'", test._name, actualError)
				continue Tests
			}

			if test.response.Tuples != nil {
				if len(test.response.Tuples) != len(actualResponse.Tuples) {
					t.Errorf("[%s] Expected response tuples length to be %d, actual %d", test._name, len(test.response.Tuples), len(actualResponse.Tuples))
					continue Tests
				}

				for i, responseTuple := range test.response.Tuples {
					responseTupleKey := responseTuple.Key
					actualTupleKey := actualResponse.Tuples[i].Key
					if responseTupleKey.Object != actualTupleKey.Object {
						t.Errorf("[%s] Expected response tuple object at index %d length to be '%s', actual %s", test._name, i, responseTupleKey.Object, actualTupleKey.Object)
						continue Tests
					}

					if responseTupleKey.Relation != actualTupleKey.Relation {
						t.Errorf("[%s] Expected response tuple relation at index %d length to be '%s', actual %s", test._name, i, responseTupleKey.Relation, actualTupleKey.Relation)
						continue Tests
					}

					if responseTupleKey.User != actualTupleKey.User {
						t.Errorf("[%s] Expected response tuple user at index %d length to be '%s', actual %s", test._name, i, responseTupleKey.Relation, actualTupleKey.Relation)
						continue Tests
					}
				}
			}
		}
	}
}
