package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/encoder"
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
)

func TestReadQuery(t *testing.T, dbTester teststorage.DatastoreTester) {
	type readQueryTest struct {
		_name           string
		typeDefinitions []*openfgav1pb.TypeDefinition
		tuples          []*openfga.TupleKey
		request         *openfgav1pb.ReadRequest
		err             error
		response        *openfgav1pb.ReadResponse
	}

	// TODO: review which of these tests should be moved to validation/types in gRPC rather than execution. e.g.: invalid relation in authorizationmodel is fine, but tuple without authorizationmodel is should be required before. see issue: https://github.com/auth0/sandcastle/issues/13
	var tests = []readQueryTest{
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
			// input
			request: &openfgav1pb.ReadRequest{
				TupleKey: &openfga.TupleKey{},
			},
			// output
			err: serverErrors.InvalidTupleSet,
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
			// input
			request: &openfgav1pb.ReadRequest{
				TupleKey: &openfga.TupleKey{
					Object: "auth0/iam",
				},
			},
			// output
			err: serverErrors.InvalidTupleSet,
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
			// input
			request: &openfgav1pb.ReadRequest{
				TupleKey: &openfga.TupleKey{
					Object: ":",
				},
			},
			// output
			err: serverErrors.InvalidTupleSet,
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
			// input
			request: &openfgav1pb.ReadRequest{
				TupleKey: &openfga.TupleKey{
					Relation: "admin",
					User:     "github|jon.allie@auth0.com",
				},
			},
			// output
			err: serverErrors.InvalidTupleSet,
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
			// input
			request: &openfgav1pb.ReadRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "repo:",
					Relation: "writer",
				},
			},
			// output
			err: serverErrors.InvalidTuple("missing objectID and user", &openfga.TupleKey{
				Object:   "repo:",
				Relation: "writer",
			}),
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
			// input
			request: &openfgav1pb.ReadRequest{
				TupleKey: &openfga.TupleKey{
					Relation: "writer",
				},
			},
			// output
			err: serverErrors.InvalidTupleSet,
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
			// input
			request: &openfgav1pb.ReadRequest{
				TupleKey: &openfga.TupleKey{
					Object:   "team:auth0/iam",
					Relation: "member",
					User:     "github|jose@auth0.com",
				},
			},
			// output
			err: serverErrors.TypeNotFound("team"),
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

	require := require.New(t)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()

	datastore, err := dbTester.New()
	require.NoError(err)

	encoder := encoder.NewNoopEncoder()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := testutils.CreateRandomString(10)
			modelID, err := id.NewString()
			if err != nil {
				t.Fatal(err)
			}
			err = datastore.WriteAuthorizationModel(ctx, store, modelID, &openfgav1pb.TypeDefinitions{TypeDefinitions: test.typeDefinitions})
			if err != nil {
				t.Fatalf("%s: WriteAuthorizationModel: err was %v, want nil", test._name, err)
			}

			if test.tuples != nil {
				if err := datastore.Write(ctx, store, []*openfga.TupleKey{}, test.tuples); err != nil {
					t.Fatalf("[%s] failed to write test tuples: %v", test._name, err)
				}
			}

			cmd := queries.NewReadQuery(datastore, datastore, tracer, logger, encoder)
			req := &openfgav1pb.ReadRequest{
				StoreId:              store,
				AuthorizationModelId: modelID,
				TupleKey:             test.request.TupleKey,
			}
			actualResponse, actualError := cmd.Execute(ctx, req)

			if test.err != nil && actualError != nil && test.err.Error() != actualError.Error() {
				t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.err, actualError)
			}

			if test.err == nil && actualError != nil {
				t.Errorf("[%s] Expected error to be nil, actual '%s'", test._name, actualError.Error())
			}

			if test.response != nil {
				if actualError != nil {
					t.Errorf("[%s] Expected no error but got '%s'", test._name, actualError)
				}

				if test.response.Tuples != nil {
					if len(test.response.Tuples) != len(actualResponse.Tuples) {
						t.Errorf("[%s] Expected response tuples length to be %d, actual %d", test._name, len(test.response.Tuples), len(actualResponse.Tuples))
					}

					for i, responseTuple := range test.response.Tuples {
						responseTupleKey := responseTuple.Key
						actualTupleKey := actualResponse.Tuples[i].Key
						if responseTupleKey.Object != actualTupleKey.Object {
							t.Errorf("[%s] Expected response tuple object at index %d length to be '%s', actual %s", test._name, i, responseTupleKey.Object, actualTupleKey.Object)
						}

						if responseTupleKey.Relation != actualTupleKey.Relation {
							t.Errorf("[%s] Expected response tuple relation at index %d length to be '%s', actual %s", test._name, i, responseTupleKey.Relation, actualTupleKey.Relation)
						}

						if responseTupleKey.User != actualTupleKey.User {
							t.Errorf("[%s] Expected response tuple user at index %d length to be '%s', actual %s", test._name, i, responseTupleKey.Relation, actualTupleKey.Relation)
						}
					}
				}
			}
		})
	}
}
