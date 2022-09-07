package test

import (
	"context"
	"testing"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestReadQuery(t *testing.T, datastore storage.OpenFGADatastore) {
	type readQueryTest struct {
		_name           string
		typeDefinitions []*openfgapb.TypeDefinition
		tuples          []*openfgapb.TupleKey
		request         *openfgapb.ReadRequest
		err             error
		response        *openfgapb.ReadResponse
	}

	// TODO: review which of these tests should be moved to validation/types in grpc rather than execution. e.g.: invalid relation in authorizationmodel is fine, but tuple without authorizationmodel is should be required before. see issue: https://github.com/openfga/sandcastle/issues/13
	var tests = []readQueryTest{
		{
			_name: "ExecuteErrorsIfOneTupleKeyHasNeitherUserObjectNorRelation",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {},
					},
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{},
			},
			// output
			err: serverErrors.InvalidTupleSet,
		},
		{
			_name: "ExecuteErrorsIfOneTupleKeyHasObjectWithoutType",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {},
					},
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object: "openfga/iam",
				},
			},
			// output
			err: serverErrors.InvalidTupleSet,
		},
		{
			_name: "ExecuteErrorsIfOneTupleKeyObjectIs':'",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {},
					},
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object: ":",
				},
			},
			// output
			err: serverErrors.InvalidTupleSet,
		},
		{
			_name: "ExecuteErrorsIfOneTupleSetHasNoObjectAndThusNoType",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {},
					},
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Relation: "admin",
					User:     "github|jon.allie@openfga",
				},
			},
			// output
			err: serverErrors.InvalidTupleSet,
		},
		{
			_name: "ExecuteErrorsIfOneTupleKeyHasNoObjectIdAndNoUserSetButHasAType",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {},
					},
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:",
					Relation: "writer",
				},
			},
			// output
			err: serverErrors.InvalidTuple("missing objectID and user", &openfgapb.TupleKey{
				Object:   "repo:",
				Relation: "writer",
			}),
		},
		{
			_name: "ExecuteErrorsIfOneTupleKeyInTupleSetOnlyHasRelation",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {},
					},
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Relation: "writer",
				},
			},
			// output
			err: serverErrors.InvalidTupleSet,
		},
		{
			_name: "ExecuteErrorsIfTypeDoesNotExist",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {},
					},
				},
				{
					Type: "org",
					Relations: map[string]*openfgapb.Userset{
						"manages": {},
					},
				}},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "team:openfga/iam",
					Relation: "member",
					User:     "github|jose@openfga",
				},
			},
			// output
			err: serverErrors.TypeNotFound("team"),
		},
		{
			_name: "ExecuteErrorsIfOneTupleHasRelationThatDoesNotExistInAuthorizationModel",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {},
					},
				},
				{
					Type: "org",
					Relations: map[string]*openfgapb.Userset{
						"manages": {},
					},
				}},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "org:",
					Relation: "owner",
					User:     "github|jose@openfga",
				},
			},
			// output
			err: serverErrors.RelationNotFound("owner", "org", &openfgapb.TupleKey{
				Object:   "org:",
				Relation: "owner",
				User:     "github|jose@openfga",
			}),
		},
		{
			_name: "ExecuteReturnsExactMatchingTupleKey",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {},
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
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:openfga/openfga",
					Relation: "admin",
					User:     "github|jose@openfga",
				},
			},
			// output
			response: &openfgapb.ReadResponse{
				Tuples: []*openfgapb.Tuple{
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "admin",
						User:     "github|jose@openfga",
					}},
				},
			},
		},
		{
			_name: "ExecuteReturnsTuplesWithProvidedUserAndObjectIdInAuthorizationModelRegardlessOfRelationIfNoRelation",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin": {},
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
					User:     "github|jose@openfga",
				},
				{
					Object:   "repo:openfga/openfgapb",
					Relation: "owner",
					User:     "github|jose@openfga",
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object: "repo:openfga/openfga",
					User:   "github|jose@openfga",
				},
			},
			// output
			response: &openfgapb.ReadResponse{
				Tuples: []*openfgapb.Tuple{
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "admin",
						User:     "github|jose@openfga",
					}},
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "owner",
						User:     "github|jose@openfga",
					}},
				},
			},
		},
		{
			_name: "ExecuteReturnsTuplesWithProvidedUserInAuthorizationModelRegardlessOfRelationAndObjectIdIfNoRelationAndNoObjectId",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin":  {},
						"writer": {},
					},
				}},
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:openfga/openfga",
					Relation: "admin",
					User:     "github|jose@openfga",
				},
				{
					Object:   "repo:openfga/openfga-server",
					Relation: "writer",
					User:     "github|jose@openfga",
				},
				{
					Object:   "org:openfga",
					Relation: "member",
					User:     "github|jose@openfga",
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object: "repo:",
					User:   "github|jose@openfga",
				},
			},
			// output
			response: &openfgapb.ReadResponse{
				Tuples: []*openfgapb.Tuple{
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "admin",
						User:     "github|jose@openfga",
					}},
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga-server",
						Relation: "writer",
						User:     "github|jose@openfga",
					}},
				},
			},
		},
		{
			_name: "ExecuteReturnsTuplesWithProvidedUserAndRelationInAuthorizationModelRegardlessOfObjectIdIfNoObjectId",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin":  {},
						"writer": {},
					},
				}},
			tuples: []*openfgapb.TupleKey{
				{
					Object:   "repo:openfga/openfga",
					Relation: "admin",
					User:     "github|jose@openfga",
				},
				{
					Object:   "repo:openfga/openfga-server",
					Relation: "writer",
					User:     "github|jose@openfga",
				},
				{
					Object:   "repo:openfga/openfga-users",
					Relation: "writer",
					User:     "github|jose@openfga",
				},
				{
					Object:   "org:openfga",
					Relation: "member",
					User:     "github|jose@openfga",
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:",
					Relation: "writer",
					User:     "github|jose@openfga",
				},
			},
			// output
			response: &openfgapb.ReadResponse{
				Tuples: []*openfgapb.Tuple{
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga-server",
						Relation: "writer",
						User:     "github|jose@openfga",
					}},
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga-users",
						Relation: "writer",
						User:     "github|jose@openfga",
					}},
				},
			},
		},
		{
			_name: "ExecuteReturnsTuplesWithProvidedObjectIdAndRelationInAuthorizationModelRegardlessOfUser",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin":  {},
						"writer": {},
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
					Relation: "admin",
					User:     "github|yenkel@openfga",
				},
				{
					Object:   "repo:openfga/openfga-users",
					Relation: "writer",
					User:     "github|jose@openfga",
				},
				{
					Object:   "org:openfga",
					Relation: "member",
					User:     "github|jose@openfga",
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:openfga/openfga",
					Relation: "admin",
				},
			},
			// output
			response: &openfgapb.ReadResponse{
				Tuples: []*openfgapb.Tuple{
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "admin",
						User:     "github|jose@openfga",
					}},
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "admin",
						User:     "github|yenkel@openfga",
					}},
				},
			},
		},
		{
			_name: "ExecuteReturnsTuplesWithProvidedObjectIdInAuthorizationModelRegardlessOfUserAndRelation",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin":  {},
						"writer": {},
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
					Relation: "writer",
					User:     "github|yenkel@openfga",
				},
				{
					Object:   "repo:openfga/openfga-users",
					Relation: "writer",
					User:     "github|jose@openfga",
				},
				{
					Object:   "org:openfga",
					Relation: "member",
					User:     "github|jose@openfga",
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object: "repo:openfga/openfga",
				},
			},
			// output
			response: &openfgapb.ReadResponse{
				Tuples: []*openfgapb.Tuple{
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "admin",
						User:     "github|jose@openfga",
					}},
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "writer",
						User:     "github|yenkel@openfga",
					}},
				},
			},
		},
		{
			_name: "ExecuteErrorsIfOneTupleIsUnauthorized",
			// state
			typeDefinitions: []*openfgapb.TypeDefinition{
				{
					Type: "repo",
					Relations: map[string]*openfgapb.Userset{
						"admin":  {},
						"writer": {},
					},
				}},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object: "repo:openfga/openfga",
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
	encoder := encoder.NewBase64Encoder()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := testutils.CreateRandomString(10)
			modelID, err := id.NewString()
			require.NoError(err)

			err = datastore.WriteAuthorizationModel(ctx, store, modelID, test.typeDefinitions)
			require.NoError(err)

			if test.tuples != nil {
				err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, test.tuples)
				require.NoError(err)
			}

			cmd := commands.NewReadQuery(datastore, tracer, logger, encoder)
			req := &openfgapb.ReadRequest{
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
