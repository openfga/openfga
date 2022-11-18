package test

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func ReadQuerySuccessTest(t *testing.T, datastore storage.OpenFGADatastore) {
	// TODO: review which of these tests should be moved to validation/types in grpc rather than execution. e.g.: invalid relation in authorizationmodel is fine, but tuple without authorizationmodel is should be required before. see issue: https://github.com/openfga/sandcastle/issues/13
	tests := []struct {
		_name    string
		model    *openfgapb.AuthorizationModel
		tuples   []*openfgapb.TupleKey
		request  *openfgapb.ReadRequest
		response *openfgapb.ReadResponse
	}{
		{
			_name: "ExecuteReturnsExactMatchingTupleKey",
			// state
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {},
						},
					},
				},
			},
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
					{
						Key: &openfgapb.TupleKey{
							Object:   "repo:openfga/openfga",
							Relation: "admin",
							User:     "github|jose@openfga",
						},
					},
				},
			},
		},
		{
			_name: "ExecuteReturnsTuplesWithProvidedUserAndObjectIdInAuthorizationModelRegardlessOfRelationIfNoRelation",
			// state
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {},
							"owner": {},
						},
					},
				},
			},
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin":  {},
							"writer": {},
						},
					},
				},
			},
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin":  {},
							"writer": {},
						},
					},
				},
			},
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin":  {},
							"writer": {},
						},
					},
				},
			},
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
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin":  {},
							"writer": {},
						},
					},
				},
			},
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
	}

	require := require.New(t)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	encoder := encoder.NewBase64Encoder()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := ulid.Make().String()
			err := datastore.WriteAuthorizationModel(ctx, store, test.model)
			require.NoError(err)

			if test.tuples != nil {
				err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, test.tuples)
				require.NoError(err)
			}

			test.request.StoreId = store
			test.request.AuthorizationModelId = test.model.Id
			resp, err := commands.NewReadQuery(datastore, tracer, logger, encoder).Execute(ctx, test.request)
			require.NoError(err)

			if test.response.Tuples != nil {
				if len(test.response.Tuples) != len(resp.Tuples) {
					t.Errorf("[%s] Expected response tuples length to be %d, actual %d", test._name, len(test.response.Tuples), len(resp.Tuples))
				}

				for i, responseTuple := range test.response.Tuples {
					responseTupleKey := responseTuple.Key
					actualTupleKey := resp.Tuples[i].Key
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
		})
	}
}

func ReadQueryErrorTest(t *testing.T, datastore storage.OpenFGADatastore) {
	// TODO: review which of these tests should be moved to validation/types in grpc rather than execution. e.g.: invalid relation in authorizationmodel is fine, but tuple without authorizationmodel is should be required before. see issue: https://github.com/openfga/sandcastle/issues/13
	tests := []struct {
		_name   string
		model   *openfgapb.AuthorizationModel
		request *openfgapb.ReadRequest
		err     error
	}{
		{
			_name: "ExecuteErrorsIfOneTupleKeyHasNeitherUserObjectNorRelation",
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {},
						},
					},
				},
			},
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{},
			},
			err: serverErrors.InvalidTupleSet,
		},
		{
			_name: "ExecuteErrorsIfOneTupleKeyHasObjectWithoutType",
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {},
						},
					},
				},
			},
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object: "openfga/iam",
				},
			},
			err: serverErrors.InvalidTupleSet,
		},
		{
			_name: "ExecuteErrorsIfOneTupleKeyObjectIs':'",
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {},
						},
					},
				},
			},
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object: ":",
				},
			},
			err: serverErrors.InvalidTupleSet,
		},
		{
			_name: "ExecuteErrorsIfOneTupleSetHasNoObjectAndThusNoType",
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {},
						},
					},
				},
			},
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Relation: "admin",
					User:     "github|jon.allie@openfga",
				},
			},
			err: serverErrors.InvalidTupleSet,
		},
		{
			_name: "ExecuteErrorsIfOneTupleKeyHasNoObjectIdAndNoUserSetButHasAType",
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {},
						},
					},
				},
			},
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:",
					Relation: "writer",
				},
			},
			err: serverErrors.InvalidTuple("missing objectID and user", &openfgapb.TupleKey{
				Object:   "repo:",
				Relation: "writer",
			}),
		},
		{
			_name: "ExecuteErrorsIfOneTupleKeyInTupleSetOnlyHasRelation",
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin": {},
						},
					},
				},
			},
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Relation: "writer",
				},
			},
			err: serverErrors.InvalidTupleSet,
		},
		{
			_name: "ExecuteErrorsIfTypeDoesNotExist",
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
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
					},
				},
			},
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "team:openfga/iam",
					Relation: "member",
					User:     "github|jose@openfga",
				},
			},
			err: serverErrors.TypeNotFound("team"),
		},
		{
			_name: "ExecuteErrorsIfOneTupleHasRelationThatDoesNotExistInAuthorizationModel",
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
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
					},
				},
			},
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "org:",
					Relation: "owner",
					User:     "github|jose@openfga",
				},
			},
			err: serverErrors.RelationNotFound("owner", "org", &openfgapb.TupleKey{
				Object:   "org:",
				Relation: "owner",
				User:     "github|jose@openfga",
			}),
		},
		{
			_name: "ExecuteErrorsIfContinuationTokenIsBad",
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgapb.Userset{
							"admin":  {},
							"writer": {},
						},
					},
				},
			},
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object: "repo:openfga/openfga",
				},
				ContinuationToken: "foo",
			},
			err: serverErrors.InvalidContinuationToken,
		},
		{
			_name: "AuthorizationModelDoesNotExist",
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "repo",
					},
				},
			},
			request: &openfgapb.ReadRequest{
				AuthorizationModelId: "01GG5WZC06ZHT2W5BZ5XEEPTW9", // hardcoded as it is used in the error below as well
				TupleKey: &openfgapb.TupleKey{
					Object: "repo:openfga/openfga",
				},
			},
			err: serverErrors.AuthorizationModelNotFound("01GG5WZC06ZHT2W5BZ5XEEPTW9"),
		},
	}

	require := require.New(t)
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()
	logger := logger.NewNoopLogger()
	encoder := encoder.NewBase64Encoder()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := ulid.Make().String()
			err := datastore.WriteAuthorizationModel(ctx, store, test.model)
			require.NoError(err)

			test.request.StoreId = store
			if test.request.AuthorizationModelId == "" {
				test.request.AuthorizationModelId = test.model.Id
			}
			_, err = commands.NewReadQuery(datastore, tracer, logger, encoder).Execute(ctx, test.request)
			require.EqualError(err, test.err.Error())
		})
	}
}
