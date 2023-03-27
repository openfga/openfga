package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/encrypter"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
					User:     "github|jose",
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
					User:     "github|jose",
				},
			},
			// output
			response: &openfgapb.ReadResponse{
				Tuples: []*openfgapb.Tuple{
					{
						Key: &openfgapb.TupleKey{
							Object:   "repo:openfga/openfga",
							Relation: "admin",
							User:     "github|jose",
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
					User:     "github|jose",
				},
				{
					Object:   "repo:openfga/openfga",
					Relation: "owner",
					User:     "github|jose",
				},
				{
					Object:   "repo:openfga/openfgapb",
					Relation: "owner",
					User:     "github|jose",
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object: "repo:openfga/openfga",
					User:   "github|jose",
				},
			},
			// output
			response: &openfgapb.ReadResponse{
				Tuples: []*openfgapb.Tuple{
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "admin",
						User:     "github|jose",
					}},
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "owner",
						User:     "github|jose",
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
					User:     "github|jose",
				},
				{
					Object:   "repo:openfga/openfga-server",
					Relation: "writer",
					User:     "github|jose",
				},
				{
					Object:   "org:openfga",
					Relation: "member",
					User:     "github|jose",
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object: "repo:",
					User:   "github|jose",
				},
			},
			// output
			response: &openfgapb.ReadResponse{
				Tuples: []*openfgapb.Tuple{
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "admin",
						User:     "github|jose",
					}},
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga-server",
						Relation: "writer",
						User:     "github|jose",
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
					User:     "github|jose",
				},
				{
					Object:   "repo:openfga/openfga-server",
					Relation: "writer",
					User:     "github|jose",
				},
				{
					Object:   "repo:openfga/openfga-users",
					Relation: "writer",
					User:     "github|jose",
				},
				{
					Object:   "org:openfga",
					Relation: "member",
					User:     "github|jose",
				},
			},
			// input
			request: &openfgapb.ReadRequest{
				TupleKey: &openfgapb.TupleKey{
					Object:   "repo:",
					Relation: "writer",
					User:     "github|jose",
				},
			},
			// output
			response: &openfgapb.ReadResponse{
				Tuples: []*openfgapb.Tuple{
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga-server",
						Relation: "writer",
						User:     "github|jose",
					}},
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga-users",
						Relation: "writer",
						User:     "github|jose",
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
					User:     "github|jose",
				},
				{
					Object:   "repo:openfga/openfga",
					Relation: "admin",
					User:     "github|yenkel",
				},
				{
					Object:   "repo:openfga/openfga-users",
					Relation: "writer",
					User:     "github|jose",
				},
				{
					Object:   "org:openfga",
					Relation: "member",
					User:     "github|jose",
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
						User:     "github|jose",
					}},
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "admin",
						User:     "github|yenkel",
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
					User:     "github|jose",
				},
				{
					Object:   "repo:openfga/openfga",
					Relation: "writer",
					User:     "github|yenkel",
				},
				{
					Object:   "repo:openfga/openfga-users",
					Relation: "writer",
					User:     "github|jose",
				},
				{
					Object:   "org:openfga",
					Relation: "member",
					User:     "github|jose",
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
						User:     "github|jose",
					}},
					{Key: &openfgapb.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "writer",
						User:     "github|yenkel",
					}},
				},
			},
		},
	}

	require := require.New(t)
	ctx := context.Background()
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
			resp, err := commands.NewReadQuery(datastore, logger, encoder).Execute(ctx, test.request)
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
						t.Errorf("[%s] Expected response tuple user at index %d length to be '%s', actual %s", test._name, i, responseTupleKey.User, actualTupleKey.User)
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
	}{
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
		},
		{
			_name: "ErrorIfRequestHasNoObjectAndThusNoType",
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
					User:     "github|jonallie",
				},
			},
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
		},
	}

	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()
	encoder := encoder.NewBase64Encoder()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := ulid.Make().String()
			err := datastore.WriteAuthorizationModel(ctx, store, test.model)
			require.NoError(err)

			test.request.StoreId = store
			_, err = commands.NewReadQuery(datastore, logger, encoder).Execute(ctx, test.request)
			require.Error(err)
		})
	}
}

func ReadAllTuplesTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	logger := logger.NewNoopLogger()
	store := ulid.Make().String()

	writes := []*openfgapb.TupleKey{
		{
			Object:   "repo:openfga/foo",
			Relation: "admin",
			User:     "github|jon.allie",
		},
		{
			Object:   "repo:openfga/bar",
			Relation: "admin",
			User:     "github|jon.allie",
		},
		{
			Object:   "repo:openfga/baz",
			Relation: "admin",
			User:     "github|jon.allie",
		},
	}
	err := datastore.Write(ctx, store, nil, writes)
	require.NoError(t, err)

	cmd := commands.NewReadQuery(datastore, logger, encoder.NewBase64Encoder())

	firstRequest := &openfgapb.ReadRequest{
		StoreId:           store,
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: "",
	}
	firstResponse, err := cmd.Execute(ctx, firstRequest)
	require.NoError(t, err)

	require.Len(t, firstResponse.Tuples, 1)
	require.NotEmpty(t, firstResponse.ContinuationToken)

	var receivedTuples []*openfgapb.TupleKey
	for _, tuple := range firstResponse.Tuples {
		receivedTuples = append(receivedTuples, tuple.Key)
	}

	secondRequest := &openfgapb.ReadRequest{StoreId: store, ContinuationToken: firstResponse.ContinuationToken}
	secondResponse, err := cmd.Execute(ctx, secondRequest)
	require.NoError(t, err)

	require.Len(t, secondResponse.Tuples, 2)
	require.Empty(t, secondResponse.ContinuationToken)

	for _, tuple := range secondResponse.Tuples {
		receivedTuples = append(receivedTuples, tuple.Key)
	}

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(openfgapb.TupleKey{}, openfgapb.Tuple{}, openfgapb.TupleChange{}, openfgapb.Assertion{}),
		cmpopts.IgnoreFields(openfgapb.Tuple{}, "Timestamp"),
		cmpopts.IgnoreFields(openfgapb.TupleChange{}, "Timestamp"),
		testutils.TupleKeyCmpTransformer,
	}

	if diff := cmp.Diff(writes, receivedTuples, cmpOpts...); diff != "" {
		t.Errorf("Tuple mismatch (-got +want):\n%s", diff)
	}
}

func ReadAllTuplesInvalidContinuationTokenTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	logger := logger.NewNoopLogger()
	store := ulid.Make().String()

	encrypter, err := encrypter.NewGCMEncrypter("key")
	require.NoError(t, err)

	encoder := encoder.NewTokenEncoder(encrypter, encoder.NewBase64Encoder())

	model := &openfgapb.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
			},
		},
	}

	err = datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(t, err)

	_, err = commands.NewReadQuery(datastore, logger, encoder).Execute(ctx, &openfgapb.ReadRequest{
		StoreId:           store,
		ContinuationToken: "foo",
	})
	require.ErrorIs(t, err, serverErrors.InvalidContinuationToken)
}
