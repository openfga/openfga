package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// Read Command delegates to [storage.ReadPage].
// TODO Tests here shouldn't assert on correctness of results because that should be tested in pkg/storage/test.
// We should pass a mock datastore and assert that mock.ReadPage was called

func ReadQuerySuccessTest(t *testing.T, datastore storage.OpenFGADatastore) {
	// TODO: review which of these tests should be moved to validation/types in grpc rather than execution. e.g.: invalid relation in authorizationmodel is fine, but tuple without authorizationmodel is should be required before. see issue: https://github.com/openfga/sandcastle/issues/13
	tests := []struct {
		_name    string
		model    *openfgav1.AuthorizationModel
		tuples   []*openfgav1.TupleKey
		request  *openfgav1.ReadRequest
		response *openfgav1.ReadResponse
	}{
		//		{
		//			_name: "ExecuteReturnsExactMatchingTupleKey",
		//			// state
		//			model: &openfgav1.AuthorizationModel{
		//				Id:            ulid.Make().String(),
		//				SchemaVersion: typesystem.SchemaVersion1_0,
		//				TypeDefinitions: parser.MustTransformDSLToProto(`model
		//  schema 1.0
		// type user
		//
		// type team
		//
		// type repo
		//  relations
		//	define owner: [team]
		//	define admin: [user]`).GetTypeDefinitions(),
		//			},
		//			tuples: []*openfgav1.TupleKey{
		//				{
		//					Object:   "repo:openfga/openfga",
		//					Relation: "admin",
		//					User:     "user:github|jose",
		//				},
		//				{
		//					Object:   "repo:openfga/openfga",
		//					Relation: "owner",
		//					User:     "team:iam",
		//				},
		//			},
		//			// input
		//			request: &openfgav1.ReadRequest{
		//				TupleKey: &openfgav1.ReadRequestTupleKey{
		//					Object:   "repo:openfga/openfga",
		//					Relation: "admin",
		//					User:     "user:github|jose",
		//				},
		//			},
		//			// output
		//			response: &openfgav1.ReadResponse{
		//				Tuples: []*openfgav1.Tuple{
		//					{
		//						Key: &openfgav1.TupleKey{
		//							Object:   "repo:openfga/openfga",
		//							Relation: "admin",
		//							User:     "user:github|jose",
		//						},
		//					},
		//				},
		//			},
		//		},
		{
			_name: "ExecuteReturnsTuplesWithProvidedUserAndObjectIdInAuthorizationModelRegardlessOfRelationIfNoRelation",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`model
  schema 1.1
type user

type repo
  relations
	define admin: [user]
	define owner: [user]`).GetTypeDefinitions(),
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("repo:openfga/openfga", "admin", "user:github|jose"),
				tuple.NewTupleKey("repo:openfga/openfga", "owner", "user:github|jose"),
			},
			// input
			request: &openfgav1.ReadRequest{
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Object: "repo:openfga/openfga",
					User:   "user:github|jose",
				},
			},
			// output
			response: &openfgav1.ReadResponse{
				Tuples: []*openfgav1.Tuple{
					{Key: tuple.NewTupleKey("repo:openfga/openfga", "admin", "user:github|jose")},
					{Key: tuple.NewTupleKey("repo:openfga/openfga", "owner", "user:github|jose")},
				},
			},
		},
		//{
		//	_name: "ExecuteReturnsTuplesWithProvidedUserInAuthorizationModelRegardlessOfRelationAndObjectIdIfNoRelationAndNoObjectId",
		//	// state
		//	model: &openfgav1.AuthorizationModel{
		//		Id:            ulid.Make().String(),
		//		SchemaVersion: typesystem.SchemaVersion1_0,
		//		TypeDefinitions: []*openfgav1.TypeDefinition{
		//			{
		//				Type: "repo",
		//				Relations: map[string]*openfgav1.Userset{
		//					"admin":  {},
		//					"writer": {},
		//				},
		//			},
		//		},
		//	},
		//	tuples: []*openfgav1.TupleKey{
		//		{
		//			Object:   "repo:openfga/openfga",
		//			Relation: "admin",
		//			User:     "github|jose",
		//		},
		//		{
		//			Object:   "repo:openfga/openfga-server",
		//			Relation: "writer",
		//			User:     "github|jose",
		//		},
		//		{
		//			Object:   "org:openfga",
		//			Relation: "member",
		//			User:     "github|jose",
		//		},
		//	},
		//	// input
		//	request: &openfgav1.ReadRequest{
		//		TupleKey: &openfgav1.ReadRequestTupleKey{
		//			Object: "repo:",
		//			User:   "github|jose",
		//		},
		//	},
		//	// output
		//	response: &openfgav1.ReadResponse{
		//		Tuples: []*openfgav1.Tuple{
		//			{Key: &openfgav1.TupleKey{
		//				Object:   "repo:openfga/openfga",
		//				Relation: "admin",
		//				User:     "github|jose",
		//			}},
		//			{Key: &openfgav1.TupleKey{
		//				Object:   "repo:openfga/openfga-server",
		//				Relation: "writer",
		//				User:     "github|jose",
		//			}},
		//		},
		//	},
		// },
		//{
		//	_name: "ExecuteReturnsTuplesWithProvidedUserAndRelationInAuthorizationModelRegardlessOfObjectIdIfNoObjectId",
		//	// state
		//	model: &openfgav1.AuthorizationModel{
		//		Id:            ulid.Make().String(),
		//		SchemaVersion: typesystem.SchemaVersion1_0,
		//		TypeDefinitions: []*openfgav1.TypeDefinition{
		//			{
		//				Type: "repo",
		//				Relations: map[string]*openfgav1.Userset{
		//					"admin":  {},
		//					"writer": {},
		//				},
		//			},
		//		},
		//	},
		//	tuples: []*openfgav1.TupleKey{
		//		{
		//			Object:   "repo:openfga/openfga",
		//			Relation: "admin",
		//			User:     "github|jose",
		//		},
		//		{
		//			Object:   "repo:openfga/openfga-server",
		//			Relation: "writer",
		//			User:     "github|jose",
		//		},
		//		{
		//			Object:   "repo:openfga/openfga-users",
		//			Relation: "writer",
		//			User:     "github|jose",
		//		},
		//		{
		//			Object:   "org:openfga",
		//			Relation: "member",
		//			User:     "github|jose",
		//		},
		//	},
		//	// input
		//	request: &openfgav1.ReadRequest{
		//		TupleKey: &openfgav1.ReadRequestTupleKey{
		//			Object:   "repo:",
		//			Relation: "writer",
		//			User:     "github|jose",
		//		},
		//	},
		//	// output
		//	response: &openfgav1.ReadResponse{
		//		Tuples: []*openfgav1.Tuple{
		//			{Key: &openfgav1.TupleKey{
		//				Object:   "repo:openfga/openfga-server",
		//				Relation: "writer",
		//				User:     "github|jose",
		//			}},
		//			{Key: &openfgav1.TupleKey{
		//				Object:   "repo:openfga/openfga-users",
		//				Relation: "writer",
		//				User:     "github|jose",
		//			}},
		//		},
		//	},
		// },
		{
			_name: "ExecuteReturnsTuplesWithProvidedObjectIdAndRelationInAuthorizationModelRegardlessOfUser",
			// state
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin":  {},
							"writer": {},
						},
					},
				},
			},
			tuples: []*openfgav1.TupleKey{
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
			request: &openfgav1.ReadRequest{
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Object:   "repo:openfga/openfga",
					Relation: "admin",
				},
			},
			// output
			response: &openfgav1.ReadResponse{
				Tuples: []*openfgav1.Tuple{
					{Key: &openfgav1.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "admin",
						User:     "github|jose",
					}},
					{Key: &openfgav1.TupleKey{
						Object:   "repo:openfga/openfga",
						Relation: "admin",
						User:     "github|yenkel",
					}},
				},
			},
		},
		//{
		//	_name: "ExecuteReturnsTuplesWithProvidedObjectIdInAuthorizationModelRegardlessOfUserAndRelation",
		//	// state
		//	model: &openfgav1.AuthorizationModel{
		//		Id:            ulid.Make().String(),
		//		SchemaVersion: typesystem.SchemaVersion1_0,
		//		TypeDefinitions: []*openfgav1.TypeDefinition{
		//			{
		//				Type: "repo",
		//				Relations: map[string]*openfgav1.Userset{
		//					"admin":  {},
		//					"writer": {},
		//				},
		//			},
		//		},
		//	},
		//	tuples: []*openfgav1.TupleKey{
		//		{
		//			Object:   "repo:openfga/openfga",
		//			Relation: "admin",
		//			User:     "github|jose",
		//		},
		//		{
		//			Object:   "repo:openfga/openfga",
		//			Relation: "writer",
		//			User:     "github|yenkel",
		//		},
		//		{
		//			Object:   "repo:openfga/openfga-users",
		//			Relation: "writer",
		//			User:     "github|jose",
		//		},
		//		{
		//			Object:   "org:openfga",
		//			Relation: "member",
		//			User:     "github|jose",
		//		},
		//	},
		//	// input
		//	request: &openfgav1.ReadRequest{
		//		TupleKey: &openfgav1.ReadRequestTupleKey{
		//			Object: "repo:openfga/openfga",
		//		},
		//	},
		//	// output
		//	response: &openfgav1.ReadResponse{
		//		Tuples: []*openfgav1.Tuple{
		//			{Key: &openfgav1.TupleKey{
		//				Object:   "repo:openfga/openfga",
		//				Relation: "admin",
		//				User:     "github|jose",
		//			}},
		//			{Key: &openfgav1.TupleKey{
		//				Object:   "repo:openfga/openfga",
		//				Relation: "writer",
		//				User:     "github|yenkel",
		//			}},
		//		},
		//	},
		// },
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := ulid.Make().String()
			err := datastore.WriteAuthorizationModel(ctx, store, test.model)
			require.NoError(t, err)

			if test.tuples != nil {
				err = datastore.Write(ctx, store, []*openfgav1.TupleKeyWithoutCondition{}, test.tuples)
				require.NoError(t, err)
			}

			test.request.StoreId = store
			resp, err := commands.NewReadQuery(datastore).Execute(ctx, test.request)
			require.NoError(t, err)

			if test.response.GetTuples() != nil {
				require.Equal(t, len(test.response.GetTuples()), len(resp.GetTuples()))

				for i, responseTuple := range test.response.GetTuples() {
					responseTupleKey := responseTuple.GetKey()
					actualTupleKey := resp.GetTuples()[i].GetKey()
					require.Equal(t, responseTupleKey.GetObject(), actualTupleKey.GetObject())
					require.Equal(t, responseTupleKey.GetRelation(), actualTupleKey.GetRelation())
					require.Equal(t, responseTupleKey.GetUser(), actualTupleKey.GetUser())
				}
			}
		})
	}
}

func ReadQueryErrorTest(t *testing.T, datastore storage.OpenFGADatastore) {
	// TODO: review which of these tests should be moved to validation/types in grpc rather than execution. e.g.: invalid relation in authorizationmodel is fine, but tuple without authorizationmodel is should be required before. see issue: https://github.com/openfga/sandcastle/issues/13
	tests := []struct {
		_name   string
		model   *openfgav1.AuthorizationModel
		request *openfgav1.ReadRequest
	}{
		{
			_name: "ExecuteErrorsIfOneTupleKeyHasObjectWithoutType",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin": {},
						},
					},
				},
			},
			request: &openfgav1.ReadRequest{
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Object: "openfga/iam",
				},
			},
		},
		{
			_name: "ExecuteErrorsIfOneTupleKeyObjectIs':'",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin": {},
						},
					},
				},
			},
			request: &openfgav1.ReadRequest{
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Object: ":",
				},
			},
		},
		{
			_name: "ErrorIfRequestHasNoObjectAndThusNoType",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin": {},
						},
					},
				},
			},
			request: &openfgav1.ReadRequest{
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Relation: "admin",
					User:     "github|jonallie",
				},
			},
		},
		{
			_name: "ExecuteErrorsIfOneTupleKeyHasNoObjectIdAndNoUserSetButHasAType",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin": {},
						},
					},
				},
			},
			request: &openfgav1.ReadRequest{
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Object:   "repo:",
					Relation: "writer",
				},
			},
		},
		{
			_name: "ExecuteErrorsIfOneTupleKeyInTupleSetOnlyHasRelation",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin": {},
						},
					},
				},
			},
			request: &openfgav1.ReadRequest{
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Relation: "writer",
				},
			},
		},
		{
			_name: "ExecuteErrorsIfContinuationTokenIsBad",
			model: &openfgav1.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_0,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"admin":  {},
							"writer": {},
						},
					},
				},
			},
			request: &openfgav1.ReadRequest{
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Object: "repo:openfga/openfga",
				},
				ContinuationToken: "foo",
			},
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			store := ulid.Make().String()
			err := datastore.WriteAuthorizationModel(ctx, store, test.model)
			require.NoError(t, err)

			test.request.StoreId = store
			_, err = commands.NewReadQuery(datastore).Execute(ctx, test.request)
			require.Error(t, err)
		})
	}
}

func ReadAllTuplesTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	store := ulid.Make().String()

	writes := []*openfgav1.TupleKey{
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

	cmd := commands.NewReadQuery(datastore)

	firstRequest := &openfgav1.ReadRequest{
		StoreId:           store,
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: "",
	}
	firstResponse, err := cmd.Execute(ctx, firstRequest)
	require.NoError(t, err)

	require.Len(t, firstResponse.GetTuples(), 1)
	require.NotEmpty(t, firstResponse.GetContinuationToken())

	var receivedTuples []*openfgav1.TupleKey
	for _, tuple := range firstResponse.GetTuples() {
		receivedTuples = append(receivedTuples, tuple.GetKey())
	}

	secondRequest := &openfgav1.ReadRequest{StoreId: store, ContinuationToken: firstResponse.GetContinuationToken()}
	secondResponse, err := cmd.Execute(ctx, secondRequest)
	require.NoError(t, err)

	require.Len(t, secondResponse.GetTuples(), 2)
	require.Empty(t, secondResponse.GetContinuationToken())

	for _, tuple := range secondResponse.GetTuples() {
		receivedTuples = append(receivedTuples, tuple.GetKey())
	}

	cmpOpts := []cmp.Option{
		protocmp.IgnoreFields(protoadapt.MessageV2Of(&openfgav1.Tuple{}), "timestamp"),
		protocmp.IgnoreFields(protoadapt.MessageV2Of(&openfgav1.TupleChange{}), "timestamp"),
		protocmp.Transform(),
		testutils.TupleKeyCmpTransformer,
	}

	if diff := cmp.Diff(writes, receivedTuples, cmpOpts...); diff != "" {
		t.Errorf("Tuple mismatch (-want +got):\n%s", diff)
	}
}

func ReadAllTuplesInvalidContinuationTokenTest(t *testing.T, datastore storage.OpenFGADatastore) {
	ctx := context.Background()
	store := ulid.Make().String()

	model := &openfgav1.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "repo",
			},
		},
	}

	err := datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(t, err)

	_, err = commands.NewReadQuery(datastore).Execute(ctx, &openfgav1.ReadRequest{
		StoreId:           store,
		ContinuationToken: "foo",
	})
	require.ErrorIs(t, err, serverErrors.InvalidContinuationToken)
}
