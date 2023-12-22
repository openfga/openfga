package main

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
)

func newOpenFGAServerAndClient(t *testing.T) openfgav1.OpenFGAServiceClient {
	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = "memory"

	cancel := tests.StartServer(t, cfg)
	t.Cleanup(func() {
		cancel()
	})

	conn, err := grpc.Dial(cfg.GRPC.Addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	t.Cleanup(func() {
		conn.Close()
	})
	require.NoError(t, err)

	client := openfgav1.NewOpenFGAServiceClient(conn)
	return client
}

func TestGRPCMaxMessageSize(t *testing.T) {
	client := newOpenFGAServerAndClient(t)

	createResp, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "max_message_size",
	})
	require.NoError(t, err)
	require.NotPanics(t, func() { ulid.MustParse(createResp.GetId()) })

	storeID := createResp.GetId()

	model := parser.MustTransformDSLToProto(`model
  schema 1.1

type user

type document
  relations
    define viewer: [user with conds]

condition conds(s: string) {
  "alpha" == s
}`)

	writeModelResp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		TypeDefinitions: model.GetTypeDefinitions(),
		Conditions:      model.GetConditions(),
		SchemaVersion:   typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)
	require.NotPanics(t, func() { ulid.MustParse(writeModelResp.GetAuthorizationModelId()) })

	modelID := writeModelResp.GetAuthorizationModelId()

	checkResp, err := client.Check(context.Background(), &openfgav1.CheckRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			Object:   "document:1",
			Relation: "viewer",
			User:     "user:jon",
		},
		Context: testutils.MustNewStruct(t, map[string]interface{}{
			"s": testutils.CreateRandomString(config.DefaultMaxRPCMessageSizeInBytes + 1),
		}),
	})
	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.ResourceExhausted, s.Code())
	require.ErrorContains(t, err, "grpc: received message larger than max")
	require.Nil(t, checkResp)
}

//func TestCheckWithQueryCacheEnabled(t *testing.T) {
//	tester := newOpenFGAServerAndClient(t, "--check-query-cache-enabled=true")
//
//	conn := connect(t, tester)
//	defer conn.Close()
//
//	client := openfgav1.NewOpenFGAServiceClient(conn)
//
//	tests := []struct {
//		name            string
//		typeDefinitions []*openfgav1.TypeDefinition
//		tuples          []*openfgav1.TupleKey
//		assertions      []checktest.Assertion
//	}{
//		{
//			name: "issue_1058",
//			typeDefinitions: parser.MustTransformDSLToProto(`model
//	schema 1.1
//type fga_user
//
//type timeslot
//  relations
//	define user: [fga_user]
//
//type commerce_store
//  relations
//	define approved_hourly_access: user from approved_timeslot and hourly_employee
//	define approved_timeslot: [timeslot]
//	define hourly_employee: [fga_user]
//`).TypeDefinitions,
//			tuples: []*openfgav1.TupleKey{
//				{Object: "commerce_store:0", Relation: "hourly_employee", User: "fga_user:anne"},
//				{Object: "commerce_store:1", Relation: "hourly_employee", User: "fga_user:anne"},
//				{Object: "commerce_store:0", Relation: "approved_timeslot", User: "timeslot:11_12"},
//				{Object: "commerce_store:1", Relation: "approved_timeslot", User: "timeslot:12_13"},
//			},
//			assertions: []checktest.Assertion{
//				{
//					Tuple: tuple.NewTupleKey("commerce_store:0", "approved_hourly_access", "fga_user:anne"),
//					ContextualTuples: []*openfgav1.TupleKey{
//						tuple.NewTupleKey("timeslot:11_12", "user", "fga_user:anne"),
//					},
//					Expectation: true,
//				},
//				{
//					Tuple: tuple.NewTupleKey("commerce_store:1", "approved_hourly_access", "fga_user:anne"),
//					ContextualTuples: []*openfgav1.TupleKey{
//						tuple.NewTupleKey("timeslot:11_12", "user", "fga_user:anne"),
//					},
//					Expectation: false,
//				},
//				{
//					Tuple: tuple.NewTupleKey("commerce_store:1", "approved_hourly_access", "fga_user:anne"),
//					ContextualTuples: []*openfgav1.TupleKey{
//						tuple.NewTupleKey("timeslot:12_13", "user", "fga_user:anne"),
//					},
//					Expectation: true,
//				},
//			},
//		},
//		{
//			name: "cache_computed_userset_subproblem_with_contextual_tuple",
//			typeDefinitions: parser.MustTransformDSLToProto(`model
//	schema 1.1
//type user
//
//type document
//  relations
//	define restricted: [user]
//	define viewer: [user] but not restricted
//`).TypeDefinitions,
//			tuples: []*openfgav1.TupleKey{
//				{Object: "document:1", Relation: "viewer", User: "user:jon"},
//			},
//			assertions: []checktest.Assertion{
//				{
//					Tuple:            tuple.NewTupleKey("document:1", "viewer", "user:jon"),
//					ContextualTuples: []*openfgav1.TupleKey{},
//					Expectation:      true,
//				},
//				{
//					Tuple: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
//					ContextualTuples: []*openfgav1.TupleKey{
//						tuple.NewTupleKey("document:1", "restricted", "user:jon"),
//					},
//					Expectation: false,
//				},
//			},
//		},
//		{
//			name: "cached_direct_relationship_with_contextual_tuple",
//			typeDefinitions: parser.MustTransformDSLToProto(`model
//	schema 1.1
//type user
//
//type document
//  relations
//	define viewer: [user]
//`).TypeDefinitions,
//			assertions: []checktest.Assertion{
//				{
//					Tuple:            tuple.NewTupleKey("document:1", "viewer", "user:jon"),
//					ContextualTuples: []*openfgav1.TupleKey{},
//					Expectation:      false,
//				},
//				{
//					Tuple: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
//					ContextualTuples: []*openfgav1.TupleKey{
//						tuple.NewTupleKey("document:1", "viewer", "user:jon"),
//					},
//					Expectation: true,
//				},
//			},
//		},
//		{
//			name: "cached_direct_userset_relationship_with_contextual_tuple",
//			typeDefinitions: parser.MustTransformDSLToProto(`model
//	schema 1.1
//type user
//
//type group
//  relations
//	define restricted: [user]
//	define member: [user] but not restricted
//
//type document
//  relations
//	define viewer: [group#member]
//`).TypeDefinitions,
//			tuples: []*openfgav1.TupleKey{
//				{Object: "document:1", Relation: "viewer", User: "group:eng#member"},
//				{Object: "group:eng", Relation: "member", User: "user:jon"},
//			},
//			assertions: []checktest.Assertion{
//				{
//					Tuple:            tuple.NewTupleKey("document:1", "viewer", "user:jon"),
//					ContextualTuples: []*openfgav1.TupleKey{},
//					Expectation:      true,
//				},
//				{
//					Tuple: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
//					ContextualTuples: []*openfgav1.TupleKey{
//						tuple.NewTupleKey("group:eng", "restricted", "user:jon"),
//					},
//					Expectation: false,
//				},
//			},
//		},
//	}
//
//	for _, test := range tests {
//		test := test
//
//		t.Run(test.name, func(t *testing.T) {
//			createResp, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
//				Name: test.name,
//			})
//			require.NoError(t, err)
//			require.NotPanics(t, func() { ulid.MustParse(createResp.GetId()) })
//
//			storeID := createResp.GetId()
//
//			writeModelResp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
//				StoreId:         storeID,
//				TypeDefinitions: test.typeDefinitions,
//				SchemaVersion:   typesystem.SchemaVersion1_1,
//			})
//			require.NoError(t, err)
//			require.NotPanics(t, func() { ulid.MustParse(writeModelResp.GetAuthorizationModelId()) })
//
//			modelID := writeModelResp.GetAuthorizationModelId()
//
//			if len(test.tuples) > 0 {
//				_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
//					StoreId:              storeID,
//					AuthorizationModelId: modelID,
//					Writes: &openfgav1.WriteRequestWrites{
//						TupleKeys: test.tuples,
//					},
//				})
//				require.NoError(t, err)
//			}
//
//			for _, assertion := range test.assertions {
//				var tk *openfgav1.CheckRequestTupleKey
//				if assertion.Tuple != nil {
//					tk = tuple.NewCheckRequestTupleKey(
//						assertion.Tuple.GetObject(),
//						assertion.Tuple.GetRelation(),
//						assertion.Tuple.GetUser(),
//					)
//				}
//
//				checkResp, err := client.Check(context.Background(), &openfgav1.CheckRequest{
//					StoreId:              storeID,
//					AuthorizationModelId: modelID,
//					TupleKey:             tk,
//					ContextualTuples: &openfgav1.ContextualTupleKeys{
//						TupleKeys: assertion.ContextualTuples,
//					},
//				})
//
//				if assertion.ErrorCode == 0 {
//					require.NoError(t, err)
//					require.Equal(t, assertion.Expectation, checkResp.GetAllowed())
//				} else {
//					require.Error(t, err)
//					e, ok := status.FromError(err)
//					require.True(t, ok)
//					require.Equal(t, assertion.ErrorCode, int(e.Code()))
//				}
//			}
//		})
//	}
//}

func TestFunctionalGRPC(t *testing.T) {
	client := newOpenFGAServerAndClient(t)

	t.Run("TestCreateStore", func(t *testing.T) { GRPCCreateStoreTest(t, client) })
	t.Run("TestGetStore", func(t *testing.T) { GRPCGetStoreTest(t, client) })
	t.Run("TestDeleteStore", func(t *testing.T) { GRPCDeleteStoreTest(t, client) })

	t.Run("TestWrite", func(t *testing.T) { GRPCWriteTest(t, client) })
	t.Run("TestRead", func(t *testing.T) { GRPCReadTest(t, client) })
	t.Run("TestReadChanges", func(t *testing.T) { GRPCReadChangesTest(t, client) })

	t.Run("TestCheck", func(t *testing.T) { GRPCCheckTest(t, client) })
	t.Run("TestListObjects", func(t *testing.T) { GRPCListObjectsTest(t, client) })

	t.Run("TestWriteAuthorizationModel", func(t *testing.T) { GRPCWriteAuthorizationModelTest(t, client) })
	t.Run("TestReadAuthorizationModel", func(t *testing.T) { GRPCReadAuthorizationModelTest(t, client) })
	t.Run("TestReadAuthorizationModels", func(t *testing.T) { GRPCReadAuthorizationModelsTest(t, client) })
}

//func TestGRPCWithPresharedKey(t *testing.T) {
//	tester := newOpenFGAServerAndClient(t, "--authn-method", "preshared", "--authn-preshared-keys", "key1,key2")
//	defer tester.Cleanup()
//
//	conn := connect(t, tester)
//	defer conn.Close()
//
//	openfgaClient := openfgav1.NewOpenFGAServiceClient(conn)
//	healthClient := healthv1pb.NewHealthClient(conn)
//
//	resp, err := healthClient.Check(context.Background(), &healthv1pb.HealthCheckRequest{
//		Service: openfgav1.OpenFGAService_ServiceDesc.ServiceName,
//	})
//	require.NoError(t, err)
//	require.Equal(t, healthv1pb.HealthCheckResponse_SERVING, resp.Status)
//
//	_, err = openfgaClient.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
//		Name: "openfga-demo",
//	})
//	require.Error(t, err)
//
//	s, ok := status.FromError(err)
//	require.True(t, ok)
//	require.Equal(t, codes.Code(openfgav1.AuthErrorCode_bearer_token_missing), s.Code())
//
//	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer key1")
//	_, err = openfgaClient.CreateStore(ctx, &openfgav1.CreateStoreRequest{
//		Name: "openfga-demo1",
//	})
//	require.NoError(t, err)
//
//	ctx = metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer key2")
//	_, err = openfgaClient.CreateStore(ctx, &openfgav1.CreateStoreRequest{
//		Name: "openfga-demo2",
//	})
//	require.NoError(t, err)
//
//	ctx = metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer key3")
//	_, err = openfgaClient.CreateStore(ctx, &openfgav1.CreateStoreRequest{
//		Name: "openfga-demo3",
//	})
//	require.Error(t, err)
//
//	s, ok = status.FromError(err)
//	require.True(t, ok)
//	require.Equal(t, codes.Code(openfgav1.AuthErrorCode_unauthenticated), s.Code())
//}

func GRPCWriteTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	type output struct {
		errorCode    codes.Code
		errorMessage string
	}

	resp, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)
	storeID := resp.GetId()

	model := parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type document
  relations
	define viewer: [user]
`)

	writeModelResp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		TypeDefinitions: model.GetTypeDefinitions(),
		Conditions:      model.GetConditions(),
		SchemaVersion:   typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)
	require.NotPanics(t, func() { ulid.MustParse(writeModelResp.GetAuthorizationModelId()) })
	modelID := writeModelResp.GetAuthorizationModelId()

	tests := []struct {
		name   string
		input  *openfgav1.WriteRequest
		output output
	}{
		{
			name: "happy_path_writes",
			input: &openfgav1.WriteRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						{Object: "document:1", Relation: "viewer", User: "user:jon"},
					},
				},
			},
			output: output{
				errorCode: codes.OK,
			},
		},
		{
			name: "happy_path_deletes",
			input: &openfgav1.WriteRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
						{
							Object: "document:1", Relation: "viewer", User: "user:jon",
						},
					},
				},
			},
			output: output{
				errorCode: codes.OK,
			},
		},
		{
			name: "invalid_store_id",
			input: &openfgav1.WriteRequest{
				StoreId:              "invalid-store-id",
				AuthorizationModelId: modelID,
				Writes:               &openfgav1.WriteRequestWrites{},
			},
			output: output{
				errorCode:    codes.InvalidArgument,
				errorMessage: `value does not match regex pattern "^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$`,
			},
		},
		{
			name: "invalid_model_id",
			input: &openfgav1.WriteRequest{
				StoreId:              storeID,
				AuthorizationModelId: "invalid-model-id",
				Writes:               &openfgav1.WriteRequestWrites{},
			},
			output: output{
				errorCode:    codes.InvalidArgument,
				errorMessage: `value does not match regex pattern "^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$`,
			},
		},
		{
			name: "nil_writes_and_deletes",
			input: &openfgav1.WriteRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
			},
			output: output{
				errorCode:    codes.Code(2003),
				errorMessage: `Invalid input. Make sure you provide at least one write, or at least one delete`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.Write(context.Background(), test.input)
			s, ok := status.FromError(err)

			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if s.Code() == codes.OK {
				require.NoError(t, err)
			} else {
				require.Contains(t, err.Error(), test.output.errorMessage)
			}
		})
	}
}

func GRPCReadTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {

}

func GRPCReadChangesTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {

}

func GRPCCreateStoreTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	type output struct {
		errorCode codes.Code
	}

	tests := []struct {
		name   string
		input  *openfgav1.CreateStoreRequest
		output output
	}{
		{
			name:  "empty_request",
			input: &openfgav1.CreateStoreRequest{},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_name_length",
			input: &openfgav1.CreateStoreRequest{
				Name: "a",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_name_characters",
			input: &openfgav1.CreateStoreRequest{
				Name: "$openfga",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "success",
			input: &openfgav1.CreateStoreRequest{
				Name: "openfga",
			},
		},
		{
			name: "duplicate_store_name_is_allowed",
			input: &openfgav1.CreateStoreRequest{
				Name: "openfga",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := client.CreateStore(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if test.output.errorCode == codes.OK {
				require.Equal(t, test.input.Name, response.Name)
				_, err = ulid.Parse(response.Id)
				require.NoError(t, err)
			}
		})
	}
}

func GRPCGetStoreTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	resp1, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	resp2, err := client.GetStore(context.Background(), &openfgav1.GetStoreRequest{
		StoreId: resp1.Id,
	})
	require.NoError(t, err)

	require.Equal(t, resp1.Name, resp2.Name)
	require.Equal(t, resp1.Id, resp2.Id)

	resp3, err := client.GetStore(context.Background(), &openfgav1.GetStoreRequest{
		StoreId: ulid.Make().String(),
	})
	require.Error(t, err)
	require.Nil(t, resp3)
}

func TestGRPCListStores(t *testing.T) {
	client := newOpenFGAServerAndClient(t)
	_, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	_, err = client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-test",
	})
	require.NoError(t, err)

	response1, err := client.ListStores(context.Background(), &openfgav1.ListStoresRequest{
		PageSize: wrapperspb.Int32(1),
	})
	require.NoError(t, err)

	require.NotEmpty(t, response1.ContinuationToken)

	var received []*openfgav1.Store
	received = append(received, response1.Stores...)

	response2, err := client.ListStores(context.Background(), &openfgav1.ListStoresRequest{
		PageSize:          wrapperspb.Int32(2),
		ContinuationToken: response1.ContinuationToken,
	})
	require.NoError(t, err)

	require.Empty(t, response2.ContinuationToken)

	received = append(received, response2.Stores...)

	require.Len(t, received, 2)
	// todo: add assertions on received Store objects
}

func GRPCDeleteStoreTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	response1, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	response2, err := client.GetStore(context.Background(), &openfgav1.GetStoreRequest{
		StoreId: response1.Id,
	})
	require.NoError(t, err)

	require.Equal(t, response1.Id, response2.Id)

	_, err = client.DeleteStore(context.Background(), &openfgav1.DeleteStoreRequest{
		StoreId: response1.Id,
	})
	require.NoError(t, err)

	response3, err := client.GetStore(context.Background(), &openfgav1.GetStoreRequest{
		StoreId: response1.Id,
	})
	require.Nil(t, response3)

	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgav1.NotFoundErrorCode_store_id_not_found), s.Code())

	// delete is idempotent, so if the store does not exist it's a noop
	_, err = client.DeleteStore(context.Background(), &openfgav1.DeleteStoreRequest{
		StoreId: ulid.Make().String(),
	})
	require.NoError(t, err)
}

func GRPCCheckTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	type testData struct {
		tuples []*openfgav1.TupleKey
		model  *openfgav1.AuthorizationModel
	}

	type output struct {
		resp      *openfgav1.CheckResponse
		errorCode codes.Code
	}

	tests := []struct {
		name     string
		input    *openfgav1.CheckRequest
		output   output
		testData *testData
	}{
		{
			name:  "empty_request",
			input: &openfgav1.CheckRequest{},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_too_short",
			input: &openfgav1.CheckRequest{
				StoreId:              "1",
				AuthorizationModelId: ulid.Make().String(),
				TupleKey:             tuple.NewCheckRequestTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_extra_chars",
			input: &openfgav1.CheckRequest{
				StoreId:              ulid.Make().String() + "A",
				AuthorizationModelId: ulid.Make().String(),
				TupleKey:             tuple.NewCheckRequestTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_invalid_chars",
			input: &openfgav1.CheckRequest{
				StoreId:              "ABCDEFGHIJKLMNOPQRSTUVWXY@",
				AuthorizationModelId: ulid.Make().String(),
				TupleKey:             tuple.NewCheckRequestTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_authorization_model_ID_because_extra_chars",
			input: &openfgav1.CheckRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String() + "A",
				TupleKey:             tuple.NewCheckRequestTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_authorization_model_ID_because_invalid_chars",
			input: &openfgav1.CheckRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: "ABCDEFGHIJKLMNOPQRSTUVWXY@",
				TupleKey:             tuple.NewCheckRequestTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing_tuplekey_field",
			input: &openfgav1.CheckRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storeID := test.input.StoreId
			if test.testData != nil {
				resp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					SchemaVersion:   test.testData.model.SchemaVersion,
					TypeDefinitions: test.testData.model.TypeDefinitions,
				})
				require.NoError(t, err)

				modelID := resp.GetAuthorizationModelId()

				_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
					StoreId:              storeID,
					AuthorizationModelId: modelID,
					Writes: &openfgav1.WriteRequestWrites{
						TupleKeys: test.testData.tuples,
					},
				})
				require.NoError(t, err)
			}

			response, err := client.Check(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if test.output.errorCode == codes.OK {
				require.Equal(t, test.output.resp.Allowed, response.Allowed)
				require.Equal(t, test.output.resp.Resolution, response.Resolution)
			}
		})
	}
}

func GRPCListObjectsTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	type testData struct {
		tuples []*openfgav1.TupleKey
		model  string
	}

	type output struct {
		resp      *openfgav1.ListObjectsResponse
		errorCode codes.Code
	}

	tests := []struct {
		name     string
		input    *openfgav1.ListObjectsRequest
		output   output
		testData *testData
	}{
		{
			name: "undefined_model_id_returns_error",
			input: &openfgav1.ListObjectsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(), // generate random ulid so it doesn't match
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:jon",
			},
			output: output{
				errorCode: codes.Code(openfgav1.ErrorCode_authorization_model_not_found),
			},
			testData: &testData{
				model: `model
	schema 1.1
type user

type document
  relations
	define viewer: [user]`,
			},
		},
		{
			name: "direct_relationships_with_intersecton_returns_expected_objects",
			input: &openfgav1.ListObjectsRequest{
				StoreId:  ulid.Make().String(),
				Type:     "document",
				Relation: "viewer",
				User:     "user:jon",
			},
			output: output{
				resp: &openfgav1.ListObjectsResponse{
					Objects: []string{"document:1"},
				},
			},
			testData: &testData{
				tuples: []*openfgav1.TupleKey{
					{Object: "document:1", Relation: "viewer", User: "user:jon"},
					{Object: "document:1", Relation: "allowed", User: "user:jon"},
				},
				model: `model
	schema 1.1
type user

type document
  relations
	define allowed: [user]
	define viewer: [user] and allowed`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typedefs := parser.MustTransformDSLToProto(test.testData.model).TypeDefinitions

			storeID := test.input.StoreId

			if test.testData != nil {
				resp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					SchemaVersion:   typesystem.SchemaVersion1_1,
					TypeDefinitions: typedefs,
				})
				require.NoError(t, err)

				modelID := resp.GetAuthorizationModelId()

				if len(test.testData.tuples) > 0 {
					_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
						StoreId:              storeID,
						AuthorizationModelId: modelID,
						Writes: &openfgav1.WriteRequestWrites{
							TupleKeys: test.testData.tuples,
						},
					})
					require.NoError(t, err)
				}
			}

			response, err := client.ListObjects(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if test.output.errorCode == codes.OK {
				require.Equal(t, test.output.resp.Objects, response.Objects)
			}
		})
	}
}

// TestCheckWorkflows are tests that involve workflows that define assertions for
// Checks against multi-model stores etc..
func TestCheckWorkflows(t *testing.T) {
	client := newOpenFGAServerAndClient(t)

	/*
	 * TypedWildcardsFromOtherModelsIgnored ensures that a typed wildcard introduced
	 * from a prior model does not impact the Check outcome of a model that should not
	 * involve it. For example,
	 *
	 * type user
	 * type document
	 *   relations
	 *     define viewer: [user:*]
	 *
	 * write(document:1#viewer@user:*)
	 *
	 * type user
	 * type document
	 *	relations
	 *	  define viewer: [user]
	 *
	 * check(document:1#viewer@user:jon) --> {allowed: false}
	 */
	t.Run("TypedWildcardsFromOtherModelsIgnored", func(t *testing.T) {
		resp1, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
			Name: "openfga-demo",
		})
		require.NoError(t, err)

		storeID := resp1.GetId()

		_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									typesystem.WildcardRelationReference("user"),
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{Object: "document:1", Relation: "viewer", User: "user:*"},
				},
			},
		})
		require.NoError(t, err)

		_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "user"},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		resp, err := client.Check(context.Background(), &openfgav1.CheckRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewCheckRequestTupleKey("document:1", "viewer", "user:jon"),
		})
		require.NoError(t, err)
		require.False(t, resp.Allowed)
	})
}

// TestExpandWorkflows are tests that involve workflows that define assertions for
// Expands against multi-model stores etc..
func TestExpandWorkflows(t *testing.T) {
	client := newOpenFGAServerAndClient(t)

	/*
	 * TypedWildcardsFromOtherModelsIgnored ensures that a typed wildcard introduced
	 * from a prior model does not impact the Expand outcome of a model that should not
	 * involve it. For example,
	 *
	 * type user
	 * type document
	 *   relations
	 *     define viewer: [user, user:*]
	 *
	 * write(document:1#viewer@user:*)
	 * write(document:1#viewer@user:jon)
	 * Expand(document:1#viewer) --> {tree: {root: {name: document:1#viewer, leaf: {users: [user:*, user:jon]}}}}
	 *
	 * type user
	 * type document
	 *	relations
	 *	  define viewer: [user]
	 *
	 * Expand(document:1#viewer) --> {tree: {root: {name: document:1#viewer, leaf: {users: [user:jon]}}}}
	 *
	 * type employee
	 * type document
	 *   relations
	 *     define viewer: [employee]
	 *
	 * type user
	 * type employee
	 * type document
	 *   relations
	 *     define viewer: [employee]
	 * Expand(document:1#viewer) --> {tree: {root: {name: document:1#viewer, leaf: {users: []}}}}
	 */
	t.Run("TypedWildcardsFromOtherModelsIgnored", func(t *testing.T) {
		resp1, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
			Name: "openfga-demo",
		})
		require.NoError(t, err)

		storeID := resp1.GetId()

		_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									typesystem.DirectRelationReference("user", ""),
									typesystem.WildcardRelationReference("user"),
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					{Object: "document:1", Relation: "viewer", User: "user:*"},
					{Object: "document:1", Relation: "viewer", User: "user:jon"},
				},
			},
		})
		require.NoError(t, err)

		expandResp, err := client.Expand(context.Background(), &openfgav1.ExpandRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewExpandRequestTupleKey("document:1", "viewer"),
		})
		require.NoError(t, err)

		if diff := cmp.Diff(&openfgav1.UsersetTree{
			Root: &openfgav1.UsersetTree_Node{
				Name: "document:1#viewer",
				Value: &openfgav1.UsersetTree_Node_Leaf{
					Leaf: &openfgav1.UsersetTree_Leaf{
						Value: &openfgav1.UsersetTree_Leaf_Users{
							Users: &openfgav1.UsersetTree_Users{
								Users: []string{"user:*", "user:jon"},
							},
						},
					},
				},
			},
		}, expandResp.GetTree(), protocmp.Transform(), protocmp.SortRepeated(func(x, y string) bool {
			return x <= y
		})); diff != "" {
			require.Fail(t, diff)
		}

		_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									typesystem.DirectRelationReference("user", ""),
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		expandResp, err = client.Expand(context.Background(), &openfgav1.ExpandRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewExpandRequestTupleKey("document:1", "viewer"),
		})
		require.NoError(t, err)

		if diff := cmp.Diff(&openfgav1.UsersetTree{
			Root: &openfgav1.UsersetTree_Node{
				Name: "document:1#viewer",
				Value: &openfgav1.UsersetTree_Node_Leaf{
					Leaf: &openfgav1.UsersetTree_Leaf{
						Value: &openfgav1.UsersetTree_Leaf_Users{
							Users: &openfgav1.UsersetTree_Users{
								Users: []string{"user:jon"},
							},
						},
					},
				},
			},
		}, expandResp.GetTree(), protocmp.Transform()); diff != "" {
			require.Fail(t, diff)
		}

		_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "employee",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "employee"},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		expandResp, err = client.Expand(context.Background(), &openfgav1.ExpandRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewExpandRequestTupleKey("document:1", "viewer"),
		})
		require.NoError(t, err)

		if diff := cmp.Diff(&openfgav1.UsersetTree{
			Root: &openfgav1.UsersetTree_Node{
				Name: "document:1#viewer",
				Value: &openfgav1.UsersetTree_Node_Leaf{
					Leaf: &openfgav1.UsersetTree_Leaf{
						Value: &openfgav1.UsersetTree_Leaf_Users{
							Users: &openfgav1.UsersetTree_Users{
								Users: []string{},
							},
						},
					},
				},
			},
		}, expandResp.GetTree(), protocmp.Transform()); diff != "" {
			require.Fail(t, diff)
		}

		_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "employee",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "employee"},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		expandResp, err = client.Expand(context.Background(), &openfgav1.ExpandRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewExpandRequestTupleKey("document:1", "viewer"),
		})
		require.NoError(t, err)

		if diff := cmp.Diff(&openfgav1.UsersetTree{
			Root: &openfgav1.UsersetTree_Node{
				Name: "document:1#viewer",
				Value: &openfgav1.UsersetTree_Node_Leaf{
					Leaf: &openfgav1.UsersetTree_Leaf{
						Value: &openfgav1.UsersetTree_Leaf_Users{
							Users: &openfgav1.UsersetTree_Users{
								Users: []string{},
							},
						},
					},
				},
			},
		}, expandResp.GetTree(), protocmp.Transform()); diff != "" {
			require.Fail(t, diff)
		}
	})
}

func GRPCReadAuthorizationModelTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	type output struct {
		errorCode codes.Code
	}

	tests := []struct {
		name   string
		input  *openfgav1.ReadAuthorizationModelRequest
		output output
	}{
		{
			name:  "empty_request",
			input: &openfgav1.ReadAuthorizationModelRequest{},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_too_short",
			input: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: "1",
				Id:      ulid.Make().String(),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_extra_chars",
			input: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: ulid.Make().String() + "A",
				Id:      ulid.Make().String(), // ulids aren't required at this time
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_authorization_model_ID_because_extra_chars",
			input: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
				Id:      ulid.Make().String() + "A",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := client.ReadAuthorizationModel(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if test.output.errorCode == codes.OK {
				_ = response // use response for assertions
				// require.Equal(t, test.output.resp.Allowed, response.Allowed)
			}
		})
	}
}

func GRPCReadAuthorizationModelsTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	storeID := ulid.Make().String()

	_, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId: storeID,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"viewer": {Userset: &openfgav1.Userset_This{}},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								typesystem.DirectRelationReference("user", ""),
							},
						},
					},
				},
			},
		},

		SchemaVersion: typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)

	_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId: storeID,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"editor": {Userset: &openfgav1.Userset_This{}},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"editor": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								typesystem.DirectRelationReference("user", ""),
							},
						},
					},
				},
			},
		},
		SchemaVersion: typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)

	resp1, err := client.ReadAuthorizationModels(context.Background(), &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:  storeID,
		PageSize: wrapperspb.Int32(1),
	})
	require.NoError(t, err)

	require.Len(t, resp1.AuthorizationModels, 1)
	require.NotEmpty(t, resp1.ContinuationToken)

	resp2, err := client.ReadAuthorizationModels(context.Background(), &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:           storeID,
		ContinuationToken: resp1.ContinuationToken,
	})
	require.NoError(t, err)

	require.Len(t, resp2.AuthorizationModels, 1)
	require.Empty(t, resp2.ContinuationToken)
}

func GRPCWriteAuthorizationModelTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	type output struct {
		errorCode codes.Code
	}

	tests := []struct {
		name   string
		input  *openfgav1.WriteAuthorizationModelRequest
		output output
	}{
		{
			name:  "empty_request",
			input: &openfgav1.WriteAuthorizationModelRequest{},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_too_short",
			input: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: "1",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_extra_chars",
			input: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: ulid.Make().String() + "A",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing_type_definitions",
			input: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_type_definition_because_empty_type_name",
			input: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "",
						Relations: map[string]*openfgav1.Userset{
							"viewer": {Userset: &openfgav1.Userset_This{}},
						},
					},
				},
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_type_definition_because_too_many_chars_in_name",
			input: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: testutils.CreateRandomString(255),
						Relations: map[string]*openfgav1.Userset{
							"viewer": {Userset: &openfgav1.Userset_This{}},
						},
					},
				},
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_type_definition_because_invalid_chars_in_name",
			input: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "some type",
						Relations: map[string]*openfgav1.Userset{
							"viewer": {Userset: &openfgav1.Userset_This{}},
						},
					},
				},
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := client.WriteAuthorizationModel(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if test.output.errorCode == codes.OK {
				_, err = ulid.Parse(response.AuthorizationModelId)
				require.NoError(t, err)
			}
		})
	}
}
