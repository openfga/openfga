package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	checktest "github.com/openfga/openfga/internal/test/check"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// newOpenFGAServerAndClient starts an OpenFGA server, waits until its is healthy, and returns a grpc client to it.
func newOpenFGAServerAndClient(t *testing.T) openfgav1.OpenFGAServiceClient {
	cfg := config.MustDefaultConfig()
	cfg.Experimentals = append(cfg.Experimentals, "enable-check-optimizations", "enable-list-objects-optimizations")
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = "memory"

	StartServer(t, cfg, "memory")
	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)

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

	model := parser.MustTransformDSLToProto(`
		model
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

// TODO make a unit test from this.
func TestCheckWithQueryCacheEnabled(t *testing.T) {
	cfg := config.MustDefaultConfig()
	cfg.CheckQueryCache.Enabled = true

	StartServer(t, cfg, "memory")

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	client := openfgav1.NewOpenFGAServiceClient(conn)

	tests := []struct {
		name            string
		typeDefinitions []*openfgav1.TypeDefinition
		tuples          []*openfgav1.TupleKey
		assertions      []checktest.Assertion
	}{
		{
			name: "issue_1058",
			typeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type fga_user

				type timeslot
					relations
						define user: [fga_user]

				type commerce_store
					relations
						define approved_hourly_access: user from approved_timeslot and hourly_employee
						define approved_timeslot: [timeslot]
						define hourly_employee: [fga_user]
				`).GetTypeDefinitions(),
			tuples: []*openfgav1.TupleKey{
				{Object: "commerce_store:0", Relation: "hourly_employee", User: "fga_user:anne"},
				{Object: "commerce_store:1", Relation: "hourly_employee", User: "fga_user:anne"},
				{Object: "commerce_store:0", Relation: "approved_timeslot", User: "timeslot:11_12"},
				{Object: "commerce_store:1", Relation: "approved_timeslot", User: "timeslot:12_13"},
			},
			assertions: []checktest.Assertion{
				{
					Tuple: tuple.NewTupleKey("commerce_store:0", "approved_hourly_access", "fga_user:anne"),
					ContextualTuples: []*openfgav1.TupleKey{
						tuple.NewTupleKey("timeslot:11_12", "user", "fga_user:anne"),
					},
					Expectation: true,
				},
				{
					Tuple: tuple.NewTupleKey("commerce_store:1", "approved_hourly_access", "fga_user:anne"),
					ContextualTuples: []*openfgav1.TupleKey{
						tuple.NewTupleKey("timeslot:11_12", "user", "fga_user:anne"),
					},
					Expectation: false,
				},
				{
					Tuple: tuple.NewTupleKey("commerce_store:1", "approved_hourly_access", "fga_user:anne"),
					ContextualTuples: []*openfgav1.TupleKey{
						tuple.NewTupleKey("timeslot:12_13", "user", "fga_user:anne"),
					},
					Expectation: true,
				},
			},
		},
		{
			name: "cache_computed_userset_subproblem_with_contextual_tuple",
			typeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user

				type document
					relations
						define restricted: [user]
						define viewer: [user] but not restricted
				`).GetTypeDefinitions(),
			tuples: []*openfgav1.TupleKey{
				{Object: "document:1", Relation: "viewer", User: "user:jon"},
			},
			assertions: []checktest.Assertion{
				{
					Tuple:            tuple.NewTupleKey("document:1", "viewer", "user:jon"),
					ContextualTuples: []*openfgav1.TupleKey{},
					Expectation:      true,
				},
				{
					Tuple: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
					ContextualTuples: []*openfgav1.TupleKey{
						tuple.NewTupleKey("document:1", "restricted", "user:jon"),
					},
					Expectation: false,
				},
			},
		},
		{
			name: "cached_direct_relationship_with_contextual_tuple",
			typeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [user]
				`).GetTypeDefinitions(),
			assertions: []checktest.Assertion{
				{
					Tuple:            tuple.NewTupleKey("document:1", "viewer", "user:jon"),
					ContextualTuples: []*openfgav1.TupleKey{},
					Expectation:      false,
				},
				{
					Tuple: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
					ContextualTuples: []*openfgav1.TupleKey{
						tuple.NewTupleKey("document:1", "viewer", "user:jon"),
					},
					Expectation: true,
				},
			},
		},
		{
			name: "cached_direct_userset_relationship_with_contextual_tuple",
			typeDefinitions: parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user

				type group
					relations
						define restricted: [user]
						define member: [user] but not restricted

				type document
					relations
						define viewer: [group#member]
				`).GetTypeDefinitions(),
			tuples: []*openfgav1.TupleKey{
				{Object: "document:1", Relation: "viewer", User: "group:eng#member"},
				{Object: "group:eng", Relation: "member", User: "user:jon"},
			},
			assertions: []checktest.Assertion{
				{
					Tuple:            tuple.NewTupleKey("document:1", "viewer", "user:jon"),
					ContextualTuples: []*openfgav1.TupleKey{},
					Expectation:      true,
				},
				{
					Tuple: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
					ContextualTuples: []*openfgav1.TupleKey{
						tuple.NewTupleKey("group:eng", "restricted", "user:jon"),
					},
					Expectation: false,
				},
			},
		},
	}

	for _, test := range tests {
		test := test

		t.Run(test.name, func(t *testing.T) {
			createResp, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
				Name: test.name,
			})
			require.NoError(t, err)
			require.NotPanics(t, func() { ulid.MustParse(createResp.GetId()) })

			storeID := createResp.GetId()

			writeModelResp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				TypeDefinitions: test.typeDefinitions,
				SchemaVersion:   typesystem.SchemaVersion1_1,
			})
			require.NoError(t, err)
			require.NotPanics(t, func() { ulid.MustParse(writeModelResp.GetAuthorizationModelId()) })

			modelID := writeModelResp.GetAuthorizationModelId()

			if len(test.tuples) > 0 {
				_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
					StoreId:              storeID,
					AuthorizationModelId: modelID,
					Writes: &openfgav1.WriteRequestWrites{
						TupleKeys: test.tuples,
					},
				})
				require.NoError(t, err)
			}

			for _, assertion := range test.assertions {
				var tk *openfgav1.CheckRequestTupleKey
				if assertion.Tuple != nil {
					tk = tuple.NewCheckRequestTupleKey(
						assertion.Tuple.GetObject(),
						assertion.Tuple.GetRelation(),
						assertion.Tuple.GetUser(),
					)
				}

				checkResp, err := client.Check(context.Background(), &openfgav1.CheckRequest{
					StoreId:              storeID,
					AuthorizationModelId: modelID,
					TupleKey:             tk,
					ContextualTuples: &openfgav1.ContextualTupleKeys{
						TupleKeys: assertion.ContextualTuples,
					},
				})

				if assertion.ErrorCode == 0 {
					require.NoError(t, err)
					require.Equal(t, assertion.Expectation, checkResp.GetAllowed())
				} else {
					require.Error(t, err)
					e, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, assertion.ErrorCode, int(e.Code()))
				}
			}
		})
	}
}

func TestFunctionalGRPC(t *testing.T) {
	// uncomment when https://github.com/hashicorp/go-retryablehttp/issues/214 is solved
	// defer goleak.VerifyNone(t)
	client := newOpenFGAServerAndClient(t)

	t.Run("TestCreateStore", func(t *testing.T) { GRPCCreateStoreTest(t, client) })
	t.Run("TestGetStore", func(t *testing.T) { GRPCGetStoreTest(t, client) })
	t.Run("TestDeleteStore", func(t *testing.T) { GRPCDeleteStoreTest(t, client) })

	t.Run("TestWrite", func(t *testing.T) { GRPCWriteTest(t, client) })
	t.Run("TestRead", func(t *testing.T) { GRPCReadTest(t, client) })
	t.Run("TestReadChanges", func(t *testing.T) { GRPCReadChangesTest(t, client) })

	t.Run("TestCheck", func(t *testing.T) { GRPCCheckTest(t, client) })
	t.Run("TestListObjects", func(t *testing.T) { GRPCListObjectsTest(t, client) })
	t.Run("TestListUsersValidation", func(t *testing.T) { GRPCListUsersValidationTest(t, client) })
	t.Run("TestWriteAuthorizationModel", func(t *testing.T) { GRPCWriteAuthorizationModelTest(t, client) })
	t.Run("TestReadAuthorizationModel", func(t *testing.T) { GRPCReadAuthorizationModelTest(t, client) })
	t.Run("TestReadAuthorizationModels", func(t *testing.T) { GRPCReadAuthorizationModelsTest(t, client) })
	t.Run("TestWriteAssertions", func(t *testing.T) { GRPCWriteAssertionsTest(t, client) })

	t.Run("TestReadAuthorizationModel", func(t *testing.T) { GRPCReadAuthorizationModelTest(t, client) })
	t.Run("TestReadAuthorizationModels", func(t *testing.T) { GRPCReadAuthorizationModelsTest(t, client) })
}

func TestGRPCWithPresharedKey(t *testing.T) {
	cfg := config.MustDefaultConfig()
	cfg.Authn.Method = "preshared"
	cfg.Authn.AuthnPresharedKeyConfig = &config.AuthnPresharedKeyConfig{Keys: []string{"key1", "key2"}}

	StartServer(t, cfg, "memory")

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	testutils.EnsureServiceHealthy(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil)

	openfgaClient := openfgav1.NewOpenFGAServiceClient(conn)

	_, err := openfgaClient.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.Error(t, err)

	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgav1.AuthErrorCode_bearer_token_missing), s.Code())

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer key1")
	_, err = openfgaClient.CreateStore(ctx, &openfgav1.CreateStoreRequest{
		Name: "openfga-demo1",
	})
	require.NoError(t, err)

	ctx = metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer key2")
	_, err = openfgaClient.CreateStore(ctx, &openfgav1.CreateStoreRequest{
		Name: "openfga-demo2",
	})
	require.NoError(t, err)

	ctx = metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer key3")
	_, err = openfgaClient.CreateStore(ctx, &openfgav1.CreateStoreRequest{
		Name: "openfga-demo3",
	})
	require.Error(t, err)

	s, ok = status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgav1.AuthErrorCode_unauthenticated), s.Code())
}

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

	model := parser.MustTransformDSLToProto(`
		model
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

func writeTuples(client openfgav1.OpenFGAServiceClient, storeID string, modelID string, count int, user string) (*openfgav1.WriteResponse, error) {
	tupleKeys := make([]*openfgav1.TupleKey, count)
	for i := 0; i < count; i++ {
		tupleKeys[i] = tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "viewer", user)
	}
	return client.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId: storeID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: tupleKeys,
		},
		AuthorizationModelId: modelID,
	})
}

func GRPCReadTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	storeResponse, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "GRPCReadChangesTest",
	})
	require.NoError(t, err)

	storeID := storeResponse.GetId()

	modelResponse, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId: storeID,
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
		SchemaVersion: "1.1",
	})
	require.NoError(t, err)

	const pageSize = 20

	modelID := modelResponse.GetAuthorizationModelId()

	// one page of tuples with user:1st
	_, err = writeTuples(client, storeID, modelID, pageSize, "user:1st")
	require.NoError(t, err)

	// one page of tuples with user:2nd - after continuation token is captured
	_, err = writeTuples(client, storeID, modelID, pageSize, "user:2nd")
	require.NoError(t, err)

	// find the continuation token for the 3rd page from the start
	require.NoError(t, err)
	firstPage, err := client.Read(context.Background(), &openfgav1.ReadRequest{
		StoreId:  storeID,
		PageSize: wrapperspb.Int32(pageSize),
	})
	require.NoError(t, err)

	continuationToken := firstPage.GetContinuationToken()

	tests := []struct {
		name     string
		input    *openfgav1.ReadRequest
		validate func(*testing.T, *openfgav1.ReadResponse)
		err      error
	}{
		{
			"empty_request",
			&openfgav1.ReadRequest{
				StoreId:  storeID,
				PageSize: wrapperspb.Int32(pageSize),
			},
			func(t *testing.T, response *openfgav1.ReadResponse) {
				require.Len(t, response.GetTuples(), pageSize)
				require.NotEmpty(t, response.GetContinuationToken())
				for _, tpl := range response.GetTuples() {
					require.NotNil(t, tpl.GetKey())
					require.Equal(t, "user:1st", tpl.GetKey().GetUser())
				}
			},
			nil,
		},
		{
			"with_tuple_key_single_document",
			&openfgav1.ReadRequest{
				StoreId: storeID,
				TupleKey: &openfgav1.ReadRequestTupleKey{
					User:     "user:1st",
					Relation: "viewer",
					Object:   "document:1",
				},
			},
			func(t *testing.T, response *openfgav1.ReadResponse) {
				require.Len(t, response.GetTuples(), 1)
				require.Empty(t, response.GetContinuationToken())
				for _, tpl := range response.GetTuples() {
					require.NotNil(t, tpl.GetKey())
					require.Equal(t, "user:1st", tpl.GetKey().GetUser())
				}
			},
			nil,
		},
		{
			"with_tuple_key_by_document1",
			&openfgav1.ReadRequest{
				StoreId: storeID,
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Relation: "viewer",
					Object:   "document:1",
				},
			},
			func(t *testing.T, response *openfgav1.ReadResponse) {
				require.Len(t, response.GetTuples(), 2)
				require.Empty(t, response.GetContinuationToken())
				for _, tpl := range response.GetTuples() {
					require.NotNil(t, tpl.GetKey())
					require.Equal(t, "document:1", tpl.GetKey().GetObject())
				}
				assert.ElementsMatch(t, []string{"user:1st", "user:2nd"},
					[]string{
						response.GetTuples()[0].GetKey().GetUser(),
						response.GetTuples()[1].GetKey().GetUser(),
					})
			},
			nil,
		},
		{
			"with_tuple_key_by_document1_page_size1",
			&openfgav1.ReadRequest{
				StoreId: storeID,
				TupleKey: &openfgav1.ReadRequestTupleKey{
					Relation: "viewer",
					Object:   "document:1",
				},
				PageSize: wrapperspb.Int32(1),
			},
			func(t *testing.T, response *openfgav1.ReadResponse) {
				require.Len(t, response.GetTuples(), 1)
				assert.NotEmpty(t, response.GetContinuationToken())
				for _, tpl := range response.GetTuples() {
					require.NotNil(t, tpl.GetKey())
					require.Equal(t, "document:1", tpl.GetKey().GetObject())
				}
				assert.ElementsMatch(t, []string{"user:1st"},
					[]string{
						response.GetTuples()[0].GetKey().GetUser(),
					})
			},
			nil,
		},
		{
			"with_continuation_token",
			&openfgav1.ReadRequest{
				StoreId:           storeID,
				ContinuationToken: continuationToken,
				PageSize:          wrapperspb.Int32(pageSize),
			},
			func(t *testing.T, response *openfgav1.ReadResponse) {
				require.Len(t, response.GetTuples(), pageSize)
				require.Empty(t, response.GetContinuationToken())
				for _, tpl := range response.GetTuples() {
					require.NotNil(t, tpl.GetKey())
					require.Equal(t, "user:2nd", tpl.GetKey().GetUser())
				}
			},
			nil,
		},
		{
			"with_invalid_continuation_token",
			&openfgav1.ReadRequest{
				StoreId:           storeID,
				ContinuationToken: "invalid-token",
			},
			func(t *testing.T, response *openfgav1.ReadResponse) {
				// do nothing
			},
			status.Error(2007, "Invalid continuation token"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := client.Read(context.Background(), test.input)
			if test.err != nil {
				require.Error(t, err)
				assert.Equal(t, test.err, err)
			} else {
				require.NoError(t, err)
				test.validate(t, response)
			}
		})
	}
}

func GRPCReadChangesTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	storeResponse, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "GRPCReadChangesTest",
	})
	require.NoError(t, err)

	storeID := storeResponse.GetId()
	modelResponse, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId: storeID,
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
		SchemaVersion: "1.1",
	})
	require.NoError(t, err)

	modelID := modelResponse.GetAuthorizationModelId()

	const pageSize = storage.DefaultPageSize

	// one page of tuples with user:before - before start_time is captured
	_, err = writeTuples(client, storeID, modelID, pageSize, "user:before")
	require.NoError(t, err)

	// wait for the tuples to be written
	time.Sleep(1 * time.Millisecond)

	startTime := time.Now()

	time.Sleep(1 * time.Millisecond)

	// one page of tuples with user:after - after start_time is captured
	_, err = writeTuples(client, storeID, modelID, pageSize, "user:after")
	require.NoError(t, err)

	// one page of tuples with user:3rd - one page after the start_time is captured
	_, err = writeTuples(client, storeID, modelID, pageSize, "user:3rd")
	require.NoError(t, err)

	// find the continuation token for the 3rd page from the start
	require.NoError(t, err)
	twoPages, err := client.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
		StoreId:  storeID,
		PageSize: wrapperspb.Int32(pageSize * 2),
	})
	require.NoError(t, err)

	continuationToken := twoPages.GetContinuationToken()

	tests := []struct {
		name     string
		input    *openfgav1.ReadChangesRequest
		validate func(*testing.T, *openfgav1.ReadChangesResponse)
		err      error
	}{
		{
			"empty_request",
			&openfgav1.ReadChangesRequest{
				StoreId: storeID,
			},
			func(t *testing.T, response *openfgav1.ReadChangesResponse) {
				require.Len(t, response.GetChanges(), pageSize)
				require.NotEmpty(t, response.GetContinuationToken())
				for _, change := range response.GetChanges() {
					require.NotNil(t, change.GetTupleKey())
					require.Equal(t, "user:before", change.GetTupleKey().GetUser())
				}
			},
			nil,
		},
		{
			"with_continuation_token",
			&openfgav1.ReadChangesRequest{
				StoreId:           storeID,
				ContinuationToken: continuationToken,
			},
			func(t *testing.T, response *openfgav1.ReadChangesResponse) {
				require.Len(t, response.GetChanges(), pageSize)
				require.NotEmpty(t, response.GetContinuationToken())
				for _, change := range response.GetChanges() {
					require.NotNil(t, change.GetTupleKey())
					require.Equal(t, "user:3rd", change.GetTupleKey().GetUser())
				}
			},
			nil,
		},
		{
			"with_start_time",
			&openfgav1.ReadChangesRequest{
				StoreId:   storeID,
				StartTime: timestamppb.New(startTime),
			},
			func(t *testing.T, response *openfgav1.ReadChangesResponse) {
				require.Len(t, response.GetChanges(), pageSize)
				require.NotEmpty(t, response.GetContinuationToken())
				for _, change := range response.GetChanges() {
					require.NotNil(t, change.GetTupleKey())
					require.Equal(t, "user:after", change.GetTupleKey().GetUser())
				}
			},
			nil,
		},
		{
			"with_start_time_and_token",
			&openfgav1.ReadChangesRequest{
				StoreId:           storeID,
				StartTime:         timestamppb.New(startTime),
				ContinuationToken: continuationToken,
			},
			func(t *testing.T, response *openfgav1.ReadChangesResponse) {
				require.Len(t, response.GetChanges(), pageSize)
				require.NotEmpty(t, response.GetContinuationToken())
				for _, change := range response.GetChanges() {
					require.NotNil(t, change.GetTupleKey())
					require.Equal(t, "user:3rd", change.GetTupleKey().GetUser())
				}
			},
			nil,
		},
		{
			"with_invalid_start_time",
			&openfgav1.ReadChangesRequest{
				StoreId: storeID,
				StartTime: timestamppb.New(startTime.
					Add(-1 * startTime.Sub(startTime)). // until the beginning of time
					Add(-1_000_000 * time.Hour),        // and then minus a million hours
				),
			},
			nil,
			status.Error(codes.Code(openfgav1.ErrorCode_invalid_start_time), "Invalid start time"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := client.ReadChanges(context.Background(), test.input)
			if test.err != nil {
				require.Error(t, err)
				assert.Equal(t, test.err, err)
			} else {
				require.NoError(t, err)
				test.validate(t, response)
			}
		})
	}
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
				require.Equal(t, test.input.GetName(), response.GetName())
				_, err = ulid.Parse(response.GetId())
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
		StoreId: resp1.GetId(),
	})
	require.NoError(t, err)

	require.Equal(t, resp1.GetName(), resp2.GetName())
	require.Equal(t, resp1.GetId(), resp2.GetId())

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

	require.NotEmpty(t, response1.GetContinuationToken())

	var received []*openfgav1.Store
	received = append(received, response1.GetStores()...)

	response2, err := client.ListStores(context.Background(), &openfgav1.ListStoresRequest{
		PageSize:          wrapperspb.Int32(2),
		ContinuationToken: response1.GetContinuationToken(),
	})
	require.NoError(t, err)

	require.Empty(t, response2.GetContinuationToken())

	received = append(received, response2.GetStores()...)

	require.Len(t, received, 2)
	// todo: add assertions on received Store objects
}

func GRPCDeleteStoreTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	response1, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	response2, err := client.GetStore(context.Background(), &openfgav1.GetStoreRequest{
		StoreId: response1.GetId(),
	})
	require.NoError(t, err)

	require.Equal(t, response1.GetId(), response2.GetId())

	_, err = client.DeleteStore(context.Background(), &openfgav1.DeleteStoreRequest{
		StoreId: response1.GetId(),
	})
	require.NoError(t, err)

	response3, err := client.GetStore(context.Background(), &openfgav1.GetStoreRequest{
		StoreId: response1.GetId(),
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
	type output struct {
		errorCode codes.Code
	}

	tests := []struct {
		name   string
		input  *openfgav1.CheckRequest
		output output
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
		{
			name: "missing_user",
			input: &openfgav1.CheckRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				TupleKey: &openfgav1.CheckRequestTupleKey{
					Relation: "relation",
					Object:   "obj:1",
				},
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing_relation",
			input: &openfgav1.CheckRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:   "user:anne",
					Object: "obj:1",
				},
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing_object",
			input: &openfgav1.CheckRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     "user:anne",
					Relation: "relation",
				},
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "model_not_found",
			input: &openfgav1.CheckRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     "user:anne",
					Object:   "obj:1",
					Relation: "relation",
				},
			},
			output: output{
				errorCode: 2001, // ErrorCode_authorization_model_not_found
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.Check(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if s.Code() == codes.OK {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func GRPCListObjectsTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	type output struct {
		errorCode codes.Code
	}

	tests := []struct {
		name   string
		input  *openfgav1.ListObjectsRequest
		output output
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
		},
		{
			name:  "empty_request",
			input: &openfgav1.ListObjectsRequest{},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_too_short",
			input: &openfgav1.ListObjectsRequest{
				StoreId:              "1",
				AuthorizationModelId: ulid.Make().String(),
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:jon",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_extra_chars",
			input: &openfgav1.ListObjectsRequest{
				StoreId:              ulid.Make().String() + "A",
				AuthorizationModelId: ulid.Make().String(),
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:jon",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_invalid_chars",
			input: &openfgav1.ListObjectsRequest{
				StoreId:              "ABCDEFGHIJKLMNOPQRSTUVWXY@",
				AuthorizationModelId: ulid.Make().String(),
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:jon",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_authorization_model_ID_because_extra_chars",
			input: &openfgav1.ListObjectsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String() + "A",
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:jon",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_authorization_model_ID_because_invalid_chars",
			input: &openfgav1.ListObjectsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: "ABCDEFGHIJKLMNOPQRSTUVWXY@",
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:jon",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing_user",
			input: &openfgav1.ListObjectsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Type:                 "document",
				Relation:             "viewer",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing_relation",
			input: &openfgav1.ListObjectsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Type:                 "document",
				User:                 "user:jon",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing_type",
			input: &openfgav1.ListObjectsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Relation:             "viewer",
				User:                 "user:jon",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "model_not_found",
			input: &openfgav1.ListObjectsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:jon",
			},
			output: output{
				errorCode: 2001, // ErrorCode_authorization_model_not_found
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.ListObjects(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if s.Code() == codes.OK {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func GRPCListUsersValidationTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	_101tuples := make([]*openfgav1.TupleKey, 101)
	for i := 0; i < 101; i++ {
		_101tuples[i] = tuple.NewTupleKey(
			fmt.Sprintf("document:%d", i), "user", fmt.Sprintf("user:%d", i),
		)
	}

	tests := []struct {
		name              string
		input             *openfgav1.ListUsersRequest
		expectedErrorCode codes.Code
	}{
		{
			name: "too_many_user_filters",
			input: &openfgav1.ListUsersRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Relation:             "viewer",
				Object: &openfgav1.Object{
					Type: "document",
					Id:   "1",
				},
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}, {Type: "employee"}},
			},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name: "zero_user_filters",
			input: &openfgav1.ListUsersRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Relation:             "viewer",
				Object: &openfgav1.Object{
					Type: "document",
					Id:   "1",
				},
				UserFilters: []*openfgav1.UserTypeFilter{},
			},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name: "object_no_type_defined",
			input: &openfgav1.ListUsersRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Relation:             "viewer",
				Object: &openfgav1.Object{
					Id: "1",
				},
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name: "object_no_id_defined",
			input: &openfgav1.ListUsersRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Relation:             "viewer",
				Object: &openfgav1.Object{
					Type: "user",
				},
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name:              "empty_request",
			input:             &openfgav1.ListUsersRequest{},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name: "invalid_storeID_because_too_short",
			input: &openfgav1.ListUsersRequest{
				StoreId:              "1",
				AuthorizationModelId: ulid.Make().String(),
				Relation:             "viewer",
				Object: &openfgav1.Object{
					Type: "document",
					Id:   "1",
				},
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name: "invalid_storeID_because_extra_chars",
			input: &openfgav1.ListUsersRequest{
				StoreId:              ulid.Make().String() + "A",
				AuthorizationModelId: ulid.Make().String(),
				Relation:             "viewer",
				Object: &openfgav1.Object{
					Type: "document",
					Id:   "1",
				},
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name: "invalid_storeID_because_invalid_chars",
			input: &openfgav1.ListUsersRequest{
				StoreId:              "ABCDEFGHIJKLMNOPQRSTUVWXY@",
				AuthorizationModelId: ulid.Make().String(),
				Relation:             "viewer",
				Object: &openfgav1.Object{
					Type: "document",
					Id:   "1",
				},
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name: "invalid_authorization_model_ID_because_extra_chars",
			input: &openfgav1.ListUsersRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String() + "A",
				Relation:             "viewer",
				Object: &openfgav1.Object{
					Type: "document",
					Id:   "1",
				},
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name: "invalid_store_ID_because_extra_chars",
			input: &openfgav1.ListUsersRequest{
				StoreId:              ulid.Make().String() + "A",
				AuthorizationModelId: ulid.Make().String(),
				Relation:             "viewer",
				Object: &openfgav1.Object{
					Type: "document",
					Id:   "1",
				},
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name: "invalid_authorization_model_ID_because_invalid_chars_in_model_ID",
			input: &openfgav1.ListUsersRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: "ABCDEFGHIJKLMNOPQRSTUVWXY@",
				Relation:             "viewer",
				Object: &openfgav1.Object{
					Type: "document",
					Id:   "1",
				},
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name: "missing_object",
			input: &openfgav1.ListUsersRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Relation:             "viewer",
				UserFilters:          []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name: "empty_object",
			input: &openfgav1.ListUsersRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Relation:             "viewer",
				Object:               &openfgav1.Object{},
				UserFilters:          []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name: "missing_relation",
			input: &openfgav1.ListUsersRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Object: &openfgav1.Object{
					Type: "document",
					Id:   "1",
				},
				UserFilters: []*openfgav1.UserTypeFilter{{Type: "user"}},
			},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			name: "too_many_contextual_tuples",
			input: &openfgav1.ListUsersRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Relation:             "viewer",
				Object: &openfgav1.Object{
					Type: "document",
					Id:   "1",
				},
				UserFilters:      []*openfgav1.UserTypeFilter{{Type: "user"}},
				ContextualTuples: _101tuples,
			},
			expectedErrorCode: codes.InvalidArgument,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := client.ListUsers(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.expectedErrorCode, s.Code())

			if s.Code() == codes.OK {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// TestExpandWorkflows are tests that involve workflows that define assertions for
// Expands against multi-model stores etc..
// TODO move to consolidated_1_1_tests.yaml.
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
	type testData struct {
		model string
	}

	type output struct {
		errorCode codes.Code
	}

	tests := []struct {
		name     string
		input    *openfgav1.ReadAuthorizationModelRequest
		output   output
		testData *testData
	}{
		{
			name: "happy_path",
			testData: &testData{
				model: `
					model
						schema 1.1
					type user`,
			},
			input: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
				Id:      ulid.Make().String(),
			},
			output: output{
				errorCode: codes.OK,
			},
		},
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
			name: "invalid_authorization_model_ID_because_too_short",
			input: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
				Id:      "1",
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
		{
			name: "missing_authorization_id",
			input: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.testData != nil {
				modelResp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         test.input.GetStoreId(),
					SchemaVersion:   typesystem.SchemaVersion1_1,
					TypeDefinitions: parser.MustTransformDSLToProto(test.testData.model).GetTypeDefinitions(),
				})
				test.input.Id = modelResp.GetAuthorizationModelId()
				require.NoError(t, err)
			}
			_, err := client.ReadAuthorizationModel(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if s.Code() == codes.OK {
				require.NoError(t, err)
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

	require.Len(t, resp1.GetAuthorizationModels(), 1)
	require.NotEmpty(t, resp1.GetContinuationToken())

	resp2, err := client.ReadAuthorizationModels(context.Background(), &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:           storeID,
		ContinuationToken: resp1.GetContinuationToken(),
	})
	require.NoError(t, err)

	require.Len(t, resp2.GetAuthorizationModels(), 1)
	require.Empty(t, resp2.GetContinuationToken())
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
			name: "zero_type_definitions",
			input: &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         ulid.Make().String(),
				TypeDefinitions: []*openfgav1.TypeDefinition{},
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
				_, err = ulid.Parse(response.GetAuthorizationModelId())
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func GRPCWriteAssertionsTest(t *testing.T, client openfgav1.OpenFGAServiceClient) {
	type testData struct {
		model string
	}
	type output struct {
		statusCode   codes.Code
		errorMessage string
	}

	tests := []struct {
		name     string
		input    *openfgav1.WriteAssertionsRequest
		testData *testData
		output   output
	}{
		{
			name: "happy_path",
			testData: &testData{
				model: `
					model
						schema 1.1
					type user

					type document
						relations
							define viewer: [user]`,
			},
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Assertions: []*openfgav1.Assertion{
					{Expectation: true, TupleKey: &openfgav1.AssertionTupleKey{
						Object:   "document:1",
						Relation: "viewer",
						User:     "user:anne",
					}},
				},
			},
			output: output{
				statusCode: codes.OK,
			},
		},
		{
			name:  "empty_request",
			input: &openfgav1.WriteAssertionsRequest{},
			output: output{
				statusCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_too_short",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              "1",
				AuthorizationModelId: ulid.Make().String(),
			},
			output: output{
				statusCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_extra_chars",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              ulid.Make().String() + "A",
				AuthorizationModelId: ulid.Make().String(),
			},
			output: output{
				statusCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_invalid_chars",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              "ABCDEFGHIJKLMNOPQRSTUVWXY@",
				AuthorizationModelId: ulid.Make().String(),
			},
			output: output{
				statusCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_authorization_model_ID_because_extra_chars",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String() + "A",
			},
			output: output{
				statusCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_authorization_model_ID_because_invalid_chars",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: "ABCDEFGHIJKLMNOPQRSTUVWXY@",
			},
			output: output{
				statusCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing_user_in_assertion",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Assertions: []*openfgav1.Assertion{
					{Expectation: true, TupleKey: &openfgav1.AssertionTupleKey{
						Object:   "obj:1",
						Relation: "viewer",
					}},
				},
			},
			output: output{
				statusCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing_relation_in_assertion",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Assertions: []*openfgav1.Assertion{
					{Expectation: true, TupleKey: &openfgav1.AssertionTupleKey{
						Object: "obj:1",
						User:   "user:anne",
					}},
				},
			},
			output: output{
				statusCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing_object_in_assertion",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Assertions: []*openfgav1.Assertion{
					{Expectation: true, TupleKey: &openfgav1.AssertionTupleKey{
						Relation: "viewer",
						User:     "user:anne",
					}},
				},
			},
			output: output{
				statusCode: codes.InvalidArgument,
			},
		},
		{
			name: "model_not_found",
			input: &openfgav1.WriteAssertionsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
				Assertions: []*openfgav1.Assertion{
					{Expectation: true, TupleKey: &openfgav1.AssertionTupleKey{
						Object:   "obj:1",
						Relation: "viewer",
						User:     "user:anne",
					}},
				},
			},
			output: output{
				statusCode: 2001, // ErrorCode_authorization_model_not_found
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.testData != nil {
				modelResp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         test.input.GetStoreId(),
					SchemaVersion:   typesystem.SchemaVersion1_1,
					TypeDefinitions: parser.MustTransformDSLToProto(test.testData.model).GetTypeDefinitions(),
				})
				test.input.AuthorizationModelId = modelResp.GetAuthorizationModelId()
				require.NoError(t, err)
			}
			_, err := client.WriteAssertions(context.Background(), test.input)

			s, ok := status.FromError(err)

			require.True(t, ok)
			require.Equal(t, test.output.statusCode.String(), s.Code().String())

			if s.Code() == codes.OK {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.output.errorMessage)
			}
		})
	}
}
