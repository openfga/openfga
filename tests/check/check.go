// Package check contains integration tests for the query APIs (ListObjects, ListUsers and Check)
package check

import (
	"context"
	"fmt"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"math"
	"sigs.k8s.io/yaml"
	"strconv"
	"strings"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/assets"
	checktest "github.com/openfga/openfga/internal/test/check"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
)

var writeMaxChunkSize = 40 // chunk write requests into a chunks of this max size

type individualTest struct {
	Name   string
	Stages []*stage
}

type checkTests struct {
	Tests []individualTest
}

type testParams struct {
	schemaVersion string
	client        ClientInterface
}

// stage is a stage of a test. All stages will be run in a single store.
type stage struct {
	Name            string // optional
	Model           string
	Tuples          []*openfgav1.TupleKey
	CheckAssertions []*checktest.Assertion `json:"checkAssertions"`
}

// ClientInterface defines client interface for running check tests.
type ClientInterface interface {
	tests.TestClientBootstrapper
	Check(ctx context.Context, in *openfgav1.CheckRequest, opts ...grpc.CallOption) (*openfgav1.CheckResponse, error)
	ListUsers(ctx context.Context, in *openfgav1.ListUsersRequest, opts ...grpc.CallOption) (*openfgav1.ListUsersResponse, error)
	ListObjects(ctx context.Context, in *openfgav1.ListObjectsRequest, opts ...grpc.CallOption) (*openfgav1.ListObjectsResponse, error)
}

func RunTestMatrix(t *testing.T, ds storage.OpenFGADatastore, experimentalsEnabled bool) {
	// if this takes in a datastore, instead of an engine string
	// then it'll work for the purposes of sandcastle
	t.Run("test_matrix_"+"TODO get logging better"+"_experimental_"+strconv.FormatBool(experimentalsEnabled), func(t *testing.T) {
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		//cfg := config.MustDefaultConfig()
		//if experimental {
		//	cfg.Experimentals = append(cfg.Experimentals, "enable-check-optimizations")
		//}
		//cfg.Log.Level = "error"
		//cfg.Datastore.Engine = engine
		//cfg.ListUsersDeadline = 0   // no deadline
		//cfg.ListObjectsDeadline = 0 // no deadline
		//// extend the timeout for the tests, coverage makes them slower
		//cfg.RequestTimeout = 10 * time.Second
		//
		//cfg.CheckIteratorCache.Enabled = true
		//
		//// you'll have to modify this
		//// or create a new one, this may be used in a many places
		//tests.StartServer(t, cfg)
		//
		//conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)
		//
		//runTestMatrix(t, testParams{typesystem.SchemaVersion1_1, openfgav1.NewOpenFGAServiceClient(conn)})
		runTestMatrix(t, ds, experimentalsEnabled)
	})
}

// RunAllTests will run all check tests.
func RunAllTests(t *testing.T, client ClientInterface) {
	t.Run("RunAllTests", func(t *testing.T) {
		t.Run("Check", func(t *testing.T) {
			t.Parallel()
			runTests(t, testParams{typesystem.SchemaVersion1_1, client})
		})
	})
}

func runTests(t *testing.T, params testParams) {
	files := []string{
		"tests/consolidated_1_1_tests.yaml",
		"tests/abac_tests.yaml",
	}

	var allTestCases []individualTest

	for _, file := range files {
		var b []byte
		var err error
		schemaVersion := params.schemaVersion
		if schemaVersion == typesystem.SchemaVersion1_1 {
			b, err = assets.EmbedTests.ReadFile(file)
		}
		require.NoError(t, err)

		var testCases checkTests
		err = yaml.Unmarshal(b, &testCases)
		require.NoError(t, err)

		allTestCases = append(allTestCases, testCases.Tests...)
	}

	for _, test := range allTestCases {
		test := test
		runTest(t, test, params, false)
		runTest(t, test, params, true)
	}
}

func runTest(t *testing.T, test individualTest, params testParams, contextTupleTest bool) {
	schemaVersion := params.schemaVersion
	client := params.client
	name := test.Name

	if contextTupleTest {
		name += "_ctxTuples"
	}

	t.Run(name, func(t *testing.T) {
		if contextTupleTest && len(test.Stages) > 1 {
			// we don't want to run special contextual tuples test for these cases
			// as multi-stages test has expectation tuples are in system
			t.Skipf("multi-stages test has expectation tuples are in system")
		}

		t.Parallel()
		ctx := context.Background()

		resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: name})
		require.NoError(t, err)

		storeID := resp.GetId()

		for stageNumber, stage := range test.Stages {
			t.Run(fmt.Sprintf("stage_%d", stageNumber), func(t *testing.T) {
				if contextTupleTest && len(stage.Tuples) > 100 {
					// https://github.com/openfga/api/blob/6e048d8023f434cb7a1d3943f41bdc3937d4a1bf/openfga/v1/openfga.proto#L222
					t.Skipf("cannot send more than 100 contextual tuples in one request")
				}
				// arrange: write model
				model := testutils.MustTransformDSLToProtoWithID(stage.Model)

				writeModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					SchemaVersion:   schemaVersion,
					TypeDefinitions: model.GetTypeDefinitions(),
					Conditions:      model.GetConditions(),
				})
				require.NoError(t, err)

				tuples := testutils.Shuffle(stage.Tuples)
				tuplesLength := len(tuples)
				// arrange: write tuples
				if tuplesLength > 0 && !contextTupleTest {
					for i := 0; i < tuplesLength; i += writeMaxChunkSize {
						end := int(math.Min(float64(i+writeMaxChunkSize), float64(tuplesLength)))
						writeChunk := (tuples)[i:end]
						_, err = client.Write(ctx, &openfgav1.WriteRequest{
							StoreId:              storeID,
							AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
							Writes: &openfgav1.WriteRequestWrites{
								TupleKeys: writeChunk,
							},
						})
						require.NoError(t, err)
					}
				}

				if len(stage.CheckAssertions) == 0 {
					t.Skipf("no check assertions defined")
				}
				for assertionNumber, assertion := range stage.CheckAssertions {
					t.Run(fmt.Sprintf("assertion_%d", assertionNumber), func(t *testing.T) {
						detailedInfo := fmt.Sprintf("Check request: %s. Model: %s. Tuples: %s. Contextual tuples: %s", assertion.Tuple, stage.Model, stage.Tuples, assertion.ContextualTuples)

						ctxTuples := testutils.Shuffle(assertion.ContextualTuples)
						if contextTupleTest {
							ctxTuples = append(ctxTuples, stage.Tuples...)
						}

						var tupleKey *openfgav1.CheckRequestTupleKey
						if assertion.Tuple != nil {
							tupleKey = &openfgav1.CheckRequestTupleKey{
								User:     assertion.Tuple.GetUser(),
								Relation: assertion.Tuple.GetRelation(),
								Object:   assertion.Tuple.GetObject(),
							}
						}
						resp, err := client.Check(ctx, &openfgav1.CheckRequest{
							StoreId:              storeID,
							AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
							TupleKey:             tupleKey,
							ContextualTuples: &openfgav1.ContextualTupleKeys{
								TupleKeys: ctxTuples,
							},
							Context: assertion.Context,
							Trace:   true,
						})

						if assertion.ErrorCode == 0 {
							require.NoError(t, err, detailedInfo)
							require.Equal(t, assertion.Expectation, resp.GetAllowed(), detailedInfo)
						} else {
							require.Error(t, err, detailedInfo)
							e, ok := status.FromError(err)
							require.True(t, ok, detailedInfo)
							require.Equal(t, assertion.ErrorCode, int(e.Code()), detailedInfo)
						}
					})
				}
			})
		}
	})
}

var matrix = individualTest{
	Name: "complete_testing_model",
	Stages: []*stage{
		{
			Name: "object_directs_user_relation_direct",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:1", Relation: "direct", User: "user:valid"},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:        "valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct", User: "user:valid"},
					Expectation: true,
				},
				{
					Name:        "invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct", User: "user:invalid"},
					Expectation: false,
				},
				{
					Name:        "ignore_valid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "ignore_invalid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: true,
				},
				{
					Name:        "ignore_valid_cond_invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: false,
				},
				{
					Name:        "ignore_invalid_cond_invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
			},
		},
		{
			Name: "object_directs_user_relation_direct_cond",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:1", Relation: "direct_cond", User: "user:valid", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:      "valid_user_no_cond",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond", User: "user:valid"},
					ErrorCode: 2000,
				},
				{
					Name:               "invalid_user_no_cond",
					Tuple:              &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond", User: "user:invalid"},
					Expectation:        false,
					ListUsersErrorCode: 2000,
				},
				{
					Name:        "valid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "invalid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:        "valid_cond_invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: false,
				},
				{
					Name:        "invalid_cond_invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
			},
		},
		{
			Name: "object_directs_user_relation_direct_wild",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:1", Relation: "direct_wild", User: "user:*"},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:        "valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild", User: "user:valid"},
					Expectation: true,
				},
				{
					Name:        "not_public_id",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:not-public", Relation: "direct_wild", User: "user:valid"},
					Expectation: false,
				},
				{
					Name:        "self",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild", User: "user:*"},
					Expectation: true,
				},
				{
					Name:        "ignore_valid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "ignore_invalid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: true,
				},
				{
					Name:        "ignore_valid_cond_invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "invalid_cond_invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: true,
				},
				{
					Name:        "ignore_valid_cond_not_public_id",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:not-public", Relation: "direct_wild", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: false,
				},
				{
					Name:        "ignore_invalid_cond_not_public_id",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:not-public", Relation: "direct_wild", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
			},
		},
		{
			Name: "object_directs_user_relation_direct_wild_cond",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:1", Relation: "direct_wild_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:      "valid_user_no_cond",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild_cond", User: "user:valid"},
					ErrorCode: 2000,
				},
				{
					Name:                 "not_public_id_no_cond",
					Tuple:                &openfgav1.TupleKey{Object: "directs-user:not-public", Relation: "direct_wild_cond", User: "user:valid"},
					Expectation:          false,
					ListObjectsErrorCode: 2000, // any tuple with user:* and a condition and missing context will be un-evaluable
				},
				{
					Name:      "self_no_cond",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild_cond", User: "user:*"},
					ErrorCode: 2000,
				},
				{
					Name:        "self_valid_cond",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild_cond", User: "user:*"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "self_invalid_cond",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild_cond", User: "user:*"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:        "valid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "invalid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:        "valid_cond_invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild_cond", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "invalid_cond_invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wild_cond", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:        "valid_cond_not_public_id",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:not-public", Relation: "direct_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: false,
				},
				{
					Name:        "invalid_cond_not_public_id",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:not-public", Relation: "direct_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
			},
		},
		{
			Name: "object_directs_user_relation_direct_and_direct_cond",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:1", Relation: "direct_and_direct_cond", User: "user:valid"},
				{Object: "directs-user:1", Relation: "direct_and_direct_cond", User: "user:validwithcond", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:               "valid_user",
					Tuple:              &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_cond", User: "user:valid"},
					Expectation:        true,
					ListUsersErrorCode: 2000,
				},
				{
					Name:               "invalid_user",
					Tuple:              &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_cond", User: "user:invalid"},
					Expectation:        false,
					ListUsersErrorCode: 2000,
				},
				{
					Name:        "ignore_valid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "ignore_invalid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: true,
				},
				{
					Name:        "ignore_valid_cond_invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_cond", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: false,
				},
				{
					Name:        "ignore_invalid_cond_invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_cond", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:      "valid_userwithcond_user",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_cond", User: "user:validwithcond"},
					ErrorCode: 2000,
				},
				{
					Name:        "valid_cond_valid_validwithcond_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_cond", User: "user:validwithcond"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "invalid_cond_validwithcond_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_cond", User: "user:validwithcond"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
			},
		},
		{
			Name: "object_directs_user_relation_direct_and_direct_wild",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:1", Relation: "direct_and_direct_wild", User: "user:*"},
				{Object: "directs-user:2", Relation: "direct_and_direct_wild", User: "user:viewer"},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:        "valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_wild", User: "user:valid"},
					Expectation: true,
				},
				{
					Name:        "valid_employee",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_wild", User: "employee:valid"},
					Expectation: false,
				},
				{
					Name:        "valid_user_not_public",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_and_direct_wild", User: "user:valid"},
					Expectation: false,
				},
				{
					Name:        "valid_employee_not_public",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_and_direct_wild", User: "employee:valid"},
					Expectation: false,
				},
				{
					Name:        "viewer_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_and_direct_wild", User: "user:viewer"},
					Expectation: true,
				},
			},
		},
		{
			Name: "object_directs_user_relation_direct_and_direct_wild_cond",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:1", Relation: "direct_and_direct_wild_cond", User: "user:*"},
				{Object: "directs-user:2", Relation: "direct_and_direct_wild_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:                 "valid_user",
					Tuple:                &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_wild_cond", User: "user:valid"},
					Expectation:          true,
					ListObjectsErrorCode: 2000, // any tuple with user:* and a condition and missing context will be un-evaluable
				},
				{
					Name:      "valid_user_not_public",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_and_direct_wild_cond", User: "user:valid"},
					ErrorCode: 2000,
				},
				{
					Name:      "valid_user_no_cond",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_and_direct_wild_cond", User: "user:valid"},
					ErrorCode: 2000,
				},
				{
					Name:        "valid_user_with_invalid_cond",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_and_direct_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:        "valid_user_with_valid_cond",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_and_direct_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "ignore_valid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "ignore_invalid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_and_direct_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: true,
				},
			},
		},
		{
			Name: "object_directs_user_relation_direct_cond_and_direct_wild",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild", User: "user:valid", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
				{Object: "directs-user:2", Relation: "direct_cond_and_direct_wild", User: "user:*"},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:      "valid_user_no_cond",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild", User: "user:valid"},
					ErrorCode: 2000,
				},
				{
					Name:        "valid_user_invalid_cond",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:        "valid_user_valid_cond",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:                 "valid_user_public_doc",
					Tuple:                &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_cond_and_direct_wild", User: "user:valid"},
					Expectation:          true,
					ListObjectsErrorCode: 2000, // any tuple with user:* and a condition and missing context will be un-evaluable
				},
				{
					Name:               "invalid_user_no_cond",
					Tuple:              &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild", User: "user:invalid"},
					Expectation:        false,
					ListUsersErrorCode: 2000, // any tuple with user:* and a condition and missing context will be un-evaluable
				},
				{
					Name:        "invalid_user_invalid_cond",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:        "invalid_user_valid_cond",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: false,
				},
				{
					Name:        "invalid_user_public_doc",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_cond_and_direct_wild", User: "user:invalid"},
					Expectation: true,
				},
				{
					Name:        "valid_user_ignore_invalid_cond",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_cond_and_direct_wild", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: true,
				},
				{
					Name:        "valid_user_with_valid_cond",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_cond_and_direct_wild", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "ignore_valid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "invalid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
			},
		},
		{
			Name: "object_directs_user_relation_direct_cond_and_direct_wild_cond",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild_cond", User: "user:valid", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
				{Object: "directs-user:2", Relation: "direct_cond_and_direct_wild_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:      "valid_user_no_cond",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild_cond", User: "user:valid"},
					ErrorCode: 2000,
				},
				{
					Name:        "valid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "invalid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:                 "invalid_user_no_cond",
					Tuple:                &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild_cond", User: "user:invalid"},
					Expectation:          false,
					ListObjectsErrorCode: 2000, // any tuple with user:* and a condition and missing context will be un-evaluable
					ListUsersErrorCode:   2000, // any tuple with user:* and a condition and missing context will be un-evaluable
				},
				{
					Name:        "valid_cond_invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild_cond", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: false,
				},
				{
					Name:        "invalid_cond_invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_cond_and_direct_wild_cond", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:      "valid_user_no_cond_public",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_cond_and_direct_wild_cond", User: "user:valid"},
					ErrorCode: 2000,
				},
				{
					Name:        "valid_user_with_invalid_cond_public",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_cond_and_direct_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:        "valid_user_with_valid_cond_public",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_cond_and_direct_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
			},
		},

		{
			Name: "object_directs_user_relation_direct_wildcard_and_direct_wildcard_cond",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:1", Relation: "direct_wildcard_and_direct_wildcard_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
				{Object: "directs-user:2", Relation: "direct_wildcard_and_direct_wildcard_cond", User: "user:*"},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:      "valid_user_no_cond",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wildcard_and_direct_wildcard_cond", User: "user:valid"},
					ErrorCode: 2000,
				},
				{
					Name:        "valid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wildcard_and_direct_wildcard_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "invalid_cond_valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:1", Relation: "direct_wildcard_and_direct_wildcard_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:                 "valid_user_no_cond_public",
					Tuple:                &openfgav1.TupleKey{Object: "directs-user:2", Relation: "direct_wildcard_and_direct_wildcard_cond", User: "user:valid"},
					Expectation:          true,
					ListObjectsErrorCode: 2000, // any tuple with user:* and a condition and missing context will be un-evaluable
				},
			},
		},
		{
			Name: "computed",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:computed", Relation: "direct", User: "user:valid"},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:        "valid",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed", Relation: "computed", User: "user:valid"},
					Expectation: true,
				},
				{
					Name:        "invalid",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed", Relation: "computed", User: "user:invalid"},
					Expectation: false,
				},
				{
					Name:        "valid_cond_ignored_falsy",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed", Relation: "computed", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: true,
				},
				{
					Name:        "valid_cond_ignored_ruthy",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed", Relation: "computed", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
			},
		},
		{
			Name: "computed_cond",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:computed_cond", Relation: "direct_cond", User: "user:valid", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:      "valid_cond_missing",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:computed_cond", Relation: "computed_cond", User: "user:valid"},
					ErrorCode: 2000,
				},
				{
					Name:        "valid_cond_falsy",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_cond", Relation: "computed_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:        "valid_cond_truthy",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_cond", Relation: "computed_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "invalid",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_cond", Relation: "computed_cond", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: false,
				},
			},
		},
		{
			Name: "computed_wild",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:computed_wild", Relation: "direct_wild", User: "user:*"},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:        "valid_1",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_wild", Relation: "computed_wild", User: "user:random1"},
					Expectation: true,
				},
				{
					Name:        "valid_2",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_wild", Relation: "computed_wild", User: "user:random2"},
					Expectation: true,
				},
				{
					Name:        "valid_1_condition_ignored_truthy",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_wild", Relation: "computed_wild", User: "user:random1"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "valid_1_condition_ignored_falsey",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_wild", Relation: "computed_wild", User: "user:random1"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: true,
				},
			},
		},
		{
			Name: "computed_wild_cond",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:computed_wild_cond", Relation: "direct_wild_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:      "valid_wildcard_cond_missing",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:computed_wild_cond", Relation: "computed_wild_cond", User: "user:*"},
					ErrorCode: 2000,
				},
				{
					Name:        "valid_wildcard_cond_truthy",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_wild_cond", Relation: "computed_wild_cond", User: "user:*"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:      "valid_cond_missing",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:computed_wild_cond", Relation: "computed_wild_cond", User: "user:valid"},
					ErrorCode: 2000,
				},
				{
					Name:        "valid_cond_truthy",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_wild_cond", Relation: "computed_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "valid_cond_falsey",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_wild_cond", Relation: "computed_wild_cond", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
			},
		},
		{
			Name: "computed_computed",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:computed_computed", Relation: "direct", User: "user:valid"},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:        "valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_computed", Relation: "computed_computed", User: "user:valid"},
					Expectation: true,
				},
				{
					Name:        "invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_computed", Relation: "computed_computed", User: "user:invalid"},
					Expectation: false,
				},
				{
					Name:        "valid_user_cond_ignored_truthy",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_computed", Relation: "computed_computed", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "valid_user_cond_ignored_falsey",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_computed", Relation: "computed_computed", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: true,
				},
			},
		},
		{
			Name: "computed_computed_computed",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:computed_computed_computed", Relation: "direct", User: "user:valid"},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:        "valid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_computed_computed", Relation: "computed_computed_computed", User: "user:valid"},
					Expectation: true,
				},
				{
					Name:        "invalid_user",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_computed_computed", Relation: "computed_computed_computed", User: "user:invalid"},
					Expectation: false,
				},
				{
					Name:        "valid_user_cond_ignored_truthy",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_computed_computed", Relation: "computed_computed_computed", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "valid_user_cond_ignored_falsey",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:computed_computed_computed", Relation: "computed_computed_computed", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: true,
				},
			},
		},
		{
			Name: "or_computed",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:or_computed", Relation: "direct", User: "user:valid1"},
				{Object: "directs-user:or_computed", Relation: "direct_cond", User: "user:valid2", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
				{Object: "directs-user:or_computed3", Relation: "direct_wild", User: "user:*"},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:               "path_1",
					Tuple:              &openfgav1.TupleKey{Object: "directs-user:or_computed", Relation: "or_computed", User: "user:valid1"},
					Expectation:        true,
					ListUsersErrorCode: 2000,
				},
				{
					Name:               "path_1_no_user",
					Tuple:              &openfgav1.TupleKey{Object: "directs-user:or_computed", Relation: "or_computed", User: "user:invalid"},
					Expectation:        false,
					ListUsersErrorCode: 2000,
				},
				{
					Name:      "path_2_without_cond",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:or_computed", Relation: "or_computed", User: "user:valid2"},
					ErrorCode: 2000,
				},
				{
					Name:        "path_2_with_cond_falsey",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:or_computed", Relation: "or_computed", User: "user:valid2"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:        "path_2_with_cond_truthy",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:or_computed", Relation: "or_computed", User: "user:valid2"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "path_2_invalid_with_cond_truthy",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:or_computed", Relation: "or_computed", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: false,
				},
				{
					Name:        "path_3",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:or_computed3", Relation: "or_computed", User: "user:valid3"},
					Expectation: true,
				},
			},
		},
		{
			Name: "and_computed",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:and_computed", Relation: "direct_cond", User: "user:valid", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
				{Object: "directs-user:and_computed", Relation: "direct_wild", User: "user:*"},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:      "user_valid_missing_cond",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:and_computed", Relation: "and_computed", User: "user:valid"},
					ErrorCode: 2000,
				},
				{
					Name:        "user_valid_cond_truthy",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:and_computed", Relation: "and_computed", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
				{
					Name:        "user_valid_cond_falsey",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:and_computed", Relation: "and_computed", User: "user:valid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:        "user_invalid_cond_truthy",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:and_computed", Relation: "and_computed", User: "user:invalid"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: false,
				},
			},
		},
		{
			Name: "butnot_computed",
			Tuples: []*openfgav1.TupleKey{
				{Object: "directs-user:butnot_computed", Relation: "direct_wild_cond", User: "user:*", Condition: &openfgav1.RelationshipCondition{Name: "xcond"}},
				{Object: "directs-user:butnot_computed", Relation: "direct", User: "user:diff"},
			},
			CheckAssertions: []*checktest.Assertion{
				{
					Name:                 "base_err_diff_true",
					Tuple:                &openfgav1.TupleKey{Object: "directs-user:butnot_computed", Relation: "butnot_computed", User: "user:diff"},
					Expectation:          false,
					ListObjectsErrorCode: 2000, // any tuple with user:* and a condition and missing context will be un-evaluable
					ListUsersErrorCode:   2000, // any tuple with user:* and a condition and missing context will be un-evaluable
				},
				{
					Name:      "base_err_diff_false",
					Tuple:     &openfgav1.TupleKey{Object: "directs-user:butnot_computed", Relation: "butnot_computed", User: "user:another"},
					ErrorCode: 2000,
				},
				{
					Name:        "base_false_diff_true",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:butnot_computed", Relation: "butnot_computed", User: "user:diff"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:        "base_false_diff_false",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:butnot_computed", Relation: "butnot_computed", User: "user:another"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("2")}},
					Expectation: false,
				},
				{
					Name:        "base_true_diff_true",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:butnot_computed", Relation: "butnot_computed", User: "user:diff"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: false,
				},
				{
					Name:        "base_true_diff_false",
					Tuple:       &openfgav1.TupleKey{Object: "directs-user:butnot_computed", Relation: "butnot_computed", User: "user:another"},
					Context:     &structpb.Struct{Fields: map[string]*structpb.Value{"x": structpb.NewStringValue("1")}},
					Expectation: true,
				},
			},
		},
	},
}

func runTestMatrix(t *testing.T, ds storage.OpenFGADatastore, experimentalsEnabled bool) {
	modelStr := `model
  schema 1.1
type user
type employee
# yes, wildcard is a userset instead of a direct, makes it easier and tests the userset path
type directs-user
  relations
    define direct: [user]
    define direct_cond: [user with xcond]
    define direct_wild: [user:*]
    define direct_wild_cond: [user:* with xcond]
    define direct_and_direct_cond: [user, user with xcond, employee]
    define direct_and_direct_wild: [user, user:*, employee:*]
    define direct_and_direct_wild_cond: [user, user:* with xcond]
    define direct_cond_and_direct_wild: [user with xcond, user:*]
    define direct_cond_and_direct_wild_cond: [user with xcond, user:* with xcond]
    define direct_wildcard_and_direct_wildcard_cond: [user:*, user:* with xcond]
    define computed: direct
    define computed_cond: direct_cond
    define computed_wild: direct_wild
    define computed_wild_cond: direct_wild_cond
    define computed_computed: computed
    define computed_computed_computed: computed_computed
    define or_computed: computed or computed_cond or direct_wild
    define or_computed_no_cond: computed or direct_wild
    define and_computed: computed_cond and computed_wild
    define butnot_computed: computed_wild_cond but not computed_computed
    define butnot_computed_cond: computed_cond but not computed_computed
    define alg_combined: butnot_computed_cond but not direct_and_direct_cond
    define tuple_cycle2: [user, usersets-user#tuple_cycle2, employee]  
    define tuple_cycle3: [user, complexity3#cycle_nested]
    define compute_tuple_cycle3: tuple_cycle3
type directs-employee
  relations
    define direct: [employee]
    define direct_2: [employee]
    define computed: direct
    define computed_computed: computed
    define computed_computed_computed: computed_computed
    define or_computed: computed_computed_computed or direct
    define and_computed: or_computed and direct
    define butnot_computed: and_computed but not direct_2
    define direct_cond: [employee with xcond]
    define direct_wild: [employee:*]
    define direct_wild_cond: [employee:* with xcond]
    define alg_combined: butnot_computed and direct_cond
type usersets-user
  relations
    define userset: [directs-user#direct, directs-employee#direct]
    define userset_alg: [directs-user#alg_combined, directs-employee#alg_combined]
    define userset_to_computed: [directs-user#computed, directs-employee#computed]
    define userset_to_computed_cond: [directs-user#computed_cond, directs-employee#direct_cond]
    define userset_to_computed_wild: [directs-user#computed_wild, directs-employee#direct_wild]
    define userset_to_computed_wild_cond: [directs-user#direct_wild_cond, directs-employee#direct_wild_cond]
    define userset_cond: [directs-user#direct with xcond]
    define userset_cond_to_computed: [directs-user#computed with xcond]
    define userset_cond_to_computed_cond: [directs-user#computed_cond with xcond]
    define userset_cond_to_computed_wild: [directs-user#computed_wild with xcond]
    define userset_cond_to_computed_wild_cond: [directs-user#computed_wild_cond with xcond]
    define userset_to_or_computed: [directs-user#or_computed]
    define userset_to_or_computed_no_cond: [directs-user#or_computed_no_cond]
    define userset_to_butnot_computed: [directs-user#butnot_computed]
    define userset_to_and_computed:[directs-user#and_computed]
    define userset_recursive: [user, usersets-user#userset_recursive]
    define userset_recursive_public: [user, user:*, usersets-user#userset_recursive_public]
    define userset_recursive_public_only: [user:*, usersets-user#userset_recursive_public_only]
    define userset_recursive_mixed_direct_assignment: [user, usersets-user#userset_recursive_mixed_direct_assignment, usersets-user#userset]
    define or_userset: userset or userset_to_computed_cond
    define and_userset: userset_to_computed_cond and userset_to_computed_wild
    define butnot_userset: userset_cond_to_computed_wild but not userset_cond
    define nested_or_userset: userset_to_or_computed or userset_to_butnot_computed
    define nested_and_userset: userset_to_and_computed and userset_to_or_computed
    define ttu_direct_userset: [ttus#direct_pa_direct_ch]
    define ttu_direct_cond_userset: [ttus#direct_cond_pa_direct_ch]
    define ttu_or_direct_userset: [ttus#or_comp_from_direct_parent]
    define ttu_and_direct_userset: [ttus#and_comp_from_direct_parent]
    define tuple_cycle2: [ttus#tuple_cycle2]
    define tuple_cycle3: [directs-user#compute_tuple_cycle3]
type ttus
  relations
    define direct_parent: [directs-user]
    define mult_parent_types: [directs-user, directs-employee]
    define mult_parent_types_cond: [directs-user with xcond, directs-employee with xcond]
    define direct_cond_parent: [directs-user with xcond]
    define userset_parent: [usersets-user]
    define userset_cond_parent: [usersets-user with xcond]
    define tuple_cycle2: tuple_cycle2 from direct_parent
    define tuple_cycle3: tuple_cycle3 from userset_parent
    define direct_pa_direct_ch: direct from mult_parent_types
    define direct_cond_pa_direct_ch: direct from mult_parent_types_cond
    define or_comp_from_direct_parent: or_computed from direct_parent
    define and_comp_from_direct_parent: and_computed from direct_cond_parent
    define butnot_comp_from_direct_parent: butnot_computed from direct_cond_parent
    define userset_pa_userset_ch: userset from userset_parent
    define userset_pa_userset_comp_ch: userset_to_computed from userset_parent
    define userset_pa_userset_comp_cond_ch: userset_to_computed_cond from userset_parent
    define userset_pa_userset_comp_wild_ch: userset_to_computed_wild from userset_parent
    define userset_pa_userset_comp_wild_cond_ch: userset_to_computed_wild_cond from userset_parent
    define userset_cond_userset_ch: userset from userset_cond_parent
    define userset_cond_userset_comp_ch: userset_to_computed from userset_cond_parent
    define userset_cond_userset_comp_cond_ch: userset_to_computed_cond from userset_cond_parent
    define userset_cond_userset_comp_wild_ch: userset_to_computed_wild from userset_cond_parent
    define userset_cond_userset_comp_wild_cond_ch: userset_to_computed_wild_cond from userset_cond_parent
    define or_ttu: direct_pa_direct_ch or direct_cond_pa_direct_ch
    define and_ttu: or_comp_from_direct_parent and direct_pa_direct_ch
    define nested_butnot_ttu: or_comp_from_direct_parent but not userset_pa_userset_comp_wild_ch
	define nested_ttu_parent: [ttus]
	define nested_ttu: [directs-user] or nested_ttu from nested_ttu_parent
	define nested_ttu_public: [directs-user, directs-user:*] or nested_ttu_public from nested_ttu_parent

type complexity3
  relations
    define ttu_parent: [ttus]
    define userset_parent: [usersets-user]
    define ttu_userset_ttu: ttu_direct_userset from userset_parent
    define ttu_ttu_userset: userset_pa_userset_ch from ttu_parent
    define userset_ttu_userset: [ttus#userset_pa_userset_ch]
    define userset_userset_ttu: [usersets-user#ttu_direct_userset] 
    define compute_ttu_userset_ttu: ttu_userset_ttu
    define compute_userset_ttu_userset: userset_ttu_userset
    define or_compute_complex3: compute_ttu_userset_ttu or compute_userset_ttu_userset
    define and_nested_complex3: [ttus#and_ttu] and compute_ttu_userset_ttu 
    define cycle_nested: [ttus#tuple_cycle3]   
type complexity4
  relations
    define userset_ttu_userset_ttu: [complexity3#ttu_userset_ttu]
    define ttu_ttu_ttu_userset: ttu_ttu_userset from parent
    define userset_or_compute_complex3: [complexity3#or_compute_complex3]
    define ttu_and_nested_complex3: and_nested_complex3 from parent
    define or_complex4: userset_or_compute_complex3 or ttu_and_nested_complex3
    define parent: [complexity3]
condition xcond(x: string) {
  x == '1'
}`
	//schemaVersion := params.schemaVersion
	//client := params.client
	name := matrix.Name

	ctx := context.Background()

	t.Run(name, func(t *testing.T) {
		stages := matrix.Stages
		stages = append(stages, ttuCompleteTestingModelTest...)
		stages = append(stages, complexityThreeTestingModelTest...)
		stages = append(stages, complexityFourTestingModelTest...)
		stages = append(stages, usersetCompleteTestingModelTest...)
		for _, stage := range stages {
			t.Run("stage_"+stage.Name, func(t *testing.T) {
				// how do we do this in command tests?
				storeID := ulid.Make().String()
				model := testutils.MustTransformDSLToProtoWithID(modelStr)

				// create store
				err := ds.WriteAuthorizationModel(ctx, storeID, model)
				require.NoError(t, err)

				tuples := testutils.Shuffle(stage.Tuples)
				err = ds.Write(context.Background(), storeID, nil, tuples)
				require.NoError(t, err)

				ts, err := typesystem.New(model)
				require.NoError(t, err)

				ctx = typesystem.ContextWithTypesystem(ctx, ts)

				// Iterator cache enabled
				// experimentals yes and no
				//
				//cfg.ListUsersDeadline = 0   // no deadline
				//cfg.ListObjectsDeadline = 0 // no deadline
				//// extend the timeout for the tests, coverage makes them slower
				//cfg.RequestTimeout = 10 * time.Second

				//cfg.CheckIteratorCache.Enabled = true
				localCheckOpts := []graph.LocalCheckerOption{
					//graph.WithResolveNodeBreadthLimit(100),
					graph.WithOptimizations(experimentalsEnabled),
				}
				//cacheOpts := []graph.CachedCheckResolverOpt{
				//	graph.WithCacheTTL(10 * time.Second),
				//}
				checkBuilderOpts := []graph.CheckResolverOrderedBuilderOpt{
					//graph.WithCachedCheckResolverOpts(true, cacheOpts...),
					graph.WithLocalCheckerOpts(localCheckOpts...),
				}

				checkResolver, checkResolverCloser, err := graph.NewOrderedCheckResolvers(checkBuilderOpts...).Build()
				require.NoError(t, err)
				t.Cleanup(checkResolverCloser)

				//////////// OLD
				//resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: name})
				//require.NoError(t, err)
				//storeID := resp.GetId()
				//modelProto := parser.MustTransformDSLToProto(model)
				//writeModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
				//	StoreId:         storeID,
				//	SchemaVersion:   schemaVersion,
				//	TypeDefinitions: modelProto.GetTypeDefinitions(),
				//	Conditions:      modelProto.GetConditions(),
				//})
				//require.NoError(t, err)
				//modelID := writeModelResponse.GetAuthorizationModelId()
				//
				//tuples := testutils.Shuffle(stage.Tuples)
				//tuplesLength := len(tuples)
				//if tuplesLength > 0 {
				//	for i := 0; i < tuplesLength; i += writeMaxChunkSize {
				//		end := int(math.Min(float64(i+writeMaxChunkSize), float64(tuplesLength)))
				//		writeChunk := (tuples)[i:end]
				//		_, err = client.Write(ctx, &openfgav1.WriteRequest{
				//			StoreId:              storeID,
				//			AuthorizationModelId: modelID,
				//			Writes: &openfgav1.WriteRequestWrites{
				//				TupleKeys: writeChunk,
				//			},
				//		})
				//		require.NoError(t, err)
				//	}
				//}

				if len(stage.CheckAssertions) == 0 {
					t.Skipf("no check assertions defined")
				}
				for _, assertion := range stage.CheckAssertions {
					t.Run("assertion_check_"+assertion.Name, func(t *testing.T) {
						query := commands.NewCheckCommand(ds, checkResolver, ts)
						//assertCheck(ctx, t, assertion, stage, client, storeID, modelID)
						assertCheck(ctx, t, assertion, stage, query, storeID)
					})
					//t.Run("assertion_list_objects_"+assertion.Name, func(t *testing.T) {
					//	assertListObjects(ctx, t, assertion, stage, client, storeID, modelID)
					//})
					//t.Run("assertion_list_users_"+assertion.Name, func(t *testing.T) {
					//	assertListUsers(ctx, t, assertion, client, storeID, modelID)
					//})
				}
			})
		}
	})
}

// This will also need to take the datastore
// func assertCheck(ctx context.Context, t *testing.T, assertion *checktest.Assertion, stage *stage, client ClientInterface, storeID string, modelID string) {
func assertCheck(
	ctx context.Context,
	t *testing.T,
	assertion *checktest.Assertion,
	stage *stage,
	checkQuery *commands.CheckQuery,
	storeID string,
	// resolver graph.CheckResolver,
	// ts typesystem.TypeSystem,
) {
	detailedInfo := fmt.Sprintf("Check request: %s. Tuples: %s. Contextual tuples: %s", assertion.Tuple, stage.Tuples, assertion.ContextualTuples)

	var tupleKey *openfgav1.CheckRequestTupleKey
	if assertion.Tuple != nil {
		tupleKey = &openfgav1.CheckRequestTupleKey{
			User:     assertion.Tuple.GetUser(),
			Relation: assertion.Tuple.GetRelation(),
			Object:   assertion.Tuple.GetObject(),
		}
	}

	//checkQuery := commands.NewCheckCommand(ds, resolver, ts)
	resp, _, err := checkQuery.Execute(ctx, &commands.CheckCommandParams{
		StoreID:  storeID,
		TupleKey: tupleKey,
		Context:  assertion.Context,
	})

	//resp, err := client.Check(ctx, &openfgav1.CheckRequest{
	//	StoreId:              storeID,
	//	AuthorizationModelId: modelID,
	//	TupleKey:             tupleKey,
	//	ContextualTuples: &openfgav1.ContextualTupleKeys{
	//		// TODO
	//		TupleKeys: []*openfgav1.TupleKey{},
	//	},
	//	Context: assertion.Context,
	//	Trace:   true,
	//})

	if assertion.ErrorCode == 0 {
		require.NoError(t, err, detailedInfo)
		require.Equal(t, assertion.Expectation, resp.GetAllowed(), detailedInfo)
	} else {
		// these will be wrong
		require.Error(t, err, detailedInfo)
		e, ok := status.FromError(err)
		require.True(t, ok, detailedInfo)
		require.Equal(t, assertion.ErrorCode, int(e.Code()), detailedInfo)
	}
}

func assertListObjects(ctx context.Context, t *testing.T, assertion *checktest.Assertion, stage *stage, client ClientInterface, storeID string, modelID string) {
	objectType, _ := tuple.SplitObject(assertion.Tuple.GetObject())
	resp, err := client.ListObjects(ctx, &openfgav1.ListObjectsRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Type:                 objectType,
		User:                 assertion.Tuple.GetUser(),
		Relation:             assertion.Tuple.GetRelation(),
		ContextualTuples: &openfgav1.ContextualTupleKeys{
			// TODO
			TupleKeys: []*openfgav1.TupleKey{},
		},
		Context: assertion.Context,
	})

	if assertion.ListObjectsErrorCode != 0 {
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, assertion.ListObjectsErrorCode, int(e.Code()))
		return
	}
	if strings.Contains(stage.Name, "usersets_userset_cond_to_computed_cond") {
		require.NoError(t, err)
		return
	}
	if assertion.ErrorCode != 0 {
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, assertion.ErrorCode, int(e.Code()))
		return
	}

	require.NoError(t, err)
	if assertion.Expectation {
		require.NotEmpty(t, resp.GetObjects(), "at least one object should be returned")
		require.Contains(t, resp.GetObjects(), assertion.Tuple.GetObject(), "object should be returned in the response")
	} else {
		require.NotContains(t, resp.GetObjects(), assertion.Tuple.GetObject(), "object should not be in the response")
	}
}

func assertListUsers(ctx context.Context, t *testing.T, assertion *checktest.Assertion, client ClientInterface, storeID string, modelID string) {
	objectType, objectID := tuple.SplitObject(assertion.Tuple.GetObject())
	userObject, userRelation := tuple.SplitObjectRelation(assertion.Tuple.GetUser())
	userObjectType, _ := tuple.SplitObject(userObject)
	relation := assertion.Tuple.GetRelation()
	resp, err := client.ListUsers(ctx, &openfgav1.ListUsersRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Object:               &openfgav1.Object{Type: objectType, Id: objectID},
		Relation:             relation,
		UserFilters:          []*openfgav1.UserTypeFilter{{Type: userObjectType, Relation: userRelation}},
		ContextualTuples:     []*openfgav1.TupleKey{}, // TODO
		Context:              assertion.Context,
	})

	if assertion.ListUsersErrorCode != 0 {
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, assertion.ListUsersErrorCode, int(e.Code()))
		return
	}
	if assertion.ErrorCode != 0 {
		require.Error(t, err)
		e, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, assertion.ErrorCode, int(e.Code()))
		return
	}

	require.NoError(t, err)
	responseUsers := make([]string, len(resp.GetUsers()))
	wildcardUserPresent := false
	for i, u := range resp.GetUsers() {
		responseUsers[i] = tuple.UserProtoToString(u)
		if strings.HasSuffix(responseUsers[i], ":*") {
			wildcardUserPresent = true
		}
	}
	if assertion.Expectation {
		if wildcardUserPresent {
			// if ListUsers response is [user:*, ...], we don't want to do the assertions below because they will fail.
			return
		}
		require.NotEmpty(t, responseUsers, "at least one user should be returned")
		require.Contains(t, responseUsers, assertion.Tuple.GetUser(), "user should be returned in response")
	} else {
		require.NotContains(t, responseUsers, assertion.Tuple.GetUser(), "user should not be returned in the response")
	}
}
