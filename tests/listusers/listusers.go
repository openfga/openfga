// Package listusers contains integration tests for the ListUsers and StreamedListUsers APIs.
package listusers

import (
	"context"
	"math"
	"testing"

	oldparser "github.com/craigpastro/openfga-dsl-parser/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/yaml"

	listuserstest "github.com/openfga/openfga/internal/test/listusers"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests/check"
)

var writeMaxChunkSize = 40 // chunk write requests into a chunks of this max size

type individualTest struct {
	Name   string
	Stages []*stage
}

type listUsersTests struct {
	Tests []individualTest
}

type testParams struct {
	schemaVersion string
	client        ClientInterface
}

type stage struct {
	Model               string
	Tuples              []*openfgav1.TupleKey
	ListUsersAssertions []*listuserstest.Assertion `json:"listUsersAssertions"`
}

type ClientInterface interface {
	check.ClientInterface
	ListUsers(ctx context.Context, in *openfgav1.ListUsersRequest, opts ...grpc.CallOption) (*openfgav1.ListUsersResponse, error)
}

// RunAllTests will invoke all list users tests
func RunAllTests(t *testing.T, client ClientInterface) {
	t.Run("RunAll", func(t *testing.T) {
		t.Run("ListUsers", func(t *testing.T) {
			t.Parallel()
			testListUsers(t, client)
		})
	})
}

func testListUsers(t *testing.T, client ClientInterface) {
	t.Run("Schema1_1", func(t *testing.T) {
		t.Parallel()
		runSchema1_1ListUsersTests(t, client)
	})
}

func runSchema1_1ListUsersTests(t *testing.T, client ClientInterface) {
	runTests(t, testParams{typesystem.SchemaVersion1_1, client})
}

func runTests(t *testing.T, params testParams) {
	files := []string{
		"tests/consolidated_1_1_tests.yaml",
		//"tests/abac_tests.yaml", TODO add back
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

		var testCases listUsersTests
		err = yaml.Unmarshal(b, &testCases)
		require.NoError(t, err)

		allTestCases = append(allTestCases, testCases.Tests...)
	}

	for _, test := range allTestCases {
		test := test
		runTest(t, test, params, false)
		// TODO uncomment
		// runTest(t, test, params, true)
	}
}

func runTest(t *testing.T, test individualTest, params testParams, contextTupleTest bool) {
	schemaVersion := params.schemaVersion
	client := params.client
	ctx := context.Background()
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
		resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: name})
		require.NoError(t, err)

		storeID := resp.GetId()

		for _, stage := range test.Stages {
			if contextTupleTest && len(stage.Tuples) > 20 {
				// https://github.com/openfga/api/blob/05de9d8be3ee12fa4e796b92dbdd4bbbf87107f2/openfga/v1/openfga.proto#L151
				t.Skipf("cannot send more than 20 contextual tuples in one request")
			}
			// arrange: write model
			var typedefs []*openfgav1.TypeDefinition
			model, err := parser.TransformDSLToProto(stage.Model)
			if err != nil {
				typedefs = oldparser.MustParse(stage.Model)
			} else {
				typedefs = model.GetTypeDefinitions()
			}

			writeModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				SchemaVersion:   schemaVersion,
				TypeDefinitions: typedefs,
				Conditions:      model.GetConditions(),
			})
			require.NoError(t, err)

			tuples := stage.Tuples
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

			if len(stage.ListUsersAssertions) == 0 {
				t.Skipf("no list users assertions defined")
			}

			for _, assertion := range stage.ListUsersAssertions {
				ctxTuples := assertion.ContextualTuples
				if contextTupleTest {
					ctxTuples = append(ctxTuples, stage.Tuples...)
				}

				// assert 1: on regular list users endpoint
				convertedRequest := assertion.Request.ConvertTestListUsersRequest()
				resp, err := client.ListUsers(ctx, &openfgav1.ListUsersRequest{
					StoreId:              storeID,
					AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
					Object:               convertedRequest.GetObject(),
					Relation:             convertedRequest.GetRelation(),
					Filters:              convertedRequest.GetFilters(),
					ContextualTuples: &openfgav1.ContextualTupleKeys{
						TupleKeys: ctxTuples,
					},
				})

				if assertion.ErrorCode == 0 {
					require.NoError(t, err)
					require.ElementsMatch(t, assertion.Expectation, resp.GetUsers())

					// assert 2: each object in the response of ListUsers should return check -> true
					for _, user := range resp.GetUsers() {
						checkResp, err := client.Check(ctx, &openfgav1.CheckRequest{
							StoreId:              storeID,
							TupleKey:             tuple.NewCheckRequestTupleKey(assertion.Request.Object, assertion.Request.Relation, tuple.ObjectKey(user)),
							AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
							ContextualTuples: &openfgav1.ContextualTupleKeys{
								TupleKeys: ctxTuples,
							},
							Context: assertion.Context,
						})
						require.NoError(t, err)
						require.True(t, checkResp.GetAllowed())
					}
				} else {
					require.Error(t, err)
					e, ok := status.FromError(err)
					require.True(t, ok)
					require.Equal(t, assertion.ErrorCode, int(e.Code()))
				}

				// TODO assert on StreamingListUsers endpoint
			}
		}
	})
}
