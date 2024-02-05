// Package check contains integration tests for the Check API.
package check

import (
	"context"
	"fmt"
	"math"
	"testing"

	oldparser "github.com/craigpastro/openfga-dsl-parser/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/yaml"

	"github.com/openfga/openfga/assets"
	checktest "github.com/openfga/openfga/internal/test/check"
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
	Model           string
	Tuples          []*openfgav1.TupleKey
	CheckAssertions []*checktest.Assertion `json:"checkAssertions"`
}

// ClientInterface defines client interface for running check tests
type ClientInterface interface {
	tests.TestClientBootstrapper
	Check(ctx context.Context, in *openfgav1.CheckRequest, opts ...grpc.CallOption) (*openfgav1.CheckResponse, error)
}

// RunAllTests will run all check tests
func RunAllTests(t *testing.T, client ClientInterface) {
	t.Run("RunAllTests", func(t *testing.T) {
		t.Run("Check", func(t *testing.T) {
			t.Parallel()
			testCheck(t, client)
		})
	})
}

func testCheck(t *testing.T, client ClientInterface) {
	t.Run("Schema1_1", func(t *testing.T) {
		t.Parallel()
		runSchema1_1CheckTests(t, client)
	})
}

func runSchema1_1CheckTests(t *testing.T, client ClientInterface) {
	runTests(t, testParams{typesystem.SchemaVersion1_1, client})
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

		for _, stage := range test.Stages {
			// arrange: write model
			var typedefs []*openfgav1.TypeDefinition
			model, err := parser.TransformDSLToProto(stage.Model)
			if err != nil {
				typedefs = oldparser.MustParse(stage.Model)
			} else {
				typedefs = model.TypeDefinitions
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
						AuthorizationModelId: writeModelResponse.AuthorizationModelId,
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
			for _, assertion := range stage.CheckAssertions {
				detailedInfo := fmt.Sprintf("Check request: %s. Model: %s. Tuples: %s. Contextual tuples: %s", assertion.Tuple, stage.Model, stage.Tuples, assertion.ContextualTuples)

				ctxTuples := assertion.ContextualTuples
				if contextTupleTest {
					ctxTuples = append(ctxTuples, stage.Tuples...)
				}

				var tupleKey *openfgav1.CheckRequestTupleKey
				if assertion.Tuple != nil {
					tupleKey = &openfgav1.CheckRequestTupleKey{
						User:     assertion.Tuple.User,
						Relation: assertion.Tuple.Relation,
						Object:   assertion.Tuple.Object,
					}
				}
				resp, err := client.Check(ctx, &openfgav1.CheckRequest{
					StoreId:              storeID,
					AuthorizationModelId: writeModelResponse.AuthorizationModelId,
					TupleKey:             tupleKey,
					ContextualTuples: &openfgav1.ContextualTupleKeys{
						TupleKeys: ctxTuples,
					},
					Context: assertion.Context,
					Trace:   true,
				})

				if assertion.ErrorCode == 0 {
					require.NoError(t, err, detailedInfo)
					require.Equal(t, assertion.Expectation, resp.Allowed, detailedInfo)
				} else {
					require.Error(t, err, detailedInfo)
					e, ok := status.FromError(err)
					require.True(t, ok, detailedInfo)
					require.Equal(t, assertion.ErrorCode, int(e.Code()), detailedInfo)
				}
			}
		}
	})
}
