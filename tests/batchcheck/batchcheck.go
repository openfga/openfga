// Package batchcheck contains integration tests for the BatchCheck api
package batchcheck

import (
	"context"
	"fmt"
	"github.com/oklog/ulid/v2"
	batchchecktest "github.com/openfga/openfga/internal/test/batchcheck"
	"math"
	"testing"

	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/openfga/openfga/pkg/typesystem"
	"sigs.k8s.io/yaml"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/tests/check"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const writeMaxChunkSize = 40

type ClientInterface interface {
	check.ClientInterface
	BatchCheck(ctx context.Context, in *openfgav1.BatchCheckRequest, opts ...grpc.CallOption) (*openfgav1.BatchCheckResponse, error)
}

// RunAllTests will invoke all BatchCheck tests.
func RunAllTests(t *testing.T, client ClientInterface) {
	t.Run("RunAll", func(t *testing.T) {
		t.Run("BatchCheck", func(t *testing.T) {
			t.Parallel()
			files := []string{
				"tests/consolidated_1_1_tests.yaml",
				"tests/abac_tests.yaml",
			}

			var allTestCases []check.IndividualTest

			for _, file := range files {
				var b []byte
				var err error
				b, err = assets.EmbedTests.ReadFile(file)
				require.NoError(t, err)

				var testCases check.CheckTests
				err = yaml.Unmarshal(b, &testCases)
				require.NoError(t, err)

				allTestCases = append(allTestCases, testCases.Tests...)
			}

			for _, test := range allTestCases {
				test := test
				runTest(t, test, client, false)
				// TODO: context tuple tests
				//runTest(t, test, client, true)
			}
		})
	})
}

func runTest(t *testing.T, test check.IndividualTest, client ClientInterface, contextTupleTest bool) {
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

		for stageNumber, stage := range test.Stages {
			// don't need to run each assertion individually
			// TODO: skip ones with error codes, that'll be different and custom
			t.Run(fmt.Sprintf("stage_%d", stageNumber), func(t *testing.T) {
				if contextTupleTest && len(stage.Tuples) > 20 {
					// https://github.com/openfga/api/blob/05de9d8be3ee12fa4e796b92dbdd4bbbf87107f2/openfga/v1/openfga.proto#L151
					t.Skipf("cannot send more than 20 contextual tuples in one request")
				}
				// arrange: write model
				var typedefs []*openfgav1.TypeDefinition
				model, err := parser.TransformDSLToProto(stage.Model)
				require.NoError(t, err)
				typedefs = model.GetTypeDefinitions()

				writeModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					SchemaVersion:   typesystem.SchemaVersion1_1,
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

				if len(stage.CheckAssertions) == 0 {
					t.Skipf("no check assertions defined")
				}

				// map of correlation_id to result
				expectedResults := map[string]bool{}

				// checks to be passed into batch check request
				protoChecks := make([]*openfgav1.BatchCheckItem, 0, len(stage.CheckAssertions))

				for _, assertion := range stage.CheckAssertions {
					if assertion.ErrorCode != 0 {
						t.Skipf("batch check integration error testing is handled in ____")
					}

					correlationId := ulid.Make().String()
					require.NoError(t, err)

					item := batchchecktest.BatchCheckItemFromAssertion(assertion, correlationId)
					protoChecks = append(protoChecks, item)
					expectedResults[correlationId] = assertion.Expectation
				}

				resp, err := client.BatchCheck(ctx, &openfgav1.BatchCheckRequest{
					StoreId:              storeID,
					AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
					Checks:               protoChecks,
				})
				require.NoError(t, err)

				result := resp.GetResult()

				for correlationId, expected := range expectedResults {
					allowed := result[correlationId].GetAllowed()
					require.Equal(t, allowed, expected)
				}
			})
		}
	})
}

// assert that some bits can error while others may not
// needs its own yaml setup
