// Package batchcheck contains integration tests for the BatchCheck api
package batchcheck

import (
	"context"
	"fmt"
	"log"
	"math"
	"testing"

	"github.com/oklog/ulid/v2"

	batchchecktest "github.com/openfga/openfga/internal/test/batchcheck"

	parser "github.com/openfga/language/pkg/go/transformer"
	"sigs.k8s.io/yaml"

	"github.com/openfga/openfga/pkg/typesystem"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/tests/check"
)

const writeMaxChunkSize = 40

type ClientInterface interface {
	check.ClientInterface
	BatchCheck(ctx context.Context, in *openfgav1.BatchCheckRequest, opts ...grpc.CallOption) (*openfgav1.BatchCheckResponse, error)
}

// RunBatchCheckTestsOnCheckAssertions will invoke BatchCheck on all existing Check assertions which do not throw errors.
// In the happy path, BatchCheck is essentially a performance wrapper around check and behaves the same.
// In the case of errors, BatchCheck's behavior diverges and those test cases are handled in _____.
func RunBatchCheckTestsOnCheckAssertions(t *testing.T, client ClientInterface) {
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
				runTest(t, test, client, true)
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
				writeModelResponse, err := writeAuthModel(ctx, client, storeID, stage.Model)
				require.NoError(t, err)

				if !contextTupleTest {
					err = writeTuples(ctx, client, storeID, stage.Tuples, writeModelResponse)
					require.NoError(t, err)
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

					// monkey patch the contextual tuples since we don't actually define them in yaml
					if contextTupleTest {
						assertion.ContextualTuples = append(assertion.ContextualTuples, stage.Tuples...)
					}

					correlationID := ulid.Make().String()

					item := batchchecktest.BatchCheckItemFromCheckAssertion(assertion, correlationID)
					protoChecks = append(protoChecks, item)
					expectedResults[correlationID] = assertion.Expectation
				}

				resp, err := client.BatchCheck(ctx, &openfgav1.BatchCheckRequest{
					StoreId:              storeID,
					AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
					Checks:               protoChecks,
				})
				require.NoError(t, err)

				result := resp.GetResult()

				for correlationID, expected := range expectedResults {
					allowed := result[correlationID].GetAllowed()
					require.Equal(t, expected, allowed)
				}
			})
		}
	})
}

// RunBatchCheckSpecificFailureScenarios are not shared with Check.
func RunBatchCheckSpecificFailureScenarios(t *testing.T, client ClientInterface) {
	t.Run("RunAll", func(t *testing.T) {
		t.Run("BatchCheckFailureScenarios", func(t *testing.T) {
			t.Parallel()
			file := "tests/batch_check_tests.yaml"

			var b []byte
			var err error
			b, err = assets.EmbedTests.ReadFile(file)
			require.NoError(t, err)

			var testCases batchchecktest.BatchCheckTests
			err = yaml.Unmarshal(b, &testCases)
			require.NoError(t, err)

			var allTestCases []batchchecktest.IndividualTest
			allTestCases = append(allTestCases, testCases.Tests...)

			for _, testCase := range allTestCases {
				runBatchCheckTest(t, testCase, client, false)
			}
		})
	})
}

func runBatchCheckTest(t *testing.T, test batchchecktest.IndividualTest, client ClientInterface, contextTupleTest bool) {
	ctx := context.Background()
	name := test.Name

	if contextTupleTest {
		name += "_ctxTuples"
	}
	t.Run(name, func(t *testing.T) {
		resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: name})
		require.NoError(t, err)

		storeID := resp.GetId()

		writeAuthModelResponse, err := writeAuthModel(
			ctx,
			client,
			storeID,
			test.Model,
		)
		require.NoError(t, err)

		if !contextTupleTest {
			err = writeTuples(ctx, client, storeID, test.Tuples, writeAuthModelResponse)
			require.NoError(t, err)
		}

		for _, assertion := range test.Assertions {
			request := assertion.Request.ToProtoRequest()

			response, _ := client.BatchCheck(ctx, &openfgav1.BatchCheckRequest{
				StoreId:              storeID,
				AuthorizationModelId: writeAuthModelResponse.GetAuthorizationModelId(),
				Checks:               request.GetChecks(),
			})

			result := response.GetResult()

			log.Printf("Justin response: %+v", response)
			log.Printf("Justin result: %+v", result)
			for _, expectation := range assertion.Expectation {
				switch {
				case expectation.InputError != 0:
					code := result[expectation.CorrelationID].GetError().GetInputError()
					require.Equal(t, expectation.InputError, int(code))
				case expectation.InternalError != 0:
					code := result[expectation.CorrelationID].GetError().GetInternalError()
					require.Equal(t, expectation.InternalError, int(code))
				default:
					require.Equal(t, expectation.Allowed, result[expectation.CorrelationID].GetAllowed())
				}
			}
		}
	})
}

func writeAuthModel(ctx context.Context, client ClientInterface, storeID string, model string) (*openfgav1.WriteAuthorizationModelResponse, error) {
	// arrange: write model
	var typedefs []*openfgav1.TypeDefinition
	parsedModel, err := parser.TransformDSLToProto(model)
	if err != nil {
		return nil, err
	}
	typedefs = parsedModel.GetTypeDefinitions()

	writeModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: typedefs,
		Conditions:      parsedModel.GetConditions(),
	})
	if err != nil {
		return nil, err
	}

	return writeModelResponse, nil
}

func writeTuples(ctx context.Context, client ClientInterface, storeID string, tuples []*openfgav1.TupleKey, writeModelResponse *openfgav1.WriteAuthorizationModelResponse) error {
	tuplesLength := len(tuples)
	// arrange: write tuples
	if tuplesLength > 0 {
		for i := 0; i < tuplesLength; i += writeMaxChunkSize {
			end := int(math.Min(float64(i+writeMaxChunkSize), float64(tuplesLength)))
			writeChunk := (tuples)[i:end]
			_, err := client.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              storeID,
				AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: writeChunk,
				},
			})

			if err != nil {
				return err
			}
		}
	}

	return nil
}
