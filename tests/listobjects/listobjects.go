// Package listobjects contains integration tests for the ListObjects and StreamedListObjects APIs.
package listobjects

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/yaml"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/assets"
	listobjectstest "github.com/openfga/openfga/internal/test/listobjects"
	"github.com/openfga/openfga/pkg/server/config"
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

type listObjectTests struct {
	Tests []individualTest
}

type testParams struct {
	schemaVersion string
	client        tests.ClientInterface
	experimentals []string
}

// stage is a stage of a test. All stages will be run in a single store.
type stage struct {
	Name                 string
	Model                string
	Tuples               []*openfgav1.TupleKey
	ListObjectAssertions []*listobjectstest.Assertion `json:"listObjectsAssertions"`
}

type matrixTests struct {
	Name  string
	Model string
	Tests []matrixTest
}

type matrixTest struct {
	Name                 string
	Tuples               []*openfgav1.TupleKey
	ListObjectAssertions []*listobjectstest.Assertion `json:"listObjectsAssertions"`
}

func RunMatrixTests(t *testing.T, engine string, client tests.ClientInterface, experimentals []string) {
	t.Run("test_matrix_"+engine+"_experimental_"+strconv.FormatBool(len(experimentals) > 0), func(t *testing.T) {
		t.Parallel()
		runTestMatrix(t, testParams{typesystem.SchemaVersion1_1, client, experimentals})
	})
}

// RunAllTests will invoke all list objects tests.
func RunAllTests(t *testing.T, client tests.ClientInterface, experimentals []string) {
	t.Run("RunAll", func(t *testing.T) {
		t.Run("ListObjects", func(t *testing.T) {
			t.Parallel()
			runTests(t, testParams{typesystem.SchemaVersion1_1, client, experimentals})
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

		var testCases listObjectTests
		err = yaml.Unmarshal(b, &testCases)
		require.NoError(t, err)

		allTestCases = append(allTestCases, testCases.Tests...)
	}

	for _, test := range allTestCases {
		runTest(t, test, params, false)
		runTest(t, test, params, true)
	}
}

func listObjectsAssertion(ctx context.Context, t *testing.T, params testParams, storeID, modelID string, contextTupleTest bool, tuples []*openfgav1.TupleKey, listAssertions []*listobjectstest.Assertion) {
	isPipeline := false
	for _, exp := range params.experimentals {
		if exp == config.ExperimentalPipelineListObjects {
			isPipeline = true
			break
		}
	}

	for assertionNumber, assertion := range listAssertions {
		t.Run(fmt.Sprintf("assertion_%d", assertionNumber), func(t *testing.T) {
			detailedInfo := fmt.Sprintf("ListObject request: %s. Model: %s. Tuples: %s. Contextual tuples: %s", assertion.Request, modelID, tuples, assertion.ContextualTuples)

			ctxTuples := testutils.Shuffle(assertion.ContextualTuples)
			if contextTupleTest {
				ctxTuples = append(ctxTuples, tuples...)
			}

			// assert 1: on regular list objects endpoint
			resp, err := params.client.ListObjects(ctx, &openfgav1.ListObjectsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Type:                 assertion.Request.GetType(),
				Relation:             assertion.Request.GetRelation(),
				User:                 assertion.Request.GetUser(),
				ContextualTuples: &openfgav1.ContextualTupleKeys{
					TupleKeys: ctxTuples,
				},
				Context: assertion.Context,
			})

			switch {
			case assertion.ErrorCode == 0:
				require.NoError(t, err, detailedInfo)
				require.ElementsMatch(t, assertion.Expectation, resp.GetObjects(), detailedInfo)
			case isPipeline && assertion.PipelineExpectation != nil:
				require.NoError(t, err, detailedInfo)
				require.ElementsMatch(t, assertion.PipelineExpectation, resp.GetObjects(), detailedInfo)
			default:
				require.Error(t, err, detailedInfo)
				e, ok := status.FromError(err)
				require.True(t, ok, detailedInfo)
				require.Equal(t, assertion.ErrorCode, int(e.Code()), detailedInfo)
			}
			// assert 2: on streaming list objects endpoint
			var streamedObjectIDs []string

			clientStream, err := params.client.StreamedListObjects(ctx, &openfgav1.StreamedListObjectsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Type:                 assertion.Request.GetType(),
				Relation:             assertion.Request.GetRelation(),
				User:                 assertion.Request.GetUser(),
				ContextualTuples: &openfgav1.ContextualTupleKeys{
					TupleKeys: ctxTuples,
				},
				Context: assertion.Context,
			}, []grpc.CallOption{}...)
			require.NoError(t, err)

			var wg errgroup.Group
			wg.Go(func() error {
				for {
					streamingResp, streamingErr := clientStream.Recv()
					if streamingErr != nil {
						if errors.Is(streamingErr, io.EOF) {
							break
						}
						return streamingErr
					}
					streamedObjectIDs = append(streamedObjectIDs, streamingResp.GetObject())
				}
				return nil
			})
			streamingErr := wg.Wait()
			require.NoError(t, err)

			switch {
			case assertion.ErrorCode == 0:
				require.NoError(t, streamingErr, detailedInfo)
				require.ElementsMatch(t, assertion.Expectation, streamedObjectIDs, detailedInfo)
			case isPipeline && assertion.PipelineExpectation != nil:
				require.NoError(t, err, detailedInfo)
				require.ElementsMatch(t, assertion.PipelineExpectation, streamedObjectIDs, detailedInfo)
			default:
				require.Error(t, streamingErr, detailedInfo)
				e, ok := status.FromError(streamingErr)
				require.True(t, ok, detailedInfo)
				require.Equal(t, assertion.ErrorCode, int(e.Code()), detailedInfo)
			}
			if assertion.ErrorCode == 0 {
				// assert 3: each object in the response of ListObjects should return check -> true
				for _, object := range resp.GetObjects() {
					checkResp, err := params.client.Check(ctx, &openfgav1.CheckRequest{
						StoreId:              storeID,
						TupleKey:             tuple.NewCheckRequestTupleKey(object, assertion.Request.GetRelation(), assertion.Request.GetUser()),
						AuthorizationModelId: modelID,
						ContextualTuples: &openfgav1.ContextualTupleKeys{
							TupleKeys: ctxTuples,
						},
						Context: assertion.Context,
					})
					require.NoError(t, err, detailedInfo)
					require.True(t, checkResp.GetAllowed(), detailedInfo)
				}
			}
		})
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

		for stageNumber, stage := range test.Stages {
			t.Run(fmt.Sprintf("stage_%d", stageNumber), func(t *testing.T) {
				if contextTupleTest && len(stage.Tuples) > 100 {
					// https://github.com/openfga/api/blob/05de9d8be3ee12fa4e796b92dbdd4bbbf87107f2/openfga/v1/openfga.proto#L151
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
				if len(stage.ListObjectAssertions) == 0 {
					t.Skipf("no list objects assertions defined")
				}

				listObjectsAssertion(ctx, t, params, storeID, writeModelResponse.GetAuthorizationModelId(), contextTupleTest, stage.Tuples, stage.ListObjectAssertions)
			})
		}
	})
}
