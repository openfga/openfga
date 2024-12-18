// Package listobjects contains integration tests for the ListObjects and StreamedListObjects APIs.
package listobjects

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/yaml"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/assets"
	listobjectstest "github.com/openfga/openfga/internal/test/listobjects"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests/check"
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
	client        ClientInterface
}

// stage is a stage of a test. All stages will be run in a single store.
type stage struct {
	Model                string
	Tuples               []*openfgav1.TupleKey
	ListObjectAssertions []*listobjectstest.Assertion `json:"listObjectsAssertions"`
}

// ClientInterface defines interface for running ListObjects and StreamedListObjects tests.
type ClientInterface interface {
	check.ClientInterface
	ListObjects(ctx context.Context, in *openfgav1.ListObjectsRequest, opts ...grpc.CallOption) (*openfgav1.ListObjectsResponse, error)
	StreamedListObjects(ctx context.Context, in *openfgav1.StreamedListObjectsRequest, opts ...grpc.CallOption) (openfgav1.OpenFGAService_StreamedListObjectsClient, error)
}

// RunAllTests will invoke all list objects tests.
func RunAllTests(t *testing.T, client ClientInterface) {
	t.Run("RunAll", func(t *testing.T) {
		t.Run("ListObjects", func(t *testing.T) {
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

		var testCases listObjectTests
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

				for assertionNumber, assertion := range stage.ListObjectAssertions {
					t.Run(fmt.Sprintf("assertion_%d", assertionNumber), func(t *testing.T) {
						detailedInfo := fmt.Sprintf("ListObject request: %s. Model: %s. Tuples: %s. Contextual tuples: %s", assertion.Request, stage.Model, stage.Tuples, assertion.ContextualTuples)

						ctxTuples := testutils.Shuffle(assertion.ContextualTuples)
						if contextTupleTest {
							ctxTuples = append(ctxTuples, stage.Tuples...)
						}

						// assert 1: on regular list objects endpoint
						resp, err := client.ListObjects(ctx, &openfgav1.ListObjectsRequest{
							StoreId:              storeID,
							AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
							Type:                 assertion.Request.GetType(),
							Relation:             assertion.Request.GetRelation(),
							User:                 assertion.Request.GetUser(),
							ContextualTuples: &openfgav1.ContextualTupleKeys{
								TupleKeys: ctxTuples,
							},
							Context: assertion.Context,
						})

						if assertion.ErrorCode == 0 {
							require.NoError(t, err, detailedInfo)
							require.ElementsMatch(t, assertion.Expectation, resp.GetObjects(), detailedInfo)
						} else {
							require.Error(t, err, detailedInfo)
							e, ok := status.FromError(err)
							require.True(t, ok, detailedInfo)
							require.Equal(t, assertion.ErrorCode, int(e.Code()), detailedInfo)
						}

						// assert 2: on streaming list objects endpoint
						done := make(chan struct{})
						var streamedObjectIDs []string

						clientStream, err := client.StreamedListObjects(ctx, &openfgav1.StreamedListObjectsRequest{
							StoreId:              storeID,
							AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
							Type:                 assertion.Request.GetType(),
							Relation:             assertion.Request.GetRelation(),
							User:                 assertion.Request.GetUser(),
							ContextualTuples: &openfgav1.ContextualTupleKeys{
								TupleKeys: ctxTuples,
							},
							Context: assertion.Context,
						}, []grpc.CallOption{}...)
						require.NoError(t, err)

						var streamingErr error
						var streamingResp *openfgav1.StreamedListObjectsResponse
						go func() {
							for {
								streamingResp, streamingErr = clientStream.Recv()
								if streamingErr == nil {
									streamedObjectIDs = append(streamedObjectIDs, streamingResp.GetObject())
								} else {
									if errors.Is(streamingErr, io.EOF) {
										streamingErr = nil
									}
									break
								}
							}
							done <- struct{}{}
						}()
						<-done

						if assertion.ErrorCode == 0 {
							require.NoError(t, streamingErr, detailedInfo)
							require.ElementsMatch(t, assertion.Expectation, streamedObjectIDs, detailedInfo)
						} else {
							require.Error(t, streamingErr, detailedInfo)
							e, ok := status.FromError(streamingErr)
							require.True(t, ok, detailedInfo)
							require.Equal(t, assertion.ErrorCode, int(e.Code()), detailedInfo)
						}

						if assertion.ErrorCode == 0 {
							// assert 3: each object in the response of ListObjects should return check -> true
							for _, object := range resp.GetObjects() {
								checkResp, err := client.Check(ctx, &openfgav1.CheckRequest{
									StoreId:              storeID,
									TupleKey:             tuple.NewCheckRequestTupleKey(object, assertion.Request.GetRelation(), assertion.Request.GetUser()),
									AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
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
			})
		}
	})
}
