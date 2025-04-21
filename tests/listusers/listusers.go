// Package listusers contains integration tests for the ListUsers and StreamedListUsers APIs.
package listusers

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/yaml"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/assets"
	listuserstest "github.com/openfga/openfga/internal/test/listusers"
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

type listUsersTests struct {
	Tests []individualTest
}

type stage struct {
	Model               string
	Tuples              []*openfgav1.TupleKey
	ListUsersAssertions []*listuserstest.Assertion `json:"listUsersAssertions"`
}

// RunAllTests will invoke all ListUsers tests.
func RunAllTests(t *testing.T, client tests.ClientInterface) {
	t.Run("RunAll", func(t *testing.T) {
		t.Run("ListUsers", func(t *testing.T) {
			t.Parallel()
			files := []string{
				"tests/consolidated_1_1_tests.yaml",
				"tests/abac_tests.yaml",
			}

			var allTestCases []individualTest

			for _, file := range files {
				var b []byte
				var err error
				b, err = assets.EmbedTests.ReadFile(file)
				require.NoError(t, err)

				var testCases listUsersTests
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

func runTest(t *testing.T, test individualTest, client tests.ClientInterface, contextTupleTest bool) {
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
					// https://github.com/openfga/api/blob/6e048d8023f434cb7a1d3943f41bdc3937d4a1bf/openfga/v1/openfga.proto#L222
					t.Skipf("cannot send more than 100 contextual tuples in one request")
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

				if len(stage.ListUsersAssertions) == 0 {
					t.Skipf("no list users assertions defined")
				}

				for assertionNumber, assertion := range stage.ListUsersAssertions {
					t.Run(fmt.Sprintf("assertion_%d", assertionNumber), func(t *testing.T) {
						detailedInfo := fmt.Sprintf("ListUsers request: %v. Model: %s. Tuples: %s. Contextual tuples: %s", assertion.Request.ToString(), stage.Model, stage.Tuples, assertion.ContextualTuples)
						ctxTuples := testutils.Shuffle(assertion.ContextualTuples)
						if contextTupleTest {
							ctxTuples = append(ctxTuples, stage.Tuples...)
						}

						// assert 1: on regular list users endpoint
						convertedRequest := assertion.Request.ToProtoRequest()
						resp, err := client.ListUsers(ctx, &openfgav1.ListUsersRequest{
							StoreId:              storeID,
							AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
							Object:               convertedRequest.GetObject(),
							Relation:             convertedRequest.GetRelation(),
							UserFilters:          convertedRequest.GetUserFilters(),
							Context:              assertion.Context,
							ContextualTuples:     ctxTuples,
						})
						if assertion.ErrorCode != 0 && len(assertion.Expectation) > 0 {
							t.Errorf("cannot have a test with the expectation of both an error code and a result")
						}

						if assertion.ErrorCode == 0 {
							require.NoError(t, err, detailedInfo)
							require.ElementsMatch(t, assertion.Expectation, listuserstest.FromUsersProto(resp.GetUsers()), detailedInfo)

							// assert 2: each user in the response of ListUsers should return check -> true
							for _, user := range resp.GetUsers() {
								checkRequestTupleKey := tuple.NewCheckRequestTupleKey(assertion.Request.Object, assertion.Request.Relation, tuple.UserProtoToString(user))
								checkResp, err := client.Check(ctx, &openfgav1.CheckRequest{
									StoreId:              storeID,
									TupleKey:             checkRequestTupleKey,
									AuthorizationModelId: writeModelResponse.GetAuthorizationModelId(),
									ContextualTuples: &openfgav1.ContextualTupleKeys{
										TupleKeys: ctxTuples,
									},
									Context: assertion.Context,
								})
								require.NoError(t, err, detailedInfo)
								require.True(t, checkResp.GetAllowed(), "Expected allowed = true", checkRequestTupleKey)
							}
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
