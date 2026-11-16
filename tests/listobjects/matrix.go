package listobjects

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/internal/test/matrixmodel"
	"github.com/openfga/openfga/pkg/testutils"
)

// MustNewStruct returns a new *structpb.Struct or panics
// on error. The new *structpb.Struct value is built from
// the map m.
func MustNewStruct(m map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err == nil {
		return s
	}
	panic(err)
}

var matrix = matrixTests{
	Name:  "matrix_test",
	Model: matrixmodel.Model,

	Tests: slices.Concat(directs, usersets, ttus),
}

func runTestMatrix(t *testing.T, params testParams) {
	schemaVersion := params.schemaVersion
	client := params.client
	name := matrix.Name

	ctx := context.Background()

	t.Run(name, func(t *testing.T) {
		tests := matrix.Tests

		for _, test := range tests {
			t.Run("test_"+test.Name, func(t *testing.T) {
				resp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: test.Name})
				require.NoError(t, err)
				storeID := resp.GetId()
				modelProto := parser.MustTransformDSLToProto(matrix.Model)
				writeModelResponse, err := client.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					SchemaVersion:   schemaVersion,
					TypeDefinitions: modelProto.GetTypeDefinitions(),
					Conditions:      modelProto.GetConditions(),
				})
				require.NoError(t, err)
				modelID := writeModelResponse.GetAuthorizationModelId()

				tuples := testutils.Shuffle(test.Tuples)
				tuplesLength := len(tuples)
				if tuplesLength > 0 {
					for i := 0; i < tuplesLength; i += writeMaxChunkSize {
						end := int(math.Min(float64(i+writeMaxChunkSize), float64(tuplesLength)))
						writeChunk := (tuples)[i:end]
						_, err = client.Write(ctx, &openfgav1.WriteRequest{
							StoreId:              storeID,
							AuthorizationModelId: modelID,
							Writes: &openfgav1.WriteRequestWrites{
								TupleKeys: writeChunk,
							},
						})
						require.NoError(t, err)
					}
				}

				if len(test.ListObjectAssertions) == 0 {
					t.Skipf("no list objects assertions defined")
				}

				listObjectsAssertion(ctx, t, params, storeID, writeModelResponse.GetAuthorizationModelId(), false, test.Tuples, test.ListObjectAssertions)
			})
		}
	})
}
