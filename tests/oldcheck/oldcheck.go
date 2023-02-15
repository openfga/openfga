package oldcheck

import (
	"context"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser"
	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"gopkg.in/yaml.v3"
)

type checkTests struct {
	Tests []checkTest
}

type checkTest struct {
	Name       string
	Model      string
	Tuples     []*pb.TupleKey
	Assertions []assertion
}

type assertion struct {
	Tuple            *pb.TupleKey
	ContextualTuples []*pb.TupleKey `yaml:"contextualTuples"`
	Expectation      bool
	Trace            string
}

type ClientInterface interface {
	CreateStore(ctx context.Context, in *pb.CreateStoreRequest, opts ...grpc.CallOption) (*pb.CreateStoreResponse, error)
	WriteAuthorizationModel(ctx context.Context, in *pb.WriteAuthorizationModelRequest, opts ...grpc.CallOption) (*pb.WriteAuthorizationModelResponse, error)
	Write(ctx context.Context, in *pb.WriteRequest, opts ...grpc.CallOption) (*pb.WriteResponse, error)
	Check(ctx context.Context, in *pb.CheckRequest, opts ...grpc.CallOption) (*pb.CheckResponse, error)
}

// RunTests is public so can be run when OpenFGA is used as a library. An
// OpenFGA server needs to be running and the client parameter is a client
// for the server.
func RunTests(t *testing.T, client ClientInterface) {
	data, err := assets.EmbedTests.ReadFile("tests/oldcheck_tests.yaml")
	require.NoError(t, err)

	var testCases checkTests
	err = yaml.Unmarshal(data, &testCases)
	require.NoError(t, err)

	ctx := context.Background()

	for _, test := range testCases.Tests {
		t.Run(test.Name, func(t *testing.T) {
			resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: test.Name})
			require.NoError(t, err)

			storeID := resp.GetId()

			_, err = client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				SchemaVersion:   typesystem.SchemaVersion1_0,
				TypeDefinitions: parser.MustParse(test.Model),
			})
			require.NoError(t, err)

			for _, tuple := range test.Tuples {
				_, err = client.Write(ctx, &pb.WriteRequest{
					StoreId: storeID,
					Writes:  &pb.TupleKeys{TupleKeys: []*pb.TupleKey{tuple}},
				})
				require.NoError(t, err)
			}

			for _, assertion := range test.Assertions {
				resp, err := client.Check(ctx, &pb.CheckRequest{
					StoreId:  storeID,
					TupleKey: assertion.Tuple,
					ContextualTuples: &pb.ContextualTupleKeys{
						TupleKeys: assertion.ContextualTuples,
					},
					Trace: false,
				})
				require.NoError(t, err)
				require.Equal(t, assertion.Expectation, resp.Allowed, assertion)
			}
		})
	}
}
