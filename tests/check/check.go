package check

import (
	"context"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

type checkTests struct {
	Tests []*checkTest
}

type checkTest struct {
	Name   string
	Stages []*stage
}

// stage is a stage of a test. All stages will be run in a single store.
type stage struct {
	Model      string
	Tuples     []*pb.TupleKey
	Assertions []*assertion
}

type assertion struct {
	Tuple            *pb.TupleKey
	ContextualTuples []*pb.TupleKey `yaml:"contextualTuples"`
	Expectation      bool
	ErrorCode        int `yaml:"errorCode"` // If ErrorCode is non-zero then we expect that the check call failed.
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
	data, err := assets.EmbedTests.ReadFile("tests/check_tests.yaml")
	require.NoError(t, err)

	var testCases checkTests
	err = yaml.Unmarshal(data, &testCases)
	require.NoError(t, err)

	ctx := context.Background()

	for _, test := range testCases.Tests {
		resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: test.Name})
		require.NoError(t, err)

		storeID := resp.GetId()

		t.Run(test.Name, func(t *testing.T) {
			for _, stage := range test.Stages {
				_, err = client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					SchemaVersion:   typesystem.SchemaVersion1_1,
					TypeDefinitions: parser.MustParse(stage.Model),
				})
				require.NoError(t, err)

				if len(stage.Tuples) > 0 {
					_, err = client.Write(ctx, &pb.WriteRequest{
						StoreId: storeID,
						Writes:  &pb.TupleKeys{TupleKeys: stage.Tuples},
					})
					require.NoError(t, err)
				}

				for _, assertion := range stage.Assertions {
					resp, err := client.Check(ctx, &pb.CheckRequest{
						StoreId:  storeID,
						TupleKey: assertion.Tuple,
						ContextualTuples: &pb.ContextualTupleKeys{
							TupleKeys: assertion.ContextualTuples,
						},
						Trace: true,
					})

					if assertion.ErrorCode == 0 {
						require.NoError(t, err)
						require.Equal(t, assertion.Expectation, resp.Allowed, assertion)
					} else {
						require.Error(t, err)
						e, ok := status.FromError(err)
						require.True(t, ok)
						require.Equal(t, assertion.ErrorCode, int(e.Code()))
					}

				}
			}
		})
	}
}
