package listobjects

import (
	"context"
	"fmt"
	"testing"

	v1parser "github.com/craigpastro/openfga-dsl-parser"
	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

type listObjectTests struct {
	Tests []*listObjectTest
}

type listObjectTest struct {
	Name   string
	Stages []*stage
}

// stage is a stage of a test. All stages will be run in a single store.
type stage struct {
	Model      string
	Tuples     []*pb.TupleKey
	Assertions []*assertion
}

type request struct {
	User             string
	Type             string
	Relation         string
	ContextualTuples *pb.ContextualTupleKeys
}

type assertion struct {
	Request     *request
	Expectation []string
	ErrorCode   int `yaml:"errorCode"` // If ErrorCode is non-zero then we expect that the ListObjects call failed.
}

type ClientInterface interface {
	CreateStore(ctx context.Context, in *pb.CreateStoreRequest, opts ...grpc.CallOption) (*pb.CreateStoreResponse, error)
	WriteAuthorizationModel(ctx context.Context, in *pb.WriteAuthorizationModelRequest, opts ...grpc.CallOption) (*pb.WriteAuthorizationModelResponse, error)
	Write(ctx context.Context, in *pb.WriteRequest, opts ...grpc.CallOption) (*pb.WriteResponse, error)
	Check(ctx context.Context, in *pb.CheckRequest, opts ...grpc.CallOption) (*pb.CheckResponse, error)
	ListObjects(ctx context.Context, in *pb.ListObjectsRequest, opts ...grpc.CallOption) (*pb.ListObjectsResponse, error)
}

// RunSchema1_1ListObjectsTests is public so can be run when OpenFGA is used as a
// library. An OpenFGA server needs to be running and the client parameter is
// a client for the server.
func RunSchema1_1ListObjectsTests(t *testing.T, client ClientInterface) {
	runTests(t, typesystem.SchemaVersion1_1, client)
}

// RunSchema1_0ListObjectsTests is the 1.0 version of RunSchema1_1CheckTests.
func RunSchema1_0ListObjectsTests(t *testing.T, client ClientInterface) {
	runTests(t, typesystem.SchemaVersion1_0, client)
}

func runTests(t *testing.T, schemaVersion string, client ClientInterface) {
	var b []byte
	var err error
	if schemaVersion == typesystem.SchemaVersion1_1 {
		b, err = assets.EmbedTests.ReadFile("tests/listobjects_1_1_tests.yaml")
	} else {
		b, err = assets.EmbedTests.ReadFile("tests/listobjects_1_0_tests.yaml")
	}
	require.NoError(t, err)

	var testCases listObjectTests
	err = yaml.Unmarshal(b, &testCases)
	require.NoError(t, err)

	ctx := context.Background()

	for _, test := range testCases.Tests {
		resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: test.Name})
		require.NoError(t, err)

		storeID := resp.GetId()

		t.Run(test.Name, func(t *testing.T) {
			for _, stage := range test.Stages {
				if schemaVersion == typesystem.SchemaVersion1_1 {
					_, err = client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
						StoreId:         storeID,
						SchemaVersion:   typesystem.SchemaVersion1_1,
						TypeDefinitions: parser.MustParse(stage.Model),
					})
				} else {
					_, err = client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
						StoreId:         storeID,
						SchemaVersion:   typesystem.SchemaVersion1_0,
						TypeDefinitions: v1parser.MustParse(stage.Model),
					})
				}
				require.NoError(t, err)

				if len(stage.Tuples) > 0 {
					_, err = client.Write(ctx, &pb.WriteRequest{
						StoreId: storeID,
						Writes:  &pb.TupleKeys{TupleKeys: stage.Tuples},
					})
					require.NoError(t, err)
				}

				for _, assertion := range stage.Assertions {
					resp, err := client.ListObjects(ctx, &pb.ListObjectsRequest{
						StoreId:          storeID,
						Type:             assertion.Request.Type,
						Relation:         assertion.Request.Relation,
						User:             assertion.Request.User,
						ContextualTuples: assertion.Request.ContextualTuples,
					})

					if assertion.ErrorCode == 0 {
						require.NoError(t, err)
						require.Subset(t, assertion.Expectation, resp.Objects)

						for _, object := range resp.Objects {
							// each object in the response of ListObjects should return check -> true
							checkResp, err := client.Check(ctx, &pb.CheckRequest{
								StoreId:          storeID,
								TupleKey:         tuple.NewTupleKey(object, assertion.Request.Relation, assertion.Request.User),
								ContextualTuples: assertion.Request.ContextualTuples,
							})
							require.NoError(t, err)
							require.True(t, checkResp.Allowed, fmt.Sprintf("Expected Check(%s#%s@%s) to be true, got false", object, assertion.Request.Relation, assertion.Request.User))
						}
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
