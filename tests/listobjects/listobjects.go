package listobjects

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	v1parser "github.com/craigpastro/openfga-dsl-parser"
	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
	"github.com/openfga/openfga/tests/check"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

type listObjectTests struct {
	Tests []struct {
		Name                  string
		Stages                []*stage
		ListObjectsMaxResults uint32 `yaml:"listObjectsMaxResults"`
	}
}

// stage is a stage of a test. All stages will be run in a single store.
type stage struct {
	Model      string
	Tuples     []*pb.TupleKey
	Assertions []*assertion
}

type assertion struct {
	Request               *pb.ListObjectsRequest
	ContextualTuples      []*pb.TupleKey `yaml:"contextualTuples"`
	ListObjectsMaxResults uint32
	Expectation           []string
	ErrorCode             int `yaml:"errorCode"` // If ErrorCode is non-zero then we expect that the ListObjects call failed.
}

type ListObjectsClientInterface interface {
	check.CheckTestClientInterface
	ListObjects(ctx context.Context, in *pb.ListObjectsRequest, opts ...grpc.CallOption) (*pb.ListObjectsResponse, error)
	StreamedListObjects(ctx context.Context, in *pb.StreamedListObjectsRequest, opts ...grpc.CallOption) (pb.OpenFGAService_StreamedListObjectsClient, error)
}

// RunSchema1_1ListObjectsTests is public so can be run when OpenFGA is used as a
// library. An OpenFGA server needs to be running and the client parameter is
// a client for the server.
func RunSchema1_1ListObjectsTests(t *testing.T, engine string) {
	runTests(t, typesystem.SchemaVersion1_1, engine)
}

// RunSchema1_0ListObjectsTests is the 1.0 version of RunSchema1_1CheckTests.
func RunSchema1_0ListObjectsTests(t *testing.T, engine string) {
	runTests(t, typesystem.SchemaVersion1_0, engine)
}

func runTests(t *testing.T, schemaVersion string, engine string) {
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
		cfg := run.MustDefaultConfigWithRandomPorts()
		cfg.Log.Level = "none"
		cfg.Datastore.Engine = engine
		listObjectsMaxResults := test.ListObjectsMaxResults
		if test.ListObjectsMaxResults != 0 {
			cfg.ListObjectsMaxResults = test.ListObjectsMaxResults
		}

		cancel := tests.StartServer(t, cfg)

		conn, err := grpc.Dial(cfg.GRPC.Addr,
			grpc.WithBlock(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			require.FailNow(t, err.Error())
		}

		client := pb.NewOpenFGAServiceClient(conn)
		test := test
		t.Run(test.Name, func(t *testing.T) {
			resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: test.Name})
			require.NoError(t, err)

			storeID := resp.GetId()

			for _, stage := range test.Stages {
				// arrange: write model
				var typedefs []*pb.TypeDefinition
				if schemaVersion == typesystem.SchemaVersion1_1 {
					typedefs = parser.MustParse(stage.Model)

				} else {
					typedefs = v1parser.MustParse(stage.Model)
				}

				_, err = client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					SchemaVersion:   schemaVersion,
					TypeDefinitions: typedefs,
				})
				require.NoError(t, err)

				// arrange: write tuples
				if len(stage.Tuples) > 0 {
					_, err = client.Write(ctx, &pb.WriteRequest{
						StoreId: storeID,
						Writes:  &pb.TupleKeys{TupleKeys: stage.Tuples},
					})
					require.NoError(t, err)
				}

				for _, assertion := range stage.Assertions {
					// assert 1: on regular list objects endpoint
					resp, err := client.ListObjects(ctx, &pb.ListObjectsRequest{
						StoreId:  storeID,
						Type:     assertion.Request.Type,
						Relation: assertion.Request.Relation,
						User:     assertion.Request.User,
						ContextualTuples: &pb.ContextualTupleKeys{
							TupleKeys: assertion.ContextualTuples,
						},
					})

					if assertion.ErrorCode == 0 {
						require.NoError(t, err)
						require.LessOrEqual(t, len(resp.Objects), int(listObjectsMaxResults))

						if len(assertion.Expectation) > int(listObjectsMaxResults) {
							require.Subset(t, assertion.Expectation, resp.Objects)
						} else {
							require.ElementsMatch(t, assertion.Expectation, resp.Objects)
						}
					} else {
						require.Error(t, err)
						e, ok := status.FromError(err)
						require.True(t, ok)
						require.Equal(t, assertion.ErrorCode, int(e.Code()))
					}

					// assert 2: on streaming list objects endpoint
					done := make(chan struct{})
					var streamedObjectIds []string

					clientStream, err := client.StreamedListObjects(ctx, &pb.StreamedListObjectsRequest{
						StoreId:  storeID,
						Type:     assertion.Request.Type,
						Relation: assertion.Request.Relation,
						User:     assertion.Request.User,
						ContextualTuples: &pb.ContextualTupleKeys{
							TupleKeys: assertion.ContextualTuples,
						},
					}, []grpc.CallOption{}...)

					require.NoError(t, err)

					var streamingErr error
					var streamingResp *pb.StreamedListObjectsResponse
					go func() {
						for {
							streamingResp, streamingErr = clientStream.Recv()
							if streamingErr == nil {
								streamedObjectIds = append(streamedObjectIds, streamingResp.Object)
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
						require.NoError(t, streamingErr)
						require.LessOrEqual(t, len(streamedObjectIds), int(listObjectsMaxResults))
						if len(assertion.Expectation) > int(listObjectsMaxResults) {
							require.Subset(t, assertion.Expectation, streamedObjectIds)
						} else {
							require.ElementsMatch(t, assertion.Expectation, streamedObjectIds)
						}
					} else {
						require.Error(t, streamingErr)
						e, ok := status.FromError(streamingErr)
						require.True(t, ok)
						require.Equal(t, assertion.ErrorCode, int(e.Code()))
					}

					if assertion.ErrorCode == 0 {
						// assert 3: each object in the response of ListObjects should return check -> true
						for _, object := range resp.Objects {
							checkResp, err := client.Check(ctx, &pb.CheckRequest{
								StoreId:  storeID,
								TupleKey: tuple.NewTupleKey(object, assertion.Request.Relation, assertion.Request.User),
								ContextualTuples: &pb.ContextualTupleKeys{
									TupleKeys: assertion.ContextualTuples,
								},
							})
							require.NoError(t, err)
							require.True(t, checkResp.Allowed, fmt.Sprintf("Expected Check(%s#%s@%s) to be true, got false", object, assertion.Request.Relation, assertion.Request.User))
						}
					}
				}
			}
		})

		t.Cleanup(func() {
			cancel()
			conn.Close()
		})
	}
}
