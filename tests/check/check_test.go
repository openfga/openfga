package check

import (
	"context"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
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

func TestCheckMemory(t *testing.T) {
	testRunAll(t, "memory")
}

func TestCheckPostgres(t *testing.T) {
	testRunAll(t, "postgres")
}

func TestCheckMySQL(t *testing.T) {
	testRunAll(t, "mysql")
}

func testRunAll(t *testing.T, engine string) {
	testCheck(t, engine)
	testBadAuthModelID(t, engine)
}

func testCheck(t *testing.T, engine string) {
	data, err := assets.EmbedTests.ReadFile("tests/check_tests.yaml")
	require.NoError(t, err)

	var testCases checkTests
	err = yaml.Unmarshal(data, &testCases)
	require.NoError(t, err)

	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = engine

	cancel := tests.StartServer(t, cfg)
	defer cancel()

	conn := tests.Connect(t, cfg.GRPC.Addr)
	defer conn.Close()

	runTests(t, pb.NewOpenFGAServiceClient(conn), testCases)

	// Shutdown the server.
	cancel()
}

func runTests(t *testing.T, client pb.OpenFGAServiceClient, tests checkTests) {
	ctx := context.Background()

	for _, test := range tests.Tests {
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

func testBadAuthModelID(t *testing.T, engine string) {

	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = engine

	cancel := tests.StartServer(t, cfg)
	defer cancel()

	conn := tests.Connect(t, cfg.GRPC.Addr)
	defer conn.Close()
	client := pb.NewOpenFGAServiceClient(conn)

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: "bad auth id"})
	require.NoError(t, err)

	storeID := resp.GetId()
	model := `
	type user

	type doc
	  relations
	    define viewer: [user] as self
	    define can_view as viewer
	`
	_, err = client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(model),
	})
	require.NoError(t, err)
	_, err = client.Check(ctx, &pb.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tuple.NewTupleKey("doc:x", "viewer", "user:y"),
		AuthorizationModelId: "01GS89AJC3R3PFQ9BNY5ZF6Q97",
	})

	require.Error(t, err)
	e, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, int(pb.ErrorCode_authorization_model_not_found), int(e.Code()))
}
