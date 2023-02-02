package oldcheck

import (
	"context"
	"os"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser"
	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
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
	Tuple       *pb.TupleKey
	Expectation bool
	Trace       string
}

func TestCheckMemory(t *testing.T) {
	testCheck(t, "memory")
}

func TestCheckPostgres(t *testing.T) {
	testCheck(t, "postgres")
}

func TestCheckMySQL(t *testing.T) {
	testCheck(t, "mysql")
}

func testCheck(t *testing.T, engine string) {
	data, err := os.ReadFile("tests.yaml")
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

	runTest(t, pb.NewOpenFGAServiceClient(conn), testCases)

	// Shutdown the server.
	cancel()
}

func runTest(t *testing.T, client pb.OpenFGAServiceClient, tests checkTests) {
	ctx := context.Background()

	for _, test := range tests.Tests {
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
					Trace:    false,
				})
				require.NoError(t, err)
				require.Equal(t, assertion.Expectation, resp.Allowed, assertion)
				if assertion.Trace != "" {
					require.Equal(t, assertion.Trace, resp.GetResolution())
				}
			}
		})
	}
}
