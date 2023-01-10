package check

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
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
	Validation *validation
}

type assertion struct {
	Tuple            *pb.TupleKey
	ContextualTuples []*pb.TupleKey `yaml:"contextualTuples"`
	Expectation      bool
	ErrorCode        int `yaml:"errorCode"` // If ErrorCode is non-zero then we expect that the check call failed.
	Trace            string
}

type validation struct {
	Tuple            *pb.TupleKey
	ContextualTuples []*pb.TupleKey `yaml:"contextualTuples"`
	Code             int
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

	var tests checkTests
	err = yaml.Unmarshal(data, &tests)
	require.NoError(t, err)

	container := storage.RunDatastoreTestContainer(t, engine)

	cfg := cmd.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = engine
	cfg.Datastore.URI = container.GetConnectionURI()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		if err := cmd.RunServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	conn, err := grpc.Dial(cfg.GRPC.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	// Ensure the service is up before continuing.
	client := healthv1pb.NewHealthClient(conn)
	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 10 * time.Second
	err = backoff.Retry(func() error {
		resp, err := client.Check(ctx, &healthv1pb.HealthCheckRequest{
			Service: pb.OpenFGAService_ServiceDesc.ServiceName,
		})
		if err != nil {
			return err
		}

		if resp.GetStatus() != healthv1pb.HealthCheckResponse_SERVING {
			return fmt.Errorf("not serving")
		}

		return nil
	}, policy)
	require.NoError(t, err)

	runTests(t, pb.NewOpenFGAServiceClient(conn), tests)

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
						if assertion.Trace != "" {
							require.Equal(t, assertion.Trace, resp.GetResolution())
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
