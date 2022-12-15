package oldcheck

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	parser "github.com/craigpastro/openfga-dsl-parser"
	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
	"gopkg.in/yaml.v2"
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
	ContextualTuples []*pb.TupleKey
	Expectation      bool
	Resolution       *pb.CheckResolutionMetadata
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

	runTest(t, pb.NewOpenFGAServiceClient(conn), tests)

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
					StoreId:          storeID,
					TupleKey:         assertion.Tuple,
					ContextualTuples: &pb.ContextualTupleKeys{TupleKeys: assertion.ContextualTuples},
					Trace:            true,
				})
				require.NoError(t, err)
				require.Equal(t, assertion.Expectation, resp.Allowed, assertion)
				if assertion.Resolution != nil {
					require.Equal(t, assertion.Resolution.Depth, resp.ResolutionMetadata.Depth)
					if assertion.Resolution.GetResolution() != "" {
						require.Equal(t, assertion.Resolution.Resolution, resp.ResolutionMetadata.Resolution)
					}
				}
			}
		})
	}
}
