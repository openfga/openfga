package oldcheck

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	parser "github.com/craigpastro/openfga-dsl-parser"
	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/pkg/testfixtures/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	Tuple       *pb.TupleKey
	Expectation bool
}

func TestCheck(t *testing.T) {
	data, err := os.ReadFile("tests.yaml")
	require.NoError(t, err)

	var tests checkTests
	err = yaml.Unmarshal(data, &tests)
	require.NoError(t, err)

	engines := []string{"memory", "postgres", "mysql"}
	for _, engine := range engines {
		name := fmt.Sprintf("TestCheckWith%s", strings.ToUpper(engine))

		t.Run(name, func(t *testing.T) {
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

			client := pb.NewOpenFGAServiceClient(conn)

			policy := backoff.NewExponentialBackOff()
			policy.MaxElapsedTime = 10 * time.Second
			resp, err := backoff.RetryWithData(func() (*pb.CreateStoreResponse, error) {
				return client.CreateStore(ctx, &pb.CreateStoreRequest{Name: name})
			}, policy)
			require.NoError(t, err)

			storeID := resp.GetId()

			runTest(t, client, storeID, tests)

			cancel() // shutdown the server
		})
	}
}

func runTest(t *testing.T, client pb.OpenFGAServiceClient, storeID string, tests checkTests) {
	ctx := context.Background()

	for _, test := range tests.Tests {
		t.Run(test.Name, func(t *testing.T) {
			_, err := client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				SchemaVersion:   "1.0",
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
				})
				require.NoError(t, err)
				require.Equal(t, assertion.Expectation, resp.Allowed, assertion)
			}

			// Delete the tuples.
			for _, tuple := range test.Tuples {
				_, err = client.Write(ctx, &pb.WriteRequest{
					StoreId: storeID,
					Deletes: &pb.TupleKeys{TupleKeys: []*pb.TupleKey{
						tupleUtils.NewTupleKey(tuple.Object, tuple.Relation, tuple.User),
					}},
				})
				require.NoError(t, err)
			}
		})
	}
}
