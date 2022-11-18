package checktestmatrix

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	parser "github.com/craigpastro/openfga-dsl-parser"
	"github.com/openfga/openfga/cmd"
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
	Tuples     []tuple
	Assertions []assertion
}

type tuple struct {
	Object   string
	Relation string
	User     string
}

type assertion struct {
	Tuple       tuple
	Expectation bool
}

func TestCheck(t *testing.T) {
	cfg := cmd.MustDefaultConfigWithRandomPorts()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := cmd.RunServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	conn, err := grpc.Dial(cfg.GRPC.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewOpenFGAServiceClient(conn)

	var resp *pb.CreateStoreResponse
	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 10 * time.Second
	err = backoff.Retry(func() error {
		resp, err = client.CreateStore(ctx, &pb.CreateStoreRequest{Name: "CheckTest"})
		if err != nil {
			return err
		}
		return nil
	}, policy)
	require.NoError(t, err)

	storeID := resp.GetId()
	
	data, err := os.ReadFile("tests.yaml")
	require.NoError(t, err)

	var tests checkTests
	err = yaml.Unmarshal(data, &tests)

	runCheckTests(t, client, storeID, tests)
}

func runCheckTests(t *testing.T, client pb.OpenFGAServiceClient, storeID string, tests checkTests) {
	ctx := context.Background()

	for _, test := range tests.Tests {
		t.Run(test.Name, func(t *testing.T) {
			_, err := client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				TypeDefinitions: parser.MustParse(test.Model),
				SchemaVersion:   "1.0",
			})
			require.NoError(t, err)

			for _, tuple := range test.Tuples {
				_, err = client.Write(ctx, &pb.WriteRequest{
					StoreId: storeID,
					Writes: &pb.TupleKeys{TupleKeys: []*pb.TupleKey{
						tupleUtils.NewTupleKey(tuple.Object, tuple.Relation, tuple.User),
					}},
				})
				require.NoError(t, err)
			}

			for _, assertion := range test.Assertions {
				resp, err := client.Check(ctx, &pb.CheckRequest{
					StoreId:  storeID,
					TupleKey: tupleUtils.NewTupleKey(assertion.Tuple.Object, assertion.Tuple.Relation, assertion.Tuple.User),
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
