package cmd

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser"
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
	cfg := MustDefaultConfigWithRandomPorts()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := runServer(ctx, cfg); err != nil {
			log.Fatal(err)
		}
	}()

	ensureServiceUp(t, cfg.GRPC.Addr, cfg.HTTP.Addr, nil, false)

	conn, err := grpc.Dial(cfg.GRPC.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewOpenFGAServiceClient(conn)
	resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: "CheckTest"})
	require.NoError(t, err)
	storeID := resp.GetId()

	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(filename), "..", "testdata", "check")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)

		var tests checkTests
		err = yaml.Unmarshal(data, &tests)
		require.NoError(t, err)

		runCheckTests(t, client, storeID, tests)
	}
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
