package model

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
)

var tests = []struct {
	name  string
	model string
}{
	//	{
	//		// Parse error
	//		name: "Case1",
	//		model: `
	//type user
	//type self
	//  relations
	//    define member: [user] as self
	//`,
	//	},
	//	{
	//		// Parse error
	//		name: "Case2",
	//		model: `
	//type user
	//type this
	//  relations
	//    define member: [user] as self
	//`,
	//	},
	//	{
	//		// Parse error
	//		name: "Case3",
	//		model: `
	//type user
	//type group
	//  relations
	//    define self: [user] as self
	//`,
	//	},
	//	{
	// 		// Parse error
	//		name: "Case4",
	//		model: `
	//type user
	//type group
	// relations
	//   define this: [user] as self
	//`,
	//	},
	//	{
	//		// TODO: Can we validate this?
	//		name: "Case5",
	//		model: `
	//type user
	//type team
	//  relations
	//    define parent: [group] as self
	//    define viewer as viewer from parent
	//type group
	//  relations
	//    define parent: [team] as self
	//    define viewer as viewer from parent
	//`,
	//	},
	{
		name: "Case6",
		model: `
type user
type group
  relations
    define group as group from group
`,
	},
	//	{
	//		// TODO: something
	//		name: "Case7",
	//		model: `
	//type user
	//type group
	//  relations
	//    define parent: [group] as self
	//    define viewer as viewer from parent
	//`,
	//	},
	//	{
	//		// TODO: something
	//		name: "Case8",
	//		model: `
	//type group
	//  relations
	//    define viewer: [group#viewer] as self
	//`,
	//	},
	{
		name: "Case9",
		model: `
type user
type org
  relations
    define member: [user] as self
type group
  relations
    define parent: [org] as self
    define viewer as viewer from parent
`,
	},
	{
		name: "Case10",
		model: `
type user
type group
  relations
    define parent: [group] as self
    define viewer as reader from parent
`,
	},
	{
		name: "Case11",
		model: `
type user
type org
type group
  relations
    define parent: [group] as self
    define viewer as viewer from org
`,
	},
	{
		name: "Case12",
		model: `
type user
type org
type group
  relations
    define parent: [group] as self
    define viewer as org from parent
`,
	},
	{
		name: "Case13",
		model: `
type user
type org
type group
  relations
    define parent: [group, group#org] as self
`,
	},
	//	{
	//		// TODO: something
	//		name: "Case14",
	//		model: `
	//type user
	//type org
	//  relations
	//    define viewer: [user] as self
	//type group
	//  relations
	//    define parent: [group] as self
	//    define viewer as viewer from parent
	//`,
	//	},
	//	{
	//		// TODO: something
	//		name: "Case16",
	//		model: `
	//type document
	//  relations
	//    define reader as writer
	//    define writer as reader
	//`,
	//	},
	{
		name: "Case17",
		model: `
type user
type folder
  relations
    define parent: [folder] as self or parent from parent
    define viewer: [user] as self or viewer from parent
`,
	},
	{
		name: "Case18",
		model: `
type user
type folder
  relations
    define root: [folder] as self
    define parent: [folder] as self or root
    define viewer: [user] as self or viewer from parent
`,
	},
	{
		name: "Case19",
		model: `
type user
type folder
  relations
    define root: [folder] as self
    define parent as root
    define viewer: [user] as self or viewer from parent
`,
	},
	{
		name: "Case20",
		model: `
type user
type folder
  relations
    define root: [folder] as self
    define parent: [folder, folder#parent] as self
    define viewer: [user] as self or viewer from parent
`,
	},
	{
		name: "Case21",
		model: `
type user
type group
  relations
    define member: [user] as self
    define reader as member and allowed
`,
	},
	{
		name: "Case22",
		model: `
type user
type group
  relations
    define member: [user] as self
    define reader as member or allowed
`,
	},
	{
		name: "Case23",
		model: `
type user
type group
  relations
    define member: [user] as self
    define reader as allowed but not member
`,
	},
	{
		name: "Case24",
		model: `
type user
type group
  relations
    define member: [user] as self
    define reader as member but not allowed
`,
	},
	{
		name: "Case25",
		model: `
type user
type org
  relations
    define member as self
`,
	},
	//	{   // Parse error
	//		name: "Case26",
	//		model: `
	//type user
	//type org
	//  relations
	//    define member: [ ]
	//`,
	//	},
}

func TestWriteAuthorizationModel(t *testing.T) {
	cfg := cmd.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = "memory"

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

	runTests(t, pb.NewOpenFGAServiceClient(conn))

	// Shutdown the server.
	cancel()
}

func runTests(t *testing.T, client pb.OpenFGAServiceClient) {
	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: "write_model_test"})
	require.NoError(t, err)

	storeID := resp.GetId()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err = client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				SchemaVersion:   typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(test.model),
			})
			require.Error(t, err)

		})
	}
}
