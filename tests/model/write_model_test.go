package model

import (
	"context"
	"log"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc/status"
)

var tt = map[string]struct {
	model string
	code  int
}{
	"case6": {
		model: `
		type user
		type group
		  relations
			define group as group from group
		`,
		code: 2056,
	},
	"case9": {
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
		code: 2056,
	},
	"case10": {
		model: `
		type user
		type group
		  relations
			define parent: [group] as self
			define viewer as reader from parent
		`,
		code: 2056,
	},
	"case11": {
		model: `
		type user
		type org
		type group
		  relations
			define parent: [group] as self
			define viewer as viewer from org
		`,
		code: 2056,
	},
	"case12": {
		model: `
		type user
		type org
		type group
		  relations
			define parent: [group] as self
			define viewer as org from parent
		`,
		code: 2056,
	},
	"case13": {
		model: `
		type user
		type org
		type group
		  relations
			define parent: [group, group#org] as self
		`,
		code: 2056,
	},
	"case17": {
		model: `
		type user
		type folder
		  relations
			define parent: [folder] as self or parent from parent
			define viewer: [user] as self or viewer from parent
		`,
		code: 2056,
	},
	"case18": {
		model: `
		type user
		type folder
		  relations
			define root: [folder] as self
			define parent: [folder] as self or root
			define viewer: [user] as self or viewer from parent
		`,
		code: 2056,
	},
	"case19": {
		model: `
		type user
		type folder
		  relations
			define root: [folder] as self
			define parent as root
			define viewer: [user] as self or viewer from parent
		`,
		code: 2056,
	},
	"case20": {
		model: `
		type user
		type folder
		  relations
			define root: [folder] as self
			define parent: [folder, folder#parent] as self
			define viewer: [user] as self or viewer from parent
		`,
		code: 2056,
	},
	"case21": {
		model: `
		type user
		type group
		  relations
			define member: [user] as self
			define reader as member and allowed
		`,
		code: 2056,
	},
	"case22": {
		model: `
		type user
		type group
		  relations
			define member: [user] as self
			define reader as member or allowed
		`,
		code: 2056,
	},
	"case23": {
		model: `
		type user
		type group
		  relations
			define member: [user] as self
			define reader as allowed but not member
		`,
		code: 2056,
	},
	"case24": {
		model: `
		type user
		type group
		  relations
			define member: [user] as self
			define reader as member but not allowed
		`,
		code: 2056,
	},
	"case25": {
		model: `
		type user
		type org
		  relations
			define member as self
		`,
		code: 2056,
	},
	"same_type_fails": {
		model: `
		type user
		type user
		`,
		code: 2056,
	},
	"difference_includes_itself_in_subtract_fails": {
		model: `
		type user
		type document
		  relations
			define viewer: [user] as self but not viewer
		`,
		code: 2056,
	},
	"union_includes_itself_fails": {
		model: `
		type user
		type document
		  relations
			define viewer: [user] as self or viewer
		`,
		code: 2056,
	},
	"intersection_includes_itself_fails": {
		model: `
		type user
		type document
		  relations
			define viewer: [user] as self and viewer
		`,
		code: 2056,
	},
	"simple_model_succeeds": {
		model: `
		type user
		type folder
		  relations
			define viewer: [user] as self
		type document
		  relations
			define parent: [folder] as self
			define viewer as viewer from parent
		`,
	},
	"no_relations_succeeds": {
		model: `
		type user
		`,
	},
	"union_may_contain_repeated_relations": {
		model: `
		type user
		type document
		  relations
			define editor: [user] as self
			define viewer as editor or editor
		`,
	},
	"intersection_may_contain_repeated_relations": {
		model: `
		type user
		type document
		  relations
			define editor: [user] as self
			define viewer as editor and editor
		`,
	},
	"exclusion_may_contain_repeated_relations": {
		model: `
		type user
		type document
		  relations
			define editor: [user] as self
			define viewer as editor but not editor
		`,
	},
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

	conn := tests.Connect(cfg.GRPC.Addr)
	defer conn.Close()

	runTests(t, pb.NewOpenFGAServiceClient(conn))

	// Shutdown the server.
	cancel()
}

func runTests(t *testing.T, client pb.OpenFGAServiceClient) {
	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: "write_model_test"})
	require.NoError(t, err)

	storeID := resp.GetId()

	for name, test := range tt {
		t.Run(name, func(t *testing.T) {
			_, err = client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
				StoreId:         storeID,
				SchemaVersion:   typesystem.SchemaVersion1_1,
				TypeDefinitions: parser.MustParse(test.model),
			})

			if test.code == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				e, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, test.code, int(e.Code()), err)
			}
		})
	}
}
