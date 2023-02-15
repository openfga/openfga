package check

import (
	"context"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc/status"
)

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
	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = engine

	cancel := tests.StartServer(t, cfg)
	defer cancel()

	conn := tests.Connect(t, cfg.GRPC.Addr)
	defer conn.Close()

	RunTests(t, pb.NewOpenFGAServiceClient(conn))
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
