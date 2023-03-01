package listobjects

import (
	"testing"

	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/tests"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	testListObjects(t, engine)
}

func testListObjects(t *testing.T, engine string) {
	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = engine

	cancel := tests.StartServer(t, cfg)
	defer cancel()

	conn, err := grpc.Dial(cfg.GRPC.Addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()
	t.Run("testListObjects", func(t *testing.T) {
		RunSchema1_1ListObjectsTests(t, pb.NewOpenFGAServiceClient(conn))
		RunSchema1_0ListObjectsTests(t, pb.NewOpenFGAServiceClient(conn))
	})
}
