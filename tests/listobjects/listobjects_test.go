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

func TestListObjectsMemory(t *testing.T) {
	testRunAll(t, "memory")
}

func TestListObjectsPostgres(t *testing.T) {
	testRunAll(t, "postgres")
}

func TestListObjectsMySQL(t *testing.T) {
	testRunAll(t, "mysql")
}

func testRunAll(t *testing.T, engine string) {
	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = engine
	cfg.AllowWriting1Dot0Models = true
	cfg.AllowEvaluating1Dot0Models = true

	cancel := tests.StartServer(t, cfg)
	defer cancel()

	conn, err := grpc.Dial(cfg.GRPC.Addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()
	RunAllTests(t, pb.NewOpenFGAServiceClient(conn))
}
