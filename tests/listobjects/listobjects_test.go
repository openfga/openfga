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

// RunAllTests will invoke all list objects tests
func RunAllTests(t *testing.T, client ListObjectsClientInterface) {
	t.Run("RunAll", func(t *testing.T) {
		t.Run("ListObjects", func(t *testing.T) {
			t.Parallel()
			testListObjects(t, client)
		})
	})
}

func testListObjects(t *testing.T, client ListObjectsClientInterface) {
	t.Run("Schema1_1", func(t *testing.T) {
		t.Parallel()
		RunSchema1_1ListObjectsTests(t, client)
	})
	t.Run("Schema1_0", func(t *testing.T) {
		t.Parallel()
		RunSchema1_0ListObjectsTests(t, client)
	})
}
