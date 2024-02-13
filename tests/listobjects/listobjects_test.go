package listobjects

import (
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"go.uber.org/goleak"

	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/tests"
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
	defer goleak.VerifyNone(t)
	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = engine

	cancel := tests.StartServer(t, cfg)
	defer cancel()

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)
	defer conn.Close()
	RunAllTests(t, openfgav1.NewOpenFGAServiceClient(conn))
}
