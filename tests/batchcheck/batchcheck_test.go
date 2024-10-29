package batchcheck

import (
	"testing"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"go.uber.org/goleak"

	"github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/tests"
)

func TestBatchCheckMemory(t *testing.T) {
	testRunAll(t, "memory")
}

func TestCheckPostgres(t *testing.T) {
	testRunAll(t, "postgres")
}

func TestCheckMySQL(t *testing.T) {
	testRunAll(t, "mysql")
}

func TestCheckSQLite(t *testing.T) {
	testRunAll(t, "sqlite")
}

func testRunAll(t *testing.T, engine string) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	cfg := config.MustDefaultConfig()
	cfg.Experimentals = append(cfg.Experimentals, "enable-check-optimizations")
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = engine
	cfg.ListUsersDeadline = 0 // no deadline
	// extend the timeout for the tests, coverage makes them slower
	cfg.RequestTimeout = 10 * time.Second

	tests.StartServer(t, cfg)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	RunAllTests(t, openfgav1.NewOpenFGAServiceClient(conn))
}
