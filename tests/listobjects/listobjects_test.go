package listobjects

import (
	"testing"
	"time"

	"go.uber.org/goleak"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/server/config"
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

func TestListObjectsSQLite(t *testing.T) {
	testRunAll(t, "sqlite")
}

func testRunAll(t *testing.T, engine string) {
	t.Cleanup(func() {
		// [Goroutine 60101 in state select, with github.com/go-sql-driver/mysql.(*mysqlConn).startWatcher.func1 on top of the stack:
		// github.com/go-sql-driver/mysql.(*mysqlConn).startWatcher.func1()
		// 	/home/runner/go/pkg/mod/github.com/go-sql-driver/mysql@v1.8.1/connection.go:628 +0x105
		// created by github.com/go-sql-driver/mysql.(*mysqlConn).startWatcher in goroutine 60029
		// 	/home/runner/go/pkg/mod/github.com/go-sql-driver/mysql@v1.8.1/connection.go:625 +0x1dd
		// ]
		goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/go-sql-driver/mysql.(*mysqlConn).startWatcher.func1"))
	})
	cfg := config.MustDefaultConfig()
	cfg.Experimentals = append(cfg.Experimentals, "enable-check-optimizations")
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = engine
	cfg.ListObjectsDeadline = 0 // no deadline
	// extend the timeout for the tests, coverage makes them slower
	cfg.RequestTimeout = 10 * time.Second

	tests.StartServer(t, cfg)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	RunAllTests(t, openfgav1.NewOpenFGAServiceClient(conn))
}
