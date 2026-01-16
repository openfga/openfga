package check

import (
	"testing"
	"time"

	"go.uber.org/goleak"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/tests"
)

func TestMatrixMemory(t *testing.T) {
	runMatrixWithEngine(t, "memory")
}

func runMatrixWithEngine(t *testing.T, engine string) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	clientWithExperimentals := tests.BuildClientInterface(t, engine, []string{config.ExperimentalCheckOptimizations})
	RunMatrixTests(t, engine, true, clientWithExperimentals)

	clientWithoutExperimentals := tests.BuildClientInterface(t, engine, []string{})
	RunMatrixTests(t, engine, false, clientWithoutExperimentals)
}

func TestCheckMemory(t *testing.T) {
	testRunAll(t, "memory")
}

func testRunAll(t *testing.T, engine string) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	cfg := config.MustDefaultConfig()
	cfg.Experimentals = append(cfg.Experimentals, config.ExperimentalCheckOptimizations)
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = engine
	// extend the timeout for the tests, coverage makes them slower
	cfg.RequestTimeout = 10 * time.Second
	cfg.SharedIterator.Enabled = true

	cfg.CheckIteratorCache.Enabled = true

	tests.StartServer(t, cfg)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	RunAllTests(t, openfgav1.NewOpenFGAServiceClient(conn))
}
