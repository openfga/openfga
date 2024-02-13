package writemodel

import (
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/tests"
)

func TestWriteAuthorizationModel(t *testing.T) {
	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = "memory"

	cancel := tests.StartServer(t, cfg)
	defer cancel()

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)
	defer conn.Close()

	RunAllTests(t, openfgav1.NewOpenFGAServiceClient(conn))
}
