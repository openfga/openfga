package writemodel

import (
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/tests"
)

func TestWriteAuthorizationModel(t *testing.T) {
	cfg := testutils.MustDefaultConfig()
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = "memory"

	tests.StartServer(t, cfg)

	conn := testutils.CreateGrpcConnection(t, cfg.GRPC.Addr)

	RunAllTests(t, openfgav1.NewOpenFGAServiceClient(conn))
}
