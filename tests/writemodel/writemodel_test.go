package writemodel

import (
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/tests"
)

func TestWriteAuthorizationModel(t *testing.T) {
	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "error"
	cfg.Datastore.Engine = "memory"

	cancel := tests.StartServer(t, cfg)
	defer cancel()

	conn, err := grpc.Dial(cfg.GRPC.Addr,
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: grpcbackoff.DefaultConfig}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	RunAllTests(t, openfgav1.NewOpenFGAServiceClient(conn))
}
