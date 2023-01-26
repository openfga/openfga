package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
)

func StartServer(t *testing.T, cfg *cmd.Config) context.CancelFunc {
	container := storage.RunDatastoreTestContainer(t, cfg.Datastore.Engine)
	cfg.Datastore.URI = container.GetConnectionURI()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		err := cmd.RunServer(ctx, cfg)
		require.NoError(t, err)
	}()

	return cancel
}

func Connect(t *testing.T, addr string) *grpc.ClientConn {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	// Ensure the service is up before continuing.
	client := healthv1pb.NewHealthClient(conn)
	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 5 * time.Second
	err = backoff.Retry(func() error {
		resp, err := client.Check(context.Background(), &healthv1pb.HealthCheckRequest{
			Service: pb.OpenFGAService_ServiceDesc.ServiceName,
		})
		if err != nil {
			return err
		}

		if resp.GetStatus() != healthv1pb.HealthCheckResponse_SERVING {
			return fmt.Errorf("not serving")
		}

		return nil
	}, policy)
	require.NoError(t, err)

	return conn
}
