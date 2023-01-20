package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
)

func Connect(addr string) *grpc.ClientConn {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(fmt.Sprintf("error dailing: %s", err))
	}

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
	if err != nil {
		panic(fmt.Sprintf("service not up: %s", err))
	}

	return conn
}
