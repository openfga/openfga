//go:build functional
// +build functional

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
)

type OpenFGATester interface {
	GetGRPCPort() string
	GetHTTPPort() string
	Cleanup() func()
}

type serverHandle struct {
	grpcPort string
	httpPort string
	cleanup  func()
}

func (s *serverHandle) GetGRPCPort() string {
	return s.grpcPort
}

func (s *serverHandle) GetHTTPPort() string {
	return s.httpPort
}

func (s *serverHandle) Cleanup() func() {
	return s.cleanup
}

// newOpenFGAContainer spins up an openfga container with the default configuration
// exposed for testing purposes. It is assumed that the openfga/functionaltest image is available
func newOpenFGAContainer(t *testing.T) OpenFGATester {
	t.Helper()

	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	require.NoError(t, err)

	containerCfg := container.Config{
		Env: []string{},
		ExposedPorts: nat.PortSet{
			nat.Port("8080/tcp"): {},
			nat.Port("8081/tcp"): {},
			nat.Port("3000/tcp"): {},
		},
		Image: "openfga/openfga:functionaltest",
		Cmd:   []string{"run"},
	}

	hostCfg := container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
		PortBindings: nat.PortMap{
			"8080/tcp": []nat.PortBinding{},
			"8081/tcp": []nat.PortBinding{},
			"3000/tcp": []nat.PortBinding{},
		},
	}

	ulid := ulid.Make().String()
	name := fmt.Sprintf("openfga-%s", ulid)

	ctx := context.Background()

	cont, err := dockerClient.ContainerCreate(ctx, &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create openfga docker container")

	stopContainer := func() {
		t.Logf("stopping container %s", name)
		timeoutSec := 5

		err := dockerClient.ContainerStop(ctx, cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("failed to stop openfga container: %v", err)
		}

		dockerClient.Close()
		t.Logf("stopped container %s", name)
	}

	err = dockerClient.ContainerStart(ctx, cont.ID, types.ContainerStartOptions{})
	require.NoError(t, err)

	t.Cleanup(func() {
		stopContainer()
	})

	// spin up a goroutine to survive any test panics or terminations to expire/stop the running container
	go func() {
		time.Sleep(2 * time.Minute)
		timeoutSec := 0

		t.Logf("expiring container %s", name)
		// swallow the error because by this point we've terminated
		_ = dockerClient.ContainerStop(ctx, cont.ID, container.StopOptions{Timeout: &timeoutSec})

		t.Logf("expired container %s", name)
	}()

	containerJSON, err := dockerClient.ContainerInspect(ctx, cont.ID)
	require.NoError(t, err)

	ports := containerJSON.NetworkSettings.Ports

	m, ok := ports["8080/tcp"]
	if !ok || len(m) == 0 {
		t.Fatalf("failed to get HTTP host port mapping from openfga container")
	}
	httpPort := m[0].HostPort

	m, ok = ports["8081/tcp"]
	if !ok || len(m) == 0 {
		t.Fatalf("failed to get grpc host port mapping from openfga container")
	}
	grpcPort := m[0].HostPort

	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	creds := insecure.NewCredentials()

	dialOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: grpcbackoff.DefaultConfig}),
	}

	conn, err := grpc.DialContext(
		timeoutCtx,
		fmt.Sprintf("localhost:%s", grpcPort),
		dialOpts...,
	)
	require.NoError(t, err)
	defer conn.Close()

	client := healthv1pb.NewHealthClient(conn)

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 30 * time.Second

	err = backoff.Retry(func() error {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		resp, err := client.Check(timeoutCtx, &healthv1pb.HealthCheckRequest{
			Service: openfgav1.OpenFGAService_ServiceDesc.ServiceName,
		})
		if err != nil {
			return err
		}

		if resp.GetStatus() != healthv1pb.HealthCheckResponse_SERVING {
			return fmt.Errorf("not serving")
		}

		return nil
	},
		backoffPolicy,
	)
	require.NoError(t, err)

	return &serverHandle{
		grpcPort: grpcPort,
		httpPort: httpPort,
		cleanup:  stopContainer,
	}
}

// createGrpcConnection connects to the underlying grpc server of the OpenFGATester and
// returns the client connection. It is up to the caller to call Close() on the connection
func createGrpcConnection(t *testing.T, tester OpenFGATester) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.Dial(
		fmt.Sprintf("localhost:%s", tester.GetGRPCPort()),
		[]grpc.DialOption{
			grpc.WithBlock(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}...,
	)
	require.NoError(t, err)

	return conn
}

// TestDocker does basic sanity tests against the Dockerfile.
// It is not meant to include functionality tests.
// For that, go to the github.com/openfga/openfga/tests package.
func TestDocker(t *testing.T) {
	tester := newOpenFGAContainer(t)
	defer tester.Cleanup()

	t.Run("grpc_endpoint_works", func(t *testing.T) {
		conn := createGrpcConnection(t, tester)
		defer conn.Close()

		grpcClient := openfgav1.NewOpenFGAServiceClient(conn)

		createResp, err := grpcClient.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
			Name: "max_message_size",
		})
		require.NoError(t, err)
		require.NotPanics(t, func() { ulid.MustParse(createResp.GetId()) })
	})

	t.Run("http_endpoint_works", func(t *testing.T) {
		// TODO
	})
}
