//go:build docker
// +build docker

package main

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/openfga/openfga/pkg/testutils"
)

type OpenFGATester interface {
	GetGRPCAddress() string
	GetHTTPAddress() string
}

type serverHandle struct {
	grpcAddress string
	httpAddress string
}

func (s *serverHandle) GetGRPCAddress() string {
	return s.grpcAddress
}

func (s *serverHandle) GetHTTPAddress() string {
	return s.httpAddress
}

// runOpenFGAContainerWithArgs spins up an openfga container with the default configuration
// exposed for testing purposes. It is assumed that the openfga/dockertest image is available.
// The caller must call the returned function after the container is no longer needed.
// This function asserts that the container's exit code was 0.
func runOpenFGAContainerWithArgs(t *testing.T, commandArgs []string) (OpenFGATester, func()) {
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
		Image: "openfga/openfga:dockertest",
		Cmd:   commandArgs,
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

	err = dockerClient.ContainerStart(ctx, cont.ID, types.ContainerStartOptions{})
	require.NoError(t, err)

	stopContainer := func() {
		t.Logf("stopping container %s", name)

		containerJSON, err := dockerClient.ContainerInspect(ctx, cont.ID)
		require.NoError(t, err)
		require.Zero(t, containerJSON.State.ExitCode, "expected exit code of the container to be zero")

		timeoutSec := 5

		err = dockerClient.ContainerStop(ctx, cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("failed to stop openfga container: %v", err)
		}

		dockerClient.Close()
		t.Logf("stopped container %s", name)
	}

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

	if len(commandArgs) > 0 && commandArgs[0] == "run" {
		policy := backoff.NewExponentialBackOff()
		policy.MaxElapsedTime = 30 * time.Second

		err = backoff.Retry(func() error {
			containerJSON, err := dockerClient.ContainerInspect(ctx, cont.ID)
			require.NoError(t, err)
			require.NotNil(t, containerJSON.State.Health)

			if containerJSON.State.Health.Status == types.Healthy {
				return nil
			}
			if containerJSON.State.Health.Status == types.Unhealthy {
				for _, healthLog := range containerJSON.State.Health.Log {
					t.Log(healthLog.Output)
				}
				return fmt.Errorf("container unhealthy")
			}
			return fmt.Errorf("container starting")
		}, policy)
		require.NoError(t, err)
	}

	return &serverHandle{
		grpcAddress: fmt.Sprintf("localhost:%s", grpcPort),
		httpAddress: fmt.Sprintf("localhost:%s", httpPort),
	}, stopContainer
}

// createGrpcConnection connects to the underlying grpc server of the OpenFGATester and
// returns the client connection. It is up to the caller to call Close() on the connection
func createGrpcConnection(t *testing.T, tester OpenFGATester) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.Dial(
		tester.GetGRPCAddress(),
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
	// uncomment when https://github.com/hashicorp/go-retryablehttp/issues/214 is solved
	//defer goleak.VerifyNone(t)
	t.Run("run_command", func(t *testing.T) {
		tester, stopContainer := runOpenFGAContainerWithArgs(t, []string{"run"})
		defer stopContainer()

		testutils.EnsureServiceHealthy(t, tester.GetGRPCAddress(), tester.GetHTTPAddress(), nil, true)

		t.Run("grpc_endpoint_works", func(t *testing.T) {
			conn := createGrpcConnection(t, tester)
			defer conn.Close()

			grpcClient := openfgav1.NewOpenFGAServiceClient(conn)

			createResp, err := grpcClient.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
				Name: "grpc_endpoint_works",
			})
			require.NoError(t, err)
			require.NotPanics(t, func() { ulid.MustParse(createResp.GetId()) })
		})

		t.Run("http_endpoint_works", func(t *testing.T) {
			resp, err := retryablehttp.Get(fmt.Sprintf("http://%s/stores", tester.GetHTTPAddress()))
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)
		})
	})

	t.Run("migrate_command", func(t *testing.T) {
		// this will be a no-op
		_, stopContainer := runOpenFGAContainerWithArgs(t, []string{"migrate", "--datastore-engine", "memory"})
		defer stopContainer()
	})

	t.Run("version_command", func(t *testing.T) {
		_, stopContainer := runOpenFGAContainerWithArgs(t, []string{"version"})
		defer stopContainer()
	})
}
