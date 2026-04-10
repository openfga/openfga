//go:build docker

package main

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/containerd/errdefs"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/testutils"
)

var (
	httpPort       = network.MustParsePort("8080/tcp")
	grpcPort       = network.MustParsePort("8081/tcp")
	playgroundPort = network.MustParsePort("3000/tcp")
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
// This function asserts that the container's exit code was 0.
func runOpenFGAContainerWithArgs(t *testing.T, commandArgs []string) OpenFGATester {
	t.Helper()

	dockerClient, err := client.New(client.FromEnv)
	require.NoError(t, err)
	t.Cleanup(func() {
		dockerClient.Close()
	})

	containerCfg := container.Config{
		Env: []string{},
		ExposedPorts: network.PortSet{
			httpPort:       {},
			grpcPort:       {},
			playgroundPort: {},
		},
		Image: "openfga/openfga:dockertest",
		Cmd:   commandArgs,
	}

	hostCfg := container.HostConfig{
		PublishAllPorts: true,
		PortBindings: network.PortMap{
			httpPort:       []network.PortBinding{},
			grpcPort:       []network.PortBinding{},
			playgroundPort: []network.PortBinding{},
		},
	}

	ulid := ulid.Make().String()
	name := fmt.Sprintf("openfga-%s", ulid)

	ctx := context.Background()

	cont, err := dockerClient.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name:       name,
		Config:     &containerCfg,
		HostConfig: &hostCfg,
	})
	require.NoError(t, err, "failed to create openfga docker container")

	_, err = dockerClient.ContainerStart(ctx, cont.ID, client.ContainerStartOptions{})
	require.NoError(t, err)

	t.Cleanup(func() {
		t.Logf("%s: stopping container %s", time.Now(), name)

		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		inspectResult, err := dockerClient.ContainerInspect(cctx, cont.ID, client.ContainerInspectOptions{})
		if errdefs.IsNotFound(err) {
			return
		}

		require.NoError(t, err)

		if inspectResult.Container.State.Running {
			timeoutSec := 5

			_, err = dockerClient.ContainerStop(cctx, cont.ID, client.ContainerStopOptions{Timeout: &timeoutSec})
			if err != nil && !errdefs.IsNotFound(err) {
				t.Logf("failed to stop openfga container: %v", err)
			}
		}

		waitResult := dockerClient.ContainerWait(cctx, cont.ID, client.ContainerWaitOptions{
			Condition: container.WaitConditionNotRunning,
		})

		var waitResponse container.WaitResponse

		select {
		case waitResponse = <-waitResult.Result:
		case err := <-waitResult.Error:
			if err != nil && !errdefs.IsNotFound(err) {
				assert.NoError(t, err)
			}
		case <-cctx.Done():
			assert.NoError(t, cctx.Err())
		}

		assert.Zero(t, waitResponse.StatusCode, "expected exit code of the container to be zero")

		_, err = dockerClient.ContainerRemove(context.Background(), cont.ID, client.ContainerRemoveOptions{Force: true})
		if err != nil && !errdefs.IsNotFound(err) {
			t.Logf("failed to remove openfga container: %v", err)
		}

		t.Logf("%s: stopped container %s", time.Now(), name)
	})

	inspectResult, err := dockerClient.ContainerInspect(ctx, cont.ID, client.ContainerInspectOptions{})
	require.NoError(t, err)

	ports := inspectResult.Container.NetworkSettings.Ports

	m, ok := ports[httpPort]
	if !ok || len(m) == 0 {
		t.Fatalf("failed to get HTTP host port mapping from openfga container")
	}
	httpHostPort := m[0].HostPort

	m, ok = ports[grpcPort]
	if !ok || len(m) == 0 {
		t.Fatalf("failed to get grpc host port mapping from openfga container")
	}
	grpcHostPort := m[0].HostPort

	if len(commandArgs) > 0 && commandArgs[0] == "run" {
		// wait for healthy service
		policy := backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(30 * time.Second))

		err = backoff.Retry(func() error {
			inspectResult, err := dockerClient.ContainerInspect(ctx, cont.ID, client.ContainerInspectOptions{})
			require.NoError(t, err)
			require.NotNil(t, inspectResult.Container.State.Health)

			if inspectResult.Container.State.Health.Status == container.Healthy {
				return nil
			}
			if inspectResult.Container.State.Health.Status == container.Unhealthy {
				for _, healthLog := range inspectResult.Container.State.Health.Log {
					t.Log(healthLog.Output)
				}
				return fmt.Errorf("container unhealthy")
			}
			return fmt.Errorf("container starting")
		}, policy)
		require.NoError(t, err)
	} else {
		// wait for command to finish
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		waitResult := dockerClient.ContainerWait(cctx, cont.ID, client.ContainerWaitOptions{
			Condition: container.WaitConditionNotRunning,
		})

		select {
		case <-waitResult.Result:
		case err := <-waitResult.Error:
			if err != nil && !errdefs.IsNotFound(err) {
				require.NoError(t, err)
			}
		case <-cctx.Done():
			require.NoError(t, cctx.Err())
		}
	}

	return &serverHandle{
		grpcAddress: fmt.Sprintf("localhost:%s", grpcHostPort),
		httpAddress: fmt.Sprintf("localhost:%s", httpHostPort),
	}
}

// TestDocker does basic sanity tests against the Dockerfile.
// It is not meant to include functionality tests.
// For that, go to the github.com/openfga/openfga/tests package.
func TestDocker(t *testing.T) {
	// uncomment when https://github.com/hashicorp/go-retryablehttp/issues/214 is solved
	// defer goleak.VerifyNone(t)
	t.Run("run_command", func(t *testing.T) {
		tester := runOpenFGAContainerWithArgs(t, []string{"run"})

		testutils.EnsureServiceHealthy(t, tester.GetGRPCAddress(), tester.GetHTTPAddress(), nil)

		t.Run("grpc_endpoint_works", func(t *testing.T) {
			conn := testutils.CreateGrpcConnection(t, tester.GetGRPCAddress())

			grpcClient := openfgav1.NewOpenFGAServiceClient(conn)

			createResp, err := grpcClient.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
				Name: "grpc_endpoint_works",
			})
			require.NoError(t, err)
			require.NotPanics(t, func() { ulid.MustParse(createResp.GetId()) })
		})

		t.Run("http_endpoint_works", func(t *testing.T) {
			response, err := retryablehttp.Get(fmt.Sprintf("http://%s/stores", tester.GetHTTPAddress()))
			require.NoError(t, err)

			t.Cleanup(func() {
				err := response.Body.Close()
				require.NoError(t, err)
			})

			require.Equal(t, http.StatusOK, response.StatusCode)
		})
	})

	t.Run("migrate_command", func(t *testing.T) {
		// this will be a no-op
		_ = runOpenFGAContainerWithArgs(t, []string{"migrate", "--datastore-engine", "memory"})
	})

	t.Run("version_command", func(t *testing.T) {
		_ = runOpenFGAContainerWithArgs(t, []string{"version"})
	})
}
