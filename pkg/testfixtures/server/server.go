package server

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

const (
	openfgaImage  = "openfga/openfga:v%s"
	expireTimeout = 10 * time.Minute
)

type OpenfgaContainer struct {
	URL string
}

func RunOpenfgaInContainer(t testing.TB, openFGAversion string, datastoreEngine string, datastoreURI string) *OpenfgaContainer {
	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	require.NoError(t, err)

	openfgaImageWithVersion := fmt.Sprintf(openfgaImage, openFGAversion)
	t.Logf("Pulling image %s", openfgaImageWithVersion)
	reader, err := dockerClient.ImagePull(context.Background(), openfgaImageWithVersion, types.ImagePullOptions{})
	require.NoError(t, err)

	_, err = io.Copy(io.Discard, reader) // consume the image pull output to make sure it's done
	require.NoError(t, err)

	containerCfg := container.Config{
		Env: []string{
			"OPENFGA_PLAYGROUND_ENABLED=false",
			"OPENFGA_PROFILER_ENABLED=false",
			"OPENFGA_METRICS_ENABLED=false",
			fmt.Sprintf("OPENFGA_DATASTORE_ENGINE=%s", datastoreEngine),
			fmt.Sprintf("OPENFGA_DATASTORE_URI=%s", datastoreURI),
		},
		ExposedPorts: nat.PortSet{
			nat.Port("8080/tcp"): {},
		},
		Image: openfgaImageWithVersion,
		Cmd:   []string{"run"},
	}

	hostCfg := container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
	}

	name := fmt.Sprintf("openfga-%s", ulid.Make().String())

	cont, err := dockerClient.ContainerCreate(context.Background(), &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create openfga docker container")

	stopContainer := func() {
		t.Logf("stopping container %s", name)
		timeoutSec := 5

		err := dockerClient.ContainerStop(context.Background(), cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("failed to stop openfga container: %v", err)
		}

		dockerClient.Close()
		t.Logf("stopped container %s", name)
	}

	err = dockerClient.ContainerStart(context.Background(), cont.ID, types.ContainerStartOptions{})
	if err != nil {
		stopContainer()
		t.Fatalf("failed to start openfga container: %v", err)
	}

	containerJSON, err := dockerClient.ContainerInspect(context.Background(), cont.ID)
	require.NoError(t, err)

	p, ok := containerJSON.NetworkSettings.Ports["8080/tcp"]
	if !ok || len(p) == 0 {
		t.Fatalf("failed to get host port mapping from openfga container")
	}

	// spin up a goroutine to survive any test panics to expire/stop the running container
	go func() {
		time.Sleep(expireTimeout)
		timeoutSec := 0

		t.Logf("expiring container %s", name)
		err := dockerClient.ContainerStop(context.Background(), cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("failed to expire mysql container: %v", err)
		}
		t.Logf("expired container %s", name)
	}()

	t.Cleanup(func() {
		stopContainer()
	})

	return &OpenfgaContainer{URL: fmt.Sprintf("localhost:%s", p[0].HostPort)}
}
