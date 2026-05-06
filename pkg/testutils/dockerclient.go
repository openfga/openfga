package testutils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// DockerClient is a simple wrapper around the Docker client
// to provide utility methods for managing containers and images in tests.
type DockerClient struct {
	client *client.Client
}

// NewDockerClient creates a new instance of DockerClient.
func NewDockerClient() (*DockerClient, error) {
	cli, err := client.New(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	return &DockerClient{client: cli}, nil
}

// Close calls the underlying Docker client's Close method, releasing its transport resources.
func (d *DockerClient) Close() error {
	return d.client.Close()
}

// PullImage checks if the specified image is already present locally, and if not, it pulls it from the registry.
func (d *DockerClient) PullImage(ctx context.Context, imageName string) error {
	imageListResult, err := d.client.ImageList(ctx, client.ImageListOptions{
		Filters: make(client.Filters).Add("reference", imageName),
	})
	if err != nil {
		return fmt.Errorf("list images: %w", err)
	}

	if len(imageListResult.Items) == 0 {
		reader, err := d.client.ImagePull(ctx, imageName, client.ImagePullOptions{})
		if err != nil {
			return fmt.Errorf("pull image: %w", err)
		}
		defer reader.Close()

		// consume the image pull output to make sure it's done
		if _, err := io.Copy(io.Discard, reader); err != nil {
			return fmt.Errorf("consume image pull output: %w", err)
		}
	}

	return nil
}

// RunContainer creates and starts a new container.
func (d *DockerClient) RunContainer(
	ctx context.Context, containerCfg *container.Config, hostCfg *container.HostConfig, containerName string,
) (*container.InspectResponse, error) {
	_, err := d.client.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name:       containerName,
		Config:     containerCfg,
		HostConfig: hostCfg,
	})

	if err != nil {
		return nil, fmt.Errorf("create %s container: %w", containerName, err)
	}

	defer func() {
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_ = d.RemoveContainer(cleanupCtx, containerName)
		}
	}()

	if _, err = d.client.ContainerStart(ctx, containerName, client.ContainerStartOptions{}); err != nil {
		return nil, fmt.Errorf("start %s container: %w", containerName, err)
	}

	backoffPolicy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
		backoff.WithMaxElapsedTime(30*time.Second),
	)

	var cont *container.InspectResponse
	err = backoff.Retry(func() error {
		inspectResult, err := d.client.ContainerInspect(ctx, containerName, client.ContainerInspectOptions{})
		if err != nil {
			if errdefs.IsNotFound(err) {
				return err
			}

			return backoff.Permanent(fmt.Errorf("inspect %s container: %w", containerName, err))
		}
		cont = &inspectResult.Container

		if cont.State == nil || !cont.State.Running {
			return fmt.Errorf("container %s is not running yet", containerName)
		}

		if len(containerCfg.ExposedPorts) == 0 {
			return nil
		}

		if len(cont.NetworkSettings.Ports) == 0 {
			return fmt.Errorf("port bindings not yet available for container %s", containerName)
		}

		for _, portBindings := range cont.NetworkSettings.Ports {
			if len(portBindings) == 0 {
				return fmt.Errorf("port bindings not yet available for container %s", containerName)
			}
		}

		return nil
	}, backoff.WithContext(backoffPolicy, ctx))

	if err != nil {
		return nil, fmt.Errorf("inspect %s container: %w", containerName, err)
	}

	return cont, nil
}

// RemoveContainer kills and removes a container.
func (d *DockerClient) RemoveContainer(ctx context.Context, containerID string) error {
	removeOpts := client.ContainerRemoveOptions{
		Force:         true,
		RemoveVolumes: true,
	}

	if _, err := d.client.ContainerRemove(ctx, containerID, removeOpts); err != nil {
		return fmt.Errorf("remove container %s: %w", containerID, err)
	}

	return nil
}

// ExecCommand executes a command in the specified container and waits for it to complete.
func (d *DockerClient) ExecCommand(ctx context.Context, containerID string, execConfig client.ExecCreateOptions) error {
	execConfig.AttachStdout = true
	execConfig.AttachStderr = true

	exec, err := d.client.ExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return fmt.Errorf("failed to create exec for command %v: %w", execConfig.Cmd, err)
	}

	resp, err := d.client.ExecAttach(ctx, exec.ID, client.ExecAttachOptions{})
	if err != nil {
		return fmt.Errorf("failed to execute command %v: %w", execConfig.Cmd, err)
	}
	defer resp.Close()

	var stdout, stderr bytes.Buffer
	_, err = stdcopy.StdCopy(&stdout, &stderr, resp.Reader)
	if err != nil {
		return fmt.Errorf("failed to read exec output for command %v: %w", execConfig.Cmd, err)
	}

	inspect, err := d.client.ExecInspect(ctx, exec.ID, client.ExecInspectOptions{})
	if err != nil {
		return fmt.Errorf("failed to inspect exec %v: %w", execConfig.Cmd, err)
	}

	if inspect.ExitCode != 0 {
		return fmt.Errorf(
			"command %v completed with exit code %d: stdout=%q stderr=%q",
			execConfig.Cmd,
			inspect.ExitCode,
			stdout.String(),
			stderr.String(),
		)
	}

	return nil
}

// GetHostPort returns the published host port for the given container port.
func (d *DockerClient) GetHostPort(inspect *container.InspectResponse, containerPort network.Port) (string, error) {
	m, ok := inspect.NetworkSettings.Ports[containerPort]
	if !ok || len(m) == 0 {
		return "", fmt.Errorf("port bindings not available for container port %s", containerPort)
	}

	return m[0].HostPort, nil
}
