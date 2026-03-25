package testutils

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

// DockerClient is a simple wrapper around the Docker client
// to provide utility methods for managing containers and images in tests.
type DockerClient struct {
	client *client.Client
}

// NewDockerClient creates a new instance of DockerClient.
func NewDockerClient() (*DockerClient, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
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
	images, err := d.client.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(
			filters.Arg("reference", imageName),
		),
	})
	if err != nil {
		return fmt.Errorf("list images: %w", err)
	}

	if len(images) == 0 {
		reader, err := d.client.ImagePull(ctx, imageName, image.PullOptions{})
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

// FindRunningContainer returns the inspection data of a running container
// matching the given name and image, or nil/false if not found.
func (d *DockerClient) FindRunningContainer(
	ctx context.Context, containerName, imageName string,
) (*container.InspectResponse, bool, error) {
	filterArgs := filters.NewArgs(
		filters.Arg("name", containerName),
		filters.Arg("ancestor", imageName),
		filters.Arg("status", "running"),
	)

	containers, err := d.client.ContainerList(ctx, container.ListOptions{
		Filters: filterArgs,
		Limit:   1,
	})
	if err != nil {
		return nil, false, fmt.Errorf("list containers: %w", err)
	}

	if len(containers) == 0 {
		return nil, false, nil
	}

	inspect, err := d.client.ContainerInspect(ctx, containers[0].ID)
	if err != nil {
		return nil, false, fmt.Errorf("inspect %s container: %w", containerName, err)
	}

	return &inspect, true, nil
}

// RunContainer starts a container and returns its inspection data.
func (d *DockerClient) RunContainer(
	ctx context.Context, containerCfg *container.Config, hostCfg *container.HostConfig, containerName string,
) (*container.InspectResponse, error) {
	cont, err := d.client.ContainerCreate(ctx, containerCfg, hostCfg, nil, nil, containerName)
	if err != nil {
		return nil, fmt.Errorf("create %s container: %w", containerName, err)
	}

	defer func() {
		if err != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			d.RemoveContainer(ctx, cont.ID)
		}
	}()

	err = d.client.ContainerStart(ctx, cont.ID, container.StartOptions{})
	if err != nil {
		return nil, fmt.Errorf("start %s container: %w", containerName, err)
	}

	backoffPolicy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
		backoff.WithMaxElapsedTime(5*time.Second),
	)

	var inspect container.InspectResponse
	err = backoff.Retry(func() error {
		inspect, err = d.client.ContainerInspect(ctx, cont.ID)
		if err != nil {
			return backoff.Permanent(fmt.Errorf("inspect %s container: %w", containerName, err))
		}

		if len(containerCfg.ExposedPorts) == 0 {
			return nil
		}

		if len(inspect.NetworkSettings.Ports) == 0 {
			return fmt.Errorf("port bindings not yet available for container %s", containerName)
		}

		for _, portBindings := range inspect.NetworkSettings.Ports {
			if len(portBindings) == 0 {
				return fmt.Errorf("port bindings not yet available for container %s", containerName)
			}
		}

		return nil
	}, backoff.WithContext(backoffPolicy, ctx))

	if err != nil {
		return nil, fmt.Errorf("inspect %s container: %w", containerName, err)
	}

	return &inspect, nil
}

// RemoveContainer kills and removes a container.
func (d *DockerClient) RemoveContainer(ctx context.Context, containerID string) error {
	removeOpts := container.RemoveOptions{
		Force:         true,
		RemoveVolumes: true,
	}

	if err := d.client.ContainerRemove(ctx, containerID, removeOpts); err != nil {
		return fmt.Errorf("remove container %s: %w", containerID, err)
	}

	return nil
}

// ExecCommand executes a command in the specified container and waits for it to complete.
func (d *DockerClient) ExecCommand(ctx context.Context, containerID string, execConfig container.ExecOptions) error {
	exec, err := d.client.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return fmt.Errorf("failed to create exec for command %v: %w", execConfig.Cmd, err)
	}

	resp, err := d.client.ContainerExecAttach(ctx, exec.ID, container.ExecAttachOptions{})
	if err != nil {
		return fmt.Errorf("failed to execute command %v: %w", execConfig.Cmd, err)
	}
	defer resp.Close()

	if _, err := io.Copy(io.Discard, resp.Reader); err != nil {
		return fmt.Errorf("failed to read exec output for command %v: %w", execConfig.Cmd, err)
	}

	inspect, err := d.client.ContainerExecInspect(ctx, exec.ID)
	if err != nil {
		return fmt.Errorf("failed to inspect exec %v: %w", execConfig.Cmd, err)
	}

	if inspect.ExitCode != 0 {
		return fmt.Errorf("command %v completed with exit code %d", execConfig.Cmd, inspect.ExitCode)
	}

	return nil
}

// GetHostPort returns the published host port for the given container port.
func (d *DockerClient) GetHostPort(inspect *container.InspectResponse, containerPort nat.Port) (string, error) {
	m, ok := inspect.NetworkSettings.Ports[containerPort]
	if !ok || len(m) == 0 {
		return "", fmt.Errorf("port bindings not available for container port %s", containerPort)
	}

	return m[0].HostPort, nil
}
