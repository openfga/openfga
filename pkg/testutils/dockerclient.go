package testutils

import (
	"context"
	"fmt"
	"io"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
)

// dockerClient is a simple wrapper around the Docker client
// to provide utility methods for managing containers and images in tests.
type dockerClient struct {
	client *client.Client
}

// NewDockerClient creates a new instance of dockerClient.
func NewDockerClient() (*dockerClient, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	return &dockerClient{client: cli}, nil
}

// Close calls the underlying Docker client's Close method, releasing its transport resources.
func (d *dockerClient) Close() error {
	return d.client.Close()
}

// PullImage checks if the specified image is already present locally, and if not, it pulls it from the registry.
func (d *dockerClient) PullImage(ctx context.Context, imageName string) error {
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

		// consume the image pull output to make sure it's done
		if _, err := io.Copy(io.Discard, reader); err != nil {
			return fmt.Errorf("consume image pull output: %w", err)
		}
	}

	return nil
}

// findRunningContainer returns the inspection data of a running container
// matching the given name and image, or nil/false if not found.
func (d *dockerClient) FindRunningContainer(
	ctx context.Context, containerName, imageName string,
) (*container.InspectResponse, bool, error) {
	filterArgs := filters.NewArgs(
		filters.Arg("name", containerName),
		filters.Arg("ancestor", imageName),
		filters.Arg("status", "running"),
	)

	containers, err := d.client.ContainerList(ctx, container.ListOptions{
		Filters: filterArgs,
		Latest:  true,
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
func (d *dockerClient) RunContainer(
	ctx context.Context, containerCfg *container.Config, hostCfg *container.HostConfig, containerName string,
) (*container.InspectResponse, error) {
	cont, err := d.client.ContainerCreate(ctx, containerCfg, hostCfg, nil, nil, containerName)
	if err != nil {
		return nil, fmt.Errorf("create %s container: %w", containerName, err)
	}

	if err := d.client.ContainerStart(ctx, cont.ID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("start %s container: %w", containerName, err)
	}

	inspect, err := d.client.ContainerInspect(ctx, cont.ID)
	if err != nil {
		return nil, fmt.Errorf("inspect %s container: %w", containerName, err)
	}

	return &inspect, nil
}

// execCommand executes a command in the specified container.
func (d *dockerClient) ExecCommand(ctx context.Context, containerID string, execConfig container.ExecOptions) error {
	exec, err := d.client.ContainerExecCreate(ctx, containerID, execConfig)
	if err != nil {
		return fmt.Errorf("failed to create exec for command %v: %w", execConfig.Cmd, err)
	}

	err = d.client.ContainerExecStart(ctx, exec.ID, container.ExecStartOptions{})
	if err != nil {
		return fmt.Errorf("failed to execute command %v: %w", execConfig.Cmd, err)
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
