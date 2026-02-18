package testutils

import (
	"context"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/stretchr/testify/require"
)

const (
	alpineImage = "alpine:3"
)

func TestPullImage(t *testing.T) {
	ctx := context.Background()

	dc, err := NewDockerClient()
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, dc.Close())
	})

	require.NoError(t, dc.PullImage(ctx, alpineImage))

	err = dc.PullImage(ctx, "nonexistent/image:fake")
	require.Error(t, err)
}

func TestContainerLifecycle_PoolAndExec(t *testing.T) {
	ctx := context.Background()

	dc, err := NewDockerClient()
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, dc.Close())
	})

	require.NoError(t, dc.PullImage(ctx, alpineImage))

	containerCfg := &container.Config{
		Image: alpineImage,
		Cmd:   []string{"sh", "-c", "sleep 30"},
	}
	hostCfg := &container.HostConfig{}

	inspect, err := dc.RunContainer(ctx, containerCfg, hostCfg, "test-dc-client")
	require.NoError(t, err)
	require.NotEmpty(t, inspect.ID)

	t.Cleanup(func() {
		_ = dc.client.ContainerRemove(ctx, inspect.ID, container.RemoveOptions{
			Force: true,
		})
	})

	foundInspect, running, err := dc.FindRunningContainer(ctx, "test-dc-client", alpineImage)
	require.NoError(t, err)
	require.True(t, running)
	require.Equal(t, inspect.ID, foundInspect.ID)

	err = dc.ExecCommand(ctx, inspect.ID, container.ExecOptions{
		Cmd: []string{"echo", "hello!"},
	})
	require.NoError(t, err)
}

func TestFindRunningContainer_NotFound(t *testing.T) {
	ctx := context.Background()

	dc, err := NewDockerClient()
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, dc.Close())
	})

	_, found, err := dc.FindRunningContainer(ctx, "does-not-exist", alpineImage)
	require.NoError(t, err)
	require.False(t, found)
}

func TestExecCommand_FailsOnBadCmd(t *testing.T) {
	ctx := context.Background()

	dc, err := NewDockerClient()
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, dc.Close())
	})

	require.NoError(t, dc.PullImage(ctx, alpineImage))

	containerCfg := &container.Config{
		Image: alpineImage,
		Cmd:   []string{"sleep", "30"},
	}

	inspect, err := dc.RunContainer(ctx, containerCfg, &container.HostConfig{}, "exec-bad-cmd")
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = dc.client.ContainerRemove(ctx, inspect.ID, container.RemoveOptions{
			Force: true,
		})
	})

	err = dc.ExecCommand(ctx, inspect.ID, container.ExecOptions{
		Cmd: []string{"badcommand"},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "command [badcommand] completed with exit code 127")
}
