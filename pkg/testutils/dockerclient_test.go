package testutils

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

const (
	alpineImage = "alpine:3"
)

var (
	testPort = network.MustParsePort("5432/tcp")
)

func TestNewDockerClient_InvalidHost(t *testing.T) {
	t.Setenv("DOCKER_HOST", "foobar")

	dc, err := NewDockerClient()
	require.Nil(t, dc)
	require.Error(t, err)
	require.ErrorContains(t, err, "create docker client")
}

func TestPullImage(t *testing.T) {
	dc, err := NewDockerClient()
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, dc.Close())
	})

	require.NoError(t, dc.PullImage(t.Context(), alpineImage))
}

func TestPullImage_Fail(t *testing.T) {
	t.Run("list_images", func(t *testing.T) {
		dc := newDockerClientMock(t, []dockerMockStep{
			{Method: http.MethodGet, Path: "/images/json", Error: "list failed"},
		})
		err := dc.PullImage(t.Context(), alpineImage)
		require.Error(t, err)
		require.ErrorContains(t, err, "list images")
	})

	t.Run("image_nonexistent", func(t *testing.T) {
		dc := newDockerClientMock(t, []dockerMockStep{
			{Method: http.MethodGet, Path: "/images/json", Body: []byte(`[]`)},
			{Method: http.MethodPost, Path: "/images/create", Error: "pull failed"},
		})
		err := dc.PullImage(t.Context(), "image:nonexistent")
		require.Error(t, err)
		require.ErrorContains(t, err, "pull image")
	})
}

func TestContainerLifecycle_PullAndExec(t *testing.T) {
	dc, err := NewDockerClient()
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, dc.Close())
	})

	require.NoError(t, dc.PullImage(t.Context(), alpineImage))

	containerCfg := &container.Config{
		Image: alpineImage,
		Cmd:   []string{"sh", "-c", "sleep 30"},
	}
	hostCfg := &container.HostConfig{}
	containerName := "test-dc-client-" + ulid.Make().String()

	inspect, err := dc.RunContainer(t.Context(), containerCfg, hostCfg, containerName)
	require.NoError(t, err)
	require.NotEmpty(t, inspect.ID)

	t.Cleanup(func() {
		_ = dc.RemoveContainer(context.Background(), inspect.ID)
	})

	foundInspect, running, err := dc.FindRunningContainer(t.Context(), containerName, alpineImage)
	require.NoError(t, err)
	require.True(t, running)
	require.Equal(t, inspect.ID, foundInspect.ID)

	err = dc.ExecCommand(t.Context(), inspect.ID, client.ExecCreateOptions{
		Cmd: []string{"echo", "hello!"},
	})
	require.NoError(t, err)
}

func TestFindRunningContainer_Fail(t *testing.T) {
	t.Run("not_found", func(t *testing.T) {
		dc := newDockerClientMock(t, []dockerMockStep{
			{Method: http.MethodGet, Path: "/containers/json", Body: []byte(`[]`)},
		})

		_, found, err := dc.FindRunningContainer(t.Context(), "does-not-exist", alpineImage)
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("list_containers", func(t *testing.T) {
		dc := newDockerClientMock(t, []dockerMockStep{
			{Method: http.MethodGet, Path: "/containers/json", Error: "list failed"},
		})

		inspect, found, err := dc.FindRunningContainer(t.Context(), "test", alpineImage)
		require.Nil(t, inspect)
		require.False(t, found)
		require.Error(t, err)
		require.ErrorContains(t, err, "list containers")
	})

	t.Run("inspect_container", func(t *testing.T) {
		dc := newDockerClientMock(t, []dockerMockStep{
			{Method: http.MethodGet, Path: "/containers/json", Body: []byte(`[{"Id":"container-id"}]`)},
			{Method: http.MethodGet, Path: "/containers/container-id/json", Error: "inspect failed"},
		})

		inspect, found, err := dc.FindRunningContainer(t.Context(), "test", alpineImage)
		require.Nil(t, inspect)
		require.False(t, found)
		require.Error(t, err)
		require.ErrorContains(t, err, "inspect test container")
	})
}

func TestRunContainer_WithExposedPorts(t *testing.T) {
	createBody := []byte(`{"Id":"container-id"}`)
	startBody := []byte(`{}`)
	portsReadyBody := []byte(`{"Id":"container-id","NetworkSettings":{"Ports":{"5432/tcp":[{"HostIp":"0.0.0.0","HostPort":"54321"}]}}}`)

	exposedCfg := &container.Config{
		Image:        alpineImage,
		ExposedPorts: network.PortSet{testPort: struct{}{}},
	}

	t.Run("ports_ready_immediately", func(t *testing.T) {
		dc := newDockerClientMock(t, []dockerMockStep{
			{Method: http.MethodPost, Path: "/containers/create", Body: createBody},
			{Method: http.MethodPost, Path: "/containers/container-name/start", Body: startBody},
			{Method: http.MethodGet, Path: "/containers/container-name/json", Body: portsReadyBody},
		})

		inspect, err := dc.RunContainer(t.Context(), exposedCfg, &container.HostConfig{}, "container-name")
		require.NoError(t, err)
		require.NotNil(t, inspect)
	})

	t.Run("empty_ports_map_then_ready", func(t *testing.T) {
		noPortsBody := []byte(`{"Id":"container-id","NetworkSettings":{"Ports":{}}}`)
		dc := newDockerClientMock(t, []dockerMockStep{
			{Method: http.MethodPost, Path: "/containers/create", Body: createBody},
			{Method: http.MethodPost, Path: "/containers/container-name/start", Body: startBody},
			{Method: http.MethodGet, Path: "/containers/container-name/json", Body: noPortsBody},
			{Method: http.MethodGet, Path: "/containers/container-name/json", Body: portsReadyBody},
		})

		inspect, err := dc.RunContainer(t.Context(), exposedCfg, &container.HostConfig{}, "container-name")
		require.NoError(t, err)
		require.NotNil(t, inspect)
	})

	t.Run("empty_port_bindings_then_ready", func(t *testing.T) {
		emptyBindingsBody := []byte(`{"Id":"container-id","NetworkSettings":{"Ports":{"5432/tcp":[]}}}`)
		dc := newDockerClientMock(t, []dockerMockStep{
			{Method: http.MethodPost, Path: "/containers/create", Body: createBody},
			{Method: http.MethodPost, Path: "/containers/container-name/start", Body: startBody},
			{Method: http.MethodGet, Path: "/containers/container-name/json", Body: emptyBindingsBody},
			{Method: http.MethodGet, Path: "/containers/container-name/json", Body: portsReadyBody},
		})

		inspect, err := dc.RunContainer(t.Context(), exposedCfg, &container.HostConfig{}, "container-name")
		require.NoError(t, err)
		require.NotNil(t, inspect)
	})
}

func TestRunContainer_Fail(t *testing.T) {
	createBody := []byte(`{"Id":"container-id"}`)
	emptyBody := []byte(`{}`)
	containerCfg := &container.Config{
		Image: alpineImage,
	}

	t.Run("create_container", func(t *testing.T) {
		dc := newDockerClientMock(t, []dockerMockStep{
			{Method: http.MethodPost, Path: "/containers/create", Error: "create failed"},
		})

		inspect, err := dc.RunContainer(t.Context(), containerCfg, &container.HostConfig{}, "test")
		require.Nil(t, inspect)
		require.Error(t, err)
		require.ErrorContains(t, err, "create test container")
	})

	t.Run("start_container", func(t *testing.T) {
		dc := newDockerClientMock(t, []dockerMockStep{
			{Method: http.MethodPost, Path: "/containers/create", Body: createBody},
			{Method: http.MethodPost, Path: "/containers/container-name/start", Error: "start failed"},
			{Method: http.MethodDelete, Path: "/containers/container-name", Body: emptyBody},
		})

		inspect, err := dc.RunContainer(t.Context(), containerCfg, &container.HostConfig{}, "container-name")
		require.Nil(t, inspect)
		require.Error(t, err)
		require.ErrorContains(t, err, "start container-name")
	})

	t.Run("inspect_container", func(t *testing.T) {
		dc := newDockerClientMock(t, []dockerMockStep{
			{Method: http.MethodPost, Path: "/containers/create", Body: createBody},
			{Method: http.MethodPost, Path: "/containers/container-name/start", Body: emptyBody},
			{Method: http.MethodGet, Path: "/containers/container-name/json", Error: "inspect failed"},
			{Method: http.MethodDelete, Path: "/containers/container-name", Body: emptyBody},
		})

		inspect, err := dc.RunContainer(t.Context(), containerCfg, &container.HostConfig{}, "container-name")
		require.Nil(t, inspect)
		require.Error(t, err)
		require.ErrorContains(t, err, "inspect container-name")
	})
}

func TestRemoveContainer_Fail(t *testing.T) {
	dc := newDockerClientMock(t, []dockerMockStep{
		{Method: http.MethodDelete, Path: "/containers/container-id", Error: "remove failed"},
	})

	err := dc.RemoveContainer(context.Background(), "container-id")
	require.Error(t, err)
	require.ErrorContains(t, err, "remove container container-id")
}

func TestExecCommand_Fail(t *testing.T) {
	t.Run("bad_cmd", func(t *testing.T) {
		dc, err := NewDockerClient()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, dc.Close()) })

		require.NoError(t, dc.PullImage(t.Context(), alpineImage))

		containerName := "exec-bad-cmd-" + ulid.Make().String()
		contConf := &container.Config{
			Image: alpineImage,
			Cmd:   []string{"sleep", "30"},
		}
		inspect, err := dc.RunContainer(t.Context(), contConf, &container.HostConfig{}, containerName)
		require.NoError(t, err)
		t.Cleanup(func() { _ = dc.RemoveContainer(context.Background(), inspect.ID) })

		err = dc.ExecCommand(t.Context(), inspect.ID, client.ExecCreateOptions{
			Cmd: []string{"badcommand"},
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "command [badcommand] completed with exit code 127")
	})

	t.Run("create_exec", func(t *testing.T) {
		dc := newDockerClientMock(t, []dockerMockStep{
			{Method: http.MethodPost, Path: "/containers/container-id/exec", Error: "create exec failed"},
		})

		err := dc.ExecCommand(t.Context(), "container-id", client.ExecCreateOptions{
			Cmd: []string{"echo", "hello"},
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to create exec")
	})

	t.Run("start_exec", func(t *testing.T) {
		dc := newDockerClientMock(t, []dockerMockStep{
			{Method: http.MethodPost, Path: "/containers/container-id/exec", Body: []byte(`{"Id":"exec-id"}`)},
			{Method: http.MethodPost, Path: "/exec/exec-id/start", Error: "start exec failed"},
		})

		err := dc.ExecCommand(t.Context(), "container-id", client.ExecCreateOptions{
			Cmd: []string{"echo", "hello"},
		})
		require.Error(t, err)
		require.ErrorContains(t, err, "failed to execute command")
	})
}

func TestGetHostPort(t *testing.T) {
	t.Run("returns_host_port", func(t *testing.T) {
		dc := &DockerClient{}
		networkSettings := &container.NetworkSettings{}
		networkSettings.Ports = network.PortMap{
			testPort: []network.PortBinding{
				{HostIP: netip.MustParseAddr("0.0.0.0"), HostPort: "54321"},
			},
		}
		inspect := &container.InspectResponse{
			NetworkSettings: networkSettings,
		}

		port, err := dc.GetHostPort(inspect, testPort)
		require.NoError(t, err)
		require.Equal(t, "54321", port)
	})

	t.Run("returns_error_when_port_missing", func(t *testing.T) {
		dc := &DockerClient{}
		networkSettings := &container.NetworkSettings{}
		networkSettings.Ports = network.PortMap{}
		inspect := &container.InspectResponse{
			NetworkSettings: networkSettings,
		}

		port, err := dc.GetHostPort(inspect, testPort)
		require.Empty(t, port)
		require.Error(t, err)
		require.ErrorContains(t, err, "port bindings not available for container port 5432/tcp")
	})

	t.Run("returns_error_when_binding_empty", func(t *testing.T) {
		dc := &DockerClient{}
		networkSettings := &container.NetworkSettings{}
		networkSettings.Ports = network.PortMap{
			testPort: []network.PortBinding{},
		}
		inspect := &container.InspectResponse{
			NetworkSettings: networkSettings,
		}

		port, err := dc.GetHostPort(inspect, testPort)
		require.Empty(t, port)
		require.Error(t, err)
		require.ErrorContains(t, err, "port bindings not available for container port 5432/tcp")
	})
}

type dockerMockStep struct {
	Method string
	Path   string
	Body   []byte
	Error  string
}

// newDockerClientMock wires a Docker client to a httptest server for unit tests.
func newDockerClientMock(t *testing.T, steps []dockerMockStep) *DockerClient {
	t.Helper()

	var stepIndex atomic.Int32

	t.Cleanup(func() {
		remaining := len(steps) - int(stepIndex.Load())
		if remaining != 0 {
			t.Errorf("%d mock step(s) were not consumed", remaining)
		}
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ignore HEAD requests since the Docker client may send them
		// to check if the server is alive before sending the actual request
		if r.Method == http.MethodHead {
			return
		}

		currentIndex := int(stepIndex.Add(1)) - 1

		if currentIndex >= len(steps) {
			t.Fatalf("no more steps left for request %s %s", r.Method, r.URL.Path)
		}

		step := steps[currentIndex]

		if r.Method != step.Method {
			t.Fatalf("expected method %s, got %s", step.Method, r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, step.Path) {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}

		if step.Body != nil {
			_, _ = w.Write(step.Body)
			return
		}

		if step.Error != "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"message": step.Error,
			})

			return
		}
	})

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	cli, err := client.New(
		client.WithHost("tcp://"+serverURL.Host),
		client.WithHTTPClient(server.Client()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, cli.Close())
	})

	return &DockerClient{client: cli}
}
