package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	_ "github.com/denisenkom/go-mssqldb" // mssql driver.
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/oklog/ulid/v2"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/assets"
)

const (
	mssqlImage = "mcr.microsoft.com/mssql/server:2022-latest"
)

type mssqlTestContainer struct {
	addr     string
	version  int64
	username string
	password string
}

// NewMSSQLServerTestContainer returns an implementation of the DatastoreTestContainer interface
// for MSSQLServer.
func NewMSSQLServerTestContainer() *mssqlTestContainer {
	return &mssqlTestContainer{}
}

func (p *mssqlTestContainer) GetDatabaseSchemaVersion() int64 {
	return p.version
}

// RunMSSQLServerTestContainer runs a MSSQLServer container, connects to it, and returns a
// bootstrapped implementation of the DatastoreTestContainer interface wired up for the
// MSSQLServer datastore engine.
func (p *mssqlTestContainer) RunMSSQLServerTestContainer(t testing.TB) DatastoreTestContainer {
	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		dockerClient.Close()
	})

	allImages, err := dockerClient.ImageList(context.Background(), image.ListOptions{All: true})
	require.NoError(t, err)

	foundMSSQLServerImage := false
	for _, image := range allImages {
		for _, tag := range image.RepoTags {
			if strings.Contains(tag, mssqlImage) {
				foundMSSQLServerImage = true
				break
			}
		}
	}

	if !foundMSSQLServerImage {
		t.Logf("Pulling image %s", mssqlImage)
		reader, err := dockerClient.ImagePull(context.Background(), mssqlImage, image.PullOptions{})
		require.NoError(t, err)		
		_, err = io.Copy(io.Discard, reader)
		require.NoError(t, err)
		defer reader.Close()
	}

	containerCfg := container.Config{
		Env: []string{
			"ACCEPT_EULA=Y",
			"MSSQL_SA_PASSWORD=Password123Str0ng!",
		},
		ExposedPorts: nat.PortSet{
			nat.Port("1433/tcp"): {},
		},
		Image: mssqlImage,
		Healthcheck: &container.HealthConfig{
			Test: []string{
				"CMD-SHELL",
				"/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Password123Str0ng!' -Q 'SELECT 1' -C",
			},
			Interval:    10 * time.Second,
			Timeout:     3 * time.Second,
			Retries:     10,
			StartPeriod: 40 * time.Second,
		},
	}

	hostCfg := container.HostConfig{
		AutoRemove: true,
		PublishAllPorts: true,
		PortBindings: nat.PortMap{
			"1433/tcp": []nat.PortBinding{
				{
					HostIP:   "127.0.0.1",
					HostPort: "0",
				},
			},
		},
		Tmpfs: map[string]string{"/var/lib/mssql":""},
	}

	name := "mssql-" + ulid.Make().String()

	cont, err := dockerClient.ContainerCreate(context.Background(), &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create mysql docker container")

	t.Cleanup(func() {
		t.Logf("Stopping container %s", name)
		timeoutSec := 5

		err := dockerClient.ContainerStop(context.Background(), cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("Failed to stop container: %v", err)
		}
		t.Logf("Stopped container %s", name)
	})

	err = dockerClient.ContainerStart(context.Background(), cont.ID, container.StartOptions{})
	require.NoError(t, err, "failed to start mssql container")

	containerJSON, err := dockerClient.ContainerInspect(context.Background(), cont.ID)
	require.NoError(t, err)

	m, ok := containerJSON.NetworkSettings.Ports["1433/tcp"]
	if !ok || len(m) == 0 {
		require.Fail(t, "failed to get host port mapping from mssql container")
	}
	hostPort := m[0].HostPort
	t.Logf("Container mapped port: %s", hostPort)

	mssqlTestContainer := &mssqlTestContainer{
		addr:     fmt.Sprintf("localhost:%s", hostPort),
		username: "sa",
		password: "Password123Str0ng!",
	}
	uri := fmt.Sprintf("sqlserver://%s:%s@%s?database=master",
		mssqlTestContainer.username, mssqlTestContainer.password, mssqlTestContainer.addr)

	goose.SetLogger(goose.NopLogger())

	db, err := goose.OpenDBWithDriver("sqlserver", uri)
	require.NoError(t, err)
	defer db.Close()

	// Wait for the server to become responsive
	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 45 * time.Second
	err = backoff.Retry(func() error {
		return db.Ping()
	}, backoffPolicy)
	require.NoError(t, err, "failed to connect to mssql container")

	execResp, err := dockerClient.ContainerExecCreate(context.Background(), cont.ID, container.ExecOptions{
		Cmd: []string{
			"/opt/mssql-tools18/bin/sqlcmd",
			"-S", "localhost",
			"-U", "sa",
			"-P", "Password123Str0ng!",
			"-Q", "IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'openfga') CREATE DATABASE openfga;",
			"-C",
		},
		AttachStdout: true,
		AttachStderr: true,
	})
	require.NoError(t, err)

	attachResp, err := dockerClient.ContainerExecAttach(context.Background(), execResp.ID, container.ExecAttachOptions{})
	require.NoError(t, err)
	defer attachResp.Close()
	output, err := io.ReadAll(attachResp.Reader)
	require.NoError(t, err)
	t.Logf("📦 SQLCMD output:\n%s", string(output))

	uri = fmt.Sprintf("sqlserver://%s:%s@%s?database=openfga",
	mssqlTestContainer.username, mssqlTestContainer.password, mssqlTestContainer.addr)
	goose.SetLogger(goose.NopLogger())
	db, err = goose.OpenDBWithDriver("sqlserver", uri)
	require.NoError(t, err)
	defer db.Close()

	inspectResp, err := dockerClient.ContainerExecInspect(context.Background(), execResp.ID)
	require.NoError(t, err)
	require.Equal(t, 0, inspectResp.ExitCode, "sqlcmd inside container failed")

	goose.SetBaseFS(assets.EmbedMigrations)

	err = goose.Up(db, assets.MSSQLMigrationDir)
	require.NoError(t, err)

	version, err := goose.GetDBVersion(db)
	require.NoError(t, err)
	mssqlTestContainer.version = version

	return mssqlTestContainer
}

// GetConnectionURI returns the mssql connection uri for the running mssql test container.
func (p *mssqlTestContainer) GetConnectionURI(includeCredentials bool) string {
	creds := ""
	if includeCredentials {
		creds = fmt.Sprintf("%s:%s@", p.username, p.password)
	}
	return fmt.Sprintf(
		"sqlserver://%s%s?database=openfga&TrustServerCertificate=true",
		creds,
		p.addr,
	)
}

func (p *mssqlTestContainer) GetUsername() string {
	return p.username
}

func (p *mssqlTestContainer) GetPassword() string {
	return p.password
}
