package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	_ "github.com/microsoft/go-mssqldb" // MSSQL Driver.
	"github.com/oklog/ulid/v2"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/assets"
)

const (
	msSQLImage = "mcr.microsoft.com/mssql/server:2022-latest"
)

type msSQLTestContainer struct {
	addr     string
	version  int64
	username string
	password string
}

// NewMSSQLTestContainer returns an implementation of the DatastoreTestContainer interface
// for MSSQL.
func NewMSSQLTestContainer() *msSQLTestContainer {
	return &msSQLTestContainer{}
}

func (m *msSQLTestContainer) GetDatabaseSchemaVersion() int64 {
	return m.version
}

// RunMSSQLTestContainer runs a MSSQL container, connects to it, and returns a
// bootstrapped implementation of the DatastoreTestContainer interface wired up for the
// MSSQL datastore engine.
func (m *msSQLTestContainer) RunMSSQLTestContainer(t testing.TB) DatastoreTestContainer {
	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		dockerClient.Close()
	})

	allImages, err := dockerClient.ImageList(context.Background(), image.ListOptions{
		All: true,
	})
	require.NoError(t, err)

	foundMssqlImage := false
	for _, image := range allImages {
		for _, tag := range image.RepoTags {
			if strings.Contains(tag, msSQLImage) {
				foundMssqlImage = true
				break
			}
		}
	}

	if !foundMssqlImage {
		t.Logf("Pulling image %s", msSQLImage)
		reader, err := dockerClient.ImagePull(context.Background(), msSQLImage, image.PullOptions{})
		require.NoError(t, err)

		_, err = io.Copy(io.Discard, reader) // consume the image pull output to make sure it's done
		require.NoError(t, err)
	}

	containerCfg := container.Config{
		Env: []string{
			"ACCEPT_EULA=Y",
			"MSSQL_SA_PASSWORD=pKC8mMA_qu5SLeaG",
		},
		ExposedPorts: nat.PortSet{
			nat.Port("1433/tcp"): {},
		},
		Image: msSQLImage,
	}

	hostCfg := container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
		Tmpfs:           map[string]string{"/var/lib/mssql": ""},
	}

	name := fmt.Sprintf("mssql-%s", ulid.Make().String())

	cont, err := dockerClient.ContainerCreate(context.Background(), &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create mssql docker container")

	t.Cleanup(func() {
		t.Logf("stopping container %s", name)
		timeoutSec := 5

		err := dockerClient.ContainerStop(context.Background(), cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("failed to stop mssql container: %v", err)
		}
		t.Logf("stopped container %s", name)
	})

	err = dockerClient.ContainerStart(context.Background(), cont.ID, container.StartOptions{})
	require.NoError(t, err, "failed to start mssql container")

	containerJSON, err := dockerClient.ContainerInspect(context.Background(), cont.ID)
	require.NoError(t, err)

	p, ok := containerJSON.NetworkSettings.Ports["1433/tcp"]
	if !ok || len(p) == 0 {
		require.Fail(t, "failed to get host port mapping from mssql container")
	}

	msSQLTestContainer := &msSQLTestContainer{
		addr:     fmt.Sprintf("localhost:%s", p[0].HostPort),
		username: "sa",
		password: "pKC8mMA_qu5SLeaG",
	}
	uri := fmt.Sprintf("sqlserver://%s:%s@%s", msSQLTestContainer.username, msSQLTestContainer.password, msSQLTestContainer.addr)

	goose.SetLogger(goose.NopLogger())

	db, err := goose.OpenDBWithDriver("sqlserver", uri)
	require.NoError(t, err)
	defer db.Close()

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 2 * time.Minute
	err = backoff.Retry(
		func() error {
			return db.Ping()
		},
		backoffPolicy,
	)
	if err != nil {
		t.Fatalf("failed to connect to mssql container: %v", err)
	}

	goose.SetBaseFS(assets.EmbedMigrations)

	err = goose.Up(db, assets.MSSQLMigrationDir)
	require.NoError(t, err)
	version, err := goose.GetDBVersion(db)
	require.NoError(t, err)
	msSQLTestContainer.version = version

	return msSQLTestContainer
}

// GetConnectionURI returns the mssql connection uri for the running mssql test container.
func (m *msSQLTestContainer) GetConnectionURI(includeCredentials bool) string {
	creds := ""
	if includeCredentials {
		creds = fmt.Sprintf("%s:%s@", m.username, m.password)
	}

	return fmt.Sprintf(
		"sqlserver://%s%s",
		creds,
		m.addr,
	)
}

func (m *msSQLTestContainer) GetUsername() string {
	return m.username
}

func (m *msSQLTestContainer) GetPassword() string {
	return m.password
}
