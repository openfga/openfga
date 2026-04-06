package storage

import (
	"context"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/containerd/errdefs"
	"github.com/go-sql-driver/mysql"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/oklog/ulid/v2"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/assets"
)

const (
	mySQLImage = "mysql:8"
)

var (
	mySQLPort = network.MustParsePort("3306/tcp")
)

type mySQLTestContainer struct {
	addr     string
	version  int64
	username string
	password string
}

// NewMySQLTestContainer returns an implementation of the DatastoreTestContainer interface
// for MySQL.
func NewMySQLTestContainer() *mySQLTestContainer {
	return &mySQLTestContainer{}
}

func (m *mySQLTestContainer) GetDatabaseSchemaVersion() int64 {
	return m.version
}

// RunMySQLTestContainer runs a MySQL container, connects to it, and returns a
// bootstrapped implementation of the DatastoreTestContainer interface wired up for the
// MySQL datastore engine.
func (m *mySQLTestContainer) RunMySQLTestContainer(t testing.TB) DatastoreTestContainer {
	dockerClient, err := client.New(client.FromEnv)
	require.NoError(t, err)
	t.Cleanup(func() {
		dockerClient.Close()
	})

	imageListResult, err := dockerClient.ImageList(context.Background(), client.ImageListOptions{
		All: true,
	})
	require.NoError(t, err)

	foundMysqlImage := false

AllImages:
	for _, image := range imageListResult.Items {
		for _, tag := range image.RepoTags {
			if strings.Contains(tag, mySQLImage) {
				foundMysqlImage = true
				break AllImages
			}
		}
	}

	if !foundMysqlImage {
		t.Logf("Pulling image %s", mySQLImage)
		reader, err := dockerClient.ImagePull(context.Background(), mySQLImage, client.ImagePullOptions{})
		require.NoError(t, err)

		_, err = io.Copy(io.Discard, reader) // consume the image pull output to make sure it's done
		require.NoError(t, err)
	}

	containerCfg := container.Config{
		Env: []string{
			"MYSQL_DATABASE=defaultdb",
			"MYSQL_ROOT_PASSWORD=secret",
		},
		ExposedPorts: network.PortSet{
			mySQLPort: {},
		},
		Image: mySQLImage,
	}

	hostCfg := container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
		Tmpfs:           map[string]string{"/var/lib/mysql": ""},
	}

	name := "mysql-" + ulid.Make().String()

	cont, err := dockerClient.ContainerCreate(context.Background(), client.ContainerCreateOptions{
		Name:       name,
		Config:     &containerCfg,
		HostConfig: &hostCfg,
	})
	require.NoError(t, err, "failed to create mysql docker container")

	t.Cleanup(func() {
		t.Logf("stopping container %s", name)
		timeoutSec := 5

		_, err := dockerClient.ContainerStop(context.Background(), cont.ID, client.ContainerStopOptions{Timeout: &timeoutSec})
		if err != nil && !errdefs.IsNotFound(err) {
			t.Logf("failed to stop mysql container: %v", err)
		}
		t.Logf("stopped container %s", name)
	})

	_, err = dockerClient.ContainerStart(context.Background(), cont.ID, client.ContainerStartOptions{})
	require.NoError(t, err, "failed to start mysql container")

	inspectResult, err := dockerClient.ContainerInspect(context.Background(), cont.ID, client.ContainerInspectOptions{})
	require.NoError(t, err)

	p, ok := inspectResult.Container.NetworkSettings.Ports[mySQLPort]
	if !ok || len(p) == 0 {
		require.Fail(t, "failed to get host port mapping from mysql container")
	}

	mySQLTestContainer := &mySQLTestContainer{
		addr:     "localhost:" + p[0].HostPort,
		username: "root",
		password: "secret",
	}

	uri := mySQLTestContainer.username + ":" + mySQLTestContainer.password + "@tcp(" + mySQLTestContainer.addr + ")/defaultdb?parseTime=true"

	err = mysql.SetLogger(log.New(io.Discard, "", 0))
	require.NoError(t, err)

	goose.SetLogger(goose.NopLogger())

	db, err := goose.OpenDBWithDriver("mysql", uri)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 2 * time.Minute
	err = backoff.Retry(
		func() error {
			return db.Ping()
		},
		backoffPolicy,
	)
	require.NoError(t, err, "failed to connect to mysql container")

	goose.SetBaseFS(assets.EmbedMigrations)

	err = goose.Up(db, assets.MySQLMigrationDir)
	require.NoError(t, err)
	version, err := goose.GetDBVersion(db)
	require.NoError(t, err)
	mySQLTestContainer.version = version

	return mySQLTestContainer
}

// GetConnectionURI returns the mysql connection uri for the running mysql test container.
func (m *mySQLTestContainer) GetConnectionURI(includeCredentials bool) string {
	creds := ""
	if includeCredentials {
		creds = m.username + ":" + m.password + "@"
	}

	return creds + "tcp(" + m.addr + ")/defaultdb?parseTime=true"
}

func (m *mySQLTestContainer) GetUsername() string {
	return m.username
}

func (m *mySQLTestContainer) GetPassword() string {
	return m.password
}

func (m *mySQLTestContainer) CreateSecondary(t testing.TB) error {
	return nil
}

func (m *mySQLTestContainer) GetSecondaryConnectionURI(includeCredentials bool) string {
	return ""
}
