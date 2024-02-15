package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/go-sql-driver/mysql"
	"github.com/oklog/ulid/v2"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/assets"
)

const (
	mySQLImage = "mysql:8"
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
	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		dockerClient.Close()
	})

	allImages, err := dockerClient.ImageList(context.Background(), types.ImageListOptions{
		All: true,
	})
	require.NoError(t, err)

	foundMysqlImage := false
	for _, image := range allImages {
		for _, tag := range image.RepoTags {
			if strings.Contains(tag, mySQLImage) {
				foundMysqlImage = true
				break
			}
		}
	}

	if !foundMysqlImage {
		t.Logf("Pulling image %s", mySQLImage)
		reader, err := dockerClient.ImagePull(context.Background(), mySQLImage, types.ImagePullOptions{})
		require.NoError(t, err)

		_, err = io.Copy(io.Discard, reader) // consume the image pull output to make sure it's done
		require.NoError(t, err)
	}

	containerCfg := container.Config{
		Env: []string{
			"MYSQL_DATABASE=defaultdb",
			"MYSQL_ROOT_PASSWORD=secret",
		},
		ExposedPorts: nat.PortSet{
			nat.Port("3306/tcp"): {},
		},
		Image: mySQLImage,
	}

	hostCfg := container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
		Tmpfs:           map[string]string{"/var/lib/mysql": ""},
	}

	name := fmt.Sprintf("mysql-%s", ulid.Make().String())

	cont, err := dockerClient.ContainerCreate(context.Background(), &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create mysql docker container")

	t.Cleanup(func() {
		t.Logf("stopping container %s", name)
		timeoutSec := 5

		err := dockerClient.ContainerStop(context.Background(), cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("failed to stop mysql container: %v", err)
		}
		t.Logf("stopped container %s", name)
	})

	err = dockerClient.ContainerStart(context.Background(), cont.ID, container.StartOptions{})
	if err != nil {
		t.Fatalf("failed to start mysql container: %v", err)
	}

	containerJSON, err := dockerClient.ContainerInspect(context.Background(), cont.ID)
	require.NoError(t, err)

	p, ok := containerJSON.NetworkSettings.Ports["3306/tcp"]
	if !ok || len(p) == 0 {
		t.Fatalf("failed to get host port mapping from mysql container")
	}

	mySQLTestContainer := &mySQLTestContainer{
		addr:     fmt.Sprintf("localhost:%s", p[0].HostPort),
		username: "root",
		password: "secret",
	}

	uri := fmt.Sprintf("%s:%s@tcp(%s)/defaultdb?parseTime=true", mySQLTestContainer.username, mySQLTestContainer.password, mySQLTestContainer.addr)

	err = mysql.SetLogger(log.New(io.Discard, "", 0))
	require.NoError(t, err)

	goose.SetLogger(goose.NopLogger())

	db, err := goose.OpenDBWithDriver("mysql", uri)
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
		t.Fatalf("failed to connect to mysql container: %v", err)
	}

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
		creds = fmt.Sprintf("%s:%s@", m.username, m.password)
	}

	return fmt.Sprintf(
		"%stcp(%s)/%s?parseTime=true",
		creds,
		m.addr,
		"defaultdb",
	)
}

func (m *mySQLTestContainer) GetUsername() string {
	return m.username
}

func (m *mySQLTestContainer) GetPassword() string {
	return m.password
}
