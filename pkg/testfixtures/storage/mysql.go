package storage

import (
	"context"
	"fmt"
	"io"
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
	"github.com/openfga/openfga/assets"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

const (
	mySQLImage = "mysql:8"
)

type mySQLTestContainer struct {
	addr    string
	creds   string
	version int64
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
	}

	name := fmt.Sprintf("mysql-%s", ulid.Make().String())

	cont, err := dockerClient.ContainerCreate(context.Background(), &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create mysql docker container")

	stopContainer := func() {

		t.Logf("stopping container %s", name)
		timeoutSec := 5

		err := dockerClient.ContainerStop(context.Background(), cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("failed to stop mysql container: %v", err)
		}

		dockerClient.Close()
		t.Logf("stopped container %s", name)
	}

	err = dockerClient.ContainerStart(context.Background(), cont.ID, types.ContainerStartOptions{})
	if err != nil {
		stopContainer()
		t.Fatalf("failed to start mysql container: %v", err)
	}

	containerJSON, err := dockerClient.ContainerInspect(context.Background(), cont.ID)
	require.NoError(t, err)

	p, ok := containerJSON.NetworkSettings.Ports["3306/tcp"]
	if !ok || len(p) == 0 {
		t.Fatalf("failed to get host port mapping from mysql container")
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

	mySQLTestContainer := &mySQLTestContainer{
		addr:  fmt.Sprintf("localhost:%s", p[0].HostPort),
		creds: "root:secret",
	}

	uri := fmt.Sprintf("%s@tcp(%s)/defaultdb?parseTime=true", mySQLTestContainer.creds, mySQLTestContainer.addr)

	err = mysql.SetLogger(goose.NopLogger())
	require.NoError(t, err)

	db, err := goose.OpenDBWithDriver("mysql", uri)
	require.NoError(t, err)

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = time.Minute
	err = backoff.Retry(
		func() error {
			return db.Ping()
		},
		backoffPolicy,
	)
	if err != nil {
		stopContainer()
		t.Fatalf("failed to connect to mysql container: %v", err)
	}

	goose.SetBaseFS(assets.EmbedMigrations)

	err = goose.Up(db, assets.MySQLMigrationDir)
	require.NoError(t, err)
	version, err := goose.GetDBVersion(db)
	require.NoError(t, err)
	mySQLTestContainer.version = version

	err = db.Close()
	require.NoError(t, err)

	return mySQLTestContainer
}

// GetConnectionURI returns the mysql connection uri for the running mysql test container.
func (m *mySQLTestContainer) GetConnectionURI() string {
	return fmt.Sprintf(
		"%s@tcp(%s)/%s?parseTime=true",
		m.creds,
		m.addr,
		"defaultdb",
	)
}
