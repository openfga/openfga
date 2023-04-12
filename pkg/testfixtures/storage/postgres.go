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
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/assets"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

const (
	postgresImage = "postgres:14"
)

var (
	expireTimeout = 10 * time.Minute //benchmarks take a while to run
)

type postgresTestContainer struct {
	addr    string
	creds   string
	version int64
}

// NewPostgresTestContainer returns an implementation of the DatastoreTestContainer interface
// for Postgres.
func NewPostgresTestContainer() *postgresTestContainer {
	return &postgresTestContainer{}
}

func (p *postgresTestContainer) GetDatabaseSchemaVersion() int64 {
	return p.version
}

// RunPostgresTestContainer runs a Postgres container, connects to it, and returns a
// bootstrapped implementation of the DatastoreTestContainer interface wired up for the
// Postgres datastore engine.
func (p *postgresTestContainer) RunPostgresTestContainer(t testing.TB) DatastoreTestContainer {
	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	require.NoError(t, err)

	allImages, err := dockerClient.ImageList(context.Background(), types.ImageListOptions{
		All: true,
	})
	require.NoError(t, err)

	foundPostgresImage := false
	for _, image := range allImages {
		for _, tag := range image.RepoTags {
			if strings.Contains(tag, postgresImage) {
				foundPostgresImage = true
				break
			}
		}
	}

	if !foundPostgresImage {
		t.Logf("Pulling image %s", postgresImage)
		reader, err := dockerClient.ImagePull(context.Background(), postgresImage, types.ImagePullOptions{})
		require.NoError(t, err)

		_, err = io.Copy(io.Discard, reader) // consume the image pull output to make sure it's done
		require.NoError(t, err)
	}

	containerCfg := container.Config{
		Env: []string{
			"POSTGRES_DB=defaultdb",
			"POSTGRES_PASSWORD=secret",
		},
		ExposedPorts: nat.PortSet{
			nat.Port("5432/tcp"): {},
		},
		Image: postgresImage,
	}

	hostCfg := container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
	}

	name := fmt.Sprintf("postgres-%s", ulid.Make().String())

	cont, err := dockerClient.ContainerCreate(context.Background(), &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create postgres docker container")

	stopContainer := func() {

		t.Logf("stopping container %s", name)
		timeoutSec := 5

		err := dockerClient.ContainerStop(context.Background(), cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("failed to stop postgres container: %v", err)
		}

		dockerClient.Close()
		t.Logf("stopped container %s", name)
	}

	err = dockerClient.ContainerStart(context.Background(), cont.ID, types.ContainerStartOptions{})
	if err != nil {
		stopContainer()
		t.Fatalf("failed to start postgres container: %v", err)
	}

	containerJSON, err := dockerClient.ContainerInspect(context.Background(), cont.ID)
	require.NoError(t, err)

	m, ok := containerJSON.NetworkSettings.Ports["5432/tcp"]
	if !ok || len(m) == 0 {
		t.Fatalf("failed to get host port mapping from postgres container")
	}

	// spin up a goroutine to survive any test panics to expire/stop the running container
	go func() {
		time.Sleep(expireTimeout)
		timeoutSec := 0

		t.Logf("expiring container %s", name)
		err := dockerClient.ContainerStop(context.Background(), cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("failed to expire postgres container: %v", err)
		}
		t.Logf("expired container %s", name)
	}()

	t.Cleanup(func() {
		stopContainer()
	})

	pgTestContainer := &postgresTestContainer{
		addr:  fmt.Sprintf("localhost:%s", m[0].HostPort),
		creds: "postgres:secret",
	}

	uri := fmt.Sprintf("postgres://%s@%s/defaultdb?sslmode=disable", pgTestContainer.creds, pgTestContainer.addr)

	db, err := goose.OpenDBWithDriver("pgx", uri)
	require.NoError(t, err)

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 30 * time.Second
	err = backoff.Retry(
		func() error {
			return db.Ping()
		},
		backoffPolicy,
	)
	if err != nil {
		stopContainer()
		t.Fatalf("failed to connect to postgres container: %v", err)
	}

	goose.SetBaseFS(assets.EmbedMigrations)

	err = goose.Up(db, assets.PostgresMigrationDir)
	require.NoError(t, err)

	version, err := goose.GetDBVersion(db)
	require.NoError(t, err)
	pgTestContainer.version = version

	err = db.Close()
	require.NoError(t, err)

	return pgTestContainer
}

// GetConnectionURI returns the postgres connection uri for the running postgres test container.
func (p *postgresTestContainer) GetConnectionURI() string {
	return fmt.Sprintf(
		"postgres://%s@%s/%s?sslmode=disable",
		p.creds,
		p.addr,
		"defaultdb",
	)
}
