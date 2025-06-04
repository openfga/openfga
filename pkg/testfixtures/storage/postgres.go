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
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver.
	"github.com/oklog/ulid/v2"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/assets"
)

const (
	postgresImage = "postgres:17"
)

type postgresTestContainer struct {
	addr     string
	version  int64
	username string
	password string
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
	t.Cleanup(func() {
		dockerClient.Close()
	})

	allImages, err := dockerClient.ImageList(context.Background(), image.ListOptions{
		All: true,
	})
	require.NoError(t, err)

	foundPostgresImage := false

AllImages:
	for _, image := range allImages {
		for _, tag := range image.RepoTags {
			if strings.Contains(tag, postgresImage) {
				foundPostgresImage = true
				break AllImages
			}
		}
	}

	if !foundPostgresImage {
		t.Logf("Pulling image %s", postgresImage)
		reader, err := dockerClient.ImagePull(context.Background(), postgresImage, image.PullOptions{})
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
		Tmpfs:           map[string]string{"/var/lib/postgresql/data": ""},
	}

	name := "postgres-" + ulid.Make().String()

	cont, err := dockerClient.ContainerCreate(context.Background(), &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create postgres docker container")

	t.Cleanup(func() {
		t.Logf("stopping container %s", name)
		timeoutSec := 5

		err := dockerClient.ContainerStop(context.Background(), cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("failed to stop postgres container: %v", err)
		}

		t.Logf("stopped container %s", name)
	})

	err = dockerClient.ContainerStart(context.Background(), cont.ID, container.StartOptions{})
	require.NoError(t, err, "failed to start postgres container")

	containerJSON, err := dockerClient.ContainerInspect(context.Background(), cont.ID)
	require.NoError(t, err)

	m, ok := containerJSON.NetworkSettings.Ports["5432/tcp"]
	if !ok || len(m) == 0 {
		require.Fail(t, "failed to get host port mapping from postgres container")
	}

	pgTestContainer := &postgresTestContainer{
		addr:     "localhost:" + m[0].HostPort,
		username: "postgres",
		password: "secret",
	}

	uri := fmt.Sprintf("postgres://%s:%s@%s/defaultdb?sslmode=disable", pgTestContainer.username, pgTestContainer.password, pgTestContainer.addr)

	goose.SetLogger(goose.NopLogger())

	db, err := goose.OpenDBWithDriver("pgx", uri)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 30 * time.Second
	err = backoff.Retry(
		func() error {
			return db.Ping()
		},
		backoffPolicy,
	)
	require.NoError(t, err, "failed to connect to postgres container")

	goose.SetBaseFS(assets.EmbedMigrations)

	err = goose.Up(db, assets.PostgresMigrationDir)
	require.NoError(t, err)

	version, err := goose.GetDBVersion(db)
	require.NoError(t, err)
	pgTestContainer.version = version

	return pgTestContainer
}

// GetConnectionURI returns the postgres connection uri for the running postgres test container.
func (p *postgresTestContainer) GetConnectionURI(includeCredentials bool) string {
	creds := ""
	if includeCredentials {
		creds = fmt.Sprintf("%s:%s@", p.username, p.password)
	}

	return fmt.Sprintf(
		"postgres://%s%s/%s?sslmode=disable",
		creds,
		p.addr,
		"defaultdb",
	)
}

func (p *postgresTestContainer) GetUsername() string {
	return p.username
}

func (p *postgresTestContainer) GetPassword() string {
	return p.password
}
