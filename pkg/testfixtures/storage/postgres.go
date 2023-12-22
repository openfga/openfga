package storage

import (
	"context"
	"database/sql"
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
	"github.com/stretchr/testify/require"

	postgres "github.com/openfga/openfga/pkg/storage/postgres/migrations"
)

const (
	postgresImage = "postgres:14"
)

var (
	expireTimeout = 10 * time.Minute // benchmarks take a while to run
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
	ctx := context.Background()

	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	require.NoError(t, err)

	allImages, err := dockerClient.ImageList(ctx, types.ImageListOptions{
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
		reader, err := dockerClient.ImagePull(ctx, postgresImage, types.ImagePullOptions{})
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

	name := fmt.Sprintf("postgres-%s", ulid.Make().String())

	cont, err := dockerClient.ContainerCreate(ctx, &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create postgres docker container")

	stopContainer := func() {
		t.Logf("stopping container %s", name)
		timeoutSec := 5

		err := dockerClient.ContainerStop(ctx, cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("failed to stop postgres container: %v", err)
		}

		dockerClient.Close()
		t.Logf("stopped container %s", name)
	}

	err = dockerClient.ContainerStart(ctx, cont.ID, types.ContainerStartOptions{})
	if err != nil {
		stopContainer()
		t.Fatalf("failed to start postgres container: %v", err)
	}

	containerJSON, err := dockerClient.ContainerInspect(ctx, cont.ID)
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
		err := dockerClient.ContainerStop(ctx, cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("failed to expire postgres container: %v", err)
		}
		t.Logf("expired container %s", name)
	}()

	t.Cleanup(func() {
		stopContainer()
	})

	pgTestContainer := &postgresTestContainer{
		addr:     fmt.Sprintf("localhost:%s", m[0].HostPort),
		username: "postgres",
		password: "secret",
	}

	uri := fmt.Sprintf("postgres://%s:%s@%s/defaultdb?sslmode=disable", pgTestContainer.username, pgTestContainer.password, pgTestContainer.addr)

	var db *sql.DB

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 30 * time.Second
	err = backoff.Retry(
		func() error {
			db, err = sql.Open("pgx", uri)
			if err != nil {
				return err
			}
			return db.Ping()
		},
		backoffPolicy,
	)
	if err != nil {
		stopContainer()
		t.Fatalf("failed to connect to postgres container: %v", err)
	}

	version, err := postgres.Migrations.Run(ctx, db)
	require.NoError(t, err)

	pgTestContainer.version = version

	err = db.Close()
	require.NoError(t, err)

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
