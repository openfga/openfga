package storage

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/id"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

const (
	migrateDir    = "../../../storage/postgres/migrations"
	postgresImage = "postgres:14"
)

var (
	expireTimeout = 60 * time.Second
)

type postgresTester[T any] struct {
	conn     *pgx.Conn
	hostname string
	port     string
	creds    string
}

func NewPostgresTester[T any]() *postgresTester[T] {
	return &postgresTester[T]{}
}

// RunPostgresForTesting returns a RunningEngineForTest for the postgres driver.
func (p *postgresTester[T]) RunPostgresForTesting(t testing.TB, bridgeNetworkName string) RunningEngineForTest[T] {
	dockerClient, err := client.NewClientWithOpts(client.FromEnv)
	require.NoError(t, err)

	reader, err := dockerClient.ImagePull(context.Background(), postgresImage, types.ImagePullOptions{})
	require.NoError(t, err)

	_, err = io.Copy(io.Discard, reader) // consume the image pull output to make sure it's done
	require.NoError(t, err)

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
		PortBindings: nat.PortMap{
			"5432/tcp": []nat.PortBinding{},
		},
	}

	ulid, err := id.NewString()
	require.NoError(t, err)

	name := fmt.Sprintf("postgres-%s", ulid)

	cont, err := dockerClient.ContainerCreate(context.Background(), &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create postgres docker container")

	stopContainer := func() {

		timeout := 5 * time.Second

		err := dockerClient.ContainerStop(context.Background(), cont.ID, &timeout)
		if err != nil && !client.IsErrNotFound(err) {
			t.Fatalf("failed to stop postgres container: %v", err)
		}
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

	port := m[0].HostPort

	// spin up a goroutine to survive any test panics to expire/stop the running container
	go func() {
		time.Sleep(expireTimeout)

		err := dockerClient.ContainerStop(context.Background(), cont.ID, nil)
		if err != nil && !client.IsErrNotFound(err) {
			t.Fatalf("failed to expire postgres container: %v", err)
		}
	}()

	t.Cleanup(func() {
		stopContainer()
	})

	builder := &postgresTester[T]{
		hostname: "localhost",
		creds:    "postgres:secret",
	}

	if bridgeNetworkName != "" {
		builder.hostname = name
		builder.port = "5432"
	} else {
		builder.port = port
	}

	uri := fmt.Sprintf("postgres://%s@localhost:%s/defaultdb?sslmode=disable", builder.creds, port)

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 30 * time.Second

	err = backoff.Retry(
		func() error {
			var err error

			timeoutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			builder.conn, err = pgx.Connect(timeoutCtx, uri)
			if err != nil {
				return err
			}

			return nil
		},
		backoffPolicy,
	)
	if err != nil {
		stopContainer()
		t.Fatalf("failed to connect to postgres container: %v", err)
	}

	return builder
}

func (p *postgresTester[T]) NewDatabase(_ testing.TB) string {

	dbName := "defaultdb"

	return fmt.Sprintf(
		"postgres://%s@%s:%s/%s?sslmode=disable",
		p.creds,
		p.hostname,
		p.port,
		dbName,
	)
}

func (p *postgresTester[T]) NewDatastore(t testing.TB, initFunc InitFunc[T]) T {
	connectStr := p.NewDatabase(t)

	db, err := sql.Open("pgx", connectStr)
	require.NoError(t, err)

	err = goose.SetDialect("postgres")
	require.NoError(t, err)

	goose.SetBaseFS(assets.EmbedMigrations)

	err = goose.Up(db, "migrations/postgres")
	require.NoError(t, err)

	return initFunc("postgres", connectStr)
}
