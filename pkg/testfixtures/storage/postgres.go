package storage

import (
	"context"
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
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
)

var createTableStmts = []string{
	`CREATE TABLE IF NOT EXISTS tuple (
		store TEXT NOT NULL,
		object_type TEXT NOT NULL,
		object_id TEXT NOT NULL,
		relation TEXT NOT NULL,
		_user TEXT NOT NULL,
		user_type TEXT NOT NULL,
		ulid TEXT NOT NULL,
		inserted_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (store, object_type, object_id, relation, _user)
	)`,
	`CREATE INDEX IF NOT EXISTS partial_user_idx ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'user'`,
	`CREATE INDEX IF NOT EXISTS partial_userset_idx ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'userset'`,
	`CREATE UNIQUE INDEX IF NOT EXISTS ulid_idx ON tuple (ulid)`,
	`CREATE TABLE IF NOT EXISTS authorization_model (
		store TEXT NOT NULL,
		authorization_model_id TEXT NOT NULL,
		type TEXT NOT NULL,
		type_definition BYTEA,
		PRIMARY KEY (store, authorization_model_id, type)
	)`,
	`CREATE TABLE IF NOT EXISTS store (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ,
		deleted_at TIMESTAMPTZ
	)`,
	`CREATE TABLE IF NOT EXISTS assertion (
		store TEXT NOT NULL,
		authorization_model_id TEXT NOT NULL,
		assertions BYTEA,
		PRIMARY KEY (store, authorization_model_id)
	)`,
	`CREATE TABLE IF NOT EXISTS changelog (
		store TEXT NOT NULL,
		object_type TEXT NOT NULL,
		object_id TEXT NOT NULL,
		relation TEXT NOT NULL,
		_user TEXT NOT NULL,
		operation INTEGER NOT NULL,
		ulid TEXT NOT NULL,
		inserted_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (store, ulid, object_type)
	)`,
}

const (
	postgresImage = "postgres:14"
)

var (
	expireTimeout = 60 * time.Second
)

type postgresTester struct {
	conn     *pgx.Conn
	hostname string
	port     string
	creds    string
}

// RunPostgresForTesting returns a RunningEngineForTest for the postgres driver.
func RunPostgresForTesting(t testing.TB, bridgeNetworkName string) RunningEngineForTest {

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

	builder := &postgresTester{
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

func (b *postgresTester) NewDatabase(t testing.TB) string {

	dbName := "defaultdb"

	return fmt.Sprintf(
		"postgres://%s@%s:%s/%s?sslmode=disable",
		b.creds,
		b.hostname,
		b.port,
		dbName,
	)
}

func (b *postgresTester) NewDatastore(t testing.TB, initFunc InitFunc) storage.OpenFGADatastore {
	connectStr := b.NewDatabase(t)

	for _, stmt := range createTableStmts {
		_, err := b.conn.Exec(context.Background(), stmt)
		require.NoError(t, err)
	}

	return initFunc("postgres", connectStr)
}
