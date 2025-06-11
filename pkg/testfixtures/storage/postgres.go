package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/containerd/errdefs"
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
	replica  *postgresReplicaContainer
}

type postgresReplicaContainer struct {
	addr     string
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
		Cmd: []string{
			"postgres",
			"-c", "wal_level=replica",
			"-c", "max_wal_senders=3",
			"-c", "max_replication_slots=3",
			"-c", "wal_keep_size=64MB",
			"-c", "hot_standby=on",
		},
	}

	hostCfg := container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
		ExtraHosts:      []string{"host.docker.internal:host-gateway"},
	}

	name := "postgres-" + ulid.Make().String()

	cont, err := dockerClient.ContainerCreate(context.Background(), &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create postgres docker container")

	t.Cleanup(func() {
		t.Logf("stopping container %s", name)
		timeoutSec := 5

		err := dockerClient.ContainerStop(context.Background(), cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !errdefs.IsNotFound(err) {
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

// CreateSecondary creates a secondary PostgreSQL container.
func (p *postgresTestContainer) CreateSecondary(t testing.TB) error {
	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		dockerClient.Close()
	})

	// Configure the master for replication.
	masterContainerID, err := p.getMasterContainerID(dockerClient)
	require.NoError(t, err)

	err = p.configureMasterForReplication(t, dockerClient, masterContainerID)
	require.NoError(t, err)

	// Wait for the master to be configured.
	time.Sleep(3 * time.Second)

	// Extract host and port from master for basebackup.
	masterHost := "host.docker.internal"
	masterPort := strings.Split(p.addr, ":")[1]

	// Use standard PostgreSQL approach with docker-entrypoint-initdb.d.
	containerCfg := container.Config{
		Env: []string{
			"POSTGRES_DB=defaultdb",
			"POSTGRES_PASSWORD=secret",
			"PGPASSWORD=secret",
			"POSTGRES_INITDB_ARGS=--auth-host=trust",
			"POSTGRES_MASTER_HOST=" + masterHost,
			"POSTGRES_MASTER_PORT=" + masterPort,
		},
		ExposedPorts: nat.PortSet{
			nat.Port("5432/tcp"): {},
		},
		Image:      postgresImage,
		Entrypoint: []string{"/bin/bash", "-c"},
		Cmd: []string{fmt.Sprintf(`
set -e

export PGPASSWORD=secret

echo "Initializing PostgreSQL replica..."

# Wait for master to be ready
until pg_isready -h %s -p %s -U postgres; do
    echo "Waiting for master..."
    sleep 2
done

echo "Master ready, creating base backup..."

# Remove default PGDATA content
rm -rf $PGDATA/*

# Create base backup
pg_basebackup -h %s -p %s -U postgres -D $PGDATA -Fp -Xs -P -R

# Configure as replica
echo "hot_standby = on" >> $PGDATA/postgresql.conf
touch $PGDATA/standby.signal

echo "Starting PostgreSQL replica..."
exec docker-entrypoint.sh postgres -c hot_standby=on -c wal_level=replica
`, masterHost, masterPort, masterHost, masterPort)},
	}

	hostCfg := container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
		ExtraHosts:      []string{"host.docker.internal:host-gateway"},
	}

	name := "postgres-replica-" + ulid.Make().String()

	cont, err := dockerClient.ContainerCreate(context.Background(), &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create postgres replica docker container")

	t.Cleanup(func() {
		t.Logf("stopping replica container %s", name)
		timeoutSec := 5

		err := dockerClient.ContainerStop(context.Background(), cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !errdefs.IsNotFound(err) {
			t.Logf("failed to stop postgres replica container: %v", err)
		}

		t.Logf("stopped replica container %s", name)
	})

	err = dockerClient.ContainerStart(context.Background(), cont.ID, container.StartOptions{})
	require.NoError(t, err, "failed to start postgres replica container")

	containerJSON, err := dockerClient.ContainerInspect(context.Background(), cont.ID)
	require.NoError(t, err)

	m, ok := containerJSON.NetworkSettings.Ports["5432/tcp"]
	if !ok || len(m) == 0 {
		require.Fail(t, "failed to get host port mapping from postgres replica container")
	}

	p.replica = &postgresReplicaContainer{
		addr:     "localhost:" + m[0].HostPort,
		username: "postgres",
		password: "secret",
	}

	// Wait for replica to be ready and synchronized.
	err = p.waitForReplicaSync(t)
	require.NoError(t, err, "failed to sync replica")

	return nil
}

// getMasterContainerID finds the master container ID.
func (p *postgresTestContainer) getMasterContainerID(dockerClient *client.Client) (string, error) {
	containers, err := dockerClient.ContainerList(context.Background(), container.ListOptions{})
	if err != nil {
		return "", err
	}

	for _, cont := range containers {
		for _, name := range cont.Names {
			if strings.Contains(name, "postgres-") && !strings.Contains(name, "replica") && !strings.Contains(name, "basebackup") {
				return cont.ID, nil
			}
		}
	}

	return "", fmt.Errorf("master container not found")
}

// configureMasterForReplication configures the master to accept replication connections.
func (p *postgresTestContainer) configureMasterForReplication(t testing.TB, dockerClient *client.Client, masterContainerID string) error {
	// Configuration for streaming replication - only pg_hba.conf and reload.
	commands := [][]string{
		{"sh", "-c", "echo 'host replication postgres all trust' >> /var/lib/postgresql/data/pg_hba.conf"},
		{"psql", "-U", "postgres", "-d", "defaultdb", "-c", "SELECT pg_reload_conf()"},
	}

	for _, cmd := range commands {
		execConfig := container.ExecOptions{
			Cmd: cmd,
		}

		exec, err := dockerClient.ContainerExecCreate(context.Background(), masterContainerID, execConfig)
		if err != nil {
			return fmt.Errorf("failed to create exec for command %v: %w", cmd, err)
		}

		err = dockerClient.ContainerExecStart(context.Background(), exec.ID, container.ExecStartOptions{})
		if err != nil {
			return fmt.Errorf("failed to execute command %v: %w", cmd, err)
		}

		// Wait for command to complete.
		inspect, err := dockerClient.ContainerExecInspect(context.Background(), exec.ID)
		if err != nil {
			return fmt.Errorf("failed to inspect exec %v: %w", cmd, err)
		}

		if inspect.ExitCode != 0 {
			t.Logf("Command %v completed with exit code %d", cmd, inspect.ExitCode)
		}
	}

	return nil
}

// waitForReplicaSync waits for the replica to be synchronized with the master.
func (p *postgresTestContainer) waitForReplicaSync(t testing.TB) error {
	uri := fmt.Sprintf("postgres://%s:%s@%s/defaultdb?sslmode=disable", p.replica.username, p.replica.password, p.replica.addr)

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 120 * time.Second // Increase to 2 minutes for initialization.
	backoffPolicy.InitialInterval = 2 * time.Second  // Start with 2 seconds.
	backoffPolicy.MaxInterval = 10 * time.Second     // Cap at 10 seconds.

	return backoff.Retry(
		func() error {
			db, err := goose.OpenDBWithDriver("pgx", uri)
			if err != nil {
				t.Logf("Connection to replica failed (expected during initialization): %v", err)
				return fmt.Errorf("failed to connect to replica: %w", err)
			}
			defer db.Close()

			// Check that replica is in recovery mode (standby)
			var inRecovery bool
			err = db.QueryRow("SELECT pg_is_in_recovery()").Scan(&inRecovery)
			if err != nil {
				t.Logf("Failed to check recovery status (replica may still be initializing): %v", err)
				return fmt.Errorf("failed to check recovery status: %w", err)
			}

			if !inRecovery {
				return fmt.Errorf("replica is not in recovery mode")
			}

			// Check that replica is receiving WAL
			var replicaLSN *string
			err = db.QueryRow("SELECT pg_last_wal_receive_lsn()").Scan(&replicaLSN)
			if err != nil {
				t.Logf("Failed to get replica LSN: %v", err)
				return fmt.Errorf("failed to get replica LSN: %w", err)
			}

			if replicaLSN == nil || *replicaLSN == "" {
				return fmt.Errorf("replica has not received any WAL yet")
			}

			t.Logf("Replica is synchronized and receiving WAL at LSN: %s", *replicaLSN)
			return nil
		},
		backoffPolicy,
	)
}

// GetSecondaryConnectionURI returns the connection URI for the read replica.
func (p *postgresTestContainer) GetSecondaryConnectionURI(includeCredentials bool) string {
	if p.replica == nil {
		return ""
	}

	creds := ""
	if includeCredentials {
		creds = fmt.Sprintf("%s:%s@", p.replica.username, p.replica.password)
	}

	return fmt.Sprintf(
		"postgres://%s%s/%s?sslmode=disable",
		creds,
		p.replica.addr,
		"defaultdb",
	)
}
