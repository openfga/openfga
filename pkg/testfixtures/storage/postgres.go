package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/containerd/errdefs"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/oklog/ulid/v2"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/testutils"
)

const (
	postgresImage     = "postgres:17-alpine"
	postgresContainer = "openfga-test-postgres"

	postgresDBPrefix   = "openfga-test-db-"
	postgresTemplateDB = postgresDBPrefix + "template"
	postgresUsername   = "postgres"
	postgresPassword   = "secret"
)

var (
	_ DatastoreTestContainer = (*postgresTestContainer)(nil)

	containerName = postgresContainer + ulid.MustNewDefault(time.Now()).String()

	postgresPort = network.MustParsePort("5432/tcp")

	mu            sync.Mutex
	cond          sync.Cond
	bootstrapping atomic.Bool
)

func init() {
	cond.L = &mu
}

type (
	postgresTestContainer struct {
		host string
		port string

		database string
		username string
		password string

		version int64
		replica *postgresReplicaContainer
	}

	postgresReplicaContainer struct {
		host string
		port string

		username string
		password string
	}
)

// GetConnectionURI returns the postgres connection uri for the running postgres test container.
func (p *postgresTestContainer) GetConnectionURI(includeCredentials bool) string {
	var username, password string
	if includeCredentials {
		username = p.username
		password = p.password
	}

	return postgresConnectionURI(p.host, p.port, p.database, username, password)
}

func (p *postgresTestContainer) GetDatabaseSchemaVersion() int64 {
	return p.version
}

func (p *postgresTestContainer) GetUsername() string {
	return p.username
}

func (p *postgresTestContainer) GetPassword() string {
	return p.password
}

// CreateSecondary creates a secondary PostgreSQL container.
func (p *postgresTestContainer) CreateSecondary(t testing.TB) error {
	p.replica = runPostgresReplica(t, p)
	return nil
}

// GetSecondaryConnectionURI returns the connection URI for the read replica.
func (p *postgresTestContainer) GetSecondaryConnectionURI(includeCredentials bool) string {
	if p.replica == nil {
		return ""
	}

	var username, password string
	if includeCredentials {
		username = p.replica.username
		password = p.replica.password
	}

	return postgresConnectionURI(p.replica.host, p.replica.port, p.database, username, password)
}

// CleanupPostgresContainer removes the shared postgres test container.
// It should be called from TestMain after all tests in a package have finished.
func CleanupPostgresContainer() {
	docker, err := testutils.NewDockerClient()
	if err != nil {
		return
	}
	defer docker.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := docker.RemoveContainer(ctx, containerName); err != nil {
		// Container may not exist or already be removed; ignore errors.
		return
	}
}

func RunPostgresTestContainer(t testing.TB) DatastoreTestContainer {
	docker, err := testutils.NewDockerClient()
	require.NoError(t, err)

	t.Cleanup(func() {
		docker.Close()
	})

	var dockerCont *container.InspectResponse

	mu.Lock()
	for {
		var found bool
		dockerCont, found, err = docker.FindRunningContainer(t.Context(), containerName, postgresImage)
		if err != nil {
			mu.Unlock()
			require.NoError(t, err, "find running postgres container")
		}

		if !found {
			if !bootstrapping.Swap(true) {
				var err error
				dockerCont, err = bootstrapPostgresContainer(t, docker)
				if err != nil {
					bootstrapping.Store(false)
					cond.Broadcast()
					mu.Unlock()
					require.NoError(t, err, "bootstrapping postgres container")
				}
				bootstrapping.Store(false)
				cond.Broadcast()
				break
			}
			cond.Wait()
			continue
		}
		break
	}
	mu.Unlock()

	port, err := docker.GetHostPort(dockerCont, postgresPort)
	require.NoError(t, err)

	version, err := latestMigrationVersion(assets.PostgresMigrationDir)
	require.NoError(t, err, "get expected postgres migration version")

	testCont := &postgresTestContainer{
		host:     "localhost",
		port:     port,
		database: postgresDBPrefix + ulid.Make().String(),
		username: postgresUsername,
		password: postgresPassword,
		version:  version,
	}

	tplURI := postgresConnectionURI(testCont.host, testCont.port, postgresTemplateDB, testCont.username, testCont.password)
	require.NoError(t, waitForMigrationVersion("pgx", tplURI, testCont.version))

	createExec := client.ExecCreateOptions{
		Cmd: []string{"createdb", "-U", testCont.username, "-T", postgresTemplateDB, testCont.database},
		Env: []string{"PGPASSWORD=" + testCont.password},
	}
	require.NoError(t, docker.ExecCommand(t.Context(), dockerCont.ID, createExec))

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		dropQuery := fmt.Sprintf("DROP DATABASE IF EXISTS \"%s\" WITH (FORCE);", testCont.database)
		dropExec := client.ExecCreateOptions{
			Cmd: []string{"psql", "-U", testCont.username, "-c", dropQuery},
			Env: []string{"PGPASSWORD=" + testCont.password},
		}
		if err := docker.ExecCommand(ctx, dockerCont.ID, dropExec); err != nil {
			t.Errorf("drop test database in the postgres container: %v", err)
		}
	})

	require.NoError(t, waitForMigrationVersion("pgx", testCont.GetConnectionURI(true), testCont.version))

	return testCont
}

func bootstrapPostgresContainer(t testing.TB, docker *testutils.DockerClient) (*container.InspectResponse, error) {
	t.Logf("No running container found for %s, creating a new one", postgresImage)

	err := docker.PullImage(t.Context(), postgresImage)
	if err != nil {
		return nil, err
	}

	// Run container
	contCfg := &container.Config{
		Env: []string{
			"POSTGRES_DB=" + postgresTemplateDB,
			"POSTGRES_PASSWORD=" + postgresPassword,
		},
		ExposedPorts: network.PortSet{
			postgresPort: {},
		},
		Image: postgresImage,
		Cmd: []string{
			"postgres",
			"-c", "wal_level=replica",
			"-c", "max_wal_senders=3",
			"-c", "max_replication_slots=3",
			"-c", "max_connections=200",
			"-c", "wal_keep_size=64MB",
			"-c", "hot_standby=on",
		},
	}

	hostCfg := &container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
		ExtraHosts:      []string{"host.docker.internal:host-gateway"},
	}

	cont, err := docker.RunContainer(t.Context(), contCfg, hostCfg, containerName)
	if err != nil {
		return nil, err
	}

	port, err := docker.GetHostPort(cont, postgresPort)
	if err != nil {
		return nil, err
	}

	dbURI := postgresConnectionURI("localhost", port, postgresTemplateDB, postgresUsername, postgresPassword)
	if err := waitForDatabase("pgx", dbURI); err != nil {
		return nil, err
	}

	allowReplicationExec := client.ExecCreateOptions{
		Cmd: []string{"sh", "-c", "echo 'host replication postgres all trust' >> /var/lib/postgresql/data/pg_hba.conf"},
	}
	err = docker.ExecCommand(t.Context(), cont.ID, allowReplicationExec)
	if err != nil {
		return nil, err
	}

	reloadExec := client.ExecCreateOptions{
		Cmd: []string{"psql", "-U", "postgres", "-c", "SELECT pg_reload_conf();"},
	}
	err = docker.ExecCommand(t.Context(), cont.ID, reloadExec)
	if err != nil {
		return nil, err
	}

	if err := waitForDatabase("pgx", dbURI); err != nil {
		return nil, err
	}

	db, err := goose.OpenDBWithDriver("pgx", dbURI)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if err := goose.Up(db, assets.PostgresMigrationDir); err != nil {
		return nil, err
	}

	return cont, nil
}

func runPostgresReplica(t testing.TB, primary *postgresTestContainer) *postgresReplicaContainer {
	docker, err := testutils.NewDockerClient()
	require.NoError(t, err)

	t.Cleanup(func() {
		docker.Close()
	})

	primaryHost := "host.docker.internal"

	// Use standard PostgreSQL approach with docker-entrypoint-initdb.d.
	contCfg := &container.Config{
		Env: []string{
			"POSTGRES_DB=" + primary.database,
			"POSTGRES_PASSWORD=" + postgresPassword,
			"PGPASSWORD=" + postgresPassword,
			"POSTGRES_INITDB_ARGS=--auth-host=trust",
			"POSTGRES_MASTER_HOST=" + primaryHost,
			"POSTGRES_MASTER_PORT=" + primary.port,
		},
		ExposedPorts: network.PortSet{
			postgresPort: {},
		},
		Image:      postgresImage,
		Entrypoint: []string{"/bin/bash", "-c"},
		Cmd: []string{fmt.Sprintf(`
set -e

export PGPASSWORD=%s

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
pg_basebackup -h %s -p %s -U postgres -D $PGDATA -Fp -Xs -P -R -c fast

# Configure as replica
echo "hot_standby = on" >> $PGDATA/postgresql.conf
touch $PGDATA/standby.signal

echo "Starting PostgreSQL replica..."
exec docker-entrypoint.sh postgres -c hot_standby=on -c max_connections=200
`, postgresPassword, primaryHost, primary.port, primaryHost, primary.port)},
	}

	hostCfg := &container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
		ExtraHosts:      []string{"host.docker.internal:host-gateway"},
	}

	contName := containerName + "-replica"
	cont, err := docker.RunContainer(t.Context(), contCfg, hostCfg, contName)
	require.NoError(t, err, "run postgres container")

	t.Cleanup(func() {
		t.Logf("stopping replica container %s", contName)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := docker.RemoveContainer(ctx, cont.ID); err != nil && !errdefs.IsNotFound(err) {
			t.Errorf("failed to stop postgres replica container: %v", err)
		}

		t.Logf("stopped replica container %s", contName)
	})

	port, err := docker.GetHostPort(cont, postgresPort)
	require.NoError(t, err)

	replicaCont := &postgresReplicaContainer{
		host:     "localhost",
		port:     port,
		username: postgresUsername,
		password: postgresPassword,
	}

	// Wait for replica to be ready and synchronized.
	dbURI := postgresConnectionURI(replicaCont.host, replicaCont.port, primary.database, replicaCont.username, replicaCont.password)
	err = waitForPostgresReplicaSync(t, dbURI)
	require.NoError(t, err, "failed to sync replica")

	return replicaCont
}

// waitForReplicaSync waits for the replica to be synchronized with the master.
func waitForPostgresReplicaSync(t testing.TB, uri string) error {
	backoffPolicy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(2*time.Second),
		backoff.WithMaxInterval(10*time.Second),
		backoff.WithMaxElapsedTime(120*time.Second),
	)

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
			if err = db.QueryRow("SELECT pg_is_in_recovery()").Scan(&inRecovery); err != nil {
				t.Logf("Failed to check recovery status (replica may still be initializing): %v", err)
				return fmt.Errorf("failed to check recovery status: %w", err)
			}

			if !inRecovery {
				return fmt.Errorf("replica is not in recovery mode")
			}

			// Check that replica is receiving WAL
			var replicaLSN sql.NullString
			if err = db.QueryRow("SELECT pg_last_wal_receive_lsn()").Scan(&replicaLSN); err != nil {
				t.Logf("Failed to get replica LSN: %v", err)
				return fmt.Errorf("failed to get replica LSN: %w", err)
			}

			if !replicaLSN.Valid || replicaLSN.String == "" {
				return fmt.Errorf("replica has not received any WAL yet")
			}

			t.Logf("Replica is synchronized and receiving WAL at LSN: %s", replicaLSN.String)
			return nil
		},
		backoffPolicy,
	)
}

func postgresConnectionURI(host, port, database, username, password string) string {
	creds := ""
	if username != "" && password != "" {
		creds = fmt.Sprintf("%s:%s@", username, password)
	}

	return fmt.Sprintf(
		"postgres://%s%s:%s/%s?sslmode=disable",
		creds,
		host,
		port,
		database,
	)
}
