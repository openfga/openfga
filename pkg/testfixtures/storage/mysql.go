package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	_ "github.com/go-sql-driver/mysql"
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
	mysqlImage                 = "mysql:8"
	mysqlDBPrefix              = "openfga-test-db-"
	mysqlTemplateDB            = mysqlDBPrefix + "template"
	mysqlTemplateDBDump        = "/tmp/" + mysqlTemplateDB + ".sql"
	mysqlTemplateDBPartialDump = mysqlTemplateDBDump + ".partial"
	mysqlUsername              = "root"
	mysqlPassword              = "secret"
)

var (
	_ DatastoreTestContainer = (*mysqlTestContainer)(nil)

	mysqlContainerName = "openfga-test-mysql-" + ulid.Make().String()
	mysqlPort          = network.MustParsePort("3306/tcp")
	mysqlDockerCont    *container.InspectResponse
	mysqlOnce          sync.Once
)

type mysqlTestContainer struct {
	host string
	port string

	database string
	username string
	password string

	version int64
}

// GetConnectionURI returns the mysql connection uri for the running mysql test container.
func (m *mysqlTestContainer) GetConnectionURI(includeCredentials bool) string {
	var username, password string
	if includeCredentials {
		username = m.username
		password = m.password
	}

	return mysqlConnectionURI(m.host, m.port, m.database, username, password)
}

func (m *mysqlTestContainer) GetDatabaseSchemaVersion() int64 {
	return m.version
}

func (m *mysqlTestContainer) GetUsername() string {
	return m.username
}

func (m *mysqlTestContainer) GetPassword() string {
	return m.password
}

func (m *mysqlTestContainer) CreateSecondary(t testing.TB) error {
	return nil
}

func (m *mysqlTestContainer) GetSecondaryConnectionURI(includeCredentials bool) string {
	return ""
}

func RunMysqlTestContainer(t testing.TB) DatastoreTestContainer {
	docker, err := testutils.NewDockerClient()
	require.NoError(t, err)

	t.Cleanup(func() {
		docker.Close()
	})

	mysqlOnce.Do(func() {
		mysqlDockerCont = bootstrapMysqlContainer(t, docker)
	})

	port, err := docker.GetHostPort(mysqlDockerCont, mysqlPort)
	require.NoError(t, err)

	version, err := latestMigrationVersion(assets.MySQLMigrationDir)
	require.NoError(t, err, "get expected mysql migration version")

	testCont := &mysqlTestContainer{
		host:     "localhost",
		port:     port,
		database: mysqlDBPrefix + ulid.Make().String(),
		username: mysqlUsername,
		password: mysqlPassword,
		version:  version,
	}

	tplURI := mysqlConnectionURI(testCont.host, testCont.port, mysqlTemplateDB, testCont.username, testCont.password)
	require.NoError(t, waitForMigrationVersion("mysql", tplURI, testCont.version))

	createExec := client.ExecCreateOptions{
		Cmd: []string{"mysql", "-u", testCont.username, "-e", fmt.Sprintf("CREATE DATABASE `%s`;", testCont.database)},
		Env: []string{"MYSQL_PWD=" + testCont.password},
	}
	require.NoError(t, docker.ExecCommand(t.Context(), mysqlDockerCont.ID, createExec))

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		dropQuery := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", testCont.database)
		dropExec := client.ExecCreateOptions{
			Cmd: []string{"mysql", "-u", testCont.username, "-e", dropQuery},
			Env: []string{"MYSQL_PWD=" + testCont.password},
		}
		if err := docker.ExecCommand(ctx, mysqlDockerCont.ID, dropExec); err != nil {
			t.Errorf("drop test database in the mysql container: %v", err)
		}
	})

	copyShell := fmt.Sprintf("mysql -u %s %s < %s", testCont.username, testCont.database, mysqlTemplateDBDump)
	copyExec := client.ExecCreateOptions{
		Cmd: []string{"sh", "-ec", copyShell},
		Env: []string{"MYSQL_PWD=" + testCont.password},
	}
	require.NoError(t, docker.ExecCommand(t.Context(), mysqlDockerCont.ID, copyExec))

	require.NoError(t, waitForMigrationVersion("mysql", testCont.GetConnectionURI(true), testCont.version))

	return testCont
}

func CleanupMysqlContainer() {
	_ = cleanupDatastoreTestContainer(mysqlContainerName)
}

func bootstrapMysqlContainer(t testing.TB, docker *testutils.DockerClient) *container.InspectResponse {
	t.Logf("No running container found for %s, creating a new one", mysqlImage)

	require.NoError(t, docker.PullImage(t.Context(), mysqlImage), "pull mysql image")

	contCfg := &container.Config{
		Env: []string{
			"MYSQL_DATABASE=" + mysqlTemplateDB,
			"MYSQL_ROOT_PASSWORD=" + mysqlPassword,
		},
		ExposedPorts: network.PortSet{
			mysqlPort: {},
		},
		Image: mysqlImage,
	}

	hostCfg := &container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
		Tmpfs:           map[string]string{"/var/lib/mysql": "", "/tmp": ""},
	}

	cont, err := docker.RunContainer(t.Context(), contCfg, hostCfg, mysqlContainerName)
	require.NoError(t, err, "run mysql container")

	port, err := docker.GetHostPort(cont, mysqlPort)
	require.NoError(t, err)

	dbURI := mysqlConnectionURI("localhost", port, mysqlTemplateDB, mysqlUsername, mysqlPassword)
	require.NoError(t, waitForDatabase("mysql", dbURI))

	db, err := goose.OpenDBWithDriver("mysql", dbURI)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, goose.Up(db, assets.MySQLMigrationDir))

	// Append goose_db_version last so it is restored after all schema tables.
	dumpShell := fmt.Sprintf(
		"mysqldump -u %[1]s --ignore-table=%[2]s.goose_db_version %[2]s > %[3]s "+
			"&& mysqldump -u %[1]s %[2]s goose_db_version >> %[3]s "+
			"&& mv %[3]s %[4]s",
		mysqlUsername, mysqlTemplateDB, mysqlTemplateDBPartialDump, mysqlTemplateDBDump,
	)
	dumpExec := client.ExecCreateOptions{
		Cmd: []string{"sh", "-ec", dumpShell},
		Env: []string{"MYSQL_PWD=" + mysqlPassword},
	}
	require.NoError(t, docker.ExecCommand(t.Context(), cont.ID, dumpExec))
	require.NoError(t, waitForMysqlDumpFile(t.Context(), docker, cont.ID))

	return cont
}

// waitForMysqlDumpFile waits until the template database dump is present and non-empty.
func waitForMysqlDumpFile(ctx context.Context, docker *testutils.DockerClient, containerID string) error {
	backoffPolicy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
		backoff.WithMaxElapsedTime(30*time.Second),
	)

	return backoff.Retry(func() error {
		checkExec := client.ExecCreateOptions{
			Cmd: []string{"test", "-s", mysqlTemplateDBDump},
		}
		if err := docker.ExecCommand(ctx, containerID, checkExec); err != nil {
			return fmt.Errorf("mysql template dump not ready: %w", err)
		}

		return nil
	}, backoff.WithContext(backoffPolicy, ctx))
}

func mysqlConnectionURI(host, port, database, username, password string) string {
	creds := ""
	if username != "" && password != "" {
		creds = fmt.Sprintf("%s:%s@", username, password)
	}

	return fmt.Sprintf("%stcp(%s:%s)/%s?parseTime=true", creds, host, port, database)
}
