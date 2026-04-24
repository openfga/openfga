package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

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
	mysqlImage          = "mysql:8"
	mysqlDBPrefix       = "openfga-test-db-"
	mysqlTemplateDB     = mysqlDBPrefix + "template"
	mysqlTemplateDBDump = "/tmp/" + mysqlTemplateDB + ".sql"
	mysqlUsername       = "root"
	mysqlPassword       = "secret"
)

var (
	_ DatastoreTestContainer = (*mysqlTestContainer)(nil)

	mysqlContainerName = "openfga-test-mysql-" + ulid.Make().String()
	mysqlPort          = network.MustParsePort("3306/tcp")
	mysqlDockerCont    *container.InspectResponse

	mysqlBootstrapping bool
	mysqlCond          = sync.NewCond(&sync.Mutex{})
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

	// Shared MySQL container bootstrap for concurrent tests using sync.Cond.
	// Only one test bootstraps the shared container at a time, while others wait efficiently using sync.Cond.
	// If bootstrap fails, waiting tests are awakened so another test can retry without being affected by the failure.
	mysqlCond.L.Lock()
	for mysqlDockerCont == nil {
		if !mysqlBootstrapping {
			mysqlBootstrapping = true
			mysqlCond.L.Unlock()

			dockerCont, err := bootstrapMysqlContainer(t.Context(), docker)
			mysqlCond.L.Lock()
			mysqlBootstrapping = false
			if err == nil {
				mysqlDockerCont = dockerCont
			}

			mysqlCond.Broadcast()

			if err != nil {
				// Unlock before failing the test to allow waiting tests to proceed with bootstrapping.
				mysqlCond.L.Unlock()
				require.NoError(t, err)
			}

			continue
		}

		mysqlCond.Wait()
	}
	mysqlCond.L.Unlock()

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

func bootstrapMysqlContainer(ctx context.Context, docker *testutils.DockerClient) (*container.InspectResponse, error) {
	if err := docker.PullImage(ctx, mysqlImage); err != nil {
		return nil, fmt.Errorf("pull mysql image: %w", err)
	}

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
		Tmpfs: map[string]string{
			"/var/lib/mysql": "size=2g",
			"/tmp":           "size=512m",
		},
	}

	cont, err := docker.RunContainer(ctx, contCfg, hostCfg, mysqlContainerName)
	if err != nil {
		return nil, fmt.Errorf("run mysql container: %w", err)
	}

	needsCleanup := true
	defer func() {
		if needsCleanup {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_ = docker.RemoveContainer(cleanupCtx, cont.ID)
		}
	}()

	port, err := docker.GetHostPort(cont, mysqlPort)
	if err != nil {
		return nil, fmt.Errorf("get mysql host port: %w", err)
	}

	dbURI := mysqlConnectionURI("localhost", port, mysqlTemplateDB, mysqlUsername, mysqlPassword)
	if err := waitForDatabase("mysql", dbURI); err != nil {
		return nil, fmt.Errorf("wait for mysql database: %w", err)
	}

	db, err := goose.OpenDBWithDriver("mysql", dbURI)
	if err != nil {
		return nil, fmt.Errorf("open mysql database: %w", err)
	}
	defer db.Close()

	if err := goose.Up(db, assets.MySQLMigrationDir); err != nil {
		return nil, fmt.Errorf("apply mysql migrations: %w", err)
	}

	// Append goose_db_version last so it is restored after all schema tables.
	dumpShell := fmt.Sprintf(
		"mysqldump -u %[1]s --ignore-table=%[2]s.goose_db_version %[2]s > %[3]s "+
			"&& mysqldump -u %[1]s %[2]s goose_db_version >> %[3]s ",
		mysqlUsername, mysqlTemplateDB, mysqlTemplateDBDump,
	)
	dumpExec := client.ExecCreateOptions{
		Cmd: []string{"sh", "-ec", dumpShell},
		Env: []string{"MYSQL_PWD=" + mysqlPassword},
	}
	if err := docker.ExecCommand(ctx, cont.ID, dumpExec); err != nil {
		return nil, fmt.Errorf("dump mysql template database: %w", err)
	}

	needsCleanup = false
	return cont, nil
}

func mysqlConnectionURI(host, port, database, username, password string) string {
	creds := ""
	if username != "" && password != "" {
		creds = fmt.Sprintf("%s:%s@", username, password)
	}

	return fmt.Sprintf("%stcp(%s:%s)/%s?parseTime=true", creds, host, port, database)
}
