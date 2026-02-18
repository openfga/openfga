package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/go-sql-driver/mysql"
	"github.com/oklog/ulid/v2"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/testutils"
)

const (
	mysqlImage           = "mysql:8"
	mysqlContainerPrefix = "openfga-test-mysql"
)

var (
	mysqlContainerCfg = &container.Config{
		Env: []string{
			"MYSQL_ROOT_PASSWORD=secret",
		},
		ExposedPorts: nat.PortSet{
			nat.Port("3306/tcp"): {},
		},
		Image: mysqlImage,
	}
	mysqlHostCfg = &container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
		Tmpfs:           map[string]string{"/var/lib/mysql": ""},
	}
)

type mySQLTestContainer struct {
	addr     string
	version  int64
	username string
	password string
	database string
}

// NewMySQLTestContainer returns an implementation of the DatastoreTestContainer interface for MySQL.
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
	docker, err := testutils.NewDockerClient()
	require.NoError(t, err)
	t.Cleanup(func() {
		docker.Close()
	})

	cont, found, err := docker.FindRunningContainer(t.Context(), mysqlContainerPrefix, mysqlImage)
	require.NoError(t, err, "find running mysql container")

	if !found {
		t.Logf("No running container found for %s, creating a new one", mysqlImage)

		require.NoError(t, docker.PullImage(t.Context(), mysqlImage), "pull mysql image")

		containerName := mysqlContainerPrefix + ulid.Make().String()
		cont, err = docker.RunContainer(t.Context(), mysqlContainerCfg, mysqlHostCfg, containerName)
		require.NoError(t, err, "run mysql container")
	}

	p, ok := cont.NetworkSettings.Ports["3306/tcp"]
	if !ok || len(p) == 0 {
		require.Fail(t, "failed to get host port mapping from mysql container")
	}

	mysqlTestContainer := &mySQLTestContainer{
		addr:     "localhost:" + p[0].HostPort,
		username: "root",
		password: "secret",
	}

	// wait for the DB server to be ready before creating the test database.
	require.NoError(t, waitForDatabase("mysql", mysqlTestContainer.GetConnectionURI(true)))

	mysqlTestContainer.database = "openfga-test-db" + ulid.Make().String()
	creatExec := createMySQLExecConfig(mysqlTestContainer)
	require.NoError(t, docker.ExecCommand(t.Context(), cont.ID, creatExec), "create test database in mysql container")

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		dropExec := dropMySQLExecConfig(mysqlTestContainer)
		err := docker.ExecCommand(ctx, cont.ID, dropExec)
		require.NoError(t, err, "drop test database in mysql container")
	})

	// wait for the test database to be created before applying migrations.
	require.NoError(t, waitForDatabase("mysql", mysqlTestContainer.GetConnectionURI(true)))

	require.NoError(t, mysql.SetLogger(log.New(io.Discard, "", 0)))
	goose.SetLogger(goose.NopLogger())
	goose.SetBaseFS(assets.EmbedMigrations)

	db, err := goose.OpenDBWithDriver("mysql", mysqlTestContainer.GetConnectionURI(true))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	require.NoError(t, goose.Up(db, assets.MySQLMigrationDir))

	version, err := goose.GetDBVersion(db)
	require.NoError(t, err)
	mysqlTestContainer.version = version

	return mysqlTestContainer
}

// GetConnectionURI returns the mysql connection uri for the running mysql test container.
func (m *mySQLTestContainer) GetConnectionURI(includeCredentials bool) string {
	creds := ""
	if includeCredentials {
		creds = m.username + ":" + m.password + "@"
	}

	return fmt.Sprintf("%stcp(%s)/%s?parseTime=true", creds, m.addr, m.database)
}

func (m *mySQLTestContainer) GetUsername() string {
	return m.username
}

func (m *mySQLTestContainer) GetPassword() string {
	return m.password
}

func (m *mySQLTestContainer) CreateSecondary(t testing.TB) error {
	return nil
}

func (m *mySQLTestContainer) GetSecondaryConnectionURI(includeCredentials bool) string {
	return ""
}

// createMySQLExecConfig returns the container.ExecOptions for creating the test database in the MySQL container.
func createMySQLExecConfig(mysqlTestContainer *mySQLTestContainer) container.ExecOptions {
	return container.ExecOptions{
		Cmd: []string{
			"mysql",
			"-u", mysqlTestContainer.username,
			"-p" + mysqlTestContainer.password,
			"-e", fmt.Sprintf("CREATE DATABASE `%s`;", mysqlTestContainer.database),
		},
	}
}

// dropMySQLExecConfig returns the container.ExecOptions for dropping the test database in the MySQL container.
func dropMySQLExecConfig(mysqlTestContainer *mySQLTestContainer) container.ExecOptions {
	return container.ExecOptions{
		Cmd: []string{
			"mysql",
			"-u", mysqlTestContainer.username,
			"-p" + mysqlTestContainer.password,
			"-e",
			fmt.Sprintf("DROP DATABASE IF EXISTS `%s`;", mysqlTestContainer.database),
		},
	}
}
