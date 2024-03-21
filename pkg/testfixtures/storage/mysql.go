package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/docker/api/types/container"
	"github.com/go-sql-driver/mysql"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testcontainersmysql "github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/openfga/openfga/assets"
)

const (
	mySQLImage = "mysql:8"
)

type mySQLTestContainer struct {
	addr     string
	version  int64
	username string
	password string
}

// NewMySQLTestContainer returns an implementation of the DatastoreTestContainer interface
// for MySQL.
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
	ctx := context.Background()

	mysqlContainer, err := testcontainersmysql.RunContainer(ctx,
		testcontainers.WithImage(mySQLImage),
		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			hostConfig.Tmpfs = map[string]string{"/var/lib/mysql": ""}
		}),
		testcontainersmysql.WithDatabase("defaultdb"),
		testcontainersmysql.WithUsername("root"),
		testcontainersmysql.WithPassword("secret"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mysqlContainer.Terminate(ctx)) })

	mysqlHost, err := mysqlContainer.Host(ctx)
	require.NoError(t, err)
	mysqlPort, err := mysqlContainer.MappedPort(ctx, "3306/tcp")
	require.NoError(t, err)

	mySQLTestContainer := &mySQLTestContainer{
		addr:     net.JoinHostPort(mysqlHost, mysqlPort.Port()),
		username: "root",
		password: "secret",
	}

	uri := fmt.Sprintf("%s:%s@tcp(%s)/defaultdb?parseTime=true", mySQLTestContainer.username, mySQLTestContainer.password, mySQLTestContainer.addr)

	err = mysql.SetLogger(log.New(io.Discard, "", 0))
	require.NoError(t, err)

	goose.SetLogger(goose.NopLogger())

	db, err := goose.OpenDBWithDriver("mysql", uri)
	require.NoError(t, err)
	defer db.Close()

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 2 * time.Minute
	err = backoff.Retry(
		func() error {
			return db.Ping()
		},
		backoffPolicy,
	)
	require.NoError(t, err, "failed to connect to mysql container")

	goose.SetBaseFS(assets.EmbedMigrations)

	err = goose.Up(db, assets.MySQLMigrationDir)
	require.NoError(t, err)
	version, err := goose.GetDBVersion(db)
	require.NoError(t, err)
	mySQLTestContainer.version = version

	return mySQLTestContainer
}

// GetConnectionURI returns the mysql connection uri for the running mysql test container.
func (m *mySQLTestContainer) GetConnectionURI(includeCredentials bool) string {
	creds := ""
	if includeCredentials {
		creds = fmt.Sprintf("%s:%s@", m.username, m.password)
	}

	return fmt.Sprintf(
		"%stcp(%s)/%s?parseTime=true",
		creds,
		m.addr,
		"defaultdb",
	)
}

func (m *mySQLTestContainer) GetUsername() string {
	return m.username
}

func (m *mySQLTestContainer) GetPassword() string {
	return m.password
}
