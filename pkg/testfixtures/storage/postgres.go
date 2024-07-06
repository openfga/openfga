package storage

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/docker/api/types/container"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver.
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testcontainerspostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/openfga/openfga/assets"
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

	postgresContainer, err := testcontainerspostgres.Run(ctx, "postgres:14",
		testcontainers.WithWaitStrategy(wait.
			ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(5*time.Second),
		),
		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			hostConfig.Tmpfs = map[string]string{"/var/lib/postgresql/data": ""}
		}),
		testcontainerspostgres.WithDatabase("defaultdb"),
		testcontainerspostgres.WithUsername("postgres"),
		testcontainerspostgres.WithPassword("secret"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, postgresContainer.Terminate(ctx)) })

	postgresHost, err := postgresContainer.Host(ctx)
	require.NoError(t, err)
	postgresPort, err := postgresContainer.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)

	pgTestContainer := &postgresTestContainer{
		addr:     net.JoinHostPort(postgresHost, postgresPort.Port()),
		username: "postgres",
		password: "secret",
	}

	uri := fmt.Sprintf("postgres://%s:%s@%s/defaultdb?sslmode=disable", pgTestContainer.username, pgTestContainer.password, pgTestContainer.addr)

	goose.SetLogger(goose.NopLogger())

	db, err := goose.OpenDBWithDriver("pgx", uri)
	require.NoError(t, err)
	defer db.Close()

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
