package storage

import (
	"context"
	"net"
	"net/url"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	testcontainersredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

const (
	redisImage = "redis:7"
)

type redisTestContainer struct {
	uri      string
	ip       string
	host     string
	port     string
	rdb      redis.UniversalClient
	version  int64
	username string
	password string
}

// NewRedisTestContainer returns an implementation of the DatastoreTestContainer interface
// for Postgres.
func NewRedisTestContainer() *redisTestContainer {
	return &redisTestContainer{}
}

func (p *redisTestContainer) GetDatabaseSchemaVersion() int64 {
	return p.version
}

// RunRedisTestContainer runs a Redis container, connects to it, and returns a
// bootstrapped implementation of the DatastoreTestContainer interface wired up for the
// redis datastore engine.
func (p *redisTestContainer) RunRedisTestContainer(t testing.TB) DatastoreTestContainer {
	ctx := context.Background()

	redisContainer, err := testcontainersredis.RunContainer(ctx,
		testcontainers.WithImage(redisImage),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, redisContainer.Terminate(ctx)) })

	p.uri, err = redisContainer.ConnectionString(ctx)
	require.NoError(t, err)

	p.rdb = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{":6379"},
	})
	defer p.rdb.Close()

	p.ip, err = redisContainer.ContainerIP(ctx)
	require.NoError(t, err)

	u, err := url.Parse(p.uri)
	require.NoError(t, err)

	p.host, p.port, err = net.SplitHostPort(u.Host)
	require.NoError(t, err)

	return p
}

// GetConnectionURI returns the postgres connection uri for the running postgres test container.
func (p *redisTestContainer) GetConnectionURI(includeCredentials bool) string {
	return p.host + ":" + p.port
}

func (p *redisTestContainer) GetUsername() string {
	return p.username
}

func (p *redisTestContainer) GetPassword() string {
	return p.password
}
