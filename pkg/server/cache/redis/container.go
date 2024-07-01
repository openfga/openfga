package redis

import (
	"context"
	"fmt"
	"net"
	"net/url"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	testcontainersredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/openfga/openfga/pkg/server/cache"
)

const (
	redisImage = "redis:latest"
)

type RedisContainer struct {
	uri       string
	ip        string
	host      string
	port      string
	db        int
	rdb       redis.UniversalClient
	container *testcontainersredis.RedisContainer
	username  string
	password  string
}

// NewRedisTestContainer returns an implementation of the DatastoreTestContainer interface
// for Postgres.
func NewContainer() (cache.Container, error) {
	return RedisContainer{}.RunTestContainer()
}

func (p RedisContainer) GetDatabase() int {
	return p.db
}

// RunRedisContainer runs a Redis container, connects to it, and returns a
// bootstrapped implementation of the RedisContainer interface wired up for the
// redis datastore engine.
func (p RedisContainer) RunTestContainer() (cache.Container, error) {
	ctx := context.Background()

	var err error
	p.container, err = testcontainersredis.RunContainer(ctx,
		testcontainers.WithImage(redisImage),
	)
	if err != nil {
		return p, err
	}

	p.uri, err = p.container.ConnectionString(ctx)
	if err != nil {
		return p, err
	}

	p.rdb = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{":6379"},
	})
	defer p.rdb.Close()

	p.ip, err = p.container.ContainerIP(ctx)
	if err != nil {
		return p, err
	}

	u, err := url.Parse(p.uri)
	if err != nil {
		return p, err
	}

	p.host, p.port, err = net.SplitHostPort(u.Host)
	if err != nil {
		return p, err
	}

	return p, nil
}

func (p RedisContainer) Close() {
	p.rdb.Close()
}

func (p RedisContainer) GetPort() string {
	return p.port
}

func (p RedisContainer) GetHost() string {
	return p.host
}

// GetConnectionURI returns the postgres connection uri for the running postgres test container.
func (p RedisContainer) GetConnectionURI(includeCredentials bool) string {
	if includeCredentials {
		return fmt.Sprintf("redis://%s:%s@%s:%s/0", p.GetUsername(), p.GetPassword(), p.GetHost(), p.GetPort())
	}
	return p.GetHost() + ":" + p.GetPort()
}

// GetHostPort returns the postgres connection uri for the running postgres test container.
func (p RedisContainer) GetHostPort(includeCredentials bool) (string, string) {
	return p.GetHost(), p.GetPort()
}

func (p RedisContainer) GetUsername() string {
	return p.username
}

func (p RedisContainer) GetPassword() string {
	return p.password
}

func (p RedisContainer) GetIP() string {
	return p.ip
}

func (p RedisContainer) Terminate() error {
	return p.container.Terminate(context.Background())
}
