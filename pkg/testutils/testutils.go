package testutils

import (
	"context"
	"math/rand"

	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/kelseyhightower/envconfig"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/memory"
	"github.com/openfga/openfga/storage/postgres"
	pgTestutils "github.com/openfga/openfga/storage/postgres/testutils"
	"go.opentelemetry.io/otel/trace"
)

const (
	AllChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var (
	configCache *testConfig
	pool        *pgxpool.Pool
)

type testConfig struct {
	BackendType string `split_words:"true" default:"memory"`
	PostgresURL string `envconfig:"POSTGRES_URL" default:"postgres://postgres:password@127.0.0.1:5432/postgres"`
}

func buildConfig() (*testConfig, error) {
	if configCache == nil {
		configCache = &testConfig{}
		if err := envconfig.Process("TEST_CONFIG", configCache); err != nil {
			return nil, err
		}
	}
	return configCache, nil
}

func BuildAllBackends(ctx context.Context, tracer trace.Tracer, logger logger.Logger) (*storage.AllBackends, error) {
	config, err := buildConfig()
	if err != nil {
		return nil, err
	}

	backends := &storage.AllBackends{}
	switch config.BackendType {
	case "memory":
		memoryBackend := memory.New(tracer, 10, 24)
		backends.TupleBackend = memoryBackend
		backends.AuthorizationModelBackend = memoryBackend
		backends.StoresBackend = memoryBackend
		backends.AssertionsBackend = memoryBackend
		backends.ChangelogBackend = memoryBackend
	case "postgres":
		if pool == nil {
			pool, err = pgxpool.Connect(ctx, config.PostgresURL)
			if err != nil {
				return nil, err
			}
			if err := pgTestutils.RecreatePostgresTestTables(ctx, pool); err != nil {
				return nil, err
			}
		}

		postgres := postgres.New(pool, tracer, logger)
		backends.TupleBackend = postgres
		backends.AuthorizationModelBackend = postgres
		backends.StoresBackend = postgres
		backends.AssertionsBackend = postgres
		backends.ChangelogBackend = postgres
	default:
		return nil, errors.Errorf("Unsupported backend type for tests: %s", config.BackendType)
	}
	return backends, nil
}

func CreateRandomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = AllChars[rand.Intn(len(AllChars))]
	}
	return string(b)
}
