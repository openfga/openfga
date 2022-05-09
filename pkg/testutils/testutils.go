package testutils

import (
	"math/rand"

	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/memory"
	"github.com/go-errors/errors"
	"github.com/kelseyhightower/envconfig"
	"go.opentelemetry.io/otel/trace"
)

const (
	AllChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var (
	configCache *testConfig
)

type testConfig struct {
	BackendType string `split_words:"true" default:"memory"`
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

func BuildAllBackends(tracer trace.Tracer) (*storage.AllBackends, error) {
	config, err := buildConfig()
	if err != nil {
		return nil, err
	}
	backends := &storage.AllBackends{}
	switch config.BackendType {
	case "memory":
		memoryBackend := memory.New(tracer, 10, 24)
		backends.TupleBackend = memoryBackend
		backends.ChangelogBackend = memoryBackend
		backends.AuthorizationModelBackend = memoryBackend
		backends.AssertionsBackend = memoryBackend
		backends.StoresBackend = memoryBackend
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
