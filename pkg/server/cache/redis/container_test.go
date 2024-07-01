package redis_test

import (
	"testing"

	"github.com/openfga/openfga/pkg/server/cache/redis"
	"github.com/stretchr/testify/require"
)

func Test_ContainerValidation(t *testing.T) {
	redisClient, err := redis.NewContainer()
	require.NoError(t, err)

	host := redisClient.GetHost()
	require.NotEqual(t, host, "")

	port := redisClient.GetPort()
	require.NotEqual(t, port, "")

	uri := redisClient.GetConnectionURI(false)
	require.NotEqual(t, uri, "")

	host, port = redisClient.GetHostPort()
	require.NotEqual(t, host, "")
	require.NotEqual(t, port, "")

	ip := redisClient.GetIP()
	require.NotEqual(t, ip, "")

	err = redisClient.Terminate()
	require.NoError(t, err)
}
