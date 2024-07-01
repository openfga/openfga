package redis_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/server/cache/redis"
)

func Test_ContainerValidation(t *testing.T) {
	redisClient, err := redis.NewContainer()
	require.NoError(t, err)

	host := redisClient.GetHost()
	require.NotEqual(t, "", host)

	port := redisClient.GetPort()
	require.NotEqual(t, "", port)

	uri := redisClient.GetConnectionURI(false)
	require.NotEqual(t, "", uri)

	host, port = redisClient.GetHostPort()
	require.NotEqual(t, "", host)
	require.NotEqual(t, "", port)

	ip := redisClient.GetIP()
	require.NotEqual(t, "", ip)

	err = redisClient.Terminate()
	require.NoError(t, err)

	db := redisClient.GetDatabase()
	require.Equal(t, 0, db)
}
