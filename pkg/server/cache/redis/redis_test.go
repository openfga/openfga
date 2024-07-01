package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/openfga/openfga/pkg/server/cache/redis"
	"github.com/stretchr/testify/require"
)

func TestRedis(t *testing.T) {
	redisContainer, err := redis.NewContainer()
	require.NoError(t, err)

	_, err = redis.New(
		redis.WithAddr(redisContainer.GetConnectionURI(false)),
	)
	require.ErrorIs(t, err, redis.ErrTTLMissing)

	_, err = redis.New(
		redis.WithTTL(time.Duration(10) * time.Second),
	)
	require.ErrorIs(t, err, redis.ErrAddrMissing)

	redisClient, err := redis.New(
		redis.WithAddr(redisContainer.GetConnectionURI(false)),
		redis.WithTTL(time.Duration(10)*time.Second),
	)
	require.NoError(t, err)

	t.Cleanup( func () {
		err := redisClient.Close()
		require.NoError(t, err)
	})
	ctx := context.Background()
	err = redisClient.Set(ctx, "key", []byte("value"))
	require.NoError(t, err)

	body, err := redisClient.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, []byte("value"), body)

	_, err = redisClient.Get(ctx, "missing_key")
	require.Equal(t, err, redis.ErrKeyNotFound)
}
