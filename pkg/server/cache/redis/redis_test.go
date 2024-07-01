package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/server/cache/redis"
)

func TestRedis(t *testing.T) {
	ctx := context.Background()
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
		redis.WithDatabase(0),
		redis.WithTTL(time.Duration(10)*time.Second),
	)
	require.NoError(t, err)

	err = redisClient.Ping(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		err := redisClient.Close()
		require.NoError(t, err)
	})
	err = redisClient.Set(ctx, "key", []byte("value"))
	require.NoError(t, err)

	body, err := redisClient.Get(ctx, "key")
	require.NoError(t, err)
	require.Equal(t, []byte("value"), body)

	ok, err := redisClient.Exists(ctx, "key")
	require.NoError(t, err)
	require.True(t, ok)

	err = redisClient.Del(ctx, "key")
	require.NoError(t, err)

	_, err = redisClient.Get(ctx, "missing_key")
	require.Equal(t, err, redis.ErrKeyNotFound)
}
