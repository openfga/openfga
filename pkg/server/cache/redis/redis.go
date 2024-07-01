package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/openfga/openfga/pkg/server/cache"
	"github.com/redis/go-redis/v9"
)

type options func(s *Handle)
type Handle struct {
	db             int
	ttl            time.Duration
	addrs          []string
	userCredential string
	passCredential string
	client         redis.UniversalClient
}

var (
	ErrKeyNotFound = fmt.Errorf("key not found")
	ErrTTLExpired  = fmt.Errorf("key removed by TTL")
	ErrTTLMissing  = fmt.Errorf("TTL must be specified")
	ErrAddrMissing = fmt.Errorf("redis addresses must be specified")
)

// WithTTL Cached item Time To Live (TTL)
func WithTTL(ttl time.Duration) options {
	return func(h *Handle) {
		h.ttl = ttl
	}
}

func WithAddr(addrs string) options {
	return func(h *Handle) {
		h.addrs = strings.Split(addrs, ",")
	}
}

func WithUserCredential(credential string) options {
	return func(h *Handle) {
		h.userCredential = credential
	}
}

func WithPassCredential(credential string) options {
	return func(h *Handle) {
		h.passCredential = credential
	}
}

func WithDatabase(db int) options {
	return func(h *Handle) {
		h.db = db
	}
}

// New create new instance cache
func New(opts ...options) (cache.Cache, error) {
	h := &Handle{}

	for _, opt := range opts {
		opt(h)
	}

	if err := h.validate(); err != nil {
		return nil, err
	}

	h.client = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    h.addrs,
		Username: h.userCredential,
		Password: h.passCredential,
	})

	return h, nil
}

func (h *Handle) validate() error {
	if h.addrs == nil {
		return ErrAddrMissing
	}

	if h.ttl == 0 {
		return ErrTTLMissing
	}

	return nil
}

// Ping returns the Redis server liveliness response
func (h *Handle) Ping(ctx context.Context) error {
	return h.client.Ping(ctx).Err()
}

// Quit Closes the server connection
func (h *Handle) Close() error {
	return h.client.Close()
}

// Del Removes the specified keys. A key is ignored if it does not exist.
func (h *Handle) Del(ctx context.Context, keys ...string) error {
	return h.client.Del(ctx, keys...).Err()
}

// Exists returns true/false the specified key exist
func (h *Handle) Exists(ctx context.Context, keys ...string) (bool, error) {
	exists, err := h.client.Exists(ctx, keys...).Result()
	return exists != 0, err
}

// Get return value associated with the key. return nil if the key is not found,  an error is returned.
func (h *Handle) Get(ctx context.Context, key string) ([]byte, error) {
	redisCmd := h.client.Get(ctx, key)
	switch {
	case errors.Is(redisCmd.Err(), redis.Nil):
		return nil, ErrKeyNotFound
	case !errors.Is(redisCmd.Err(), nil):
		return nil, redisCmd.Err()
	case redisCmd.Val() == "":
		return nil, ErrTTLExpired
	default:
		return []byte(redisCmd.Val()), nil
	}
}

// Set key to hold the string value. If key already holds a value, it is overwritten.
func (h Handle) Set(ctx context.Context, key string, value []byte) error {
	return h.client.Set(ctx, key, string(value), h.ttl).Err()
}
