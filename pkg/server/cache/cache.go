package cache

import (
	"context"
)

// Container represents a runnable container for testing redis.
type Container interface {
	// RunTestContainer()
	// GetConnectionURI returns a connection string to the datastore instance running inside
	// the container.
	GetConnectionURI(includeCredentials bool) string

	// GetDatabaseSchemaVersion returns the last migration applied (e.g. 3) when the container was created
	GetDatabase() int

	// GetUserName return redis username.
	GetUsername() string

	// GetPassword returns redis password.
	GetPassword() string

	// GetHostPort returns containers host address and port.
	GetHostPort(includeCredentials bool) (string, string)

	// GetIP returns container IP address.
	GetIP() string

	// GetHost returns container Host address.
	GetHost() string

	// GetHost returns container Port.
	GetPort() string

	// Terminate the container
	Terminate() error
}

type Cache interface {
	// Ping returns the Redis server liveliness response
	Ping(ctx context.Context) error

	// Quit Closes the server connection
	Close() error

	// Del Removes the specified keys. A key is ignored if it does not exist.
	Del(ctx context.Context, keys ...string) error

	// Exists returns true/false the specified key exist
	Exists(ctx context.Context, key ...string) (bool, error)

	// Get return value associated with the key. return nil if the key is not found,  an error is returned.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set key to hold the value. If key already holds a value, it is overwritten. Any
	// previous TTL associated with the key is discarded on successful SET operation.
	Set(ctx context.Context, key string, value []byte) error
}
