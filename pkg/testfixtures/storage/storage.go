// Package storage contains containers that can be used to test all available data stores.
package storage

import (
	"testing"
)

// DatastoreTestContainer represents a runnable container for testing specific datastore engines.
type DatastoreTestContainer interface {

	// GetConnectionURI returns a connection string to the datastore instance running inside
	// the container.
	GetConnectionURI(includeCredentials bool) string

	// GetDatabaseSchemaVersion returns the last migration applied (e.g. 3) when the container was created
	GetDatabaseSchemaVersion() int64

	GetUsername() string
	GetPassword() string

	// CreateSecondary creates a secondary datastore if supported.
	// Returns an error if the operation fails or if the datastore doesn't support secondary datastores.
	CreateSecondary(t testing.TB) error

	// GetSecondaryConnectionURI returns the connection URI for the secondary datastore if one exists.
	// Returns an empty string if no secondary datastore exists.
	GetSecondaryConnectionURI(includeCredentials bool) string
}

type memoryTestContainer struct{}

func (m memoryTestContainer) GetConnectionURI(includeCredentials bool) string {
	return ""
}

func (m memoryTestContainer) GetUsername() string {
	return ""
}

func (m memoryTestContainer) GetPassword() string {
	return ""
}

func (m memoryTestContainer) GetDatabaseSchemaVersion() int64 {
	return 1
}

func (m memoryTestContainer) CreateSecondary(t testing.TB) error {
	return nil
}

func (m memoryTestContainer) GetSecondaryConnectionURI(includeCredentials bool) string {
	return ""
}

// RunDatastoreTestContainer constructs and runs a specific DatastoreTestContainer for the provided
// datastore engine. If applicable, it also runs all existing database migrations.
// The resources used by the test engine will be cleaned up after the test has finished.
func RunDatastoreTestContainer(t testing.TB, engine string) DatastoreTestContainer {
	switch engine {
	case "mysql":
		return NewMySQLTestContainer().RunMySQLTestContainer(t)
	case "postgres":
		return NewPostgresTestContainer().RunPostgresTestContainer(t)
	case "memory":
		return memoryTestContainer{}
	case "sqlite":
		return NewSqliteTestContainer().RunSqliteTestDatabase(t)
	case "dsql":
		return NewDSQLTestContainer().RunDSQLTestContainer(t)
	default:
		t.Fatalf("unsupported datastore engine: %q", engine)
		return nil
	}
}
