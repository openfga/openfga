package storage

import (
	"testing"
)

// DatastoreTestContainer represents a runnable container for testing specific datastore engines.
type DatastoreTestContainer interface {

	// GetConnectionURI returns a connection string to the datastore instance running inside
	// the container.
	GetConnectionURI() string
}

type memoryTestContainer struct{}

func (m memoryTestContainer) GetConnectionURI() string {
	return ""
}

// RunDatastoreTestContainer constructs and runs a specific DatastoreTestContainer for the provided
// datastore engine. The resources used by the test engine will be cleaned up after the test
// has finished.
func RunDatastoreTestContainer(t testing.TB, engine string) DatastoreTestContainer {
	switch engine {
	case "mysql":
		return NewMySQLTestContainer().RunMySQLTestContainer(t)
	case "postgres":
		return NewPostgresTestContainer().RunPostgresTestContainer(t)
	case "memory":
		return memoryTestContainer{}
	default:
		t.Fatalf("'%s' engine is not supported by RunDatastoreTestContainer", engine)
		return nil
	}
}
