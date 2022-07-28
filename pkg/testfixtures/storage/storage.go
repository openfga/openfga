package storage

import (
	"testing"
)

// DatastoreTestContainer represents a runnable container for testing specific datastore engines.
type DatastoreTestContainer interface {
	GetConnectionURI() string
}

// RunDatastoreTestContainer constructs and runs a specifc DatastoreTestContainer for the provided
// datastore engine. The resources used by the test engine will be cleaned up after the test
// has finished.
func RunDatastoreTestContainer(t testing.TB, engine string) DatastoreTestContainer {
	switch engine {
	case "postgres":
		return NewPostgresTestContainer().RunPostgresTestContainer(t)
	default:
		t.Fatalf("'%s' engine is not supported by RunDatastoreTestContainer", engine)
		return nil
	}
}
