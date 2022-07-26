package storage

import (
	"testing"

	"github.com/openfga/openfga/storage"
)

// DatastoreInitFunc defines a function to initialize a new datastore instance.
type DatastoreInitFunc[T any] func(engine, uri string) T

// DatastoreTestEngine defines a generic interface to run any generic datastore engine
// for testing purposes. A DatastoreTestEngine can be used in tests to spin up and tear
// down storage instances needed for testing.
type DatastoreTestEngine[T any] interface {

	// NewDatabase initializes a database and returns a connection string
	// that can be used to connect to it.
	NewDatabase(t testing.TB) string

	// NewDatastore constructs and initializes a new datastore with the provided init
	// function.
	NewDatastore(t testing.TB, init DatastoreInitFunc[T]) T
}

type datastoreTestEngine[T any] struct{}

func NewDatastoreTestEngine[T any]() *datastoreTestEngine[T] {
	return &datastoreTestEngine[T]{}
}

// RunDatastoreTestEngine constructs and runs a specifc DatastoreTestEngine for the provided
// datastore engine. The resources used by the test engine will be cleaned up after the test
// has finished.
func (d *datastoreTestEngine[T]) RunDatastoreTestEngine(t testing.TB, engine string) DatastoreTestEngine[T] {
	switch engine {
	case "memory":
		return RunMemoryTestEngine[T](t)
	case "postgres":
		return NewPostgresTestEngine[T]().RunPostgresTestEngine(t)
	default:
		t.Fatalf("'%s' engine is not supported by RunDatastoreTestEngine", engine)
		return nil
	}
}

// RunOpenFGADatastoreTestEngine runs a datastore test engine specifically for the OpenFGADatastore storage interface.
func RunOpenFGADatastoreTestEngine(t testing.TB, engine string) DatastoreTestEngine[storage.OpenFGADatastore] {
	testEngine := NewDatastoreTestEngine[storage.OpenFGADatastore]()
	return testEngine.RunDatastoreTestEngine(t, engine)
}
