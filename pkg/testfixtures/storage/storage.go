package storage

import (
	"testing"

	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
)

// InitFunc initializes a datastore instance from a uri that has been
// generated from a TestStorageBuilder
type InitFunc[T any] func(engine, uri string) T

// RunningEngineForTest represents an instance of a datastore engine running with its backing
// database/service, expressly for testing.
type RunningEngineForTest[T any] interface {

	// NewDatabase returns the connection string to a new initialized logical database,
	// but one that is not migrated.
	NewDatabase(t testing.TB) string

	// NewDatastore returns a new logical datastore initialized with the
	// initFunc. For example, the sql based stores will create a new logical
	// database in the database instance, migrate it and provide the URI for it
	// to initFunc
	NewDatastore(t testing.TB, initFunc InitFunc[T]) T
}

type DatastoreTestEngine[T any] struct{}

func NewDatastoreTestEngine[T any]() *DatastoreTestEngine[T] {
	return &DatastoreTestEngine[T]{}
}

// RunDatastoreTestEngine starts the backing database or service necessary for the given engine
// for testing and sets the defaults for that database or service. Note that this does *NOT*
// create the logical database nor call migrate; callers can do so via NewDatabase and NewDatastore
// respectively. Note also that the backing database or service will be shutdown automatically via
// the Cleanup of the testing object.
func (d *DatastoreTestEngine[T]) RunDatastoreTestEngine(t testing.TB, engine string) RunningEngineForTest[T] {
	return d.RunDatastoreTestEngineWithBridge(t, engine, "")
}

// RunDatastoreTestEngineWithBridge runs a datastore engine on a specific bridge. If a bridge is
// specified, then the hostnames returned by the engines are those to be called from another
// container on the bridge.
func (d *DatastoreTestEngine[T]) RunDatastoreTestEngineWithBridge(t testing.TB, engine, bridgeNetworkName string) RunningEngineForTest[T] {
	switch engine {
	case "memory":
		require.Equal(t, "", bridgeNetworkName, "memory datastore does not support bridge networking")
		return RunMemoryForTesting[T](t)
	case "postgres":
		pgTester := NewPostgresTester[T]()
		return pgTester.RunPostgresForTesting(t, bridgeNetworkName)
	default:
		t.Fatalf("found missing engine for RunDatastoreEngine: %s", engine)
		return nil
	}
}

// RunOpenFGADatastoreTestEngine runs a datastore test engine specifically for the OpenFGADatastore storage interface.
func RunOpenFGADatastoreTestEngine(t testing.TB, engine string) RunningEngineForTest[storage.OpenFGADatastore] {
	testEngine := NewDatastoreTestEngine[storage.OpenFGADatastore]()
	return testEngine.RunDatastoreTestEngine(t, engine)
}
