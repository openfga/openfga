package storage

import (
	"testing"

	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
)

// InitFunc initializes a datastore instance from a uri that has been
// generated from a TestStorageBuilder
type InitFunc func(engine, uri string) storage.OpenFGADatastore

// RunningEngineForTest represents an instance of a database engine running with its backing
// database/service, expressly for testing.
type RunningEngineForTest interface {

	// NewDatabase returns the connection string to a new initialized logical database,
	// but one that is not migrated.
	NewDatabase(t testing.TB) string

	// NewDatastore returns a new logical datastore initialized with the
	// initFunc. For example, the sql based stores will create a new logical
	// database in the database instance, migrate it and provide the URI for it
	// to initFunc
	NewDatastore(t testing.TB, initFunc InitFunc) storage.OpenFGADatastore
}

// RunDatastoreEngine starts the backing database or service necessary for the given engine
// for testing and sets the defaults for that database or service. Note that this does *NOT*
// create the logical database nor call migrate; callers can do so via NewDatabase and NewDatastore
// respectively. Note also that the backing database or service will be shutdown automatically via
// the Cleanup of the testing object.
func RunDatastoreEngine(t testing.TB, engine string) RunningEngineForTest {
	return RunDatastoreEngineWithBridge(t, engine, "")
}

// RunDatastoreEngineWithBridge runs a datastore engine on a specific bridge. If a bridge is
// specified, then the hostnames returned by the engines are those to be called from another
// container on the bridge.
func RunDatastoreEngineWithBridge(t testing.TB, engine string, bridgeNetworkName string) RunningEngineForTest {
	switch engine {
	case "memory":
		require.Equal(t, "", bridgeNetworkName, "memory datastore does not support bridge networking")
		return RunMemoryForTesting(t)
	case "postgres":
		return RunPostgresForTesting(t, bridgeNetworkName)
	default:
		t.Fatalf("found missing engine for RunDatastoreEngine: %s", engine)
		return nil
	}
}
