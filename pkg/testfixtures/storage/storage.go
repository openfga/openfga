package storage

import (
	"testing"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/stretchr/testify/require"
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

// RunDatastoreTestContainer constructs and runs a specifc DatastoreTestContainer for the provided
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
	default:
		t.Fatalf("'%s' engine is not supported by RunDatastoreTestContainer", engine)
		return nil
	}
}

func MustBootstrapDatastore(t testing.TB, engine string) storage.OpenFGADatastore {
	testDatastore := RunDatastoreTestContainer(t, engine)

	uri := testDatastore.GetConnectionURI(true)

	var ds storage.OpenFGADatastore
	var err error

	switch engine {
	case "memory":
		ds = memory.New(10, 24)
	case "postgres":
		ds, err = postgres.New(uri, sqlcommon.NewConfig())
	case "mysql":
		ds, err = mysql.New(uri, sqlcommon.NewConfig())
	default:
		t.Fatalf("'%s' is not a supported datastore engine", engine)
	}
	require.NoError(t, err)

	return ds
}
