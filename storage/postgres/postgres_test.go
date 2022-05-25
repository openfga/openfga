package postgres_test

import (
	"testing"

	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/postgres"
	"github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
)

func TestPostgresDatastore(t *testing.T) {
	testEngine := storagefixtures.RunOpenFGADatastoreTestEngine(t, "postgres")

	test.TestAll(t, test.DatastoreTesterFunc(func() (storage.OpenFGADatastore, error) {
		ds := testEngine.NewDatastore(t, func(engine, uri string) storage.OpenFGADatastore {
			ds, err := postgres.NewPostgresDatastore(uri)
			require.NoError(t, err)

			return ds
		})

		return ds, nil
	}))
}
