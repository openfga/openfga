package postgres_test

import (
	"testing"

	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/storage/postgres"
	"github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
)

func TestPostgresDatastore(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "postgres")

	uri := testDatastore.GetConnectionURI()
	ds, err := postgres.NewPostgresDatastore(uri)
	require.NoError(t, err)

	test.RunAllTests(t, ds)
}
