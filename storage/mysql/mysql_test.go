package mysql_test

import (
	"testing"

	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/mysql"
	"github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
)

func TestMySQLDatastore(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")

	uri := testDatastore.GetConnectionURI()
	ds, err := mysql.New(uri, storage.NewConfig())
	require.NoError(t, err)
	defer ds.Close()
	test.RunAllTests(t, ds)
}
