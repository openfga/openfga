package mysql_test

import (
	"context"
	"testing"

	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/openfga/openfga/storage/mysql"
	"github.com/openfga/openfga/storage/test"
	"github.com/stretchr/testify/require"
)

func TestMySQLDatastore(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")

	uri := testDatastore.GetConnectionURI()
	ds, err := mysql.NewMySQLDatastore(uri)
	require.NoError(t, err)
	defer ds.Close(context.Background())
	test.RunAllTests(t, ds)
}
