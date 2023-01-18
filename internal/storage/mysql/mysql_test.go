package mysql_test

import (
	"testing"

	"github.com/openfga/openfga/internal/storage/common"
	"github.com/openfga/openfga/internal/storage/mysql"
	"github.com/openfga/openfga/internal/storage/test"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/stretchr/testify/require"
)

func TestMySQLDatastore(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")

	uri := testDatastore.GetConnectionURI()
	ds, err := mysql.New(uri, common.NewConfig())
	require.NoError(t, err)
	defer ds.Close()
	test.RunAllTests(t, ds)
}
