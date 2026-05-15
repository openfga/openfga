package mysql

import (
	"os"
	"testing"

	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
)

func TestMain(m *testing.M) {
	code := m.Run()
	storagefixtures.CleanupMysqlContainer()
	os.Exit(code)
}
