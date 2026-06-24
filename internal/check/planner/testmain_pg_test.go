//go:build docker

package planner

import (
	"os"
	"testing"

	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
)

// TestMain tears down the shared PostgreSQL test container after the planner integration
// tests finish. It mirrors pkg/storage/postgres/testmain_test.go; the container itself is
// bootstrapped lazily by storagefixtures.RunDatastoreTestContainer.
func TestMain(m *testing.M) {
	code := m.Run()
	storagefixtures.CleanupPostgresContainer()
	os.Exit(code)
}
