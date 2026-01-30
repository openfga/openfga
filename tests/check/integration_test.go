//go:build integration

package check

import "testing"

func TestMatrixPostgres(t *testing.T) {
	runMatrixWithEngine(t, "postgres")
}

func TestMatrixMySQL(t *testing.T) {
	runMatrixWithEngine(t, "mysql")
}

// TODO: re-enable after investigating write contention in test
// func TestMatrixSqlite(t *testing.T) {
//	runMatrixWithEngine(t, "sqlite")
//}

func TestCheckPostgres(t *testing.T) {
	testRunAll(t, "postgres")
}

func TestCheckMySQL(t *testing.T) {
	testRunAll(t, "mysql")
}

func TestCheckSQLite(t *testing.T) {
	testRunAll(t, "sqlite")
}
