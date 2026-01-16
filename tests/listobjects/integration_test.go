//go:build integration

package listobjects

import "testing"

func TestMatrixPostgres(t *testing.T) {
	runMatrixWithEngine(t, "postgres")
}

// TODO: re-enable
// func TestMatrixMysql(t *testing.T) {
//	runMatrixWithEngine(t, "mysql")
//}

// TODO: re-enable after investigating write contention in test
// func TestMatrixSqlite(t *testing.T) {
//	runMatrixWithEngine(t, "sqlite")
//}

func TestListObjectsPostgres(t *testing.T) {
	testRunAll(t, "postgres")
}

func TestListObjectsMySQL(t *testing.T) {
	testRunAll(t, "mysql")
}

func TestListObjectsSQLite(t *testing.T) {
	testRunAll(t, "sqlite")
}
