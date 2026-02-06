//go:build integration

package listusers

import "testing"

func TestListUsersPostgres(t *testing.T) {
	testRunAll(t, "postgres")
}

func TestListUsersMySQL(t *testing.T) {
	testRunAll(t, "mysql")
}

func TestListUsersSQLite(t *testing.T) {
	testRunAll(t, "sqlite")
}
