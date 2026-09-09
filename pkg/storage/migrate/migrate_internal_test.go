package migrate

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBuildPostgresMigrationURI is a regression test for
// https://github.com/openfga/openfga/issues/1832, where `migrate` running behind a
// connection pooler such as PgBouncer in transaction pooling mode could fail with
// `relation "goose_db_version" already exists` on a second run. Pgx defaults to
// server-side prepared statements, which pgx's own documentation states are
// incompatible with PgBouncer; a stale prepared statement causes goose's
// version-check query to fail, which goose misinterprets as "table missing" and
// then fails when it tries to recreate a table that already exists.
func TestBuildPostgresMigrationURI(t *testing.T) {
	t.Run("disables prepared statements by default", func(t *testing.T) {
		got, err := buildPostgresMigrationURI("postgres://user:pass@localhost:5432/openfga", "", "")
		require.NoError(t, err)

		parsed, err := url.Parse(got)
		require.NoError(t, err)
		require.Equal(t, "simple_protocol", parsed.Query().Get("default_query_exec_mode"))
	})

	t.Run("does not override an explicit default_query_exec_mode", func(t *testing.T) {
		got, err := buildPostgresMigrationURI("postgres://user:pass@localhost:5432/openfga?default_query_exec_mode=cache_statement", "", "")
		require.NoError(t, err)

		parsed, err := url.Parse(got)
		require.NoError(t, err)
		require.Equal(t, "cache_statement", parsed.Query().Get("default_query_exec_mode"))
	})

	t.Run("applies username/password overrides", func(t *testing.T) {
		got, err := buildPostgresMigrationURI("postgres://olduser:oldpass@localhost:5432/openfga", "newuser", "newpass")
		require.NoError(t, err)

		parsed, err := url.Parse(got)
		require.NoError(t, err)
		require.Equal(t, "newuser", parsed.User.Username())
		password, ok := parsed.User.Password()
		require.True(t, ok)
		require.Equal(t, "newpass", password)
	})

	t.Run("preserves existing credentials when no override given", func(t *testing.T) {
		got, err := buildPostgresMigrationURI("postgres://olduser:oldpass@localhost:5432/openfga", "", "")
		require.NoError(t, err)

		parsed, err := url.Parse(got)
		require.NoError(t, err)
		require.Equal(t, "olduser", parsed.User.Username())
		password, ok := parsed.User.Password()
		require.True(t, ok)
		require.Equal(t, "oldpass", password)
	})

	t.Run("returns an error for an invalid uri", func(t *testing.T) {
		_, err := buildPostgresMigrationURI(":not a uri", "", "")
		require.Error(t, err)
	})
}
