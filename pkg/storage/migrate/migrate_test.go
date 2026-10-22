package migrate_test

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver.
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/pkg/storage/migrate"
)

func TestMigrateCommandRollbacks(t *testing.T) {
	type EngineConfig struct {
		Engine     string
		MinVersion int64
	}
	engines := []EngineConfig{
		{Engine: "postgres"},
		{Engine: "mysql"},
		{Engine: "sqlite", MinVersion: 5},
	}

	for _, e := range engines {
		t.Run(e.Engine, func(t *testing.T) {
			container, _, uri := util.MustBootstrapDatastore(t, e.Engine)

			// going from version 3 to 4 when migration #4 doesn't exist is a no-op
			version := container.GetDatabaseSchemaVersion() + 1

			for version >= e.MinVersion {
				t.Logf("migrating to version %d", version)
				err := migrate.RunMigrations(migrate.MigrationConfig{
					Engine:        e.Engine,
					URI:           uri,
					TargetVersion: uint(version),
					Timeout:       5 * time.Second,
					PingTimeout:   1 * time.Second,
					Verbose:       true,
				})
				require.NoError(t, err)
				version--
			}
		})
	}
}

func TestMigratePostgresRebuildsInvalidIndex(t *testing.T) {
	_, _, uri := util.MustBootstrapDatastore(t, "postgres")

	db, err := sql.Open("pgx", uri)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	cfg := migrate.MigrationConfig{
		Engine:      "postgres",
		URI:         uri,
		Timeout:     5 * time.Second,
		PingTimeout: 1 * time.Second,
	}

	// Roll back to the state before the rebuild migration, then leave idx_user_lookup
	// as a failed CREATE INDEX CONCURRENTLY leaves it.
	cfg.TargetVersion = 6
	require.NoError(t, migrate.RunMigrations(cfg))

	_, err = db.Exec(`UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'idx_user_lookup'::regclass`)
	require.NoError(t, err)

	cfg.TargetVersion = 0
	require.NoError(t, migrate.RunMigrations(cfg))

	var valid bool
	err = db.QueryRow(`SELECT indisvalid FROM pg_index WHERE indexrelid = 'idx_user_lookup'::regclass`).Scan(&valid)
	require.NoError(t, err)
	require.True(t, valid)
}
