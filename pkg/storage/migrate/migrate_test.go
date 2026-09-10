package migrate_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/pkg/storage/migrate"
)

func TestRunMigrationsRequiresDatastoreURI(t *testing.T) {
	tests := []struct {
		name        string
		engine      string
		uri         string
		expectedErr string
	}{
		{
			name:        "postgres empty uri",
			engine:      "postgres",
			expectedErr: "missing datastore uri for postgres datastore engine",
		},
		{
			name:        "postgres whitespace uri",
			engine:      "postgres",
			uri:         " ",
			expectedErr: "missing datastore uri for postgres datastore engine",
		},
		{
			name:        "mysql empty uri",
			engine:      "mysql",
			expectedErr: "missing datastore uri for mysql datastore engine",
		},
		{
			name:        "mysql whitespace uri",
			engine:      "mysql",
			uri:         " ",
			expectedErr: "missing datastore uri for mysql datastore engine",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := migrate.RunMigrations(migrate.MigrationConfig{
				Engine:  test.engine,
				Timeout: 5 * time.Second,
			})
			require.EqualError(t, err, test.expectedErr)
		})
	}
}

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
