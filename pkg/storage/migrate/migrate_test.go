package migrate_test

import (
	"path/filepath"
	"testing"
	"time"

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

func TestMigrateDefaultPingTimeout(t *testing.T) {
	// Code written before MigrationConfig had PingTimeout leaves it at zero.
	tests := []struct {
		name        string
		pingTimeout time.Duration
	}{
		{name: "zero", pingTimeout: 0},
		{name: "negative", pingTimeout: -time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := migrate.RunMigrations(migrate.MigrationConfig{
				Engine:      "sqlite",
				URI:         "file:" + filepath.Join(t.TempDir(), "openfga.db"),
				Timeout:     time.Second,
				PingTimeout: test.pingTimeout,
			})
			require.NoError(t, err)
		})
	}
}

func TestMigrateTimeoutAndPingTimeoutUnset(t *testing.T) {
	// With neither field set, every ping used to fail and the retry loop had no limit, so the
	// call never returned. Run it in a goroutine so a regression fails the test instead of
	// hanging the suite.
	uri := "file:" + filepath.Join(t.TempDir(), "openfga.db")
	errCh := make(chan error, 1)
	go func() {
		errCh <- migrate.RunMigrations(migrate.MigrationConfig{
			Engine: "sqlite",
			URI:    uri,
		})
	}()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("RunMigrations did not return within 30s with Timeout and PingTimeout unset")
	}
}
