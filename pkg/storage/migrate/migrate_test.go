package migrate_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
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

// migrateChildURI is set in the environment of the child process that
// TestMigrateTimeoutAndPingTimeoutUnset starts.
const migrateChildURI = "OPENFGA_TEST_MIGRATE_CHILD_URI"

func TestMigrateTimeoutAndPingTimeoutUnset(t *testing.T) {
	if uri := os.Getenv(migrateChildURI); uri != "" {
		migrateAndExit(uri)
	}

	// With neither field set, every ping used to fail and the retry loop had no limit, so the
	// call never returned. Run it in a child process: a regression fails the test after 30s,
	// and the child is killed and waited for, so no migration keeps retrying afterwards.
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(), migrateChildURI+"=file:"+filepath.Join(t.TempDir(), "openfga.db"))
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatal("RunMigrations did not return within 30s with Timeout and PingTimeout unset")
	}
	require.NoError(t, err, string(out))
}

// migrateAndExit runs the migrations with Timeout and PingTimeout unset in the child process.
// It exits directly, so TestMain doesn't remove the database containers that other test
// processes share.
func migrateAndExit(uri string) {
	if err := migrate.RunMigrations(migrate.MigrationConfig{Engine: "sqlite", URI: uri}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
