package migrate

import (
	"strconv"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/cmd/util"
)

const defaultDuration = 1 * time.Minute

func TestMigrateCommandRollbacks(t *testing.T) {
	engines := []string{"postgres", "mysql"}

	for _, engine := range engines {
		t.Run(engine, func(t *testing.T) {
			container, _, uri := util.MustBootstrapDatastore(t, engine)

			// going from version 3 to 4 when migration #4 doesn't exist is a no-op
			version := container.GetDatabaseSchemaVersion() + 1

			migrateCommand := NewMigrateCommand()

			for version >= 0 {
				t.Logf("migrating to version %d", version)
				migrateCommand.SetArgs([]string{"--datastore-engine", engine, "--datastore-uri", uri, "--version", strconv.Itoa(int(version))})
				err := migrateCommand.Execute()
				require.NoError(t, err)
				version--
			}
		})
	}
}

func TestMigrateCommandNoConfigDefaultValues(t *testing.T) {
	util.PrepareTempConfigDir(t)
	migrateCmd := NewMigrateCommand()
	migrateCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "", viper.GetString(datastoreURIFlag))
		require.Equal(t, "", viper.GetString(datastoreUsernameFlag))
		require.Equal(t, "", viper.GetString(datastorePasswordFlag))
		require.Equal(t, uint(0), viper.GetUint(versionFlag))
		require.Equal(t, defaultDuration, viper.GetDuration(timeoutFlag))
		require.False(t, viper.GetBool(verboseMigrationFlag))
		return nil
	}

	cmd := cmd.NewRootCommand()
	cmd.AddCommand(migrateCmd)
	cmd.SetArgs([]string{"migrate"})
	require.NoError(t, cmd.Execute())
}

func TestMigrateCommandConfigFileValuesAreParsed(t *testing.T) {
	config := `datastore:
    engine: oneEngine
    uri: postgres://postgres:password@127.0.0.1:5432/postgres
`
	util.PrepareTempConfigFile(t, config)

	migrateCmd := NewMigrateCommand()
	migrateCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "oneEngine", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "postgres://postgres:password@127.0.0.1:5432/postgres", viper.GetString(datastoreURIFlag))
		require.Equal(t, uint(0), viper.GetUint(versionFlag))
		require.Equal(t, defaultDuration, viper.GetDuration(timeoutFlag))
		require.False(t, viper.GetBool(verboseMigrationFlag))
		return nil
	}

	cmd := cmd.NewRootCommand()
	cmd.AddCommand(migrateCmd)
	cmd.SetArgs([]string{"migrate"})
	require.NoError(t, cmd.Execute())
}

func TestMigrateCommandConfigIsMerged(t *testing.T) {
	config := `datastore:
    engine: randomEngine
`
	util.PrepareTempConfigFile(t, config)

	t.Setenv("OPENFGA_DATASTORE_URI", "postgres://postgres:PASS2@127.0.0.1:5432/postgres")
	t.Setenv("OPENFGA_VERBOSE", "true")

	migrateCmd := NewMigrateCommand()
	migrateCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "randomEngine", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "postgres://postgres:PASS2@127.0.0.1:5432/postgres", viper.GetString(datastoreURIFlag))
		require.Equal(t, uint(0), viper.GetUint(versionFlag))
		require.Equal(t, defaultDuration, viper.GetDuration(timeoutFlag))
		require.True(t, viper.GetBool(verboseMigrationFlag))
		return nil
	}

	cmd := cmd.NewRootCommand()
	cmd.AddCommand(migrateCmd)
	cmd.SetArgs([]string{"migrate"})
	require.NoError(t, cmd.Execute())
}
