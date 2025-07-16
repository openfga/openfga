package migrate

import (
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/cmd/util"
)

const defaultDuration = 1 * time.Minute

func TestMigrateCommandNoConfigDefaultValues(t *testing.T) {
	util.PrepareTempConfigDir(t)
	migrateCmd := NewMigrateCommand()
	migrateCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Empty(t, viper.GetString(datastoreEngineFlag))
		require.Empty(t, viper.GetString(datastoreURIFlag))
		require.Empty(t, viper.GetString(datastoreUsernameFlag))
		require.Empty(t, viper.GetString(datastorePasswordFlag))
		require.Equal(t, uint(0), viper.GetUint(versionFlag))
		require.Equal(t, defaultDuration, viper.GetDuration(timeoutFlag))
		require.False(t, viper.GetBool(verboseMigrationFlag))
		require.Equal(t, "text", viper.GetString(logFormatFlag))
		require.Equal(t, "info", viper.GetString(logLevelFlag))
		require.Equal(t, "Unix", viper.GetString(logTimestampFormatFlag))
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
func TestMigrateCommandLoggingConfigurationFromEnv(t *testing.T) {
	util.PrepareTempConfigDir(t)
	
	t.Setenv("OPENFGA_LOG_FORMAT", "json")
	t.Setenv("OPENFGA_LOG_LEVEL", "debug")
	t.Setenv("OPENFGA_LOG_TIMESTAMP_FORMAT", "ISO8601")

	migrateCmd := NewMigrateCommand()
	migrateCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "json", viper.GetString(logFormatFlag))
		require.Equal(t, "debug", viper.GetString(logLevelFlag))
		require.Equal(t, "ISO8601", viper.GetString(logTimestampFormatFlag))
		return nil
	}

	cmd := cmd.NewRootCommand()
	cmd.AddCommand(migrateCmd)
	cmd.SetArgs([]string{"migrate"})
	require.NoError(t, cmd.Execute())
}
