package cmd

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

const defaultDuration = 1 * time.Minute

func prepareTempConfigDir(t *testing.T) string {
	_, err := os.Stat("/etc/openfga/config.yaml")
	require.ErrorIs(t, err, os.ErrNotExist, "Config file at /etc/openfga/config.yaml would disturb test result.")

	homedir := t.TempDir()
	t.Setenv("HOME", homedir)

	confdir := filepath.Join(homedir, ".openfga")
	require.Nil(t, os.Mkdir(confdir, 0750))

	return confdir
}

func prepareTempConfigFile(t *testing.T, config string) {
	confdir := prepareTempConfigDir(t)
	confFile, err := os.Create(filepath.Join(confdir, "config.yaml"))
	require.Nil(t, err)
	_, err = confFile.WriteString(config)
	require.Nil(t, err)
	require.Nil(t, confFile.Close())
}

func TestNoConfigDefaultValues(t *testing.T) {
	prepareTempConfigDir(t)
	migrateCmd := NewMigrateCommand()
	migrateCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "", viper.GetString(datastoreURIFlag))
		require.Equal(t, uint(0), viper.GetUint(versionFlag))
		require.Equal(t, defaultDuration, viper.GetDuration(timeoutFlag))
		return nil
	}
	require.Nil(t, migrateCmd.Execute())
}

func TestConfigFileValuesAreParsed(t *testing.T) {
	config := `datastore:
    engine: postgres
    uri: postgres://postgres:password@127.0.0.1:5432/postgres
`
	prepareTempConfigFile(t, config)

	migrateCmd := NewMigrateCommand()
	migrateCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "postgres", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "postgres://postgres:password@127.0.0.1:5432/postgres", viper.GetString(datastoreURIFlag))
		require.Equal(t, uint(0), viper.GetUint(versionFlag))
		require.Equal(t, defaultDuration, viper.GetDuration(timeoutFlag))
		return nil
	}
	require.Nil(t, migrateCmd.Execute())
}

func TestConfigIsMerged(t *testing.T) {
	config := `datastore:
    engine: postgres
`
	prepareTempConfigFile(t, config)

	t.Setenv("OPENFGA_DATASTORE_URI", "postgres://postgres:PASS2@127.0.0.1:5432/postgres")

	migrateCmd := NewMigrateCommand()
	migrateCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "postgres", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "postgres://postgres:PASS2@127.0.0.1:5432/postgres", viper.GetString(datastoreURIFlag))
		require.Equal(t, uint(0), viper.GetUint(versionFlag))
		require.Equal(t, defaultDuration, viper.GetDuration(timeoutFlag))
		return nil
	}
	require.Nil(t, migrateCmd.Execute())
}

func TestDatastoreURIIsVerified(t *testing.T) {
	config := `datastore:
    engine: mysqlx
    uri: postgres://postgres:password@127.0.0.1:5432/postgres
`
	prepareTempConfigFile(t, config)

	migrateCmd := NewMigrateCommand()
	migrateCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "mysqlx", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "postgres://postgres:password@127.0.0.1:5432/postgres", viper.GetString(datastoreURIFlag))
		require.Equal(t, uint(0), viper.GetUint(versionFlag))
		require.Equal(t, defaultDuration, viper.GetDuration(timeoutFlag))
		return nil
	}
	require.Nil(t, migrateCmd.Execute())
}
