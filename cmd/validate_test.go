package cmd

import (
	"testing"

	"github.com/openfga/openfga/cmd/util"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestValidateModelsCommandWhenInvalidEngine(t *testing.T) {
	for _, tc := range []struct {
		engine        string
		errorExpected string
	}{
		{
			engine:        "memory",
			errorExpected: "invalid datastore engine type",
		},
		{
			engine:        "",
			errorExpected: "missing datastore engine type",
		},
	} {
		t.Run(tc.engine, func(t *testing.T) {
			validateModelsCommand := NewValidateCommand()
			validateModelsCommand.SetArgs([]string{"--datastore-engine", tc.engine, "--datastore-uri", ""})
			err := validateModelsCommand.Execute()
			require.ErrorContains(t, err, tc.errorExpected)
		})
	}
}

func TestValidateModelsCommandNoConfigDefaultValues(t *testing.T) {
	util.PrepareTempConfigDir(t)
	validateCommand := NewValidateCommand()
	validateCommand.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "", viper.GetString(datastoreURIFlag))
		return nil
	}

	cmd := NewRootCommand()
	cmd.AddCommand(validateCommand)
	cmd.SetArgs([]string{"validate-models"})
	require.Nil(t, cmd.Execute())
}

func TestValidateModelsCommandConfigFileValuesAreParsed(t *testing.T) {
	config := `datastore:
    engine: someEngine
    uri: postgres://postgres:password@127.0.0.1:5432/postgres
`
	util.PrepareTempConfigFile(t, config)

	validateCmd := NewValidateCommand()
	validateCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "someEngine", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "postgres://postgres:password@127.0.0.1:5432/postgres", viper.GetString(datastoreURIFlag))
		return nil
	}

	cmd := NewRootCommand()
	cmd.AddCommand(validateCmd)
	cmd.SetArgs([]string{"validate-models"})
	require.Nil(t, cmd.Execute())
}

func TestValidateModelsCommandConfigIsMerged(t *testing.T) {
	config := `datastore:
    engine: anotherEngine
`
	util.PrepareTempConfigFile(t, config)

	t.Setenv("OPENFGA_DATASTORE_URI", "postgres://postgres:PASS2@127.0.0.1:5432/postgres")

	validateCmd := NewValidateCommand()
	validateCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "anotherEngine", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "postgres://postgres:PASS2@127.0.0.1:5432/postgres", viper.GetString(datastoreURIFlag))
		return nil
	}

	cmd := NewRootCommand()
	cmd.AddCommand(validateCmd)
	cmd.SetArgs([]string{"validate-models"})
	require.Nil(t, cmd.Execute())
}
