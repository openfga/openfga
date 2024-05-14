// Package cmd contains all the commands included in the binary file.
package cmd

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	datastoreEngineFlag = "datastore-engine"
	datastoreEngineConf = "datastore.engine"
	datastoreURIFlag    = "datastore-uri"
	datastoreURIConf    = "datastore.uri"
)

// NewRootCommand enables all children commands to read flags from CLI flags, environment variables prefixed with OPENFGA, or config.yaml (in that order).
func NewRootCommand() *cobra.Command {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	viper.SetEnvPrefix("OPENFGA")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	configPaths := []string{"/etc/openfga", "$HOME/.openfga", "."}
	for _, path := range configPaths {
		viper.AddConfigPath(path)
	}

	viper.SetDefault(datastoreEngineFlag, "")
	viper.SetDefault(datastoreURIFlag, "")
	err := viper.ReadInConfig()
	if err == nil {
		viper.SetDefault(datastoreEngineFlag, viper.Get(datastoreEngineConf))
		viper.SetDefault(datastoreURIFlag, viper.Get(datastoreURIConf))
	}

	return &cobra.Command{
		Use:   "openfga",
		Short: "A high performance and flexible authorization/permission engine built for developers and inspired by Google Zanzibar",
		Long: `A high performance and flexible authorization/permission engine built for developers and inspired by Google Zanzibar.

OpenFGA is designed to make it easy for developers to model their application permissions, and to add and integrate fine-grained authorization into their applications.
Complete documentation is available at https://openfga.dev.`,
	}
}
