package migrate

import (
	"github.com/openfga/openfga/cmd/util"
	"github.com/spf13/cobra"
)

// bindRunFlags binds the cobra cmd flags to the equivalent config value being managed
// by viper. This bridges the config between cobra flags and viper flags.
func bindRunFlags(command *cobra.Command, _ []string) {
	flags := command.Flags()

	util.MustBindPFlag(datastoreEngineFlag, flags.Lookup(datastoreEngineFlag))
	util.MustBindPFlag(datastoreURIFlag, flags.Lookup(datastoreURIFlag))
	util.MustBindPFlag(versionFlag, flags.Lookup(versionFlag))
	util.MustBindPFlag(timeoutFlag, flags.Lookup(timeoutFlag))
	util.MustBindPFlag(verboseMigrationFlag, flags.Lookup(verboseMigrationFlag))
}
