package validatemodels

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/openfga/openfga/cmd/util"
)

// bindRunFlags binds the cobra cmd flags to the equivalent config value being managed
// by viper. This bridges the config between cobra flags and viper flags.
func bindRunFlagsFunc(flags *pflag.FlagSet) func(*cobra.Command, []string) {
	return func(cmd *cobra.Command, args []string) {
		util.MustBindPFlag(datastoreEngineFlag, flags.Lookup(datastoreEngineFlag))
		util.MustBindPFlag(datastoreURIFlag, flags.Lookup(datastoreURIFlag))
	}
}
