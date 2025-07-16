package migrate

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/openfga/openfga/cmd/util"
)

// bindRunFlagsFunc binds the cobra cmd flags to the equivalent config value being managed
// by viper. This bridges the config between cobra flags and viper flags.
func bindRunFlagsFunc(flags *pflag.FlagSet) func(*cobra.Command, []string) {
	return func(cmd *cobra.Command, args []string) {
		util.MustBindPFlag(datastoreEngineFlag, flags.Lookup(datastoreEngineFlag))
		util.MustBindEnv(datastoreEngineFlag, "OPENFGA_DATASTORE_ENGINE")

		util.MustBindPFlag(datastoreURIFlag, flags.Lookup(datastoreURIFlag))
		util.MustBindEnv(datastoreURIFlag, "OPENFGA_DATASTORE_URI")

		util.MustBindPFlag(datastoreUsernameFlag, flags.Lookup(datastoreUsernameFlag))
		util.MustBindEnv(datastoreUsernameFlag, "OPENFGA_DATASTORE_USERNAME")

		util.MustBindPFlag(datastorePasswordFlag, flags.Lookup(datastorePasswordFlag))
		util.MustBindEnv(datastorePasswordFlag, "OPENFGA_DATASTORE_PASSWORD")

		util.MustBindPFlag(versionFlag, flags.Lookup(versionFlag))

		util.MustBindPFlag(timeoutFlag, flags.Lookup(timeoutFlag))
		util.MustBindEnv(timeoutFlag, "OPENFGA_TIMEOUT")

		util.MustBindPFlag(verboseMigrationFlag, flags.Lookup(verboseMigrationFlag))
		util.MustBindEnv(verboseMigrationFlag, "OPENFGA_VERBOSE")

		util.MustBindPFlag(logFormatFlag, flags.Lookup(logFormatFlag))
		util.MustBindEnv(logFormatFlag, "OPENFGA_LOG_FORMAT")

		util.MustBindPFlag(logLevelFlag, flags.Lookup(logLevelFlag))
		util.MustBindEnv(logLevelFlag, "OPENFGA_LOG_LEVEL")

		util.MustBindPFlag(logTimestampFormatFlag, flags.Lookup(logTimestampFormatFlag))
		util.MustBindEnv(logTimestampFormatFlag, "OPENFGA_LOG_TIMESTAMP_FORMAT")
	}
}
