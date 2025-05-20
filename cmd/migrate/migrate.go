// Package migrate contains the command to perform database migrations.
package migrate

import (
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver.
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/openfga/openfga/pkg/storage/migrate"
)

const (
	datastoreEngineFlag   = "datastore-engine"
	datastoreURIFlag      = "datastore-uri"
	datastoreUsernameFlag = "datastore-username"
	datastorePasswordFlag = "datastore-password"
	versionFlag           = "version"
	timeoutFlag           = "timeout"
	verboseMigrationFlag  = "verbose"
)

func NewMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Run database schema migrations needed for the OpenFGA server",
		Long:  `The migrate command is used to migrate the database schema needed for OpenFGA.`,
		RunE:  runMigration,
		Args:  cobra.NoArgs,
	}

	flags := cmd.Flags()

	flags.String(datastoreEngineFlag, "", "(required) the datastore engine that will be used for persistence")
	flags.String(datastoreURIFlag, "", "(required) the connection uri of the database to run the migrations against (e.g. 'postgres://postgres:password@localhost:5432/postgres')")
	flags.String(datastoreUsernameFlag, "", "(optional) overwrite the username in the connection string")
	flags.String(datastorePasswordFlag, "", "(optional) overwrite the password in the connection string")
	flags.Uint(versionFlag, 0, "the version to migrate to (if omitted the latest schema will be used)")
	flags.Duration(timeoutFlag, 1*time.Minute, "a timeout for the time it takes the migrate process to connect to the database")
	flags.Bool(verboseMigrationFlag, false, "enable verbose migration logs (default false)")

	// NOTE: if you add a new flag here, update the function below, too

	cmd.PreRun = bindRunFlagsFunc(flags)

	return cmd
}

func runMigration(_ *cobra.Command, _ []string) error {
	engine := viper.GetString(datastoreEngineFlag)
	uri := viper.GetString(datastoreURIFlag)
	targetVersion := viper.GetUint(versionFlag)
	timeout := viper.GetDuration(timeoutFlag)
	verbose := viper.GetBool(verboseMigrationFlag)
	username := viper.GetString(datastoreUsernameFlag)
	password := viper.GetString(datastorePasswordFlag)

	cfg := migrate.MigrationConfig{
		Engine:        engine,
		URI:           uri,
		TargetVersion: targetVersion,
		Timeout:       timeout,
		Verbose:       verbose,
		Username:      username,
		Password:      password,
	}
	return migrate.RunMigrations(cfg)
}
