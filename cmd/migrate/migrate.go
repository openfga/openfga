// Package migrate contains the command to perform database migrations.
package migrate

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v4"
	mysqldriver "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/openfga/openfga/pkg/storage/migrate"
	mysql "github.com/openfga/openfga/pkg/storage/mysql/migrations"
	postgres "github.com/openfga/openfga/pkg/storage/postgres/migrations"
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

func runMigration(cmd *cobra.Command, _ []string) error {
	var (
		driver     string
		migrations *migrate.Registry
	)

	ctx := cmd.Context()

	engine := viper.GetString(datastoreEngineFlag)
	uri := viper.GetString(datastoreURIFlag)
	targetVersion := viper.GetInt64(versionFlag)
	timeout := viper.GetDuration(timeoutFlag)
	verbose := viper.GetBool(verboseMigrationFlag)
	username := viper.GetString(datastoreUsernameFlag)
	password := viper.GetString(datastorePasswordFlag)

	switch engine {
	case "memory":
		log.Println("no migrations to run for `memory` datastore")
		return nil
	case "mysql":
		driver = "mysql"
		migrations = mysql.Migrations

		// Parse the database uri with the mysql drivers function for it and update username/password, if set via flags
		dsn, err := mysqldriver.ParseDSN(uri)
		if err != nil {
			return fmt.Errorf("invalid database uri: %v", err)
		}
		if username != "" {
			dsn.User = username
		}
		if password != "" {
			dsn.Passwd = password
		}

		uri = dsn.FormatDSN()

	case "postgres":
		driver = "pgx"
		migrations = postgres.Migrations

		// Parse the database uri with url.Parse() and update username/password, if set via flags
		dbURI, err := url.Parse(uri)
		if err != nil {
			return fmt.Errorf("invalid database uri: %v", err)
		}
		if username == "" && dbURI.User != nil {
			username = dbURI.User.Username()
		}
		if password == "" && dbURI.User != nil {
			password, _ = dbURI.User.Password()
		}
		dbURI.User = url.UserPassword(username, password)

		// Replace CLI uri with the one we just updated.
		uri = dbURI.String()
	case "":
		return fmt.Errorf("missing datastore engine type")
	default:
		return fmt.Errorf("unknown datastore engine type: %s", engine)
	}

	db, err := goose.OpenDBWithDriver(driver, uri)
	if err != nil {
		return fmt.Errorf("failed to open a connection to the datastore: %w", err)
	}
	defer db.Close()

	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = timeout
	err = backoff.Retry(func() error {
		return db.PingContext(context.Background())
	}, policy)
	if err != nil {
		return fmt.Errorf("failed to initialize database connection: %w", err)
	}

	_, err = migrations.Run(ctx, db,
		migrate.WithTargetVersion(targetVersion),
		migrate.WithVerbose(verbose),
	)
	if err != nil {
		return fmt.Errorf("failed to run migrations: %v", err)
	}

	log.Println("migration done")

	return nil
}
