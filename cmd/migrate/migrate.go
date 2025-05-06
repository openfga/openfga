// Package migrate contains the command to perform database migrations.
package migrate

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver.
	// MSSQL driver.
	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/storage/sqlite"
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

	goose.SetLogger(goose.NopLogger())
	goose.SetVerbose(verbose)

	var driver, migrationsPath string
	switch engine {
	case "memory":
		log.Println("no migrations to run for `memory` datastore")
		return nil
	case "mysql":
		driver = "mysql"
		migrationsPath = assets.MySQLMigrationDir

		// Parse the database uri with the mysql drivers function for it and update username/password, if set via flags
		dsn, err := mysql.ParseDSN(uri)
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
		migrationsPath = assets.PostgresMigrationDir

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
	case "sqlite":
		driver = "sqlite"
		migrationsPath = assets.SqliteMigrationDir

		var err error
		uri, err = sqlite.PrepareDSN(uri)
		if err != nil {
			return err
		}
	case "mssql":
		driver = "mssql"
		migrationsPath = assets.MSSQLMigrationDir

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

	goose.SetBaseFS(assets.EmbedMigrations)

	currentVersion, err := goose.GetDBVersion(db)
	if err != nil {
		return fmt.Errorf("failed to get db version: %w", err)
	}

	log.Printf("current version %d", currentVersion)

	if targetVersion == 0 {
		log.Println("running all migrations")
		if err := goose.Up(db, migrationsPath); err != nil {
			return fmt.Errorf("failed to run migrations: %w", err)
		}
		log.Println("migration done")
		return nil
	}

	log.Printf("migrating to %d", targetVersion)
	targetInt64Version := int64(targetVersion)

	switch {
	case targetInt64Version < currentVersion:
		if err := goose.DownTo(db, migrationsPath, targetInt64Version); err != nil {
			return fmt.Errorf("failed to run migrations down to %v: %w", targetInt64Version, err)
		}
	case targetInt64Version > currentVersion:
		if err := goose.UpTo(db, migrationsPath, targetInt64Version); err != nil {
			return fmt.Errorf("failed to run migrations up to %v: %w", targetInt64Version, err)
		}
	default:
		log.Println("nothing to do")
		return nil
	}

	log.Println("migration done")
	return nil
}
