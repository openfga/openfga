package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/cenkalti/backoff/v4"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/cmd/util"
	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	versionFlag          = "version"
	timeoutFlag          = "timeout"
	verboseMigrationFlag = "verbose"
)

func NewMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Run database schema migrations needed for the OpenFGA server",
		Long:  `The migrate command is used to migrate the database schema needed for OpenFGA.`,
		RunE:  runMigration,
		Args:  cobra.NoArgs,
		PreRun: func(cmd *cobra.Command, args []string) {
			flags := cmd.Flags()

			util.MustBindPFlag(datastoreEngineFlag, flags.Lookup(datastoreEngineFlag))
			util.MustBindPFlag(datastoreURIFlag, flags.Lookup(datastoreURIFlag))
			util.MustBindPFlag(versionFlag, flags.Lookup(versionFlag))
			util.MustBindPFlag(timeoutFlag, flags.Lookup(timeoutFlag))
			util.MustBindPFlag(verboseMigrationFlag, flags.Lookup(verboseMigrationFlag))
		},
	}

	flags := cmd.Flags()

	flags.String(datastoreEngineFlag, "", "(required) the datastore engine that will be used for persistence")
	flags.String(datastoreURIFlag, "", "(required) the connection uri of the database to run the migrations against (e.g. 'postgres://postgres:password@localhost:5432/postgres')")
	flags.Uint(versionFlag, 0, "the version to migrate to (if omitted the latest schema will be used)")
	flags.Duration(timeoutFlag, 1*time.Minute, "a timeout after which the migration process will terminate")
	flags.Bool(verboseMigrationFlag, false, "enable verbose migration logs (default false)")

	// NOTE: if you add a new flag here, add the binding in PreRunE

	return cmd
}

func runMigration(_ *cobra.Command, _ []string) error {
	engine := viper.GetString(datastoreEngineFlag)
	uri := viper.GetString(datastoreURIFlag)
	targetVersion := viper.GetUint(versionFlag)
	timeout := viper.GetDuration(timeoutFlag)
	verbose := viper.GetBool(verboseMigrationFlag)

	goose.SetLogger(goose.NopLogger())
	goose.SetVerbose(verbose)

	var driver, dialect, migrationsPath string
	switch engine {
	case "memory":
		log.Println("no migrations to run for `memory` datastore")
		return nil
	case "mysql":
		driver = "mysql"
		dialect = "mysql"
		migrationsPath = assets.MySQLMigrationDir
	case "postgres":
		driver = "pgx"
		dialect = "postgres"
		migrationsPath = assets.PostgresMigrationDir
	case "":
		return fmt.Errorf("missing datastore engine type")
	default:
		return fmt.Errorf("unknown datastore engine type: %s", engine)
	}

	db, err := sql.Open(driver, uri)
	if err != nil {
		log.Fatalf("failed to open a connection to the datastore: %v", err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			log.Fatalf("failed to close the datastore: %v", err)
		}
	}()

	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = timeout
	err = backoff.Retry(func() error {
		err = db.PingContext(context.Background())
		if err != nil {
			return err
		}

		return nil
	}, policy)
	if err != nil {
		log.Fatalf("failed to initialize database connection: %v", err)
	}

	if err := goose.SetDialect(dialect); err != nil {
		log.Fatalf("failed to initialize the migrate command: %v", err)
	}

	goose.SetBaseFS(assets.EmbedMigrations)

	currentVersion, err := goose.GetDBVersion(db)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("current version %d", currentVersion)

	if targetVersion == 0 {
		log.Println("running all migrations")
		if err := goose.Up(db, migrationsPath); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Printf("migrating to %d", targetVersion)
		targetInt64Version := int64(targetVersion)
		if targetInt64Version < currentVersion {
			if err := goose.DownTo(db, migrationsPath, targetInt64Version); err != nil {
				log.Fatal(err)
			}
		} else if targetInt64Version > currentVersion {
			if err := goose.UpTo(db, migrationsPath, targetInt64Version); err != nil {
				log.Fatal(err)
			}
		} else {
			log.Println("nothing to do")
			return nil
		}
	}

	log.Println("migration done")

	return nil
}
