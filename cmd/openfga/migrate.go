package main

import (
	"database/sql"
	"log"
	"strings"

	"github.com/go-errors/errors"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/openfga/openfga/assets"
	cmdutil "github.com/openfga/openfga/cmd/openfga/util"
	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	datastoreEngineFlag = "datastore-engine"
	datastoreURIFlag    = "datastore-uri"
	versionFlag         = "version"
)

func NewMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Run database schema migrations needed for the OpenFGA server",
		Long:  `The migrate command is used to migrate the database schema needed for OpenFGA.`,
		RunE:  runMigration,
	}

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	configPaths := []string{"/etc/openfga", "$HOME/.openfga", "."}
	for _, path := range configPaths {
		viper.AddConfigPath(path)
	}

	viper.SetEnvPrefix("OPENFGA")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	bindMigrateFlags(cmd)

	return cmd
}

func runMigration(_ *cobra.Command, _ []string) error {
	engine := viper.GetString(datastoreEngineFlag)
	uri := viper.GetString(datastoreURIFlag)
	version := viper.GetUint(versionFlag)

	goose.SetLogger(goose.NopLogger())

	switch engine {
	case "memory":
		return nil
	case "mysql":
		db, err := sql.Open("mysql", uri)
		if err != nil {
			log.Fatal("failed to parse the config from the connection uri", err)
		}

		defer func() {
			if err := db.Close(); err != nil {
				log.Fatal("failed to close the db", err)
			}
		}()

		if err := goose.SetDialect("mysql"); err != nil {
			log.Fatal("failed to initialize the migrate command", err)
		}

		goose.SetBaseFS(assets.EmbedMigrations)

		if version > 0 {
			currentVersion, err := goose.GetDBVersion(db)
			if err != nil {
				log.Fatal(err)
			}

			int64Version := int64(version)
			if int64Version < currentVersion {
				if err := goose.DownTo(db, assets.MySQLMigrationDir, int64Version); err != nil {
					log.Fatal(err)
				}
			}

			if err := goose.UpTo(db, assets.MySQLMigrationDir, int64Version); err != nil {
				log.Fatal(err)
			}
		}

		if err := goose.Up(db, assets.MySQLMigrationDir); err != nil {
			log.Fatal(err)
		}

		return nil
	case "postgres":
		db, err := sql.Open("pgx", uri)
		if err != nil {
			log.Fatal("failed to parse the config from the connection uri", err)
		}

		defer func() {
			if err := db.Close(); err != nil {
				log.Fatal("failed to close the db", err)
			}
		}()

		if err := goose.SetDialect("postgres"); err != nil {
			log.Fatal("failed to initialize the migrate command", err)
		}

		goose.SetBaseFS(assets.EmbedMigrations)

		if version > 0 {
			currentVersion, err := goose.GetDBVersion(db)
			if err != nil {
				log.Fatal(err)
			}

			int64Version := int64(version)
			if int64Version < currentVersion {
				if err := goose.DownTo(db, assets.PostgresMigrationDir, int64Version); err != nil {
					log.Fatal(err)
				}
			}

			if err := goose.UpTo(db, assets.PostgresMigrationDir, int64Version); err != nil {
				log.Fatal(err)
			}

			return nil
		}

		if err := goose.Up(db, assets.PostgresMigrationDir); err != nil {
			log.Fatal(err)
		}

		return nil
	default:
		return errors.Errorf("unknown datastore engine type: %s", engine)
	}
}

func bindMigrateFlags(cmd *cobra.Command) {
	flags := cmd.Flags()

	flags.String(datastoreEngineFlag, "", "(required) the datastore engine that will be used for persistence")
	cmdutil.MustBindPFlag(datastoreEngineFlag, flags.Lookup(datastoreEngineFlag))

	flags.String(datastoreURIFlag, "", "(required) the connection uri of the database to run the migrations against (e.g. 'postgres://postgres:password@localhost:5432/postgres')")
	cmdutil.MustBindPFlag(datastoreURIFlag, flags.Lookup(datastoreURIFlag))

	flags.Uint(versionFlag, 0, "`the version to migrate to. If omitted, the latest version of the schema will be used`")
	cmdutil.MustBindPFlag("version", flags.Lookup(versionFlag))
}
