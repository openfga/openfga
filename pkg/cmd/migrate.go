package cmd

import (
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/openfga/openfga/assets"
	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
)

const (
	datastoreEngineFlag = "datastore-engine"
	datastoreURIFlag    = "datastore-uri"
	versionFlag         = "version"

	postgresMigrationsDir = "migrations/postgres"
)

func NewMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Run database schema migrations needed for the OpenFGA server",
		Long:  `The migrate command is used to migrate the database schema needed for OpenFGA.`,
		RunE:  runMigration,
	}

	cmd.Flags().String(datastoreEngineFlag, "", "(required) the database engine to run the migrations for")
	if err := cmd.MarkFlagRequired(datastoreEngineFlag); err != nil {
		panic(err)
	}

	cmd.Flags().String(datastoreURIFlag, "", "(required) the connection uri of the database to run the migrations against (e.g. 'postgres://postgres:password@localhost:5432/postgres')")
	if err := cmd.MarkFlagRequired(datastoreURIFlag); err != nil {
		panic(err)
	}

	cmd.Flags().Int64(versionFlag, -1, `the version to migrate to. If omitted, the latest version of the schema will be used`)

	return cmd
}

func runMigration(cmd *cobra.Command, _ []string) error {
	engine, err := cmd.Flags().GetString(datastoreEngineFlag)
	if err != nil {
		return err
	}

	uri, err := cmd.Flags().GetString(datastoreURIFlag)
	if err != nil {
		return err
	}

	version, err := cmd.Flags().GetInt64(versionFlag)
	if err != nil {
		return err
	}

	switch engine {
	case "postgres":
		db, err := sql.Open("pgx", uri)
		if err != nil {
			return fmt.Errorf("failed to parse config from uri: %w", err)
		}

		if err := goose.SetDialect("postgres"); err != nil {
			return fmt.Errorf("failed to initialize the migrate command: %w", err)
		}

		goose.SetBaseFS(assets.EmbedMigrations)

		if version >= 0 {
			currentVersion, err := goose.GetDBVersion(db)
			if err != nil {
				return err
			}

			if version < currentVersion {
				return goose.DownTo(db, assets.PostgresMigrationDir, version)
			}

			return goose.UpTo(db, assets.PostgresMigrationDir, version)
		}

		return goose.Up(db, assets.PostgresMigrationDir)
	default:
		return fmt.Errorf("unable to run migrations for datastore engine type: %s", engine)
	}
}
