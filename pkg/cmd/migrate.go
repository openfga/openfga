package cmd

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/openfga/openfga/assets"
	cmdutil "github.com/openfga/openfga/pkg/cmd/util"
	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
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

	cmd.Flags().String(datastoreEngineFlag, "", "(required) the database engine to run the migrations for")
	cmdutil.MustMarkFlagRequired(cmd, datastoreEngineFlag)

	cmd.Flags().String(datastoreURIFlag, "", "(required) the connection uri of the database to run the migrations against (e.g. 'postgres://postgres:password@localhost:5432/postgres')")
	cmdutil.MustMarkFlagRequired(cmd, datastoreURIFlag)

	cmd.Flags().Uint(versionFlag, 0, `the version to migrate to. If omitted, the latest version of the schema will be used`)

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

	version, err := cmd.Flags().GetUint(versionFlag)
	if err != nil {
		return err
	}

	switch engine {
	case "postgres":
		db, err := sql.Open("pgx", uri)
		if err != nil {
			log.Fatal("failed to parse the config from the connection uri", err)
		}

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
		}

		if err := goose.Up(db, assets.PostgresMigrationDir); err != nil {
			log.Fatal(err)
		}

		return nil
	default:
		return fmt.Errorf("unknown datastore engine type: %s", engine)
	}
}
