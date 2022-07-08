package cmd

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/openfga/openfga/assets"
	"github.com/pressly/goose/v3"
	"github.com/spf13/cobra"
)

const (
	databaseEngineFlagName = "database-engine"
	databaseURIFlagName    = "database-uri"
)

func NewMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate [flags] [revision]",
		Short: "Run database schema migrations needed for the OpenFGA server",
		Long:  `The migrate command is used to migrate up to a specific revision of the database schema needed for OpenFGA. If the revision is omitted the latest, or "HEAD", revision of the schema will be used.`,
		Args:  cobra.MaximumNArgs(1),
		RunE:  runMigration,
	}

	cmd.Flags().String(databaseEngineFlagName, "", "the database engine to run the migrations for")
	cmd.Flags().String(databaseURIFlagName, "", "the connection uri of the database to run the migrations against (e.g. 'postgres://postgres:password@localhost:5432/postgres')")

	return cmd
}

func runMigration(cmd *cobra.Command, args []string) error {

	engine, err := cmd.Flags().GetString(databaseEngineFlagName)
	if err != nil {
		return err
	}

	var steps int64
	if len(args) > 0 {
		steps, err = strconv.ParseInt(args[0], 10, 64)
		if err != nil {
			return errors.New("revision must be an integer")
		}
	}

	switch engine {
	case "postgres":
		uri, err := cmd.Flags().GetString(databaseURIFlagName)
		if err != nil || uri == "" {
			return errors.New("a datastore uri is required to be specified for the postgres datastore option")
		}

		db, err := sql.Open("pgx", uri)
		if err != nil {
			return fmt.Errorf("failed to parse config from uri: %w", err)
		}

		goose.SetBaseFS(assets.EmbedMigrations)

		if err := goose.SetDialect("postgres"); err != nil {
			return fmt.Errorf("failed to initialize migrate command: %w", err)
		}

		if steps > 0 {
			return goose.UpTo(db, "migrations/postgres", steps)
		}

		return goose.Up(db, "migrations/postgres")
	default:
		return fmt.Errorf("unable to run migrations for datastore engine type: %s", engine)
	}
}
