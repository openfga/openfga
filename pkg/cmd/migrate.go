package cmd

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/pressly/goose"
	"github.com/spf13/cobra"
)

const (
	datastoreEngineFlagName = "datastore-engine"
	datastoreURIFlagName    = "datastore-uri"
)

func NewMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate [flags] [revision]",
		Short: "Run database schema migrations needed for the OpenFGA server",
		Long:  `The migrate command is used to migrate up to a specific revision of the database schema needed for OpenFGA. If the revision is omitted the latest, or "HEAD", revision of the schema will be used.`,
		Args:  cobra.MaximumNArgs(1),
		RunE:  runMigration,
	}

	cmd.Flags().String(datastoreEngineFlagName, "", "the database engine to run the migrations for: postgres")
	cmd.Flags().String(datastoreURIFlagName, "", "the connection uri of the database to run the migrations against (e.g. 'postgres://postgres:password@localhost:5432/postgres')")

	return cmd
}

func runMigration(cmd *cobra.Command, args []string) error {
	const dir = "storage/postgres/migrations"

	engine, err := cmd.Flags().GetString(datastoreEngineFlagName)
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

	if engine == "postgres" {
		uri, err := cmd.Flags().GetString(datastoreURIFlagName)
		if err != nil || uri == "" {
			return errors.New("a datastore uri is required to be specified for the postgres datastore option")
		}

		db, err := sql.Open("pgx", uri)
		if err != nil {
			return fmt.Errorf("failed to parse config from uri: %v", err)
		}

		if err := goose.SetDialect("postgres"); err != nil {
			panic(err)
		}

		if steps > 0 {
			return goose.UpTo(db, dir, steps)
		}

		return goose.Up(db, dir)
	}

	return fmt.Errorf("unable to run migrations for datastore engine type: %s", engine)
}
