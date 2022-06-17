package cmd

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/spf13/cobra"
)

const (
	datastoreEngineFlagName = "datastore-engine"
	datastoreURIFlagName    = "datastore-uri"
)

func NewMigrateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate [revision]",
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
	engine, err := cmd.Flags().GetString(datastoreEngineFlagName)
	if err != nil {
		return err
	}

	var steps int
	if len(args) > 0 {
		steps, err = strconv.Atoi(args[0])
		if err != nil {
			return errors.New("revision must be an integer")
		}
	}

	if engine == "postgres" {
		uri, err := cmd.Flags().GetString(datastoreURIFlagName)
		if err != nil || uri == "" {
			return errors.New("a datastore uri is required to be specified for the postgres datastore option")
		}

		m, err := migrate.New(
			"file://storage/postgres/migrations",
			uri)
		if err != nil {
			return fmt.Errorf("error applying migration: %w", err)
		}

		if steps > 0 {
			return m.Steps(steps)
		}

		return m.Up()
	}

	return fmt.Errorf("unable to run migrations for datastore engine type: %s", engine)
}
