package migrate

import (
	"fmt"

	"github.com/pressly/goose/v3"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/internal/dsql"
	"github.com/openfga/openfga/pkg/logger"
)

// dsqlMigrationConfig holds DSQL-specific migration configuration.
type dsqlMigrationConfig struct {
	driver         string
	migrationsPath string
	uri            string
}

// prepareDSQLMigration prepares the migration configuration for DSQL.
func prepareDSQLMigration(uri string, log logger.Logger) (*dsqlMigrationConfig, error) {
	pgURI, err := dsql.PreparePostgresURI(uri)
	if err != nil {
		return nil, fmt.Errorf("prepare DSQL URI: %w", err)
	}

	if err := ensureGooseTableForDSQL(pgURI, log); err != nil {
		return nil, fmt.Errorf("create goose version table: %w", err)
	}

	log.Info("using DSQL datastore with IAM authentication")

	return &dsqlMigrationConfig{
		driver:         "pgx",
		migrationsPath: assets.DSQLMigrationDir,
		uri:            pgURI,
	}, nil
}

// ensureGooseTableForDSQL creates the goose_db_version table if it doesn't exist.
func ensureGooseTableForDSQL(uri string, log logger.Logger) error {
	db, err := goose.OpenDBWithDriver("pgx", uri)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}
	defer db.Close()

	if err := dsql.EnsureGooseTable(db); err != nil {
		return err
	}

	log.Info("ensured goose_db_version table exists for DSQL")
	return nil
}
