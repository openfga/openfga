package postgres

import (
	"context"
	"fmt"

	"github.com/awslabs/aurora-dsql-connectors/go/pgx/dsql"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openfga/openfga/pkg/storage/sqlcommon"
)

// initDSQLDB initializes a new Aurora DSQL database connection.
// DSQL uses IAM authentication which the connector handles automatically.
func initDSQLDB(uri string, cfg *sqlcommon.Config) (*pgxpool.Pool, error) {
	dsqlCfg, err := dsql.ParseConnectionString(uri)
	if err != nil {
		return nil, fmt.Errorf("parse DSQL URI: %w", err)
	}

	// Override username from config (DSQL uses IAM tokens, not passwords).
	if cfg.Username != "" {
		dsqlCfg.User = cfg.Username
	}

	// Apply OpenFGA pool settings via pgxpool.Config.
	poolCfg, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, fmt.Errorf("parse pgx pool config: %w", err)
	}
	if cfg.MaxOpenConns != 0 {
		poolCfg.MaxConns = int32(cfg.MaxOpenConns)
	}
	if cfg.MinOpenConns != 0 {
		poolCfg.MinConns = int32(cfg.MinOpenConns)
	}
	if cfg.ConnMaxLifetime != 0 {
		poolCfg.MaxConnLifetime = cfg.ConnMaxLifetime
	}
	if cfg.ConnMaxIdleTime != 0 {
		poolCfg.MaxConnIdleTime = cfg.ConnMaxIdleTime
	}

	return dsql.NewPool(context.Background(), dsqlCfg, poolCfg)
}
