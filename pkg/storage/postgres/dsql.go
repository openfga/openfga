package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws-samples/aurora-dsql-samples/go/dsql-pgx-connector/dsql"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openfga/openfga/pkg/storage/sqlcommon"
)

// isOCCError checks if the error is a DSQL optimistic concurrency control conflict.
// DSQL returns OC000 for mutation conflicts and OC001 for schema conflicts.
func isOCCError(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "OC000" || pgErr.Code == "OC001" || pgErr.Code == "40001"
	}
	return false
}

// withOCCRetry executes fn with automatic retry on DSQL OCC errors.
func withOCCRetry(ctx context.Context, fn func() error) error {
	policy := backoff.NewExponentialBackOff()
	policy.InitialInterval = 10 * time.Millisecond
	policy.MaxElapsedTime = 5 * time.Second

	return backoff.Retry(func() error {
		err := fn()
		if err == nil {
			return nil
		}
		if isOCCError(err) {
			return err
		}
		return backoff.Permanent(err)
	}, backoff.WithContext(policy, ctx))
}

// initDSQLDB initializes a new Aurora DSQL database connection.
// DSQL uses IAM authentication which the connector handles automatically.
func initDSQLDB(uri string, cfg *sqlcommon.Config) (*pgxpool.Pool, error) {
	dsqlCfg, err := dsql.ParseConnectionString(uri)
	if err != nil {
		return nil, fmt.Errorf("parse DSQL URI: %w", err)
	}

	// Apply OpenFGA pool settings
	if cfg.MaxOpenConns != 0 {
		dsqlCfg.MaxConns = int32(cfg.MaxOpenConns)
	}
	if cfg.MinOpenConns != 0 {
		dsqlCfg.MinConns = int32(cfg.MinOpenConns)
	}
	if cfg.ConnMaxLifetime != 0 {
		dsqlCfg.MaxConnLifetime = cfg.ConnMaxLifetime
	}
	if cfg.ConnMaxIdleTime != 0 {
		dsqlCfg.MaxConnIdleTime = cfg.ConnMaxIdleTime
	}

	pool, err := dsql.NewPool(context.Background(), dsqlCfg)
	if err != nil {
		return nil, fmt.Errorf("create DSQL pool: %w", err)
	}

	return pool.Pool, nil
}
