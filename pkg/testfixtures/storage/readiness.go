package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pressly/goose/v3"
)

// waitForDatabase attempts to establish a connection to the database
// and ping it until it's ready or a timeout occurs.
func waitForDatabase(driverName, uri string) error { //nolint:unparam
	backoffPolicy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
		backoff.WithMaxElapsedTime(60*time.Second),
	)

	err := backoff.Retry(func() error {
		db, err := sql.Open(driverName, uri)
		if err != nil {
			return fmt.Errorf("open connection to %s: %w", driverName, err)
		}
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		return db.PingContext(ctx)
	}, backoffPolicy)

	if err != nil {
		return fmt.Errorf("ping %s database: %w", driverName, err)
	}

	return nil
}

// waitForMigrationVersion attempts to read the database migration version
// until it matches the expected version or a timeout occurs.
func waitForMigrationVersion(driverName, uri string, expectedVersion int64) error {
	backoffPolicy := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
		backoff.WithMaxElapsedTime(60*time.Second),
	)

	err := backoff.Retry(func() error {
		db, err := goose.OpenDBWithDriver(driverName, uri)
		if err != nil {
			return fmt.Errorf("open connection to %s: %w", driverName, err)
		}
		defer db.Close()

		version, err := goose.GetDBVersion(db)
		if err != nil {
			return fmt.Errorf("get database version: %w", err)
		}

		if version != expectedVersion {
			return fmt.Errorf("database version %d does not match expected version %d", version, expectedVersion)
		}

		return nil
	}, backoffPolicy)

	if err != nil {
		return fmt.Errorf("%s migration version: %w", driverName, err)
	}

	return nil
}
