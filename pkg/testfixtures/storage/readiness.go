package storage

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// waitForDatabase attempts to establish a connection to the database and ping it until it's ready or a timeout occurs.
func waitForDatabase(driverName, uri string) error {
	db, err := sql.Open(driverName, uri)
	if err != nil {
		return fmt.Errorf("open connection to %s: %w", driverName, err)
	}
	defer db.Close()

	backoffPolicy := backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(30 * time.Second))
	if err := backoff.Retry(db.Ping, backoffPolicy); err != nil {
		return fmt.Errorf("ping %s database: %w", driverName, err)
	}

	return nil
}
