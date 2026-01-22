// Package dsql provides shared utilities for Aurora DSQL connections.
package dsql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws-samples/aurora-dsql-samples/go/dsql-pgx-connector/dsql"
)

// PreparePostgresURI converts a dsql:// URI to a postgres:// URI with IAM authentication.
func PreparePostgresURI(uri string) (string, error) {
	token, err := dsql.GenerateTokenConnString(context.Background(), uri)
	if err != nil {
		return "", fmt.Errorf("generate DSQL auth token: %w", err)
	}

	pgURI := "postgres" + strings.TrimPrefix(uri, "dsql")
	dbURI, err := url.Parse(pgURI)
	if err != nil {
		return "", fmt.Errorf("parse database URI: %w", err)
	}

	username := "admin"
	if dbURI.User != nil {
		username = dbURI.User.Username()
	}
	dbURI.User = url.UserPassword(username, token)

	q := dbURI.Query()
	q.Set("sslmode", "require")
	q.Del("region")
	dbURI.RawQuery = q.Encode()

	return dbURI.String(), nil
}

// GooseTableDDL is the DDL for creating a goose version table compatible with DSQL.
const GooseTableDDL = `
	CREATE TABLE IF NOT EXISTS goose_db_version (
		id BIGINT PRIMARY KEY DEFAULT (EXTRACT(EPOCH FROM now()) * 1000000)::BIGINT,
		version_id BIGINT NOT NULL,
		is_applied BOOLEAN NOT NULL,
		tstamp TIMESTAMP DEFAULT now()
	)
`

// EnsureGooseTable creates the goose_db_version table if it doesn't exist.
func EnsureGooseTable(db *sql.DB) error {
	if _, err := db.Exec(GooseTableDDL); err != nil {
		return fmt.Errorf("create goose table: %w", err)
	}

	// Goose expects an initial row with version 0 to exist
	var hasRows bool
	if err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM goose_db_version)`).Scan(&hasRows); err != nil {
		return fmt.Errorf("check goose table: %w", err)
	}

	if !hasRows {
		if _, err := db.Exec(`INSERT INTO goose_db_version (version_id, is_applied) VALUES (0, TRUE)`); err != nil {
			return fmt.Errorf("insert initial goose row: %w", err)
		}
	}

	return nil
}
