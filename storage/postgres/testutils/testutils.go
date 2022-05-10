package testutils

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

var createTablesString = `
CREATE TABLE IF NOT EXISTS tuple (
	store TEXT NOT NULL,
	object_type TEXT NOT NULL,
	object_id TEXT NOT NULL,
	relation TEXT NOT NULL,
	_user TEXT NOT NULL,
	user_type TEXT NOT NULL,
	ulid TEXT NOT NULL,
	inserted_at TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (store, object_type, object_id, relation, _user)
);

CREATE INDEX IF NOT EXISTS partial_user_idx ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'user';
CREATE INDEX IF NOT EXISTS partial_userset_idx ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'userset';
CREATE UNIQUE INDEX IF NOT EXISTS ulid_idx ON tuple (ulid);

CREATE TABLE IF NOT EXISTS authorization_model (
	store TEXT NOT NULL,
	authorization_model_id TEXT NOT NULL,
	type TEXT NOT NULL,
	type_definition BYTEA NOT NULL,
	inserted_at TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (store, authorization_model_id, type)
);

CREATE TABLE IF NOT EXISTS assertion (
	store TEXT NOT NULL,
	authorization_model_id TEXT NOT NULL,
	assertions BYTEA NOT NULL,
	PRIMARY KEY (store, authorization_model_id)
);

CREATE TABLE IF NOT EXISTS changelog (
	store TEXT NOT NULL,
	object_type TEXT NOT NULL,
	object_id TEXT NOT NULL,
	relation TEXT NOT NULL,
	_user TEXT NOT NULL,
	operation INTEGER NOT NULL,
	ulid TEXT NOT NULL,
	inserted_at TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (store, ulid, object_type)
);
`

func CreatePostgresTestTables(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, createTablesString); err != nil {
		return fmt.Errorf("error creating Postgres tables: %w", err)
	}
	return nil
}
