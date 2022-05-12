package testutils

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

var dropTablesString = `
DROP TABLE IF EXISTS tuple;
DROP TABLE IF EXISTS authorization_model;
DROP TABLE IF EXISTS store;
DROP TABLE IF EXISTS assertion;
DROP TABLE IF EXISTS changelog;
`

var createTablesString = `
CREATE TABLE tuple (
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

CREATE INDEX partial_user_idx ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'user';
CREATE INDEX partial_userset_idx ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'userset';
CREATE UNIQUE INDEX ulid_idx ON tuple (ulid);

CREATE TABLE authorization_model (
	store TEXT NOT NULL,
	authorization_model_id TEXT NOT NULL,
	type TEXT NOT NULL,
	type_definition BYTEA NOT NULL,
	inserted_at TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (store, authorization_model_id, type)
);

CREATE TABLE store (
	id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL,
	updated_at TIMESTAMPTZ,
	deleted_at TIMESTAMPTZ
);

CREATE TABLE assertion (
	store TEXT NOT NULL,
	authorization_model_id TEXT NOT NULL,
	assertions BYTEA NOT NULL,
	PRIMARY KEY (store, authorization_model_id)
);

CREATE TABLE changelog (
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
	if _, err := pool.Exec(ctx, dropTablesString[1:]); err != nil {
		return fmt.Errorf("error dropping Postgres tables: %w", err)
	}

	if _, err := pool.Exec(ctx, createTablesString[1:]); err != nil {
		return fmt.Errorf("error creating Postgres tables: %w", err)
	}
	return nil
}
