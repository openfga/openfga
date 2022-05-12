package testutils

import (
	"context"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var dropTablesString = `
DROP TABLE IF EXISTS tuple;
DROP TABLE IF EXISTS authorization_model;
DROP TABLE IF EXISTS store;
DROP TABLE IF EXISTS assertion;
DROP TABLE IF EXISTS changelog;
`

var stmts = []string{
	`CREATE TABLE IF NOT EXISTS tuple (
		store TEXT NOT NULL,
		object_type TEXT NOT NULL,
		object_id TEXT NOT NULL,
		relation TEXT NOT NULL,
		_user TEXT NOT NULL,
		user_type TEXT NOT NULL,
		ulid TEXT NOT NULL,
		inserted_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (store, object_type, object_id, relation, _user)
	)`,
	`CREATE INDEX IF NOT EXISTS partial_user_idx ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'user'`,
	`CREATE INDEX IF NOT EXISTS partial_userset_idx ON tuple (store, object_type, object_id, relation, _user) WHERE user_type = 'userset'`,
	`CREATE UNIQUE INDEX IF NOT EXISTS ulid_idx ON tuple (ulid)`,
	`CREATE TABLE IF NOT EXISTS authorization_model (
		store TEXT NOT NULL,
		authorization_model_id TEXT NOT NULL,
		type TEXT NOT NULL,
		type_definition BYTEA,
		PRIMARY KEY (store, authorization_model_id, type)
	)`,
	`CREATE TABLE IF NOT EXISTS store (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL,
		updated_at TIMESTAMPTZ,
		deleted_at TIMESTAMPTZ
	)`,
	`CREATE TABLE IF NOT EXISTS assertion (
		store TEXT NOT NULL,
		authorization_model_id TEXT NOT NULL,
		assertions BYTEA,
		PRIMARY KEY (store, authorization_model_id)
	)`,
	`CREATE TABLE IF NOT EXISTS changelog (
		store TEXT NOT NULL,
		object_type TEXT NOT NULL,
		object_id TEXT NOT NULL,
		relation TEXT NOT NULL,
		_user TEXT NOT NULL,
		operation INTEGER NOT NULL,
		ulid TEXT NOT NULL,
		inserted_at TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (store, ulid, object_type)
	)`,
}

func RecreatePostgresTestTables(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, dropTablesString[1:]); err != nil {
		return err
	}

	batch := &pgx.Batch{}
	for _, stmt := range stmts {
		batch.Queue(stmt)
	}
	return pool.SendBatch(ctx, batch).Close()
}
