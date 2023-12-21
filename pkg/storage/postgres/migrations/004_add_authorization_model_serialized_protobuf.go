package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func up004(ctx context.Context, tx *sql.Tx) error {
	stmt := `ALTER TABLE authorization_model ADD COLUMN serialized_protobuf BYTEA;`
	_, err := tx.ExecContext(ctx, stmt)
	return err
}

func down004(ctx context.Context, tx *sql.Tx) error {
	stmt := `ALTER TABLE authorization_model DROP COLUMN serialized_protobuf;`
	_, err := tx.ExecContext(ctx, stmt)
	return err
}

func init() {
	register(
		goose.NewGoMigration(
			4,
			&goose.GoFunc{RunTx: up004},
			&goose.GoFunc{RunTx: down004},
		),
	)
}
