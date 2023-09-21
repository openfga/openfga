package migrations

import (
	"context"
	"database/sql"
)

func up004(ctx context.Context, tx *sql.Tx) error {
	stmt := "ALTER TABLE authorization_model ADD COLUMN pbdata LONGBLOB;"
	_, err := tx.ExecContext(ctx, stmt)
	return err
}

func down004(ctx context.Context, tx *sql.Tx) error {
	stmt := "ALTER TABLE authorization_model DROP COLUMN pbdata;"
	_, err := tx.ExecContext(ctx, stmt)
	return err
}

func init() {
	migrations["004_add_authorization_model_pbdata"] = migration{up004, down004}
}
