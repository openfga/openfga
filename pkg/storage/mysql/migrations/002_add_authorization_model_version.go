package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func up002(ctx context.Context, tx *sql.Tx) error {
	stmt := `ALTER TABLE authorization_model ADD COLUMN schema_version VARCHAR(5) NOT NULL DEFAULT '1.0';`
	_, err := tx.ExecContext(ctx, stmt)
	return err
}

func down002(ctx context.Context, tx *sql.Tx) error {
	stmt := `ALTER TABLE authorization_model DROP COLUMN schema_version;`
	_, err := tx.ExecContext(ctx, stmt)
	return err
}

func init() {
	register(
		goose.NewGoMigration(
			2,
			&goose.GoFunc{RunTx: up002},
			&goose.GoFunc{RunTx: down002},
		),
	)
}
