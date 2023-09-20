package migrations

import (
	"context"
	"database/sql"
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
	migrations["002_add_authorization_model_version"] = migration{up002, down002}
}
