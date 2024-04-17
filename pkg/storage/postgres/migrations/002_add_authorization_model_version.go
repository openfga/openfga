package migrations

import (
	"context"
	"database/sql"

	"github.com/openfga/openfga/pkg/storage/migrate"
)

func up002(ctx context.Context, tx *sql.Tx) error {
	stmt := `ALTER TABLE authorization_model ADD COLUMN schema_version TEXT NOT NULL DEFAULT '1.0';`
	_, err := tx.ExecContext(ctx, stmt)
	return err
}

func down002(ctx context.Context, tx *sql.Tx) error {
	stmt := `ALTER TABLE authorization_model DROP COLUMN schema_version;`
	_, err := tx.ExecContext(ctx, stmt)
	return err
}

func init() {
	Migrations.MustRegister(
		&migrate.Migration{
			Version:  2,
			Forward:  up002,
			Backward: down002,
		},
	)
}
