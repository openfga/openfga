package migrations

import (
	"context"
	"database/sql"

	"github.com/openfga/openfga/pkg/storage/migrate"
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
	Migrations.Register(
		&migrate.Migration{
			Version:  4,
			Forward:  up004,
			Backward: down004,
		},
	)
}
