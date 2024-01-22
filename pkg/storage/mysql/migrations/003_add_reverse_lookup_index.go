package migrations

import (
	"context"
	"database/sql"

	"github.com/openfga/openfga/pkg/storage/migrate"
)

func up003(ctx context.Context, tx *sql.Tx) error {
	stmt := `CREATE INDEX idx_reverse_lookup_user on tuple (store, object_type, relation, _user);`
	_, err := tx.ExecContext(ctx, stmt)
	return err
}

func down003(ctx context.Context, tx *sql.Tx) error {
	stmt := `DROP INDEX  idx_reverse_lookup_user on tuple;`
	_, err := tx.ExecContext(ctx, stmt)
	return err
}

func init() {
	Migrations.Register(
		&migrate.Migration{
			Version:  3,
			Forward:  up003,
			Backward: down003,
		},
	)
}
