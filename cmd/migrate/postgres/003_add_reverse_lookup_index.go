package migrations

import (
	"context"
	"database/sql"
)

func up003(ctx context.Context, tx *sql.Tx) error {
	stmt := `CREATE INDEX idx_reverse_lookup_user on tuple (store, object_type, relation, _user);`

	_, err := tx.ExecContext(ctx, stmt)
	return err
}

func down003(ctx context.Context, tx *sql.Tx) error {
	stmt := `DROP INDEX IF EXISTS idx_reverse_lookup_user ;`
	_, err := tx.ExecContext(ctx, stmt)
	return err
}

func init() {
	migrations["003_add_reverse_lookup_index"] = migration{up003, down003}
}
