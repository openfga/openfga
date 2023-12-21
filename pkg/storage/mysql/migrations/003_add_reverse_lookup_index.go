package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
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
	register(
		goose.NewGoMigration(
			3,
			&goose.GoFunc{RunTx: up003},
			&goose.GoFunc{RunTx: down003},
		),
	)
}
