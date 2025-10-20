package postgres

import (
	"context"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openfga/openfga/pkg/storage/sqlcommon"
)

// pgxpoolIterQuery is a helper to run queries using pgxpool when used in sqlcommon iterator.
type pgxpoolIterQuery struct {
	db    *pgxpool.Pool
	query string
	args  []interface{}
}

var _ sqlcommon.SQLIteratorRowGetter = (*pgxpoolIterQuery)(nil)

func newPgxPoolGetRows(db *pgxpool.Pool, sb sq.SelectBuilder) (*pgxpoolIterQuery, error) {
	stmt, args, err := sb.ToSql()
	if err != nil {
		return nil, err
	}
	return &pgxpoolIterQuery{
		db:    db,
		query: stmt,
		args:  args,
	}, nil
}

// GetRows executes the pgxpool query and returns the sqlcommon.Rows.
func (p *pgxpoolIterQuery) GetRows(ctx context.Context) (sqlcommon.Rows, error) {
	rows, err := p.db.Query(ctx, p.query, p.args...)
	if err != nil {
		return nil, HandleSQLError(err)
	}
	return &pgxRowsWrapper{rows: rows}, nil
}

// pgxtxnIterQuery is a helper to run queries using pgxpool when used in sqlcommon iterator.
type pgxtxnIterQuery struct {
	txn   pgx.Tx
	query string
	args  []interface{}
}

var _ sqlcommon.SQLIteratorRowGetter = (*pgxtxnIterQuery)(nil)

func newPgxTxnGetRows(txn pgx.Tx, sb sq.SelectBuilder) (*pgxtxnIterQuery, error) {
	stmt, args, err := sb.ToSql()
	if err != nil {
		return nil, err
	}
	return &pgxtxnIterQuery{
		txn:   txn,
		query: stmt,
		args:  args,
	}, nil
}

// GetRows executes the txn query and returns the sqlcommon.Rows.
func (p *pgxtxnIterQuery) GetRows(ctx context.Context) (sqlcommon.Rows, error) {
	rows, err := p.txn.Query(ctx, p.query, p.args...)
	if err != nil {
		return nil, HandleSQLError(err)
	}
	return &pgxRowsWrapper{rows: rows}, nil
}

// pgxRowsWrapper wraps pgx.Rows to implement sqlcommon.Rows interface.
type pgxRowsWrapper struct {
	rows pgx.Rows
}

func (r *pgxRowsWrapper) Err() error {
	return r.rows.Err()
}

func (r *pgxRowsWrapper) Next() bool {
	return r.rows.Next()
}

func (r *pgxRowsWrapper) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r *pgxRowsWrapper) Close() error {
	r.rows.Close()
	return nil
}

var _ sqlcommon.Rows = (*pgxRowsWrapper)(nil)
