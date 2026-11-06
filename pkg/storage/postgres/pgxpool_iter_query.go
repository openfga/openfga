package postgres

//go:generate mockgen -source=pgxpool_iter_query.go --destination ../../../internal/mocks/mock_pgx_tx.go --package mocks

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/openfga/openfga/pkg/storage/sqlcommon"
)

type pgxConnConnection pgxpool.Conn

var _ sqlcommon.Connection = (*pgxConnConnection)(nil)

func (c *pgxConnConnection) Query(ctx context.Context, sql string, args ...any) (sqlcommon.Rows, error) {
	conn := (*pgxpool.Conn)(c)

	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRowsWrapper{conn: conn, rows: rows}, nil
}

func (c *pgxConnConnection) Close() error {
	conn := (*pgxpool.Conn)(c)
	conn.Release()
	return nil
}

type pgxTxConnection struct {
	tx pgx.Tx
}

var _ sqlcommon.Connection = (*pgxTxConnection)(nil)

func (c *pgxTxConnection) Query(ctx context.Context, sql string, args ...any) (sqlcommon.Rows, error) {
	rows, err := c.tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRowsWrapper{rows: rows}, nil
}

func (c *pgxTxConnection) Close() error {
	return nil
}

type pgxPoolConnector pgxpool.Pool

var _ sqlcommon.Connector = (*pgxPoolConnector)(nil)

func (c *pgxPoolConnector) Connect(ctx context.Context) (sqlcommon.Connection, error) {
	pool := (*pgxpool.Pool)(c)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	return (*pgxConnConnection)(conn), nil
}

type pgxTxConnector struct {
	tx pgx.Tx
}

var _ sqlcommon.Connector = (*pgxTxConnector)(nil)

func (c *pgxTxConnector) Connect(ctx context.Context) (sqlcommon.Connection, error) {
	return &pgxTxConnection{tx: c.tx}, nil
}

// PgxExec interface allows pgx Exec functionality.
type PgxExec interface {
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
}

// pgxRowsWrapper wraps pgx.Rows to implement sqlcommon.Rows interface.
type pgxRowsWrapper struct {
	conn *pgxpool.Conn
	rows pgx.Rows
}

func (r *pgxRowsWrapper) Err() error {
	return r.rows.Err()
}

func (r *pgxRowsWrapper) Next() bool {
	if !r.rows.Next() {
		if r.conn != nil {
			r.conn.Release()
			r.conn = nil
		}
		return false
	}
	return true
}

func (r *pgxRowsWrapper) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r *pgxRowsWrapper) Close() error {
	r.rows.Close()
	if r.conn != nil {
		r.conn.Release()
		r.conn = nil
	}
	return nil
}

var _ sqlcommon.Rows = (*pgxRowsWrapper)(nil)
