package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// queryCanceledCode is the SQLSTATE of a statement PostgreSQL ended on a cancel request.
const queryCanceledCode = "57014"

// contextError maps the error of a query cancelled server-side, while its
// context is done, to that context error: what pgx returns when it closes the
// connection instead. Any other error is returned unchanged.
func contextError(ctx context.Context, err error) error {
	if err == nil || ctx == nil || ctx.Err() == nil || errors.Is(err, ctx.Err()) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == queryCanceledCode {
		return fmt.Errorf("%w: %w", ctx.Err(), err)
	}
	return err
}

// contextErrorPool is a pgxpool.Pool whose queries return the context error
// when they are cancelled server-side. See contextError.
type contextErrorPool struct {
	*pgxpool.Pool
}

func (p *contextErrorPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tag, err := p.Pool.Exec(ctx, sql, args...)
	return tag, contextError(ctx, err)
}

func (p *contextErrorPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := p.Pool.Query(ctx, sql, args...)
	return &contextErrorRows{Rows: rows, ctx: ctx}, contextError(ctx, err)
}

func (p *contextErrorPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return &contextErrorRow{Row: p.Pool.QueryRow(ctx, sql, args...), ctx: ctx}
}

func (p *contextErrorPool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	tx, err := p.Pool.BeginTx(ctx, txOptions)
	if err != nil {
		return nil, contextError(ctx, err)
	}
	return &contextErrorTx{Tx: tx}, nil
}

type contextErrorTx struct {
	pgx.Tx
}

func (t *contextErrorTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tag, err := t.Tx.Exec(ctx, sql, args...)
	return tag, contextError(ctx, err)
}

func (t *contextErrorTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	rows, err := t.Tx.Query(ctx, sql, args...)
	return &contextErrorRows{Rows: rows, ctx: ctx}, contextError(ctx, err)
}

func (t *contextErrorTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return &contextErrorRow{Row: t.Tx.QueryRow(ctx, sql, args...), ctx: ctx}
}

func (t *contextErrorTx) Commit(ctx context.Context) error {
	return contextError(ctx, t.Tx.Commit(ctx))
}

type contextErrorRows struct {
	pgx.Rows
	ctx context.Context
}

func (r *contextErrorRows) Err() error {
	return contextError(r.ctx, r.Rows.Err())
}

type contextErrorRow struct {
	pgx.Row
	ctx context.Context
}

func (r *contextErrorRow) Scan(dest ...any) error {
	return contextError(r.ctx, r.Row.Scan(dest...))
}
