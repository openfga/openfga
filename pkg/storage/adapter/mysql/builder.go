// Package mysql is a standalone implementation of the adapter query builder
// (pkg/storage/adapter) that renders MySQL SQL and its bind arguments. Unlike the pg and
// sqlite adapters, it does not build on the internal/ansi renderer: MySQL cannot execute
// several ANSI constructs the shared renderer emits (most notably an aggregate's
// FILTER (WHERE ...) clause), so this package owns the full node algebra and rendering and
// substitutes MySQL-compatible forms where the two diverge (see aggregate.go and
// predicate.go).
//
// It owns rendering but not execution: a Builder is constructed with a *sql.DB, and every
// Query additionally exposes Build, which returns the rendered SQL text and "?" bind
// placeholders without running anything. MySQL's bind placeholder is "?", so placeholders
// are emitted inline while rendering.
//
// Construct a Builder with New (wrapping an existing *sql.DB) or Open (which opens one via
// the go-sql-driver/mysql driver). Pass a nil *sql.DB to New for a render-only builder:
// its queries never execute, but each can be rendered via Query.Build.
package mysql

import (
	"context"
	"database/sql"

	// Register the go-sql-driver/mysql driver under the "mysql" name for database/sql.
	_ "github.com/go-sql-driver/mysql"

	"github.com/openfga/openfga/pkg/storage/adapter"
)

// executor runs rendered statements against a *sql.DB using the standard database/sql
// MySQL driver. It is the seam Query.Execute calls; rendering is this package's concern.
// A nil db yields a render-only builder whose queries render but never execute.
type executor struct {
	db *sql.DB
}

// Query runs the rendered SQL against the database. The "?" placeholders this package
// emits are MySQL's native positional form, so args bind in order. *sql.Rows already
// satisfies adapter.Rows, so the cursor is returned directly.
func (e *executor) Query(ctx context.Context, query string, args []any) (adapter.Rows, error) {
	return e.db.QueryContext(ctx, query, args...)
}

// Query is the runnable query surface this package adds on top of adapter.Query: in
// addition to executing, every query can Build itself into MySQL SQL text and bind
// arguments without running anything. Both the SELECT and set-operation builders satisfy
// it, so an adapter.Query from this package may be asserted to it.
type Query interface {
	adapter.Query

	// Build renders the query and returns its MySQL SQL text (with "?" placeholders) and
	// the positional bind arguments those placeholders refer to.
	Build() (sql string, args []any)
}

// builder is the concrete adapter.Builder. It mints leaf expressions and statements and
// renders them as MySQL SQL; execution is delegated to its executor.
type builder struct {
	exec *executor
}

// New returns a Builder that renders MySQL and executes through the supplied database
// handle. The caller owns the handle's lifecycle (e.g. via Open). A nil handle yields a
// render-only builder: every node still builds (use Query.Build), but Query.Execute will
// panic.
func New(db *sql.DB) adapter.Builder {
	return &builder{exec: &executor{db: db}}
}

// Open opens a database handle against dataSourceName using the go-sql-driver/mysql driver
// and returns a Builder over it together with the handle, which the caller is responsible
// for closing.
func Open(dataSourceName string) (adapter.Builder, *sql.DB, error) {
	db, err := sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, nil, err
	}
	return New(db), db, nil
}

func (b *builder) Tuple(alias string) adapter.Tuple {
	return &tupleTable{alias: alias}
}

func (b *builder) Lit(value any) adapter.Expression {
	return newExpr(&litNode{value: value})
}

func (b *builder) Bind(value any) adapter.Expression {
	return newExpr(&bindNode{value: value})
}

func (b *builder) Func(fn adapter.ScalarFunc, args ...adapter.Operand) adapter.Expression {
	return newExpr(&funcNode{fn: fn, args: operandWriters(args)})
}

func (b *builder) Aggregate(fn adapter.AggregateFunc, args ...adapter.Operand) adapter.AggregateExpr {
	return newAggregate(fn, operandWriters(args))
}

func (b *builder) Case() adapter.CaseExpr {
	return newCase()
}

func (b *builder) Cast(expr adapter.Operand, sqlType string) adapter.Expression {
	inner := operandWriter(expr)
	return newExpr(writerFunc(func(r *renderer) {
		r.write("CAST(")
		r.node(inner)
		r.write(" AS ")
		r.write(sqlType)
		r.write(")")
	}))
}

func (b *builder) Select(columns ...adapter.Projection) adapter.SelectBuilder {
	s := &selectStmt{b: b}
	return s.Columns(columns...)
}

func (b *builder) Join(jt adapter.JoinType, table adapter.Tuple) adapter.Join {
	return &joinNode{jt: jt, table: table.(*tupleTable)}
}

// run renders a query node and hands the SQL text and bind arguments to the executor.
func (b *builder) run(ctx context.Context, q sqlWriter) (adapter.Rows, error) {
	sql, args := render(q)
	return b.exec.Query(ctx, sql, args)
}

func operandWriters(args []adapter.Operand) []sqlWriter {
	out := make([]sqlWriter, len(args))
	for i, a := range args {
		out[i] = operandWriter(a)
	}
	return out
}
