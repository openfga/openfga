package ansi

import (
	"context"

	"github.com/openfga/openfga/pkg/storage/adapter"
)

// Executor runs a query that the ansi builder has already rendered to SQL text and
// ordinal bind arguments. It is the single seam an engine-specific package supplies:
// the builder owns all rendering and calls Query with the output of Build, leaving the
// Executor to feed sql and args into its own driver and adapt the result to
// adapter.Rows.
type Executor interface {
	// Query runs the rendered statement and returns its result cursor. sql carries
	// ordinal $N placeholders bound to args in order.
	Query(ctx context.Context, sql string, args []any) (adapter.Rows, error)
}

// Query is the runnable query surface this package adds on top of adapter.Query: in
// addition to executing, every query can Build itself into SQL text and bind arguments
// without running anything. Engine packages rarely need this — they supply an Executor
// and let Query.Execute drive it — but it is exported so callers can inspect, log, or
// hand the rendered statement to a driver directly. Both the SELECT and set-operation
// builders satisfy it, so an adapter.Query from this package may be asserted to it.
type Query interface {
	adapter.Query

	// Build renders the query and returns its ANSI SQL text (with ordinal $N
	// placeholders) and the positional bind arguments those placeholders refer to.
	Build() (sql string, args []any)
}

// builder is the concrete adapter.Builder. It mints leaf expressions and statements
// and renders them under its Dialect; execution is delegated to its Executor.
type builder struct {
	exec    Executor
	dialect Dialect
}

// Option configures a builder at construction. Engine-specific packages typically pass
// WithDialect to swap in their dialect.
type Option func(*builder)

// WithDialect sets the Dialect the builder renders under, replacing the default standard
// dialect. Engine packages pass their own dialect (embedding ANSIDialect for Placeholder
// and StandardColumn, then implementing Column for their schema).
func WithDialect(d Dialect) Option {
	return func(b *builder) { b.dialect = d }
}

// New returns a Builder that renders SQL and runs statements through exec. By default it
// renders portable standard-schema SQL; pass WithDialect to target an engine. A nil exec
// yields a render-only builder: every node still builds (use the Build method on the
// returned Query, reachable via the exported Query interface), but Query.Execute will
// panic. Engine-specific packages wrap New with an Executor and dialect backed by their
// driver.
func New(exec Executor, opts ...Option) adapter.Builder {
	b := &builder{exec: exec, dialect: standardDialect{}}
	for _, opt := range opts {
		opt(b)
	}
	return b
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

// run renders a query node under the builder's dialect and hands the SQL text and bind
// arguments to the Executor.
func (b *builder) run(ctx context.Context, q sqlWriter) (adapter.Rows, error) {
	sql, args := render(q, b.dialect)
	return b.exec.Query(ctx, sql, args)
}

func operandWriters(args []adapter.Operand) []sqlWriter {
	out := make([]sqlWriter, len(args))
	for i, a := range args {
		out[i] = operandWriter(a)
	}
	return out
}
