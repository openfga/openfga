package mysql

import (
	"context"

	"github.com/openfga/openfga/pkg/storage/adapter"
)

// selectStmt accumulates the clauses of one SELECT and renders them in canonical order.
type selectStmt struct {
	b *builder

	distinct   bool
	distinctOn []sqlWriter
	columns    []aliased
	from       []*tupleTable
	joins      []sqlWriter
	where      []sqlWriter
	groupBy    []sqlWriter
	having     []sqlWriter
	orderBy    []sqlWriter
	limit      *uint64
	offset     *uint64
}

func (s *selectStmt) Distinct(on ...adapter.Expression) adapter.SelectBuilder {
	s.distinct = true
	for _, e := range on {
		s.distinctOn = append(s.distinctOn, exprWriter(e))
	}
	return s
}

func (s *selectStmt) Columns(columns ...adapter.Projection) adapter.SelectBuilder {
	for _, c := range columns {
		s.columns = append(s.columns, projWriter(c))
	}
	return s
}

func (s *selectStmt) From(sources ...adapter.Tuple) adapter.SelectBuilder {
	for _, src := range sources {
		s.from = append(s.from, src.(*tupleTable))
	}
	return s
}

func (s *selectStmt) JoinClause(join adapter.Join) adapter.SelectBuilder {
	s.joins = append(s.joins, join.(*joinNode))
	return s
}

func (s *selectStmt) Where(conditions ...adapter.Predicate) adapter.SelectBuilder {
	for _, c := range conditions {
		s.where = append(s.where, predWriter(c))
	}
	return s
}

func (s *selectStmt) GroupBy(exprs ...adapter.Operand) adapter.SelectBuilder {
	for _, e := range exprs {
		s.groupBy = append(s.groupBy, operandWriter(e))
	}
	return s
}

func (s *selectStmt) Having(conditions ...adapter.Predicate) adapter.SelectBuilder {
	for _, c := range conditions {
		s.having = append(s.having, predWriter(c))
	}
	return s
}

func (s *selectStmt) OrderBy(terms ...adapter.OrderTerm) adapter.SelectBuilder {
	for _, t := range terms {
		s.orderBy = append(s.orderBy, orderTermWriter(t))
	}
	return s
}

func (s *selectStmt) Limit(n uint64) adapter.SelectBuilder {
	s.limit = &n
	return s
}

func (s *selectStmt) Offset(n uint64) adapter.SelectBuilder {
	s.offset = &n
	return s
}

func (s *selectStmt) Set(op adapter.SetOp, all bool, other adapter.Query) adapter.SetQueryBuilder {
	return newSetQuery(s.b, s, op, all, other)
}

func (s *selectStmt) writeSQL(r *renderer) {
	r.write("SELECT ")
	if s.distinct {
		r.write("DISTINCT ")
		if len(s.distinctOn) > 0 {
			r.write("ON (")
			r.list(s.distinctOn, ", ")
			r.write(") ")
		}
	}
	s.writeColumns(r)

	if len(s.from) > 0 {
		r.write(" FROM ")
		for i, src := range s.from {
			if i > 0 {
				r.write(", ")
			}
			src.fromSQL(r)
		}
	}
	for _, j := range s.joins {
		r.write(" ")
		r.node(j)
	}
	writeConditions(r, " WHERE ", s.where, " AND ")
	if len(s.groupBy) > 0 {
		r.write(" GROUP BY ")
		r.list(s.groupBy, ", ")
	}
	writeConditions(r, " HAVING ", s.having, " AND ")
	if len(s.orderBy) > 0 {
		r.write(" ORDER BY ")
		r.list(s.orderBy, ", ")
	}
	writeLimitOffset(r, s.limit, s.offset)
}

func (s *selectStmt) writeColumns(r *renderer) {
	if len(s.columns) == 0 {
		r.write("*")
		return
	}
	for i, c := range s.columns {
		if i > 0 {
			r.write(", ")
		}
		r.node(c)
		if a := c.alias(); a != "" {
			r.write(" AS ")
			r.write(a)
		}
	}
}

// --- Query surface shared by selectStmt and setQuery ---

func (s *selectStmt) Execute(ctx context.Context) (adapter.Rows, error) {
	return s.b.run(ctx, s)
}

// Build renders the SELECT to MySQL SQL text and its bind arguments.
func (s *selectStmt) Build() (string, []any) { return render(s) }

func (s *selectStmt) ScalarExpr() adapter.Expression { return scalarSubquery(s) }
func (s *selectStmt) Exists() adapter.Predicate      { return existsPredicate(s) }

// setQuery composes queries under set operators (UNION / INTERSECT / EXCEPT).
type setQuery struct {
	b     *builder
	left  sqlWriter
	parts []setPart

	orderBy []sqlWriter
	limit   *uint64
	offset  *uint64
}

type setPart struct {
	op    adapter.SetOp
	all   bool
	query sqlWriter
}

func newSetQuery(b *builder, left sqlWriter, op adapter.SetOp, all bool, q adapter.Query) *setQuery {
	return &setQuery{
		b:     b,
		left:  left,
		parts: []setPart{{op: op, all: all, query: queryWriter(q)}},
	}
}

func (s *setQuery) Set(op adapter.SetOp, all bool, q adapter.Query) adapter.SetQueryBuilder {
	s.parts = append(s.parts, setPart{op: op, all: all, query: queryWriter(q)})
	return s
}

func (s *setQuery) OrderBy(terms ...adapter.OrderTerm) adapter.SetQueryBuilder {
	for _, t := range terms {
		s.orderBy = append(s.orderBy, orderTermWriter(t))
	}
	return s
}

func (s *setQuery) Limit(n uint64) adapter.SetQueryBuilder {
	s.limit = &n
	return s
}

func (s *setQuery) Offset(n uint64) adapter.SetQueryBuilder {
	s.offset = &n
	return s
}

func (s *setQuery) writeSQL(r *renderer) {
	r.node(s.left)
	for _, p := range s.parts {
		r.write(" ")
		r.write(setSQL(p.op))
		if p.all {
			r.write(" ALL")
		}
		r.write(" ")
		r.node(p.query)
	}
	if len(s.orderBy) > 0 {
		r.write(" ORDER BY ")
		r.list(s.orderBy, ", ")
	}
	writeLimitOffset(r, s.limit, s.offset)
}

func (s *setQuery) Execute(ctx context.Context) (adapter.Rows, error) {
	return s.b.run(ctx, s)
}

// Build renders the set operation to MySQL SQL text and its bind arguments.
func (s *setQuery) Build() (string, []any) { return render(s) }

func (s *setQuery) ScalarExpr() adapter.Expression { return scalarSubquery(s) }
func (s *setQuery) Exists() adapter.Predicate      { return existsPredicate(s) }

// scalarSubquery wraps a query as a parenthesised scalar expression.
func scalarSubquery(q sqlWriter) adapter.Expression {
	return newExpr(writerFunc(func(r *renderer) {
		r.write("(")
		r.node(q)
		r.write(")")
	}))
}

// existsPredicate wraps a query as an EXISTS predicate.
func existsPredicate(q sqlWriter) adapter.Predicate {
	return newPredicate(writerFunc(func(r *renderer) {
		r.write("EXISTS (")
		r.node(q)
		r.write(")")
	}))
}

func writeConditions(r *renderer, lead string, conds []sqlWriter, sep string) {
	if len(conds) == 0 {
		return
	}
	r.write(lead)
	r.list(conds, sep)
}

func writeLimitOffset(r *renderer, limit, offset *uint64) {
	if limit != nil {
		r.write(" LIMIT ")
		r.write(utoa(*limit))
	}
	if offset != nil {
		r.write(" OFFSET ")
		r.write(utoa(*offset))
	}
}

func utoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
