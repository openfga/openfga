package sqlite

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
)

type renderer struct {
	sb   strings.Builder
	args []any
	err  error // first ErrUnsupported; output is discarded once set
}

func (r *renderer) write(s string) { r.sb.WriteString(s) }

func (r *renderer) bind(v any) {
	r.args = append(r.args, v)
	r.write("?")
}

// --- statement --------------------------------------------------------------------------

func (r *renderer) selectStmt(s *ast.Select) {
	if s.Having != nil && len(s.GroupBy) == 0 && !hasAggregate(s.Columns) {
		r.implicitGroup(s)
		return
	}
	r.write("SELECT ")
	if s.Distinct {
		r.write("DISTINCT ")
	}
	if len(s.Columns) == 0 {
		r.write("*")
	} else {
		r.projections(s.Columns)
	}

	if len(s.From) > 0 {
		r.write(" FROM ")
		for i, t := range s.From {
			if i > 0 {
				r.write(", ")
			}
			r.write("tuple " + quoteAlias(t.Alias))
		}
	}
	for _, j := range s.Joins {
		r.write(" " + joinKeyword(j.Type) + " tuple " + quoteAlias(j.Table.Alias))
		if j.On != nil {
			r.write(" ON ")
			r.predicate(j.On)
		}
	}

	r.clause("WHERE", s.Where)
	if len(s.GroupBy) > 0 {
		r.write(" GROUP BY ")
		r.values(s.GroupBy)
	}
	r.clause("HAVING", s.Having)
	if len(s.OrderBy) > 0 {
		r.write(" ORDER BY ")
		r.orderBy(s.OrderBy)
	}
	if s.Limit != nil {
		r.write(" LIMIT " + strconv.FormatUint(*s.Limit, 10))
	}
	if s.Offset != nil {
		// SQLite requires LIMIT before OFFSET; -1 means unbounded.
		if s.Limit == nil {
			r.write(" LIMIT -1")
		}
		r.write(" OFFSET " + strconv.FormatUint(*s.Offset, 10))
	}
}

// implicitGroup rewrites HAVING without GROUP BY, which SQLite rejects unless a result column
// aggregates, as WHERE EXISTS (SELECT COUNT(*) ... HAVING ...). COUNT(*) keeps the single
// implicit group even over no rows. ORDER BY is dropped: at most one row is returned.
func (r *renderer) implicitGroup(s *ast.Select) {
	inner := *s
	inner.Distinct = false
	inner.Columns = []ast.Projection{ast.AggNode{Fn: ast.AggCount}}
	inner.OrderBy, inner.Limit, inner.Offset = nil, nil, nil
	r.selectStmt(&ast.Select{
		Distinct: s.Distinct,
		Columns:  s.Columns,
		Where:    ast.ExistsNode{Stmt: &inner},
		Limit:    s.Limit,
		Offset:   s.Offset,
	})
}

// hasAggregate ignores aggregates inside subqueries; they don't make this SELECT aggregate.
func hasAggregate(cols []ast.Projection) bool {
	for _, c := range cols {
		if a, ok := c.(ast.AliasNode); ok {
			c = a.Inner
		}
		if v, ok := c.(ast.ScalarValue); ok && valueAgg(v) {
			return true
		}
	}
	return false
}

func valueAgg(n ast.ScalarValue) bool {
	switch x := n.(type) {
	case ast.AggNode:
		return true
	case ast.CastNode:
		return valueAgg(x.Inner)
	case ast.FuncNode:
		return slices.ContainsFunc(x.Args, valueAgg)
	case ast.JSONObjectNode:
		return slices.ContainsFunc(x.Pairs, func(p ast.JSONPairNode) bool { return valueAgg(p.Key) || valueAgg(p.Value) })
	case ast.CaseSearchedNode:
		return (x.Else != nil && valueAgg(x.Else)) ||
			slices.ContainsFunc(x.Branches, func(b ast.SearchedBranch) bool { return predAgg(b.When) || valueAgg(b.Then) })
	case ast.CaseSimpleNode:
		return valueAgg(x.Base) || (x.Else != nil && valueAgg(x.Else)) ||
			slices.ContainsFunc(x.Branches, func(b ast.SimpleBranch) bool { return valueAgg(b.When) || valueAgg(b.Then) })
	case ast.ColNode, ast.BindNode, ast.LitNode, ast.SubqueryNode:
		return false
	default:
		panic(fmt.Sprintf("sqlite: unhandled value %T", n))
	}
}

func predAgg(n ast.Predicate) bool {
	switch x := n.(type) {
	case ast.CompareNode:
		return valueAgg(x.Left) || valueAgg(x.Right)
	case ast.LikeNode:
		return valueAgg(x.Left) || valueAgg(x.Pattern)
	case ast.BetweenNode:
		return valueAgg(x.Inner) || valueAgg(x.Lo) || valueAgg(x.Hi)
	case ast.IsNullNode:
		return valueAgg(x.Inner)
	case ast.InNode:
		return valueAgg(x.Left) || slices.ContainsFunc(x.Elems, valueAgg)
	case ast.QuantifiedNode:
		return valueAgg(x.Left)
	case ast.LogicalNode:
		return slices.ContainsFunc(x.Parts, predAgg)
	case ast.NotNode:
		return predAgg(x.Inner)
	case ast.ExistsNode, ast.ConstPredNode:
		return false
	default:
		panic(fmt.Sprintf("sqlite: unhandled predicate %T", n))
	}
}

func (r *renderer) clause(keyword string, pred ast.Predicate) {
	if pred == nil {
		return
	}
	r.write(" " + keyword + " ")
	r.predicate(pred)
}

func (r *renderer) values(ns []ast.ScalarValue) {
	for i, n := range ns {
		if i > 0 {
			r.write(", ")
		}
		r.value(n)
	}
}

func (r *renderer) projections(ns []ast.Projection) {
	for i, n := range ns {
		if i > 0 {
			r.write(", ")
		}
		r.projection(n)
	}
}

func (r *renderer) orderBy(terms []ast.OrderTerm) {
	for i, t := range terms {
		if i > 0 {
			r.write(", ")
		}
		r.value(t.Expr)
		if dir := sortDirection(t.Dir); dir != "" {
			r.write(" " + dir)
		}
	}
}

// --- nodes (one walk function per ast category interface) ------------------------------

func (r *renderer) predicate(n ast.Predicate) {
	switch x := n.(type) {
	case ast.CompareNode:
		r.value(x.Left)
		r.write(" " + op(x.Op) + " ")
		r.value(x.Right)

	case ast.LikeNode:
		r.value(x.Left)
		r.write(" LIKE ")
		r.value(x.Pattern)

	case ast.BetweenNode:
		r.value(x.Inner)
		r.write(" BETWEEN ")
		r.value(x.Lo)
		r.write(" AND ")
		r.value(x.Hi)

	case ast.IsNullNode:
		r.value(x.Inner)
		r.write(" IS NULL")

	case ast.InNode:
		r.value(x.Left)
		r.write(" IN (")
		r.values(x.Elems)
		r.write(")")

	case ast.QuantifiedNode:
		r.quantified(x)

	case ast.ExistsNode:
		r.write("EXISTS (")
		r.selectStmt(x.Stmt)
		r.write(")")

	case ast.LogicalNode:
		r.write("(")
		for i, p := range x.Parts {
			if i > 0 {
				r.write(" " + logical(x.Op) + " ")
			}
			r.predicate(p)
		}
		r.write(")")

	case ast.NotNode:
		r.write("NOT (")
		r.predicate(x.Inner)
		r.write(")")

	case ast.ConstPredNode:
		if x.Value {
			r.write("1 = 1")
		} else {
			r.write("1 = 0")
		}

	default:
		panic(fmt.Sprintf("sqlite: unhandled predicate %T", n))
	}
}

func (r *renderer) value(n ast.ScalarValue) {
	switch x := n.(type) {
	case ast.ColNode:
		r.write(sqliteColumn(x.Name, x.Alias))

	case ast.BindNode:
		r.bind(x.Value)

	case ast.LitNode:
		r.write(literal(x.Value))

	case ast.CastNode:
		r.write("CAST(")
		r.value(x.Inner)
		r.write(" AS " + sqliteType(x.Type) + ")")

	case ast.FuncNode:
		// SQLite rejects 1-arg COALESCE, which is just its argument.
		if x.Fn == ast.FuncCoalesce && len(x.Args) == 1 {
			r.value(x.Args[0])
			return
		}
		r.write(sqliteFunc(x.Fn) + "(")
		r.values(x.Args)
		r.write(")")

	case ast.JSONObjectNode:
		r.write("json_object(")
		for i, p := range x.Pairs {
			if i > 0 {
				r.write(", ")
			}
			r.value(p.Key)
			r.write(", ")
			r.value(p.Value)
		}
		r.write(")")

	case ast.AggNode:
		r.aggregate(x)

	case ast.CaseSearchedNode:
		r.write("CASE")
		for _, b := range x.Branches {
			r.write(" WHEN ")
			r.predicate(b.When)
			r.write(" THEN ")
			r.value(b.Then)
		}
		r.caseElse(x.Else)

	case ast.CaseSimpleNode:
		r.write("CASE ")
		r.value(x.Base)
		for _, b := range x.Branches {
			r.write(" WHEN ")
			r.value(b.When)
			r.write(" THEN ")
			r.value(b.Then)
		}
		r.caseElse(x.Else)

	case ast.SubqueryNode:
		r.write("(")
		r.selectStmt(x.Stmt)
		r.write(")")

	default:
		panic(fmt.Sprintf("sqlite: unhandled value %T", n))
	}
}

func (r *renderer) set(n ast.SetValue) {
	switch x := n.(type) {
	case ast.SubqueryNode:
		r.selectStmt(x.Stmt)

	default:
		panic(fmt.Sprintf("sqlite: unhandled set %T", n))
	}
}

func (r *renderer) projection(n ast.Projection) {
	switch x := n.(type) {
	case ast.AliasNode:
		r.projection(x.Inner)
		r.write(" AS " + quoteAlias(x.Alias))

	default:
		// ScalarValue embeds Projection.
		v, ok := n.(ast.ScalarValue)
		if !ok {
			panic(fmt.Sprintf("sqlite: unhandled projection %T", n))
		}
		r.value(v)
	}
}

func (r *renderer) caseElse(e ast.ScalarValue) {
	if e != nil {
		r.write(" ELSE ")
		r.value(e)
	}
	r.write(" END")
}

// aggregate emits FILTER natively (SQLite 3.30+), with no CASE emulation.
func (r *renderer) aggregate(x ast.AggNode) {
	r.write(sqliteAgg(x.Fn) + "(")
	if x.Distinct {
		r.write("DISTINCT ")
	}
	if len(x.Args) == 0 {
		r.write("*")
	} else {
		r.values(x.Args)
	}
	r.write(")")
	if x.Filter != nil {
		r.write(" FILTER (WHERE ")
		r.predicate(x.Filter)
		r.write(")")
	}
}

// quantified expands a bound set inline, since SQLite has no array operand.
func (r *renderer) quantified(x ast.QuantifiedNode) {
	sb, ok := x.Set.(ast.SetBindNode)
	if !ok {
		// SQLite has no ANY / ALL operator; only the IN-equivalent forms translate exactly.
		var kw string
		switch {
		case x.Op == ast.OpEq && x.Q == ast.Any:
			kw = " IN ("
		case x.Op == ast.OpNe && x.Q == ast.All:
			kw = " NOT IN ("
		default:
			if r.err == nil {
				r.err = fmt.Errorf("%w: %s %s over a subquery", ErrUnsupported, op(x.Op), quantifier(x.Q))
			}
			return
		}
		r.value(x.Left)
		r.write(kw)
		r.set(x.Set)
		r.write(")")
		return
	}

	elems := sb.Elems
	if len(elems) == 0 {
		// An empty set is a constant: ALL holds vacuously, ANY never does.
		if x.Q == ast.All {
			r.write("1 = 1")
		} else {
			r.write("1 = 0")
		}
		return
	}
	if x.Op == ast.OpEq && x.Q == ast.Any {
		r.value(x.Left)
		r.write(" IN (")
		for i, e := range elems {
			if i > 0 {
				r.write(", ")
			}
			r.bind(e)
		}
		r.write(")")
		return
	}
	conn := " OR "
	if x.Q == ast.All {
		conn = " AND "
	}
	r.write("(")
	for i, e := range elems {
		if i > 0 {
			r.write(conn)
		}
		r.value(x.Left)
		r.write(" " + op(x.Op) + " ")
		r.bind(e)
	}
	r.write(")")
}
