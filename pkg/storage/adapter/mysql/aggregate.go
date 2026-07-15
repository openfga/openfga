package mysql

import "github.com/openfga/openfga/pkg/storage/adapter"

// aggregateNode renders an aggregate function call with its optional DISTINCT, ORDER BY
// (within the argument list), and Filter modifiers. An exprNode wraps it so the
// comparison/ordering algebra of an Expression applies to the aggregate result.
type aggregateNode struct {
	fn       adapter.AggregateFunc
	args     []sqlWriter
	distinct bool
	orderBy  []sqlWriter
	filter   sqlWriter
}

func newAggregate(fn adapter.AggregateFunc, args []sqlWriter) *aggExpr {
	an := &aggregateNode{fn: fn, args: args}
	return &aggExpr{node: an, expr: newExpr(an)}
}

func (a *aggregateNode) writeSQL(r *renderer) {
	name, jsonPairs := aggregateFuncSQL(a.fn)
	r.write(name)
	r.write("(")
	if a.distinct {
		r.write("DISTINCT ")
	}
	if a.filter != nil {
		// MySQL has no aggregate FILTER (WHERE ...) clause. Emulate it by aggregating a
		// CASE that yields the argument only when the filter holds and NULL otherwise, so
		// the aggregate skips the filtered-out rows exactly as FILTER would.
		a.writeFilteredArgs(r, jsonPairs)
	} else {
		writeArgs(r, a.args, jsonPairs)
	}
	if len(a.orderBy) > 0 {
		r.write(" ORDER BY ")
		r.list(a.orderBy, ", ")
	}
	r.write(")")
}

// writeFilteredArgs renders the argument list with each argument wrapped so the aggregate
// only sees rows where the filter predicate holds:
//
//   - no arguments (e.g. COUNT(*)): "CASE WHEN <cond> THEN 1 END";
//   - one or more arguments: each becomes "CASE WHEN <cond> THEN <arg> END", preserving
//     the argument count;
//   - JSON object pairs: only the value of each "key, value" pair is wrapped, so a
//     filtered-out row contributes a NULL value the aggregate skips.
func (a *aggregateNode) writeFilteredArgs(r *renderer, jsonPairs bool) {
	if len(a.args) == 0 {
		r.write("CASE WHEN ")
		r.node(a.filter)
		r.write(" THEN 1 END")
		return
	}
	if jsonPairs {
		for i := 0; i+1 < len(a.args); i += 2 {
			if i > 0 {
				r.write(", ")
			}
			r.node(a.args[i])
			r.write(", ")
			a.writeCase(r, a.args[i+1])
		}
		return
	}
	for i, arg := range a.args {
		if i > 0 {
			r.write(", ")
		}
		a.writeCase(r, arg)
	}
}

// writeCase renders "CASE WHEN <filter> THEN <arg> END".
func (a *aggregateNode) writeCase(r *renderer, arg sqlWriter) {
	r.write("CASE WHEN ")
	r.node(a.filter)
	r.write(" THEN ")
	r.node(arg)
	r.write(" END")
}

// aggExpr is the AggregateExpr facade: it forwards the full Expression algebra to its
// embedded exprNode and adds the aggregate-only modifiers, each returning itself for
// chaining.
type aggExpr struct {
	node *aggregateNode
	expr *exprNode
}

func (a *aggExpr) writeSQL(r *renderer)               { r.node(a.node) }
func (a *aggExpr) As(alias string) adapter.Projection { return a.expr.As(alias) }

func (a *aggExpr) Compare(op adapter.ComparisonOp, other adapter.Expression) adapter.Predicate {
	return a.expr.Compare(op, other)
}
func (a *aggExpr) Eq(o adapter.Expression) adapter.Predicate  { return a.expr.Eq(o) }
func (a *aggExpr) Lt(o adapter.Expression) adapter.Predicate  { return a.expr.Lt(o) }
func (a *aggExpr) Lte(o adapter.Expression) adapter.Predicate { return a.expr.Lte(o) }
func (a *aggExpr) Gt(o adapter.Expression) adapter.Predicate  { return a.expr.Gt(o) }
func (a *aggExpr) Gte(o adapter.Expression) adapter.Predicate { return a.expr.Gte(o) }
func (a *aggExpr) Quantified(op adapter.ComparisonOp, q adapter.Quantifier, right any) adapter.Predicate {
	return a.expr.Quantified(op, q, right)
}
func (a *aggExpr) In(v ...adapter.Expression) adapter.Predicate { return a.expr.In(v...) }
func (a *aggExpr) Between(lo, hi adapter.Expression) adapter.Predicate {
	return a.expr.Between(lo, hi)
}
func (a *aggExpr) Like(p adapter.Expression) adapter.Predicate {
	return a.expr.Like(p)
}
func (a *aggExpr) IsNull() adapter.Predicate                       { return a.expr.IsNull() }
func (a *aggExpr) Order(d adapter.SortDirection) adapter.OrderTerm { return a.expr.Order(d) }
func (a *aggExpr) Asc() adapter.OrderTerm                          { return a.expr.Asc() }
func (a *aggExpr) Desc() adapter.OrderTerm                         { return a.expr.Desc() }

func (a *aggExpr) Distinct() adapter.AggregateExpr {
	a.node.distinct = true
	return a
}

// Filter restricts the aggregated rows to those satisfying condition. MySQL has no FILTER
// clause, so the aggregate renders a CASE over its argument instead (see
// aggregateNode.writeSQL).
func (a *aggExpr) Filter(condition adapter.Predicate) adapter.AggregateExpr {
	a.node.filter = predWriter(condition)
	return a
}

func (a *aggExpr) OrderBy(terms ...adapter.OrderTerm) adapter.AggregateExpr {
	for _, t := range terms {
		a.node.orderBy = append(a.node.orderBy, orderTermWriter(t))
	}
	return a
}
