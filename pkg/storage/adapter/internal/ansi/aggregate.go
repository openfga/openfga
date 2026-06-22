package ansi

import "github.com/openfga/openfga/pkg/storage/adapter"

// aggregateNode renders an aggregate function call with its optional DISTINCT, ORDER
// BY (within the argument list), and FILTER (WHERE ...) modifiers. An exprNode wraps it
// so the comparison/ordering algebra of an Expression applies to the aggregate result.
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
	writeArgs(r, a.args, jsonPairs)
	if len(a.orderBy) > 0 {
		r.write(" ORDER BY ")
		r.list(a.orderBy, ", ")
	}
	r.write(")")
	if a.filter != nil {
		r.write(" FILTER (WHERE ")
		r.node(a.filter)
		r.write(")")
	}
}

// aggExpr is the AggregateExpr facade: it forwards the full Expression algebra to its
// embedded exprNode and adds the aggregate-only modifiers, each returning itself for
// chaining.
type aggExpr struct {
	node *aggregateNode
	expr *exprNode
}

func (a *aggExpr) writeSQL(r *renderer)             { r.node(a.node) }
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
func (a *aggExpr) IsNull() adapter.Predicate                     { return a.expr.IsNull() }
func (a *aggExpr) Order(d adapter.SortDirection) adapter.OrderTerm { return a.expr.Order(d) }
func (a *aggExpr) Asc() adapter.OrderTerm                        { return a.expr.Asc() }
func (a *aggExpr) Desc() adapter.OrderTerm                       { return a.expr.Desc() }

func (a *aggExpr) Distinct() adapter.AggregateExpr {
	a.node.distinct = true
	return a
}

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
