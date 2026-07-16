package ansi

import "github.com/openfga/openfga/pkg/storage/adapter"

// aliased is the rendering contract for any projection: it can write its own SQL and
// report an optional output alias.
type aliased interface {
	sqlWriter
	alias() string
}

// projectionNode is the bare-Projection tier: a node that can only be projected and
// aliased. It backs select-only columns such as Tuple.ConditionContext.
type projectionNode struct {
	inner    sqlWriter
	outAlias string
}

func (p *projectionNode) writeSQL(r *renderer) { r.node(p.inner) }
func (p *projectionNode) alias() string        { return p.outAlias }

func (p *projectionNode) As(a string) adapter.Projection {
	return &projectionNode{inner: p.inner, outAlias: a}
}

// operandNode adds nothing to projectionNode's algebra but satisfies the Operand tier,
// the type used for GROUP BY terms and function/aggregate/CAST arguments.
type operandNode struct {
	inner    sqlWriter
	outAlias string
}

func (o *operandNode) writeSQL(r *renderer) { r.node(o.inner) }
func (o *operandNode) alias() string        { return o.outAlias }

func (o *operandNode) As(a string) adapter.Projection {
	return &operandNode{inner: o.inner, outAlias: a}
}

// exprNode is the full Expression tier: every comparison, range, pattern, and ordering
// operator hangs off it. Its inner sqlWriter renders the underlying value (a column, a
// literal, a function call, a CASE, a CAST, or a scalar subquery).
type exprNode struct {
	inner    sqlWriter
	outAlias string
}

func newExpr(inner sqlWriter) *exprNode { return &exprNode{inner: inner} }

func (e *exprNode) writeSQL(r *renderer) { r.node(e.inner) }
func (e *exprNode) alias() string        { return e.outAlias }

func (e *exprNode) As(a string) adapter.Projection {
	return &exprNode{inner: e.inner, outAlias: a}
}

func (e *exprNode) Compare(op adapter.ComparisonOp, other adapter.Expression) adapter.Predicate {
	return newPredicate(writerFunc(func(r *renderer) {
		r.node(e)
		r.write(" ")
		r.write(comparisonSQL(op))
		r.write(" ")
		r.node(exprWriter(other))
	}))
}

func (e *exprNode) Eq(other adapter.Expression) adapter.Predicate {
	return e.Compare(adapter.OpEq, other)
}
func (e *exprNode) Lt(other adapter.Expression) adapter.Predicate {
	return e.Compare(adapter.OpLt, other)
}
func (e *exprNode) Lte(other adapter.Expression) adapter.Predicate {
	return e.Compare(adapter.OpLte, other)
}
func (e *exprNode) Gt(other adapter.Expression) adapter.Predicate {
	return e.Compare(adapter.OpGt, other)
}
func (e *exprNode) Gte(other adapter.Expression) adapter.Predicate {
	return e.Compare(adapter.OpGte, other)
}

func (e *exprNode) Quantified(op adapter.ComparisonOp, q adapter.Quantifier, right any) adapter.Predicate {
	return newPredicate(writerFunc(func(r *renderer) {
		r.node(e)
		r.write(" ")
		r.write(comparisonSQL(op))
		r.write(" ")
		r.write(quantifierSQL(q))
		r.write(" (")
		switch v := right.(type) {
		case adapter.Query:
			r.node(queryWriter(v))
		case adapter.Expression:
			r.node(exprWriter(v))
		default:
			panic("pkg/storage/adapter/ansi: Quantified right must be a Query or Expression")
		}
		r.write(")")
	}))
}

func (e *exprNode) In(values ...adapter.Expression) adapter.Predicate {
	return newPredicate(writerFunc(func(r *renderer) {
		r.node(e)
		r.write(" IN (")
		for i, v := range values {
			if i > 0 {
				r.write(", ")
			}
			r.node(exprWriter(v))
		}
		r.write(")")
	}))
}

func (e *exprNode) Between(lo, hi adapter.Expression) adapter.Predicate {
	return newPredicate(writerFunc(func(r *renderer) {
		r.node(e)
		r.write(" BETWEEN ")
		r.node(exprWriter(lo))
		r.write(" AND ")
		r.node(exprWriter(hi))
	}))
}

func (e *exprNode) Like(pattern adapter.Expression) adapter.Predicate {
	return newPredicate(writerFunc(func(r *renderer) {
		r.node(e)
		r.write(" LIKE ")
		r.node(exprWriter(pattern))
	}))
}

func (e *exprNode) IsNull() adapter.Predicate {
	return newPredicate(writerFunc(func(r *renderer) {
		r.node(e)
		r.write(" IS NULL")
	}))
}

func (e *exprNode) Order(dir adapter.SortDirection) adapter.OrderTerm {
	return &orderTerm{expr: e, dir: dir}
}
func (e *exprNode) Asc() adapter.OrderTerm  { return e.Order(adapter.Ascending) }
func (e *exprNode) Desc() adapter.OrderTerm { return e.Order(adapter.Descending) }

// litNode is a bound parameter literal: it renders as the dialect's placeholder and
// contributes its Go value to the bind-argument list.
type litNode struct{ value any }

func (l *litNode) writeSQL(r *renderer) { r.bind(l.value) }

// funcNode renders a scalar function call.
type funcNode struct {
	fn   adapter.ScalarFunc
	args []sqlWriter
}

func (f *funcNode) writeSQL(r *renderer) {
	name, jsonPairs := scalarFuncSQL(f.fn)
	r.write(name)
	r.write("(")
	writeArgs(r, f.args, jsonPairs)
	r.write(")")
}

// writeArgs renders a function/aggregate argument list. When jsonPairs is set the args
// are taken pairwise as "key VALUE value" (the ANSI JSON_OBJECT / JSON_OBJECTAGG form).
func writeArgs(r *renderer, args []sqlWriter, jsonPairs bool) {
	if jsonPairs {
		for i := 0; i+1 < len(args); i += 2 {
			if i > 0 {
				r.write(", ")
			}
			r.node(args[i])
			r.write(" VALUE ")
			r.node(args[i+1])
		}
		return
	}
	r.list(args, ", ")
}

// caseNode renders a CASE expression (simple when base is set, otherwise searched).
type caseNode struct {
	base     sqlWriter // simple-CASE operand, nil for searched CASE
	whens    []caseWhen
	elseExpr sqlWriter
}

type caseWhen struct {
	cond   sqlWriter
	result sqlWriter
}

func (c *caseNode) writeSQL(r *renderer) {
	r.write("CASE")
	if c.base != nil {
		r.write(" ")
		r.node(c.base)
	}
	for _, w := range c.whens {
		r.write(" WHEN ")
		r.node(w.cond)
		r.write(" THEN ")
		r.node(w.result)
	}
	if c.elseExpr != nil {
		r.write(" ELSE ")
		r.node(c.elseExpr)
	}
	r.write(" END")
}

// caseBuilder is the CaseExpr fork: a CASE whose form is not yet chosen. It is not an
// Expression — Value or When commits to a form and returns the matching variant, so the
// two forms cannot be mixed and an empty (formless) CASE is never a value.
type caseBuilder struct {
	node *caseNode
}

func newCase() *caseBuilder { return &caseBuilder{node: &caseNode{}} }

func (c *caseBuilder) Value(operand adapter.Expression) adapter.SimpleCaseExpr {
	c.node.base = exprWriter(operand)
	return &simpleCase{exprNode: newExpr(c.node), node: c.node}
}

func (c *caseBuilder) When(condition adapter.Predicate, result adapter.Expression) adapter.SearchedCaseExpr {
	c.node.whens = append(c.node.whens, caseWhen{cond: predWriter(condition), result: exprWriter(result)})
	return &searchedCase{exprNode: newExpr(c.node), node: c.node}
}

// simpleCase builds a simple CASE. It embeds the exprNode wrapping its caseNode, so the
// full Expression algebra is promoted; its own methods add the branch builders.
type simpleCase struct {
	*exprNode
	node *caseNode
}

func (c *simpleCase) When(value, result adapter.Expression) adapter.SimpleCaseExpr {
	c.node.whens = append(c.node.whens, caseWhen{cond: exprWriter(value), result: exprWriter(result)})
	return c
}

func (c *simpleCase) Else(result adapter.Expression) adapter.SimpleCaseExpr {
	c.node.elseExpr = exprWriter(result)
	return c
}

// searchedCase builds a searched CASE, mirroring simpleCase but with boolean branch
// conditions.
type searchedCase struct {
	*exprNode
	node *caseNode
}

func (c *searchedCase) When(condition adapter.Predicate, result adapter.Expression) adapter.SearchedCaseExpr {
	c.node.whens = append(c.node.whens, caseWhen{cond: predWriter(condition), result: exprWriter(result)})
	return c
}

func (c *searchedCase) Else(result adapter.Expression) adapter.SearchedCaseExpr {
	c.node.elseExpr = exprWriter(result)
	return c
}
