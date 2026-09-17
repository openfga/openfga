package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
)

// renderer accumulates SQL text and the positional bind arguments its "?" placeholders refer
// to, as it walks the ast tree.
type renderer struct {
	sb   strings.Builder
	args []any
}

func (r *renderer) write(s string) { r.sb.WriteString(s) }

func (r *renderer) bind(v any) {
	r.args = append(r.args, v)
	r.write("?") // MySQL is always positional "?"
}

// --- statement --------------------------------------------------------------------------

func (r *renderer) selectStmt(s *ast.Select) {
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
			r.write("tuple " + t.Alias)
		}
	}
	for _, j := range s.Joins {
		r.write(" " + joinKeyword(j.Type) + " tuple " + j.Table.Alias)
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
		// MySQL requires a LIMIT before OFFSET. The largest unsigned-64-bit value is the
		// idiomatic "no limit" stand-in.
		if s.Limit == nil {
			r.write(" LIMIT 18446744073709551615")
		}
		r.write(" OFFSET " + strconv.FormatUint(*s.Offset, 10))
	}
}

// clause renders a keyword-introduced predicate, or nothing when absent. The predicate carries
// its own grouping, so a nil check is the whole contract.
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

// --- nodes ------------------------------------------------------------------------------
//
// One walk function per POSITION CATEGORY, matching ast's four category interfaces. Splitting
// the walk this way means each switch covers one category, so its default panic is reachable
// only for a genuinely unknown node kind, and the recursion's position is the function it is
// in rather than something to track.

// predicate renders a truth-valued node.
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
		panic(fmt.Sprintf("mysql: unhandled predicate %T", n))
	}
}

// value renders a single-valued node.
func (r *renderer) value(n ast.ScalarValue) {
	switch x := n.(type) {
	case ast.ColNode:
		r.write(mysqlColumn(x.Name, x.Alias))

	case ast.BindNode:
		r.bind(x.Value)

	case ast.LitNode:
		r.write(literal(x.Value))

	case ast.CastNode:
		r.write("CAST(")
		r.value(x.Inner)
		r.write(" AS " + mysqlType(x.Type) + ")")

	case ast.FuncNode:
		r.write(mysqlFunc(x.Fn) + "(")
		r.values(x.Args)
		r.write(")")

	case ast.JSONObjectNode:
		r.write("JSON_OBJECT(")
		for i, p := range x.Pairs {
			if i > 0 {
				r.write(", ")
			}
			r.jsonPair(p)
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
		panic(fmt.Sprintf("mysql: unhandled value %T", n))
	}
}

// set renders a set-valued node. Its only caller is quantified, for the non-bound flavours.
func (r *renderer) set(n ast.SetValue) {
	switch x := n.(type) {
	case ast.SetBindNode:
		r.write("(")
		for i, e := range x.Elems {
			if i > 0 {
				r.write(", ")
			}
			r.bind(e)
		}
		r.write(")")

	case ast.SubqueryNode:
		r.selectStmt(x.Stmt)

	default:
		panic(fmt.Sprintf("mysql: unhandled set %T", n))
	}
}

// projection renders a SELECT-list item.
func (r *renderer) projection(n ast.Projection) {
	switch x := n.(type) {
	case ast.AliasNode:
		r.projection(x.Inner)
		r.write(" AS " + x.Alias)

	default:
		// Everything else in the category is a ScalarValue, ScalarValue embedding Projection.
		v, ok := n.(ast.ScalarValue)
		if !ok {
			panic(fmt.Sprintf("mysql: unhandled projection %T", n))
		}
		r.value(v)
	}
}

// jsonPair renders one JSON object pair. MySQL's JSON_OBJECT takes a flat "k, v" argument list,
// not the ANSI "k VALUE v" — which is why ast keeps the pair as a node instead of flattening it
// at construction time.
func (r *renderer) jsonPair(p ast.JSONPairNode) {
	r.value(p.Key)
	r.write(", ")
	r.value(p.Value)
}

// filtered writes an aggregate's argument, wrapped in the FILTER emulation when there is a
// filter. MySQL has no FILTER clause, so a filtered aggregate aggregates a CASE that yields the
// argument only when the filter holds and NULL otherwise; aggregates skip NULLs, so filtered-out
// rows do not contribute, exactly as FILTER would.
func (r *renderer) filtered(filter ast.Predicate, arg ast.ScalarValue) {
	if filter == nil {
		r.value(arg)
		return
	}
	r.write("CASE WHEN ")
	r.predicate(filter)
	r.write(" THEN ")
	r.value(arg)
	r.write(" END")
}

func (r *renderer) caseElse(e ast.ScalarValue) {
	if e != nil {
		r.write(" ELSE ")
		r.value(e)
	}
	r.write(" END")
}

// aggregate renders a value-producing aggregate, emulating FILTER. COUNT(*) has no argument to
// wrap, so a filtered one becomes COUNT(CASE WHEN <filter> THEN 1 END).
func (r *renderer) aggregate(x ast.AggNode) {
	r.write(mysqlAgg(x.Fn) + "(")
	if x.Distinct {
		r.write("DISTINCT ")
	}

	switch {
	case len(x.Args) == 0 && x.Filter != nil:
		r.write("CASE WHEN ")
		r.predicate(x.Filter)
		r.write(" THEN 1 END")
	case len(x.Args) == 0:
		r.write("*")
	default:
		for i, a := range x.Args {
			if i > 0 {
				r.write(", ")
			}
			r.filtered(x.Filter, a)
		}
	}

	r.write(")")
}

// quantified renders a quantified comparison. A bound set is ALWAYS expanded, since MySQL has
// no array operand.
func (r *renderer) quantified(x ast.QuantifiedNode) {
	sb, ok := x.Set.(ast.SetBindNode)
	if !ok {
		// A non-bound set keeps the standard quantified form.
		r.value(x.Left)
		r.write(" " + op(x.Op) + " " + quantifier(x.Q) + " (")
		r.set(x.Set)
		r.write(")")
		return
	}

	elems := sb.Elems
	if len(elems) == 0 {
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
