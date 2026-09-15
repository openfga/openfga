package pg

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
)

// renderer accumulates SQL text and the positional bind arguments its "$N" placeholders refer
// to, as it walks the ast tree.
type renderer struct {
	sb   strings.Builder
	args []any
}

func (r *renderer) write(s string) { r.sb.WriteString(s) }

func (r *renderer) bind(v any) {
	r.args = append(r.args, v)
	r.write("$" + strconv.Itoa(len(r.args))) // PostgreSQL ordinal placeholder
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
		// A CROSS join carries no condition; every other flavour does.
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
	// Pointers, so "no LIMIT" stays distinct from "LIMIT 0". PostgreSQL accepts OFFSET on its
	// own, so — unlike MySQL — no synthetic LIMIT is needed ahead of it.
	if s.Limit != nil {
		r.write(" LIMIT " + strconv.FormatUint(*s.Limit, 10))
	}
	if s.Offset != nil {
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
// One walk function per POSITION CATEGORY, matching ast's four category interfaces. Each switch
// covers one category, so its default panic is reachable only for a genuinely unknown node kind,
// and the recursion's position is the function it is in rather than something to track.

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
		panic(fmt.Sprintf("pg: unhandled predicate %T", n))
	}
}

// value renders a single-valued node.
func (r *renderer) value(n ast.ScalarValue) {
	switch x := n.(type) {
	case ast.ColNode:
		r.write(pgColumn(x.Name, x.Alias))

	case ast.BindNode:
		r.bind(x.Value)

	case ast.LitNode:
		r.write(literal(x.Value))

	case ast.CastNode:
		r.write("CAST(")
		r.value(x.Inner)
		r.write(" AS " + pgType(x.Type) + ")")

	case ast.FuncNode:
		r.write(pgFunc(x.Fn) + "(")
		r.values(x.Args)
		r.write(")")

	case ast.JSONObjectNode:
		// PostgreSQL spells the constructor jsonb_build_object and takes a flat "k, v" list.
		r.write("jsonb_build_object(")
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
		panic(fmt.Sprintf("pg: unhandled value %T", n))
	}
}

// set renders a set-valued node. Its only caller is quantified, for the non-bound flavours (a
// bound set is handled inline as a "= ANY ($N)" array parameter).
func (r *renderer) set(n ast.SetValue) {
	switch x := n.(type) {
	case ast.SubqueryNode:
		r.selectStmt(x.Stmt)

	default:
		panic(fmt.Sprintf("pg: unhandled set %T", n))
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
			panic(fmt.Sprintf("pg: unhandled projection %T", n))
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

// aggregate renders a value-producing aggregate. PostgreSQL supports the FILTER (WHERE ...)
// clause natively, so a filtered aggregate emits it directly rather than emulating it with a
// CASE — the divergence that forces MySQL to own its renderer.
func (r *renderer) aggregate(x ast.AggNode) {
	r.write(aggFunc(x.Fn) + "(")
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

// quantified renders a quantified comparison. A bound set is PostgreSQL's real advantage here:
// it binds the whole slice as ONE array parameter and compares with "<op> <quant> ($N)", which
// pgx encodes as a PostgreSQL array — no per-element expansion.
func (r *renderer) quantified(x ast.QuantifiedNode) {
	r.value(x.Left)
	r.write(" " + op(x.Op) + " " + quantifier(x.Q) + " (")
	if sb, ok := x.Set.(ast.SetBindNode); ok {
		r.bind(sb.Elems)
	} else {
		r.set(x.Set)
	}
	r.write(")")
}
