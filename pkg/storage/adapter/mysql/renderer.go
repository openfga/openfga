package mysql

import (
	"strings"

	"github.com/openfga/openfga/pkg/storage/adapter"
)

// sqlWriter is the internal contract every node satisfies: append its SQL fragment
// (and any bind arguments) to the renderer.
type sqlWriter interface {
	writeSQL(r *renderer)
}

// writerFunc adapts a closure to a sqlWriter, so a node can be built inline without a
// dedicated struct.
type writerFunc func(r *renderer)

func (f writerFunc) writeSQL(r *renderer) { f(r) }

// renderer accumulates SQL text and the positional bind arguments that back its
// placeholders. MySQL uses "?" placeholders, emitted inline as arguments are bound.
type renderer struct {
	sb   strings.Builder
	args []any
}

// write appends raw SQL text.
func (r *renderer) write(s string) { r.sb.WriteString(s) }

// node renders a child node into this renderer.
func (r *renderer) node(n sqlWriter) { n.writeSQL(r) }

// bind records a positional argument and emits MySQL's "?" placeholder for it.
func (r *renderer) bind(v any) {
	r.args = append(r.args, v)
	r.write("?")
}

// list renders nodes separated by sep.
func (r *renderer) list(nodes []sqlWriter, sep string) {
	for i, n := range nodes {
		if i > 0 {
			r.write(sep)
		}
		n.writeSQL(r)
	}
}

// render runs a top-level query node and returns the SQL text and its bind arguments.
func render(n sqlWriter) (string, []any) {
	r := &renderer{}
	n.writeSQL(r)
	return r.sb.String(), r.args
}

// --- enum → SQL keyword maps ---

func comparisonSQL(op adapter.ComparisonOp) string {
	switch op {
	case adapter.OpEq:
		return "="
	case adapter.OpNotEq:
		return "<>"
	case adapter.OpLt:
		return "<"
	case adapter.OpLte:
		return "<="
	case adapter.OpGt:
		return ">"
	case adapter.OpGte:
		return ">="
	default:
		panic("pkg/storage/adapter/mysql: unknown ComparisonOp")
	}
}

func quantifierSQL(q adapter.Quantifier) string {
	switch q {
	case adapter.QuantifierAny:
		return "ANY"
	case adapter.QuantifierAll:
		return "ALL"
	default:
		panic("pkg/storage/adapter/mysql: unknown Quantifier")
	}
}

func sortSQL(d adapter.SortDirection) string {
	switch d {
	case adapter.Ascending:
		return "ASC"
	case adapter.Descending:
		return "DESC"
	default:
		panic("pkg/storage/adapter/mysql: unknown SortDirection")
	}
}

func joinSQL(jt adapter.JoinType) string {
	switch jt {
	case adapter.InnerJoin:
		return "INNER JOIN"
	case adapter.LeftOuterJoin:
		return "LEFT OUTER JOIN"
	case adapter.RightOuterJoin:
		return "RIGHT OUTER JOIN"
	case adapter.FullOuterJoin:
		// MySQL has no FULL OUTER JOIN; the keyword renders as-is and will fail at
		// execution. The Check engine does not use it.
		return "FULL OUTER JOIN"
	case adapter.CrossJoinType:
		return "CROSS JOIN"
	default:
		panic("pkg/storage/adapter/mysql: unknown JoinType")
	}
}

func setSQL(op adapter.SetOp) string {
	switch op {
	case adapter.SetUnion:
		return "UNION"
	case adapter.SetIntersect:
		// INTERSECT / EXCEPT require MySQL 8.0.31+; the keywords render as-is. The Check
		// engine does not use them.
		return "INTERSECT"
	case adapter.SetExcept:
		return "EXCEPT"
	default:
		panic("pkg/storage/adapter/mysql: unknown SetOp")
	}
}

// scalarFuncSQL maps a ScalarFunc to its MySQL function name. The jsonPairs result reports
// whether the arguments are rendered as comma-separated "k, v" pairs (JSON object
// construction).
func scalarFuncSQL(fn adapter.ScalarFunc) (name string, jsonPairs bool) {
	switch fn {
	case adapter.FuncCoalesce:
		return "COALESCE", false
	case adapter.FuncJSONObject:
		return "JSON_OBJECT", true
	case adapter.FuncJSONArray:
		return "JSON_ARRAY", false
	default:
		panic("pkg/storage/adapter/mysql: unknown ScalarFunc")
	}
}

// aggregateFuncSQL maps an AggregateFunc to its MySQL name. The jsonPairs result reports
// whether the arguments are rendered as a comma-separated "k, v" pair (JSON object
// aggregation).
func aggregateFuncSQL(fn adapter.AggregateFunc) (name string, jsonPairs bool) {
	switch fn {
	case adapter.AggCount:
		return "COUNT", false
	case adapter.AggEvery:
		return "EVERY", false
	case adapter.AggAny:
		return "ANY", false
	case adapter.AggArrayAgg:
		return "ARRAY_AGG", false
	case adapter.AggJSONObjectAgg:
		return "JSON_OBJECTAGG", true
	case adapter.AggJSONArrayAgg:
		return "JSON_ARRAYAGG", false
	default:
		panic("pkg/storage/adapter/mysql: unknown AggregateFunc")
	}
}

// --- public-interface → sqlWriter adapters ---
//
// Every value returned by this package's factories is one of its own node types, so
// these assertions are total; a foreign implementation passed across the boundary is a
// programming error and panics.

func asWriter(v any) sqlWriter {
	if w, ok := v.(sqlWriter); ok {
		return w
	}
	panic("pkg/storage/adapter/mysql: value did not originate from this builder")
}

func exprWriter(e adapter.Expression) sqlWriter { return asWriter(e) }
func operandWriter(o adapter.Operand) sqlWriter { return asWriter(o) }
func predWriter(p adapter.Predicate) sqlWriter  { return asWriter(p) }
func queryWriter(q adapter.Query) sqlWriter     { return asWriter(q) }
func projWriter(p adapter.Projection) aliased   { return p.(aliased) }
