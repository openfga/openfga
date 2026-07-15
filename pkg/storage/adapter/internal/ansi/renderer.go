// Package ansi is an extensible, concrete implementation of the driver interfaces
// that renders an ANSI SQL query string and its bind arguments. It owns the full node
// algebra and rendering, but not execution: a Builder is constructed with an Executor
// seam, and every Query additionally exposes Build, which returns the rendered SQL text
// and ordinal bind arguments without running anything.
//
// This lets engine-specific packages reuse all of its rendering and supply only an
// Executor that feeds Build's output into their own driver, plus a Dialect that
// overrides the few constructs where their engine diverges from ANSI (placeholders and
// the subject view). See pkg/storage/adapter/pg for such a package, which targets PostgreSQL over
// the native pgx/v5 driver.
//
// By default statements render under the standard dialect: portable, standard-conforming
// SQL with "?" placeholders over the standard schema. Pass WithDialect to New to target
// an engine.
package ansi

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
// placeholders. Its dialect supplies the placeholder form and the per-column SQL — the
// engine-specific fragments nodes consult while rendering.
type renderer struct {
	sb      strings.Builder
	args    []any
	dialect Dialect
}

// write appends raw SQL text.
func (r *renderer) write(s string) { r.sb.WriteString(s) }

// node renders a child node into this renderer.
func (r *renderer) node(n sqlWriter) { n.writeSQL(r) }

// bind records a positional argument and emits the dialect's placeholder for it.
func (r *renderer) bind(v any) {
	r.args = append(r.args, v)
	r.write(r.dialect.Placeholder(len(r.args)))
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

// render runs a top-level query node under the given dialect and returns the SQL text
// and its bind arguments.
func render(n sqlWriter, dialect Dialect) (string, []any) {
	r := &renderer{dialect: dialect}
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
		panic("pkg/storage/adapter/internal/ansi: unknown ComparisonOp")
	}
}

func quantifierSQL(q adapter.Quantifier) string {
	switch q {
	case adapter.QuantifierAny:
		return "ANY"
	case adapter.QuantifierAll:
		return "ALL"
	default:
		panic("pkg/storage/adapter/internal/ansi: unknown Quantifier")
	}
}

func sortSQL(d adapter.SortDirection) string {
	switch d {
	case adapter.Ascending:
		return "ASC"
	case adapter.Descending:
		return "DESC"
	default:
		panic("pkg/storage/adapter/internal/ansi: unknown SortDirection")
	}
}

func nullsSQL(n adapter.NullOrdering) string {
	switch n {
	case adapter.NullsDefault:
		return ""
	case adapter.NullsFirst:
		return "NULLS FIRST"
	case adapter.NullsLast:
		return "NULLS LAST"
	default:
		panic("pkg/storage/adapter/internal/ansi: unknown NullOrdering")
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
		return "FULL OUTER JOIN"
	case adapter.CrossJoinType:
		return "CROSS JOIN"
	default:
		panic("pkg/storage/adapter/internal/ansi: unknown JoinType")
	}
}

func setSQL(op adapter.SetOp) string {
	switch op {
	case adapter.SetUnion:
		return "UNION"
	case adapter.SetIntersect:
		return "INTERSECT"
	case adapter.SetExcept:
		return "EXCEPT"
	default:
		panic("pkg/storage/adapter/internal/ansi: unknown SetOp")
	}
}

// scalarFuncSQL maps a ScalarFunc to its ANSI function name. The jsonPairs result
// reports whether the arguments are rendered as "k VALUE v" pairs (JSON object
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
		panic("pkg/storage/adapter/internal/ansi: unknown ScalarFunc")
	}
}

// aggregateFuncSQL maps an AggregateFunc to its ANSI name. The jsonPairs result reports
// whether the arguments are rendered as a "k VALUE v" pair (JSON object aggregation).
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
		panic("pkg/storage/adapter/internal/ansi: unknown AggregateFunc")
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
	panic("pkg/storage/adapter/internal/ansi: value did not originate from this builder")
}

func exprWriter(e adapter.Expression) sqlWriter { return asWriter(e) }
func operandWriter(o adapter.Operand) sqlWriter { return asWriter(o) }
func predWriter(p adapter.Predicate) sqlWriter  { return asWriter(p) }
func queryWriter(q adapter.Query) sqlWriter     { return asWriter(q) }
func projWriter(p adapter.Projection) aliased   { return p.(aliased) }
