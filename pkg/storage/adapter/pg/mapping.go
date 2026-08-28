package pg

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
)

// This file holds PostgreSQL's SQL spellings of the ast constructs. It is deliberately
// self-contained: the constructs SQL engines spell identically (operators, connectives, the
// inline-literal form) are COPIED here as unexported helpers rather than imported from a
// shared package, so this production adapter carries no dependency on the throwaway spike
// tree. A little copying is better than that dependency.
//
// The constructs that genuinely diverge — columns, cast targets, scalar and aggregate function
// names — are PostgreSQL's own, and each switch below is exhaustive with no default
// pass-through, so adding a logical construct forces this adapter to state how it is spelled.

// --- PostgreSQL's own mappings (divergent across engines) -------------------------------

// pgColumn maps a logical column to PostgreSQL SQL. Because ast.ColNode carries no SQL text,
// this adapter owns the FULL mapping — physical names included, not just the divergent bits.
//
// The three subject columns are decoded from the packed `_user` column with PostgreSQL's
// split_part, a proprietary function that reads more directly than the portable
// SUBSTRING/POSITION surgery: split_part returns the empty string when the delimiter is absent,
// which is exactly the wanted behaviour for a subject that carries no "#relation" suffix.
func pgColumn(name ast.Column, alias string) string {
	u := alias + "._user"
	switch name {
	case ast.ColSubjectType:
		// Everything before the first ':'.
		return "split_part(" + u + ", ':', 1)"
	case ast.ColSubjectID:
		// The segment between the first ':' and the optional '#relation' suffix.
		afterColon := "substring(" + u + " FROM position(':' IN " + u + ") + 1)"
		return "split_part(" + afterColon + ", '#', 1)"
	case ast.ColSubjectRelation:
		// The segment after '#', or '' when the subject is not a userset.
		return "split_part(" + u + ", '#', 2)"
	case ast.ColObjectType:
		return alias + ".object_type"
	case ast.ColObjectID:
		return alias + ".object_id"
	case ast.ColObjectRelation:
		return alias + ".relation"
	case ast.ColStore:
		return alias + ".store"
	case ast.ColCondition:
		return alias + ".condition_name"
	case ast.ColConditionContext:
		return alias + ".condition_context"
	default:
		panic(fmt.Sprintf("pg: unmapped column %v", name))
	}
}

// pgType spells each cast target PostgreSQL's way: VARCHAR is text and VARBINARY is bytea,
// while the numeric targets match the standard spelling.
func pgType(t ast.CastType) string {
	switch t {
	case ast.TypeVarchar:
		return "text"
	case ast.TypeInteger:
		return "INTEGER"
	case ast.TypeBigint:
		return "BIGINT"
	case ast.TypeNumeric:
		return "NUMERIC"
	case ast.TypeVarbinary:
		return "bytea"
	default:
		panic(fmt.Sprintf("pg: unmapped cast target %v", t))
	}
}

// pgFunc spells each scalar function PostgreSQL's way. The JSON array constructor is
// jsonb_build_array; the JSON object constructor is not in this enum (its arguments are pairs)
// and is spelled at its case in renderer.value.
func pgFunc(f ast.ScalarFunc) string {
	switch f {
	case ast.FuncCoalesce:
		return "COALESCE"
	case ast.FuncLower:
		return "LOWER"
	case ast.FuncUpper:
		return "UPPER"
	case ast.FuncJSONArray:
		return "jsonb_build_array"
	default:
		panic(fmt.Sprintf("pg: unmapped scalar function %v", f))
	}
}

// aggFunc spells each value-producing aggregate PostgreSQL's way. COUNT is the only aggregate
// the tightened AST models, and it matches the standard spelling.
func aggFunc(a ast.AggFunc) string {
	switch a {
	case ast.AggCount:
		return "COUNT"
	default:
		panic(fmt.Sprintf("pg: unmapped aggregate %v", a))
	}
}

// --- shared spellings, copied locally (invariant across SQL engines) --------------------

// op renders a comparison operator.
func op(o ast.Op) string {
	switch o {
	case ast.OpEq:
		return "="
	case ast.OpNe:
		return "<>"
	case ast.OpLt:
		return "<"
	case ast.OpLte:
		return "<="
	case ast.OpGt:
		return ">"
	case ast.OpGte:
		return ">="
	default:
		panic("pg: bad ast.Op " + strconv.Itoa(int(o)))
	}
}

// logical renders a boolean connective keyword.
func logical(l ast.LogicalOp) string {
	switch l {
	case ast.And:
		return "AND"
	case ast.Or:
		return "OR"
	default:
		panic("pg: bad ast.LogicalOp " + strconv.Itoa(int(l)))
	}
}

// quantifier renders the ANY / ALL keyword.
func quantifier(q ast.Quantifier) string {
	switch q {
	case ast.Any:
		return "ANY"
	case ast.All:
		return "ALL"
	default:
		panic("pg: bad ast.Quantifier " + strconv.Itoa(int(q)))
	}
}

// joinKeyword renders the join keywords. The ast.JoinType enum carries only the flavours every
// SQL target renders (INNER, LEFT OUTER, CROSS).
func joinKeyword(j ast.JoinType) string {
	switch j {
	case ast.JoinInner:
		return "INNER JOIN"
	case ast.JoinLeftOuter:
		return "LEFT OUTER JOIN"
	case ast.JoinCross:
		return "CROSS JOIN"
	default:
		panic("pg: bad ast.JoinType " + strconv.Itoa(int(j)))
	}
}

// sortDirection renders the direction keyword, or "" for the default (ascending). Callers must
// guard on the empty string rather than concatenating unconditionally.
func sortDirection(s ast.SortDirection) string {
	switch s {
	case ast.Ascending:
		return ""
	case ast.Descending:
		return "DESC"
	default:
		panic("pg: bad ast.SortDirection " + strconv.Itoa(int(s)))
	}
}

// literal renders an ast.LitNode value as inline SQL text. It dispatches on reflect.Kind, not
// concrete type, so a NAMED scalar (type storeID string) is inlined as its underlying kind.
// Kinds outside package query's Literal constraint are rejected: a type whose literal spelling
// diverges across engines — a timestamp above all — must be bound, not inlined, and panicking
// here is how that stays true instead of silently emitting a Go-formatted value as SQL.
func literal(v any) string {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		return quote(rv.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64)
	default:
		panic(fmt.Sprintf("pg: value of type %T cannot be inlined as a literal; bind it instead", v))
	}
}

// quote renders a string as a single-quoted SQL literal, doubling embedded quotes.
func quote(s string) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, '\'')
	for i := range len(s) {
		if s[i] == '\'' {
			out = append(out, '\'')
		}
		out = append(out, s[i])
	}
	return string(append(out, '\''))
}
