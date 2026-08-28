package mysql

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
)

// This file holds MySQL's SQL spellings of the ast constructs. It is deliberately
// self-contained: the constructs SQL engines spell identically (operators, connectives, the
// inline-literal form) are COPIED here as unexported helpers rather than imported from a
// shared package, so this production adapter carries no dependency on the throwaway spike
// tree. A little copying is better than that dependency.
//
// The constructs that genuinely diverge across engines — columns, cast targets, scalar and
// aggregate function names — are MySQL's own, and each switch below is exhaustive with no
// default pass-through, so adding a logical construct forces this adapter to state how it is
// spelled rather than guessing.

// --- MySQL's own mappings (divergent across engines) ------------------------------------

// mysqlColumn maps a logical column to MySQL SQL. Because ast.ColNode carries no SQL text,
// this adapter owns the FULL mapping — physical names included, not just the divergent bits.
//
// The three subject columns are decoded from the packed `_user` column with MySQL's own string
// functions.
func mysqlColumn(name ast.Column, alias string) string {
	u := alias + "._user"
	switch name {
	case ast.ColSubjectType:
		return "SUBSTRING_INDEX(" + u + ", ':', 1)"
	case ast.ColSubjectID:
		afterColon := "SUBSTRING_INDEX(" + u + ", ':', -1)"
		return "SUBSTRING_INDEX(" + afterColon + ", '#', 1)"
	case ast.ColSubjectRelation:
		return "IF(LOCATE('#', " + u + ") = 0, '', SUBSTRING_INDEX(" + u + ", '#', -1))"
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
		panic(fmt.Sprintf("mysql: unmapped column %v", name))
	}
}

// mysqlType spells each cast target MySQL's way. MySQL's CAST accepts a narrow set of target
// types that barely overlaps the ANSI names: there is no VARCHAR target (it is CHAR) and no
// VARBINARY (it is BINARY).
func mysqlType(t ast.CastType) string {
	switch t {
	case ast.TypeVarchar:
		return "CHAR"
	case ast.TypeInteger, ast.TypeBigint:
		return "SIGNED"
	case ast.TypeNumeric:
		return "DECIMAL"
	case ast.TypeVarbinary:
		return "BINARY"
	default:
		panic(fmt.Sprintf("mysql: unmapped cast target %v", t))
	}
}

// mysqlFunc spells each scalar function MySQL's way. JSON_ARRAY happens to match the ANSI name;
// the JSON object constructor is not in this enum at all, since its arguments are pairs — it is
// spelled at its case in renderer.value, where its argument form is also decided.
func mysqlFunc(f ast.ScalarFunc) string {
	switch f {
	case ast.FuncCoalesce:
		return "COALESCE"
	case ast.FuncLower:
		return "LOWER"
	case ast.FuncUpper:
		return "UPPER"
	case ast.FuncJSONArray:
		return "JSON_ARRAY"
	default:
		panic(fmt.Sprintf("mysql: unmapped scalar function %v", f))
	}
}

// mysqlAgg spells each value-producing aggregate MySQL's way. COUNT is the only aggregate the
// tightened AST models, and it happens to match the ANSI spelling.
func mysqlAgg(a ast.AggFunc) string {
	switch a {
	case ast.AggCount:
		return "COUNT"
	default:
		panic(fmt.Sprintf("mysql: unmapped aggregate %v", a))
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
		panic("mysql: bad ast.Op " + strconv.Itoa(int(o)))
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
		panic("mysql: bad ast.LogicalOp " + strconv.Itoa(int(l)))
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
		panic("mysql: bad ast.Quantifier " + strconv.Itoa(int(q)))
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
		panic("mysql: bad ast.JoinType " + strconv.Itoa(int(j)))
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
		panic("mysql: bad ast.SortDirection " + strconv.Itoa(int(s)))
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
		panic(fmt.Sprintf("mysql: value of type %T cannot be inlined as a literal; bind it instead", v))
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
