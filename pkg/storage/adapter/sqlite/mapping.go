package sqlite

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
)

// This file holds SQLite's SQL spellings of the ast constructs. It is deliberately
// self-contained: the constructs SQL engines spell identically (operators, connectives, the
// inline-literal form) are COPIED here as unexported helpers rather than imported from a
// shared package, so this production adapter carries no dependency on the throwaway spike
// tree. A little copying is better than that dependency.
//
// The constructs that genuinely diverge across engines — columns, cast targets, scalar and
// aggregate function names — are SQLite's own, and each switch below is exhaustive with no
// default pass-through, so adding a logical construct forces this adapter to state how it is
// spelled rather than guessing.

// --- SQLite's own mappings (divergent across engines) -----------------------------------

// sqliteColumn maps a logical column to SQLite SQL. Because ast.ColNode carries no SQL text,
// this adapter owns the FULL mapping — physical names included, not just the divergent bits.
//
// SQLite's `tuple` table stores the subject DISCRETELY across user_object_type /
// user_object_id / user_relation columns (see assets/migrations/sqlite), so each subject
// field is a plain column reference — no packed-`_user` decode, the string surgery MySQL and
// PostgreSQL are forced into. (The physical user_type column — 'user' / 'userset' / wildcard —
// is not part of the logical view and is never referenced here.)
func sqliteColumn(name ast.Column, alias string) string {
	switch name {
	case ast.ColSubjectType:
		return alias + ".user_object_type"
	case ast.ColSubjectID:
		return alias + ".user_object_id"
	case ast.ColSubjectRelation:
		return alias + ".user_relation"
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
		panic(fmt.Sprintf("sqlite: unmapped column %v", name))
	}
}

// sqliteType spells each cast target as one of SQLite's type affinities. SQLite has a single
// integer affinity, so both INTEGER and BIGINT map to INTEGER; NUMERIC maps to REAL and the
// binary target to BLOB.
func sqliteType(t ast.CastType) string {
	switch t {
	case ast.TypeVarchar:
		return "TEXT"
	case ast.TypeInteger, ast.TypeBigint:
		return "INTEGER"
	case ast.TypeNumeric:
		return "REAL"
	case ast.TypeVarbinary:
		return "BLOB"
	default:
		panic(fmt.Sprintf("sqlite: unmapped cast target %v", t))
	}
}

// sqliteFunc spells each scalar function SQLite's way. The JSON array constructor is
// json_array; the JSON object constructor is not in this enum (its arguments are pairs) and is
// spelled at its case in renderer.value.
func sqliteFunc(f ast.ScalarFunc) string {
	switch f {
	case ast.FuncCoalesce:
		return "COALESCE"
	case ast.FuncLower:
		return "LOWER"
	case ast.FuncUpper:
		return "UPPER"
	case ast.FuncJSONArray:
		return "json_array"
	default:
		panic(fmt.Sprintf("sqlite: unmapped scalar function %v", f))
	}
}

// sqliteAgg spells each value-producing aggregate SQLite's way. COUNT is the only aggregate the
// tightened AST models, and it matches the standard spelling.
func sqliteAgg(a ast.AggFunc) string {
	switch a {
	case ast.AggCount:
		return "COUNT"
	default:
		panic(fmt.Sprintf("sqlite: unmapped aggregate %v", a))
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
		panic("sqlite: bad ast.Op " + strconv.Itoa(int(o)))
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
		panic("sqlite: bad ast.LogicalOp " + strconv.Itoa(int(l)))
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
		panic("sqlite: bad ast.Quantifier " + strconv.Itoa(int(q)))
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
		panic("sqlite: bad ast.JoinType " + strconv.Itoa(int(j)))
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
		panic("sqlite: bad ast.SortDirection " + strconv.Itoa(int(s)))
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
		panic(fmt.Sprintf("sqlite: value of type %T cannot be inlined as a literal; bind it instead", v))
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
