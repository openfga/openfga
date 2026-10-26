package sqlite

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
)

// --- SQLite's own mappings --------------------------------------------------------------

func sqliteColumn(name ast.Column, alias string) string {
	alias = quoteAlias(alias)
	switch name {
	case ast.ColSubjectType:
		return alias + ".user_object_type"
	case ast.ColSubjectID:
		return alias + ".user_object_id"
	case ast.ColSubjectRelation:
		return alias + ".user_relation"
	case ast.ColSubjectKind:
		return alias + ".user_type"
	case ast.ColObjectType:
		return alias + ".object_type"
	case ast.ColObjectID:
		return alias + ".object_id"
	case ast.ColObjectRelation:
		return alias + ".relation"
	case ast.ColStore:
		return alias + ".store"
	case ast.ColCondition:
		// Use coalesce to handle NULL condition_name.
		return "COALESCE(" + alias + ".condition_name, '')"
	case ast.ColConditionContext:
		return alias + ".condition_context"
	default:
		panic(fmt.Sprintf("sqlite: unmapped column %v", name))
	}
}

func sqliteType(t ast.CastType) string {
	switch t {
	case ast.TypeVarchar:
		return "TEXT"
	case ast.TypeInteger, ast.TypeBigint:
		// SQLite has one integer affinity.
		return "INTEGER"
	case ast.TypeNumeric:
		return "REAL"
	case ast.TypeVarbinary:
		return "BLOB"
	default:
		panic(fmt.Sprintf("sqlite: unmapped cast target %v", t))
	}
}

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

func sqliteAgg(a ast.AggFunc) string {
	switch a {
	case ast.AggCount:
		return "COUNT"
	default:
		panic(fmt.Sprintf("sqlite: unmapped aggregate %v", a))
	}
}

// --- shared spellings (invariant across SQL engines) ------------------------------------

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

// sortDirection returns "" for ascending, the SQL default.
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

// literal switches on reflect.Kind so named scalar types (type storeID string) inline too.
// Other kinds panic: types whose literal spelling varies by engine (timestamps) must be bound.
func literal(v any) string {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		return quoteLit(rv.String())
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

func quoteLit(s string) string { return quoteWith(s, '\'') }

func quoteAlias(s string) string { return quoteWith(s, '"') }

func quoteWith(s string, q byte) string {
	out := make([]byte, 0, len(s)+2)
	out = append(out, q)
	for i := range len(s) {
		if s[i] == q {
			out = append(out, q)
		}
		out = append(out, s[i])
	}
	return string(append(out, q))
}
