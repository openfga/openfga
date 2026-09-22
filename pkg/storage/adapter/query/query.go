// Package query is a compile-time type-safe construction surface for the query AST
// (package ast). It exposes generic functions rather than methods, because only a function
// may introduce a type parameter, and that parameter carries operand types through the
// algebra so the compiler enforces them (Eq(stringCol, Bind(5)) does not compile).
//
// Five typed wrappers model everything:
//
//	Expr[T]     — a SQL value of Go type T, T constrained to Scalar (a column, a bound value,
//	              a function result)
//	Predicate   — a truth-valued expression (a comparison, a connective, a quantified test)
//	BoundSet[T] — a set of T values bound as a parameter; a Quantified right operand only
//	SetExpr[T]  — a set of T values an engine can also project (a multi-row subquery)
//	Aliased     — a named output column, legal only in a projection list
//
// Predicate is distinct from Expr[bool], and bool is not a Scalar: package ast models truth
// values and single values as disjoint categories, and mirroring that split here turns
// Select(As(Eq(...), "hit")) and GroupBy(pred) into compile errors rather than trees an
// adapter must reject at render time. The same split explains the two set wrappers: a bound
// set is a parameter and cannot be a column, while an aggregated set can.
//
// The wrappers exist only at construction time: each holds an ast node, and the built tree is
// plain ast data carrying no Go type parameters. This package does not render — a renderer
// takes the *Statement built here and walks its embedded ast.Select.
package query

import (
	"time"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
)

// --- typed value wrappers ---------------------------------------------------------------

// Expr is a SQL value-producing fragment of Go type T. Bind stores a T as a real bind
// argument, so T is constrained to Scalar to keep a non-driver value from reaching the
// database. The field is an ast.ScalarValue (not ast.Node) so a predicate can never be placed
// in an Expr; since ast.ScalarValue embeds ast.Projection, that one field serves both the
// value and projection accessors.
type Expr[T Scalar] struct{ n ast.ScalarValue }

// Predicate is a truth-valued expression: a comparison, a connective, a quantified test, a
// truth-valued aggregate.
//
// It is its own type rather than Expr[bool] so it does not satisfy the projection and value
// accessors (a method set is uniform over a type parameter, so Expr[bool] could not withhold
// them); this makes Select(As(Eq(...), "hit")) a compile error rather than a tree some engines
// cannot express. Correspondingly bool is absent from Scalar, so there is no Expr[bool]: use
// True/False for a constant predicate, and a CASE yielding 1/0 for a boolean column.
type Predicate struct{ n ast.Predicate }

// BoundSet is a set of T values bound as a parameter, usable only as a Quantified right
// operand. It is not projectable or groupable: a bound parameter set is not a column, so this
// wrapper has no projection accessor.
type BoundSet[T Scalar] struct{ n ast.SetValue }

// SetExpr is a set of T values an engine can also emit as a column: a multi-row subquery. Like
// BoundSet it is a Quantified right operand, but it is additionally a Projection — the whole
// distinction between the two wrappers.
type SetExpr[T Scalar] struct {
	n interface {
		ast.SetValue
		ast.Projection
	}
}

// AnyExpr is the erased-T scalar expression, for type-heterogeneous or type-changing positions
// (a COUNT argument, a CAST input, a GROUP BY term). Every Expr[T] satisfies it; Predicate and
// the set wrappers deliberately do not, which is what keeps GroupBy(pred) and GroupBy(boundSet)
// from compiling.
type AnyExpr interface {
	value() ast.ScalarValue
}

// AnySet is the erased-T set operand: either a BoundSet or a SetExpr. Quantified takes one, so
// the two flavours are interchangeable exactly where a set operand is legal, and nowhere else.
// T is carried so the element type is still checked against the left operand.
type AnySet[T Scalar] interface {
	set() ast.SetValue
}

// Projection is anything that can stand in a SELECT list: a scalar expression, a projectable
// set, or either bound to an output alias. Aliased satisfies Projection but not AnyExpr, so an
// alias cannot be nested inside an expression.
type Projection interface {
	projection() ast.Projection
}

// Aliased is a projection bound to an output column name. Build one with As.
type Aliased struct{ n ast.Projection }

func (e Expr[T]) value() ast.ScalarValue        { return e.n }
func (e Expr[T]) projection() ast.Projection    { return e.n }
func (e Expr[T]) countArg() []ast.ScalarValue   { return []ast.ScalarValue{e.n} }
func (s BoundSet[T]) set() ast.SetValue         { return s.n }
func (s SetExpr[T]) set() ast.SetValue          { return s.n }
func (s SetExpr[T]) projection() ast.Projection { return s.n }
func (a Aliased) projection() ast.Projection    { return a.n }

// As binds a projection to an output column alias for use in a SELECT list. It takes a
// Projection rather than an AnyExpr so a projectable set can also be aliased, while a bound set
// or a Predicate cannot.
func As(p Projection, alias string) Aliased {
	return Aliased{n: ast.AliasNode{Inner: p.projection(), Alias: alias}}
}

// --- constraints ------------------------------------------------------------------------

// ordinal is the numeric-and-string core shared by Ordered and Literal, factored out so those
// two constraints can differ on time.Time without restating the list.
type ordinal interface {
	~string | ~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// Ordered admits the ordering comparisons (Lt/Lte/Gt/Gte/Between), including time.Time for
// timestamp ranges.
type Ordered interface {
	ordinal | time.Time
}

// Literal admits the types Lit may inline. It is ordinal rather than Ordered because a
// time.Time has no portable inline form (its literal syntax differs across engines) and so
// must be bound, never inlined. bool is absent for the same reason it is absent from Scalar.
type Literal interface {
	ordinal
}

// Scalar admits the Go types that can be a single SQL value. It constrains the T of Expr[T]
// and SetExpr[T]; since Bind passes T to the driver, this keeps a nested expression (a
// query.Expr struct) from reaching the database as a bind argument.
//
// Membership tracks database/sql/driver.Value (int64, float64, bool, []byte, string,
// time.Time), with three deliberate deviations:
//
//   - The sized ints and float32 are admitted; driver.DefaultParameterConverter widens them to
//     int64/float64 losslessly.
//   - time.Time is spelled without the ~ every other term carries: the converter does not
//     unwrap a named struct (`type ts time.Time` fails with "unsupported type, a struct"), so
//     ~time.Time would reintroduce the run-time failure this constraint prevents.
//   - bool is excluded, though it is a driver.Value: package ast models truth values as a
//     category disjoint from single values (some engines, e.g. Oracle before 23c, have no
//     boolean value type). This leaves Predicate as the sole route to a truth value; a caller
//     wanting a boolean column writes a CASE yielding 1/0.
//
// It must be a union rather than `comparable`: a union is structural so no struct satisfies it,
// which rejects a nested Expr/SetExpr; `comparable` would admit those structs and would wrongly
// exclude []byte (a legitimate value type that is not Go-comparable — see Quantified).
type Scalar interface {
	Ordered | ~[]byte
}

// Comparable is Scalar restricted to the types SQL equality may be applied to: what
// Eq/Ne/In/Quantified require. It differs from Scalar on exactly one member, []byte, which is a
// legitimate value type but not Go-comparable — so the encoded condition context can be
// projected, grouped, cast, and null-tested but never compared (see Tuple.ConditionContext).
type Comparable interface {
	Scalar
	comparable
}

// --- re-exported enums ------------------------------------------------------------------

// These are type aliases of the ast enums, so the constants below are the same values the
// nodes store and a caller that only builds queries never has to import ast.
type (
	Op            = ast.Op
	Quantifier    = ast.Quantifier
	JoinType      = ast.JoinType
	SortDirection = ast.SortDirection
	CastType      = ast.CastType
)

const (
	OpEq  = ast.OpEq
	OpNe  = ast.OpNe
	OpLt  = ast.OpLt
	OpLte = ast.OpLte
	OpGt  = ast.OpGt
	OpGte = ast.OpGte

	Any = ast.Any
	All = ast.All

	// Only the join flavours every target renders are exposed; RIGHT/FULL OUTER are absent
	// because MySQL and older SQLite cannot render them.
	JoinInner     = ast.JoinInner
	JoinLeftOuter = ast.JoinLeftOuter
	JoinCross     = ast.JoinCross

	Ascending  = ast.Ascending
	Descending = ast.Descending

	// There is no TypeBoolean: a cast yields a value, and truth values are a disjoint category
	// (Cast[bool] does not compile).
	TypeVarchar   = ast.TypeVarchar
	TypeInteger   = ast.TypeInteger
	TypeBigint    = ast.TypeBigint
	TypeNumeric   = ast.TypeNumeric
	TypeVarbinary = ast.TypeVarbinary
)

// --- leaves -----------------------------------------------------------------------------

// Bind binds any Go value as a single parameter, typed as Expr[T].
func Bind[T Scalar](v T) Expr[T] { return Expr[T]{n: ast.BindNode{Value: v}} }

// Lit marks a constant to be inlined into the query rather than bound as a parameter — a
// plan-caching and readability preference, not a capability (Bind accepts everything Lit does).
// The value is stored raw; escaping happens in the consumer, which knows the target's rules.
func Lit[T Literal](v T) Expr[T] { return Expr[T]{n: ast.LitNode{Value: v}} }

// BindAll binds a slice as a set operand; element type T flows into BoundSet[T]. It returns a
// BoundSet rather than a SetExpr because a bound parameter set is not a projectable column.
func BindAll[T Scalar](vs []T) BoundSet[T] {
	elems := make([]any, len(vs))
	for i, v := range vs {
		elems[i] = v
	}
	return BoundSet[T]{n: ast.SetBindNode{Elems: elems}}
}

// True and False are the constant predicates. A truth value is a category, not a bool, so a
// constant one is its own node (ast.ConstPredNode); each target spells it differently (many
// have no bare boolean and need "1 = 1").
func True() Predicate  { return Predicate{n: ast.ConstPredNode{Value: true}} }
func False() Predicate { return Predicate{n: ast.ConstPredNode{Value: false}} }

// --- comparison operators (mismatched types do not compile) -----------------------------

// Eq / Ne compare two expressions of the SAME type T.
func Eq[T Comparable](a, b Expr[T]) Predicate { return compare(ast.OpEq, a, b) }
func Ne[T Comparable](a, b Expr[T]) Predicate { return compare(ast.OpNe, a, b) }
func Lt[T Ordered](a, b Expr[T]) Predicate    { return compare(ast.OpLt, a, b) }
func Lte[T Ordered](a, b Expr[T]) Predicate   { return compare(ast.OpLte, a, b) }
func Gt[T Ordered](a, b Expr[T]) Predicate    { return compare(ast.OpGt, a, b) }
func Gte[T Ordered](a, b Expr[T]) Predicate   { return compare(ast.OpGte, a, b) }
func Compare[T Ordered](op Op, a, b Expr[T]) Predicate {
	return compare(op, a, b)
}

func compare[T Scalar](op Op, a, b Expr[T]) Predicate {
	return Predicate{n: ast.CompareNode{Op: op, Left: a.n, Right: b.n}}
}

// Like is defined only over Expr[string], so it is unavailable on any other type. ANSI SQL
// has no case-insensitive LIKE; fold both operands with Lower if that is the intent.
func Like(a, pattern Expr[string]) Predicate {
	return Predicate{n: ast.LikeNode{Left: a.n, Pattern: pattern.n}}
}

// Between renders "a BETWEEN lo AND hi" over an Ordered T. Negate with Not.
func Between[T Ordered](a, lo, hi Expr[T]) Predicate {
	return Predicate{n: ast.BetweenNode{Inner: a.n, Lo: lo.n, Hi: hi.n}}
}

// IsNull renders "a IS NULL". Unlike the comparisons it is defined for every T, including the
// non-comparable ones: null-testing the condition-context column is meaningful even though
// comparing its bytes is not. Negate with Not for IS NOT NULL.
func IsNull[T Scalar](a Expr[T]) Predicate {
	return Predicate{n: ast.IsNullNode{Inner: a.n}}
}

// In enumerates an explicit IN list. Every element must be an Expr of the same type T, but may
// be any mix of forms (binds, literals, columns, casts, scalar subqueries). For membership in a
// bound slice or a multi-row subquery use Quantified with a SetExpr instead. Negate with Not
// for NOT IN.
func In[T Comparable](a Expr[T], vs ...Expr[T]) Predicate {
	elems := make([]ast.ScalarValue, len(vs))
	for i, v := range vs {
		elems[i] = v.n
	}
	return Predicate{n: ast.InNode{Left: a.n, Elems: elems}}
}

// Quantified compares an Expr[T] against a set of T; element types must match. The set operand
// is an AnySet[T], so BoundSet and SetExpr are interchangeable here and nowhere else. The
// Comparable constraint excludes []byte, so a set of []byte can be built and projected but never
// quantified against (the same rule Tuple.ConditionContext follows).
func Quantified[T Comparable](a Expr[T], op Op, q Quantifier, set AnySet[T]) Predicate {
	return Predicate{n: ast.QuantifiedNode{Left: a.n, Op: op, Q: q, Set: set.set()}}
}

// Exists renders "EXISTS (<stmt>)". Negate with Not for NOT EXISTS.
func Exists(s *Statement) Predicate {
	return Predicate{n: ast.ExistsNode{Stmt: &s.Select}}
}

// --- logical connectives ----------------------------------------------------------------

// And and Or combine predicates with the named connective. The first predicate is a separate
// parameter, so "at least one" is compiler-enforced: an empty group cannot be built, and a
// caller wanting the identity writes True()/False() explicitly. A one-element combine returns
// that predicate unwrapped, adding no redundant grouping.
func And(p Predicate, ps ...Predicate) Predicate { return combine(ast.And, p, ps) }
func Or(p Predicate, ps ...Predicate) Predicate  { return combine(ast.Or, p, ps) }
func Not(p Predicate) Predicate                  { return Predicate{n: ast.NotNode{Inner: p.n}} }

func combine(op ast.LogicalOp, first Predicate, rest []Predicate) Predicate {
	if len(rest) == 0 {
		return first
	}
	parts := make([]ast.Predicate, 0, len(rest)+1)
	parts = append(parts, first.n)
	for _, p := range rest {
		parts = append(parts, p.n)
	}
	return Predicate{n: ast.LogicalNode{Op: op, Parts: parts}}
}

// --- ordering ---------------------------------------------------------------------------

// OrderTerm is a single ordering term used by Statement.OrderBy. It is not an AnyExpr: an
// ordering term is a clause item, not an operand. There is no NULLS FIRST/LAST control, since
// MySQL lacks the clause and defaults differently, which would break the all-or-nothing render
// guarantee.
type OrderTerm struct{ t ast.OrderTerm }

// Order adapts any expression into an ordering term.
func Order(e AnyExpr, dir SortDirection) OrderTerm {
	return OrderTerm{t: ast.OrderTerm{Expr: e.value(), Dir: dir}}
}

// Asc / Desc are shorthands for Order.
func Asc(e AnyExpr) OrderTerm  { return Order(e, ast.Ascending) }
func Desc(e AnyExpr) OrderTerm { return Order(e, ast.Descending) }

// --- scalar functions -------------------------------------------------------------------

// Coalesce returns the first non-NULL argument. Every argument and the result share type T.
func Coalesce[T Scalar](es ...Expr[T]) Expr[T] {
	return Expr[T]{n: ast.FuncNode{Fn: ast.FuncCoalesce, Args: nodes(es)}}
}

// Lower / Upper fold case, and exist only over Expr[string].
func Lower(e Expr[string]) Expr[string] {
	return Expr[string]{n: ast.FuncNode{Fn: ast.FuncLower, Args: []ast.ScalarValue{e.n}}}
}

func Upper(e Expr[string]) Expr[string] {
	return Expr[string]{n: ast.FuncNode{Fn: ast.FuncUpper, Args: []ast.ScalarValue{e.n}}}
}

// JSONPair is one key/value pair of a JSON object constructor, built with Pair. It is its own
// type so JSONObject cannot be handed a bare expression where a pair belongs.
type JSONPair struct{ n ast.JSONPairNode }

// Pair builds a JSON object key/value pair. The value may be of any type; the key is text.
func Pair[T Scalar](key Expr[string], value Expr[T]) JSONPair {
	return JSONPair{n: ast.JSONPairNode{Key: key.n, Value: value.n}}
}

// JSONObject builds a JSON object from key/value pairs. JSON is modelled as Expr[[]byte], so
// the result can be projected, cast, or aggregated but not compared. It builds an
// ast.JSONObjectNode rather than a FuncNode because a pair is not a value.
func JSONObject(pairs ...JSONPair) Expr[[]byte] {
	ps := make([]ast.JSONPairNode, len(pairs))
	for i, p := range pairs {
		ps[i] = p.n
	}
	return Expr[[]byte]{n: ast.JSONObjectNode{Pairs: ps}}
}

// JSONArray builds a JSON array. Its elements are genuinely heterogeneous, so they are
// AnyExpr.
func JSONArray(es ...AnyExpr) Expr[[]byte] {
	return Expr[[]byte]{n: ast.FuncNode{Fn: ast.FuncJSONArray, Args: anyNodes(es)}}
}

// --- aggregates -------------------------------------------------------------------------

// AggOption is a modifier on an aggregate call: AggDistinct or AggFilter. It is an opaque struct
// so building a query never mentions ast, and a caller cannot construct one — the set of
// aggregate modifiers is closed. It applies to a neutral aggMods, so a future aggregate node
// reuses the same option set.
type AggOption struct{ apply func(*aggMods) }

// aggMods is the modifier set an aggregate can carry, collected before it is written into
// whichever node the aggregate produces.
type aggMods struct {
	distinct bool
	filter   ast.Predicate
}

// AggDistinct applies DISTINCT to the aggregate's arguments, e.g. COUNT(DISTINCT x). It is a
// value, not a call: Count(x, AggDistinct).
var AggDistinct = AggOption{apply: func(m *aggMods) { m.distinct = true }}

// AggFilter restricts the aggregated rows to those satisfying cond. Not every engine has a
// FILTER clause, so a renderer that lacks one must emulate the effect — see mysql,
// which wraps the aggregate's argument in a CASE.
func AggFilter(cond Predicate) AggOption {
	return AggOption{apply: func(m *aggMods) { m.filter = cond.n }}
}

func mods(opts []AggOption) aggMods {
	var m aggMods
	for _, o := range opts {
		// A zero AggOption (var o AggOption) has no apply func; guard rather than panic.
		if o.apply != nil {
			o.apply(&m)
		}
	}
	return m
}

// CountArg is what Count accepts: any expression, or Star for COUNT(*). It is narrower than
// AnyExpr so the "*" sentinel is confined to this one position and cannot leak into a CAST,
// alias, ORDER BY, or GROUP BY. The method returns the argument list rather than a single node,
// so Star models an absent argument (nil, matching ast.AggNode's empty Args for COUNT(*)).
type CountArg interface{ countArg() []ast.ScalarValue }

// starArg is the unexported type behind Star, so no caller can construct another value of it.
type starArg struct{}

// countArg returns no arguments: COUNT(*) is COUNT with an empty argument list.
func (starArg) countArg() []ast.ScalarValue { return nil }

// Star is the COUNT(*) sentinel: Count(Star) is COUNT(*), Count(Star, AggFilter(p)) a filtered
// row count. It satisfies CountArg only, never AnyExpr.
var Star starArg

// Count is COUNT(x), or COUNT(*) when passed Star. The argument is erased but the result is
// always Expr[int64].
func Count(e CountArg, opts ...AggOption) Expr[int64] {
	m := mods(opts)
	return Expr[int64]{n: ast.AggNode{
		Fn:       ast.AggCount,
		Args:     e.countArg(),
		Distinct: m.distinct,
		Filter:   m.filter,
	}}
}

// --- CASE -------------------------------------------------------------------------------

// CaseSearched builds a searched CASE: each branch carries its own boolean condition. Its
// result type T is fixed up front, so every branch and the ELSE must yield Expr[T].
//
// Build it with Case, chain When and optionally Else, and finish with End:
//
//	query.Case[string]().
//		When(query.Eq(t.ObjectType(), query.Lit("folder")), t.ObjectID()).
//		Else(query.Lit("")).
//		End()
//
// Case[bool] does not compile: a CASE yields a value, and a truth value is not one. To get a
// predicate use the connectives, or produce 1/0 and compare.
type CaseSearched[T Scalar] struct{ n ast.CaseSearchedNode }

// Case begins a searched CASE yielding T. T is explicit because no argument implies it.
func Case[T Scalar]() *CaseSearched[T] { return &CaseSearched[T]{} }

// When adds a branch guarded by cond, yielding then.
func (c *CaseSearched[T]) When(cond Predicate, then Expr[T]) *CaseSearched[T] {
	c.n.Branches = append(c.n.Branches, ast.SearchedBranch{When: cond.n, Then: then.n})
	return c
}

// Else sets the fallback result, replacing any prior one. Without it a CASE that matches no
// branch yields NULL.
func (c *CaseSearched[T]) Else(result Expr[T]) *CaseSearched[T] {
	c.n.Else = result.n
	return c
}

// End completes the CASE into an Expr[T]. It panics on a CASE with no branches, which is not
// a SQL expression at all.
func (c *CaseSearched[T]) End() Expr[T] {
	if len(c.n.Branches) == 0 {
		panic("query: CASE with no WHEN branches")
	}
	return Expr[T]{n: c.n}
}

// CaseSimple builds a simple CASE: a base operand compared for equality against each branch's
// value. It carries two type parameters — the result type T and the base/branch-value type B —
// so the compiler enforces both that every branch value is comparable to the base and that
// every result agrees.
//
// T is written explicitly and B is inferred from the base operand:
//
//	query.CaseOf[int64](t.ObjectType()).
//		When(query.Lit("folder"), query.Lit[int64](1)).
//		Else(query.Lit[int64](0)).
//		End()
//
// It builds ast.CaseSimpleNode rather than CaseSearched's ast.CaseSearchedNode because the two
// forms' WHEN differs in category: a predicate there, a value here.
type CaseSimple[T Scalar, B Comparable] struct{ n ast.CaseSimpleNode }

// CaseOf begins a simple CASE over base, yielding T.
func CaseOf[T Scalar, B Comparable](base Expr[B]) *CaseSimple[T, B] {
	return &CaseSimple[T, B]{n: ast.CaseSimpleNode{Base: base.n}}
}

// When adds a branch taken when value equals the base operand.
func (c *CaseSimple[T, B]) When(value Expr[B], then Expr[T]) *CaseSimple[T, B] {
	c.n.Branches = append(c.n.Branches, ast.SimpleBranch{When: value.n, Then: then.n})
	return c
}

// Else sets the fallback result, replacing any prior one.
func (c *CaseSimple[T, B]) Else(result Expr[T]) *CaseSimple[T, B] {
	c.n.Else = result.n
	return c
}

// End completes the CASE into an Expr[T].
func (c *CaseSimple[T, B]) End() Expr[T] {
	if len(c.n.Branches) == 0 {
		panic("query: CASE with no WHEN branches")
	}
	return Expr[T]{n: c.n}
}

// --- tuple + typed columns --------------------------------------------------------------

// Tuple is one aliased occurrence of the sole `tuple` table: both a FROM/JOIN source and the
// namespace for its columns. Each accessor yields a column already qualified by this instance's
// alias, so self-joins are unambiguous. The accessor set is the closed logical schema — there
// is no by-name escape hatch, so a column reference can only name what ast.Column enumerates.
type Tuple struct{ t ast.Table }

func NewTuple(alias string) Tuple { return Tuple{t: ast.Table{Alias: alias}} }

func (t Tuple) col(name ast.Column) ast.ScalarValue {
	return ast.ColNode{Alias: t.t.Alias, Name: name}
}

// str is the common case: a text-valued column of this tuple.
func (t Tuple) str(name ast.Column) Expr[string] { return Expr[string]{n: t.col(name)} }

// Logical object columns.
func (t Tuple) ObjectType() Expr[string] { return t.str(ast.ColObjectType) }
func (t Tuple) ObjectID() Expr[string]   { return t.str(ast.ColObjectID) }

// ObjectRelation is the object-side relation (the physical `relation` column).
func (t Tuple) ObjectRelation() Expr[string] { return t.str(ast.ColObjectRelation) }

// Logical subject view, synthesized by each renderer from what it physically stores (the packed
// `_user` string on Postgres/MySQL, split columns on SQLite). A userset subject is one with a
// non-empty SubjectRelation (or a wildcard SubjectID); there is no subject-kind column.
func (t Tuple) SubjectType() Expr[string]     { return t.str(ast.ColSubjectType) }
func (t Tuple) SubjectID() Expr[string]       { return t.str(ast.ColSubjectID) }
func (t Tuple) SubjectRelation() Expr[string] { return t.str(ast.ColSubjectRelation) }

// Store is the multi-tenant scope, filtered on every query.
func (t Tuple) Store() Expr[string] { return t.str(ast.ColStore) }

// Condition is the name of the ABAC condition attached to the tuple, if any.
func (t Tuple) Condition() Expr[string] { return t.str(ast.ColCondition) }

// ConditionContext is the encoded condition context, as Expr[[]byte]. Because []byte is neither
// comparable nor Ordered, Eq/In/Lt on it do not compile: the column can be projected, grouped,
// cast, aggregated, and IsNull-tested, but its raw bytes cannot be compared.
func (t Tuple) ConditionContext() Expr[[]byte] {
	return Expr[[]byte]{n: t.col(ast.ColConditionContext)}
}

// --- SELECT builder ---------------------------------------------------------------------

// Statement is the typed builder around an ast.Select, which it embeds so a renderer accepting
// a *Statement walks the embedded s.Select directly. Clause methods append (Columns, From,
// Where, GroupBy, Having, OrderBy, the joins); Distinct, Limit, and Offset replace.
type Statement struct{ ast.Select }

// Select begins a SELECT over heterogeneous typed projections. With no columns the projection
// may be filled in later with Columns.
func Select(cols ...Projection) *Statement {
	s := &Statement{}
	return s.Columns(cols...)
}

// Columns appends projection items; use As to bind an output alias.
func (s *Statement) Columns(cols ...Projection) *Statement {
	for _, c := range cols {
		s.Select.Columns = append(s.Select.Columns, c.projection())
	}
	return s
}

// Distinct renders SELECT DISTINCT over the whole projection. There is no DISTINCT ON, which
// only Postgres has and would break the all-or-nothing render guarantee.
func (s *Statement) Distinct() *Statement {
	s.Select.Distinct = true // qualified: the method name shadows the field
	return s
}

// From appends source tuples. Multiple sources form a comma-separated FROM list — a cross
// self-join of `tuple`.
func (s *Statement) From(ts ...Tuple) *Statement {
	for _, t := range ts {
		s.Select.From = append(s.Select.From, t.t)
	}
	return s
}

// Join adds an INNER self-join, the common case; the ON predicate has the same column-type
// enforcement as WHERE.
func (s *Statement) Join(t Tuple, on Predicate) *Statement {
	return s.JoinOn(ast.JoinInner, t, on)
}

// JoinOn adds a join of the given flavour. Only INNER, LEFT OUTER, and CROSS are constructible,
// so every JoinType reaching a renderer is emittable.
func (s *Statement) JoinOn(jt JoinType, t Tuple, on Predicate) *Statement {
	s.Joins = append(s.Joins, ast.JoinClause{Type: jt, Table: t.t, On: on.n})
	return s
}

// CrossJoin adds a CROSS join. It has its own method because a CROSS join carries no ON
// condition.
func (s *Statement) CrossJoin(t Tuple) *Statement {
	s.Joins = append(s.Joins, ast.JoinClause{Type: ast.JoinCross, Table: t.t})
	return s
}

// Where sets the search condition. It takes exactly one predicate, so the caller combines
// several with And/Or and the connective is explicit. Calling Where twice replaces rather than
// conjoins; to filter on nothing, do not call it at all.
func (s *Statement) Where(p Predicate) *Statement {
	s.Select.Where = p.n
	return s
}

// GroupBy appends grouping terms. Each is an AnyExpr, so a column such as ConditionContext
// may be grouped on even though it cannot be compared.
func (s *Statement) GroupBy(es ...AnyExpr) *Statement {
	s.Select.GroupBy = append(s.Select.GroupBy, anyNodes(es)...)
	return s
}

// Having sets the condition on grouped rows. Single-predicate for the same reason as Where, and
// replacing rather than accumulating.
func (s *Statement) Having(p Predicate) *Statement {
	s.Select.Having = p.n
	return s
}

// OrderBy appends ordering terms.
func (s *Statement) OrderBy(terms ...OrderTerm) *Statement {
	s.Select.OrderBy = append(s.Select.OrderBy, orderTerms(terms)...)
	return s
}

// Limit caps the row count; Offset skips leading rows. Each replaces any prior value. The
// underlying fields are pointers, so "no LIMIT" is distinguishable from "LIMIT 0".
func (s *Statement) Limit(n uint64) *Statement {
	s.Select.Limit = &n
	return s
}

func (s *Statement) Offset(n uint64) *Statement {
	s.Select.Offset = &n
	return s
}

// --- erasure hatches --------------------------------------------------------------------

// Cast changes an expression's stored type: the caller names the target type To, the input
// erases. The target is a CastType enum rather than a type string because type names diverge
// across engines (text / VARCHAR / CHAR); the adapter spells it. Cast[bool] does not compile,
// there being no TypeBoolean and truth values being a disjoint category.
func Cast[To Scalar](e AnyExpr, t CastType) Expr[To] {
	return Expr[To]{n: ast.CastNode{Inner: e.value(), Type: t}}
}

// ScalarExpr adapts a subquery into a scalar Expr[T]; caller asserts the row type T.
func ScalarExpr[T Scalar](s *Statement) Expr[T] {
	return Expr[T]{n: ast.SubqueryNode{Stmt: &s.Select}}
}

// SetSubExpr adapts a multi-row subquery into a SetExpr[T] for use with Quantified. T is
// supplied explicitly, since there is no value to infer it from.
func SetSubExpr[T Scalar](s *Statement) SetExpr[T] {
	return SetExpr[T]{n: ast.SubqueryNode{Stmt: &s.Select}}
}

// --- node plumbing ----------------------------------------------------------------------

func nodes[T Scalar](es []Expr[T]) []ast.ScalarValue {
	out := make([]ast.ScalarValue, len(es))
	for i, e := range es {
		out[i] = e.n
	}
	return out
}

func anyNodes(es []AnyExpr) []ast.ScalarValue {
	out := make([]ast.ScalarValue, len(es))
	for i, e := range es {
		out[i] = e.value()
	}
	return out
}

func orderTerms(ts []OrderTerm) []ast.OrderTerm {
	out := make([]ast.OrderTerm, len(ts))
	for i, t := range ts {
		out[i] = t.t
	}
	return out
}
