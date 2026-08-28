// Package query is the compile-time type-safe construction surface for the query AST
// (package ast). It exposes package-level GENERIC functions — not interface methods —
// because a function may introduce a type parameter while a method may not, and that type
// parameter is what carries operand types through the algebra so the compiler enforces
// them (Eq(stringCol, Bind(5)) does not compile).
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
// Predicate is NOT Expr[bool], and bool is NOT a Scalar. Truth values and single values are
// disjoint categories in the tree (see package ast), and mirroring that here is what makes
// Select(As(Eq(...), "hit")) and GroupBy(pred) compile errors rather than trees an adapter has
// to reject at render time. The same split explains the two set wrappers: a bound set is a
// parameter and cannot be a column, while an aggregated set can.
//
// The wrappers exist ONLY at construction time: each holds an ast node of the appropriate
// category, and once built the tree is plain ast data carrying no Go type parameters. This
// package does not render: a renderer (the shared ansi package, or a backend's own) takes the
// *Statement this package builds and walks its embedded ast.Select, not these typed wrappers.
//
// # Modifiers are options, not chained builders
//
// An aggregate's DISTINCT / FILTER modifiers are variadic AggOption values rather
// than methods on a returned builder. A chained builder would have to return its own type,
// which is not an Expr[T], forcing a conversion call before the aggregate could be compared
// or projected; options let Count(...) return Expr[int64] directly, so a modified aggregate
// composes exactly like an unmodified one. CASE is the exception — its branches must all
// agree on T, which reads far better chained — so it uses a builder with an explicit End.
package query

import (
	"time"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
)

// --- typed value wrappers ---------------------------------------------------------------

// Expr is a SQL value-producing fragment of Go type T. T is phantom in the tree — the built
// node carries no trace of it — but it is NOT phantom at run time: Bind stores a T as a
// bind argument, so T is constrained to Scalar to keep a non-driver value from reaching the
// database. Without that constraint Bind(someColumn) type-checks and renders, passing a
// query.Expr struct where the driver expects a value.
//
// The field is an ast.ScalarValue, not an ast.Node, so the position invariant holds inside this
// package too: there is no assignment anywhere that could put a predicate in an Expr. Because
// ast.ScalarValue embeds ast.Projection, one field serves both the value and the projection
// accessor with no type assertion.
type Expr[T Scalar] struct{ n ast.ScalarValue }

// Predicate is a truth-valued expression: a comparison, a connective, a quantified test, a
// truth-valued aggregate.
//
// It is its OWN type rather than Expr[bool], which it was until the position categories landed.
// An alias would have made every predicate satisfy the projection and value interfaces below,
// because a Go method set is uniform over a type parameter — there is no way to give Expr[string]
// a projection method and withhold it from Expr[bool]. So `Select(As(Eq(...), "hit"))` compiled
// and produced a tree that Postgres renders, MySQL renders, and Oracle cannot express at all.
// A distinct type holding an ast.Predicate makes that a compile error at the call site, and
// makes this wrapper's field type an accurate statement rather than a hopeful one.
//
// Correspondingly, bool is absent from Scalar, so there is no Expr[bool] for the alias to have
// named: Lit(true), Bind(true), Cast[bool], and Case[bool] no longer compile. Use True/False for
// a constant predicate, and a CASE yielding 1/0 if a boolean COLUMN is genuinely wanted.
type Predicate struct{ n ast.Predicate }

// BoundSet is a set of T values bound as a parameter, usable ONLY as a Quantified right
// operand. It is not projectable and not groupable, because a bound parameter set is not a
// column — ast.SetBindNode is a SetValue and deliberately not a Projection, and this wrapper
// mirrors that by having no projection accessor.
type BoundSet[T Scalar] struct{ n ast.SetValue }

// SetExpr is a set of T values an engine can also emit as a column: a multi-row subquery. It
// is a Quantified right operand like BoundSet, and additionally a Projection — which is the
// whole distinction between the two wrappers.
//
// T is constrained to Scalar exactly as Expr[T] is, so the ELEMENT type is checked as well as
// the position: a set of sets and a set of statements are both compile errors.
type SetExpr[T Scalar] struct {
	n interface {
		ast.SetValue
		ast.Projection
	}
}

// AnyExpr is the erased-T scalar expression: the honest boundary type for inherently
// type-heterogeneous or type-changing positions — a COUNT argument, a CAST input, a GROUP BY
// term. Every Expr[T] satisfies it.
//
// Note what does NOT: Predicate, because a truth value is not a value; and the set wrappers,
// because a set is not a scalar. Both exclusions are load-bearing — they are what keep
// GroupBy(pred) and GroupBy(boundSet) from compiling.
type AnyExpr interface {
	value() ast.ScalarValue
}

// AnySet is the erased-T set operand: either a BoundSet or a SetExpr. Quantified takes one, so
// the two set flavours are interchangeable exactly where a set operand is legal, and nowhere
// else. T is carried so the element type is still checked against the left operand.
type AnySet[T Scalar] interface {
	set() ast.SetValue
}

// Projection is anything that can stand in a SELECT list: a scalar expression, a projectable
// set, or either bound to an output alias. Aliased satisfies Projection but NOT AnyExpr, so an
// alias cannot be nested inside an expression — the one position ast.AliasNode is legal in is
// the only position the type system admits it.
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
// Projection rather than an AnyExpr so that a projectable SET can be aliased too — a bound set
// cannot, and a Predicate cannot, which is the point.
func As(p Projection, alias string) Aliased {
	return Aliased{n: ast.AliasNode{Inner: p.projection(), Alias: alias}}
}

// --- constraints ------------------------------------------------------------------------

// ordinal is the numeric-and-string core shared by Ordered and Literal. It is unexported
// because it names no rule of its own: it exists only so those two constraints can differ on
// time.Time without restating the list.
type ordinal interface {
	~string | ~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// Ordered admits the ordering comparisons (Lt/Lte/Gt/Gte/Between). A time.Time belongs here
// because a timestamp range is the main reason to hold a time at all — a time that could only
// be tested for equality would be nearly useless.
type Ordered interface {
	ordinal | time.Time
}

// Literal admits the types Lit may inline. It is deliberately `ordinal` rather than `Ordered`:
// a time.Time is orderable but has no portable inline form — its literal syntax differs across
// engines and several need an explicit type or format — so a time must be bound, never inlined.
// Whether a type HAS a portable inline form is knowable here; what that form is belongs to the
// consumer.
//
// It admits no bool, for the same reason Scalar does not: there is no boolean value in this
// tree. Lit(true) is not an inline-literal question but a category one. Use True/False.
type Literal interface {
	ordinal
}

// Scalar admits the Go types that can be a single SQL value. It constrains the T of both
// Expr[T] and SetExpr[T], so it is what keeps a nested expression — Expr[Expr[int64]], a set of
// sets, a set of statements — from being constructible. Such a value is not merely useless: T
// reaches the driver for real through Bind, so Bind(someColumn) would otherwise pass a
// query.Expr struct as a bind argument and fail (or silently stringify) at the database.
//
// The membership rule is database/sql/driver.Value, whose documented set is int64, float64,
// bool, []byte, string, and time.Time. Three deliberate deviations:
//
//   - The sized ints and float32 are admitted even though they are not Value types, because
//     driver.DefaultParameterConverter widens them to int64/float64. That widening is
//     documented and lossless.
//   - time.Time (inherited here from Ordered) is spelled WITHOUT the ~ every other term
//     carries. The converter unwraps a named string or int to its underlying Value type, but
//     it does NOT unwrap a named struct:
//     `type ts time.Time` fails with "unsupported type, a struct". Admitting ~time.Time would
//     therefore reintroduce a narrower version of the very run-time failure this constraint
//     exists to prevent.
//   - bool is EXCLUDED, though it is a driver.Value. That is not a driver concern but a
//     category one: package ast models truth values as a category disjoint from single values,
//     because a target may separate them syntactically (Oracle before 23c has no boolean value
//     type at all) and several nodes admit only one or the other. Excluding bool here is what
//     leaves Predicate as the sole route to a truth value, so there is no Expr[bool] to leak
//     into a projection, a GROUP BY, or a bind argument. A caller wanting a boolean COLUMN
//     writes a CASE yielding 1/0, which is what every engine does anyway.
//
// It must be a UNION and not `comparable`. A union is structural, so no struct type satisfies
// it, which is what rejects a nested Expr or SetExpr; `comparable` would admit one, because
// those are structs holding a single interface field and are therefore spec-comparable.
// `comparable` would also wrongly exclude []byte, which is a legitimate value type even though
// it cannot be compared — see Quantified.
type Scalar interface {
	Ordered | ~[]byte
}

// Comparable is Scalar restricted to the types SQL equality may be applied to: it is what
// Eq/Ne/In/Quantified require. The two constraints are NOT redundant — Scalar is what an Expr's
// T must be, comparable is what a comparison needs — and they differ on exactly one member,
// []byte, which is a legitimate value type but not a Go comparable. That is why the encoded
// condition context can be projected, grouped, cast, and null-tested but never compared; see
// Tuple.ConditionContext.
type Comparable interface {
	Scalar
	comparable
}

// --- re-exported enums ------------------------------------------------------------------

// These are type aliases of the ast enums, so the constants below are the very same values
// the nodes store — no conversion, and a caller that only BUILDS queries never has to import
// ast at all. Renderers do import it, since walking the tree is the whole point.
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

	// The join flavours are prefixed rather than suffixed so that typing "Join" offers all of
	// them, matching the Op / Type groups above. Only the flavours every target renders are
	// exposed: RIGHT/FULL OUTER are absent because MySQL and older SQLite cannot render them.
	JoinInner     = ast.JoinInner
	JoinLeftOuter = ast.JoinLeftOuter
	JoinCross     = ast.JoinCross

	Ascending  = ast.Ascending
	Descending = ast.Descending

	// There is no TypeBoolean: a cast yields a value, and truth values are a disjoint
	// category. Cast[bool] does not compile either, bool having left Scalar.
	TypeVarchar   = ast.TypeVarchar
	TypeInteger   = ast.TypeInteger
	TypeBigint    = ast.TypeBigint
	TypeNumeric   = ast.TypeNumeric
	TypeVarbinary = ast.TypeVarbinary
)

// --- leaves -----------------------------------------------------------------------------

// Bind binds any Go value as a single parameter, typed as Expr[T].
func Bind[T Scalar](v T) Expr[T] { return Expr[T]{n: ast.BindNode{Value: v}} }

// Lit marks a constant to be INLINED into the query rather than bound as a parameter — a
// plan-caching and readability preference, never a capability: Bind accepts everything Lit does.
//
// The value is stored raw, exactly as Bind stores it. Escaping happens in the consumer, which is
// the only layer that knows the target's rules; a builder that pre-escaped would be guessing.
// The Literal constraint still applies, because whether a type HAS a portable inline form is
// knowable here even though the form itself is not — a time.Time has no such form and must be
// bound, which is why Literal omits it while Ordered includes it.
func Lit[T Literal](v T) Expr[T] { return Expr[T]{n: ast.LitNode{Value: v}} }

// BindAll binds a slice as a set operand; element type T flows into BoundSet[T].
//
// It returns a BoundSet rather than a SetExpr because a bound parameter set is not a column:
// ast.SetBindNode is a SetValue and not a Projection, so there is no node here to project even
// if the wrapper offered to. Quantified accepts either wrapper, which is the one position both
// are legal in.
func BindAll[T Scalar](vs []T) BoundSet[T] {
	elems := make([]any, len(vs))
	for i, v := range vs {
		elems[i] = v
	}
	return BoundSet[T]{n: ast.SetBindNode{Elems: elems}}
}

// True and False are the constant predicates. They are the replacement for Lit(true) /
// Lit(false), which no longer exist: a truth value is a category in this tree, not a value of
// type bool, so a constant one needs its own node (ast.ConstPredNode) rather than a boolean
// literal. Every target spells it differently — most have no bare boolean at all and need
// "1 = 1" — which is the second reason it is a node and not text.
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

// IsNull renders "a IS NULL". Unlike the comparisons it is defined for EVERY T, including
// types the comparisons exclude: testing whether the encoded condition-context column is
// absent is meaningful even though comparing its raw bytes is not. Negate with Not for IS
// NOT NULL.
func IsNull[T Scalar](a Expr[T]) Predicate {
	return Predicate{n: ast.IsNullNode{Inner: a.n}}
}

// In enumerates an explicit IN list. Every element must be an Expr of the same type T as the
// left operand, but the elements may be any mix of expression FORMS — binds, literals,
// columns of other aliases, casts, scalar subqueries — which is what the list is for. For
// membership in a bound slice or a multi-row subquery use Quantified with a SetExpr, which
// lets the renderer choose the optimal set lowering. Negate with Not for NOT IN.
func In[T Comparable](a Expr[T], vs ...Expr[T]) Predicate {
	elems := make([]ast.ScalarValue, len(vs))
	for i, v := range vs {
		elems[i] = v.n
	}
	return Predicate{n: ast.InNode{Left: a.n, Elems: elems}}
}

// Quantified compares an Expr[T] against a set of T; element types must match.
//
// The set operand is an AnySet[T], so a BoundSet (from BindAll) and a SetExpr (from SetSubExpr)
// are interchangeable HERE and nowhere else — which is the whole content of the distinction
// between the two wrappers. T is still carried by the interface, so the element type is checked
// against the left operand exactly as it was.
//
// T needs BOTH constraints, and they are not redundant: Scalar is what a set requires of an
// element type, while comparable is what the comparison itself requires. They differ on
// []byte, which is a legal set element but not comparable — so a set of []byte can be built
// and projected, yet never quantified against, the same rule Tuple.ConditionContext follows.
func Quantified[T Comparable](a Expr[T], op Op, q Quantifier, set AnySet[T]) Predicate {
	return Predicate{n: ast.QuantifiedNode{Left: a.n, Op: op, Q: q, Set: set.set()}}
}

// Exists renders "EXISTS (<stmt>)". Negate with Not for NOT EXISTS.
func Exists(s *Statement) Predicate {
	return Predicate{n: ast.ExistsNode{Stmt: &s.Select}}
}

// --- logical connectives ----------------------------------------------------------------

// And and Or combine predicates with the named connective. The first predicate is a SEPARATE
// parameter from the rest, so "at least one" is enforced by the compiler: And() does not build an
// empty predicate group, it fails to compile.
//
// That signature is why neither function needs a runtime guard. An empty group has a defensible
// mathematical reading (identity: true for And, false for Or), but a caller reaching one has
// almost always lost track of a set they expected to be non-empty, and quietly substituting a
// constant would turn that into a query matching everything or nothing. A caller who genuinely
// wants the identity writes True() or False() and says so.
//
// Folding a one-element set returns that predicate unwrapped, so an accumulated list of length
// one adds no redundant grouping.
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

// OrderTerm is a single ordering term, used by Statement.OrderBy. It is not an AnyExpr: an
// ordering term is a clause item, never an operand, so it cannot appear where a value is
// expected.
//
// There is no NULLS FIRST / NULLS LAST control: MySQL has no such clause and its default
// null-ordering differs, so exposing it would break the all-or-nothing render guarantee.
type OrderTerm struct{ t ast.OrderTerm }

// Order adapts any expression into an ordering term.
func Order(e AnyExpr, dir SortDirection) OrderTerm {
	return OrderTerm{t: ast.OrderTerm{Expr: e.value(), Dir: dir}}
}

// Asc / Desc are shorthands for Order.
func Asc(e AnyExpr) OrderTerm  { return Order(e, ast.Ascending) }
func Desc(e AnyExpr) OrderTerm { return Order(e, ast.Descending) }

// --- scalar functions -------------------------------------------------------------------

// Coalesce returns the first non-NULL argument. Every argument and the result share type T,
// which is stronger than the untyped variadic an enum-plus-args call would give.
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
// type so JSONObject cannot be handed a bare expression where a pair belongs — and it mirrors
// ast.JSONPairNode, which belongs to no position category for exactly that reason.
type JSONPair struct{ n ast.JSONPairNode }

// Pair builds a JSON object key/value pair. The value may be of any type; the key is text.
func Pair[T Scalar](key Expr[string], value Expr[T]) JSONPair {
	return JSONPair{n: ast.JSONPairNode{Key: key.n, Value: value.n}}
}

// JSONObject builds a JSON object from key/value pairs. JSON is modelled as Expr[[]byte]:
// []byte is neither comparable nor Ordered, so the result can be projected, cast, or
// aggregated but not compared — the same protection ConditionContext gets.
//
// It builds an ast.JSONObjectNode rather than a FuncNode, because a pair is not a value and a
// FuncNode's arguments are values. That is the same reason ast dropped FuncJSONObject from the
// ScalarFunc enum.
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

// AggOption is a modifier on an aggregate call: AggDistinct or AggFilter.
//
// It is an OPAQUE struct rather than a func over a node type, so that building a query never
// mentions the ast package. Exposing the underlying node here would have made ast part of this
// package's callable surface for one modifier — AggDistinct — and the whole point of the typed
// surface is that a caller composes queries without knowing a node algebra exists. A caller
// cannot construct an AggOption of their own, which is correct: the set of aggregate modifiers is
// closed by what the aggregate nodes can carry.
//
// It applies to a neutral aggMods rather than to one node type, so a future aggregate that
// produces a different node reuses the same option set unchanged.
type AggOption struct{ apply func(*aggMods) }

// aggMods is the modifier set an aggregate can carry, collected before it is written into
// whichever node the aggregate produces.
type aggMods struct {
	distinct bool
	filter   ast.Predicate
}

// AggDistinct applies DISTINCT to the aggregate's arguments, e.g. COUNT(DISTINCT x).
//
// It is a value, not a function call, because it takes no arguments — Count(x, AggDistinct)
// rather than Count(x, AggDistinct()).
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
		// A zero AggOption has no apply func: guard rather than panic, since a zero value is
		// reachable (var o AggOption) even though no constructor produces one.
		if o.apply != nil {
			o.apply(&m)
		}
	}
	return m
}

// CountArg is what Count accepts: any expression, or Star for COUNT(*).
//
// It is deliberately NARROWER than AnyExpr. Star is not an expression — "*" is a row-count
// shorthand legal in exactly one place — so making it an AnyExpr would admit CAST(* AS text),
// * AS alias, ORDER BY *, JSON_ARRAY(*), and GROUP BY *, none of which mean anything. A
// dedicated interface confines the sentinel to the one position that accepts it.
//
// The method returns the aggregate's argument LIST rather than a single node, which is what
// lets Star model the absence of an argument (nil) instead of a fabricated "*" literal node:
// ast.AggNode already spells COUNT(*) as empty Args, and each renderer decides how to emit it —
// mysql, for instance, must turn a filtered COUNT(*) into COUNT(CASE WHEN ... THEN 1 END).
type CountArg interface{ countArg() []ast.ScalarValue }

// starArg is the unexported type behind Star. Unexported so Star is the only value of it, and
// so no caller can construct a second one.
type starArg struct{}

// countArg returns no arguments at all: COUNT(*) is COUNT with an empty argument list.
func (starArg) countArg() []ast.ScalarValue { return nil }

// Star is the COUNT(*) sentinel: Count(Star) is COUNT(*), and Count(Star, AggFilter(p)) is a
// filtered row count. It satisfies CountArg only, never AnyExpr.
var Star starArg

// Count is COUNT(x), or COUNT(*) when passed Star. The result type is fixed by the aggregate,
// so no erasure escapes: the argument is erased but the result is always Expr[int64].
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
// result type T is fixed up front, so EVERY branch and the ELSE must yield Expr[T] — a
// guarantee the interface version could not express, since its When took an untyped
// Expression and nothing tied the branches together.
//
// Build it with Case, chain When and optionally Else, and finish with End:
//
//	query.Case[string]().
//		When(query.Eq(t.ObjectType(), query.Lit("folder")), t.ObjectID()).
//		Else(query.Lit("")).
//		End()
//
// Case[bool] does not compile, bool having left Scalar: a CASE yields a value, and a truth value
// is not one. To branch on a condition and get a predicate, use the connectives — Or(And(c, a),
// And(Not(c), b)) — or produce 1/0 and compare.
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
// value. It carries TWO type parameters — the result type T and the base/branch-value type B —
// so the compiler enforces both that every branch value is comparable to the base and that
// every result agrees with the others.
//
// T is written explicitly and B is inferred from the base operand:
//
//	query.CaseOf[int64](t.ObjectType()).
//		When(query.Lit("folder"), query.Lit[int64](1)).
//		Else(query.Lit[int64](0)).
//		End()
//
// It builds a DIFFERENT node type from CaseSearched — ast.CaseSimpleNode rather than
// ast.CaseSearchedNode — because the two forms differ in the category of their WHEN: a predicate
// there, a value here. A single node could only have typed that slot as the union of the two,
// putting the discrimination back in every walker.
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
// namespace for its columns. Each accessor yields a column already qualified by this
// instance's alias, so self-joins are unambiguous by construction. The accessor set is the
// closed logical schema — there is no by-name column escape hatch, so a column reference can
// only name something ast.Column enumerates and every renderer can therefore map.
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

// Logical subject view. A renderer synthesizes these from whatever it physically stores —
// the packed `_user` string on Postgres/MySQL, split columns on SQLite. A userset subject is
// one with a non-empty SubjectRelation (or a wildcard SubjectID); there is no separate
// subject-kind column in the logical view.
func (t Tuple) SubjectType() Expr[string]     { return t.str(ast.ColSubjectType) }
func (t Tuple) SubjectID() Expr[string]       { return t.str(ast.ColSubjectID) }
func (t Tuple) SubjectRelation() Expr[string] { return t.str(ast.ColSubjectRelation) }

// Store is the multi-tenant scope, filtered on every query.
func (t Tuple) Store() Expr[string] { return t.str(ast.ColStore) }

// Condition is the name of the ABAC condition attached to the tuple, if any. It is a
// full Expr[string]: projectable and comparable.
func (t Tuple) Condition() Expr[string] { return t.str(ast.ColCondition) }

// ConditionContext is the encoded condition context. It is Expr[[]byte], and []byte is
// neither comparable nor Ordered, so Eq/In/Lt on it do not compile — the column can be
// projected, grouped on, cast, or aggregated, and tested with IsNull, but comparing its raw
// encoded bytes is unexpressible. This is how the typed surface enforces what the interface
// documented as "an Operand, not a full Expression".
func (t Tuple) ConditionContext() Expr[[]byte] {
	return Expr[[]byte]{n: t.col(ast.ColConditionContext)}
}

// --- SELECT builder ---------------------------------------------------------------------

// Statement is the typed builder around an ast.Select. It embeds the ast node rather than
// holding a pointer, so a renderer that accepts a *Statement walks the embedded s.Select
// directly. The builder methods below sit at depth 0 and so shadow the like-named promoted
// fields for the fluent selector, while still writing through to them.
//
// Clause methods APPEND (Columns, From, Where, GroupBy, Having, OrderBy, the joins); Distinct,
// Limit, and Offset REPLACE.
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

// Distinct renders SELECT DISTINCT over the whole projection. There is no DISTINCT ON: only
// Postgres has it, so exposing it would break the all-or-nothing render guarantee.
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

// Join adds an INNER self-join; the ON predicate enjoys the same column-type enforcement as
// WHERE. It is the common case, so it has its own method.
func (s *Statement) Join(t Tuple, on Predicate) *Statement {
	return s.JoinOn(ast.JoinInner, t, on)
}

// JoinOn adds a join of the given flavour. Only the flavours every target renders are
// constructible (INNER, LEFT OUTER, CROSS), so every JoinType reaching a renderer is emittable.
func (s *Statement) JoinOn(jt JoinType, t Tuple, on Predicate) *Statement {
	s.Joins = append(s.Joins, ast.JoinClause{Type: jt, Table: t.t, On: on.n})
	return s
}

// CrossJoin adds a CROSS join, which carries no ON condition — which is why it is a distinct
// method rather than a JoinType passed to JoinOn with a predicate that would be discarded.
func (s *Statement) CrossJoin(t Tuple) *Statement {
	s.Joins = append(s.Joins, ast.JoinClause{Type: ast.JoinCross, Table: t.t})
	return s
}

// Where sets the search condition. It takes exactly ONE predicate: several conditions are
// combined by the caller with And or Or, so the connective is written at the point it applies
// rather than implied by the call. A variadic Where would have to pick a connective on the
// caller's behalf, and reading the code would not tell you which one it picked.
//
// There is no accumulation, so calling Where twice REPLACES rather than conjoins. To filter on
// nothing, do not call Where at all — the clause is absent when the field is nil, which is why
// the empty case needs no representation here.
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

// Limit caps the row count; Offset skips leading rows. Each replaces any prior value. They
// take a value rather than a pointer, and the underlying fields are pointers, so "no LIMIT"
// is distinguishable from "LIMIT 0".
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
// erases. The target is an ast.CastType enum rather than a type string, because type names
// diverge across engines (text / VARCHAR / CHAR) — the adapter spells it.
// Cast[bool] does not compile and there is no TypeBoolean to name: a cast produces a value, and
// truth values are a disjoint category.
func Cast[To Scalar](e AnyExpr, t CastType) Expr[To] {
	return Expr[To]{n: ast.CastNode{Inner: e.value(), Type: t}}
}

// ScalarExpr adapts a subquery into a scalar Expr[T]; caller asserts the row type T.
func ScalarExpr[T Scalar](s *Statement) Expr[T] {
	return Expr[T]{n: ast.SubqueryNode{Stmt: &s.Select}}
}

// SetSubExpr adapts a multi-row subquery into a SetExpr[T] for use with Quantified. T is
// supplied explicitly (there is no value to infer it from), which is exactly where the Scalar
// constraint earns its keep: it is the one set producer a caller can hand an arbitrary type.
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
