// Package query builds the query AST (package ast) with compile-time type checks. Its types can
// only be built through its functions, so a hand-built ast node cannot skip the checks.
//
// The finished tree is plain ast data. This package does not produce SQL; a backend reads the
// ast.Select inside a *Statement.
package query

import (
	"time"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
)

// --- typed value wrappers ---------------------------------------------------------------

// Expr is a SQL value of Go type T. T is limited to Scalar because Bind sends a T to the database.
type Expr[T Scalar] struct{ n ast.ScalarValue }

// Predicate is a true/false expression: a comparison, AND/OR, ANY/ALL test, or true/false aggregate.
//
// It is its own type, not Expr[bool], so it cannot be used as a value or a result column
// (Select(As(Eq(...), "x")) does not compile): some databases have no boolean type. For a constant,
// use True/False; for a boolean column, use a CASE that returns 1/0.
type Predicate struct{ n ast.Predicate }

// BoundSet is a list of T values sent as one parameter. It can only be the right side of ANY/ALL.
type BoundSet[T Scalar] struct{ n ast.SetValue }

// SetExpr is a multi-row subquery of T values. Unlike BoundSet, it can also be a result column.
type SetExpr[T Scalar] struct {
	n interface {
		ast.SetValue
		ast.Projection
	}
}

// AnyExpr is any Expr with T hidden, for places that mix or change types (COUNT, CAST, GROUP BY).
// Predicate and the set types are not AnyExprs, so GroupBy(pred) does not compile.
type AnyExpr interface {
	value() ast.ScalarValue
}

// AnySet is a BoundSet or SetExpr, as taken by Quantified. The elem method is never called; it ties
// the set to T, so a BoundSet[int] is not an AnySet[string].
type AnySet[T Scalar] interface {
	set() ast.SetValue
	elem() T
}

// Projection is anything that can go in a SELECT list. Aliased is a Projection but not an AnyExpr,
// so a named column cannot be used inside an expression.
type Projection interface {
	projection() ast.Projection
}

// Aliased is a result column with a name. Build one with As.
type Aliased struct{ n ast.Projection }

func (e Expr[T]) value() ast.ScalarValue        { return e.n }
func (e Expr[T]) projection() ast.Projection    { return e.n }
func (e Expr[T]) countArg() []ast.ScalarValue   { return []ast.ScalarValue{e.n} }
func (s BoundSet[T]) set() ast.SetValue         { return s.n }
func (s BoundSet[T]) elem() (_ T)               { return }
func (s SetExpr[T]) set() ast.SetValue          { return s.n }
func (s SetExpr[T]) elem() (_ T)                { return }
func (s SetExpr[T]) projection() ast.Projection { return s.n }
func (a Aliased) projection() ast.Projection    { return a.n }

// As names a result column. It takes a Projection, so a SetExpr can be named but a BoundSet or
// Predicate cannot.
func As(p Projection, alias string) Aliased {
	return Aliased{n: ast.AliasNode{Inner: p.projection(), Alias: alias}}
}

// --- constraints ------------------------------------------------------------------------

// ordinal is the types shared by Ordered and Literal, which differ only on time.Time.
type ordinal interface {
	~string | ~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// Ordered is the types that Lt, Lte, Gt, Gte, and Between accept.
type Ordered interface {
	ordinal | time.Time
}

// Literal is the types Lit accepts. It leaves out time.Time, whose literal syntax differs by
// database, so a time must be bound.
type Literal interface {
	ordinal
}

// Scalar is the Go types that can be a single SQL value. Bind sends them to the driver, so the list
// follows database/sql/driver.Value, with three differences:
//
//   - Sized ints and float32 are included; the driver widens them without loss.
//   - time.Time has no ~: the driver rejects a named struct type such as `type ts time.Time`.
//   - bool is left out: true/false is a Predicate.
//
// It is a union, not `comparable`, so no struct (such as a nested Expr) can match, but []byte can.
type Scalar interface {
	Ordered | ~[]byte
}

// Comparable is Scalar without []byte: the types that Eq, Ne, In, and Quantified accept.
type Comparable interface {
	Scalar
	comparable
}

// --- re-exported enums ------------------------------------------------------------------

// Aliases for the ast enums, so callers do not need to import ast.
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

	JoinInner     = ast.JoinInner
	JoinLeftOuter = ast.JoinLeftOuter
	JoinCross     = ast.JoinCross
	// RIGHT and FULL OUTER are left out; see ast.JoinType.

	Ascending  = ast.Ascending
	Descending = ast.Descending

	TypeVarchar   = ast.TypeVarchar
	TypeInteger   = ast.TypeInteger
	TypeBigint    = ast.TypeBigint
	TypeNumeric   = ast.TypeNumeric
	TypeVarbinary = ast.TypeVarbinary
	// There is no TypeBoolean: true/false is a Predicate, not a value.
)

// --- leaves -----------------------------------------------------------------------------

// Bind sends v to the database as a parameter.
func Bind[T Scalar](v T) Expr[T] { return Expr[T]{n: ast.BindNode{Value: v}} }

// Lit writes v into the SQL text instead of sending it as a parameter, for readability and plan
// caching. The backend escapes it.
func Lit[T Literal](v T) Expr[T] { return Expr[T]{n: ast.LitNode{Value: v}} }

// BindAll sends vs to the database as one list parameter.
func BindAll[T Scalar](vs []T) BoundSet[T] {
	elems := make([]any, len(vs))
	for i, v := range vs {
		elems[i] = v
	}
	return BoundSet[T]{n: ast.SetBindNode{Elems: elems}}
}

// True and False are constant predicates. Each database writes them its own way (some need "1 = 1").
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

// Like is "a LIKE pattern". ANSI SQL has no case-insensitive LIKE; for one, wrap both sides in Lower.
func Like(a, pattern Expr[string]) Predicate {
	return Predicate{n: ast.LikeNode{Left: a.n, Pattern: pattern.n}}
}

// Between is "a BETWEEN lo AND hi" over an Ordered T. Negate with Not.
func Between[T Ordered](a, lo, hi Expr[T]) Predicate {
	return Predicate{n: ast.BetweenNode{Inner: a.n, Lo: lo.n, Hi: hi.n}}
}

// IsNull is "a IS NULL", for any Scalar including []byte. Negate with Not for IS NOT NULL.
func IsNull[T Scalar](a Expr[T]) Predicate {
	return Predicate{n: ast.IsNullNode{Inner: a.n}}
}

// In is "a IN (vs...)". For a list parameter or a multi-row subquery, use Quantified. Negate with Not
// for NOT IN.
func In[T Comparable](a Expr[T], vs ...Expr[T]) Predicate {
	elems := make([]ast.ScalarValue, len(vs))
	for i, v := range vs {
		elems[i] = v.n
	}
	return Predicate{n: ast.InNode{Left: a.n, Elems: elems}}
}

// Quantified is "a op ANY (set)" or "a op ALL (set)".
func Quantified[T Comparable](a Expr[T], op Op, q Quantifier, set AnySet[T]) Predicate {
	return Predicate{n: ast.QuantifiedNode{Left: a.n, Op: op, Q: q, Set: set.set()}}
}

// Exists is "EXISTS (stmt)". Negate with Not for NOT EXISTS.
func Exists(s *Statement) Predicate {
	return Predicate{n: ast.ExistsNode{Stmt: &s.Select}}
}

// --- logical connectives ----------------------------------------------------------------

// And and Or combine predicates. They need at least one, so an empty group cannot be built; a single
// predicate is returned as-is.
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

// OrderTerm is one ORDER BY term. There is no NULLS FIRST/LAST, because MySQL does not support it.
type OrderTerm struct{ t ast.OrderTerm }

// Order turns any expression into an ordering term.
func Order(e AnyExpr, dir SortDirection) OrderTerm {
	return OrderTerm{t: ast.OrderTerm{Expr: e.value(), Dir: dir}}
}

// Asc / Desc are shorthand for Order.
func Asc(e AnyExpr) OrderTerm  { return Order(e, ast.Ascending) }
func Desc(e AnyExpr) OrderTerm { return Order(e, ast.Descending) }

// --- scalar functions -------------------------------------------------------------------

// Coalesce returns the first non-NULL argument.
func Coalesce[T Scalar](es ...Expr[T]) Expr[T] {
	return Expr[T]{n: ast.FuncNode{Fn: ast.FuncCoalesce, Args: nodes(es)}}
}

// Lower and Upper change the case of a string.
func Lower(e Expr[string]) Expr[string] {
	return Expr[string]{n: ast.FuncNode{Fn: ast.FuncLower, Args: []ast.ScalarValue{e.n}}}
}

func Upper(e Expr[string]) Expr[string] {
	return Expr[string]{n: ast.FuncNode{Fn: ast.FuncUpper, Args: []ast.ScalarValue{e.n}}}
}

// JSONPair is one key/value pair for JSONObject. Build one with Pair.
type JSONPair struct{ n ast.JSONPairNode }

// Pair builds a JSON key/value pair.
func Pair[T Scalar](key Expr[string], value Expr[T]) JSONPair {
	return JSONPair{n: ast.JSONPairNode{Key: key.n, Value: value.n}}
}

// JSONObject builds a JSON object. JSON is Expr[[]byte], so the result cannot be compared.
func JSONObject(pairs ...JSONPair) Expr[[]byte] {
	ps := make([]ast.JSONPairNode, len(pairs))
	for i, p := range pairs {
		ps[i] = p.n
	}
	return Expr[[]byte]{n: ast.JSONObjectNode{Pairs: ps}}
}

// JSONArray builds a JSON array. Its elements can be of different types.
func JSONArray(es ...AnyExpr) Expr[[]byte] {
	return Expr[[]byte]{n: ast.FuncNode{Fn: ast.FuncJSONArray, Args: anyNodes(es)}}
}

// --- aggregates -------------------------------------------------------------------------

// AggOption modifies an aggregate: AggDistinct or AggFilter. Callers cannot make their own.
type AggOption struct{ apply func(*aggMods) }

// aggMods collects the AggOptions for one aggregate call.
type aggMods struct {
	distinct bool
	filter   ast.Predicate
}

// AggDistinct adds DISTINCT: Count(x, AggDistinct) is COUNT(DISTINCT x).
var AggDistinct = AggOption{apply: func(m *aggMods) { m.distinct = true }}

// AggFilter limits the aggregate to rows matching cond (FILTER (WHERE cond)). A backend without
// FILTER must emulate it.
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

// CountArg is what Count accepts: any Expr, or Star. Star is not an AnyExpr, so "*" cannot be used
// anywhere else.
type CountArg interface{ countArg() []ast.ScalarValue }

// starArg is unexported so Star is its only value.
type starArg struct{}

// countArg returns no arguments: COUNT(*) is COUNT with an empty argument list.
func (starArg) countArg() []ast.ScalarValue { return nil }

// Star is the argument for COUNT(*): Count(Star).
var Star starArg

// Count is COUNT(e), or COUNT(*) for Star.
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

// CaseSearched builds a searched CASE, where each branch has its own condition. Every branch and the
// ELSE return T. Start one with [Case]. E.g., this returns the object id for folder rows and an
// empty string for all others:
//
//	query.Case[string]().
//		When(query.Eq(t.ObjectType(), query.Lit("folder")), t.ObjectID()).
//		Else(query.Lit("")).
//		End()
type CaseSearched[T Scalar] struct{ n ast.CaseSearchedNode }

// Case starts a searched CASE returning T.
func Case[T Scalar]() *CaseSearched[T] { return &CaseSearched[T]{} }

// When adds a branch guarded by cond, returning then.
func (c *CaseSearched[T]) When(cond Predicate, then Expr[T]) *CaseSearched[T] {
	c.n.Branches = append(c.n.Branches, ast.SearchedBranch{When: cond.n, Then: then.n})
	return c
}

// Else sets the fallback result, replacing any earlier one. Without it, a CASE that matches nothing
// returns NULL.
func (c *CaseSearched[T]) Else(result Expr[T]) *CaseSearched[T] {
	c.n.Else = result.n
	return c
}

// End finishes the CASE. It panics if there are no branches, which is not valid SQL.
func (c *CaseSearched[T]) End() Expr[T] {
	if len(c.n.Branches) == 0 {
		panic("query: CASE with no WHEN branches")
	}
	return Expr[T]{n: c.n}
}

// CaseSimple builds a simple CASE, which compares a base value of type B with each branch's value
// and returns T. B is inferred from the base. Start one with [CaseOf]. E.g., this returns 1 for
// folder rows and 0 for all others:
//
//	query.CaseOf[int64](t.ObjectType()).
//		When(query.Lit("folder"), query.Lit[int64](1)).
//		Else(query.Lit[int64](0)).
//		End()
type CaseSimple[T Scalar, B Comparable] struct{ n ast.CaseSimpleNode }

// CaseOf starts a simple CASE over base, returning T.
func CaseOf[T Scalar, B Comparable](base Expr[B]) *CaseSimple[T, B] {
	return &CaseSimple[T, B]{n: ast.CaseSimpleNode{Base: base.n}}
}

// When adds a branch taken when value equals the base.
func (c *CaseSimple[T, B]) When(value Expr[B], then Expr[T]) *CaseSimple[T, B] {
	c.n.Branches = append(c.n.Branches, ast.SimpleBranch{When: value.n, Then: then.n})
	return c
}

// Else sets the fallback result, replacing any earlier one.
func (c *CaseSimple[T, B]) Else(result Expr[T]) *CaseSimple[T, B] {
	c.n.Else = result.n
	return c
}

// End finishes the CASE. It panics if there are no branches.
func (c *CaseSimple[T, B]) End() Expr[T] {
	if len(c.n.Branches) == 0 {
		panic("query: CASE with no WHEN branches")
	}
	return Expr[T]{n: c.n}
}

// --- tuple + typed columns --------------------------------------------------------------

// Tuple is the `tuple` table under an alias, for use in FROM or JOIN. Its methods return columns
// qualified by that alias, so self-joins are unambiguous, and they are the only way to get a column.
type Tuple struct{ t ast.Table }

func NewTuple(alias string) Tuple { return Tuple{t: ast.Table{Alias: alias}} }

func (t Tuple) col(name ast.Column) ast.ScalarValue {
	return ast.ColNode{Alias: t.t.Alias, Name: name}
}

// str is the common case: a text column of this tuple.
func (t Tuple) str(name ast.Column) Expr[string] { return Expr[string]{n: t.col(name)} }

func (t Tuple) ObjectType() Expr[string] { return t.str(ast.ColObjectType) }
func (t Tuple) ObjectID() Expr[string]   { return t.str(ast.ColObjectID) }

// ObjectRelation is the object-side relation (the physical `relation` column).
func (t Tuple) ObjectRelation() Expr[string] { return t.str(ast.ColObjectRelation) }

func (t Tuple) SubjectType() Expr[string] { return t.str(ast.ColSubjectType) }
func (t Tuple) SubjectID() Expr[string]   { return t.str(ast.ColSubjectID) }

// SubjectRelation is the subject-side relation, e.g., `member` in `group:eng#member`. It is empty for
// a wildcard such as `user:*`, which storage still counts as a userset, so an empty value does not
// mean the subject is not a userset.
func (t Tuple) SubjectRelation() Expr[string] { return t.str(ast.ColSubjectRelation) }

func (t Tuple) Store() Expr[string] { return t.str(ast.ColStore) }

func (t Tuple) Condition() Expr[string] { return t.str(ast.ColCondition) }

// ConditionContext is the condition context as a proto-encoded structpb.Struct.
func (t Tuple) ConditionContext() Expr[[]byte] {
	return Expr[[]byte]{n: t.col(ast.ColConditionContext)}
}

// --- SELECT builder ---------------------------------------------------------------------

// Statement builds an ast.Select, which it embeds so a backend can read it directly. Where, Having,
// Limit, and Offset replace any earlier value; the other methods add to the statement.
type Statement struct{ ast.Select }

// Select starts a SELECT with the given result columns.
func Select(cols ...Projection) *Statement {
	s := &Statement{}
	return s.Columns(cols...)
}

// Columns adds result columns; name one with As.
func (s *Statement) Columns(cols ...Projection) *Statement {
	for _, c := range cols {
		s.Select.Columns = append(s.Select.Columns, c.projection())
	}
	return s
}

// Distinct makes it SELECT DISTINCT. There is no DISTINCT ON, because only Postgres has it.
func (s *Statement) Distinct() *Statement {
	s.Select.Distinct = true // qualified: the method name shadows the field
	return s
}

// From adds tables. Several tables make a cross join (FROM a, b).
func (s *Statement) From(ts ...Tuple) *Statement {
	for _, t := range ts {
		s.Select.From = append(s.Select.From, t.t)
	}
	return s
}

// Join adds an INNER JOIN.
func (s *Statement) Join(t Tuple, on Predicate) *Statement {
	return s.JoinOn(ast.JoinInner, t, on)
}

// JoinOn adds a join of type jt.
func (s *Statement) JoinOn(jt JoinType, t Tuple, on Predicate) *Statement {
	s.Joins = append(s.Joins, ast.JoinClause{Type: jt, Table: t.t, On: on.n})
	return s
}

// CrossJoin adds a CROSS JOIN, which has no ON condition.
func (s *Statement) CrossJoin(t Tuple) *Statement {
	s.Joins = append(s.Joins, ast.JoinClause{Type: ast.JoinCross, Table: t.t})
	return s
}

// Where sets the WHERE condition. Combine several with And/Or.
func (s *Statement) Where(p Predicate) *Statement {
	s.Select.Where = p.n
	return s
}

// GroupBy adds GROUP BY terms. It takes AnyExpr, so a column that cannot be compared, like
// ConditionContext, can still be grouped.
func (s *Statement) GroupBy(es ...AnyExpr) *Statement {
	s.Select.GroupBy = append(s.Select.GroupBy, anyNodes(es)...)
	return s
}

// Having sets the HAVING condition.
func (s *Statement) Having(p Predicate) *Statement {
	s.Select.Having = p.n
	return s
}

// OrderBy adds ORDER BY terms.
func (s *Statement) OrderBy(terms ...OrderTerm) *Statement {
	s.Select.OrderBy = append(s.Select.OrderBy, orderTerms(terms)...)
	return s
}

// Limit sets LIMIT and Offset sets OFFSET.
func (s *Statement) Limit(n uint64) *Statement {
	s.Select.Limit = &n
	return s
}

func (s *Statement) Offset(n uint64) *Statement {
	s.Select.Offset = &n
	return s
}

// --- casts and subqueries ---------------------------------------------------------------

// Cast converts e to the database type t. It takes a CastType, not a type name, because type names
// differ by database.
func Cast[To Scalar](e AnyExpr, t CastType) Expr[To] {
	return Expr[To]{n: ast.CastNode{Inner: e.value(), Type: t}}
}

// ScalarExpr uses a single-value subquery as an Expr[T].
func ScalarExpr[T Scalar](s *Statement) Expr[T] {
	return Expr[T]{n: ast.SubqueryNode{Stmt: &s.Select}}
}

// SetSubExpr uses a multi-row subquery as a SetExpr[T].
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
