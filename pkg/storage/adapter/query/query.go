// Package query is a type-safe builder for package ast. It does not produce SQL text.
package query

import (
	"time"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
)

// --- typed value wrappers ---------------------------------------------------------------

// Expr is a SQL value of Go type T.
type Expr[T Scalar] struct{ n ast.ScalarValue }

// Predicate is a true/false expression. It is not an Expr[bool] because some databases have no
// boolean type, so it cannot be a value or result column.
type Predicate struct{ n ast.Predicate }

// BoundSet is a list of T values sent as one parameter, for use with Quantified.
type BoundSet[T Scalar] struct{ n ast.SetValue }

// SetExpr is a multi-row subquery of T values.
type SetExpr[T Scalar] struct {
	n interface {
		ast.SetValue
		ast.Projection
	}
}

// AnyExpr is an Expr of any T.
type AnyExpr interface {
	value() ast.ScalarValue
}

// AnySet is a BoundSet or SetExpr. Elem is never called; it only ties the set to T.
type AnySet[T Scalar] interface {
	set() ast.SetValue
	elem() T
}

// Projection is anything that can go in a SELECT list.
type Projection interface {
	projection() ast.Projection
}

// Aliased is a named result column.
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

// As names a result column.
func As(p Projection, alias string) Aliased {
	return Aliased{n: ast.AliasNode{Inner: p.projection(), Alias: alias}}
}

// --- constraints ------------------------------------------------------------------------

type ordinal interface {
	~string | ~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

type Ordered interface {
	ordinal | time.Time
}

// Literal excludes time.Time, whose literal syntax differs by database.
type Literal interface {
	ordinal
}

// Scalar follows database/sql/driver.Value, minus bool (use Predicate). Time.Time has no ~
// because drivers reject named struct types.
type Scalar interface {
	Ordered | ~[]byte
}

// Comparable is Scalar without []byte.
type Comparable interface {
	Scalar
	comparable
}

// --- re-exported enums ------------------------------------------------------------------

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

	Ascending  = ast.Ascending
	Descending = ast.Descending

	TypeVarchar   = ast.TypeVarchar
	TypeInteger   = ast.TypeInteger
	TypeBigint    = ast.TypeBigint
	TypeNumeric   = ast.TypeNumeric
	TypeVarbinary = ast.TypeVarbinary
)

// --- leaves -----------------------------------------------------------------------------

// Bind sends v as a parameter.
func Bind[T Scalar](v T) Expr[T] { return Expr[T]{n: ast.BindNode{Value: v}} }

// Lit inlines v into the SQL text.
func Lit[T Literal](v T) Expr[T] { return Expr[T]{n: ast.LitNode{Value: v}} }

// BindAll sends vs as one list parameter.
func BindAll[T Scalar](vs []T) BoundSet[T] {
	elems := make([]any, len(vs))
	for i, v := range vs {
		elems[i] = v
	}
	return BoundSet[T]{n: ast.SetBindNode{Elems: elems}}
}

func True() Predicate  { return Predicate{n: ast.ConstPredNode{Value: true}} }
func False() Predicate { return Predicate{n: ast.ConstPredNode{Value: false}} }

// --- comparison operators -------------------------------------------------------------

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

// Like is "a LIKE pattern". For case-insensitive matching, wrap both sides in Lower.
func Like(a, pattern Expr[string]) Predicate {
	return Predicate{n: ast.LikeNode{Left: a.n, Pattern: pattern.n}}
}

// Between is "a BETWEEN lo AND hi".
func Between[T Ordered](a, lo, hi Expr[T]) Predicate {
	return Predicate{n: ast.BetweenNode{Inner: a.n, Lo: lo.n, Hi: hi.n}}
}

// IsNull is "a IS NULL".
func IsNull[T Scalar](a Expr[T]) Predicate {
	return Predicate{n: ast.IsNullNode{Inner: a.n}}
}

// In is "a IN (vs...)". For a list parameter or subquery, use Quantified.
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

// Exists is "EXISTS (stmt)".
func Exists(s *Statement) Predicate {
	return Predicate{n: ast.ExistsNode{Stmt: &s.Select}}
}

// --- logical connectives ----------------------------------------------------------------

// And and Or require at least one predicate; a single predicate is returned as-is.
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

// OrderTerm is one ORDER BY term.
type OrderTerm struct{ t ast.OrderTerm }

func Order(e AnyExpr, dir SortDirection) OrderTerm {
	return OrderTerm{t: ast.OrderTerm{Expr: e.value(), Dir: dir}}
}

func Asc(e AnyExpr) OrderTerm  { return Order(e, ast.Ascending) }
func Desc(e AnyExpr) OrderTerm { return Order(e, ast.Descending) }

// --- scalar functions -------------------------------------------------------------------

func Coalesce[T Scalar](es ...Expr[T]) Expr[T] {
	return Expr[T]{n: ast.FuncNode{Fn: ast.FuncCoalesce, Args: nodes(es)}}
}

func Lower(e Expr[string]) Expr[string] {
	return Expr[string]{n: ast.FuncNode{Fn: ast.FuncLower, Args: []ast.ScalarValue{e.n}}}
}

func Upper(e Expr[string]) Expr[string] {
	return Expr[string]{n: ast.FuncNode{Fn: ast.FuncUpper, Args: []ast.ScalarValue{e.n}}}
}

// JSONPair is one key/value pair for JSONObject.
type JSONPair struct{ n ast.JSONPairNode }

func Pair[T Scalar](key Expr[string], value Expr[T]) JSONPair {
	return JSONPair{n: ast.JSONPairNode{Key: key.n, Value: value.n}}
}

func JSONObject(pairs ...JSONPair) Expr[[]byte] {
	ps := make([]ast.JSONPairNode, len(pairs))
	for i, p := range pairs {
		ps[i] = p.n
	}
	return Expr[[]byte]{n: ast.JSONObjectNode{Pairs: ps}}
}

func JSONArray(es ...AnyExpr) Expr[[]byte] {
	return Expr[[]byte]{n: ast.FuncNode{Fn: ast.FuncJSONArray, Args: anyNodes(es)}}
}

// --- aggregates -------------------------------------------------------------------------

// AggOption is AggDistinct or AggFilter.
type AggOption struct{ apply func(*aggMods) }

type aggMods struct {
	distinct bool
	filter   ast.Predicate
}

// AggDistinct makes Count(x, AggDistinct) COUNT(DISTINCT x).
var AggDistinct = AggOption{apply: func(m *aggMods) { m.distinct = true }}

// AggFilter is FILTER (WHERE cond).
func AggFilter(cond Predicate) AggOption {
	return AggOption{apply: func(m *aggMods) { m.filter = cond.n }}
}

func mods(opts []AggOption) aggMods {
	var m aggMods
	for _, o := range opts {
		// zero-value AggOption
		if o.apply != nil {
			o.apply(&m)
		}
	}
	return m
}

// CountArg is any Expr, or Star.
type CountArg interface{ countArg() []ast.ScalarValue }

type starArg struct{}

func (starArg) countArg() []ast.ScalarValue { return nil }

// Star makes Count(Star) COUNT(*).
var Star starArg

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

// CaseSearched builds "CASE WHEN cond THEN ... END". Start one with [Case]:
//
//	query.Case[string]().
//		When(query.Eq(t.ObjectType(), query.Lit("folder")), t.ObjectID()).
//		Else(query.Lit("")).
//		End()
type CaseSearched[T Scalar] struct{ n ast.CaseSearchedNode }

func Case[T Scalar]() *CaseSearched[T] { return &CaseSearched[T]{} }

func (c *CaseSearched[T]) When(cond Predicate, then Expr[T]) *CaseSearched[T] {
	c.n.Branches = append(c.n.Branches, ast.SearchedBranch{When: cond.n, Then: then.n})
	return c
}

// Else replaces any earlier Else.
func (c *CaseSearched[T]) Else(result Expr[T]) *CaseSearched[T] {
	c.n.Else = result.n
	return c
}

// End panics if there are no branches.
func (c *CaseSearched[T]) End() Expr[T] {
	if len(c.n.Branches) == 0 {
		panic("query: CASE with no WHEN branches")
	}
	return Expr[T]{n: c.n}
}

// CaseSimple builds "CASE base WHEN value THEN ... END". Start one with [CaseOf]:
//
//	query.CaseOf[int64](t.ObjectType()).
//		When(query.Lit("folder"), query.Lit[int64](1)).
//		Else(query.Lit[int64](0)).
//		End()
type CaseSimple[T Scalar, B Comparable] struct{ n ast.CaseSimpleNode }

func CaseOf[T Scalar, B Comparable](base Expr[B]) *CaseSimple[T, B] {
	return &CaseSimple[T, B]{n: ast.CaseSimpleNode{Base: base.n}}
}

func (c *CaseSimple[T, B]) When(value Expr[B], then Expr[T]) *CaseSimple[T, B] {
	c.n.Branches = append(c.n.Branches, ast.SimpleBranch{When: value.n, Then: then.n})
	return c
}

// Else replaces any earlier Else.
func (c *CaseSimple[T, B]) Else(result Expr[T]) *CaseSimple[T, B] {
	c.n.Else = result.n
	return c
}

// End panics if there are no branches.
func (c *CaseSimple[T, B]) End() Expr[T] {
	if len(c.n.Branches) == 0 {
		panic("query: CASE with no WHEN branches")
	}
	return Expr[T]{n: c.n}
}

// --- tuple + typed columns --------------------------------------------------------------

// Tuple is the tuple table under an alias. Its methods return columns qualified by that alias.
type Tuple struct{ t ast.Table }

func NewTuple(alias string) Tuple { return Tuple{t: ast.Table{Alias: alias}} }

func (t Tuple) col(name ast.Column) ast.ScalarValue {
	return ast.ColNode{Alias: t.t.Alias, Name: name}
}

func (t Tuple) str(name ast.Column) Expr[string] { return Expr[string]{n: t.col(name)} }

func (t Tuple) ObjectType() Expr[string] { return t.str(ast.ColObjectType) }
func (t Tuple) ObjectID() Expr[string]   { return t.str(ast.ColObjectID) }

func (t Tuple) ObjectRelation() Expr[string] { return t.str(ast.ColObjectRelation) }

func (t Tuple) SubjectType() Expr[string] { return t.str(ast.ColSubjectType) }
func (t Tuple) SubjectID() Expr[string]   { return t.str(ast.ColSubjectID) }

// SubjectRelation is e.g. `member` in `group:eng#member`. It is also empty for wildcards like
// `user:*`, so empty does not imply a non-userset subject.
func (t Tuple) SubjectRelation() Expr[string] { return t.str(ast.ColSubjectRelation) }

func (t Tuple) Store() Expr[string] { return t.str(ast.ColStore) }

func (t Tuple) Condition() Expr[string] { return t.str(ast.ColCondition) }

// ConditionContext is a proto-encoded structpb.Struct.
func (t Tuple) ConditionContext() Expr[[]byte] {
	return Expr[[]byte]{n: t.col(ast.ColConditionContext)}
}

// --- SELECT builder ---------------------------------------------------------------------

// Statement builds an ast.Select. Where, Having, Limit, and Offset replace earlier values; other
// methods append.
type Statement struct{ ast.Select }

func Select(cols ...Projection) *Statement {
	s := &Statement{}
	return s.Columns(cols...)
}

func (s *Statement) Columns(cols ...Projection) *Statement {
	for _, c := range cols {
		s.Select.Columns = append(s.Select.Columns, c.projection())
	}
	return s
}

func (s *Statement) Distinct() *Statement {
	s.Select.Distinct = true
	return s
}

func (s *Statement) From(ts ...Tuple) *Statement {
	for _, t := range ts {
		s.Select.From = append(s.Select.From, t.t)
	}
	return s
}

func (s *Statement) Join(t Tuple, on Predicate) *Statement {
	return s.JoinOn(ast.JoinInner, t, on)
}

func (s *Statement) JoinOn(jt JoinType, t Tuple, on Predicate) *Statement {
	s.Joins = append(s.Joins, ast.JoinClause{Type: jt, Table: t.t, On: on.n})
	return s
}

func (s *Statement) CrossJoin(t Tuple) *Statement {
	s.Joins = append(s.Joins, ast.JoinClause{Type: ast.JoinCross, Table: t.t})
	return s
}

func (s *Statement) Where(p Predicate) *Statement {
	s.Select.Where = p.n
	return s
}

func (s *Statement) GroupBy(es ...AnyExpr) *Statement {
	s.Select.GroupBy = append(s.Select.GroupBy, anyNodes(es)...)
	return s
}

func (s *Statement) Having(p Predicate) *Statement {
	s.Select.Having = p.n
	return s
}

func (s *Statement) OrderBy(terms ...OrderTerm) *Statement {
	s.Select.OrderBy = append(s.Select.OrderBy, orderTerms(terms)...)
	return s
}

func (s *Statement) Limit(n uint64) *Statement {
	s.Select.Limit = &n
	return s
}

func (s *Statement) Offset(n uint64) *Statement {
	s.Select.Offset = &n
	return s
}

// --- casts and subqueries ---------------------------------------------------------------

func Cast[To Scalar](e AnyExpr, t CastType) Expr[To] {
	return Expr[To]{n: ast.CastNode{Inner: e.value(), Type: t}}
}

// ScalarExpr uses a single-value subquery as an Expr.
func ScalarExpr[T Scalar](s *Statement) Expr[T] {
	return Expr[T]{n: ast.SubqueryNode{Stmt: &s.Select}}
}

// SetSubExpr uses a multi-row subquery as a SetExpr.
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
