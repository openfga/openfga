// Package adapter declares a fluent, dialect-agnostic interface for composing and
// executing ANSI SQL data-query (SELECT) statements. It is an interface-only
// contract: it defines the shapes a query builder exposes, but provides no
// implementation. A concrete builder satisfies these interfaces and owns both the
// rendering of SQL and its execution against a datastore.
//
// There is deliberately no public stringify step: a built query renders and runs
// internally to the concrete builder, which exposes only Query.Execute. Callers
// never see SQL text or bind arguments.
//
// The surface covers a pragmatic subset of ANSI DQL: projections, table references
// and joins, WHERE / GROUP BY / HAVING, ORDER BY, LIMIT / OFFSET, and a full expression
// and predicate algebra.
//
// The builder targets a single fixed table, `tuple`, so there are no table- or
// column-name strings: Builder.Tuple mints an aliased occurrence of that table, and
// its accessors yield the columns. Every join is a self-join of `tuple`.
//
// Builder is the entry point. A statement is composed fluently, then lowered to a
// runnable Query with Builder.Build, which is what executes:
//
//	t := b.Tuple("t")
//	stmt := b.Select(t.ObjectID(), t.SubjectID()).
//		From(t).
//		Where(t.ObjectType().Eq(b.Bind("document")).
//			And(t.ObjectRelation().Eq(b.Bind("viewer")))).
//		OrderBy(t.ObjectID().Desc()).
//		Limit(10)
//	rows, err := b.Build(stmt).Execute(ctx)
//
// Splitting composition (SelectBuilder) from execution (Query) lets a Builder be wrapped
// by overriding only Build (to wrap the Query it returns) and Query.Execute, rather than
// every fluent SelectBuilder method.
//
// Build a leaf value with a Tuple column accessor, Builder.Bind (a parameter placeholder,
// the safe choice for user-controlled values), Builder.Lit (an inline literal for fixed
// constants), or Query.ScalarExpr, then chain operators off it. Negation uses Predicate.Not
// rather than dedicated NOT-variants (e.g. t.ObjectID().In(b.Bind(1), b.Bind(2)).Not() for
// NOT IN).
//
// Operands are Expression throughout; the few argument positions typed `any` accept a
// small, fixed set of types that each method documents.
package adapter

import "context"

// Builder is the fluent entry point: the single factory that mints leaf
// expressions, query statements, and joins. The algebra (comparisons, logical
// connectives) lives on the Expression and Predicate values it returns, not here, so
// the factory stays lean.
//
// A concrete Builder is typically bound to a dialect and a datastore connection;
// constructing one is the implementation's concern.
type Builder interface {
	// Tuple references the sole `tuple` table under the given alias, returning a
	// handle whose accessors yield columns already qualified by that alias. Distinct
	// aliases denote distinct occurrences for self-joins.
	Tuple(alias string) Tuple

	// Lit renders a Go value inline as SQL text, using the value's type's string form
	// (strings single-quoted and escaped, numbers and booleans rendered bare). Reserve it
	// for fixed, trusted constants; use Bind for user-controlled values so they stay
	// parameterized.
	Lit(value any) Expression

	// Bind binds a Go value as a parameter literal, rendering the dialect's placeholder and
	// contributing the value to the ordinal bind-argument list. This is the safe choice for
	// any user-controlled value.
	Bind(value any) Expression

	// Func calls an ANSI SQL scalar function, e.g. Func(FuncLower, t.ObjectID()).
	// The function is chosen from the ScalarFunc enumeration, so only ANSI-standard
	// functions are expressible. Each arg is an Operand, so a select-only column such
	// as Tuple.ConditionContext may be passed.
	Func(fn ScalarFunc, args ...Operand) Expression

	// Aggregate builds an ANSI SQL aggregate call (AggCount, AggSum, ...) with
	// DISTINCT / FILTER / ORDER BY modifiers via the returned AggregateExpr. The
	// aggregate is chosen from the AggregateFunc enumeration, so only ANSI-standard
	// aggregates are expressible. Each arg is an Operand, so a select-only column such
	// as Tuple.ConditionContext may be passed.
	Aggregate(fn AggregateFunc, args ...Operand) AggregateExpr

	// Case begins a CASE expression.
	Case() CaseExpr

	// Cast renders "CAST(expr AS sqlType)". The expr is an Operand.
	Cast(expr Operand, sqlType string) Expression

	// Select begins a SELECT projecting the given items (use Projection.As to alias).
	// Each item is a Projection: any Expression, or a select-only column such as
	// Tuple.ConditionContext. With no columns the projection may be filled in later or
	// rendered as "SELECT *".
	Select(columns ...Projection) SelectBuilder

	// Join builds a join clause of the given type against another tuple instance (a
	// self-join). The condition is attached via the returned Join (On).
	Join(jt JoinType, table Tuple) Join

	// Build lowers a composed SELECT statement into a runnable Query. This is the seam
	// that separates statement composition (SelectBuilder) from execution (Query): a
	// custom Builder can wrap a delegate's Query here — overriding only Build and
	// Query.Execute — instead of reimplementing every fluent SelectBuilder method.
	Build(stmt SelectBuilder) Query
}

// Query is a complete, runnable query — a SELECT or a set operation over SELECTs —
// that can also be embedded into another query. Execution is internal to the
// concrete builder; Query exposes no SQL-text form.
type Query interface {
	// Execute renders and runs the query internally and returns its result cursor.
	Execute(ctx context.Context) (Rows, error)

	// ScalarExpr adapts the query for use as a scalar subquery in expression
	// position (a projected column, comparison operand, etc.).
	ScalarExpr() Expression

	// Exists adapts the query into an "EXISTS (<query>)" predicate. Use Not for NOT
	// EXISTS.
	Exists() Predicate
}

// Rows is the forward-only result cursor returned by Query.Execute. It mirrors the
// standard database/sql cursor shape without binding to a particular driver, so the
// concrete builder is free to back it with any datastore.
type Rows interface {
	// Next advances to the next row, reporting false when the result is exhausted or
	// an error occurred (check Err).
	Next() bool

	// Scan copies the current row's columns into the destinations.
	Scan(dest ...any) error

	// Close releases the cursor's resources.
	Close() error

	// Err reports the error, if any, that terminated iteration.
	Err() error
}

// SelectBuilder composes a single SELECT statement. Every method returns the builder
// so calls chain fluently; clause methods append unless documented as replacing. It is
// not itself runnable: lower it with Builder.Build to obtain a Query that executes
// directly or embeds as a subquery / derived table.
type SelectBuilder interface {
	// Distinct renders SELECT DISTINCT. With one or more expressions it renders
	// DISTINCT ON (exprs...).
	Distinct(on ...Expression) SelectBuilder

	// Columns appends projection items (use Projection.As to alias). Each is a
	// Projection: any Expression, or a select-only column such as
	// Tuple.ConditionContext.
	Columns(columns ...Projection) SelectBuilder

	// From sets the source(s); each is a tuple instance (Builder.Tuple). Multiple
	// sources form a comma-separated FROM list (a cross self-join of `tuple`).
	From(sources ...Tuple) SelectBuilder

	// JoinClause appends a join built via Builder.Join.
	JoinClause(join Join) SelectBuilder

	// Where appends search conditions, ANDed with any existing conditions.
	Where(conditions ...Predicate) SelectBuilder

	// GroupBy appends grouping terms. Each is an Operand, so a select-only column such
	// as Tuple.ConditionContext may be grouped on.
	GroupBy(exprs ...Operand) SelectBuilder
	// Having appends conditions on grouped rows, ANDed together.
	Having(conditions ...Predicate) SelectBuilder

	// OrderBy appends ordering terms. Each is an OrderTerm carrying direction and null
	// ordering; build one from a Tuple column accessor with Expression.Asc / Desc /
	// Order.
	OrderBy(terms ...OrderTerm) SelectBuilder

	// Limit caps the row count; Offset skips leading rows. Each replaces any prior
	// value.
	Limit(n uint64) SelectBuilder
	Offset(n uint64) SelectBuilder
}

// Tuple is one aliased occurrence of the sole `tuple` table. It is both a FROM/JOIN
// source and the namespace for its columns: each accessor yields a column Expression
// already qualified by this instance's alias, so self-joins are unambiguous by
// construction. Obtain one with Builder.Tuple.
type Tuple interface {
	// Logical object columns.
	ObjectType() Expression
	ObjectID() Expression
	// ObjectRelation is the object-side relation (the physical `relation` column).
	ObjectRelation() Expression

	// Logical subject view. On Postgres/MySQL the renderer synthesizes these from the
	// physical `_user` string; on SQLite they map to the split user columns. A
	// userset subject is one with a non-empty SubjectRelation (or a wildcard
	// SubjectID); there is no separate user_type column in the logical view.
	SubjectType() Expression
	SubjectID() Expression
	SubjectRelation() Expression

	// Store is the multi-tenant scope, filtered on every query.
	Store() Expression

	// Condition is the name of the ABAC condition attached to the tuple (the
	// `condition` column), if any. It may be projected or filtered on, so it is a
	// full Expression.
	Condition() Expression
	// ConditionContext is the encoded condition context (the `condition_context`
	// column). It is an Operand, not a full Expression: it can be projected, grouped
	// on, and passed as a function/aggregate argument, but never compared or used in a
	// predicate — comparing its raw encoded bytes is meaningless.
	ConditionContext() Operand
}

// Join is a single join clause, built via Builder.Join and completed with its ON
// condition, then attached to a query with SelectBuilder.JoinClause. Because every
// join is a self-join of `tuple`, the ON predicate compares column accessors from two
// distinct Tuple aliases:
//
//	t := b.Tuple("t")
//	g := b.Tuple("g")
//	j := b.Join(InnerJoin, g).On(t.SubjectID().Eq(g.ObjectID()))
//	rows, err := b.Select(t.ObjectID(), g.SubjectID()).
//		From(t).
//		JoinClause(j).
//		Execute(ctx)
//
// Compound conditions chain with Predicate.And. A CROSS join carries no condition.
type Join interface {
	// On sets the join's ON predicate, replacing any prior condition.
	On(predicate Predicate) Join
}

// Projection is anything that can stand in a SELECT list: a full Expression, an
// Operand, or a select-only column. The bare Projection tier exposes no operator
// algebra at all — it can only be projected and aliased. Use As to bind an output
// alias.
type Projection interface {
	// As binds the item to an output alias for use in a SELECT list.
	As(alias string) Projection
}

// Operand is a value usable in non-comparison operand positions — GROUP BY terms and
// function / aggregate / CAST arguments — in addition to a SELECT list. It
// deliberately exposes no comparison, range, pattern, or ordering algebra, so an
// encoded column such as Tuple.ConditionContext can be grouped or passed to a function
// without admitting meaningless comparisons against its raw bytes. Every Expression is
// an Operand; the converse does not hold.
//
// Note: with no sealing marker (this package is implemented out-of-package, so an
// unexported marker is unavailable), Operand and a bare Projection are structurally
// interchangeable. The meaningful guarantee is the absence of comparison operators on
// an Operand, not exclusion of arbitrary Projections from operand positions.
type Operand interface {
	Projection
}

// Expression is any value-producing SQL fragment usable as a comparison operand: a
// column reference, a literal, a function expression, a CASE, a CAST, or a scalar
// subquery. It is the common currency of predicate operands, ORDER BY items, and — via
// Operand — GROUP BY items, function arguments, and SELECT lists. The comparison and
// set/range/pattern operators return a Predicate; negate with Predicate.Not (e.g.
// expr.In(...).Not() for NOT IN).
//
// Operands are Expression values throughout: build a column with a Tuple accessor, a
// bound literal with Builder.Lit, and a scalar subquery with Query.ScalarExpr.
type Expression interface {
	Operand

	// Compare applies a comparison operator against other, e.g.
	// Compare(OpEq, Lit(5)) -> "self = 5".
	Compare(op ComparisonOp, other Expression) Predicate

	// Eq is shorthand for Compare(OpEq, other); negate with Predicate.Not for "<>".
	Eq(other Expression) Predicate

	// Lt / Lte / Gt / Gte are shorthands for the ordering comparisons.
	Lt(other Expression) Predicate
	Lte(other Expression) Predicate
	Gt(other Expression) Predicate
	Gte(other Expression) Predicate

	// Quantified applies a comparison against ANY/ALL of a subquery or array, e.g.
	// Quantified(OpEq, QuantifierAny, sub) -> "self = ANY (<sub>)" (an IN-subquery).
	// right is a Query (a multi-row subquery) or an array-valued Expression.
	Quantified(op ComparisonOp, q Quantifier, right any) Predicate

	// In renders "self IN (values...)" over a list of scalar operands. For the
	// subquery form "self IN (<sub>)" use Quantified with QuantifierAny. Use
	// Predicate.Not for NOT IN.
	In(values ...Expression) Predicate

	// Between renders "self BETWEEN lo AND hi". Use Predicate.Not for NOT BETWEEN.
	Between(lo, hi Expression) Predicate

	// Like renders "self LIKE pattern". Use Predicate.Not for NOT LIKE. ANSI SQL has no
	// case-insensitive LIKE; fold both operands with LOWER if that is the intent.
	Like(pattern Expression) Predicate

	// IsNull renders "self IS NULL". Use Predicate.Not for IS NOT NULL.
	IsNull() Predicate

	// Order adapts the expression into an ORDER BY term with the given direction.
	Order(dir SortDirection) OrderTerm

	// Asc / Desc are shorthands for Order.
	Asc() OrderTerm
	Desc() OrderTerm
}

// AggregateExpr is an aggregate function call such as COUNT, SUM, or ARRAY_AGG, with
// the modifiers ANSI permits.
type AggregateExpr interface {
	Expression

	// Distinct applies DISTINCT to the aggregate's arguments, e.g. COUNT(DISTINCT x).
	Distinct() AggregateExpr

	// Filter restricts the aggregated rows with a FILTER (WHERE ...) clause.
	Filter(condition Predicate) AggregateExpr

	// OrderBy orders the aggregate's input (e.g. for ARRAY_AGG / STRING_AGG); each
	// term is an OrderTerm carrying direction and null ordering (build one with
	// Expression.Asc / Desc / Order).
	OrderBy(terms ...OrderTerm) AggregateExpr
}

// CaseExpr is a CASE expression under construction, before either form is chosen. It is
// the fork between the two ANSI CASE forms and is not yet a value: Value commits to a
// simple CASE (a base operand compared against each branch value), while When commits to
// a searched CASE (boolean branch conditions). Picking a form yields the matching
// builder, so the two forms cannot be mixed.
type CaseExpr interface {
	// Value sets the base operand, committing to a simple CASE.
	Value(operand Expression) SimpleCaseExpr

	// When adds the first branch with a boolean condition, committing to a searched
	// CASE; result is the branch's value.
	When(condition Predicate, result Expression) SearchedCaseExpr
}

// SimpleCaseExpr builds a simple CASE: a base operand (set via CaseExpr.Value) compared
// for equality against each branch value. It is an Expression, so a completed CASE drops
// into any operand position.
type SimpleCaseExpr interface {
	Expression

	// When adds a branch: value is compared against the base operand; result is the
	// branch's value.
	When(value Expression, result Expression) SimpleCaseExpr

	// Else sets the fallback result.
	Else(result Expression) SimpleCaseExpr
}

// SearchedCaseExpr builds a searched CASE: each branch carries its own boolean condition
// (no base operand). It is an Expression, so a completed CASE drops into any operand
// position.
type SearchedCaseExpr interface {
	Expression

	// When adds a branch guarded by condition; result is the branch's value.
	When(condition Predicate, result Expression) SearchedCaseExpr

	// Else sets the fallback result.
	Else(result Expression) SearchedCaseExpr
}

// Predicate is a boolean-valued SQL fragment usable in WHERE, HAVING, JOIN ON, CASE
// WHEN, and aggregate FILTER positions. Predicates compose with the logical
// connectives, and any predicate negates with Not — so there are no dedicated
// NOT-variant constructors (e.g. NOT IN is expr.In(...).Not()).
type Predicate interface {
	// And combines this predicate with others using AND.
	And(others ...Predicate) Predicate
	// Or combines this predicate with others using OR.
	Or(others ...Predicate) Predicate
	// Not negates this predicate ("NOT (...)").
	Not() Predicate
}

// OrderTerm is a single ORDER BY item, produced by Expression.Order / Asc / Desc.
// Nulls refines where NULLs sort.
type OrderTerm interface {
	// Nulls sets where NULLs sort for this term.
	Nulls(ordering NullOrdering) OrderTerm
}

// ScalarFunc enumerates the ANSI SQL scalar functions callable via Builder.Func. It
// is a pragmatic subset of the standard rather than an exhaustive list; extend it as
// new functions are needed. Dialect-specific functions are deliberately excluded —
// only constructs defined by ANSI SQL belong here.
type ScalarFunc int

const (
	// FuncCoalesce is "COALESCE(a, b, ...)" — first non-NULL argument.
	FuncCoalesce ScalarFunc = iota
	// FuncJSONObject is "JSON_OBJECT(k VALUE v, ...)" — build a JSON object from
	// key/value pairs (the ANSI form of dialect "jsonb_build_object").
	FuncJSONObject
	// FuncJSONArray is "JSON_ARRAY(a, b, ...)" — build a JSON array.
	FuncJSONArray
)

// AggregateFunc enumerates the ANSI SQL aggregate functions callable via
// Builder.Aggregate. Like ScalarFunc it is a pragmatic, extensible subset and admits
// only ANSI-standard aggregates, not dialect extensions.
type AggregateFunc int

const (
	// AggCount is "COUNT(x)" (or "COUNT(*)").
	AggCount AggregateFunc = iota
	// AggEvery is "EVERY(p)" — true when the predicate holds for every row.
	AggEvery
	// AggAny is "ANY(p)" / "SOME(p)" — true when the predicate holds for any row.
	AggAny
	// AggArrayAgg is "ARRAY_AGG(x)" — collect inputs into an array.
	AggArrayAgg
	// AggJSONObjectAgg is "JSON_OBJECTAGG(k VALUE v)" — aggregate key/value pairs
	// into a JSON object (the ANSI form of dialect "jsonb_agg").
	AggJSONObjectAgg
	// AggJSONArrayAgg is "JSON_ARRAYAGG(x)" — aggregate inputs into a JSON array.
	AggJSONArrayAgg
)

// ComparisonOp enumerates the ANSI comparison operators, used where an operator is
// named as a value (Expression.Compare and quantified comparisons).
type ComparisonOp int

const (
	// OpEq is "=".
	OpEq ComparisonOp = iota
	// OpNotEq is "<>".
	OpNotEq
	// OpLt is "<".
	OpLt
	// OpLte is "<=".
	OpLte
	// OpGt is ">".
	OpGt
	// OpGte is ">=".
	OpGte
)

// Quantifier is the ANY / ALL quantifier of a quantified comparison.
type Quantifier int

const (
	// QuantifierAny renders "ANY" (holds if the comparison is true for at least one
	// element); SOME is an ANSI synonym.
	QuantifierAny Quantifier = iota
	// QuantifierAll renders "ALL" (holds only if true for every element; vacuously
	// true when empty).
	QuantifierAll
)

// SortDirection is the ASC / DESC direction of an ORDER BY term.
type SortDirection int

const (
	// Ascending is "ASC" (the ANSI default).
	Ascending SortDirection = iota
	// Descending is "DESC".
	Descending
)

// NullOrdering controls where NULLs sort relative to non-NULL values.
type NullOrdering int

const (
	// NullsDefault defers to the dialect's default null ordering.
	NullsDefault NullOrdering = iota
	// NullsFirst renders "NULLS FIRST".
	NullsFirst
	// NullsLast renders "NULLS LAST".
	NullsLast
)

// JoinType enumerates the ANSI join flavours.
type JoinType int

const (
	// InnerJoin is "INNER JOIN".
	InnerJoin JoinType = iota
	// LeftOuterJoin is "LEFT OUTER JOIN".
	LeftOuterJoin
	// RightOuterJoin is "RIGHT OUTER JOIN".
	RightOuterJoin
	// FullOuterJoin is "FULL OUTER JOIN".
	FullOuterJoin
	// CrossJoinType is "CROSS JOIN".
	CrossJoinType
)
