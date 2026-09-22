// Package ast is the pure, target-neutral query AST: data only, with no rendering, execution,
// or typed construction. Package query builds these nodes through type-safe functions, and each
// backend adapter walks the tree.
//
// # Position is part of the type
//
// The node set is divided into four position categories, each a sealed sub-interface of Node,
// and every field is typed with the category it accepts:
//
//   - Predicate    — legal after WHERE / HAVING / ON, as a searched CASE's WHEN, and as an
//     aggregate's FILTER.
//   - ScalarValue  — legal wherever a single value is: a function argument, a cast input, a
//     comparison operand, GROUP BY, ORDER BY. Every ScalarValue is also a Projection, which is
//     why the interface embeds it.
//   - SetValue     — legal as a quantified comparison's right operand. Some set nodes are also
//     projectable (a subquery) and some are not (a bound set), which is why Projection is a
//     separate category rather than a synonym for "value".
//   - Projection   — legal in a Select.Columns list. AliasNode is a Projection and nothing else,
//     so an output alias cannot be nested inside an expression.
//
// Because the categories are types, a mis-positioned tree does not compile, and a walker can be
// written as one function per category with the compiler guaranteeing the recursion never crosses
// between them — no walker needs to thread an "am I in value or predicate position?" parameter.
//
// SubqueryNode is the one type spanning three categories (ScalarValue, SetValue, Projection): a
// scalar subquery, a set subquery, and a projected subquery are the same construct. Crossing
// between categories still requires an explicit type assertion, so it stays visible.
//
// # Sealed to extend, open to inspect
//
// Every category interface carries an unexported marker method, so the set of node kinds is closed
// and no foreign type can join a category. Every concrete node type and field is exported, so any
// package can walk the tree by type-switching on concrete types. This guarantees position, not
// provenance: an outside package can build any node with a struct literal, so a walker may rely on
// where a node sits but not on where the tree came from.
//
// # What the field types do NOT carry
//
// The categories type what kind of thing fills a slot. Three things they do not, so a walker must
// be told rather than shown:
//
//   - Cardinality. A []Predicate field does not say it must be non-empty. Each node requiring a
//     minimum arity says so in its own doc; a tree violating one is malformed, so a consumer may
//     panic rather than emit a degenerate construct. The list: LogicalNode.Parts, InNode.Elems,
//     FuncNode.Args (per function), AggNode.Args (per aggregate), JSONObjectNode.Pairs,
//     CaseSearchedNode.Branches, and CaseSimpleNode.Branches.
//
//   - Required-non-nil. An interface-typed or pointer field is required unless its doc says it is
//     nil-able. The nil-able ones are every Filter, every Else, JoinClause.On, Select.Where,
//     Select.Having, Select.Limit, and Select.Offset.
//
//   - Cross-field relationships. A Column value must name a Table alias in scope, which nothing
//     here checks, since a node cannot see its enclosing statement.
//
// Node.Validate checks all three: each node checks its own rules and recurses into its children,
// so one call on the root validates the whole tree. It returns the first violation reached, with
// the path it was found at, and stops. A consumer should call it where a tree enters its boundary
// and treat the error as tree corruption; see README.md and validate.go.
//
// # No target-language text
//
// Nothing here holds query text. Every operator, connective, quantifier, join flavour, column,
// cast target, function, and aggregate is a closed enum carrying no spelling of its own, and nodes
// carry values rather than rendered fragments. Turning any of it into text is a consumer's job.
// The shared SQL spellings live in package sql, which SQL adapters may use and others ignore.
//
// The enums divide by how much work a consumer owns:
//
//   - Invariant across SQL engines: Op, LogicalOp, Quantifier, JoinType, SortDirection. A SQL
//     adapter can take these spellings from package sql.
//   - Divergent even among SQL engines, so every adapter owns the mapping: Column, CastType,
//     ScalarFunc, AggFunc (Postgres casts to `text` where MySQL casts to `CHAR`; Postgres builds
//     JSON with `jsonb_build_object` where the ANSI form is `JSON_OBJECT`). Each carries a Count
//     sentinel so a consumer can prove it covers the whole enum.
//
// The algebra is relational — projections, aliased table occurrences, joins, filters, grouping,
// aggregates, ordering, row limits — so a non-relational adapter is translating between models,
// not merely respelling constructs.
//
// The only strings in the tree are a table alias and an output-column alias.
package ast

import "strconv"

// Node is any query AST node and the common supertype of the four position categories. A
// category-agnostic walk (a debug dump, a node counter) ranges over it; a renderer should take
// the narrowest category a position admits so the compiler checks the walk.
//
// Validate reports the first way the node or its subtree violates this package's contract, or
// nil. See validate.go.
type Node interface {
	isNode()

	Validate() error
}

// Projection is a node legal in a Select.Columns list. Its members are every ScalarValue (the
// interface embeds Projection) plus two nodes that are not ScalarValues: AliasNode (a Projection
// and nothing else) and SubqueryNode (also a ScalarValue and a SetValue). SetBindNode is
// deliberately not a Projection, since a bound set is a parameter rather than a column.
//
// A consumer's Projection walker handles those two and delegates the rest to its ScalarValue
// walker via a type assertion. There is no Count sentinel to prove that assertion total, so if a
// third non-scalar Projection is ever added, this list must be updated alongside it, and a
// consumer's assertion should fail loudly rather than silently skip the node.
type Projection interface {
	Node
	isProjection()
}

// ScalarValue is a node that yields one value: a comparison operand, a function argument, a cast
// input, a GROUP BY or ORDER BY term. It embeds Projection because every single-valued expression
// can also stand as an output column.
type ScalarValue interface {
	Projection
	isScalarValue()
}

// SetValue is a node that yields a set of values, legal only as QuantifiedNode.Set. Its members
// are exactly two: SetBindNode (a set and nothing else, not projectable) and SubqueryNode (also a
// ScalarValue and a Projection). It does not embed Projection because whether a set is projectable
// depends on the node.
type SetValue interface {
	Node
	isSetValue()
}

// Predicate is a node that yields a truth value: legal after WHERE, HAVING, and ON, as a
// searched CASE's WHEN, as an aggregate's FILTER, and as a part of a connective.
//
// A Predicate is deliberately not a ScalarValue: some engines have no boolean value type (Oracle
// before 23c), so keeping the truth-valued and single-valued worlds disjoint means the tree
// expresses only what every relational target can. A caller wanting a boolean column writes the
// CASE that produces one, making the lowering visible in the tree.
type Predicate interface {
	Node
	isPredicate()
}

// --- enums invariant across SQL engines (package sql has their spellings) ----------------

// Op is a comparison operator.
type Op int

const (
	OpEq Op = iota
	OpNe
	OpLt
	OpLte
	OpGt
	OpGte
)

// LogicalOp is a boolean connective. It is an enum rather than the raw word so a renderer
// cannot be handed an arbitrary infix string.
type LogicalOp int

const (
	And LogicalOp = iota
	Or
)

// Quantifier is ANY/ALL.
type Quantifier int

const (
	Any Quantifier = iota
	All
)

// JoinType enumerates the join flavours every SQL target can render. RIGHT and FULL OUTER are
// deliberately absent: MySQL has no FULL OUTER JOIN and SQLite gained RIGHT/FULL only in 3.39, so
// a target-neutral tree cannot promise them.
type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeftOuter
	JoinCross
)

// SortDirection is the ASC / DESC direction of an ORDER BY term.
type SortDirection int

const (
	// Ascending is the default; most targets emit nothing for it.
	Ascending SortDirection = iota
	Descending
)

// --- divergent enums (no text at all; every adapter owns the mapping) --------------------

// Column is a logical column of the tuple table — an opaque enum tag carrying no text of its own.
// A column's physical spelling is a per-backend fact (Postgres and MySQL synthesize the subject
// columns from a packed `_user` string; SQLite reads three split columns), so it belongs to each
// renderer's mapping. There is no default name: an adapter must map every column explicitly.
//
// The set is closed, and ColCount lets a renderer prove statically that it covers all of it.
type Column uint8

const (
	ColObjectType Column = iota
	ColObjectID
	// ColObjectRelation is the object-side relation.
	ColObjectRelation

	// The logical subject view; each renderer projects these from its physical layout.
	ColSubjectType
	ColSubjectID
	ColSubjectRelation

	// ColStore is the multi-tenant scope, filtered on every query.
	ColStore

	// ColCondition is the name of the ABAC condition attached to the tuple;
	// ColConditionContext is its encoded context.
	ColCondition
	ColConditionContext

	// ColCount is the number of logical columns — not a column. It lets a renderer assert
	// exhaustive coverage of the enum at compile time. Being a Column, it is assignable to
	// ColNode.Name; `ColNode{Name: ColCount}` is a malformed tree that a renderer's exhaustive
	// switch reaches through its default, which is one more reason that default should panic. The
	// same applies to TypeCount, FuncCount, and AggCount_.
	ColCount
)

// String names the column for diagnostics only; it is never a physical column name and must never
// reach a query.
func (c Column) String() string {
	switch c {
	case ColObjectType:
		return "ObjectType"
	case ColObjectID:
		return "ObjectID"
	case ColObjectRelation:
		return "ObjectRelation"
	case ColSubjectType:
		return "SubjectType"
	case ColSubjectID:
		return "SubjectID"
	case ColSubjectRelation:
		return "SubjectRelation"
	case ColStore:
		return "Store"
	case ColCondition:
		return "Condition"
	case ColConditionContext:
		return "ConditionContext"
	default:
		return "Column(" + strconv.Itoa(int(c)) + ")"
	}
}

// CastType is a CAST target type — an opaque tag, like Column, because type names diverge: the
// same logical "text" target is `VARCHAR` in ANSI, `text` on Postgres, and `CHAR` on MySQL. A
// renderer owns the spelling; this enum only fixes the closed set of targets.
//
// There is no boolean target, because there is no boolean value in this tree. Nor does a target
// carry a length or precision, so an adapter needing a size (Oracle's VARCHAR2, RAW) must choose a
// maximum and document it.
type CastType uint8

const (
	TypeVarchar CastType = iota
	TypeInteger
	TypeBigint
	TypeNumeric
	TypeVarbinary

	// TypeCount is the number of cast targets — not a target; it lets a renderer prove exhaustive
	// coverage.
	TypeCount
)

// String names the cast target for diagnostics only; it never reaches a query.
func (t CastType) String() string {
	switch t {
	case TypeVarchar:
		return "Varchar"
	case TypeInteger:
		return "Integer"
	case TypeBigint:
		return "Bigint"
	case TypeNumeric:
		return "Numeric"
	case TypeVarbinary:
		return "Varbinary"
	default:
		return "CastType(" + strconv.Itoa(int(t)) + ")"
	}
}

// ScalarFunc enumerates the scalar functions callable in the tree — a pragmatic, extensible
// subset. Like Column and CastType it carries no text of its own: ANSI spells a JSON constructor
// `JSON_OBJECT`, Postgres `jsonb_build_object`.
//
// The JSON OBJECT constructor is not here: it takes key/value pairs rather than a flat argument
// list, so it is its own node (JSONObjectNode). FuncJSONArray remains, since its arguments are
// ordinary values.
type ScalarFunc uint8

// Each member below states its arity, because FuncNode.Args is one slice for all of them. A
// FuncNode whose argument count contradicts its Fn is a malformed tree.
const (
	// FuncCoalesce is the first non-NULL argument. Arity: one or more.
	FuncCoalesce ScalarFunc = iota
	// FuncLower / FuncUpper fold case. ANSI has no case-insensitive LIKE, so folding both operands
	// is how that intent is expressed. Arity: exactly one.
	FuncLower
	FuncUpper
	// FuncJSONArray builds a JSON array from plain arguments. Arity: zero or more — JSON_ARRAY() is
	// valid, so this is the one function here whose argument list may be empty.
	FuncJSONArray

	// FuncCount is the number of scalar functions — not a function; it exists for
	// exhaustive-coverage assertions.
	FuncCount
)

// String names the function for diagnostics only; it never reaches a query.
func (f ScalarFunc) String() string {
	switch f {
	case FuncCoalesce:
		return "Coalesce"
	case FuncLower:
		return "Lower"
	case FuncUpper:
		return "Upper"
	case FuncJSONArray:
		return "JSONArray"
	default:
		return "ScalarFunc(" + strconv.Itoa(int(f)) + ")"
	}
}

// AggFunc enumerates the value-producing aggregates — those whose result and arguments are
// ScalarValues. It carries no text of its own; the divergence is not only spelling but existence.
//
// The enum is intentionally minimal: only the aggregates every SQL target renders. The collecting
// aggregates (ARRAY_AGG, the JSON aggregates) and the truth-valued ones (EVERY / SOME) are absent
// because they are not portable across Postgres, MySQL, and SQLite.
type AggFunc uint8

const (
	// AggCount is COUNT(x), or COUNT(*) when AggNode.Args is empty. Arity: zero or one. See AggNode
	// for why the row-counting form is an absent argument rather than a flag. COUNT(DISTINCT *) is
	// not a construct: AggNode.Distinct is meaningful only alongside an argument.
	AggCount AggFunc = iota

	// AggCount_ is the number of aggregates — not an aggregate; it exists for exhaustive-coverage
	// assertions. The trailing underscore avoids colliding with AggCount, the COUNT aggregate.
	AggCount_
)

// String names the aggregate for diagnostics only; it never reaches a query.
func (a AggFunc) String() string {
	switch a {
	case AggCount:
		return "Count"
	default:
		return "AggFunc(" + strconv.Itoa(int(a)) + ")"
	}
}

// --- leaf nodes -------------------------------------------------------------------------

// ColNode is a column reference: one logical column of one aliased table occurrence.
//
// Alias should name a Table in scope for the Select this node appears in; nothing here checks it.
// See Select's cross-field rules.
type ColNode struct {
	Alias string
	Name  Column
}

// BindNode is a single bound parameter value.
//
// Value is `any`, the widest hole in this tree: a consumer passes it to its driver essentially
// unexamined, so a Value the driver cannot encode surfaces as a driver error at execution time,
// and a Value that merely means the wrong thing surfaces not at all.
//
// Validate rejects the two values the tree's own model excludes: a bool (truth values are the
// Predicate category) and an AST node (`BindNode{Value: ColNode{}}` is a legal Go value). It does
// not — and cannot — check whether the driver can encode the value, which is why the field stays
// `any`; a consumer wanting that caught earlier checks it at its boundary.
//
// Note the asymmetry with LitNode: a LitNode's value also passes through a consumer's inlining
// logic, which rejects what it cannot spell, so a bad value there has two chances to fail loudly
// where this one has one.
type BindNode struct{ Value any }

// LitNode is a literal the builder asked to be inlined into the query rather than bound as a
// parameter. It holds the raw Go value exactly as BindNode does; the two differ only in that
// request. Inlining is a plan-caching and readability preference, never a capability.
//
// Value is the raw value, not pre-rendered text, so escaping belongs to the consumer that knows
// the target. A consumer that cannot inline safely may treat a LitNode exactly like a BindNode.
//
// As with BindNode, Validate rejects a bool; a constant truth value is ConstPredNode.
type LitNode struct{ Value any }

// SetBindNode is a bound set of values; a renderer decides how to lower it (an array parameter
// where supported, an expanded IN/AND/OR chain otherwise).
//
// Elems may be empty, which is meaningful rather than malformed — the one arity exception among
// the slice fields; QuantifiedNode documents the constant it lowers to. Elements are raw Go values
// with the same caveats as BindNode.Value.
//
// It is a SetValue and nothing else — not a Projection, since a bound set is a parameter rather
// than a column — so it appears only as QuantifiedNode.Set.
type SetBindNode struct{ Elems []any }

// --- predicate nodes --------------------------------------------------------------------
//
// Every operand field on the fixed-arity nodes below (CompareNode, LikeNode, BetweenNode,
// IsNullNode) is required, per the package-level rule. The nodes with arity or nil-ability rules
// of their own — InNode, QuantifiedNode, ExistsNode, LogicalNode — say so.

// CompareNode renders "<Left> <Op> <Right>" for one of the six comparison operators.
type CompareNode struct {
	Op          Op
	Left, Right ScalarValue
}

// LikeNode renders "<Left> LIKE <Pattern>". It is its own node rather than a CompareNode with a
// LIKE operator because LIKE cannot be quantified (there is no ANSI "LIKE ANY"), which keeps
// QuantifiedNode's Op field admitting only quantifiable values.
//
// There is no ESCAPE field, so a pattern's metacharacters are always significant: a bound pattern
// containing a literal % or _ wildcards.
type LikeNode struct {
	Left, Pattern ScalarValue
}

// BetweenNode renders "<Inner> BETWEEN <Lo> AND <Hi>".
type BetweenNode struct {
	Inner, Lo, Hi ScalarValue
}

// IsNullNode renders "<Inner> IS NULL". Negate with NotNode for IS NOT NULL.
type IsNullNode struct{ Inner ScalarValue }

// InNode is an explicit "<Left> IN (Elems...)" over enumerated expressions.
//
// Elems must hold at least one element: "x IN ()" is a syntax error on every engine, so an empty
// list is a malformed tree. A caller meaning "matches nothing" writes ConstPredNode{Value: false}.
type InNode struct {
	Left  ScalarValue
	Elems []ScalarValue
}

// QuantifiedNode is a quantified comparison of Left against the set operand Set. Both fields are
// required. Set is the only field in the tree typed SetValue.
//
// A SetBindNode with no elements is legal here and must be lowered to a constant: All is vacuously
// true, Any vacuously false, both spelled with a ConstPredNode. Most engines have no representation
// for an empty set operand, so this is the only correct output — and it is why an empty
// InNode.Elems is malformed while an empty SetBindNode.Elems is not.
type QuantifiedNode struct {
	Left ScalarValue
	Op   Op
	Q    Quantifier
	Set  SetValue
}

// ExistsNode renders "EXISTS (<Stmt>)". Negate with NotNode for NOT EXISTS.
//
// Stmt is required and never nil; being a concrete pointer, the cost of getting it wrong is a nil
// dereference rather than a clean panic.
type ExistsNode struct{ Stmt *Select }

// LogicalNode is an AND/OR of its parts.
//
// Parts must hold at least one predicate: "WHERE ()" is a syntax error, so an empty Parts is
// corruption. A single-part LogicalNode is legal and renders as that one operand — permitted so a
// builder accumulating conditions into a slice may emit one unconditionally; collapsing the
// length-one case to its operand is equally correct.
type LogicalNode struct {
	Op    LogicalOp
	Parts []Predicate
}

// NotNode negates its inner predicate. Inner is required.
type NotNode struct{ Inner Predicate }

// ConstPredNode is the constant predicate: unconditionally true or false.
//
// It exists because every adapter needs one and no target spells a bare boolean the same way. The
// forcing case is lowering an empty bound set — "x = ANY ({})" is vacuously false, "x = ALL ({})"
// vacuously true — which most engines cannot represent directly. It also lets the builder express
// an unconditional clause without a boolean literal, which this tree has no value type for.
type ConstPredNode struct{ Value bool }

// --- value nodes ------------------------------------------------------------------------

// CastNode renders a cast of Inner to Type. Inner is required. Type is the CastType enum, so the
// target spelling — including any length or precision — is the renderer's to choose.
type CastNode struct {
	Inner ScalarValue
	Type  CastType
}

// FuncNode is a scalar function call over ordinary value arguments. The JSON OBJECT constructor is
// not among them: it takes key/value pairs, so it is JSONObjectNode.
//
// The required arity of Args depends on Fn and is documented on each ScalarFunc member. Args being
// one slice for every function makes this a documented contract rather than a typed one, so a
// consumer wanting it checked must check it — and may panic when it fails.
type FuncNode struct {
	Fn   ScalarFunc
	Args []ScalarValue
}

// JSONPairNode is one key/value pair of a JSON object constructor.
//
// It belongs to no position category, so the only place it can appear is JSONObjectNode.Pairs — a
// pair is not an expression and must not be droppable into a projection list. The pair structure
// survives into the renderer because targets need it in different shapes: the ANSI form is
// "k VALUE v" while MySQL and Postgres take a flat "k, v" argument list.
//
// Both fields are required.
type JSONPairNode struct {
	Key, Value ScalarValue
}

// JSONObjectNode is the JSON object constructor. It is its own node rather than a ScalarFunc
// because its arguments are pairs rather than values, and typing Pairs as []JSONPairNode confines
// a pair to the one construct that gives it meaning.
//
// Pairs must hold at least one pair. The empty object is excluded because JSON_OBJECT() is not
// portable; contrast FuncJSONArray, whose empty form is admitted.
type JSONObjectNode struct{ Pairs []JSONPairNode }

// --- the aggregate node and its modifiers -----------------------------------------------
//
// AggNode is the only aggregate. It carries two modifier fields:
//
//   - Distinct — de-duplicate the input before aggregating. Meaningful only when there is an
//     argument: COUNT(DISTINCT *) is not a construct.
//   - Filter  — restrict which rows reach the aggregate; nil when absent. A consumer lacking the
//     ANSI FILTER clause emulates it by pushing the condition into the aggregated expression,
//     where an excluded row must yield NULL rather than a falsy value or the result changes.
//
// There is no input-ORDER-BY modifier: the only surviving aggregate (COUNT) is order-insensitive.

// AggNode is a value-producing aggregate call with the modifiers ANSI permits.
//
// The required arity of Args is per-Fn and documented on each AggFunc member. For AggCount, empty
// Args means COUNT rows and one argument means count that expression's values.
//
// That AggCount's row-counting form is an absent argument rather than a flag is load-bearing: a
// backend restructuring a filtered aggregate can substitute a constant for the absent argument, a
// rewrite that would produce nonsense if the form were a fabricated "*" operand.
//
// See the comment above for the Distinct and Filter modifiers.
type AggNode struct {
	Fn       AggFunc
	Args     []ScalarValue
	Distinct bool
	Filter   Predicate
}

// CaseSearchedNode is a searched CASE: each branch's When is a predicate, evaluated on its own.
// Else is nil when the CASE has no ELSE. Branches are evaluated in order; the first match wins.
//
// Branches must hold at least one branch: "CASE END" is a syntax error.
//
// The two CASE forms are separate node types rather than one discriminated by a nil Base because
// their WHEN differs in category — a predicate here, a value in the simple form — and a single
// node could only type that slot as the union, putting the discrimination back on the walker.
type CaseSearchedNode struct {
	Branches []SearchedBranch
	Else     ScalarValue
}

// SearchedBranch is one WHEN/THEN pair of a searched CASE: a predicate guard, a value result.
// Both fields are required.
type SearchedBranch struct {
	When Predicate
	Then ScalarValue
}

// CaseSimpleNode is a simple CASE: each branch's When is a value, compared for equality against
// Base. Else is nil when the CASE has no ELSE.
//
// Base is required — a nil Base is not the searched form but a malformed tree. Branches must hold
// at least one branch, as in the searched form.
type CaseSimpleNode struct {
	Base     ScalarValue
	Branches []SimpleBranch
	Else     ScalarValue
}

// SimpleBranch is one WHEN/THEN pair of a simple CASE: both sides are values, and both required.
type SimpleBranch struct {
	When ScalarValue
	Then ScalarValue
}

// AliasNode binds an output name to a projection. It is a Projection and nothing else — not a
// ScalarValue — so it cannot be nested inside an expression.
//
// Inner is required. Alias is one of the two free strings in the tree (Table.Alias is the other)
// and is not validated for character set or collision here; a consumer emitting text owes it
// whatever quoting or rejection its target requires.
type AliasNode struct {
	Inner Projection
	Alias string
}

// SubqueryNode embeds a Select as a parenthesised subquery. It is the one node spanning three
// categories: a scalar subquery is a ScalarValue, a multi-row subquery is a SetValue, and either
// may be projected.
//
// Stmt is required and never nil, as with ExistsNode.Stmt.
//
// Which category a consumer reached it through determines the correct rendering — a scalar subquery
// generally needs its own parentheses, a set operand usually sits inside those the quantified form
// supplies — and that is read off the walk function it arrives in.
type SubqueryNode struct{ Stmt *Select }

// --- statement --------------------------------------------------------------------------

// Select is a SELECT statement node. Its fields are exported so a renderer walks it directly;
// package query provides the typed builder that populates it.
//
// The field types are load-bearing: Columns is []Projection, Where and Having are Predicate,
// GroupBy is []ScalarValue — so `Where: someColNode` does not compile.
//
// Where and Having are each a single predicate node, nil when the clause is absent; a caller
// wanting several conditions composes them with a LogicalNode, so the operator is written where it
// applies. Limit and Offset are pointers so nil distinguishes absent from a meaningful zero.
//
// # Cross-field rules
//
// Two relationships between these fields are part of the contract and typed by nothing:
//
//   - HAVING without GROUPBY is permitted, and means "over the whole result as one group". It is
//     not malformed and needs no special handling.
//
//   - Alias scope is unchecked. A ColNode's Alias should name a Table in From or Joins of the
//     Select it is used in (or an enclosing one, for a correlated subquery). Validate does not
//     verify it — that is a resolver's job, not a shape-checker's — so a ColNode naming an
//     out-of-scope alias renders as a plausible qualified column and fails at the engine. Validate
//     does catch the local half: a duplicate alias among this statement's own tables.
//
// An empty From is legal and expresses a constant-only projection. Columns may be empty, meaning
// all columns.
type Select struct {
	Distinct bool
	Columns  []Projection
	From     []Table
	Joins    []JoinClause
	Where    Predicate
	GroupBy  []ScalarValue
	Having   Predicate
	OrderBy  []OrderTerm
	Limit    *uint64
	Offset   *uint64
}

// Table is one aliased occurrence of the tuple table. Alias is a free, unvalidated string, as
// AliasNode.Alias is. A duplicate alias within one Select would make a ColNode referring to it
// ambiguous.
type Table struct{ Alias string }

// JoinClause is a join of tuple against another alias. On is nil for a CROSS join and required for
// every other JoinType — a nil On with JoinInner is malformed, not an implicit cross join, and a
// non-nil On with JoinCross is likewise malformed, since dropping the condition changes which rows
// return.
type JoinClause struct {
	Type  JoinType
	Table Table
	On    Predicate
}

// OrderTerm is a single ordering term, used by Select.OrderBy. It is a clause item rather than a
// node in any category, so it cannot appear where a value or predicate is expected.
//
// Expr is required. Dir has a meaningful zero value (Ascending). There is no NULLS FIRST / NULLS
// LAST control: MySQL has no such clause.
type OrderTerm struct {
	Expr ScalarValue
	Dir  SortDirection
}

// --- category membership ----------------------------------------------------------------
//
// The markers below are the category assignments; reading them top to bottom shows which node may
// appear where.

// Predicates.
func (CompareNode) isNode()      {}
func (CompareNode) isPredicate() {}

func (LikeNode) isNode()      {}
func (LikeNode) isPredicate() {}

func (BetweenNode) isNode()      {}
func (BetweenNode) isPredicate() {}

func (IsNullNode) isNode()      {}
func (IsNullNode) isPredicate() {}

func (InNode) isNode()      {}
func (InNode) isPredicate() {}

func (QuantifiedNode) isNode()      {}
func (QuantifiedNode) isPredicate() {}

func (ExistsNode) isNode()      {}
func (ExistsNode) isPredicate() {}

func (LogicalNode) isNode()      {}
func (LogicalNode) isPredicate() {}

func (NotNode) isNode()      {}
func (NotNode) isPredicate() {}

func (ConstPredNode) isNode()      {}
func (ConstPredNode) isPredicate() {}

// Scalar values. Each is a Projection too, via the embedding in ScalarValue.
func (ColNode) isNode()        {}
func (ColNode) isProjection()  {}
func (ColNode) isScalarValue() {}

func (BindNode) isNode()        {}
func (BindNode) isProjection()  {}
func (BindNode) isScalarValue() {}

func (LitNode) isNode()        {}
func (LitNode) isProjection()  {}
func (LitNode) isScalarValue() {}

func (CastNode) isNode()        {}
func (CastNode) isProjection()  {}
func (CastNode) isScalarValue() {}

func (FuncNode) isNode()        {}
func (FuncNode) isProjection()  {}
func (FuncNode) isScalarValue() {}

func (JSONObjectNode) isNode()        {}
func (JSONObjectNode) isProjection()  {}
func (JSONObjectNode) isScalarValue() {}

func (CaseSearchedNode) isNode()        {}
func (CaseSearchedNode) isProjection()  {}
func (CaseSearchedNode) isScalarValue() {}

func (CaseSimpleNode) isNode()        {}
func (CaseSimpleNode) isProjection()  {}
func (CaseSimpleNode) isScalarValue() {}

func (AggNode) isNode()        {}
func (AggNode) isProjection()  {}
func (AggNode) isScalarValue() {}

// Set values. SetBindNode is a set and nothing else; SubqueryNode is also projectable.
func (SetBindNode) isNode()     {}
func (SetBindNode) isSetValue() {}

// SubqueryNode spans three categories.
func (SubqueryNode) isNode()        {}
func (SubqueryNode) isProjection()  {}
func (SubqueryNode) isScalarValue() {}
func (SubqueryNode) isSetValue()    {}

// AliasNode is a Projection and nothing else.
func (AliasNode) isNode()       {}
func (AliasNode) isProjection() {}

// JSONPairNode belongs to no category: it is reachable only through JSONObjectNode.Pairs. It
// satisfies Node so a category-agnostic walk can still see it.
func (JSONPairNode) isNode() {}

// *Select satisfies Node only. No node field holds one loosely — ExistsNode.Stmt and
// SubqueryNode.Stmt are typed concretely — so a statement never appears in an operand position.
func (*Select) isNode() {}
