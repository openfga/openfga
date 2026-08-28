// Package ast is the pure, target-neutral query AST. It is data and nothing
// else: no rendering, no execution, no typed construction. Those live elsewhere —
// package query builds these nodes through type-safe generic functions, and each backend
// adapter walks this tree.
//
// # Position is part of the type
//
// The node set is divided into four POSITION CATEGORIES, each a sealed sub-interface of Node,
// and every field is typed with the category it accepts:
//
//   - Predicate    — legal after WHERE / HAVING / ON, as a searched CASE's WHEN, and as an
//     aggregate's FILTER. A comparison, a connective, a quantified test.
//   - ScalarValue  — legal wherever a single value is: a function argument, a cast input, a
//     comparison operand, GROUP BY, ORDER BY. Every ScalarValue is also a
//     Projection, which is why the interface embeds it.
//   - SetValue     — legal as a quantified comparison's right operand. Some set nodes are also
//     projectable (a subquery) and some are not (a bound set), which is
//     exactly why Projection is a separate category rather than a synonym for
//     "value".
//   - Projection   — legal in a Select.Columns list. AliasNode is a Projection and NOTHING
//     else, so an output alias cannot be nested inside an expression.
//
// This is the load-bearing design decision of the package, and it exists for the WALKER's
// benefit. An adapter receives an *ast.Select and cannot tell what built it; every field is
// exported, so a hand-written struct literal is as valid a tree as anything package query
// produces. If position were merely a convention that package query upheld, an adapter could
// not rely on it without trusting provenance it has no way to verify. Because the categories
// are types, a mis-positioned tree does not compile — for the builder and the forger alike —
// and a walker can therefore be written as one function PER CATEGORY, with the compiler
// guaranteeing the recursion never crosses from one to another.
//
// The practical payoff is that no walker needs to thread a "am I in value or predicate
// position?" parameter through its recursion. That parameter is unavoidable when a tree
// erases the distinction, and it is the single largest cost a target that separates the two
// syntactically — Oracle before 23c, which has no boolean value type — pays to consume such a
// tree. Here, position is read off the static type of the field being visited.
//
// SubqueryNode is the one type spanning three categories (ScalarValue, SetValue, Projection),
// which is correct: a scalar subquery, a set subquery, and a projected subquery are the same
// construct. Crossing between categories still requires an explicit type assertion, so it stays
// visible.
//
// # Sealed to extend, open to inspect
//
// Every category interface carries an unexported marker method, so no foreign type can join a
// category and the set of node kinds is CLOSED. Yet every concrete node type and every field is
// exported, so any package can walk the tree by type-switching on the concrete types.
//
// Note precisely what this does and does not give a walker. It does NOT mean a tree can only
// come from package query: an outside package can build any node with a struct literal, and
// nothing here records provenance. What it means is that the trees which can be built at all
// are well-POSITIONED, because the field types admit nothing else. A walker may rely on
// position; it may not rely on where the tree came from.
//
// # What the field types do NOT carry
//
// The categories type WHAT KIND of thing fills a slot. Three things they do not type, each of
// which a walker must therefore be told rather than shown:
//
//   - CARDINALITY. A slice field typed []Predicate does not say it must be non-empty, and the
//     zero value of every node is a legal Go value. Each node below that requires a minimum
//     arity says so in its own doc, and the requirement is part of this package's contract: a
//     tree violating one is MALFORMED, so a consumer may panic rather than emit a degenerate
//     construct. The complete list is LogicalNode.Parts, InNode.Elems, FuncNode.Args (per
//     function), AggNode.Args (per aggregate), JSONObjectNode.Pairs, CaseSearchedNode.Branches,
//     and CaseSimpleNode.Branches.
//
//   - REQUIRED-NON-NIL. Rule: an interface-typed or pointer field is required unless its doc
//     says it is nil-able. The nil-able ones are every Filter, every Else, JoinClause.On,
//     Select.Where, Select.Having, Select.Limit, and Select.Offset. Everything else — including
//     ExistsNode.Stmt, SubqueryNode.Stmt, and AliasNode.Inner — must be populated.
//
//   - CROSS-FIELD relationships. A Column value must name a Table alias that is in scope, which
//     is the one rule nothing here checks, since a node cannot see its enclosing statement.
//
// These are the residue of the same problem the categories solved for position, and they are not
// typed because encoding them would cost more than it returns: a non-empty slice type would infect
// every constructor, and the cross-field rules are properly a validator's job.
//
// So they are a validator's job. Node.Validate checks all three, and every node checks its own
// rules and recurses into its children, so ONE call on the root validates the whole tree. It
// returns the FIRST violation it reaches, with the path it was found at, and stops there — fixing
// that one reveals the next. A consumer should call it where a tree enters its boundary and
// treat the error as tree corruption, per the failure-mode split in README.md; validate.go says
// precisely what is checked and what is not. The per-node docs below still state each rule, because
// a rule is worth knowing before you violate it — but they are now claims the package enforces
// rather than promises it asks a consumer to trust.
//
// # No target-language text
//
// Nothing in this package holds query text, in any language. Every operator, connective,
// quantifier, join flavour, column, cast target, function, and aggregate is a closed enum
// carrying no spelling of its own, and the nodes carry values rather than rendered
// fragments. Turning any of it into text is entirely a consumer's job.
//
// That invariant is unconditional, which is a deliberate narrowing. These enums once
// carried SQL methods for the constructs every SQL engine spells alike, on the reasoning
// that such a mapping belongs to the construct rather than to a backend. But "invariant
// across SQL engines" is not "invariant" — an adapter over a key-value or document store
// needs none of those keywords. The shared SQL spellings moved to package sql, which SQL
// adapters may use and others simply ignore.
//
// The enums still divide by one question, since it tells a consumer how much work it owns:
//
//   - INVARIANT across SQL engines: Op, LogicalOp, Quantifier, JoinType, SortDirection. A SQL
//     adapter can take these spellings from package sql.
//   - DIVERGENT even among SQL engines, so every adapter owns the mapping: Column, CastType,
//     ScalarFunc, AggFunc (Postgres casts to `text` where MySQL casts to `CHAR`; Postgres
//     builds JSON with `jsonb_build_object` where the ANSI form is `JSON_OBJECT`). Each
//     carries a Count sentinel so a consumer can prove it covers the whole enum.
//
// The algebra itself is relational — projections, aliased table occurrences, joins, filters,
// grouping, aggregates, ordering, row limits — and that is not neutral. A non-relational
// adapter is translating between models, not merely respelling constructs.
//
// The only strings in the tree are the two a caller genuinely supplies: a table alias and an
// output-column alias.
package ast

import "strconv"

// Node is any query AST node, and the common supertype of the four position categories. It is
// what a category-agnostic walk (a debug dump, a node counter) ranges over; a RENDERER should
// take the narrowest category a position admits instead, so that the compiler checks the walk.
//
// Validate reports a way the node or its subtree violates this package's contract — the
// cardinality, required-field, and cross-field rules the field types cannot carry. Each node
// checks its own rules and then recurses into its direct children, so ONE call on the root
// validates the whole tree; it returns the FIRST violation reached and stops, labelled with the
// path it was found at. See validate.go for what is and is not checked, and for the check order
// that decides which violation a multi-fault tree reports; the errors wrap the sentinels
// declared there.
type Node interface {
	isNode()

	Validate() error
}

// Projection is a node legal in a Select.Columns list.
//
// Its members are every ScalarValue (the interface embeds Projection), plus exactly two nodes
// that are not ScalarValues:
//
//	AliasNode      — a Projection and nothing else
//	SubqueryNode   — also a ScalarValue and a SetValue
//
// SetBindNode is deliberately NOT a Projection, since a bound set is a parameter rather than a
// column; that contrast is why Projection exists as its own category instead of being a synonym
// for "value".
//
// A consumer's Projection walker therefore handles those two and delegates the rest to its
// ScalarValue walker via a type assertion. That assertion is total today, and there is no Count
// sentinel to prove it the way the divergent enums provide one — so if a third non-scalar
// Projection is ever added, this list is what must be updated alongside it, and a consumer's
// assertion should FAIL LOUDLY rather than silently skip the node.
type Projection interface {
	Node
	isProjection()
}

// ScalarValue is a node that yields ONE value: a comparison operand, a function argument, a
// cast input, a GROUP BY or ORDER BY term. It embeds Projection because every single-valued
// expression can also stand as an output column, so a consumer holding a ScalarValue never has
// to assert to project it.
type ScalarValue interface {
	Projection
	isScalarValue()
}

// SetValue is a node that yields a SET of values, legal only as QuantifiedNode.Set. Its members
// are exactly two:
//
//	SetBindNode    — a set and nothing else; NOT projectable
//	SubqueryNode   — also a ScalarValue and a Projection
//
// It does not embed Projection: whether a set is projectable depends on the node, so each set type
// states it. This is the smallest category, and a consumer's set walker is correspondingly the
// smallest of its four — often two cases, one of which is unreachable when the consumer expands
// bound sets itself.
type SetValue interface {
	Node
	isSetValue()
}

// Predicate is a node that yields a truth value: legal after WHERE, HAVING, and ON, as a
// searched CASE's WHEN, as an aggregate's FILTER, and as a part of a connective.
//
// A Predicate is NOT a ScalarValue, and that asymmetry is intentional rather than an
// oversight. Several engines do let a boolean be selected or grouped; several do not, and
// Oracle before 23c has no boolean value type at all. Modelling the truth-valued and the
// single-valued worlds as disjoint means the tree expresses only what every relational target
// can express, and a target that happens to be more permissive loses nothing it needs. A
// caller who genuinely wants a boolean COLUMN writes the CASE that produces one, which is what
// every engine would have done for them anyway — and now the lowering is visible in the tree
// instead of being each adapter's invention.
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

// JoinType enumerates the join flavours every SQL target can render. Every join is a self-join
// of `tuple`. RIGHT and FULL OUTER are deliberately absent: MySQL has no FULL OUTER JOIN and
// SQLite gained RIGHT/FULL only in 3.39, so a target-neutral tree cannot promise them.
type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeftOuter
	JoinCross
)

// SortDirection is the ASC / DESC direction of an ORDER BY term.
type SortDirection int

const (
	// Ascending is the conventional default; most targets need emit nothing for it.
	Ascending SortDirection = iota
	Descending
)

// --- divergent enums (no text at all; every adapter owns the mapping) --------------------

// Column is a LOGICAL column of the tuple table — an opaque enum tag, deliberately carrying
// no text of its own. A column's physical spelling is a per-backend fact (Postgres and MySQL
// synthesize the subject columns from a packed `_user` string, SQLite reads three split
// columns whose names differ again), so it belongs to each renderer's mapping and not to this
// target-neutral tree. There is no default name to fall back on: an adapter must map every
// column explicitly. Nothing here even asserts a column is STORED as a column.
//
// The set is closed. Package query's Tuple accessors are the only way to mint a ColNode, so a
// renderer has a finite, known domain to cover, and ColCount lets it prove statically that it
// covers all of it.
type Column uint8

const (
	ColObjectType Column = iota
	ColObjectID
	// ColObjectRelation is the object-side relation (physically the `relation` column on
	// current backends — but that spelling is the renderer's to know, not this constant's).
	ColObjectRelation

	// The logical subject view. No backend stores these three as-is everywhere; each
	// renderer decides how to project them out of its physical layout.
	ColSubjectType
	ColSubjectID
	ColSubjectRelation

	// ColStore is the multi-tenant scope, filtered on every query.
	ColStore

	// ColCondition is the name of the ABAC condition attached to the tuple;
	// ColConditionContext is its encoded context.
	ColCondition
	ColConditionContext

	// ColCount is the number of logical columns — not a column. It exists so a renderer can
	// assert exhaustive coverage of the enum at compile time.
	//
	// Being a Column, it is assignable to ColNode.Name, which Go cannot prevent. `ColNode{Name:
	// ColCount}` is a malformed tree, and a renderer's exhaustive switch will reach its default —
	// which is the correct outcome, and one more reason that default should panic rather than
	// return a fallback. The same caveat applies to TypeCount, FuncCount, and AggCount_.
	ColCount
)

// String names the column for diagnostics — panic messages, test failures, %v in a debug
// dump. It is NOT a physical column name and must never reach a query; adapters map columns
// through their own target-aware table.
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

// CastType is a CAST target type — an opaque tag, like Column, because type NAMES diverge
// sharply: the same logical "text" target is `VARCHAR` in ANSI, `text` on Postgres, and
// `CHAR` on MySQL. A renderer therefore owns the spelling; this enum only fixes the closed set
// of targets, which is what keeps an arbitrary type string out of the tree.
//
// There is no boolean target, because there is no boolean VALUE in this tree: a cast produces a
// ScalarValue, and truth values live in the disjoint Predicate category. Nor does a target
// carry a LENGTH or PRECISION, which is a real limitation — Oracle's VARCHAR2 and RAW require
// one — so an adapter needing a size must choose a maximum and document it.
type CastType uint8

const (
	TypeVarchar CastType = iota
	TypeInteger
	TypeBigint
	TypeNumeric
	TypeVarbinary

	// TypeCount is the number of cast targets — not a target. It exists so a renderer can
	// prove exhaustive coverage.
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

// ScalarFunc enumerates the scalar functions callable in the tree. It is a pragmatic,
// extensible subset. Like Column and CastType it carries no text of its own: the ANSI spelling
// of a JSON constructor is `JSON_OBJECT`, but Postgres spells it `jsonb_build_object`, so the
// name is a renderer's concern.
//
// The JSON OBJECT constructor is not here: it takes key/value pairs rather than a flat argument
// list, so it is its own node (JSONObjectNode). FuncJSONArray remains, since its arguments are
// ordinary values.
type ScalarFunc uint8

// Each member below states its ARITY, because FuncNode.Args is one slice for all of them and the
// requirement therefore cannot be a field type. A consumer may rely on these: a FuncNode whose
// argument count contradicts its Fn is a malformed tree.
const (
	// FuncCoalesce is the first non-NULL argument. Arity: ONE OR MORE (two or more is the
	// meaningful case; COALESCE() is a syntax error).
	FuncCoalesce ScalarFunc = iota
	// FuncLower / FuncUpper fold case. ANSI has no case-insensitive LIKE, so folding both
	// operands with one of these is how that intent is expressed. Arity: EXACTLY ONE.
	FuncLower
	FuncUpper
	// FuncJSONArray builds a JSON array from plain arguments. Arity: ZERO OR MORE — the empty
	// array is meaningful, and JSON_ARRAY() is valid SQL, so this is the one function here whose
	// argument list may be empty.
	FuncJSONArray

	// FuncCount is the number of scalar functions — not a function; it exists for
	// exhaustive-coverage assertions in renderers.
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

// AggFunc enumerates the VALUE-producing aggregates — those whose result is a ScalarValue and
// whose arguments are ScalarValues. It carries no text of its own; the divergence is not only
// spelling but existence.
//
// The enum is intentionally minimal: it holds only the aggregates every SQL target renders.
// The collecting aggregates (ARRAY_AGG, the JSON aggregates) and the truth-valued ones
// (EVERY / SOME) are absent because they do not exist portably across Postgres, MySQL, and
// SQLite — a target-neutral tree cannot promise a construct one target cannot spell.
type AggFunc uint8

const (
	// AggCount is COUNT(x), or COUNT(*) when AggNode.Args is empty. Arity: ZERO OR ONE. See
	// AggNode for why the row-counting form is an absent argument rather than a flag.
	//
	// Note that COUNT(DISTINCT *) is not a construct: AggNode.Distinct is meaningful only
	// alongside an argument, and setting it with empty Args is malformed.
	AggCount AggFunc = iota

	// AggCount_ is the number of aggregates — not an aggregate; it exists for
	// exhaustive-coverage assertions in renderers. It is spelled with a trailing underscore
	// because AggCount is already the COUNT aggregate.
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
// Alias should name a Table in scope for the Select this node appears in; nothing here checks it,
// and a consumer walking one clause at a time is not well placed to either. See Select's
// cross-field rules.
type ColNode struct {
	Alias string
	Name  Column
}

// BindNode is a single bound parameter value.
//
// Value is `any`, and that is the widest hole left in this tree — the one place a malformed tree
// still yields a query that RUNS and is wrong rather than one that fails. A consumer passes Value
// to its driver essentially unexamined, so a Value the driver cannot encode surfaces as a driver
// error at execution time, far from the node that caused it, and a Value that merely means the
// wrong thing surfaces not at all.
//
// Validate closes the half of the hole this package can close — the half that follows from the
// tree's own model rather than from a driver's limits. It rejects two values:
//
//   - A BOOL, including a named one. There is no boolean VALUE in this tree — truth values are the
//     Predicate category — so a bool here contradicts the model, and the engines with no boolean
//     type (Oracle before 23c) have nothing to bind it to.
//   - AN AST NODE. `BindNode{Value: ColNode{}}` is a legal Go value that renders a clean
//     placeholder and hands a struct to the driver.
//
// What Validate does NOT check, and cannot: whether the driver can ENCODE the value. That set is a
// driver's fact, which is also why the field stays `any` — a constraint here would either exclude a
// legitimate type or duplicate a rule this package cannot know. So an unencodable Value still
// surfaces as a driver error at execution time, far from the node that caused it, and a consumer
// wanting it caught earlier checks that itself at its boundary.
//
// Note the asymmetry with LitNode, since the two are otherwise documented as carrying the same
// thing: a LitNode's value additionally passes through a consumer's inlining logic, which rejects
// what it cannot spell, so a bad value there has two chances to fail loudly where this one has
// one. That is worth knowing before following the advice on LitNode to treat the two identically —
// the advice is about the RENDERING being interchangeable.
type BindNode struct{ Value any }

// LitNode is a literal the builder asked to be INLINED into the query rather than bound as a
// parameter. It holds the raw Go value, exactly as BindNode does; the two differ only in that
// request. Inlining is a plan-caching and readability preference, never a capability: every
// value a LitNode can carry, a BindNode can carry too.
//
// Value is the raw value and NOT pre-rendered text, so escaping belongs to the consumer that
// knows the target — the builder cannot know it. A consumer that cannot inline safely, or at
// all, may treat a LitNode exactly like a BindNode; that is always correct.
//
// As with BindNode, a bool is rejected by Validate: this tree has no boolean value, and a constant
// truth value is ConstPredNode. Beyond that, a consumer's inlining logic sees every value and tends
// to reject the ones it cannot spell — which is the practical difference between the two nodes and
// the reason this one is the safer of the pair to be handed a bad value.
type LitNode struct{ Value any }

// SetBindNode is a bound set of values; a renderer decides how to lower it (an array
// parameter where the engine supports one, an expanded IN/AND/OR chain otherwise).
//
// Elems may be EMPTY, and that case is meaningful rather than malformed: it is the one arity
// exception among the slice fields. QuantifiedNode documents the required lowering, which is a
// ConstPredNode rather than an empty list. Elements are raw Go values with the same caveats as
// BindNode.Value.
//
// It is a SetValue and NOTHING else — not a Projection — because a bound set is a parameter
// rather than a column. It can therefore appear in exactly one place: QuantifiedNode.Set.
type SetBindNode struct{ Elems []any }

// --- predicate nodes --------------------------------------------------------------------
//
// Every operand field on the fixed-arity nodes below (CompareNode, LikeNode, BetweenNode,
// IsNullNode) is REQUIRED, per the package-level rule: an interface-typed field is required
// unless its own doc says it is nil-able. None of these has a nil-able field, so they are not
// annotated individually. The nodes with arity or nil-ability rules of their own — InNode,
// QuantifiedNode, ExistsNode, LogicalNode — say so.

// CompareNode renders "<Left> <Op> <Right>" for one of the six comparison operators. Op is
// the Op enum, not a raw symbol, so no unrecognized infix operator can reach a renderer.
type CompareNode struct {
	Op          Op
	Left, Right ScalarValue
}

// LikeNode renders "<Left> LIKE <Pattern>". Pattern matching is its own node rather than a
// CompareNode with a LIKE operator, because LIKE is not a comparison operator: it cannot be
// quantified (there is no ANSI "LIKE ANY"), and keeping it separate means QuantifiedNode's Op
// field admits only values that are actually quantifiable.
//
// There is no ESCAPE field, so a pattern's metacharacters are always significant: a bound
// pattern containing a literal % or _ wildcards, and the tree cannot say it should not.
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
// Elems must hold at least ONE element. An empty IN list has no SQL rendering — "x IN ()" is a
// syntax error on every engine — so a consumer reaching one has been handed a malformed tree and
// should treat it as corruption. A caller who genuinely means "matches nothing" writes
// ConstPredNode{Value: false}, which is what the empty-set lowering of a QuantifiedNode produces;
// see that node.
type InNode struct {
	Left  ScalarValue
	Elems []ScalarValue
}

// QuantifiedNode is a quantified comparison of Left against the set operand Set. Both fields are
// required. Set is the ONLY field in the tree typed SetValue, so a set node reaching a consumer
// anywhere else means the consumer walked into it by mistake.
//
// A SetBindNode with no elements is legal here and must be lowered to a CONSTANT: All is vacuously
// true, Any vacuously false, both spelled with a ConstPredNode's rendering. Most engines have no
// representation for an empty set operand, so this is not an optimization but the only correct
// output — and it is why an empty InNode.Elems is malformed while an empty SetBindNode.Elems is not:
// this node says what the empty case means, and InNode has nowhere to say it.
type QuantifiedNode struct {
	Left ScalarValue
	Op   Op
	Q    Quantifier
	Set  SetValue
}

// ExistsNode renders "EXISTS (<Stmt>)". Negate with NotNode for NOT EXISTS.
//
// Stmt is REQUIRED and never nil. It is the one pointer field in the tree that is not nil-able
// (with SubqueryNode.Stmt), so a consumer need not guard it — but note that the cost of being
// wrong is a nil dereference rather than a clean panic, which is why it is called out.
type ExistsNode struct{ Stmt *Select }

// LogicalNode is an AND/OR of its parts.
//
// Parts must hold at least ONE predicate; two or more is the meaningful case. An empty Parts has
// no rendering — "WHERE ()" is a syntax error — and a consumer should treat one as corruption
// rather than emitting an empty group. A caller wanting an unconditional clause writes
// ConstPredNode; a caller with one condition uses it directly rather than wrapping it.
//
// A single-part LogicalNode is legal, and renders as that one operand with whatever grouping the
// consumer applies to the node — the connective simply has nothing to join. It is permitted so
// that a builder accumulating conditions into a slice may emit one unconditionally; a builder that
// prefers to collapse the length-one case to its operand is equally correct, and produces less
// nesting.
type LogicalNode struct {
	Op    LogicalOp
	Parts []Predicate
}

// NotNode negates its inner predicate. Inner is required.
type NotNode struct{ Inner Predicate }

// ConstPredNode is the constant predicate: unconditionally true or unconditionally false.
//
// It exists because every adapter needs one and no target spells a bare boolean the same way.
// The forcing case is lowering an EMPTY bound set: "x = ANY ({})" is vacuously false and
// "x = ALL ({})" vacuously true, and most engines have no representation for an empty set
// operand, so each renderer was independently inventing "1 = 1" / "1 = 0". Giving the constant
// a node means the builder can also express an unconditional clause without reaching for a
// boolean literal — which this tree no longer has, truth values being a category rather than a
// value type.
type ConstPredNode struct{ Value bool }

// --- value nodes ------------------------------------------------------------------------

// CastNode renders a cast of Inner to Type. Inner is required. Type is the CastType enum, so the
// target spelling is the renderer's to choose — including any length or precision the target
// requires, which CastType cannot carry.
type CastNode struct {
	Inner ScalarValue
	Type  CastType
}

// FuncNode is a scalar function call over ordinary value arguments. The JSON OBJECT
// constructor is not among them: it takes key/value pairs, so it is JSONObjectNode.
//
// The required ARITY of Args depends on Fn and is documented on each ScalarFunc member: exactly
// one for the case-folding functions, one or more for FuncCoalesce, and zero or more for
// FuncJSONArray alone. Args being one slice for every function is what makes this a documented
// contract rather than a typed one, so a consumer that wants the guarantee checked must check it —
// and, per the package doc, may panic when it fails.
type FuncNode struct {
	Fn   ScalarFunc
	Args []ScalarValue
}

// JSONPairNode is one key/value pair of a JSON object constructor.
//
// It belongs to NO position category — it is not a Predicate, ScalarValue, SetValue, or
// Projection — so the only place it can appear is JSONObjectNode.Pairs. That is deliberate: a
// pair is not an expression, and before the categories existed a bare pair could be dropped
// into a projection list, where a renderer would dutifully emit a fragment like
// "KEY 'k' VALUE x" as though it were a column.
//
// The pair structure survives into the renderer because consumers need it in different shapes:
// the ANSI form is "k VALUE v" while MySQL and Postgres both take a flat "k, v" argument list.
//
// Both fields are required.
type JSONPairNode struct {
	Key, Value ScalarValue
}

// JSONObjectNode is the JSON object constructor. It is its own node rather than a ScalarFunc
// because its arguments are pairs rather than values, and typing Pairs as []JSONPairNode is
// what confines a pair to the one construct that gives it meaning.
//
// Pairs must hold at least ONE pair. The empty object is arguably meaningful and JSON_OBJECT() is
// valid SQL on several engines, but it is excluded here because it is not portable — so a caller
// who wants one must bind or inline the two-character literal instead of asking each adapter to
// decide. Contrast FuncJSONArray, whose empty form IS admitted.
type JSONObjectNode struct{ Pairs []JSONPairNode }

// --- the aggregate node and its modifiers -----------------------------------------------
//
// AggNode is the only aggregate. It carries two modifier fields, and a consumer renders them
// uniformly:
//
//   - Distinct — de-duplicate the input before aggregating. Meaningful only when there IS an
//     argument, so it is malformed alongside AggNode's empty-Args form: COUNT(DISTINCT *) is not
//     a construct.
//   - Filter  — restrict WHICH ROWS reach the aggregate; nil when absent. Several engines lack
//     the ANSI FILTER clause, so a consumer that cannot emit one emulates it by pushing the
//     condition into the aggregated expression. When doing so, an excluded row must yield NULL
//     rather than a falsy value, or the emulation changes the result.
//
// There is no input-ORDER-BY modifier: the only aggregate that survives (COUNT) is
// order-insensitive, so ordering its input would change nothing. The collecting aggregates that
// made input order observable were dropped for lacking portable spellings across the targets.

// AggNode is a VALUE-producing aggregate call with the modifiers ANSI permits.
//
// The required ARITY of Args is per-Fn and documented on each AggFunc member. For AggCount, empty
// Args means COUNT ROWS and one argument means count that expression's values; a consumer keying
// its "*" rendering on len(Args) == 0 does so inside the AggCount case.
//
// That AggCount's row-counting form is an ABSENT argument rather than a flag is load-bearing: a
// backend which must restructure a filtered aggregate can substitute a constant for the absent
// argument, a rewrite that would produce nonsense if the form were modelled as a fabricated "*"
// operand.
//
// Distinct and Filter are the aggregate modifiers; see the comment above them for what a consumer
// may assume, and note that Distinct is meaningful only with an argument present.
type AggNode struct {
	Fn       AggFunc
	Args     []ScalarValue
	Distinct bool
	Filter   Predicate
}

// CaseSearchedNode is a searched CASE: each branch's When is a PREDICATE, evaluated on its own.
// Else is nil when the CASE has no ELSE. Branches are evaluated in order; the first match wins.
//
// Branches must hold at least ONE branch: "CASE END" is a syntax error, and a CASE with only an
// ELSE is just that value. A consumer reaching an empty Branches has a malformed tree.
//
// The two CASE forms are separate node types rather than one node discriminated by a nil Base,
// because the forms differ in the CATEGORY of their WHEN — a predicate here, a value in the
// simple form — and a single node could only have typed that slot as the union of the two,
// putting the discrimination back in the walker's hands.
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

// CaseSimpleNode is a simple CASE: each branch's When is a VALUE, compared for equality
// against Base. Else is nil when the CASE has no ELSE.
//
// Base is REQUIRED — it is what distinguishes this form, and a nil Base is not the searched form
// but a malformed tree. Branches must hold at least ONE branch, as in the searched form.
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

// AliasNode binds an output name to a projection. It is a Projection and NOTHING else — not a
// ScalarValue — so it cannot be nested inside an expression. The one position it is legal in is
// the only position the type system admits it.
//
// Inner is required. Alias is one of the two free strings in the tree (Table.Alias is the other)
// and is NOT validated here: nothing checks its length, its character set, or whether it collides
// with another output name. A consumer emitting text owes it whatever quoting or rejection its
// target requires — this is the last unconstrained string a hostile or merely careless caller can
// reach a consumer with.
type AliasNode struct {
	Inner Projection
	Alias string
}

// SubqueryNode embeds a Select as a parenthesised subquery. It is the one node spanning three
// categories: a scalar subquery is a ScalarValue, a multi-row subquery is a SetValue, and either
// may be projected.
//
// Stmt is REQUIRED and never nil, as with ExistsNode.Stmt.
//
// Because the node spans categories, WHICH category a consumer reached it through determines the
// correct rendering — a scalar subquery generally needs its own parentheses, a set operand usually
// sits inside parentheses the quantified form already supplies. That is read off the walk function
// it arrives in, which is one of the concrete payoffs of splitting walkers by category.
type SubqueryNode struct{ Stmt *Select }

// --- statement --------------------------------------------------------------------------

// Select is a SELECT statement node. Its fields are exported so a renderer walks it directly;
// package query provides the typed builder that populates it.
//
// Note the field types: Columns is []Projection, Where and Having are Predicate, GroupBy is
// []ScalarValue. Those are not documentation — they are why `Where: someColNode` and
// `Columns: []Projection{someCompareNode}` do not compile.
//
// Where and Having are each a SINGLE predicate node, nil when the clause is absent. They were
// once []Node ANDed by convention, which had two costs: the conjunction was invisible at the
// call site (a caller passing two predicates got an AND without writing one), and a consumer
// had to know the convention to render the slice correctly. A caller that wants several
// conditions now composes them explicitly with a LogicalNode, so the operator is written where
// it applies and a consumer reads the combination off the tree instead of a comment.
//
// Limit and Offset are nil when absent, which is why they are pointers: zero is a meaningful
// LIMIT.
//
// # Cross-field rules
//
// Two relationships between these fields are part of the contract and are typed by nothing. The
// first is permitted and needs no handling; the second is the one rule Validate cannot fully
// reach:
//
//   - HAVING WITHOUT GROUPBY is permitted, and means "over the whole result as one group", which
//     is what SQL means by it. It is not malformed and needs no special handling.
//
//   - ALIAS SCOPE IS UNCHECKED. A ColNode's Alias should name a Table appearing in From or Joins
//     of the Select it is used in (or an enclosing one, for a correlated subquery). Validate does
//     not verify it — doing so means collecting column references from every clause and threading
//     enclosing scopes through subqueries, which is a resolver rather than a shape-checker — and a
//     consumer walking a clause at a time cannot either. A ColNode naming an alias that is nowhere
//     in scope renders as a plausible qualified column and fails at the engine, which is the
//     acceptable end of the failure spectrum. Validate does catch the local half of the problem: a
//     DUPLICATE alias among this statement's own tables, which would make such a column ambiguous.
//
// An empty From is legal and expresses a constant-only projection; a consumer whose target has no
// FROM-less SELECT supplies its own pseudo-table. Columns may be empty, meaning all columns.
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

// Table is one aliased occurrence of the tuple table. Alias is a free, UNVALIDATED string, as
// AliasNode.Alias is; see that node. Nothing checks for a duplicate alias within one Select either,
// which would make a ColNode referring to it ambiguous.
type Table struct{ Alias string }

// JoinClause is a join of tuple against another alias. On is nil for a CROSS join, which
// carries no condition, and required for every other JoinType — a nil On with JoinInner is
// malformed, not an implicit cross join. A non-nil On with JoinCross is likewise malformed; a
// consumer must not silently drop the condition, since doing so changes which rows return.
type JoinClause struct {
	Type  JoinType
	Table Table
	On    Predicate
}

// OrderTerm is a single ordering term, used by Select.OrderBy. It is a clause item rather than a
// node in any category: it is never an operand, so it cannot appear where a value or a predicate
// is expected.
//
// Expr is required. Dir has a meaningful zero value (Ascending), so the zero OrderTerm is
// malformed only in its Expr. There is no NULLS FIRST / NULLS LAST control: MySQL has no such
// clause, so a target-neutral tree cannot carry one.
type OrderTerm struct {
	Expr ScalarValue
	Dir  SortDirection
}

// --- category membership ----------------------------------------------------------------
//
// The markers below ARE the category assignments. Reading them top to bottom is the fastest
// way to see which node may appear where.

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
// SubqueryNode.Stmt are typed concretely as *Select — so a statement never appears in an
// operand position, and a walker recurses into a subquery through its statement function.
func (*Select) isNode() {}
