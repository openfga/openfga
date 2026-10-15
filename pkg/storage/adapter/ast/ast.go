// Package ast holds a SELECT query as a tree of plain Go structs with no SQL text. Package query
// builds the tree; each backend writes it as SQL. See README.md for writing a backend.
package ast

import "strconv"

// Node is any piece of the query — one operator, value, clause, or subquery — and every node type
// implements it. A function that walks the tree should take the narrowest of the four interfaces its
// slot allows, so the compiler rejects a wrong-kind node.
type Node interface {
	isNode()

	// Validate returns the first problem in this node or its children, or nil. Parents are checked
	// before children, so the reported problem is usually the cause. The error starts with its path
	// (`Where: Parts[1]: Left: ...`) and wraps an Err sentinel.
	//
	// It does not check alias scope, driver value types, or whether non-aggregated columns are in
	// GROUP BY.
	Validate() error
}

// Projection is a node that can be a result column in the SELECT list: every ScalarValue, plus
// AliasNode and SubqueryNode.
//
// The enums that differ between databases each end in a Count value, so a backend can check that
// it handles every case. Node types do not have that, so a backend cannot check that it handles
// every Projection; it should panic on an unknown type.
type Projection interface {
	Node
	isProjection()
}

// ScalarValue is a node that is a single value: one side of a comparison, a function argument, a
// cast input, a GROUP BY or ORDER BY term. A single value can also be a result column, so it is a
// Projection too.
type ScalarValue interface {
	Projection
	isScalarValue()
}

// SetValue is a node that is a group of values, used only as the right side of an ANY/ALL comparison
// (QuantifiedNode.Set). Its two members are SetBindNode (a bound list, not a column) and SubqueryNode.
// It is not a Projection because only some sets can be a column.
type SetValue interface {
	Node
	isSetValue()
}

// Predicate is a node that is a true/false test: WHERE, HAVING, ON, a WHEN in a searched CASE, an
// aggregate's FILTER, or part of an AND/OR.
//
// A Predicate is deliberately not a value. Some databases have no boolean type (e.g. Oracle before
// 23c), so the tree keeps tests and values apart. For a boolean result column, write a CASE that
// returns one.
type Predicate interface {
	Node
	isPredicate()
}

// --- enums the same on every SQL database -----------------------------------------------

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

// LogicalOp is AND or OR.
type LogicalOp int

const (
	And LogicalOp = iota
	Or
)

// Quantifier is ANY or ALL.
type Quantifier int

const (
	Any Quantifier = iota
	All
)

// JoinType is the kind of join the tree supports. RIGHT and FULL OUTER are left out: MySQL has no
// FULL OUTER JOIN, and SQLite only got RIGHT/FULL in 3.39, so they are not safe on every database.
type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeftOuter
	JoinCross
)

// SortDirection is the ASC / DESC direction of an ORDER BY term.
type SortDirection int

const (
	// Ascending is the default; most databases write nothing for it.
	Ascending SortDirection = iota
	Descending
)

// --- enums that differ between databases (no text; each backend writes its own) ----------

// Column is one column of the tuple table, as a number with no name attached. The real column name
// may differ per database, so each backend supplies the names. ColCount lets a backend check that it
// handles every column.
type Column uint8

const (
	ColObjectType Column = iota
	ColObjectID
	// ColObjectRelation is the object-side relation.
	ColObjectRelation

	ColSubjectType
	ColSubjectID
	// ColSubjectRelation is the subject-side relation, e.g., `member` in `group:eng#member`.
	ColSubjectRelation

	// ColStore is the tenant scope, filtered on every query.
	ColStore

	// ColCondition is the name of the ABAC condition on the tuple.
	ColCondition
	// ColConditionContext is the encoded context for ColCondition.
	ColConditionContext

	// ColCount is the number of columns, not a column. A backend can use it to check it handles
	// every column. Because it is still a Column value, `ColNode{Name: ColCount}` compiles but is
	// invalid, so a backend's switch over columns should panic in its default case. Same idea for
	// TypeCount, FuncCount, and AggCount_.
	ColCount
)

// String names the column for error messages only; it is never used as a real column name.
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

// CastType is the target type of a CAST, as a number with no name attached (like Column), because the
// name differs per database: the same "text" target is `VARCHAR` in ANSI SQL, `text` on Postgres, and
// `CHAR` on MySQL. Each backend supplies the name.
//
// There is no boolean target, because the tree has no boolean value. A target has no length or
// precision either, so a backend that needs a size picks a maximum.
type CastType uint8

const (
	TypeVarchar CastType = iota
	TypeInteger
	TypeBigint
	TypeNumeric
	TypeVarbinary

	// TypeCount is the number of cast types, not a type; a backend can use it to check coverage.
	TypeCount
)

// String names the cast type for error messages only; it is never used as a real type name.
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

// ScalarFunc is a scalar function the tree can call — a small, growable set. Like Column and CastType
// it has no name attached: ANSI SQL spells a JSON constructor `JSON_OBJECT`, Postgres spells it
// `jsonb_build_object`.
//
// The JSON object constructor is not here: it takes key/value pairs, not a plain argument list, so it
// is its own node (JSONObjectNode). FuncJSONArray stays, because its arguments are plain values.
type ScalarFunc uint8

// Each member states how many arguments it takes, because FuncNode.Args is one list used by all of
// them. A FuncNode with the wrong number of arguments for its function is invalid.
const (
	// FuncCoalesce returns the first non-NULL argument. Takes one or more.
	FuncCoalesce ScalarFunc = iota
	// FuncLower / FuncUpper change case. ANSI SQL has no case-insensitive LIKE, so lowering both sides
	// is how you get that. Takes exactly one.
	FuncLower
	FuncUpper
	// FuncJSONArray builds a JSON array from plain arguments. Takes zero or more — the only function
	// here that can take none.
	FuncJSONArray

	// FuncCount is the number of functions, not a function; used to check coverage.
	FuncCount
)

// String names the function for error messages only; it is never used as a real function name.
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

// AggFunc is an aggregate whose result and arguments are single values. It has no name attached; here
// databases differ not just in spelling but in whether the aggregate exists at all.
//
// The set is kept small: only aggregates every SQL database has. The collecting ones (ARRAY_AGG, the
// JSON aggregates) and the true/false ones (EVERY / SOME) are left out, as they are not supported on
// Postgres, MySQL, and SQLite alike.
type AggFunc uint8

const (
	// AggCount is COUNT(x), or COUNT(*) when AggNode.Args is empty. Takes zero or one argument; see
	// AggNode for why the row-counting form is an empty argument list, not a flag. COUNT(DISTINCT *)
	// does not exist, so AggNode.Distinct only makes sense with an argument.
	AggCount AggFunc = iota

	// AggCount_ is the number of aggregates, not an aggregate; used to check coverage. The trailing
	// underscore keeps it from clashing with AggCount, the COUNT aggregate.
	AggCount_
)

// String names the aggregate for error messages only; it is never used as a real aggregate name.
func (a AggFunc) String() string {
	switch a {
	case AggCount:
		return "Count"
	default:
		return "AggFunc(" + strconv.Itoa(int(a)) + ")"
	}
}

// --- leaf nodes -------------------------------------------------------------------------

// ColNode refers to one column of one table in the query. Alias should match a Table used in the same
// SELECT; nothing here checks that. See Select's "Rules across fields".
type ColNode struct {
	Alias string
	Name  Column
}

// BindNode is a single value sent to the database as a parameter.
//
// Value is `any` — the loosest spot in the tree. The backend hands it to the database driver mostly
// as-is, so a value the driver cannot handle fails only when the query runs.
//
// Validate rejects the two kinds of value the tree does not allow here: a bool (true/false is a
// Predicate, not a value) and an AST node. It cannot tell whether the driver accepts the value, which
// is why the field stays `any`.
type BindNode struct{ Value any }

// LitNode is a value the builder wants written straight into the SQL text instead of sent as a
// parameter. It holds the raw Go value just like BindNode; only that choice differs. Inlining is for
// plan-cache behaviour and readability, never because a value cannot be a parameter.
//
// Value is the raw value, not ready-made SQL, so escaping is the backend's job. A backend that cannot
// inline safely may treat a LitNode exactly like a BindNode. As with BindNode, Validate rejects a
// bool; a constant true/false is ConstPredNode.
type LitNode struct{ Value any }

// SetBindNode is a list of values sent as a parameter. The backend decides how to write it (an array
// parameter where supported, otherwise an expanded IN / AND / OR list).
//
// Elems may be empty, and that is meaningful, not a mistake — the one list in the tree allowed to be
// empty; QuantifiedNode explains what an empty set becomes. It is a set only, never a result column (a
// parameter is not a column), so it is used only as QuantifiedNode.Set.
type SetBindNode struct{ Elems []any }

// --- predicate nodes --------------------------------------------------------------------
//
// On the fixed-size nodes below (CompareNode, LikeNode, BetweenNode, IsNullNode) every field is
// required. The nodes with their own rules — InNode, QuantifiedNode, ExistsNode, LogicalNode — say so.

// CompareNode is "Left Op Right" for one of the six comparison operators.
type CompareNode struct {
	Op          Op
	Left, Right ScalarValue
}

// LikeNode is "Left LIKE Pattern". It is its own node, not a CompareNode with a LIKE operator, because
// LIKE cannot be used with ANY/ALL (there is no "LIKE ANY" in ANSI SQL).
//
// There is no ESCAPE option, so pattern characters always count: a parameter holding a literal % or _
// acts as a wildcard.
type LikeNode struct {
	Left, Pattern ScalarValue
}

// BetweenNode is "Inner BETWEEN Lo AND Hi".
type BetweenNode struct {
	Inner, Lo, Hi ScalarValue
}

// IsNullNode is "Inner IS NULL". Wrap in NotNode for IS NOT NULL.
type IsNullNode struct{ Inner ScalarValue }

// InNode is "Left IN (Elems...)" over a listed-out set of values.
//
// Elems must have at least one value: "x IN ()" is a syntax error everywhere. To mean "matches
// nothing", use ConstPredNode{Value: false}.
type InNode struct {
	Left  ScalarValue
	Elems []ScalarValue
}

// QuantifiedNode compares Left against a set, using ANY or ALL. Set is the only field in the tree
// typed as SetValue.
//
// An empty SetBindNode is allowed here and becomes a constant: ALL of an empty set is true, ANY of it
// is false — both written as a ConstPredNode. Most databases cannot write an empty set directly, which
// is why an empty InNode is invalid but an empty SetBindNode is fine.
type QuantifiedNode struct {
	Left ScalarValue
	Op   Op
	Q    Quantifier
	Set  SetValue
}

// ExistsNode is "EXISTS (Stmt)". Wrap in NotNode for NOT EXISTS. Stmt is required.
type ExistsNode struct{ Stmt *Select }

// LogicalNode is an AND or OR of its parts.
//
// Parts must have at least one predicate; empty is invalid. One part is allowed and renders as just
// that part, so a builder collecting conditions into a list can always emit a LogicalNode.
type LogicalNode struct {
	Op    LogicalOp
	Parts []Predicate
}

// NotNode negates its inner predicate. Inner is required.
type NotNode struct{ Inner Predicate }

// ConstPredNode is a constant true or false.
//
// It exists because every backend needs one and no database writes a bare boolean the same way. The
// case that forces it is an empty set: "x = ANY ({})" is always false and "x = ALL ({})" always true,
// which most databases cannot write directly.
type ConstPredNode struct{ Value bool }

// --- value nodes ------------------------------------------------------------------------

// CastNode casts Inner to Type. The backend chooses the exact type name, including any length or
// precision.
type CastNode struct {
	Inner ScalarValue
	Type  CastType
}

// FuncNode is a call to a scalar function with plain value arguments. The JSON object constructor is
// not one of these — it takes key/value pairs, so it is JSONObjectNode.
//
// How many arguments Args needs depends on Fn and is written on each ScalarFunc; code that wants this
// checked must check it.
type FuncNode struct {
	Fn   ScalarFunc
	Args []ScalarValue
}

// JSONPairNode is one key/value pair in a JSON object.
//
// It fits none of the four slots, so it can only appear in JSONObjectNode.Pairs — a pair is not a
// value. The pair is kept whole because databases need it in different shapes: ANSI SQL writes
// "k VALUE v", while MySQL and Postgres take a flat "k, v" list.
//
// Both fields are required.
type JSONPairNode struct {
	Key, Value ScalarValue
}

// JSONObjectNode builds a JSON object. It is its own node, not a ScalarFunc, because its arguments are
// pairs, not plain values.
//
// Pairs must have at least one pair: an empty JSON_OBJECT() is not supported everywhere, unlike
// FuncJSONArray, whose empty form is allowed.
type JSONObjectNode struct{ Pairs []JSONPairNode }

// --- the aggregate node and its options -------------------------------------------------
//
// AggNode is the only aggregate. It has two options:
//
//   - Distinct — remove duplicate inputs before aggregating. Only makes sense with an argument:
//     COUNT(DISTINCT *) does not exist.
//   - Filter  — count only the rows that match a test; nil when not used. A backend without the ANSI
//     FILTER clause fakes it by pushing the test into the counted expression, where a skipped row must
//     become NULL — a false-y value would change the result.
//
// There is no "ORDER BY inside the aggregate" option: the one aggregate left, COUNT, does not care
// about order.

// AggNode is an aggregate call with the options ANSI SQL allows.
//
// How many arguments Args needs depends on Fn. For AggCount, no argument counts rows and one argument
// counts that expression's values.
//
// Keeping COUNT's row-counting form as an empty argument list (not a flag) matters: a backend
// rewriting a filtered aggregate can drop in a constant for the missing argument, which would make no
// sense if the form were a fake "*" argument. See above for Distinct and Filter.
type AggNode struct {
	Fn       AggFunc
	Args     []ScalarValue
	Distinct bool
	Filter   Predicate
}

// CaseSearchedNode is a searched CASE: each branch has its own true/false test. Branches are checked
// in order; the first match wins. Else is nil when there is no ELSE. There must be at least one branch.
//
// The two CASE forms are separate node types, not one node with an optional Base, because their WHEN
// differs: a test here, a value in the simple form.
type CaseSearchedNode struct {
	Branches []SearchedBranch
	Else     ScalarValue
}

// SearchedBranch is one WHEN/THEN of a searched CASE: a test, then a value. Both are required.
type SearchedBranch struct {
	When Predicate
	Then ScalarValue
}

// CaseSimpleNode is a simple CASE: each branch's value is compared for equality against Base. Else is
// nil when there is no ELSE.
//
// Base is required — a nil Base is not the searched form, it is an invalid simple one. There must be
// at least one branch, same as the searched form.
type CaseSimpleNode struct {
	Base     ScalarValue
	Branches []SimpleBranch
	Else     ScalarValue
}

// SimpleBranch is one WHEN/THEN of a simple CASE: both sides are values, both required.
type SimpleBranch struct {
	When ScalarValue
	Then ScalarValue
}

// AliasNode gives a result column a name. It can only be a result column, never part of an expression.
// Alias is one of the two free-text strings in the tree (Table.Alias is the other); it is not checked
// here for allowed characters or clashes.
type AliasNode struct {
	Inner Projection
	Alias string
}

// SubqueryNode wraps a Select as a subquery in parentheses. It is the one node that fits three slots:
// a single value, a set, and a result column. Stmt is required.
//
// Which slot it was used in decides how to write it — a value subquery usually needs its own
// parentheses, a set subquery sits inside the ones the ANY/ALL form already adds.
type SubqueryNode struct{ Stmt *Select }

// --- statement --------------------------------------------------------------------------

// Select is a SELECT statement. Its fields are exported so a backend can read it directly; package
// query has the type-safe builder that fills it in.
//
// # Rules across fields
//
//   - HAVING without GROUP BY is allowed, and means "treat the whole result as one group".
//   - Table aliases are not checked for scope. A column's Alias should match a Table in this SELECT's
//     From or Joins (or in a SELECT around it, for a correlated subquery). Validate does not check this
//     — that is a resolver's job — so a wrong alias makes valid-looking SQL that fails at the database.
//     Validate does catch two of this SELECT's own tables sharing an alias.
//
// An empty From means a SELECT with no table (constants only). Empty Columns means all columns.
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

// Table is one use of the tuple table under an alias. Alias is free text and unchecked here. Two tables
// with the same alias in one SELECT make a column that names it ambiguous.
type Table struct{ Alias string }

// JoinClause joins the tuple table against another alias. On is nil for a CROSS join and required for
// every other kind — a nil On elsewhere is invalid, and so is a non-nil On on a CROSS join, since
// dropping the condition would change which rows come back.
type JoinClause struct {
	Type  JoinType
	Table Table
	On    Predicate
}

// OrderTerm is one ORDER BY term, used by Select.OrderBy. It fits none of the four slots — it is a
// clause item, not an expression. Dir defaults to Ascending. There is no NULLS FIRST / NULLS LAST
// option since MySQL has none.
type OrderTerm struct {
	Expr ScalarValue
	Dir  SortDirection
}

// --- slot membership --------------------------------------------------------------------
//
// The methods below put each node into its slots; reading top to bottom shows which node goes where.

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

// Single values. Each is also a result column, through ScalarValue.
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

// Sets. SetBindNode is a set only; SubqueryNode can also be a result column.
func (SetBindNode) isNode()     {}
func (SetBindNode) isSetValue() {}

// SubqueryNode fits three slots.
func (SubqueryNode) isNode()        {}
func (SubqueryNode) isProjection()  {}
func (SubqueryNode) isScalarValue() {}
func (SubqueryNode) isSetValue()    {}

// AliasNode can only be a result column.
func (AliasNode) isNode()       {}
func (AliasNode) isProjection() {}

// JSONPairNode fits no slot: it is reached only through JSONObjectNode.Pairs. It is still a Node so a
// walk over the whole tree can see it.
func (JSONPairNode) isNode() {}

// *Select is a Node only. No field holds one loosely — ExistsNode.Stmt and SubqueryNode.Stmt are typed
// as *Select — so a statement never shows up where a value or test is expected.
func (*Select) isNode() {}
