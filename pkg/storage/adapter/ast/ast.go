// Package ast holds a SELECT query as plain Go structs with no SQL text. Package query builds it;
// each backend renders it.
package ast

import "strconv"

// Node is any node in the tree. Walkers should accept the narrowest of the four slot interfaces.
type Node interface {
	isNode()

	// Validate returns the first structural problem in the node or its children. It does not
	// check alias scope, driver value types, or GROUP BY coverage.
	Validate() error
}

// Projection is a node allowed in the SELECT list.
//
// Unlike the enums, node types have no Count value, so backends should panic on unknown types.
type Projection interface {
	Node
	isProjection()
}

// ScalarValue is a node that is a single value.
type ScalarValue interface {
	Projection
	isScalarValue()
}

// SetValue is a node that is a group of values; only valid as QuantifiedNode.Set.
type SetValue interface {
	Node
	isSetValue()
}

// Predicate is a true/false test. It is deliberately not a value, since some databases have no
// boolean type; use a CASE for a boolean result column.
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

// JoinType omits RIGHT and FULL OUTER, which MySQL and older SQLite lack.
type JoinType int

const (
	JoinInner JoinType = iota
	JoinLeftOuter
	JoinCross
)

// SortDirection is the direction of an ORDER BY term.
type SortDirection int

const (
	Ascending SortDirection = iota
	Descending
)

// --- enums that differ between databases (each backend supplies the spelling) ----------

// Column is a logical column of the tuple table.
type Column uint8

const (
	ColObjectType Column = iota
	ColObjectID
	ColObjectRelation

	ColSubjectType
	ColSubjectID
	// ColSubjectRelation is e.g., `member` in `group:eng#member`.
	ColSubjectRelation

	ColStore

	ColCondition
	ColConditionContext

	// ColCount is not a column - it's the number of columns, so backends can ensure full coverage.
	ColCount
)

// String is for error messages only.
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

// CastType is the target type of a CAST. It has no boolean member and no length/precision.
type CastType uint8

const (
	TypeVarchar CastType = iota
	TypeInteger
	TypeBigint
	TypeNumeric
	TypeVarbinary

	// TypeCount is not a cast type - it's the number of types, so backends can ensure full coverage.
	TypeCount
)

// String is for error messages only.
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

// ScalarFunc is a scalar function.
type ScalarFunc uint8

const (
	// FuncCoalesce takes one or more arguments.
	FuncCoalesce ScalarFunc = iota
	// FuncLower and FuncUpper take exactly one.
	FuncLower
	FuncUpper
	// FuncJSONArray takes zero or more.
	FuncJSONArray

	// FuncCount is not a scalar function - it's the number of functions, so backends can ensure full coverage.
	FuncCount
)

// String is for error messages only.
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

// AggFunc is an aggregate function. Only aggregates every supported database has are included.
type AggFunc uint8

const (
	// AggCount takes zero arguments (COUNT(*)) or one.
	AggCount AggFunc = iota

	// AggCount_ is not an aggregate function - it's the number of functions, so backends can ensure full coverage.
	AggCount_
)

// String is for error messages only.
func (a AggFunc) String() string {
	switch a {
	case AggCount:
		return "Count"
	default:
		return "AggFunc(" + strconv.Itoa(int(a)) + ")"
	}
}

// --- leaf nodes -------------------------------------------------------------------------

// ColNode is a column of the table with the given alias.
type ColNode struct {
	Alias string
	Name  Column
}

// BindNode is a value sent as a parameter. Value is passed to the driver as-is, so unsupported
// types fail only at execution. Positional parameters must be numbered in SQL output order.
type BindNode struct{ Value any }

// LitNode is a value inlined into the SQL text. The backend escapes it, or may bind it instead.
// Switch on reflect.Kind, not Go type: a named string type misses "case string" and may be inlined unquoted.
type LitNode struct{ Value any }

// SetBindNode is a list of values sent as a parameter. Elems may be empty.
type SetBindNode struct{ Elems []any }

// --- predicate nodes --------------------------------------------------------------------

// CompareNode is "Left Op Right".
type CompareNode struct {
	Op          Op
	Left, Right ScalarValue
}

// LikeNode is "Left LIKE Pattern", case-sensitive, with only % and _ special (no backslash escape).
// LIKE differs on databases; backends must add what's needed (e.g., Postgres ESCAPE), or return an error.
type LikeNode struct {
	Left, Pattern ScalarValue
}

// BetweenNode is "Inner BETWEEN Lo AND Hi".
type BetweenNode struct {
	Inner, Lo, Hi ScalarValue
}

// IsNullNode is "Inner IS NULL".
type IsNullNode struct{ Inner ScalarValue }

// InNode is "Left IN (Elems...)". Elems must be non-empty.
type InNode struct {
	Left  ScalarValue
	Elems []ScalarValue
}

// QuantifiedNode is "Left Op ANY|ALL (Set)". An empty SetBindNode should render as a constant
// (ANY -> false, ALL -> true).
type QuantifiedNode struct {
	Left ScalarValue
	Op   Op
	Q    Quantifier
	Set  SetValue
}

// ExistsNode is "EXISTS (Stmt)".
type ExistsNode struct{ Stmt *Select }

// LogicalNode is an AND or OR of Parts. Parts must be non-empty.
type LogicalNode struct {
	Op    LogicalOp
	Parts []Predicate
}

// NotNode negates Inner.
type NotNode struct{ Inner Predicate }

// ConstPredNode is a constant true or false.
type ConstPredNode struct{ Value bool }

// --- value nodes ------------------------------------------------------------------------

// CastNode casts Inner to Type.
type CastNode struct {
	Inner ScalarValue
	Type  CastType
}

// FuncNode is a scalar function call.
type FuncNode struct {
	Fn   ScalarFunc
	Args []ScalarValue
}

// JSONPairNode is one key/value pair of a JSONObjectNode.
type JSONPairNode struct {
	Key, Value ScalarValue
}

// JSONObjectNode builds a JSON object. Pairs must be non-empty.
type JSONObjectNode struct{ Pairs []JSONPairNode }

// AggNode is an aggregate call. Filter is optional. A backend emulating Filter must map skipped
// rows to NULL so they are not counted.
type AggNode struct {
	Fn       AggFunc
	Args     []ScalarValue
	Distinct bool
	Filter   Predicate
}

// CaseSearchedNode is "CASE WHEN cond THEN ... [ELSE ...] END". Branches must be non-empty.
type CaseSearchedNode struct {
	Branches []SearchedBranch
	Else     ScalarValue
}

// SearchedBranch is one WHEN/THEN of a CaseSearchedNode.
type SearchedBranch struct {
	When Predicate
	Then ScalarValue
}

// CaseSimpleNode is "CASE Base WHEN value THEN ... [ELSE ...] END". Branches must be non-empty.
type CaseSimpleNode struct {
	Base     ScalarValue
	Branches []SimpleBranch
	Else     ScalarValue
}

// SimpleBranch is one WHEN/THEN of a CaseSimpleNode.
type SimpleBranch struct {
	When ScalarValue
	Then ScalarValue
}

// AliasNode names a result column.
type AliasNode struct {
	Inner Projection
	Alias string
}

// SubqueryNode is a parenthesized subquery, usable as a value, a set, or a result column.
type SubqueryNode struct{ Stmt *Select }

// --- statement --------------------------------------------------------------------------

// Select is a SELECT statement. Empty From means no table; empty Columns means all columns.
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

// Table is the tuple table under an alias.
type Table struct{ Alias string }

// JoinClause is a join. On must be nil for JoinCross and non-nil otherwise.
type JoinClause struct {
	Type  JoinType
	Table Table
	On    Predicate
}

// OrderTerm is one ORDER BY term.
type OrderTerm struct {
	Expr ScalarValue
	Dir  SortDirection
}

// --- slot membership --------------------------------------------------------------------

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

func (SetBindNode) isNode()     {}
func (SetBindNode) isSetValue() {}

func (SubqueryNode) isNode()        {}
func (SubqueryNode) isProjection()  {}
func (SubqueryNode) isScalarValue() {}
func (SubqueryNode) isSetValue()    {}

func (AliasNode) isNode()       {}
func (AliasNode) isProjection() {}

func (JSONPairNode) isNode() {}

func (*Select) isNode() {}
