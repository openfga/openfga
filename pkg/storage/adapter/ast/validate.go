package ast

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

var (
	// ErrMissing means a required field (interface or pointer) was left nil.
	ErrMissing = errors.New("required field is nil")

	// ErrArity means a list is shorter than the node needs, or a function/aggregate got the wrong
	// number of arguments.
	ErrArity = errors.New("wrong number of elements")

	// ErrEnum means an enum field is outside its range — usually a Count value, which can be assigned
	// to the field it counts.
	ErrEnum = errors.New("enum value out of range")

	// ErrCrossField means each field is fine on its own but the combination is not, e.g.
	// COUNT(DISTINCT *) — Distinct set with no argument.
	ErrCrossField = errors.New("invalid combination of fields")

	// ErrValue means a bound or inlined value the tree cannot hold — a bool (true/false is a
	// Predicate) or an AST node.
	ErrValue = errors.New("invalid value")

	// ErrAlias means an alias is empty. Allowed characters and quoting are the backend's concern.
	ErrAlias = errors.New("invalid alias")
)

// --- helpers ----------------------------------------------------------------------------

// validator is everything these helpers walk over: every Node, plus the five clause items that hold
// child nodes but fit no slot (SearchedBranch, SimpleBranch, OrderTerm, Table, JoinClause).
type validator interface{ Validate() error }

// enum is the underlying kind of every enum in this package.
type enum interface{ ~int | ~uint8 }

// at adds the field name to an error, building up the path as the recursion unwinds. Wrapping with %w
// keeps errors.Is working at every step.
func at(field string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", field, err)
}

// isNil reports whether x holds no node: either a nil interface, or an interface holding a typed nil
// pointer such as (*NotNode)(nil). The second case matters because the node methods have value
// receivers, so a typed nil satisfies the interface but panics when Validate tries to use the
// receiver. The stmt helper does the same check for *Select by hand.
func isNil[T validator](x T) bool {
	if any(x) == nil {
		return true
	}
	v := reflect.ValueOf(any(x))
	return v.Kind() == reflect.Pointer && v.IsNil()
}

// required validates a child that must be present, reporting ErrMissing when it is nil instead of
// panicking.
func required[T validator](field string, x T) error {
	if isNil(x) {
		return fmt.Errorf("%s: %w", field, ErrMissing)
	}
	return at(field, x.Validate())
}

// optional validates a child that may be absent.
func optional[T validator](field string, x T) error {
	if isNil(x) {
		return nil
	}
	return at(field, x.Validate())
}

// stmt validates a required *Select. It cannot use required, because a typed nil pointer inside an
// interface is not a nil interface.
func stmt(field string, s *Select) error {
	if s == nil {
		return fmt.Errorf("%s: %w", field, ErrMissing)
	}
	return at(field, s.Validate())
}

// each validates elements in order and returns at the first bad one, tagging the error with that
// element's index.
func each[T validator](field string, xs []T) error {
	for i, x := range xs {
		if err := required(field+"["+strconv.Itoa(i)+"]", x); err != nil {
			return err
		}
	}
	return nil
}

// fault reports a problem at a named field. It exists so the message can start with a capitalized field
// name without breaking the lint rule against capitalized error strings.
func fault(field string, sentinel error, msg string) error {
	return fmt.Errorf("%s: %w: %s", field, sentinel, msg)
}

// nonEmpty checks the one count rule shared by every list that has one: at least one element. No node
// needs two or more; a one-element form still renders fine.
func nonEmpty[T any](field string, xs []T) error {
	if len(xs) == 0 {
		return fmt.Errorf("%s: %w: empty, need at least one element", field, ErrArity)
	}
	return nil
}

// arity checks an exact count, or a range when lo and hi differ.
func arity(field string, have, lo, hi int) error {
	if have < lo || have > hi {
		if lo == hi {
			return fmt.Errorf("%s: %w: have %d, need exactly %d", field, ErrArity, have, lo)
		}
		return fmt.Errorf("%s: %w: have %d, need %d to %d", field, ErrArity, have, lo, hi)
	}
	return nil
}

// inRange rejects an enum value outside its range, Count values included.
func inRange[T enum](field string, v, lo, hi T) error {
	if v < lo || v > hi {
		return fmt.Errorf("%s: %w: %v", field, ErrEnum, v)
	}
	return nil
}

// alias rejects an empty alias.
func alias(field, s string) error {
	if s == "" {
		return fmt.Errorf("%s: %w: empty", field, ErrAlias)
	}
	return nil
}

// value rejects the Go values the tree does not allow in a bound or inlined slot. The bool check uses
// reflect.Kind, so a named bool (type flag bool) is caught too.
func value(field string, v any) error {
	if v == nil {
		return nil // a bound or inlined NULL.
	}
	if _, ok := v.(Node); ok {
		return fmt.Errorf("%s: %w: %T is an AST node, not a value", field, ErrValue, v)
	}
	if reflect.ValueOf(v).Kind() == reflect.Bool {
		return fmt.Errorf("%s: %w: %T is a bool; truth values are the Predicate category, and a "+
			"constant one is ConstPredNode", field, ErrValue, v)
	}
	return nil
}

// --- leaf nodes -------------------------------------------------------------------------

// Validate reports a table with no alias or an out-of-range column. It cannot check that Alias is in
// scope; only the surrounding SELECT could, and not even that for a correlated reference.
func (n ColNode) Validate() error {
	if err := alias("Alias", n.Alias); err != nil {
		return err
	}
	return inRange("Name", n.Name, ColObjectType, ColCount-1)
}

// Validate reports a Value the tree cannot hold. It does not report a value the driver cannot handle;
// see BindNode.
func (n BindNode) Validate() error { return value("Value", n.Value) }

// Validate reports a Value the tree cannot hold. Whether the value can be written straight into SQL is
// a separate, database-specific question, answered when the backend inlines it.
func (n LitNode) Validate() error { return value("Value", n.Value) }

// Validate checks the elements in order and returns at the first bad one. An empty Elems is fine — the
// one empty list the package allows.
func (n SetBindNode) Validate() error {
	for i, e := range n.Elems {
		if err := value("Elems["+strconv.Itoa(i)+"]", e); err != nil {
			return err
		}
	}
	return nil
}

// --- predicate nodes --------------------------------------------------------------------

// Validate checks the operator and both sides.
func (n CompareNode) Validate() error {
	if err := inRange("Op", n.Op, OpEq, OpGte); err != nil {
		return err
	}
	if err := required("Left", n.Left); err != nil {
		return err
	}
	return required("Right", n.Right)
}

// Validate checks both sides.
func (n LikeNode) Validate() error {
	if err := required("Left", n.Left); err != nil {
		return err
	}
	return required("Pattern", n.Pattern)
}

// Validate checks all three values. It does not check that Lo is below Hi — that is about the values,
// not the shape, and may not be known until the query runs.
func (n BetweenNode) Validate() error {
	if err := required("Inner", n.Inner); err != nil {
		return err
	}
	if err := required("Lo", n.Lo); err != nil {
		return err
	}
	return required("Hi", n.Hi)
}

// Validate checks the value.
func (n IsNullNode) Validate() error { return required("Inner", n.Inner) }

// Validate rejects an empty Elems: "x IN ()" cannot be written on any database. To mean "matches
// nothing", use ConstPredNode{Value: false}.
func (n InNode) Validate() error {
	if err := nonEmpty("Elems", n.Elems); err != nil {
		return err
	}
	if err := required("Left", n.Left); err != nil {
		return err
	}
	return each("Elems", n.Elems)
}

// Validate checks the operator, the ANY/ALL, and both sides. An empty SetBindNode in Set is fine;
// QuantifiedNode explains what it becomes.
func (n QuantifiedNode) Validate() error {
	if err := inRange("Op", n.Op, OpEq, OpGte); err != nil {
		return err
	}
	if err := inRange("Q", n.Q, Any, All); err != nil {
		return err
	}
	if err := required("Left", n.Left); err != nil {
		return err
	}
	return required("Set", n.Set)
}

// Validate checks the subquery.
func (n ExistsNode) Validate() error { return stmt("Stmt", n.Stmt) }

// Validate rejects an empty Parts: "WHERE ()" is a syntax error. One part is fine, per LogicalNode.
func (n LogicalNode) Validate() error {
	if err := inRange("Op", n.Op, And, Or); err != nil {
		return err
	}
	if err := nonEmpty("Parts", n.Parts); err != nil {
		return err
	}
	return each("Parts", n.Parts)
}

// Validate checks the negated predicate.
func (n NotNode) Validate() error { return required("Inner", n.Inner) }

// Validate always passes: both true and false are fine, and there is nothing else to check.
func (ConstPredNode) Validate() error { return nil }

// --- value nodes ------------------------------------------------------------------------

// Validate checks the cast type and the value.
func (n CastNode) Validate() error {
	if err := inRange("Type", n.Type, TypeVarchar, TypeCount-1); err != nil {
		return err
	}
	return required("Inner", n.Inner)
}

// Validate checks the argument count for each ScalarFunc. The count depends on Fn, not Args (one list
// serves every function), so it is checked against Fn — and Fn is checked first, since an out-of-range
// Fn would make any count check meaningless.
func (n FuncNode) Validate() error {
	if err := inRange("Fn", n.Fn, FuncCoalesce, FuncCount-1); err != nil {
		return err
	}
	if err := n.argArity(); err != nil {
		return err
	}
	return each("Args", n.Args)
}

// argArity applies the per-function rule written on each ScalarFunc.
func (n FuncNode) argArity() error {
	switch n.Fn {
	case FuncCoalesce:
		return nonEmpty("Args", n.Args)
	case FuncLower, FuncUpper:
		return arity("Args", len(n.Args), 1, 1)
	case FuncJSONArray:
		return nil // Zero or more: JSON_ARRAY() is valid, and the only such function here.
	case FuncCount:
		return nil // Not a function; already reported by Validate's range check.
	default:
		return nil
	}
}

// Validate checks both halves of the pair.
func (n JSONPairNode) Validate() error {
	if err := required("Key", n.Key); err != nil {
		return err
	}
	return required("Value", n.Value)
}

// Validate rejects an empty Pairs. An empty JSON object is not supported everywhere, unlike
// FuncJSONArray, whose empty form is allowed.
func (n JSONObjectNode) Validate() error {
	if err := nonEmpty("Pairs", n.Pairs); err != nil {
		return err
	}
	return each("Pairs", n.Pairs)
}

// Validate checks the argument count for each AggFunc — AggCount takes zero or one — and rejects
// Distinct with no argument, since COUNT(DISTINCT *) does not exist.
func (n AggNode) Validate() error {
	if err := inRange("Fn", n.Fn, AggCount, AggCount_-1); err != nil {
		return err
	}
	if err := n.argArity(); err != nil {
		return err
	}
	if err := each("Args", n.Args); err != nil {
		return err
	}
	return optional("Filter", n.Filter)
}

// argArity applies the per-aggregate rule on each AggFunc, plus the one cross-field rule it implies:
// Distinct only makes sense with an argument.
func (n AggNode) argArity() error {
	switch n.Fn {
	case AggCount:
		if err := arity("Args", len(n.Args), 0, 1); err != nil {
			return err
		}
		if n.Distinct && len(n.Args) == 0 {
			return fault("Distinct", ErrCrossField,
				"COUNT(DISTINCT *) is not a construct; Distinct needs an argument")
		}
		return nil
	case AggCount_:
		return nil // Not an aggregate; already reported by Validate's range check.
	default:
		return nil
	}
}

// Validate rejects an empty Branches: "CASE END" is a syntax error.
func (n CaseSearchedNode) Validate() error {
	if err := nonEmpty("Branches", n.Branches); err != nil {
		return err
	}
	if err := each("Branches", n.Branches); err != nil {
		return err
	}
	return optional("Else", n.Else)
}

// Validate checks the test and the result.
func (b SearchedBranch) Validate() error {
	if err := required("When", b.When); err != nil {
		return err
	}
	return required("Then", b.Then)
}

// Validate rejects a nil Base as well as an empty Branches. A nil Base is an invalid simple CASE, not
// the searched form.
func (n CaseSimpleNode) Validate() error {
	if err := nonEmpty("Branches", n.Branches); err != nil {
		return err
	}
	if err := required("Base", n.Base); err != nil {
		return err
	}
	if err := each("Branches", n.Branches); err != nil {
		return err
	}
	return optional("Else", n.Else)
}

// Validate checks both sides of the pair.
func (b SimpleBranch) Validate() error {
	if err := required("When", b.When); err != nil {
		return err
	}
	return required("Then", b.Then)
}

// Validate checks the result column and rejects an empty name. Whether the name needs quoting or
// clashes with another is decided by the Select and the backend.
func (n AliasNode) Validate() error {
	if err := alias("Alias", n.Alias); err != nil {
		return err
	}
	return required("Inner", n.Inner)
}

// Validate checks the embedded statement.
func (n SubqueryNode) Validate() error { return stmt("Stmt", n.Stmt) }

// --- statement and clause items ---------------------------------------------------------

// Validate checks that no two of the statement's tables share an alias, then checks every clause,
// in order.
func (s *Select) Validate() error {
	if s == nil {
		return ErrMissing
	}
	if err := s.aliasErr(); err != nil {
		return err
	}
	for _, check := range []func() error{
		func() error { return each("Columns", s.Columns) },
		func() error { return each("From", s.From) },
		func() error { return each("Joins", s.Joins) },
		func() error { return optional("Where", s.Where) },
		func() error { return each("GroupBy", s.GroupBy) },
		func() error { return optional("Having", s.Having) },
		func() error { return each("OrderBy", s.OrderBy) },
	} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

// aliasErr reports the first table alias used twice in one statement, which makes any column naming it
// ambiguous. Empty aliases are left to Table.Validate.
func (s *Select) aliasErr() error {
	seen := make(map[string]struct{}, len(s.From)+len(s.Joins))
	for _, a := range s.aliases() {
		if a == "" {
			continue
		}
		if _, dup := seen[a]; dup {
			return fmt.Errorf("%w: table alias %q is used twice, which makes a column naming it "+
				"ambiguous", ErrCrossField, a)
		}
		seen[a] = struct{}{}
	}
	return nil
}

// aliases lists every table alias the statement introduces, in order.
func (s *Select) aliases() []string {
	out := make([]string, 0, len(s.From)+len(s.Joins))
	for _, t := range s.From {
		out = append(out, t.Alias)
	}
	for _, j := range s.Joins {
		out = append(out, j.Table.Alias)
	}
	return out
}

// Validate rejects a table with no alias, which would render as a bare table name no column could
// attach to.
func (t Table) Validate() error { return alias("Alias", t.Alias) }

// Validate checks the join kind, the joined table, and whether On is present — required for every kind
// but CROSS, and forbidden for CROSS, since quietly dropping the condition would change which rows come
// back.
func (j JoinClause) Validate() error {
	if err := inRange("Type", j.Type, JoinInner, JoinCross); err != nil {
		return err
	}
	if err := j.onErr(); err != nil {
		return err
	}
	if err := at("Table", j.Table.Validate()); err != nil {
		return err
	}
	return optional("On", j.On)
}

// onErr applies the rule between Type and On.
func (j JoinClause) onErr() error {
	switch cross := j.Type == JoinCross; {
	case cross && !isNil(j.On):
		return fault("On", ErrCrossField, "a cross join carries no condition")
	case !cross && isNil(j.On):
		return fault("On", ErrMissing, "only a cross join may omit it")
	default:
		return nil
	}
}

// Validate checks the ordering expression and its direction. Dir has a usable default, so only Expr can
// be missing.
func (t OrderTerm) Validate() error {
	if err := inRange("Dir", t.Dir, Ascending, Descending); err != nil {
		return err
	}
	return required("Expr", t.Expr)
}
