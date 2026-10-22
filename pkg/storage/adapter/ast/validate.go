package ast

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

var (
	ErrMissing    = errors.New("required field is nil")
	ErrArity      = errors.New("wrong number of elements")
	ErrEnum       = errors.New("enum value out of range")
	ErrCrossField = errors.New("invalid combination of fields")
	ErrValue      = errors.New("invalid value")
	ErrAlias      = errors.New("invalid alias")
)

// --- helpers ----------------------------------------------------------------------------

// validator is used instead of Node so the helpers also accept clause items like OrderTerm.
type validator interface{ Validate() error }

type enum interface{ ~int | ~uint8 }

// at prefixes err with field, building the error path.
func at(field string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", field, err)
}

// isNil also treats a typed nil pointer as nil, since calling a value-receiver Validate on it panics.
func isNil[T validator](x T) bool {
	if any(x) == nil {
		return true
	}
	v := reflect.ValueOf(any(x))
	return v.Kind() == reflect.Pointer && v.IsNil()
}

func required[T validator](field string, x T) error {
	if isNil(x) {
		return fmt.Errorf("%s: %w", field, ErrMissing)
	}
	return at(field, x.Validate())
}

func optional[T validator](field string, x T) error {
	if isNil(x) {
		return nil
	}
	return at(field, x.Validate())
}

func stmt(field string, s *Select) error {
	if s == nil {
		return fmt.Errorf("%s: %w", field, ErrMissing)
	}
	return at(field, s.Validate())
}

func each[T validator](field string, xs []T) error {
	for i, x := range xs {
		if err := required(field+"["+strconv.Itoa(i)+"]", x); err != nil {
			return err
		}
	}
	return nil
}

// fault exists so messages can start with a capitalized field name without tripping lint.
func fault(field string, sentinel error, msg string) error {
	return fmt.Errorf("%s: %w: %s", field, sentinel, msg)
}

func nonEmpty[T any](field string, xs []T) error {
	if len(xs) == 0 {
		return fmt.Errorf("%s: %w: empty, need at least one element", field, ErrArity)
	}
	return nil
}

func arity(field string, have, lo, hi int) error {
	if have < lo || have > hi {
		if lo == hi {
			return fmt.Errorf("%s: %w: have %d, need exactly %d", field, ErrArity, have, lo)
		}
		return fmt.Errorf("%s: %w: have %d, need %d to %d", field, ErrArity, have, lo, hi)
	}
	return nil
}

func inRange[T enum](field string, v, lo, hi T) error {
	if v < lo || v > hi {
		return fmt.Errorf("%s: %w: %v", field, ErrEnum, v)
	}
	return nil
}

func alias(field, s string) error {
	if s == "" {
		return fmt.Errorf("%s: %w: empty", field, ErrAlias)
	}
	return nil
}

// value checks Kind rather than type so named bools are rejected too.
func value(field string, v any) error {
	if v == nil {
		return nil // NULL
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

func (n ColNode) Validate() error {
	if err := alias("Alias", n.Alias); err != nil {
		return err
	}
	return inRange("Name", n.Name, ColObjectType, ColCount-1)
}

func (n BindNode) Validate() error { return value("Value", n.Value) }

func (n LitNode) Validate() error { return value("Value", n.Value) }

func (n SetBindNode) Validate() error {
	for i, e := range n.Elems {
		if err := value("Elems["+strconv.Itoa(i)+"]", e); err != nil {
			return err
		}
	}
	return nil
}

// --- predicate nodes --------------------------------------------------------------------

func (n CompareNode) Validate() error {
	if err := inRange("Op", n.Op, OpEq, OpGte); err != nil {
		return err
	}
	if err := required("Left", n.Left); err != nil {
		return err
	}
	return required("Right", n.Right)
}

func (n LikeNode) Validate() error {
	if err := required("Left", n.Left); err != nil {
		return err
	}
	return required("Pattern", n.Pattern)
}

func (n BetweenNode) Validate() error {
	if err := required("Inner", n.Inner); err != nil {
		return err
	}
	if err := required("Lo", n.Lo); err != nil {
		return err
	}
	return required("Hi", n.Hi)
}

func (n IsNullNode) Validate() error { return required("Inner", n.Inner) }

func (n InNode) Validate() error {
	if err := nonEmpty("Elems", n.Elems); err != nil {
		return err
	}
	if err := required("Left", n.Left); err != nil {
		return err
	}
	return each("Elems", n.Elems)
}

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

func (n ExistsNode) Validate() error { return stmt("Stmt", n.Stmt) }

func (n LogicalNode) Validate() error {
	if err := inRange("Op", n.Op, And, Or); err != nil {
		return err
	}
	if err := nonEmpty("Parts", n.Parts); err != nil {
		return err
	}
	return each("Parts", n.Parts)
}

func (n NotNode) Validate() error { return required("Inner", n.Inner) }

func (ConstPredNode) Validate() error { return nil }

// --- value nodes ------------------------------------------------------------------------

func (n CastNode) Validate() error {
	if err := inRange("Type", n.Type, TypeVarchar, TypeCount-1); err != nil {
		return err
	}
	return required("Inner", n.Inner)
}

// Validate checks Fn before arity, since arity depends on Fn.
func (n FuncNode) Validate() error {
	if err := inRange("Fn", n.Fn, FuncCoalesce, FuncCount-1); err != nil {
		return err
	}
	if err := n.argArity(); err != nil {
		return err
	}
	return each("Args", n.Args)
}

func (n FuncNode) argArity() error {
	switch n.Fn {
	case FuncCoalesce:
		return nonEmpty("Args", n.Args)
	case FuncLower, FuncUpper:
		return arity("Args", len(n.Args), 1, 1)
	case FuncJSONArray:
		return nil
	default:
		return nil
	}
}

func (n JSONPairNode) Validate() error {
	if err := required("Key", n.Key); err != nil {
		return err
	}
	return required("Value", n.Value)
}

func (n JSONObjectNode) Validate() error {
	if err := nonEmpty("Pairs", n.Pairs); err != nil {
		return err
	}
	return each("Pairs", n.Pairs)
}

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
	default:
		return nil
	}
}

func (n CaseSearchedNode) Validate() error {
	if err := nonEmpty("Branches", n.Branches); err != nil {
		return err
	}
	if err := each("Branches", n.Branches); err != nil {
		return err
	}
	return optional("Else", n.Else)
}

func (b SearchedBranch) Validate() error {
	if err := required("When", b.When); err != nil {
		return err
	}
	return required("Then", b.Then)
}

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

func (b SimpleBranch) Validate() error {
	if err := required("When", b.When); err != nil {
		return err
	}
	return required("Then", b.Then)
}

func (n AliasNode) Validate() error {
	if err := alias("Alias", n.Alias); err != nil {
		return err
	}
	return required("Inner", n.Inner)
}

func (n SubqueryNode) Validate() error { return stmt("Stmt", n.Stmt) }

// --- statement and clause items ---------------------------------------------------------

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

// aliasErr reports a table alias used twice. Empty aliases are left to Table.Validate.
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

func (t Table) Validate() error { return alias("Alias", t.Alias) }

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

func (t OrderTerm) Validate() error {
	if err := inRange("Dir", t.Dir, Ascending, Descending); err != nil {
		return err
	}
	return required("Expr", t.Expr)
}
