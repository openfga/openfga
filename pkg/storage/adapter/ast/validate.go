package ast

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

// --- validation -------------------------------------------------------------------------
//
// Validate is this package's answer to the three things the position categories cannot type,
// enumerated in the package doc: CARDINALITY, REQUIRED-NON-NIL, and CROSS-FIELD relationships.
// Those rules were documented per-node long before they were checked, which left every consumer
// to either re-derive them or trust them; a tree violating one still compiled, and several
// violations rendered as valid-looking nonsense (`WHERE ()`, `COUNT(DISTINCT *)`) rather than
// failing. Validate makes those rules mechanical.
//
// Each node checks its OWN rules and then calls Validate on each direct child node, returning the
// FIRST error it finds and stopping there. Two consequences worth planning for:
//
//   - One call on the root validates the WHOLE tree. A consumer needs exactly one call, at its
//     boundary, and does not walk anything itself to get this.
//   - The result names ONE violation, not all of them. A tree with three faults reports one, and
//     fixing it reveals the next. The error carries the path it was found at (`Where: Parts[1]:
//     Left: required field is nil`), which is what makes a single finding actionable.
//
// Because only the first fault is reported, the ORDER of the checks is part of the behaviour rather
// than an implementation detail. Every node checks in the same order: its own local rules first —
// enum range, arity, alias, cross-field — and only then recurses into children, in field order.
// That reports the shallowest, cheapest fault available, which is also the one most likely to be
// the cause rather than a consequence: a node whose Fn is a Count sentinel is worth hearing about
// before the arity that sentinel makes meaningless.
//
// Every error wraps one of the sentinels below, so a consumer can branch on the KIND of
// violation with errors.Is without parsing text. The path prefix is for humans only.
//
// What Validate deliberately does NOT check:
//
//   - ALIAS SCOPE. That a ColNode.Alias names a Table in From or Joins is checkable only with the
//     enclosing Select in hand, and for a correlated subquery only with the whole enclosing chain.
//     A node validating its own subtree cannot see either. Select does check for DUPLICATE aliases
//     among its own tables, which is local and makes a ColNode ambiguous.
//   - WHETHER A BOUND VALUE IS ENCODABLE. Which Go types a driver accepts is the driver's fact.
//     Validate rejects only the values this tree's own model excludes — a bool, and an AST node.
//   - GROUPING CORRECTNESS. That every non-aggregated projection appears in GROUP BY is a rule
//     about the whole statement's semantics, not its shape, and engines themselves disagree on it.
var (
	// ErrMissing reports a required interface-typed or pointer field left nil.
	ErrMissing = errors.New("required field is nil")

	// ErrArity reports a slice field whose length violates the node's documented minimum, or the
	// per-function arity of a FuncNode or AggNode.
	ErrArity = errors.New("wrong number of elements")

	// ErrEnum reports an enum field outside its defined set — most often a Count sentinel, which
	// is assignable to the field it counts and which Go cannot prevent.
	ErrEnum = errors.New("enum value out of range")

	// ErrCrossField reports fields that are each individually well-formed but jointly meaningless,
	// such as a COUNT(DISTINCT *) — Distinct set with no aggregate argument.
	ErrCrossField = errors.New("invalid combination of fields")

	// ErrValue reports a bound or inlined Go value this tree cannot carry: a bool, since truth
	// values are the Predicate category, or an AST node.
	ErrValue = errors.New("invalid value")

	// ErrAlias reports an empty alias string. Nothing else about an alias is checked here —
	// character set and quoting are a target's concern.
	ErrAlias = errors.New("invalid alias")
)

// --- helpers ----------------------------------------------------------------------------

// validator is what the helpers below range over. It is every Node, plus the five clause-item
// types that hold child nodes without belonging to a category themselves (SearchedBranch,
// SimpleBranch, OrderTerm, Table, JoinClause).
type validator interface{ Validate() error }

// enum is the underlying kind of every enum in this package.
type enum interface{ ~int | ~uint8 }

// at labels an error with the field it was found in, building the path prefix one frame at a time
// as the recursion returns. Wrapping with %w keeps errors.Is working through every frame, so a
// consumer can still branch on the sentinel however deep the finding was.
func at(field string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", field, err)
}

// required validates a child that must be present. It reports ErrMissing for a nil interface
// rather than panicking, since reporting is the whole point of this pass.
func required[T validator](field string, x T) error {
	if any(x) == nil {
		return fmt.Errorf("%s: %w", field, ErrMissing)
	}
	return at(field, x.Validate())
}

// optional validates a child that may be absent.
func optional[T validator](field string, x T) error {
	if any(x) == nil {
		return nil
	}
	return at(field, x.Validate())
}

// stmt validates a required *Select. It cannot go through required, because a typed nil pointer
// in an interface is not a nil interface — exactly the case that would otherwise nil-dereference.
func stmt(field string, s *Select) error {
	if s == nil {
		return fmt.Errorf("%s: %w", field, ErrMissing)
	}
	return at(field, s.Validate())
}

// each validates elements in order and returns at the first bad one, labelling the error with that
// element's index. Stopping early means an element's subtree is never walked once an earlier
// sibling has failed.
func each[T validator](field string, xs []T) error {
	for i, x := range xs {
		if err := required(field+"["+strconv.Itoa(i)+"]", x); err != nil {
			return err
		}
	}
	return nil
}

// fault reports a violation at a named field. It exists so a message can LEAD with a field name —
// capitalized, as every exported Go field is — without each call site tripping the lint against
// capitalized error strings. The field is data here rather than the start of a sentence.
func fault(field string, sentinel error, msg string) error {
	return fmt.Errorf("%s: %w: %s", field, sentinel, msg)
}

// nonEmpty enforces the minimum arity shared by every slice field that has one: at least one
// element. No node in this package requires two or more — the meaningful-but-not-required cases
// (a second LogicalNode part, a second COALESCE argument) are documented, not checked, because a
// one-element form still renders correctly.
func nonEmpty[T any](field string, xs []T) error {
	if len(xs) == 0 {
		return fmt.Errorf("%s: %w: empty, need at least one element", field, ErrArity)
	}
	return nil
}

// arity enforces an exact arity, or a range when lo and hi differ.
func arity(field string, have, lo, hi int) error {
	if have < lo || have > hi {
		if lo == hi {
			return fmt.Errorf("%s: %w: have %d, need exactly %d", field, ErrArity, have, lo)
		}
		return fmt.Errorf("%s: %w: have %d, need %d to %d", field, ErrArity, have, lo, hi)
	}
	return nil
}

// inRange rejects an enum value outside its defined set, Count sentinels included.
func inRange[T enum](field string, v, lo, hi T) error {
	if v < lo || v > hi {
		return fmt.Errorf("%s: %w: %v", field, ErrEnum, v)
	}
	return nil
}

// alias rejects an empty alias string.
func alias(field, s string) error {
	if s == "" {
		return fmt.Errorf("%s: %w: empty", field, ErrAlias)
	}
	return nil
}

// value rejects the Go values this tree's model excludes from a bound or inlined slot. The bool
// test is on reflect.Kind, not on the concrete type, so a NAMED bool (type flag bool) is caught
// too — the same reason package sql dispatches literals on kind.
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

// Validate reports an unnamed table or an out-of-range column. It cannot check that Alias is in
// SCOPE; only the enclosing Select could, and not even it for a correlated reference.
func (n ColNode) Validate() error {
	if err := alias("Alias", n.Alias); err != nil {
		return err
	}
	return inRange("Name", n.Name, ColObjectType, ColCount-1)
}

// Validate reports a Value this tree cannot carry. It does NOT report a value the driver cannot
// encode, which is the driver's rule to state — see the note on BindNode about the width of this
// field.
func (n BindNode) Validate() error { return value("Value", n.Value) }

// Validate reports a Value this tree cannot carry. Whether the value can be SPELLED inline is a
// separate, target-specific question that a consumer's literal rendering answers.
func (n LitNode) Validate() error { return value("Value", n.Value) }

// Validate checks elements in order and returns at the first bad one. An EMPTY Elems is valid — the
// one arity exception in the package — and QuantifiedNode documents the constant it lowers to.
func (n SetBindNode) Validate() error {
	for i, e := range n.Elems {
		if err := value("Elems["+strconv.Itoa(i)+"]", e); err != nil {
			return err
		}
	}
	return nil
}

// --- predicate nodes --------------------------------------------------------------------

// Validate checks the operator and both operands.
func (n CompareNode) Validate() error {
	if err := inRange("Op", n.Op, OpEq, OpGte); err != nil {
		return err
	}
	if err := required("Left", n.Left); err != nil {
		return err
	}
	return required("Right", n.Right)
}

// Validate checks both operands.
func (n LikeNode) Validate() error {
	if err := required("Left", n.Left); err != nil {
		return err
	}
	return required("Pattern", n.Pattern)
}

// Validate checks all three operands. It does not check that Lo precedes Hi, which is a fact
// about values rather than shape and may not be knowable until execution.
func (n BetweenNode) Validate() error {
	if err := required("Inner", n.Inner); err != nil {
		return err
	}
	if err := required("Lo", n.Lo); err != nil {
		return err
	}
	return required("Hi", n.Hi)
}

// Validate checks the operand.
func (n IsNullNode) Validate() error { return required("Inner", n.Inner) }

// Validate rejects an EMPTY Elems: "x IN ()" has no rendering on any engine. A caller meaning
// "matches nothing" writes ConstPredNode{Value: false}.
func (n InNode) Validate() error {
	if err := nonEmpty("Elems", n.Elems); err != nil {
		return err
	}
	if err := required("Left", n.Left); err != nil {
		return err
	}
	return each("Elems", n.Elems)
}

// Validate checks the operator, the quantifier, and both operands. An empty SetBindNode in Set is
// valid and carries the lowering documented on this node.
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

// Validate rejects an EMPTY Parts: "WHERE ()" is a syntax error. A single part is valid, per the
// note on this node.
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

// Validate always succeeds: both truth values are meaningful and there is nothing else to check.
func (ConstPredNode) Validate() error { return nil }

// --- value nodes ------------------------------------------------------------------------

// Validate checks the cast target and its operand.
func (n CastNode) Validate() error {
	if err := inRange("Type", n.Type, TypeVarchar, TypeCount-1); err != nil {
		return err
	}
	return required("Inner", n.Inner)
}

// Validate enforces the PER-FUNCTION arity documented on each ScalarFunc member. The arity is not
// a property of Args — one slice serves every function — so it can only be checked against Fn, and
// Fn is therefore checked FIRST: an out-of-range Fn makes any arity verdict meaningless.
func (n FuncNode) Validate() error {
	if err := inRange("Fn", n.Fn, FuncCoalesce, FuncCount-1); err != nil {
		return err
	}
	if err := n.argArity(); err != nil {
		return err
	}
	return each("Args", n.Args)
}

// argArity applies the per-Fn rule stated on each ScalarFunc member.
func (n FuncNode) argArity() error {
	switch n.Fn {
	case FuncCoalesce:
		return nonEmpty("Args", n.Args)
	case FuncLower, FuncUpper:
		return arity("Args", len(n.Args), 1, 1)
	case FuncJSONArray:
		return nil // Zero or more: JSON_ARRAY() is meaningful, and the only such function here.
	case FuncCount:
		return nil // Not a function; already reported by Validate's range check.
	default:
		return nil
	}
}

// Validate checks both halves of the pair. A pair is reachable only from the two JSON object
// constructors, so this is never called from a projection walk.
func (n JSONPairNode) Validate() error {
	if err := required("Key", n.Key); err != nil {
		return err
	}
	return required("Value", n.Value)
}

// Validate rejects an EMPTY Pairs. The empty object is excluded because it is not portable —
// contrast FuncJSONArray, whose empty form is admitted.
func (n JSONObjectNode) Validate() error {
	if err := nonEmpty("Pairs", n.Pairs); err != nil {
		return err
	}
	return each("Pairs", n.Pairs)
}

// Validate enforces the PER-AGGREGATE arity documented on each AggFunc member — AggCount takes
// zero or one — and rejects Distinct without an argument, since COUNT(DISTINCT *) is not a
// construct. Those two rules together are what stops the empty-Args form from rendering as a
// fabricated "*" operand for an aggregate that has no such form.
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

// argArity applies the per-Fn rule stated on each AggFunc member, plus the one cross-field rule it
// implies: Distinct is meaningful only alongside an argument.
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

// Validate rejects an EMPTY Branches: "CASE END" is a syntax error, and a CASE with only an ELSE
// is just that value.
func (n CaseSearchedNode) Validate() error {
	if err := nonEmpty("Branches", n.Branches); err != nil {
		return err
	}
	if err := each("Branches", n.Branches); err != nil {
		return err
	}
	return optional("Else", n.Else)
}

// Validate checks the guard and the result.
func (b SearchedBranch) Validate() error {
	if err := required("When", b.When); err != nil {
		return err
	}
	return required("Then", b.Then)
}

// Validate rejects a nil Base as well as an empty Branches. A nil Base is not the searched form —
// that is a separate node — but a malformed simple one.
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

// Validate checks the aliased projection and rejects an empty output name. Nothing else about the
// name is checked: whether it needs quoting, and whether it collides with another output name in
// the same list, are decided in the Select that holds it and the target that renders it.
func (n AliasNode) Validate() error {
	if err := alias("Alias", n.Alias); err != nil {
		return err
	}
	return required("Inner", n.Inner)
}

// Validate checks the embedded statement.
func (n SubqueryNode) Validate() error { return stmt("Stmt", n.Stmt) }

// --- statement and clause items ---------------------------------------------------------

// Validate checks the one cross-field rule local to this statement — no two of its tables may
// share an alias — and then every clause, in source order. Having without GroupBy is permitted,
// as documented, and so is an empty From.
//
// The alias check comes first deliberately. It is cheap, and its consequence is quiet: an
// ambiguous column returns wrong rows rather than failing, so it is worth reporting ahead of a
// nil operand that would announce itself.
//
// It does NOT check that a ColNode anywhere below names an alias in scope. Doing so would mean
// collecting column references from every clause and threading enclosing scopes through
// subqueries, which is a different traversal than this one — a resolver rather than a
// shape-checker — and a correlated subquery makes it non-local in principle.
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

// aliasErr reports the first table alias used twice in one statement, which makes every ColNode
// naming it ambiguous. Empty aliases are left to Table.Validate, so they are not also reported here
// as duplicates of each other.
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

// aliases lists every table alias the statement introduces, in source order.
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

// Validate rejects an unnamed table occurrence, which would render as a bare table name that no
// column could qualify against.
func (t Table) Validate() error { return alias("Alias", t.Alias) }

// Validate checks the join flavour, the joined table, and On's presence — required for every
// flavour but JoinCross, and forbidden for that one, since a consumer silently dropping the
// condition would change which rows return.
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

// onErr applies the cross-field rule between Type and On.
func (j JoinClause) onErr() error {
	switch cross := j.Type == JoinCross; {
	case cross && j.On != nil:
		return fault("On", ErrCrossField, "a cross join carries no condition")
	case !cross && j.On == nil:
		return fault("On", ErrMissing, "only a cross join may omit it")
	default:
		return nil
	}
}

// Validate checks the ordering expression and its direction. Dir has a meaningful zero value, so
// only Expr can be missing.
func (t OrderTerm) Validate() error {
	if err := inRange("Dir", t.Dir, Ascending, Descending); err != nil {
		return err
	}
	return required("Expr", t.Expr)
}
