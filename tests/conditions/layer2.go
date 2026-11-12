package conditions

import (
	"fmt"
	"strings"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// tri is a three-valued truth value. A conditioned tuple that cannot be
// evaluated is unknown, and unknown must never turn into an allow.
type tri int

const (
	triFalse tri = iota
	triTrue
	triUnknown
)

func triNot(a tri) tri {
	switch a {
	case triTrue:
		return triFalse
	case triFalse:
		return triTrue
	default:
		return triUnknown
	}
}

func triAnd(a, b tri) tri {
	switch {
	case a == triFalse || b == triFalse:
		return triFalse
	case a == triUnknown || b == triUnknown:
		return triUnknown
	default:
		return triTrue
	}
}

func triOr(a, b tri) tri {
	switch {
	case a == triTrue || b == triTrue:
		return triTrue
	case a == triUnknown || b == triUnknown:
		return triUnknown
	default:
		return triFalse
	}
}

func (a tri) result() Result {
	switch a {
	case triTrue:
		return Allowed
	case triFalse:
		return Denied
	default:
		return Error
	}
}

// tupleOutcome is how one conditioned tuple evaluates for a request. The
// context builder says what the request sends for the tuple's parameter.
type tupleOutcome struct {
	Name  string
	Value tri
	// Sent reports the parameter value to send. ok false leaves it out.
	Sent func() (value any, ok bool)
}

func sent(v any) func() (any, bool) { return func() (any, bool) { return v, true } }
func notSent() (any, bool)          { return nil, false }

// The conditioned tuples in Layer 2 use `<param> == 'A'` on a string parameter.
var (
	outcomeTrue      = tupleOutcome{Name: "true", Value: triTrue, Sent: sent("A")}
	outcomeFalse     = tupleOutcome{Name: "false", Value: triFalse, Sent: sent("B")}
	outcomeMissing   = tupleOutcome{Name: "missing", Value: triUnknown, Sent: notSent}
	outcomeWrongType = tupleOutcome{Name: "wrong-type", Value: triUnknown, Sent: sent(float64(7))}

	// Outcomes of the tuple that applies to the user.
	userOutcomes = []tupleOutcome{outcomeTrue, outcomeFalse, outcomeMissing, outcomeWrongType}
	// Outcomes of a second tuple that does not apply to the user.
	otherOutcomes = []tupleOutcome{outcomeTrue, outcomeFalse, outcomeMissing}
)

// slot is one way a conditioned tuple can reach the user.
type slot struct {
	Name string
	// Relations are the DSL lines that define the relation `slot`.
	// COND stands for the condition name.
	Relations string
	// UserTuples are the tuples that make `slot` apply to user:u on object.
	UserTuples func(n int, object string, cond *openfgav1.RelationshipCondition) []*openfgav1.TupleKey
	// OtherTuples are tuples on the same relation that apply to user:v only.
	// Nil when the slot is only ever read for the user being checked.
	OtherTuples func(n int, object string, cond *openfgav1.RelationshipCondition) []*openfgav1.TupleKey
}

var slots = []slot{
	{
		Name:      "direct",
		Relations: "    define slot: [user with COND]\n",
		UserTuples: func(_ int, object string, cond *openfgav1.RelationshipCondition) []*openfgav1.TupleKey {
			return []*openfgav1.TupleKey{Tuple("user:u", "slot", object, cond)}
		},
	},
	{
		Name:      "wildcard",
		Relations: "    define slot: [user:* with COND]\n",
		UserTuples: func(_ int, object string, cond *openfgav1.RelationshipCondition) []*openfgav1.TupleKey {
			return []*openfgav1.TupleKey{Tuple("user:*", "slot", object, cond)}
		},
	},
	{
		Name:      "userset",
		Relations: "    define slot: [group#member with COND]\n",
		UserTuples: func(_ int, object string, cond *openfgav1.RelationshipCondition) []*openfgav1.TupleKey {
			return []*openfgav1.TupleKey{Tuple("group:gu#member", "slot", object, cond)}
		},
		OtherTuples: func(_ int, object string, cond *openfgav1.RelationshipCondition) []*openfgav1.TupleKey {
			return []*openfgav1.TupleKey{Tuple("group:gv#member", "slot", object, cond)}
		},
	},
	{
		Name:      "parent",
		Relations: "    define parent: [folder with COND]\n    define slot: viewer from parent\n",
		UserTuples: func(n int, object string, cond *openfgav1.RelationshipCondition) []*openfgav1.TupleKey {
			folder := fmt.Sprintf("folder:fu%d", n)
			return []*openfgav1.TupleKey{Tuple("user:u", "viewer", folder, nil), Tuple(folder, "parent", object, cond)}
		},
		OtherTuples: func(n int, object string, cond *openfgav1.RelationshipCondition) []*openfgav1.TupleKey {
			folder := fmt.Sprintf("folder:fv%d", n)
			return []*openfgav1.TupleKey{Tuple("user:v", "viewer", folder, nil), Tuple(folder, "parent", object, cond)}
		},
	},
	{
		Name:      "parent-computed",
		Relations: "    define plain_parent: [folder]\n    define slot: cond_viewer from plain_parent\n",
		UserTuples: func(n int, object string, cond *openfgav1.RelationshipCondition) []*openfgav1.TupleKey {
			folder := fmt.Sprintf("folder:fc%d", n)
			return []*openfgav1.TupleKey{Tuple("user:u", "cond_viewer", folder, cond), Tuple(folder, "plain_parent", object, nil)}
		},
	},
}

// wrapper is one way the relation `slot` is used by the relation `r` that
// the cell checks. The user always has `base` and never has `nobase`.
type wrapper struct {
	Name      string
	Relations string
	Eval      func(slot tri) tri
}

var wrappers = []wrapper{
	{Name: "self", Relations: "    define r: slot\n", Eval: func(s tri) tri { return s }},
	{Name: "slot-and-base", Relations: "    define r: slot and base\n", Eval: func(s tri) tri { return triAnd(s, triTrue) }},
	{Name: "base-and-slot", Relations: "    define r: base and slot\n", Eval: func(s tri) tri { return triAnd(triTrue, s) }},
	{Name: "nobase-and-slot", Relations: "    define r: nobase and slot\n", Eval: func(s tri) tri { return triAnd(triFalse, s) }},
	{Name: "base-or-slot", Relations: "    define r: base or slot\n", Eval: func(s tri) tri { return triOr(triTrue, s) }},
	{Name: "nobase-or-slot", Relations: "    define r: nobase or slot\n", Eval: func(s tri) tri { return triOr(triFalse, s) }},
	{Name: "base-but-not-slot", Relations: "    define r: base but not slot\n", Eval: func(s tri) tri { return triAnd(triTrue, triNot(s)) }},
	{Name: "slot-but-not-nobase", Relations: "    define r: slot but not nobase\n", Eval: func(s tri) tri { return triAnd(s, triNot(triFalse)) }},
	{Name: "nobase-but-not-slot", Relations: "    define r: nobase but not slot\n", Eval: func(s tri) tri { return triAnd(triFalse, triNot(s)) }},
	{
		Name:      "nested-but-not",
		Relations: "    define inner: base but not slot\n    define r: base but not inner\n",
		Eval:      func(s tri) tri { return triAnd(triTrue, triNot(triAnd(triTrue, triNot(s)))) },
	},
}

// secondTuple says whether a second conditioned tuple sits on the same
// relation, and whether it is written before or after the user's tuple.
type secondTuple string

const (
	secondNone   secondTuple = "none"
	secondBefore secondTuple = "second-written-first"
	secondAfter  secondTuple = "second-written-last"
)

// Layer2 builds the Composition layer: every slot under every wrapper, for
// every outcome of the tuple that applies to the user.
//
// With second set, the slots that can hold one get a second conditioned tuple
// that applies to another user only, and every outcome of that tuple is
// crossed with every outcome of the user's tuple. The second tuple never
// changes the correct answer for the user. It is there because a server may
// still read and evaluate it.
//
// The second-tuple variants are generated for $expression.
func Layer2(kind Kind, second secondTuple) *Group {
	g := &Group{Name: fmt.Sprintf("layer2-%s-%s", kind, second)}

	cond1Name := InlineName
	cond1 := InlineCondition(`p == 'A'`, map[string]string{"p": "string"})
	cond2 := InlineCondition(`q == 'A'`, map[string]string{"q": "string"})
	conditions := ""
	if kind == KindNamed {
		cond1Name = "c1"
		cond1, cond2 = NamedCondition("c1"), NamedCondition("c2")
		conditions = "\ncondition c1(p: string) {\n  p == 'A'\n}\n\ncondition c2(q: string) {\n  q == 'A'\n}\n"
	}

	var model strings.Builder
	fmt.Fprintf(&model, `model
  schema 1.1

type user

type group
  relations
    define member: [user]

type folder
  relations
    define viewer: [user]
    define cond_viewer: [user with %s]
`, cond1Name)

	g.Tuples = append(g.Tuples,
		Tuple("user:u", "member", "group:gu", nil),
		Tuple("user:v", "member", "group:gv", nil),
	)

	n := 0
	for _, s := range slots {
		if second != secondNone && s.OtherTuples == nil {
			continue
		}
		for _, w := range wrappers {
			n++
			objectType := fmt.Sprintf("p%d", n)
			object := objectType + ":1"

			relations := strings.ReplaceAll(s.Relations, "COND", cond1Name)
			if kind == KindNamed {
				// The second tuple uses its own named condition.
				relations = strings.ReplaceAll(relations, " with c1]", " with c1, "+restrictionSubject(s)+" with c2]")
			}
			fmt.Fprintf(&model, "\ntype %s\n  relations\n    define base: [user]\n    define nobase: [user]\n%s%s",
				objectType, relations, w.Relations)

			own := s.UserTuples(n, object, cond1)
			var other []*openfgav1.TupleKey
			if second != secondNone {
				other = s.OtherTuples(n, object, cond2)
			}
			g.Tuples = append(g.Tuples, Tuple("user:u", "base", object, nil))
			if second == secondBefore {
				g.Tuples = append(g.Tuples, other...)
				g.Tuples = append(g.Tuples, own...)
			} else {
				g.Tuples = append(g.Tuples, own...)
				g.Tuples = append(g.Tuples, other...)
			}

			others := []tupleOutcome{{Name: "", Value: triFalse, Sent: notSent}}
			if second != secondNone {
				others = otherOutcomes
			}
			for _, mine := range userOutcomes {
				for _, theirs := range others {
					g.Cells = append(g.Cells, layer2Cell(kind, second, s, w, object, mine, theirs))
				}
			}
		}
	}

	g.ModelDSL = model.String() + conditions
	return g
}

// restrictionSubject is the subject part of the slot's conditioned type
// restriction, used to add a second named condition to it.
func restrictionSubject(s slot) string {
	switch s.Name {
	case "userset":
		return "group#member"
	case "parent":
		return "folder"
	case "wildcard":
		return "user:*"
	default:
		return "user"
	}
}

func layer2Cell(kind Kind, second secondTuple, s slot, w wrapper, object string, mine, theirs tupleOutcome) Cell {
	reqCtx := map[string]any{}
	if v, ok := mine.Sent(); ok {
		reqCtx["p"] = v
	}
	if v, ok := theirs.Sent(); ok {
		reqCtx["q"] = v
	}
	if len(reqCtx) == 0 {
		reqCtx = nil
	}

	id := fmt.Sprintf("L2/%s/%s/%s/%s/mine=%s", kind, s.Name, w.Name, second, mine.Name)
	if second != secondNone {
		id += "/other=" + theirs.Name
	}

	// The second tuple applies to another user, so the answer for user:u
	// depends on the user's own tuple alone.
	want := w.Eval(mine.Value).result()
	cell := Cell{ID: id, User: "user:u", Relation: "r", Object: object, Context: reqCtx, Want: want}

	otherUnknown := second != secondNone && theirs.Value == triUnknown
	switch {
	case want == Error:
		// An error is the only correct result.
	case mine.Value == triUnknown:
		// The user's tuple cannot be evaluated, and the rest of the relation
		// decides the result anyway.
		cell.ErrorAlsoOK = AllAPIs
		cell.Provisional = ProvisionalTrueWithUnevaluable
	case otherUnknown && mine.Value == triTrue:
		cell.ErrorAlsoOK = AllAPIs
		cell.Provisional = ProvisionalTrueWithUnevaluable
	case otherUnknown:
		cell.ErrorAlsoOK = AllAPIs
		cell.Provisional = ProvisionalUnrelatedUnevaluable
	}
	return cell
}
