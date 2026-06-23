// Package planner translates an OpenFGA Check request into a single SQL query against
// the `tuple` table and resolves it in process.
//
// It traverses the weighted authorization model graph
// (github.com/openfga/language/pkg/go/graph, obtained via
// typesystem.TypeSystem.GetWeightedGraph) from the entry node `objectType#relation`
// toward the terminal subject type, building a plan tree as it goes: interior
// CombineNode values carry the relation's union / intersection / exclusion operator, and
// leaf QueryNode values describe a region of the model (a relation that grants the bound
// subject directly).
//
// The set operations run in the database. Because CEL conditions cannot be evaluated in
// SQL, the planner compiles the tree into one of two query shapes:
//
//   - A condition-free plan compiles to a single boolean query whose HAVING clause folds
//     the union / intersection / exclusion over COUNT(*) FILTER (...) atoms — the database
//     decides the whole Check.
//   - A plan that mentions any ABAC condition compiles to a single "gather" scan that
//     pulls every candidate tuple (relation + condition columns) for the tree's operands;
//     the executor then evaluates CEL in process and folds the operators over the rows.
//
// This iteration supports weight-1 resolution paths only — those that reach the subject
// type directly (stored `this` tuples, same-type computed usersets, and same-type set
// operators). A path that needs a userset or tuple-to-userset hop has weight greater than
// one, and recursion has infinite weight; traversal returns ErrUnsupportedWeight for both.
package planner

import (
	"errors"

	"github.com/openfga/openfga/pkg/storage/adapter"
)

// ErrUnsupportedWeight is returned when traversal reaches a resolution path whose weight
// to the terminal subject type is greater than one — a userset or tuple-to-userset hop,
// or a recursive (infinite-weight) relation. This iteration supports weight-1 paths only;
// higher-weight handling lands in a future iteration.
var ErrUnsupportedWeight = errors.New("planner: resolution path weight not yet supported")

// CombineOp is the set operation a CombineNode applies over its children, mirroring the
// model's union / intersection / exclusion rewrites.
type CombineOp int

const (
	// CombineUnion yields subjects present in any child (OR).
	CombineUnion CombineOp = iota
	// CombineIntersect yields subjects present in every child (AND).
	CombineIntersect
	// CombineExcept yields subjects in the first child and not the second
	// (base BUT NOT subtract). It always has exactly two children.
	CombineExcept
)

// Node is one element of a plan tree: either a CombineNode (a set operation over
// children) or a QueryNode (a leaf describing a directly-granting relation).
type Node interface {
	isNode()
	// ConditionFree reports whether this node and everything under it can be resolved
	// without evaluating any ABAC condition, so its set algebra can be decided entirely
	// in the database.
	ConditionFree() bool
}

// QueryNode is a leaf of the plan: a relation on the bound object that grants the bound
// subject directly. It records the coordinates needed to filter the `tuple` table for
// this region (the in-effect relation, the bound subject, and the accepted conditions),
// rather than a prebuilt query — the planner compiles the whole tree into one query.
//
// In this iteration every QueryNode has Weight == 1 (a same-type region). The field is
// retained so future weight-2 userset / TTU hops slot in without reshaping the tree.
type QueryNode struct {
	// Relation is the object-side relation (the `relation` column) whose stored tuples
	// grant this region, after any computed-userset rewrite (`viewer: editor` → editor).
	Relation string
	// SubjectID is the subject this leaf matches: the bound Check subject's id, or "*"
	// for a public-access (type-bound wildcard) terminal.
	SubjectID string
	// SubjectRelation is the bound subject's relation ("" for a non-userset subject,
	// which is also satisfied by the public-access wildcard tuple).
	SubjectRelation string
	// Conditions are the contributing edge's ABAC conditions, e.g. [""] for an
	// unconditioned `[user]`, ["c"] for `[user with c]`, ["", "c"] for `[user, user with c]`.
	// The empty string is the unconditioned sentinel (graph.NoCond).
	Conditions []string
	// Weight is the traversal cost of the path this leaf resolves. Always 1 here.
	Weight int
	// Label identifies the relation this leaf was built for, e.g. "document#viewer", for
	// tracing and test assertions.
	Label string
}

func (*QueryNode) isNode() {}

// ConditionFree reports whether the leaf accepts only unconditioned tuples. A leaf is
// condition-free iff its sole accepted condition is the empty-string sentinel; a nil or
// multi-valued Conditions slice means a condition may apply, so it is not.
func (q *QueryNode) ConditionFree() bool {
	return len(q.Conditions) == 1 && q.Conditions[0] == ""
}

// CombineNode is an interior plan node: it combines its children's result sets using Op.
type CombineNode struct {
	Op       CombineOp
	Children []Node
}

func (*CombineNode) isNode() {}

// ConditionFree reports whether every child is condition-free. An empty CombineNode (the
// unreachable-subject placeholder) is vacuously condition-free.
func (c *CombineNode) ConditionFree() bool {
	for _, child := range c.Children {
		if !child.ConditionFree() {
			return false
		}
	}
	return true
}

// unitKind selects how Plan.Execute resolves the compiled query.
type unitKind int

const (
	// The unitFalse kind needs no query because the Check is trivially false (the subject
	// type is unreachable).
	unitFalse unitKind = iota
	// The unitHaving kind runs one boolean query; the database folds the set algebra in
	// HAVING and the Check is granted iff the query returns a row.
	unitHaving
	// The unitGather kind runs one scan that gathers candidate tuples; the executor
	// evaluates CEL and folds the operator tree over the rows.
	unitGather
)

// unit is the compiled execution form of a Plan: the query to run (if any) and, for the
// gather form, the leaves whose satisfaction the executor attributes from the rows.
type unit struct {
	kind   unitKind
	query  adapter.Query
	leaves []*QueryNode
}

// Plan is the root of a planned Check: the query tree plus the bound store and subject
// the executor tests membership against. The subject fields are the parsed parts of the
// Check user (via pkg/tuple.ToUserParts): for "user:alice" they are ("user", "alice", "");
// for the userset "group:eng#member" they are ("group", "eng", "member").
type Plan struct {
	Root            Node
	StoreID         string
	SubjectType     string
	SubjectID       string
	SubjectRelation string

	// unit is the compiled query form, built at plan time so Execute needs no builder.
	unit unit
}
