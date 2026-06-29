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
// SQL, a weight-1 tree compiles into one of two query shapes:
//
//   - A condition-free plan compiles to a single boolean query whose HAVING clause folds
//     the union / intersection / exclusion over COUNT(*) FILTER (...) atoms — the database
//     decides the whole Check.
//   - A plan that mentions any ABAC condition compiles to a single "gather" scan that
//     pulls every candidate tuple (relation + condition columns) for the tree's operands;
//     the executor then evaluates CEL in process and folds the operators over the rows.
//
// This iteration supports weight-1 and weight-2 resolution paths. Weight-1 paths reach the
// subject type directly (stored `this` tuples, same-type computed usersets, and same-type
// set operators). A weight-2 path needs exactly one userset or tuple-to-userset hop; each
// such hop compiles to its own self-join query (a JoinNode) that cannot fold with the rest
// of the tree, so a plan containing any weight-2 hop runs every leaf query in parallel and
// folds their booleans in process. Paths above weight two, and recursion (infinite weight),
// return ErrUnsupportedWeight.
package planner

import (
	"errors"

	"github.com/openfga/openfga/pkg/storage/adapter"
)

// ErrUnsupportedWeight is returned when traversal reaches a resolution path whose weight
// to the terminal subject type exceeds two — a path needing more than one userset or
// tuple-to-userset hop — or a recursive (infinite-weight) relation. This iteration supports
// weight-1 and weight-2 paths; deeper and recursive handling lands in a future iteration.
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

// JoinNode is a weight-2 leaf of the plan: a single tuple-to-userset or userset hop that
// resolves to the subject through one intermediate object. It cannot fold into the shared
// weight-1 query, so it compiles to its own self-join query and runs in parallel with the
// rest of the tree; its boolean result is then combined in process by the enclosing
// CombineNodes.
//
// A JoinNode covers exactly one intermediate type. The model may grant a relation through
// several intermediate types (e.g. `parent: [folder, org]`); the traversal emits one
// JoinNode per type, all under a union, so each query stays single-type and index-friendly.
//
//	hop 1 (Hop1Relation): the bound object's tupleset / userset relation, whose stored
//	  tuples name the intermediate objects (TTU: `document#parent` tuples → folder objects;
//	  userset: `document#viewer` tuples → `group:..#member` usersets).
//	hop 2 (Hop2): on the intermediate type, the weight-1 plan subtree that grants the bound
//	  subject (e.g. `folder#admin` resolving to `[user]`, a union, or a full set-operation
//	  tree like `a and b`). The subtree is folded per intermediate object, so a hop-2
//	  intersection requires one intermediate object that satisfies every operand — not
//	  operands spread across different objects.
type JoinNode struct {
	// Hop1Relation is the relation on the bound object whose tuples name intermediate
	// objects: the tupleset relation for a TTU (`parent`), or the relation being checked
	// for a userset (`viewer`).
	Hop1Relation string
	// Hop1Conditions are the ABAC conditions on the hop-1 edge (the edge into this
	// intermediate type), e.g. [""] when unconditioned. For a TTU the conditions live on
	// the tupleset tuples; for a userset on the `viewer` tuples.
	Hop1Conditions []string
	// Hop1SubjectRelation is the subject_relation the hop-1 tuples carry, and the source of
	// truth for the self-join's t1 subject side: "" for a TTU (whose tuples name an
	// intermediate object), or the declared userset relation for a userset (whose tuples
	// name an intermediate userset, e.g. "member" in `[group#member]`). This is not always
	// a hop-2 leaf relation: `[group#member]` where member is itself `a or b` keeps the
	// userset relation "member" on t1 while the hop-2 subtree resolves to a and b.
	Hop1SubjectRelation string
	// IsTTU records the hop shape for tracing and tests; it is informational only, since
	// the SQL is driven by Hop1SubjectRelation (IsTTU is equivalent to it being empty).
	IsTTU bool
	// IntermediateType is the object type the hop lands on (e.g. "folder" or "group").
	IntermediateType string
	// Hop2 is the weight-1 plan subtree, on the intermediate type, that grants the bound
	// subject. It is the same shape the planner builds for a top-level weight-1 relation (a
	// QueryNode, or a CombineNode of QueryNodes for set operations), and is folded per
	// intermediate object: the hop is satisfied iff some intermediate object reached from
	// the bound object via hop 1 satisfies it.
	Hop2 Node
	// Weight is the traversal cost of this hop. Always 2 here.
	Weight int
	// Label identifies the hop this node was built for, e.g. "document#viewer→folder#admin",
	// for tracing and test assertions.
	Label string
}

func (*JoinNode) isNode() {}

// ConditionFree reports whether neither hop carries an ABAC condition: the hop-1 edge is
// unconditioned and the entire hop-2 subtree is condition-free. Only then can the join
// resolve as a single boolean self-join with no in-process CEL.
func (j *JoinNode) ConditionFree() bool {
	if len(j.Hop1Conditions) != 1 || j.Hop1Conditions[0] != "" {
		return false
	}
	return j.Hop2.ConditionFree()
}

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
	// The unitMulti kind runs one query per leaf (a weight-1 QueryNode or a weight-2
	// JoinNode), in parallel, then folds the operator tree over the per-leaf booleans. It
	// is used whenever the tree contains a weight-2 join, which cannot fold into a single
	// statement.
	unitMulti
)

// leafKind selects how the executor resolves one leaf query in a unitMulti plan.
type leafKind int

const (
	// The leafBool kind runs a boolean existence query: granted iff it returns a row.
	leafBool leafKind = iota
	// The leafGather kind runs a scan whose rows the executor evaluates with CEL: a
	// single-tuple gather for a conditioned weight-1 leaf.
	leafGather
	// The leafJoinGather kind runs a self-join scan for a conditioned weight-2 hop: the
	// executor evaluates the conditioned hop(s) over the joined rows.
	leafJoinGather
)

// leafQuery pairs a plan node with its compiled query and the kind that tells the executor how
// to resolve it. The node is the fold key: the executor records each unit's boolean result
// against its node, then folds the operator tree (multiRoot) over those results.
//
// A unit is one of: a single weight-1 leaf, a single weight-2 JoinNode, or — when weight-1
// siblings were merged — a whole weight-1 region (a CombineNode root). For a merged region that
// carries a condition, subLeaves are the region's leaves: the executor gathers the region's rows
// in one scan, attributes them to these leaves, then folds the region subtree (node) over them.
// It is nil for a boolean unit (condition-free region or JoinNode) and for a lone leaf.
type leafQuery struct {
	node      Node
	query     adapter.Query
	kind      leafKind
	subLeaves []*QueryNode
}

// unit is the compiled execution form of a Plan: the query to run (if any) and, for the
// gather form, the leaves whose satisfaction the executor attributes from the rows. For the
// multi form it holds the per-leaf queries and the tree root to fold.
type unit struct {
	kind   unitKind
	query  adapter.Query
	leaves []*QueryNode

	// multi and multiRoot are set only for unitMulti: one query per leaf, plus the root
	// over which the executor folds the per-leaf booleans.
	multi     []leafQuery
	multiRoot Node
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
