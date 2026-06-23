package planner

import (
	"fmt"

	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/tuple"
)

// Planner builds a Plan for a Check by traversing a weighted authorization model graph
// and emitting adapter queries. A Planner is stateless and safe to reuse.
type Planner struct {
	builder adapter.Builder
}

// New returns a Planner that builds its queries with the given adapter.Builder. Pass a
// render-only builder (no executor) to inspect or test the generated SQL, or an
// engine-backed builder to produce executable queries.
func New(builder adapter.Builder) *Planner {
	return &Planner{builder: builder}
}

// Plan traverses the weighted graph from the entry node objectType#relation toward the
// Check subject's type and returns the resulting Plan. User is the full Check subject
// (e.g. "user:alice" or "group:eng#member"); it is split with tuple.ToUserParts.
//
// This iteration supports weight-1 resolution paths only. If the relation reaches the
// subject type through any userset or tuple-to-userset hop (weight > 1) or recursion
// (infinite weight), Plan returns ErrUnsupportedWeight.
func (p *Planner) Plan(g *graph.WeightedAuthorizationModelGraph, store, objectType, objectID, relation, user string) (*Plan, error) {
	subjectType, subjectID, subjectRelation := tuple.ToUserParts(user)

	entryID := tuple.ToObjectRelationString(objectType, relation)
	entry, ok := g.GetNodeByID(entryID)
	if !ok {
		return nil, fmt.Errorf("planner: no node for %q", entryID)
	}

	w := &walker{
		planner:         p,
		graph:           g,
		store:           store,
		objectType:      objectType,
		objectID:        objectID,
		subjectType:     subjectType,
		subjectID:       subjectID,
		subjectRelation: subjectRelation,
		wildcardKey:     subjectType + ":*",
		visited:         make(map[string]struct{}),
	}

	// The node's aggregate weight to the subject type is the max over every
	// contributing path, so a weight of 1 guarantees the whole relevant subgraph is
	// weight-1; anything higher means at least one path needs a hop or recursion we do
	// not yet support. (The walk re-checks per edge defensively.)
	weight, reachable := w.weightTo(entry)
	if !reachable {
		// The relation cannot reach the subject type at all: the Check is trivially
		// false. An empty union resolves to false in the executor.
		return &Plan{
			Root:            &CombineNode{Op: CombineUnion},
			StoreID:         store,
			SubjectType:     subjectType,
			SubjectID:       subjectID,
			SubjectRelation: subjectRelation,
		}, nil
	}
	if weight != 1 {
		return nil, ErrUnsupportedWeight
	}

	root, err := w.walk(entry, relation, nil)
	if err != nil {
		return nil, err
	}

	plan := &Plan{
		Root:            root,
		StoreID:         store,
		SubjectType:     subjectType,
		SubjectID:       subjectID,
		SubjectRelation: subjectRelation,
	}
	plan.unit = p.compile(bound{
		store:           store,
		objectType:      objectType,
		objectID:        objectID,
		subjectType:     subjectType,
		subjectID:       subjectID,
		subjectRelation: subjectRelation,
	}, root)
	return plan, nil
}

// compile reduces a planned tree to the single query the executor runs. A condition-free
// tree folds its whole set algebra in the database via a HAVING clause; a tree that
// mentions any ABAC condition gathers its candidate tuples in one scan for in-process CEL
// evaluation and folding. An empty tree (unreachable subject type) needs no query.
func (p *Planner) compile(bnd bound, root Node) unit {
	if cn, ok := root.(*CombineNode); ok && len(cn.Children) == 0 {
		return unit{kind: unitFalse}
	}
	if root.ConditionFree() {
		return unit{kind: unitHaving, query: buildHavingQuery(p.builder, bnd, root)}
	}
	leaves := collectLeaves(root)
	return unit{kind: unitGather, query: buildGatherQuery(p.builder, bnd, leaves), leaves: leaves}
}

// walker carries the immutable Check context threaded through the recursive traversal.
type walker struct {
	planner         *Planner
	graph           *graph.WeightedAuthorizationModelGraph
	store           string
	objectType      string
	objectID        string
	subjectType     string
	subjectID       string
	subjectRelation string
	wildcardKey     string // subjectType + ":*", the weight key for public-access paths

	visited map[string]struct{} // node ids on the current resolution, guards against cycles
}

// walk returns the plan subtree for node, where relation is the object-side relation in
// effect at this point of the traversal (it changes as computed-userset edges are
// followed) and conds are the conditions of the edge that reached node (nil at the entry
// node, and unused except at a terminal leaf). The returned Node is either a QueryNode (a
// leaf same-type query) or a CombineNode (an operator over children).
func (w *walker) walk(node *graph.WeightedAuthorizationModelNode, relation string, conds []string) (Node, error) {
	switch node.GetNodeType() {
	case graph.SpecificType, graph.SpecificTypeWildcard:
		// Reached the terminal subject type: the bound object stores tuples for
		// `relation` granting this subject type directly. A wildcard terminal is only
		// satisfied by the public-access tuple (subject_id = "*"). The reaching edge's
		// conditions decide whether this leaf needs CEL evaluation.
		subjectID := w.subjectID
		if node.GetNodeType() == graph.SpecificTypeWildcard {
			subjectID = wildcardID
		}
		return &QueryNode{
			Relation:        relation,
			SubjectID:       subjectID,
			SubjectRelation: w.subjectRelation,
			Conditions:      conds,
			Weight:          1,
			Label:           tuple.ToObjectRelationString(w.objectType, relation),
		}, nil

	case graph.OperatorNode:
		return w.walkOperator(node, relation)

	case graph.SpecificTypeAndRelation:
		// A relation node names its own object-side relation (object_type#relation). For
		// weight-1 paths every such node is on the bound object type, so its relation
		// becomes the one in effect for everything reached from it — this is how
		// computed usersets (`viewer: editor`) switch the relation, whether the model
		// reaches the sibling via a computed or a rewrite edge.
		_, rel := tuple.SplitObjectRelation(node.GetUniqueLabel())
		return w.walkEdges(node, rel)

	case graph.LogicalDirectGrouping, graph.LogicalTTUGrouping:
		return w.walkEdges(node, relation)

	default:
		return nil, fmt.Errorf("planner: unsupported node type %d for %q", node.GetNodeType(), node.GetUniqueLabel())
	}
}

// walkOperator builds a CombineNode for a union / intersection / exclusion node.
//
// Exclusion (`base BUT NOT subtract`) is positional: the graph builder always emits the
// base edge before the subtract edge, so it is handled separately to preserve those
// roles. If the subtract operand cannot reach the subject type, it can never subtract a
// relevant grant, so the exclusion reduces to its base. Union and intersection are
// order-independent and handled by walking every relevant operand edge.
func (w *walker) walkOperator(node *graph.WeightedAuthorizationModelNode, relation string) (Node, error) {
	op, err := combineOpFor(node.GetLabel())
	if err != nil {
		return nil, err
	}

	if op == CombineExcept {
		return w.walkExclusion(node, relation)
	}

	// Union and intersection are order-independent: walk every relevant operand edge.
	combined, err := w.walkEdges(node, relation)
	if err != nil {
		return nil, err
	}
	cn, ok := combined.(*CombineNode)
	if !ok {
		// A single relevant operand collapsed to a leaf; wrap it so the operator is
		// explicit (an intersection of one is still that one, but stays well-formed).
		cn = &CombineNode{Children: []Node{combined}}
	}
	cn.Op = op
	return cn, nil
}

// walkExclusion builds the CombineExcept node for `base BUT NOT subtract`, using edge
// position (base first, subtract second) rather than subject-type relevance.
func (w *walker) walkExclusion(node *graph.WeightedAuthorizationModelNode, relation string) (Node, error) {
	if err := w.enter(node); err != nil {
		return nil, err
	}
	defer w.leave(node)

	edges, ok := w.graph.GetEdgesFromNode(node)
	if !ok || len(edges) != 2 {
		return nil, fmt.Errorf("planner: exclusion %q expected 2 operand edges, got %d", node.GetUniqueLabel(), len(edges))
	}

	baseWeight, baseRelevant := w.edgeWeight(edges[0])
	if !baseRelevant {
		// The base cannot reach the subject type, so nothing is granted: false.
		return &CombineNode{Op: CombineUnion}, nil
	}
	if baseWeight != 1 {
		return nil, ErrUnsupportedWeight
	}
	base, err := w.walkEdge(edges[0], relation)
	if err != nil {
		return nil, err
	}

	subWeight, subRelevant := w.edgeWeight(edges[1])
	if !subRelevant {
		// The subtract cannot contain a relevant subject; exclusion reduces to base.
		return base, nil
	}
	if subWeight != 1 {
		return nil, ErrUnsupportedWeight
	}
	subtract, err := w.walkEdge(edges[1], relation)
	if err != nil {
		return nil, err
	}

	return &CombineNode{Op: CombineExcept, Children: []Node{base, subtract}}, nil
}

// walkEdges walks every relevant outgoing edge of node and returns their children under
// a CombineNode (union). A single child is returned directly so trivial rewrites (e.g.
// `viewer: editor`) stay flat.
func (w *walker) walkEdges(node *graph.WeightedAuthorizationModelNode, relation string) (Node, error) {
	if err := w.enter(node); err != nil {
		return nil, err
	}
	defer w.leave(node)

	edges, err := w.relevantEdges(node)
	if err != nil {
		return nil, err
	}
	children, err := w.walkChildren(edges, relation)
	if err != nil {
		return nil, err
	}
	if len(children) == 1 {
		return children[0], nil
	}
	return &CombineNode{Op: CombineUnion, Children: children}, nil
}

// relevantEdges returns node's outgoing edges that have a weight-1 path to the subject
// type, erroring if any relevant edge has weight greater than one.
func (w *walker) relevantEdges(node *graph.WeightedAuthorizationModelNode) ([]*graph.WeightedAuthorizationModelEdge, error) {
	edges, ok := w.graph.GetEdgesFromNode(node)
	if !ok {
		return nil, fmt.Errorf("planner: no edges from %q", node.GetUniqueLabel())
	}
	var relevant []*graph.WeightedAuthorizationModelEdge
	for _, edge := range edges {
		weight, ok := w.edgeWeight(edge)
		if !ok {
			continue
		}
		if weight != 1 {
			return nil, ErrUnsupportedWeight
		}
		relevant = append(relevant, edge)
	}
	return relevant, nil
}

// walkChildren walks each edge to its child plan node.
func (w *walker) walkChildren(edges []*graph.WeightedAuthorizationModelEdge, relation string) ([]Node, error) {
	var children []Node
	for _, edge := range edges {
		child, err := w.walkEdge(edge, relation)
		if err != nil {
			return nil, err
		}
		if child != nil {
			children = append(children, child)
		}
	}
	return children, nil
}

// enter / leave maintain the visited set that detects cycles (which imply recursion, an
// unsupported weight).
func (w *walker) enter(node *graph.WeightedAuthorizationModelNode) error {
	if _, seen := w.visited[node.GetUniqueLabel()]; seen {
		return ErrUnsupportedWeight
	}
	w.visited[node.GetUniqueLabel()] = struct{}{}
	return nil
}

func (w *walker) leave(node *graph.WeightedAuthorizationModelNode) {
	delete(w.visited, node.GetUniqueLabel())
}

// walkEdge follows a single weight-1 edge to its child plan node. Relation is the
// object-side relation currently in effect; when the target is a relation node, walk
// re-derives the relation from that node, so computed (`viewer: editor`) and rewrite
// edges to a sibling relation both switch correctly.
func (w *walker) walkEdge(edge *graph.WeightedAuthorizationModelEdge, relation string) (Node, error) {
	switch edge.GetEdgeType() {
	case graph.DirectEdge, graph.RewriteEdge, graph.ComputedEdge, graph.DirectLogicalEdge, graph.TTULogicalEdge:
		// Direct edges reach the terminal type or a relation reference; rewrite/computed
		// edges reach an operator or sibling relation; logical edges reach a grouping
		// wrapper. In every weight-1 case the target node dictates how to proceed (and a
		// relation node re-derives the in-effect relation), so defer to walk. The edge's
		// conditions travel with it: they are recorded only if the target is a terminal
		// leaf, and ignored for operator / relation / grouping targets (which carry the
		// unconditioned sentinel and reach their own leaves through further edges).
		return w.walk(edge.GetTo(), relation, edge.GetConditions())

	case graph.TTUEdge:
		// Tuple-to-userset is always a hop (weight >= 2); unsupported this iteration.
		return nil, ErrUnsupportedWeight

	default:
		return nil, fmt.Errorf("planner: unsupported edge type %d", edge.GetEdgeType())
	}
}

// weightTo returns a node's weight to the subject type, preferring the concrete type
// and falling back to the public-access wildcard key. Reachable is false when neither
// key has a weight (no path to the subject type).
func (w *walker) weightTo(node *graph.WeightedAuthorizationModelNode) (weight int, reachable bool) {
	if v, ok := node.GetWeight(w.subjectType); ok {
		return v, true
	}
	if v, ok := node.GetWeight(w.wildcardKey); ok {
		return v, true
	}
	return 0, false
}

// edgeWeight is weightTo for an edge: it reports the edge's weight to the subject type
// (or wildcard) and whether the edge has any path to it.
func (w *walker) edgeWeight(edge *graph.WeightedAuthorizationModelEdge) (weight int, relevant bool) {
	if v, ok := edge.GetWeight(w.subjectType); ok {
		return v, true
	}
	if v, ok := edge.GetWeight(w.wildcardKey); ok {
		return v, true
	}
	return 0, false
}

// combineOpFor maps an operator node's label to a CombineOp.
func combineOpFor(label string) (CombineOp, error) {
	switch label {
	case graph.UnionOperator:
		return CombineUnion, nil
	case graph.IntersectionOperator:
		return CombineIntersect, nil
	case graph.ExclusionOperator:
		return CombineExcept, nil
	default:
		return 0, fmt.Errorf("planner: unknown operator %q", label)
	}
}
